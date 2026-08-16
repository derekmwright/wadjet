package exec

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// Partitioned parallel aggregation (docs in pipeline.runParallel):
// instead of every clone aggregating an arbitrary slice of rows — which
// duplicates hot groups per clone, multiplies state by k for
// high-cardinality keys, and forces the drain-sort-merge machinery
// (ClickBench Q17: 88% of a 30s profile inside drainStateToRuns) — each
// worker OWNS a hash partition of the group-key space. Workers split every
// batch into per-partition selection views and deliver them to the owning
// worker's queue; every group lives in exactly one sink, so no state is
// duplicated, drains fire only if a single partition alone exceeds its
// bound, and the post-run merge inserts disjoint keys.

// partitionedAggEnabled is the kill switch (WADJET_PARTITIONED_AGG=0).
var partitionedAggEnabled = os.Getenv("WADJET_PARTITIONED_AGG") != "0"

// PartitionedAggRuns counts pipelines that ran in partitioned mode
// (observability + test assertions).
var PartitionedAggRuns atomic.Int64

// partitionFallbacks counts whole-batch fallback consumptions (diagnostics).
var partitionFallbacks atomic.Int64

var debugPartition = os.Getenv("WADJET_DEBUG_PARTITION") == "1"

// BufferReusingOperator marks operators whose Execute output aliases
// buffers reused on the NEXT call (e.g. the aggregate pre-projection's
// computed vectors). Their batches cannot be shared across partition
// owners that consume asynchronously, so partitioned aggregation is
// disabled when one sits in the chain.
type BufferReusingOperator interface {
	ReusesOutputBuffers() bool
}

// OutputSharingAware operators can switch to per-call output allocation so
// their batches become safe to share across asynchronous partition owners
// (at the cost of losing buffer reuse). The pipeline enables it instead of
// disabling partitioned aggregation.
type OutputSharingAware interface {
	EnableSharedOutputs()
}

func opsReuseBuffers(ops []UnaryOperator) bool {
	for _, op := range ops {
		if br, ok := op.(BufferReusingOperator); ok && br.ReusesOutputBuffers() {
			if sa, ok := op.(OutputSharingAware); ok {
				sa.EnableSharedOutputs()
				continue
			}
			return true
		}
	}
	return false
}

// sharedBatch refcounts a batch fanned out to multiple partition owners.
type sharedBatch struct {
	b    *batch.RecordBatch
	refs atomic.Int32
}

func (s *sharedBatch) release() {
	if s.refs.Add(-1) == 0 {
		s.b.Release()
	}
}

// partitionItem is one partition's view of a shared batch: shared columns,
// private selection vector.
type partitionItem struct {
	view *batch.RecordBatch
	src  *sharedBatch
}

// selView builds a shallow view over b restricted to sel. The view shares
// column vectors (read-only) and is detached so its Release is a no-op —
// the shared batch's refcount owns the real release.
func selView(b *batch.RecordBatch, sel []uint32) *batch.RecordBatch {
	nb := *b
	nb.Sel = sel
	nb.Detach()
	return &nb
}

// PartitionSelectors splits b's active rows into parts selection lists by
// group-key hash. Returns nil when a group column is missing or of an
// unsupported type — callers fall back to unpartitioned consumption.
// scratch is reused across calls; the returned slices alias it.
func (h *HashAggregate) PartitionSelectors(b *batch.RecordBatch, parts int, scratch [][]uint32) [][]uint32 {
	if len(h.GroupByCols) == 0 {
		return nil
	}
	cols := make([]*batch.Vector, len(h.GroupByCols))
	for i, name := range h.GroupByCols {
		idx := columnIndexFallback(b, name)
		if idx < 0 {
			return nil
		}
		v := b.Columns[idx]
		switch v.Type {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration,
			batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate,
			batch.TypeFloat64, batch.TypeFloat32, batch.TypeBool,
			batch.TypeString, batch.TypeBytes, batch.TypeCIDR, batch.TypeIPv6, batch.TypeUUID:
		default:
			return nil
		}
		cols[i] = v
	}

	if cap(scratch) < parts {
		scratch = make([][]uint32, parts)
	}
	scratch = scratch[:parts]
	for i := range scratch {
		scratch[i] = scratch[i][:0]
	}

	hashRow := func(row int) uint64 {
		var acc uint64 = 0x9e3779b97f4a7c15
		for _, v := range cols {
			var hv uint64
			if v.Nulls.IsNullFast(row) {
				hv = 0xdeadbeef
			} else {
				switch v.Type {
				case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
					hv = mix64(uint64(v.Int64Data[row]))
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					hv = mix64(uint64(uint32(v.Int32Data[row])))
				case batch.TypeFloat64:
					hv = mix64(f64bits(v.Float64Data[row]))
				case batch.TypeFloat32:
					hv = mix64(f64bits(float64(v.Float32Data[row])))
				case batch.TypeBool:
					if v.BoolData[row] {
						hv = 0x9e37
					} else {
						hv = 0x79b9
					}
				default: // string-class
					hv = fnv1aBytes(v.BytesData.Value(row))
				}
			}
			acc = mix64(acc ^ hv)
		}
		return acc
	}

	up := uint64(parts)
	if b.Sel != nil {
		for _, idx := range b.Sel {
			p := hashRow(int(idx)) % up
			scratch[p] = append(scratch[p], idx)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			p := hashRow(i) % up
			scratch[p] = append(scratch[p], uint32(i))
		}
	}
	return scratch
}

func f64bits(f float64) uint64 {
	// NaN and ±0 canonicalization is unnecessary here: group-key equality
	// in the aggregate uses the same raw bit patterns.
	return math.Float64bits(f)
}

func fnv1aBytes(b []byte) uint64 {
	var h uint64 = 0xcbf29ce484222325
	for _, c := range b {
		h ^= uint64(c)
		h *= 0x100000001b3
	}
	return h
}

// partitionAndDeliver splits b by group-key hash and delivers each
// partition's view to its owner. The caller's own partition is consumed
// inline; deliveries drain the caller's own queue while blocked so two
// workers sending to each other's full queues can never deadlock. Falls
// back to consuming the whole batch locally when partition selectors can't
// be built (unresolvable or unsupported group columns).
func partitionAndDeliver(ctx context.Context, hasher *HashAggregate, sink Sink, b *batch.RecordBatch, workerID int, queues []chan partitionItem) error {
	parts := len(queues)
	sels := hasher.PartitionSelectors(b, parts, nil)
	if debugPartition && sels != nil {
		again := hasher.PartitionSelectors(b, parts, nil)
		for j := range sels {
			if len(sels[j]) != len(again[j]) {
				panic(fmt.Sprintf("partition nondeterminism: part %d %d vs %d rows", j, len(sels[j]), len(again[j])))
			}
		}
	}
	if sels == nil {
		partitionFallbacks.Add(1)
		if err := sink.Consume(ctx, b); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
		b.Release()
		return nil
	}

	// Pre-warm lazily-cached bitmap state single-threaded: HasNulls()
	// caches its answer on first call, and concurrent partition owners
	// would otherwise race on that write (same value, but a data race).
	// After this pass every owner's HasNulls() is a pure read.
	for _, col := range b.Columns {
		col.Nulls.HasNulls()
	}

	sh := &sharedBatch{b: b}
	sh.refs.Store(int32(parts))
	own := queues[workerID]
	for j := 0; j < parts; j++ {
		if j == workerID {
			continue
		}
		if len(sels[j]) == 0 {
			sh.release()
			continue
		}
		item := partitionItem{view: selView(b, sels[j]), src: sh}
		delivered := false
		for !delivered {
			select {
			case queues[j] <- item:
				delivered = true
			case it, ok := <-own:
				if !ok {
					// Own queue closed while still producing cannot happen
					// (this worker's producersWG slot is still held); treat
					// defensively as cancellation.
					sh.release()
					return ctx.Err()
				}
				if err := consumePartitionItem(ctx, sink, it); err != nil {
					sh.release()
					return err
				}
			case <-ctx.Done():
				sh.release()
				return ctx.Err()
			}
		}
	}

	// Own partition, consumed inline (empty is fine — skip the call).
	if len(sels[workerID]) > 0 {
		if err := sink.Consume(ctx, selView(b, sels[workerID])); err != nil {
			sh.release()
			return fmt.Errorf("sink consume: %w", err)
		}
	}
	sh.release()
	return nil
}

// consumePartitionItem consumes one delivered partition view and releases
// its shared-batch reference.
func consumePartitionItem(ctx context.Context, sink Sink, it partitionItem) error {
	err := sink.Consume(ctx, it.view)
	it.src.release()
	if err != nil {
		return fmt.Errorf("sink consume: %w", err)
	}
	return nil
}

// drainPartitionQueue consumes remaining deliveries until the queue closes
// (all producers done). On cancellation it keeps draining WITHOUT
// consuming, releasing references so shared batches aren't leaked.
func drainPartitionQueue(ctx context.Context, sink Sink, q chan partitionItem) {
	for it := range q {
		if ctx.Err() != nil {
			it.src.release()
			continue
		}
		if err := consumePartitionItem(ctx, sink, it); err != nil {
			// The error is surfaced by the worker that hit it first (or by
			// Finalize); keep draining to release references.
			continue
		}
	}
}
