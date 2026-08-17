package exec

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
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

// partitionedAggToggle is the kill switch (WADJET_PARTITIONED_AGG=0),
// registered so the invariance oracle sweeps it like every other
// optimization (it predates the optswitch registry).
var partitionedAggToggle = optswitch.Register("partitioned-agg", "WADJET_PARTITIONED_AGG",
	"per-worker hash-partitioned parallel aggregation (workers own disjoint group-key partitions)")

// PartitionedAggRuns counts pipelines that ran in partitioned mode
// (observability + test assertions).
var PartitionedAggRuns atomic.Int64

// partitionFallbacks counts whole-batch fallback consumptions (diagnostics).
var partitionFallbacks atomic.Int64

// HashOnceRoutedRows counts rows whose sink hash table consumed the router's
// hash instead of computing its own (observability + test assertions).
var HashOnceRoutedRows atomic.Int64

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

// sharedBatch refcounts a batch fanned out to multiple partition owners. It
// also owns the routing arrays those owners read (row ids + key hashes): the
// last reference to drop is by definition the point at which no partition view
// can still be reading them, so that is where they go back to the pool.
type sharedBatch struct {
	b    *batch.RecordBatch
	buf  *routedBuf
	refs atomic.Int32
}

func (s *sharedBatch) release() {
	if s.refs.Add(-1) == 0 {
		s.b.Release()
		if s.buf != nil {
			routedBufPool.Put(s.buf)
			s.buf = nil
		}
	}
}

// routedBuf backs one batch's routing output. Both arrays are carved into
// per-partition runs, handed to other workers, and recycled together.
type routedBuf struct {
	rows   []uint32
	hashes []uint64
}

var routedBufPool = sync.Pool{New: func() any { return new(routedBuf) }}

// routedBatch is one batch's routing result. The slice-of-slices headers are
// scratch (valid until the next PartitionSelectors call on the same scratch);
// the runs they point at live in buf, whose ownership passes to the
// sharedBatch.
type routedBatch struct {
	sels   [][]uint32
	hashes [][]uint64
	plan   *routePlan
	buf    *routedBuf
}

// partitionItem is one partition's view of a shared batch: shared columns,
// private selection vector, and — when the router and the owner's hash table
// agree on a key hash (see routePlan) — the per-row key hashes the owner's
// table would otherwise recompute. hashes[i] belongs to view.Sel[i].
type partitionItem struct {
	view   *batch.RecordBatch
	src    *sharedBatch
	hashes []uint64
	plan   *routePlan
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

// --- hash once: one key hash feeds partition selection AND slot indexing ---
//
// Every row used to be hashed TWICE under partitioned aggregation: the router
// hashed the whole key to pick an owner, then the owning sink's table hashed
// the same key again to pick a slot. On ClickBench Q34 that is the same ~88
// byte URL through two full hash passes per row.
//
// hashOnceToggle threads the router's hash to the sink instead. The two
// consumers read DISJOINT bit windows of the same 64-bit value — bit 63 down
// to bit 0, partition at the top, slot at the bottom, untouched bits between:
//
// partition owner = partitionFor(hash, parts), the high half of hash*parts,
// i.e. a function of the TOP ceil(log2(parts)) bits (exactly
// `hash >> (64 - radixBits)` when parts is a power of two, DuckDB's Shift).
//
// table slot = hash & (cap-1), the LOW log2(cap) bits — byte-for-byte what
// every table already did.
//
// parts is one per worker (<= 4096 in any plausible deployment) so the
// partition window is at most 12 bits; a table masking 40 bits would need
// 24 TiB of entries. The windows never meet. Nor does partitioning skew what
// is left: fixing the owner confines the hash to a contiguous interval of
// 2^64/parts >= 2^52 values, over which the low 40 bits are still uniform.
//
// WHICH function is unified matters. We adopt the SINK's, per key shape:
//
//	int64 key   fibHash(key) = key * phi. Its low bits are a bijection on the
//	            key's low bits, so dense integer ids keep the collision-free,
//	            sequential slot layout intHashTable was built around; its top
//	            bits depend on every input bit, which is the textbook use of a
//	            multiplicative hash and what the partitioner takes.
//	packed key  packedHash(lo, hi) — the 128-bit fold in packed_hash.go.
//	string key  strHash(key), whose low 32 bits are also strEntry.hashTag, so
//	            threading it skips the tag computation too.
//
// Going the other way (tables adopt the router's mix64/fnv1a) would have
// re-spread every table's slot distribution and cost the int path its
// sequential locality, for no gain. This way the tables see bit-for-bit the
// slot sequence they saw before; only the second hash pass disappears.
var hashOnceToggle = optswitch.Register("hash-once", "WADJET_HASH_ONCE",
	"thread the partition router's group-key hash into the owning sink's hash table (one hash per row instead of two)")

// keyHashKind names the hash function a routePlan commits to. It is frozen
// for the life of the aggregate: changing hash functions mid-query would
// re-route live keys and split a group across two owners.
type keyHashKind uint8

const (
	hashKindNone   keyHashKind = iota // legacy composite hash; nothing threaded
	hashKindInt                       // fibHash over a single int-class column
	hashKindPacked                    // packedHash over the 128-bit composite key
	hashKindStr                       // strHash over a single string/bytes column
)

// routeNullHash is where every NULL-keyed row routes. NULL keys never reach a
// threaded table (the int and packed paths migrate the whole aggregate to the
// generic path before consuming such a batch; the string path serves them from
// strNullGroupSlot without probing), so the value only has to be a constant —
// all NULLs are one group and belong to one owner.
const routeNullHash uint64 = 0x9ae16a3b2f90404f

// routePlan is the frozen decision about how this aggregate's group keys are
// hashed for routing. Built once from the first batch's schema and reused for
// every batch — see the mid-query re-routing hazard above.
//
// It deliberately carries its OWN copy of the packed layout rather than
// reading h.packedLayout: the router runs concurrently on the primary
// aggregate while that same instance is resolving its own indices as worker
// 0's sink.
type routePlan struct {
	kind   keyHashKind
	isI32  bool          // hashKindInt: key column is Int32-class
	layout []packedField // hashKindPacked: column -> word/shift in the 128-bit key
}

// partitionFor maps a key hash to one of parts owners using the HIGH bits.
//
// The predecessor was `hash % parts`, a hardware 64-bit divide per row (~20-40
// cycles, not pipelined). parts is the worker count — runtime.NumCPU() by way
// of scanParallelism(), so 12, 24, 96: NOT a power of two, and forcing it to
// one would either idle workers or oversubscribe them. So this is Lemire's
// multiply-shift instead of a mask: one MULQ, uniform over [0, parts), and
// for a power-of-two parts it is bit-identical to `hash >> (64 - radixBits)`.
func partitionFor(hash uint64, parts uint64) uint64 {
	hi, _ := bits.Mul64(hash, parts)
	return hi
}

// routePlanFor returns the aggregate's frozen routing plan, building it from
// b's schema on first call. Concurrency: the router is the shared primary
// aggregate, called from every worker goroutine; sync.Once both serializes the
// build and publishes the result.
func (h *HashAggregate) routePlanFor(b *batch.RecordBatch) *routePlan {
	h.routeOnce.Do(func() {
		h.routePlanV = buildRoutePlan(h.GroupByCols, b)
	})
	return h.routePlanV
}

// buildRoutePlan mirrors resolveIndices' fast-path selection using only the
// group columns' types, so the hash it names is the one the owning sink's
// table will use. A shape it cannot mirror (or a disabled kill switch) yields
// hashKindNone: routing still works, it just costs the legacy composite hash
// and threads nothing.
func buildRoutePlan(cols []string, b *batch.RecordBatch) *routePlan {
	if !hashOnceToggle.On() || len(cols) == 0 {
		return &routePlan{kind: hashKindNone}
	}
	types := make([]batch.TypeID, len(cols))
	for i, name := range cols {
		idx := columnIndexFallback(b, name)
		if idx < 0 {
			return &routePlan{kind: hashKindNone}
		}
		types[i] = b.Columns[idx].Type
	}
	if len(types) == 1 {
		switch types[0] {
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			return &routePlan{kind: hashKindInt, isI32: true}
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			return &routePlan{kind: hashKindInt}
		case batch.TypeString, batch.TypeBytes:
			return &routePlan{kind: hashKindStr}
		}
		return &routePlan{kind: hashKindNone}
	}
	// Same gate resolveIndices applies: 3-4 column shapes are packed only
	// while the packed-keys switch is on.
	if layout := buildPackedLayout(types); layout != nil &&
		(len(types) == 2 || packedKeysToggle.On()) {
		return &routePlan{kind: hashKindPacked, layout: layout}
	}
	return &routePlan{kind: hashKindNone}
}

// partitionScratch holds the routing pass's per-worker buffers: everything
// that does NOT leave the call. The row-id and hash runs do leave — they
// travel to other workers' queues — so they come from routedBufPool instead
// and are returned by the shared batch's last release.
type partitionScratch struct {
	hbuf   []uint64
	starts []int32
	offs   []int32
	sels   [][]uint32
	hashes [][]uint64
	cols   []*batch.Vector
	packed []packedKeyCol
	out    routedBatch
}

// PartitionSelectors splits b's active rows into parts selection lists by
// group-key hash. The result carries the per-partition row ids, the matching
// per-row key hashes (nil runs when the plan threads nothing), the plan, and
// the pooled buffer the caller must hand to the sharedBatch that owns the
// views. Returns nil when a group column is missing or of an unsupported type
// — callers fall back to unpartitioned consumption.
//
// Shape: a counting sort, not per-partition appends. One pass hashes every
// active row and builds the histogram; the prefix sums cut one output array
// into per-partition runs; the second pass scatters row ids (and hashes) into
// place. Row order within a partition is unchanged (the scatter is stable),
// and the per-batch allocation goes from ~parts append chains to a single
// pooled buffer the caller hands to the shared batch.
func (h *HashAggregate) PartitionSelectors(b *batch.RecordBatch, parts int, sc *partitionScratch) *routedBatch {
	if len(h.GroupByCols) == 0 || parts <= 0 {
		return nil
	}
	cols := sc.cols[:0]
	for _, name := range h.GroupByCols {
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
		cols = append(cols, v)
	}
	sc.cols = cols

	plan := h.routePlanFor(b)
	n := b.ActiveLen()
	if cap(sc.hbuf) < n {
		sc.hbuf = make([]uint64, n)
	}
	hb := sc.hbuf[:n]
	if !hashRowsByPlan(plan, cols, b, sc, hb) {
		// The frozen plan does not fit this batch's columns (a schema change
		// mid-pipeline, which no live path produces). Refusing to route is the
		// safe answer: silently switching hash functions would send live keys
		// to a different owner.
		return nil
	}

	if cap(sc.starts) < parts+1 {
		sc.starts = make([]int32, parts+1)
		sc.offs = make([]int32, parts)
	}
	starts := sc.starts[:parts+1]
	offs := sc.offs[:parts]
	for i := range starts {
		starts[i] = 0
	}
	up := uint64(parts)
	for i := 0; i < n; i++ {
		starts[partitionFor(hb[i], up)+1]++
	}
	for i := 0; i < parts; i++ {
		starts[i+1] += starts[i]
		offs[i] = starts[i]
	}

	buf := routedBufPool.Get().(*routedBuf)
	if cap(buf.rows) < n {
		buf.rows = make([]uint32, n)
	}
	selOut := buf.rows[:n]
	var hashOut []uint64
	if plan.kind != hashKindNone {
		if cap(buf.hashes) < n {
			buf.hashes = make([]uint64, n)
		}
		hashOut = buf.hashes[:n]
	}
	if b.Sel != nil {
		for i, row := range b.Sel {
			p := partitionFor(hb[i], up)
			d := offs[p]
			offs[p]++
			selOut[d] = row
			if hashOut != nil {
				hashOut[d] = hb[i]
			}
		}
	} else {
		for i := 0; i < n; i++ {
			p := partitionFor(hb[i], up)
			d := offs[p]
			offs[p]++
			selOut[d] = uint32(i)
			if hashOut != nil {
				hashOut[d] = hb[i]
			}
		}
	}

	if cap(sc.sels) < parts {
		sc.sels = make([][]uint32, parts)
		sc.hashes = make([][]uint64, parts)
	}
	sels := sc.sels[:parts]
	hashes := sc.hashes[:parts]
	for i := 0; i < parts; i++ {
		lo, hi := starts[i], starts[i+1]
		sels[i] = selOut[lo:hi:hi]
		if hashOut != nil {
			hashes[i] = hashOut[lo:hi:hi]
		} else {
			hashes[i] = nil
		}
	}
	sc.out.sels, sc.out.hashes, sc.out.plan, sc.out.buf = sels, hashes, plan, buf
	return &sc.out
}

// hashRowsByPlan fills out[i] with the key hash of the i'th ACTIVE row under
// the plan's hash function. Reports false when the plan does not match the
// batch's columns.
func hashRowsByPlan(p *routePlan, cols []*batch.Vector, b *batch.RecordBatch, sc *partitionScratch, out []uint64) bool {
	sel := b.Sel
	switch p.kind {
	case hashKindInt:
		if len(cols) != 1 {
			return false
		}
		v := cols[0]
		nulls := v.Nulls.HasNulls()
		if p.isI32 {
			if v.Int32Data == nil {
				return false
			}
			if sel != nil {
				for i, row := range sel {
					out[i] = routeHashInt32(v, nulls, int(row))
				}
			} else {
				for i := range out {
					out[i] = routeHashInt32(v, nulls, i)
				}
			}
		} else {
			if v.Int64Data == nil {
				return false
			}
			if sel != nil {
				for i, row := range sel {
					out[i] = routeHashInt64(v, nulls, int(row))
				}
			} else {
				for i := range out {
					out[i] = routeHashInt64(v, nulls, i)
				}
			}
		}
	case hashKindPacked:
		if len(cols) != len(p.layout) {
			return false
		}
		pc := sc.packed[:0]
		nulls := false
		for i, v := range cols {
			f := p.layout[i]
			c := packedKeyCol{nulls: &v.Nulls, word: f.word, shift: f.shift}
			if f.i32 {
				c.i32 = v.Int32Data
			} else {
				c.i64 = v.Int64Data
			}
			if c.i32 == nil && c.i64 == nil {
				return false
			}
			pc = append(pc, c)
			nulls = nulls || v.Nulls.HasNulls()
		}
		sc.packed = pc
		if sel != nil {
			for i, row := range sel {
				out[i] = routeHashPacked(pc, nulls, int(row))
			}
		} else {
			for i := range out {
				out[i] = routeHashPacked(pc, nulls, i)
			}
		}
	case hashKindStr:
		if len(cols) != 1 || cols[0].BytesData.Offsets == nil {
			return false
		}
		v := cols[0]
		nulls := v.Nulls.HasNulls()
		if sel != nil {
			for i, row := range sel {
				out[i] = routeHashStr(v, nulls, int(row))
			}
		} else {
			for i := range out {
				out[i] = routeHashStr(v, nulls, i)
			}
		}
	default:
		if sel != nil {
			for i, row := range sel {
				out[i] = legacyCompositeHash(cols, int(row))
			}
		} else {
			for i := range out {
				out[i] = legacyCompositeHash(cols, i)
			}
		}
	}
	return true
}

func routeHashInt32(v *batch.Vector, nulls bool, row int) uint64 {
	if nulls && v.Nulls.IsNullFast(row) {
		return routeNullHash
	}
	return fibHash(int64(v.Int32Data[row]))
}

func routeHashInt64(v *batch.Vector, nulls bool, row int) uint64 {
	if nulls && v.Nulls.IsNullFast(row) {
		return routeNullHash
	}
	return fibHash(v.Int64Data[row])
}

func routeHashStr(v *batch.Vector, nulls bool, row int) uint64 {
	if nulls && v.Nulls.IsNullFast(row) {
		return routeNullHash
	}
	return strHash(v.BytesData.Value(row))
}

func routeHashPacked(cols []packedKeyCol, nulls bool, row int) uint64 {
	if nulls && packedRowHasNull(cols, row) {
		return routeNullHash
	}
	lo, hi := packedKeyAt(cols, row)
	return packedHash(lo, hi)
}

// legacyCompositeHash is the routing hash for key shapes no sink table
// exposes (floats, bools, mixed multi-column, wide composites): the sink
// re-hashes its own serialized key, so this only has to be deterministic and
// well spread.
func legacyCompositeHash(cols []*batch.Vector, row int) uint64 {
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

func f64bits(f float64) uint64 {
	// NaN and ±0 canonicalization is unnecessary here: group-key equality
	// in the aggregate uses the same raw bit patterns.
	return math.Float64bits(f)
}

// fnv1aBytes routes string group keys to partition owners. Word-at-a-time
// (8 bytes per multiply, same shape as strHash but distinct constants so
// partition routing stays independent of the sink hash tables): the
// byte-at-a-time FNV loop was 5% of ClickBench Q34's profile on ~80-byte
// URLs, hashing every row serially in the partitioning stage. Only
// within-run consistency matters — every partitioner and sink in a query
// uses this same function — so the hash change is invisible outside.
func fnv1aBytes(b []byte) uint64 {
	h := uint64(0xcbf29ce484222325)
	for len(b) >= 8 {
		k := binary.LittleEndian.Uint64(b)
		h = (h ^ k) * 0x100000001b3
		b = b[8:]
	}
	for _, c := range b {
		h = (h ^ uint64(c)) * 0x100000001b3
	}
	h ^= h >> 29
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 32
	return h
}

// partitionAndDeliver splits b by group-key hash and delivers each
// partition's view to its owner. The caller's own partition is consumed
// inline; deliveries drain the caller's own queue while blocked so two
// workers sending to each other's full queues can never deadlock. Falls
// back to consuming the whole batch locally when partition selectors can't
// be built (unresolvable or unsupported group columns).
func partitionAndDeliver(ctx context.Context, hasher *HashAggregate, sink Sink, b *batch.RecordBatch, workerID int, queues []chan partitionItem, sc *partitionScratch) error {
	parts := len(queues)
	rt := hasher.PartitionSelectors(b, parts, sc)
	if rt == nil {
		partitionFallbacks.Add(1)
		if err := sink.Consume(ctx, b); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
		b.Release()
		return nil
	}
	sels, hashes, plan := rt.sels, rt.hashes, rt.plan
	if debugPartition {
		// A second scratch: the first call's slice headers live in sc and
		// would be overwritten by the re-run.
		again := hasher.PartitionSelectors(b, parts, &partitionScratch{})
		for j := range sels {
			if len(sels[j]) != len(again.sels[j]) {
				panic(fmt.Sprintf("partition nondeterminism: part %d %d vs %d rows", j, len(sels[j]), len(again.sels[j])))
			}
			for k := range sels[j] {
				if sels[j][k] != again.sels[j][k] {
					panic(fmt.Sprintf("partition nondeterminism: part %d row %d: %d vs %d", j, k, sels[j][k], again.sels[j][k]))
				}
			}
		}
	}

	// Pre-warm lazily-cached bitmap state single-threaded: HasNulls()
	// caches its answer on first call, and concurrent partition owners
	// would otherwise race on that write (same value, but a data race).
	// After this pass every owner's HasNulls() is a pure read.
	for _, col := range b.Columns {
		col.Nulls.HasNulls()
	}

	sh := &sharedBatch{b: b, buf: rt.buf}
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
		item := partitionItem{view: selView(b, sels[j]), src: sh, hashes: hashes[j], plan: plan}
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
		if err := consumeRouted(ctx, sink, selView(b, sels[workerID]), hashes[workerID], plan); err != nil {
			sh.release()
			return fmt.Errorf("sink consume: %w", err)
		}
	}
	sh.release()
	return nil
}

// consumeRouted hands a routed partition view to its owner, passing the
// pre-computed key hashes when the sink can use them. Every sink in
// partitioned mode is a *HashAggregate; the type assertion is what keeps the
// threading optional rather than a Sink-interface change.
func consumeRouted(ctx context.Context, sink Sink, view *batch.RecordBatch, hashes []uint64, plan *routePlan) error {
	if hashes != nil {
		if agg, ok := sink.(*HashAggregate); ok {
			return agg.ConsumeHashed(ctx, view, hashes, plan)
		}
	}
	return sink.Consume(ctx, view)
}

// consumePartitionItem consumes one delivered partition view and releases
// its shared-batch reference.
func consumePartitionItem(ctx context.Context, sink Sink, it partitionItem) error {
	err := consumeRouted(ctx, sink, it.view, it.hashes, it.plan)
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
