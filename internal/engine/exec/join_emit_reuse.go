package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// vectorReuse gates the per-operator reuse of vector backing on the hash-join
// emit path. Off, every output batch allocates its index slices and gathered
// columns fresh — the pre-2026-08-22 behavior, kept as the invariance-oracle
// arm and as the kill switch for the reuse ownership rule below.
//
// Why it exists: the 2026-08-22 SF100 window put the Go runtime heap lock
// (mheap.allocSpan + the unattributable contended-lock bucket) at 88% of ALL
// worker mutex delay, 81 s per worker per suite run, once the application-level
// stage-sink lock was removed. HashJoinProbe.emitViewOutput is its single
// largest caller at 19.4%, and every allocation it makes at join-emit widths
// is a large object (>32 KB) that takes that lock directly.
var vectorReuse = optswitch.Register("vector-reuse", "WADJET_VECTOR_REUSE",
	"per-operator reuse of hash-join emit vector backing (index slices, gathered build columns)")

// probeEmitBuf is one HashJoinProbe's reusable late-materialization output:
// the batch shell, its column slice, the probe/build index arrays the view
// columns address through, the composition buffers a view-over-view needs,
// and the owned storage of the eagerly-gathered build columns.
//
// # Ownership rule
//
// Everything here was written into the batch the probe returned on its LAST
// call and is written over on the next one. That is sound because a consumer
// that keeps a batch — or anything pointing into its column storage — past the
// call that handed it over must claim it with RecordBatch.Detach. Sort,
// Window, CollectSink, BatchSink, SpillableBatchCollector, SortMergeJoin, the
// hash-join build and partitioned aggregation's per-partition views all do;
// the consumers that do NOT copy what they need out before returning (the
// stage and shuffle sinks' bulk row append, HashAggregate's key arena). That
// is the same contract BatchPool recycling has always relied on — this buffer
// only drops the additional requirement that the driver call Release(), which
// the worker's fragment driver does not, which is why nothing was being reused
// on the distributed path at all.
//
// Detach records the claim on every column VECTOR, not just on the batch
// shell, because the batch a retaining consumer holds is not always the batch
// the probe emitted: ColumnPrune, the set-op emitter and partitioned
// aggregation mint a derived RecordBatch over the same *Vector pointers, and a
// view minted downstream over one of our gathered columns propagates the claim
// through Vector.Base. reusable() therefore tests the columns, not just the
// shell.
//
// The buffer is surrendered whole or not at all: the shell, the index arrays
// and the gather vectors are all reachable from a batch a consumer kept, so
// one claim drops everything and the next batch is freshly allocated.
type probeEmitBuf struct {
	out      *batch.RecordBatch // the shell handed downstream last call
	cols     []*batch.Vector    // its columns: views (re-minted) and gather storage (reused)
	composed [][]uint32         // per column, the index array a composed view adopted
	probeIdx []uint32           // shared probe-side view indices
	buildIdx []uint32           // shared build-side view indices
}

// reusable reports whether last call's output may be written over. mapping is
// the probe's cached column mapping, so its length and per-slot kind are
// constant for the operator's lifetime; the length check only guards a probe
// reused across schemas.
func (e *probeEmitBuf) reusable(mapping []outColSource) bool {
	if !vectorReuse.On() || e.out == nil || len(e.cols) != len(mapping) {
		return false
	}
	if e.out.Retained() {
		return false
	}
	for _, v := range e.cols {
		if v.Claimed() {
			return false
		}
	}
	return true
}

// reset surrenders the whole buffer — see the ownership rule.
func (e *probeEmitBuf) reset() { *e = probeEmitBuf{} }

// ensureU32 re-slices s to exactly n elements, reusing the backing array when
// it is large enough. No clearing: every element is written before it is read.
func ensureU32(s []uint32, n int) []uint32 {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]uint32, n)
}

// reusableGather reports whether v is owned flat storage matching col, i.e.
// safe to hand to Vector.ResetForWrite. Nested ARRAY/MAP/ROW columns build
// their element storage by appending and are never reused.
func reusableGather(v *batch.Vector, col parquet.Column) bool {
	if v == nil || v.Base != nil || v.Type != col.Type {
		return false
	}
	switch col.Type {
	case batch.TypeArray, batch.TypeMap, batch.TypeRow:
		return false
	case batch.TypeVector:
		return v.VectorDim == col.Dimension && col.Dimension > 0
	}
	return true
}
