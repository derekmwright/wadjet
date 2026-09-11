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

// probeEmitBuf reuses the last output's shell, column slice, view indices,
// composition buffers and gathered storage on the next probe call.
// A consumer retaining a batch or any column storage must RecordBatch.Detach it;
// other consumers must copy what they need before returning. Driver Release is not required.
// Detach claims every vector, including vectors shared by derived batch shells,
// and propagates claims through Vector.Base. reusable must check columns and shell.
// Surrender the entire buffer on any claim: shell, indices and gathers remain reachable,
// so the next output must allocate fresh storage rather than reuse any part.
// See docs/internals/join-probe-output-buffer-ownership.md for the design.
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
