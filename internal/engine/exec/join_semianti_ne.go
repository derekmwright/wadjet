package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Distinct-pair build for `probe.col <> build.col` semi/anti joins
// (docs/design/semianti-distinct-pair.md).
//
// The decorrelated-EXISTS self-inequality class (Q21's "another supplier
// on the same order" legs) probes with exactly one residual condition:
// build value ≠ probe value. For that predicate, two distinct build-side
// values per key answer the EXISTS for EVERY probe value — if a key has
// ≥2 distinct values, at least one differs from any x; with exactly 1,
// the answer is v1 ≠ x. So the build collapses from a row-storing hash
// table (batches + arena + per-candidate filter closure walks) to
// key → (v1, v2, n≤2): no batch storage, no arena, probe = one lookup
// plus at most two integer compares.
//
// NULL semantics (SQL three-valued, matching the closure path):
//   - build rows with NULL value can never satisfy `<>` — skipped at
//     insert (a key whose every value is NULL behaves as absent: EXISTS
//     is false).
//   - probe rows with NULL value make every comparison UNKNOWN — EXISTS
//     is false regardless of build content: semi drops, anti emits.
//   - NULL probe keys keep the existing convention (semi drops, anti
//     emits).

// nePair is the per-key distinct-value state. n is 1 or 2; v2 is valid
// only when n == 2. 24 bytes/key (padded) — a 6M-key Q21 leg is ~150 MB
// of pairs+table vs multi-GB stored batches on the generic path.
type nePair struct {
	v1, v2 int64
	n      uint32
	_      uint32
}

// NEActive reports whether the distinct-pair build engaged (post-Build).
// Callers use it for engagement logging — the A/B marker for this path.
func (h *HashJoin) NEActive() bool { return h.neActive }

// semiAntiNEEligible reports whether the planner recognized this join's
// filter as a single `<>` condition (wiring sets both columns together).
// Build-path dispatch uses it BEFORE the first batch arrives, so it must
// not depend on runtime resolution — neActive is the post-first-batch
// truth.
func (h *HashJoin) semiAntiNEEligible() bool {
	return (h.JoinType == SemiJoin || h.JoinType == AntiJoin) &&
		h.SemiAntiNEProbeCol != "" && h.SemiAntiNEBuildCol != ""
}

// neTryEnable resolves the build-side value column on the first build
// batch and activates the distinct-pair path when both the key fast path
// and an integer value vector are available. Called under h.mu with
// buildSchema/tryEnableIntKey already settled. On failure the build
// falls through to the generic filtered path (correct, just slower) —
// the planner's catalog type gate makes that a defensive branch, not an
// expected one.
func (h *HashJoin) neTryEnable(b *batch.RecordBatch) {
	if !h.semiAntiNEEligible() || !h.useIntKey {
		return
	}
	idx := columnIndexFallback(b, h.SemiAntiNEBuildCol)
	if idx < 0 {
		return
	}
	switch b.Columns[idx].Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		h.neValInt32 = true
	case batch.TypeInt64:
		h.neValInt32 = false
	default:
		return
	}
	h.neValIdx = idx
	h.neActive = true
	if h.BuildRowHint > 0 {
		h.nePairs = make([]nePair, 0, h.BuildRowHint/4)
	}
}

// insertNEBatch folds one build batch into the distinct-pair table.
// Called under h.mu (serial flat build path).
func (h *HashJoin) insertNEBatch(b *batch.RecordBatch) {
	keyCol := b.Columns[h.buildKeyIdx[0]]
	valCol := b.Columns[h.neValIdx]
	h.intIndex.EnsureCapacity(b.ActiveLen())
	insert := func(row int) {
		key, ok := intKeyFromVector(keyCol, row)
		if !ok {
			return // NULL key never equals any probe key
		}
		val, ok := intKeyFromVector(valCol, row)
		if !ok {
			return // NULL value never satisfies `<>`
		}
		slot, existed := h.intIndex.GetOrInsertNoGrow(key, int32(len(h.nePairs)))
		if !existed {
			h.nePairs = append(h.nePairs, nePair{v1: val, n: 1})
			return
		}
		p := &h.nePairs[slot]
		if p.n == 1 && p.v1 != val {
			p.v2 = val
			p.n = 2
		}
	}
	if b.Sel != nil {
		for _, si := range b.Sel {
			insert(int(si))
		}
	} else {
		for i := 0; i < b.Len; i++ {
			insert(i)
		}
	}
	h.intIndex.CheckGrow()
}

// probeNESemiAnti is the typed probe loop for the distinct-pair path.
// Appends emitted probe-row indices to sel and returns it. isSemi
// selects semi (emit on EXISTS) vs anti (emit on NOT EXISTS).
func (h *HashJoin) probeNESemiAnti(in *batch.RecordBatch, keyIdx, valIdx int, isSemi bool, sel []uint32) []uint32 {
	keyCol := in.Columns[keyIdx]
	valCol := in.Columns[valIdx]
	intIdx := h.intIndex
	hasBloom := h.bloom != nil
	check := func(row int) {
		key, ok := intKeyFromVector(keyCol, row)
		if !ok {
			if !isSemi {
				sel = append(sel, uint32(row))
			}
			return
		}
		x, ok := intKeyFromVector(valCol, row)
		if !ok {
			// NULL probe value: every comparison UNKNOWN → EXISTS false.
			if !isSemi {
				sel = append(sel, uint32(row))
			}
			return
		}
		if hasBloom && !h.bloomMayContain(bloomHashInt(key)) {
			if !isSemi {
				sel = append(sel, uint32(row))
			}
			return
		}
		slot, found := intIdx.Get(key)
		var exists bool
		if found {
			p := &h.nePairs[slot]
			exists = p.n >= 2 || p.v1 != x
		}
		if (isSemi && exists) || (!isSemi && !exists) {
			sel = append(sel, uint32(row))
		}
	}
	if in.Sel != nil {
		for _, idx := range in.Sel {
			check(int(idx))
		}
	} else {
		for i := 0; i < in.Len; i++ {
			check(i)
		}
	}
	return sel
}
