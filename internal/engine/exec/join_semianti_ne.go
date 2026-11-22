// SPDX-License-Identifier: MIT

package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// For a semi/anti join with exactly one residual probe.col <> build.col, retain
// at most two distinct build values per key: two ensure a differing value for any
// non-NULL probe, one answers by v1 != probe. No row batches or arena are needed.
// Build NULL values are skipped; an all-NULL value key behaves as absent (EXISTS false).
// Probe NULL values make EXISTS false regardless of build: semi drops, anti emits.
// NULL probe keys also keep the semi-drop/anti-emit convention.
// See docs/design/semianti-distinct-pair.md.
// See docs/internals/semi-anti-distinct-pair-build.md for the design.

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

// neExactPair reports whether comparing the build value and the probe value
// as their integer storage is PostgreSQL's comparison of the two types, so
// that the distinct-pair state answers `probe <> build` exactly. That holds
// for int4 / int8 against int4 / int8 (PostgreSQL's cross-type integer
// operators are exact) and for a date, port or protocol against its own type.
// It does not hold for anything else the int reader accepts or the build
// keys: int8 against float8 compares in float8, where 2^60 and 2^60+3 are
// equal; a numeric or float is not an integer at all; a date against a
// timestamp compares the timestamp at midnight, not a day count against
// microseconds (#1681, line 1681c). Two distinct integers are also not
// "two distinct values" in such a comparison, so the pair cannot be made to
// answer those types; they keep the generic filtered build, whose residual
// is the engine's own comparison.
func neExactPair(build, probe batch.TypeID) bool {
	switch build {
	case batch.TypeInt32, batch.TypeInt64:
		return probe == batch.TypeInt32 || probe == batch.TypeInt64
	case batch.TypeDate, batch.TypePort, batch.TypeProtocol:
		return probe == build
	}
	return false
}

// neHintType resolves col in a plan-declared side schema; ok is false when
// the declaration is absent or does not name it once.
func neHintType(hint []parquet.Column, col string) (batch.TypeID, bool) {
	if len(hint) == 0 {
		return 0, false
	}
	i := columnIndexFallback(&batch.RecordBatch{Schema: hint}, col)
	if i < 0 {
		return 0, false
	}
	return hint[i].Type, true
}

// SemiAntiNEDeclaredTypes resolves the probe and build value columns of a
// recognized `<>` residual in the plan-declared side schemas, exactly as the
// distinct-pair enabling does (semiAntiNEEligible, neTryEnable). They are
// the plan's inputs to the choice between the distinct-pair build and the
// generic filtered one, so a build cache must not share a build between two
// joins that declare them differently. ok reports each resolution.
func SemiAntiNEDeclaredTypes(probeCol, buildCol string, probeHint, buildHint []parquet.Column) (probe batch.TypeID, probeOK bool, build batch.TypeID, buildOK bool) {
	probe, probeOK = neHintType(probeHint, probeCol)
	build, buildOK = neHintType(buildHint, buildCol)
	return probe, probeOK, build, buildOK
}

// semiAntiNEEligible reports whether the planner recognized this join's
// filter as a single `<>` condition (wiring sets both columns together)
// AND the plan declares both value columns with a pair of types the
// distinct-pair state compares exactly (neExactPair). Build-path dispatch
// uses it BEFORE the first batch arrives, so it reads the declarations,
// not the batches — neActive is the post-first-batch truth. An undeclared
// pair is not eligible: the optimization never enables where it cannot
// show the comparison is exact.
func (h *HashJoin) semiAntiNEEligible() bool {
	if (h.JoinType != SemiJoin && h.JoinType != AntiJoin) ||
		h.SemiAntiNEProbeCol == "" || h.SemiAntiNEBuildCol == "" {
		return false
	}
	pt, ok := neHintType(h.ProbeSchemaHint, h.SemiAntiNEProbeCol)
	if !ok {
		return false
	}
	bt, ok := neHintType(h.BuildSchemaHint, h.SemiAntiNEBuildCol)
	return ok && neExactPair(bt, pt)
}

// neTryEnable resolves the build-side value column on the first build
// batch and activates the distinct-pair path when both the key fast path
// and an integer value vector are available, and that vector's type pairs
// exactly with the probe value's declared type. Called under h.mu with
// buildSchema/tryEnableIntKey already settled. On failure the build
// falls through to the generic filtered path (correct, just slower).
func (h *HashJoin) neTryEnable(b *batch.RecordBatch) {
	if !h.semiAntiNEEligible() || !h.useIntKey {
		return
	}
	idx := columnIndexFallback(b, h.SemiAntiNEBuildCol)
	if idx < 0 {
		return
	}
	pt, _ := neHintType(h.ProbeSchemaHint, h.SemiAntiNEProbeCol)
	if !neExactPair(b.Columns[idx].Type, pt) {
		return
	}
	h.neValType = b.Columns[idx].Type
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
	// The NE build takes the flat path, so it has one index part.
	intIdx := h.parts[0].intTable(b.ActiveLen())
	intIdx.EnsureCapacity(b.ActiveLen())
	insert := func(row int) {
		key, ok := intKeyFromVector(keyCol, row)
		if !ok {
			return // NULL key never equals any probe key
		}
		val, ok := intKeyFromVector(valCol, row)
		if !ok {
			return // NULL value never satisfies `<>`
		}
		slot, existed := intIdx.GetOrInsertNoGrow(key, int32(len(h.nePairs)))
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
	intIdx.CheckGrow()
}

// probeNESemiAnti is the typed probe loop for the distinct-pair path.
// Appends emitted probe-row indices to sel and returns it. isSemi
// selects semi (emit on EXISTS) vs anti (emit on NOT EXISTS).
func (h *HashJoin) probeNESemiAnti(in *batch.RecordBatch, keyIdx, valIdx int, isSemi bool, sel []uint32) []uint32 {
	keyCol := in.Columns[keyIdx]
	valCol := in.Columns[valIdx]
	intIdx := h.parts[0].ints
	if intIdx == nil {
		if isSemi {
			return sel
		}
		// An anti join over an empty build emits every row.
		if in.Sel != nil {
			return append(sel, in.Sel...)
		}
		for i := 0; i < in.Len; i++ {
			sel = append(sel, uint32(i))
		}
		return sel
	}
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
