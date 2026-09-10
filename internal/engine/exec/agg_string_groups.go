// This file holds string, compact, and generic group-table consumption.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// ensureGroupIndexBuf returns a []int32 of at least length n, reusing the buffer.
func (h *HashAggregate) ensureGroupIndexBuf(n int) []int32 {
	if cap(h.groupIndexBuf) < n {
		h.groupIndexBuf = make([]int32, n)
	}
	return h.groupIndexBuf[:n]
}

// consumeBatchCompactGroup is the fast path for multi-column GROUP BY where
// the binary-encoded key fits in int64. Uses intHashTable for group lookup
// with SoA flat accumulator scatter (two-phase like consumeBatchIntGroup).
// Phase 1: Hash lookup builds group index array.
// Phase 2: Per-aggregate typed scatter update (one pass per agg, no per-row dispatch).
// Falls back to generic path if any key exceeds 8 bytes.
func (h *HashAggregate) consumeBatchCompactGroup(b *batch.RecordBatch) {
	intIdx := h.intGroupIndex

	// Pre-reserve key/state capacity. The flat accumulators grow once, after
	// the lookup loop, to the final group count.
	batchRows := b.ActiveLen()
	h.intGroupStates = ensureAppendCap(h.intGroupStates, batchRows)
	h.compactKeys = ensureAppendCap(h.compactKeys, batchRows)
	h.keys = ensureAppendCap(h.keys, batchRows)

	// Phase 1: Encode keys, hash lookup, build group index array.
	var gi []int32
	var sel []uint32
	var iterLen int

	encodeKey := func(row int) bool {
		h.keyBuf = h.keyBuf[:0]
		for ci, idx := range h.groupColIdx {
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			h.keyBuf = append(h.keyBuf, 0)
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[ci])
		}
		return len(h.keyBuf) <= 8
	}

	newGroup := func(row int, key int64) {
		intIdx.CheckGrow()
		keyVals := make([]any, len(h.GroupByCols))
		for ki, idx := range h.groupColIdx {
			if idx >= 0 {
				keyVals[ki] = b.Columns[idx].GetValue(row)
			}
		}
		gs := h.gsPool.alloc()
		gs.ensureExtras().keyValues = keyVals
		h.intGroupStates = append(h.intGroupStates, gs)
		h.numIntGroups++
		h.compactKeys = append(h.compactKeys, string(h.keyBuf))
		h.compactKeyBytes += int64(len(h.keyBuf))
		h.keys = append(h.keys, keyVals)
	}

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if !encodeKey(row) {
				h.compactFallback(b, gi, sel, si, iterLen)
				return
			}
			key := packKeyInt64(h.keyBuf)
			newIdx := int32(h.numIntGroups)
			gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
			if ok {
				gi[si] = gsIdx
			} else {
				newGroup(row, key)
				gi[si] = newIdx
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			if !encodeKey(row) {
				h.compactFallback(b, gi, nil, row, iterLen)
				return
			}
			key := packKeyInt64(h.keyBuf)
			newIdx := int32(h.numIntGroups)
			gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
			if ok {
				gi[row] = gsIdx
			} else {
				newGroup(row, key)
				gi[row] = newIdx
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	h.scatterBatchAggs(h.numIntGroups, b, gi, sel, iterLen)
}

// compactFallback handles the case where a compact key exceeds 8 bytes.
// Scatters the already-indexed rows via Phase 2, materializes SoA accumulators,
// migrates to generic path, and processes remaining rows per-row.
func (h *HashAggregate) compactFallback(b *batch.RecordBatch, gi []int32, sel []uint32, fallbackAt int, totalRows int) {
	// Phase 2 for rows already indexed.
	if fallbackAt > 0 {
		truncSel := sel
		if truncSel != nil {
			truncSel = truncSel[:fallbackAt]
		}
		h.scatterBatchAggs(h.numIntGroups, b, gi[:fallbackAt], truncSel, fallbackAt)
	}

	// Materialize SoA → AoS and migrate to generic.
	h.materializeFlatAccums()
	h.migrateCompactToGeneric()

	// Process remaining rows (including the fallback row) generically.
	if sel != nil {
		for j := fallbackAt; j < totalRows; j++ {
			h.processRow(b, int(sel[j]))
		}
	} else {
		for j := fallbackAt; j < totalRows; j++ {
			h.processRow(b, j)
		}
	}
}

// arenaString builds a string header over hash-table arena bytes without
// copying them. Sound ONLY for bytes handed out by strHashTable's chunked key
// arena: those chunks are append-only, are never reallocated, and are never
// written again once a key lands in them (see str_hash.go), so the bytes
// behind the header can never change. The GC keeps the chunk alive through the
// header's interior pointer, so the string also outlives the table itself —
// which is what the emit, spill, drain-cursor and merge readers of
// serializedKeys rely on after Next() drops strGroupIndex.
func arenaString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// consumeBatchStrGroup is the fast path for single-column string GROUP BY.
// Uses strHashTable for group lookup with SoA flat accumulator scatter.
// Two-phase approach matches consumeBatchIntGroup: hash lookup then typed scatter.
//
// Group keys are stored ONCE, in the hash table's key arena: serializedKeys
// entries alias those bytes (arenaString) instead of holding a private copy.
// Their bytes are therefore accounted by strGroupIndex.MemoryUsage(), not by
// h.serializedKeyBytes — bumping the counter here would double-charge them.
func (h *HashAggregate) consumeBatchStrGroup(b *batch.RecordBatch) {
	gkVec := b.Columns[h.strGroupKeyCol]
	hasNulls := gkVec.Nulls.HasNulls()
	strIdx := h.strGroupIndex

	// Hash once: the router already ran strHash over these bytes to pick the
	// owner — the expensive half of the probe on ClickBench Q34's ~88-byte
	// URLs. Reusing it also supplies strEntry.hashTag, which is its low half.
	var ph []uint64
	if p := h.provPlan; p != nil && p.kind == hashKindStr && len(h.provHashes) == b.ActiveLen() {
		ph = h.provHashes
		HashOnceRoutedRows.Add(int64(len(ph)))
	}

	// Pre-reserve key/state capacity (same rationale as consumeBatchIntGroup);
	// the flat accumulators grow once after the lookup loop.
	batchRows := b.ActiveLen()
	h.strGroupStates = ensureAppendCap(h.strGroupStates, batchRows)
	h.serializedKeys = ensureAppendCap(h.serializedKeys, batchRows)

	// Phase 1: Hash lookup — build group index array. NULL keys get their
	// own first-class group slot (strNullGroupIdx) so the typed scatter in
	// Phase 2 updates its flat accumulators like any other group. The
	// previous shape diverted null-key rows to processRow, which appends a
	// groupState WITHOUT a flat-accumulator slot — strGroupStates and
	// intFlatAccs went out of alignment and the NULL group emitted zeroed
	// aggregates (and a 1-byte "\x01" string key could collide with the
	// binary null sentinel in the shared hash table).
	var gi []int32
	var sel []uint32
	var iterLen int

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRefAt(key, ph[si], int32(len(h.strGroupStates)))
				if found {
					gi[si] = gsIdx
				} else {
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[si] = gsIdx
				}
			}
		} else {
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRef(key, int32(len(h.strGroupStates)))
				if found {
					gi[si] = gsIdx
				} else {
					// Deferred state: the serializedKeys entry IS the key, and
					// it aliases the table's arena copy rather than making a
					// second one (arenaString). Simple aggs live in the flat
					// SoA arrays. The per-group groupState + []any box +
					// h.keys slot were ~100B of overhead per group (Q34's 18M
					// URL groups).
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[si] = gsIdx
				}
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
			for row := 0; row < iterLen; row++ {
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[row] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRefAt(key, ph[row], int32(len(h.strGroupStates)))
				if found {
					gi[row] = gsIdx
				} else {
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[row] = gsIdx
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[row] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRef(key, int32(len(h.strGroupStates)))
				if found {
					gi[row] = gsIdx
				} else {
					// Deferred state: the serializedKeys entry IS the key, and
					// it aliases the table's arena copy rather than making a
					// second one (arenaString). Simple aggs live in the flat
					// SoA arrays. The per-group groupState + []any box +
					// h.keys slot were ~100B of overhead per group (Q34's 18M
					// URL groups).
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[row] = gsIdx
				}
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	h.scatterBatchAggs(len(h.strGroupStates), b, gi, sel, iterLen)

}

// strNullGroupSlot returns the NULL-key group's flat-accumulator slot for
// the single-string fast path, creating it on first use. The group lives in
// strGroupStates with an aligned slot in every flat accumulator, but is
// deliberately NOT inserted into strGroupIndex — a raw 1-byte string key
// could otherwise collide with any in-band null sentinel. serializedKeys
// gets the generic binary form (single 0x01 null flag) so spill/merge
// round-trips distinguish the NULL group from every real string.
func (h *HashAggregate) strNullGroupSlot() int32 {
	if h.strNullGroupIdx >= 0 {
		return h.strNullGroupIdx
	}
	gs := h.gsPool.alloc()
	gs.ensureExtras().keyValues = []any{nil}
	h.strNullGroupIdx = int32(len(h.strGroupStates))
	h.strGroupStates = append(h.strGroupStates, gs)
	h.keys = append(h.keys, []any{nil})
	h.serializedKeys = append(h.serializedKeys, "\x01")
	h.serializedKeyBytes++
	h.appendFlatAccumSlot()
	return h.strNullGroupIdx
}

// mergeNullGroupSlot resolves the destination slot for another sink's
// NULL-key group during the in-memory merge, returning (slot, found).
//
// The single-string fast path keeps its NULL group OUT of strGroupIndex on
// purpose (strNullGroupSlot: that table stores raw string keys, so a real
// one-byte "\x01" value would land on the sentinel), which means the merge
// loop's table probe can never match it. Every merge therefore appended one
// more NULL group and GROUP BY over a nullable string reported the NULL row
// once per parallel partial instead of once — the split-group half of #338.
// GROUP BY treats all NULLs as one group, so the match has to run through
// strNullGroupIdx here.
func (h *HashAggregate) mergeNullGroupSlot(key string, newIdx int32) (int32, bool) {
	if h.strNullGroupIdx >= 0 {
		return h.strNullGroupIdx, true
	}
	// A generic-path destination carries its NULL key in the table like any
	// other key (the binary encoding of a single NULL column IS "\x01"), so
	// there the ordinary probe is the correct match.
	if !h.useStrGroupKey {
		return h.strGroupIndex.GetOrInsert([]byte(key), newIdx)
	}
	// String-path destination with no NULL group yet: the incoming one
	// becomes it, so the next merge matches instead of appending again.
	h.strNullGroupIdx = newIdx
	return newIdx, false
}

// consumeBatchGenericSoA is the SoA fast path for multi-column GROUP BY
// that doesn't fit int/packed/compact/single-string paths.
// Uses binary key serialization into strHashTable with two-phase scatter.
// Phase 1: Serialize keys, hash lookup, build group index array.
// Phase 2: Per-aggregate typed scatter update (one pass per agg, no per-row dispatch).
func (h *HashAggregate) consumeBatchGenericSoA(b *batch.RecordBatch) {
	strIdx := h.strGroupIndex

	batchRows := b.ActiveLen()
	h.strGroupStates = ensureAppendCap(h.strGroupStates, batchRows)
	h.serializedKeys = ensureAppendCap(h.serializedKeys, batchRows)
	if !h.deferGenericKeyBoxing {
		h.keys = ensureAppendCap(h.keys, batchRows)
	}

	// Phase 1: Serialize keys, hash lookup, build group index array.
	var gi []int32
	var sel []uint32
	var iterLen int

	serializeKey := func(row int) {
		h.keyBuf = h.keyBuf[:0]
		for ci, idx := range h.groupColIdx {
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			h.keyBuf = append(h.keyBuf, 0)
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[ci])
		}
	}

	newGroup := func(row int) {
		gs := h.gsPool.alloc()
		if !h.deferGenericKeyBoxing {
			keyVals := make([]any, len(h.GroupByCols))
			for ki, idx := range h.groupColIdx {
				if idx >= 0 {
					keyVals[ki] = b.Columns[idx].GetValue(row)
				}
			}
			gs.ensureExtras().keyValues = keyVals
			h.keys = append(h.keys, keyVals)
		}
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
		h.serializedKeyBytes += int64(len(h.keyBuf))
	}

	if h.deferGenericKeyBoxing {
		// Typed lookup: hash and chain-verify straight off the typed
		// column storage; serialize only on insert. strGroupIndex is not
		// maintained here — serializedKeys + genKeyIdx/genKeyNext carry
		// the identity (see ensureStrGroupIndexForMerge for the one
		// consumer that still needs the string table).
		h.keySerCols = buildKeySerCols(h.keySerCols, b, h.groupColIdx, h.groupColTypes)
		kcols := h.keySerCols
		if h.genKeyIdx == nil {
			h.genKeyIdx = newIntHashTable(4096)
		}
		h.genKeyNext = ensureAppendCap(h.genKeyNext, batchRows)

		newTypedGroup := func(row int, chainHead int32) int32 {
			newIdx := int32(len(h.strGroupStates))
			// nil state — simple aggs live entirely in the flat SoA
			// arrays + serializedKeys; materializeFlatAccums reifies on
			// the migration/merge cold paths (same deferral as packed keys:
			// 32B x groups of pure overhead otherwise).
			h.strGroupStates = append(h.strGroupStates, nil)
			h.keyBuf = serializeGroupKey(h.keyBuf[:0], kcols, row)
			h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
			h.serializedKeyBytes += int64(len(h.keyBuf))
			h.genKeyNext = append(h.genKeyNext, chainHead)
			return newIdx
		}
		lookup := func(row int) int32 {
			ck := int64(typedRowHash(kcols, row))
			if head, ok := h.genKeyIdx.Get(ck); ok {
				for g := head; g >= 0; g = h.genKeyNext[g] {
					if serializedKeyMatchesRow(h.serializedKeys[g], kcols, row) {
						return g
					}
				}
				newIdx := newTypedGroup(row, head)
				h.genKeyIdx.Put(ck, newIdx)
				return newIdx
			}
			newIdx := newTypedGroup(row, -1)
			h.genKeyIdx.Put(ck, newIdx)
			return newIdx
		}

		if b.Sel != nil {
			iterLen = len(b.Sel)
			sel = b.Sel
			gi = h.ensureGroupIndexBuf(iterLen)
			for si, selIdx := range b.Sel {
				gi[si] = lookup(int(selIdx))
			}
		} else {
			iterLen = b.Len
			gi = h.ensureGroupIndexBuf(iterLen)
			for row := 0; row < iterLen; row++ {
				gi[row] = lookup(row)
			}
		}
	} else if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			serializeKey(row)
			gsIdx, found := strIdx.GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
			if found {
				gi[si] = gsIdx
			} else {
				newGroup(row)
				gi[si] = gsIdx
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			serializeKey(row)
			gsIdx, found := strIdx.GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
			if found {
				gi[row] = gsIdx
			} else {
				newGroup(row)
				gi[row] = gsIdx
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	h.scatterBatchAggs(len(h.strGroupStates), b, gi, sel, iterLen)
}

// ensureStrGroupIndexForMerge rebuilds the string hash table from
// serializedKeys when the typed generic path (which doesn't maintain it)
// produced the groups. Cold path: only the slow in-memory merge fallback
// needs the string table.
func (h *HashAggregate) ensureStrGroupIndexForMerge() {
	if h.useGenericSoA && h.deferGenericKeyBoxing {
		idx := newStrHashTable(len(h.strGroupStates) + 16)
		for i, k := range h.serializedKeys {
			idx.Put([]byte(k), int32(i))
		}
		h.strGroupIndex = idx
		return
	}
	// A sink that never resolved a key path has no table at all (Init no
	// longer pre-builds one so resolveIndices' NDV pre-size can take
	// effect); the dedup loop that follows probes it unconditionally.
	if h.strGroupIndex == nil {
		h.strGroupIndex = newStrHashTable(len(h.strGroupStates) + 16)
	}
}

// migrateCompactToGeneric moves all groups from intHashTable to the string map
// when compact mode cannot handle a key that exceeds 8 bytes.
func (h *HashAggregate) migrateCompactToGeneric() {
	h.useCompactGroupKey = false
	h.strGroupIndex = newStrHashTable(h.numIntGroups)
	h.strGroupStates = make([]*groupState, 0, h.numIntGroups)
	for i, gs := range h.intGroupStates {
		key := h.compactKeys[i]
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
		h.serializedKeyBytes += int64(len(key))
	}
	h.intGroupStates = nil
	h.numIntGroups = 0
	h.intGroupIndex = nil
	h.compactKeys = nil
	h.compactKeyBytes = 0
}

// packKeyInt64 interprets up to 8 bytes as a little-endian int64.
func packKeyInt64(b []byte) int64 {
	var v int64
	for i := 0; i < len(b); i++ {
		v |= int64(b[i]) << uint(i*8)
	}
	return v
}

// strIndexForRow returns the string group index used by the generic per-row
// paths, creating it on first use. Those paths are reached without going
// through resolveIndices' pre-sized branches (spill replay, grouping sets on
// a sink that never resolved a fast path), so they own the fallback
// construction now that Init no longer hands out a fixed 4096-slot table.
func (h *HashAggregate) strIndexForRow() *strHashTable {
	if h.strGroupIndex == nil {
		h.strGroupIndex = newStrHashTable(4096)
	}
	return h.strGroupIndex
}
