// This file holds integer and packed group-table consumption paths.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

func (h *HashAggregate) consumeBatch(b *batch.RecordBatch) {
	// Terminal defense for the #277 panic family: a zero-column batch can
	// reach here through paths the Consume-entry guard cannot cover (the
	// flushSpilledOps drain feeds Consume without gates). Pure
	// COUNT(*)-style aggregates (every aggColIdx < 0) legitimately consume
	// schemaless batches and proceed. Otherwise skipping is no worse than
	// the panic it replaces. NOTE: the 2026-08-02 SF100 Q18 join-8 stacks
	// that were first read as "Columns emptied between this guard and the
	// updater loop" were actually #279 — a nil batchUpdaters scratch after
	// adoptStateFrom, one line below the b.Columns index on the same
	// source line. The batch was never mutated; no such race exists.
	if len(b.Columns) == 0 {
		for _, ci := range h.aggColIdx {
			if ci >= 0 {
				return
			}
		}
		if !h.isScalarAgg {
			return // grouped: keys unavailable without columns
		}
	}
	// Scalar aggregate fast path: use batch-level kernels (no per-row dispatch)
	if h.isScalarAgg {
		for i := range h.Aggs {
			idx := h.aggColIdx[i]
			var vec *batch.Vector
			if idx >= 0 {
				vec = b.Columns[idx]
			}
			h.batchAggKernels[i](&h.scalarAccs[i], vec, b.Sel, b.Len)
		}
		return
	}

	// Select no-null-check updaters for columns without nulls in this batch.
	// Applies to all grouped paths: int, compact, and generic.
	for i := 0; i < len(h.Aggs); i++ {
		ci := h.aggColIdx[i]
		if ci >= 0 && h.aggUpdatersNoNull[i] != nil && !b.Columns[ci].Nulls.HasNulls() {
			h.batchUpdaters[i] = h.aggUpdatersNoNull[i]
		} else {
			h.batchUpdaters[i] = h.aggUpdaters[i]
		}
	}

	// Rebuild flat accumulators if they were cleared by materializeFlatAccums
	// (e.g. after parallel merge). This happens when flushSpilledOps replays
	// spilled batches through Consume after the merge phase.
	if h.intFlatAccs == nil && (h.useIntGroupKey || h.usePackedGroupKey || h.useCompactGroupKey || h.useStrGroupKey || h.useGenericSoA) {
		h.rebuildFlatAccums(b)
	}

	// Single-column integer GROUP BY fast path
	// NULL group keys cannot live in the int hash tables. The int paths
	// used to divert null-key rows into strGroupStates via processRow — a
	// second store that Next(), the SoA merges, and the migrations all
	// ignored, so the NULL group silently vanished from results (GROUP BY
	// over a nullable int column dropped its NULL row; DISTINCT would drop
	// NULLs). On the first batch whose key column actually contains nulls,
	// migrate to the generic path once and stay there — null keys are rare,
	// and a single store is the only shape every reader gets right.
	if h.useIntGroupKey && b.Columns[h.intGroupKeyCol].Nulls.HasNulls() {
		h.migrateToGenericMap()
	} else if h.usePackedGroupKey {
		// Same rule for every packed key column: a NULL in ANY of them
		// migrates the whole aggregate to the generic path BEFORE this batch
		// is consumed. This is what makes an in-table NULL impossible, which
		// in turn is what lets the packing be total — no bit pattern has to
		// be reserved to mean NULL, so no real value can be mistaken for one.
		for _, ci := range h.groupColIdx {
			if b.Columns[ci].Nulls.HasNulls() {
				h.migrateToGenericMap()
				break
			}
		}
	}

	if h.useIntGroupKey {
		h.consumeBatchIntGroup(b)
		return
	}

	// Packed composite-key GROUP BY fast path
	if h.usePackedGroupKey {
		h.consumeBatchPackedGroup(b)
		return
	}

	// Multi-column compact GROUP BY fast path
	if h.useCompactGroupKey {
		h.consumeBatchCompactGroup(b)
		return
	}

	// Single-column string GROUP BY fast path
	if h.useStrGroupKey {
		h.consumeBatchStrGroup(b)
		return
	}

	// Multi-column generic GROUP BY SoA fast path
	if h.useGenericSoA {
		h.consumeBatchGenericSoA(b)
		return
	}

	// Grouping sets single-pass: insert each row once per set
	if len(h.GroupingSets) > 0 {
		if b.Sel != nil {
			for _, idx := range b.Sel {
				h.processRowGroupingSets(b, int(idx))
			}
		} else {
			for i := 0; i < b.Len; i++ {
				h.processRowGroupingSets(b, i)
			}
		}
		return
	}

	if b.Sel != nil {
		for _, idx := range b.Sel {
			h.processRow(b, int(idx))
		}
	} else {
		for i := 0; i < b.Len; i++ {
			h.processRow(b, i)
		}
	}
}

// consumeBatchIntGroup is the fast path for single-column integer GROUP BY.
// Uses intHashTable for group lookup — no key serialization, no string allocation.
//
// Two-phase SoA (Struct of Arrays) approach:
//
//	Phase 1: Hash lookup — compute group indices for all rows in the batch.
//	Phase 2: Per-aggregate typed scatter update using flat accumulator arrays.
//
// This eliminates per-row function pointer overhead (indirect calls can't inline),
// removes the inner nAggs loop per row, and stores accumulators in contiguous arrays
// instead of scattered per-group heap objects (~16MB vs ~192MB working set for 2M groups).
func (h *HashAggregate) consumeBatchIntGroup(b *batch.RecordBatch) {
	gkVec := b.Columns[h.intGroupKeyCol]
	isInt32 := h.groupColTypes[0] == batch.TypeInt32 ||
		h.groupColTypes[0] == batch.TypePort ||
		h.groupColTypes[0] == batch.TypeProtocol ||
		h.groupColTypes[0] == batch.TypeDate
	hasNulls := gkVec.Nulls.HasNulls()

	intIdx := h.intGroupIndex
	// Live-entry count before this batch: the conversion decision at the
	// bottom needs the batch's new-group count, and the free-list path
	// recycles slots without moving numIntGroups.
	liveBefore := 0
	if intIdx != nil {
		liveBefore = intIdx.Len()
	}

	// Hash once: the partition router already computed fibHash over this
	// column for every routed row. Accept its array only when the plan names
	// the same key extraction this loop performs. The routed and self-hashing
	// loops below are written out separately so this test never enters the
	// per-row path — see the note in consumeBatchPackedGroup for the cost.
	var ph []uint64
	if p := h.provPlan; p != nil && p.kind == hashKindInt && p.isI32 == isInt32 &&
		len(h.provHashes) == b.ActiveLen() {
		ph = h.provHashes
		HashOnceRoutedRows.Add(int64(len(ph)))
	}

	// Pre-reserve key capacity so per-group appends in the hash lookup loop
	// don't trigger growslice reallocations. The batch size is an upper
	// bound on new groups this batch can create. The flat accumulators are
	// NOT touched here — they are grown once, after the loop, to the final
	// group count (scatterBatchAggs below).
	batchRows := b.ActiveLen()
	h.intKeys = ensureAppendCap(h.intKeys, batchRows)

	// Phase 1: Hash lookup — build group index array.
	// gi[i] maps iteration index i to its group state index, or -1 for null keys.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	if tl := h.intTwoLevel; tl != nil {
		gi, sel, iterLen, hasNullKeys = h.intGroupPhase1TwoLevel(b, tl, gkVec, isInt32, hasNulls, ph)
	} else if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				var key int64
				if isInt32 {
					key = int64(gkVec.Int32Data[row])
				} else {
					key = gkVec.Int64Data[row]
				}
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrowAt(key, ph[si], newIdx)
				if ok {
					gi[si] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[si] = newIdx
				}
			}
		} else {
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				var key int64
				if isInt32 {
					key = int64(gkVec.Int32Data[row])
				} else {
					key = gkVec.Int64Data[row]
				}
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
				if ok {
					gi[si] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						// No per-group state at all: the key lives in the intKeys
						// SoA and the accumulators in the flat arrays, which are
						// grown to numIntGroups once this loop finishes.
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[si] = newIdx
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
					gi[row] = -1
					hasNullKeys = true
					continue
				}
				var key int64
				if isInt32 {
					key = int64(gkVec.Int32Data[row])
				} else {
					key = gkVec.Int64Data[row]
				}
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrowAt(key, ph[row], newIdx)
				if ok {
					gi[row] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[row] = newIdx
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[row] = -1
					hasNullKeys = true
					continue
				}
				var key int64
				if isInt32 {
					key = int64(gkVec.Int32Data[row])
				} else {
					key = gkVec.Int64Data[row]
				}
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
				if ok {
					gi[row] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						// No per-group state at all: the key lives in the intKeys
						// SoA and the accumulators in the flat arrays, which are
						// grown to numIntGroups once this loop finishes.
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[row] = newIdx
				}
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	// One pass per aggregate with inlined typed arithmetic (no function pointers).
	h.scatterBatchAggs(h.numIntGroups, b, gi, sel, iterLen)

	// Handle null-key rows via generic path (rare: only when GROUP BY key is nullable).
	if hasNullKeys {
		if sel != nil {
			for si, selIdx := range sel {
				if gi[si] < 0 {
					h.processRow(b, int(selIdx))
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if gi[row] < 0 {
					h.processRow(b, row)
				}
			}
		}
	}

	// Bucketed conversion is decided ONCE per batch, here at the end: the
	// loops above hoisted their table pointer, and the decision needs this
	// batch's new-group count (two_level_hash.go).
	if intIdx != nil {
		h.maybeConvertIntIndex(intIdx.Len() - liveBefore)
	}
}

// intGroupPhase1TwoLevel is consumeBatchIntGroup's phase 1 against the
// BUCKETED index (two_level_hash.go). Everything else about the batch —
// key extraction, free-list reuse, the intKeys SoA append, phase 2's
// scatter, the null-key fallback — is identical; only the table the probe
// lands in differs, and group ids stay dense globals.
//
// Written out rather than branching inside the flat loops for the reason
// recorded in consumeBatchPackedGroup: a per-row test on a hoistable
// condition measured +4% on this class of loop. The routed and self-hashing
// variants are likewise separate; the two-level probe needs the hash as a
// value (it selects the bucket), so the self-hashing variant computes
// fibHash once and hands it to the same entry point.
func (h *HashAggregate) intGroupPhase1TwoLevel(b *batch.RecordBatch, tl *intTwoLevelTable,
	gkVec *batch.Vector, isInt32, hasNulls bool, ph []uint64) (gi []int32, sel []uint32, iterLen int, hasNullKeys bool) {
	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				var key int64
				if isInt32 {
					key = int64(gkVec.Int32Data[row])
				} else {
					key = gkVec.Int64Data[row]
				}
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := tl.GetOrInsertAt(key, ph[si], newIdx)
				if ok {
					gi[si] = gsIdx
				} else {
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[si] = newIdx
				}
			}
			return gi, sel, iterLen, hasNullKeys
		}
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				gi[si] = -1
				hasNullKeys = true
				continue
			}
			var key int64
			if isInt32 {
				key = int64(gkVec.Int32Data[row])
			} else {
				key = gkVec.Int64Data[row]
			}
			var newIdx int32
			fromFree := false
			if nf := len(h.freeGroupIDs); nf > 0 {
				newIdx = h.freeGroupIDs[nf-1]
				fromFree = true
			} else {
				newIdx = int32(h.numIntGroups)
			}
			gsIdx, ok := tl.GetOrInsertAt(key, fibHash(key), newIdx)
			if ok {
				gi[si] = gsIdx
			} else {
				if fromFree {
					h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
					h.intKeys[newIdx] = key
				} else {
					h.numIntGroups++
					h.intKeys = append(h.intKeys, key)
				}
				gi[si] = newIdx
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}

	iterLen = b.Len
	gi = h.ensureGroupIndexBuf(iterLen)
	if ph != nil {
		ph = ph[:iterLen]
		for row := 0; row < iterLen; row++ {
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				gi[row] = -1
				hasNullKeys = true
				continue
			}
			var key int64
			if isInt32 {
				key = int64(gkVec.Int32Data[row])
			} else {
				key = gkVec.Int64Data[row]
			}
			var newIdx int32
			fromFree := false
			if nf := len(h.freeGroupIDs); nf > 0 {
				newIdx = h.freeGroupIDs[nf-1]
				fromFree = true
			} else {
				newIdx = int32(h.numIntGroups)
			}
			gsIdx, ok := tl.GetOrInsertAt(key, ph[row], newIdx)
			if ok {
				gi[row] = gsIdx
			} else {
				if fromFree {
					h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
					h.intKeys[newIdx] = key
				} else {
					h.numIntGroups++
					h.intKeys = append(h.intKeys, key)
				}
				gi[row] = newIdx
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}
	for row := 0; row < iterLen; row++ {
		if hasNulls && gkVec.Nulls.IsNullFast(row) {
			gi[row] = -1
			hasNullKeys = true
			continue
		}
		var key int64
		if isInt32 {
			key = int64(gkVec.Int32Data[row])
		} else {
			key = gkVec.Int64Data[row]
		}
		var newIdx int32
		fromFree := false
		if nf := len(h.freeGroupIDs); nf > 0 {
			newIdx = h.freeGroupIDs[nf-1]
			fromFree = true
		} else {
			newIdx = int32(h.numIntGroups)
		}
		gsIdx, ok := tl.GetOrInsertAt(key, fibHash(key), newIdx)
		if ok {
			gi[row] = gsIdx
		} else {
			if fromFree {
				h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
				h.intKeys[newIdx] = key
			} else {
				h.numIntGroups++
				h.intKeys = append(h.intKeys, key)
			}
			gi[row] = newIdx
		}
	}
	return gi, sel, iterLen, hasNullKeys
}

// consumeBatchPackedGroup is the fast path for multi-column int-class GROUP
// BY whose packed key fits in 128 bits. Two-phase SoA approach like
// consumeBatchIntGroup, with the composite key held inline in the hash
// entry: ONE probe per row resolves the group (or mints it), against the
// dual-int predecessor's Get-then-Put plus a chain walk across three
// separate per-group arrays.
func (h *HashAggregate) consumeBatchPackedGroup(b *batch.RecordBatch) {
	// Resolve each key column's typed slice and its fixed slot in the key
	// once per batch.
	cols := h.packedCols[:0]
	hasNulls := false
	for ci, colIdx := range h.groupColIdx {
		v := b.Columns[colIdx]
		f := h.packedLayout[ci]
		pc := packedKeyCol{nulls: &v.Nulls, word: f.word, shift: f.shift}
		if f.i32 {
			pc.i32 = v.Int32Data
		} else {
			pc.i64 = v.Int64Data
		}
		cols = append(cols, pc)
		hasNulls = hasNulls || v.Nulls.HasNulls()
	}
	h.packedCols = cols

	idx := h.packedIdx
	liveBefore := 0
	if idx != nil {
		liveBefore = idx.Len()
	}

	// Hash once: the router already folded this row's 128-bit key through
	// packedHash to choose the owner. Accept its array only when the plan's
	// key layout is identical to this aggregate's, so the lo/hi words the
	// router packed are the ones this loop packs.
	var ph []uint64
	if p := h.provPlan; p != nil && p.kind == hashKindPacked &&
		samePackedLayout(p.layout, h.packedLayout) && len(h.provHashes) == b.ActiveLen() {
		ph = h.provHashes
		HashOnceRoutedRows.Add(int64(len(ph)))
	}

	// Pre-reserve key capacity for this batch. The flat accumulators grow
	// once, after the lookup loop, to the final group count.
	batchRows := b.ActiveLen()
	h.packedKeys = ensureAppendCap(h.packedKeys, batchRows)

	// Phase 1: hash lookup — one probe per row.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	// hasNulls is defensively false in practice: consumeBatch migrates the
	// whole aggregate to the generic path before a batch with a NULL key
	// column reaches here. Keeping the branch costs one predictable test per
	// row and keeps null-key rows accounted if that ever changes.
	// The routed and self-hashing loops are written out separately rather than
	// branching on ph per row. That branch is perfectly predicted and still
	// measured +4% on this path's own benchmark (two-int64 2.22 -> 2.32 ms,
	// same-window A/B) — at ~4 ns/row a single extra compare is real money, and
	// hoisting it is why the self-hashing loops below are byte-identical to
	// what they were before hash-once landed.
	if tl := h.packedTwoLevel; tl != nil {
		gi, sel, iterLen, hasNullKeys = h.packedPhase1TwoLevel(b, tl, cols, hasNulls, ph)
	} else if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && packedRowHasNull(cols, row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrowAt(lo, hi, ph[si], newIdx)
				gi[si] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		} else {
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && packedRowHasNull(cols, row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				// newIdx is the id this row would claim; the table hands back
				// exactly it when the key was new (see GetOrInsertNoGrow).
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrow(lo, hi, newIdx)
				gi[si] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					// No per-group state object: this path is gated to simple
					// aggregates, whose state lives entirely in the flat SoA
					// arrays + packedKeys. materializeFlatAccums reifies on
					// demand for the migration/merge cold paths.
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
			for row := 0; row < iterLen; row++ {
				if hasNulls && packedRowHasNull(cols, row) {
					gi[row] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrowAt(lo, hi, ph[row], newIdx)
				gi[row] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if hasNulls && packedRowHasNull(cols, row) {
					gi[row] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrow(lo, hi, newIdx)
				gi[row] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		}
	}

	// Phase 2: per-aggregate typed scatter update using flat arrays.
	h.scatterBatchAggs(h.numIntGroups, b, gi, sel, iterLen)

	// Handle null-key rows via generic path (rare).
	if hasNullKeys {
		if sel != nil {
			for si, selIdx := range sel {
				if gi[si] < 0 {
					h.processRow(b, int(selIdx))
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if gi[row] < 0 {
					h.processRow(b, row)
				}
			}
		}
	}

	// Bucketed conversion, decided once per batch at the end — see
	// consumeBatchIntGroup.
	if idx != nil {
		h.maybeConvertPackedIndex(idx.Len() - liveBefore)
	}
}

// packedPhase1TwoLevel is consumeBatchPackedGroup's phase 1 against the
// BUCKETED index (two_level_hash.go) — same key packing, same dense group
// ids, same packedKeys SoA append; only the probe's table differs.
func (h *HashAggregate) packedPhase1TwoLevel(b *batch.RecordBatch, tl *packedTwoLevelTable,
	cols []packedKeyCol, hasNulls bool, ph []uint64) (gi []int32, sel []uint32, iterLen int, hasNullKeys bool) {
	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && packedRowHasNull(cols, row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := tl.GetOrInsertAt(lo, hi, ph[si], newIdx)
				gi[si] = gsIdx
				if gsIdx == newIdx {
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
			return gi, sel, iterLen, hasNullKeys
		}
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if hasNulls && packedRowHasNull(cols, row) {
				gi[si] = -1
				hasNullKeys = true
				continue
			}
			lo, hi := packedKeyAt(cols, row)
			newIdx := int32(h.numIntGroups)
			gsIdx := tl.GetOrInsertAt(lo, hi, packedHash(lo, hi), newIdx)
			gi[si] = gsIdx
			if gsIdx == newIdx {
				h.numIntGroups++
				h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}

	iterLen = b.Len
	gi = h.ensureGroupIndexBuf(iterLen)
	if ph != nil {
		ph = ph[:iterLen]
		for row := 0; row < iterLen; row++ {
			if hasNulls && packedRowHasNull(cols, row) {
				gi[row] = -1
				hasNullKeys = true
				continue
			}
			lo, hi := packedKeyAt(cols, row)
			newIdx := int32(h.numIntGroups)
			gsIdx := tl.GetOrInsertAt(lo, hi, ph[row], newIdx)
			gi[row] = gsIdx
			if gsIdx == newIdx {
				h.numIntGroups++
				h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}
	for row := 0; row < iterLen; row++ {
		if hasNulls && packedRowHasNull(cols, row) {
			gi[row] = -1
			hasNullKeys = true
			continue
		}
		lo, hi := packedKeyAt(cols, row)
		newIdx := int32(h.numIntGroups)
		gsIdx := tl.GetOrInsertAt(lo, hi, packedHash(lo, hi), newIdx)
		gi[row] = gsIdx
		if gsIdx == newIdx {
			h.numIntGroups++
			h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
		}
	}
	return gi, sel, iterLen, hasNullKeys
}

// packedRowHasNull reports whether any key column is NULL at row.
func packedRowHasNull(cols []packedKeyCol, row int) bool {
	for i := range cols {
		if cols[i].nulls.IsNullFast(row) {
			return true
		}
	}
	return false
}
