// This file holds flat accumulator merging and group-layout migration.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// mergeIntGroupSoA merges another int-keyed SoA aggregate directly, avoiding
// materializeFlatAccums + migrateToGenericMap + per-group Accumulator.Merge.
// Operates on flat arrays (count, sumI64, sumF64, min, max) with int hash lookup.
func (h *HashAggregate) mergeIntGroupSoA(o *HashAggregate) {
	// A merge that will drive the destination past its load factor converts
	// FIRST, so the inserted groups land in per-bucket tables instead of
	// driving whole-table rehashes. Same rule as the consume path: the
	// incoming group count is the lookahead, and a merge that fits under the
	// existing capacity stays flat because there is no rehash to displace
	// (two_level_hash.go).
	if idx := h.intGroupIndex; idx != nil &&
		h.indexConverts(idx.Len(), idx.Slots(), o.numIntGroups) {
		h.intTwoLevel = convertIntHashTableToTwoLevel(idx, h.offheapReg())
		h.intGroupIndex = nil
		h.indexConversions++
		TwoLevelConversions.Add(1)
	}
	for i := 0; i < o.numIntGroups; i++ {
		newIdx := int32(h.numIntGroups)
		gsIdx, found := h.intIndexGetOrInsert(o.intKeys[i], newIdx)
		if found {
			mergeFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(gsIdx), i)
		} else {
			// New group: claim the slot, then copy o's row into it.
			h.appendIntGroupSlot(o.groupStateAt(i))
			h.intKeys = append(h.intKeys, o.intKeys[i])
			copyFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(newIdx), i)
		}
	}
}

// mergeFlatAccumRow combines src's row srcIdx into dst's existing row dstIdx
// for every aggregate: sums add, MIN/MAX take the extreme, and an unseeded
// destination adopts the source's value. The single definition serves every
// SoA merge path (int-keyed and packed-keyed); it used to be copy-pasted per
// key mode.
func mergeFlatAccumRow(dst, src []flatAccumArrays, dstIdx, srcIdx int) {
	for ai := range dst {
		hfa := &dst[ai]
		ofa := &src[ai]
		if hfa.count != nil {
			hfa.count[dstIdx] += ofa.count[srcIdx]
		}
		if hfa.sumI64 != nil {
			// The int64 fold is CHECKED for the same reason the scatter is:
			// two group partials that each held their sum can wrap when
			// combined, and this is where a drained partial rejoins.
			cur, add := hfa.sumI64[dstIdx], ofa.sumI64[srcIdx]
			sum := cur + add
			if (cur^sum)&(add^sum) < 0 {
				hfa.sumIntOverflow = true
			}
			hfa.sumI64[dstIdx] = sum
		}
		if hfa.sumF64 != nil {
			hfa.sumF64[dstIdx] += ofa.sumF64[srcIdx]
		}
		if hfa.sumDec != nil {
			sum, ok := hfa.sumDec[dstIdx].AddChecked(ofa.sumDec[srcIdx])
			hfa.sumDec[dstIdx] = sum
			if !ok {
				hfa.sumDecOverflow = true
			}
		}
		if ofa.sumDecOverflow {
			hfa.sumDecOverflow = true
		}
		if ofa.sumIntOverflow {
			hfa.sumIntOverflow = true
		}
		if ofa.hasMin != nil && ofa.hasMin[srcIdx] {
			if hfa.hasMin[dstIdx] {
				if hfa.isFloat {
					if kernel.CompareFloat64(ofa.minF64[srcIdx], hfa.minF64[dstIdx]) < 0 {
						hfa.minF64[dstIdx] = ofa.minF64[srcIdx]
					}
				} else if hfa.isDecimal {
					if ofa.minDec[srcIdx].Less(hfa.minDec[dstIdx]) {
						hfa.minDec[dstIdx] = ofa.minDec[srcIdx]
					}
				} else {
					if ofa.minI64[srcIdx] < hfa.minI64[dstIdx] {
						hfa.minI64[dstIdx] = ofa.minI64[srcIdx]
					}
				}
			} else {
				hfa.hasMin[dstIdx] = true
				if hfa.minI64 != nil {
					hfa.minI64[dstIdx] = ofa.minI64[srcIdx]
				}
				if hfa.minF64 != nil {
					hfa.minF64[dstIdx] = ofa.minF64[srcIdx]
				}
				if hfa.minDec != nil {
					hfa.minDec[dstIdx] = ofa.minDec[srcIdx]
				}
			}
		}
		if ofa.hasMax != nil && ofa.hasMax[srcIdx] {
			if hfa.hasMax[dstIdx] {
				if hfa.isFloat {
					if kernel.CompareFloat64(ofa.maxF64[srcIdx], hfa.maxF64[dstIdx]) > 0 {
						hfa.maxF64[dstIdx] = ofa.maxF64[srcIdx]
					}
				} else if hfa.isDecimal {
					if !ofa.maxDec[srcIdx].Less(hfa.maxDec[dstIdx]) {
						hfa.maxDec[dstIdx] = ofa.maxDec[srcIdx]
					}
				} else {
					if ofa.maxI64[srcIdx] > hfa.maxI64[dstIdx] {
						hfa.maxI64[dstIdx] = ofa.maxI64[srcIdx]
					}
				}
			} else {
				hfa.hasMax[dstIdx] = true
				if hfa.maxI64 != nil {
					hfa.maxI64[dstIdx] = ofa.maxI64[srcIdx]
				}
				if hfa.maxF64 != nil {
					hfa.maxF64[dstIdx] = ofa.maxF64[srcIdx]
				}
				if hfa.maxDec != nil {
					hfa.maxDec[dstIdx] = ofa.maxDec[srcIdx]
				}
			}
		}
	}
}

// copyFlatAccumRow grows dst to cover slot dstIdx and copies every live field
// of src's row srcIdx into it. Growth is idempotent per array, so a count
// array shared by several aggregates is extended (and copied) exactly once —
// which is why the merge paths grow-then-assign instead of appending per
// field.
func copyFlatAccumRow(dst, src []flatAccumArrays, dstIdx, srcIdx int) {
	for ai := range dst {
		dfa := &dst[ai]
		sfa := &src[ai]
		dfa.growTo(dstIdx + 1)
		if dfa.count != nil {
			dfa.count[dstIdx] = sfa.count[srcIdx]
		}
		if dfa.sumI64 != nil {
			dfa.sumI64[dstIdx] = sfa.sumI64[srcIdx]
		}
		if dfa.sumF64 != nil {
			dfa.sumF64[dstIdx] = sfa.sumF64[srcIdx]
		}
		if dfa.sumDec != nil {
			dfa.sumDec[dstIdx] = sfa.sumDec[srcIdx]
		}
		if sfa.sumDecOverflow {
			dfa.sumDecOverflow = true
		}
		if sfa.sumIntOverflow {
			dfa.sumIntOverflow = true
		}
		if dfa.minI64 != nil {
			dfa.minI64[dstIdx] = sfa.minI64[srcIdx]
		}
		if dfa.maxI64 != nil {
			dfa.maxI64[dstIdx] = sfa.maxI64[srcIdx]
		}
		if dfa.minF64 != nil {
			dfa.minF64[dstIdx] = sfa.minF64[srcIdx]
		}
		if dfa.maxF64 != nil {
			dfa.maxF64[dstIdx] = sfa.maxF64[srcIdx]
		}
		if dfa.minDec != nil {
			dfa.minDec[dstIdx] = sfa.minDec[srcIdx]
		}
		if dfa.maxDec != nil {
			dfa.maxDec[dstIdx] = sfa.maxDec[srcIdx]
		}
		if dfa.hasMin != nil {
			dfa.hasMin[dstIdx] = sfa.hasMin[srcIdx]
		}
		if dfa.hasMax != nil {
			dfa.hasMax[dstIdx] = sfa.hasMax[srcIdx]
		}
	}
}

// mergePackedGroupSoA merges another packed-key SoA aggregate directly. One
// probe per source group against the composite key held inline in the entry
// — the dual-int predecessor did a Get, a chain walk over three arrays, and
// then a second Get before its Put.
func (h *HashAggregate) mergePackedGroupSoA(o *HashAggregate) {
	if idx := h.packedIdx; idx != nil &&
		h.indexConverts(idx.Len(), idx.Slots(), o.numIntGroups) {
		h.packedTwoLevel = convertPackedHashTableToTwoLevel(idx, h.offheapReg())
		h.packedIdx = nil
		h.indexConversions++
		TwoLevelConversions.Add(1)
	}
	for i := 0; i < o.numIntGroups; i++ {
		k := o.packedKeys[i]
		newIdx := int32(h.numIntGroups)
		gsIdx, found := h.packedIndexGetOrInsert(k.lo, k.hi, newIdx)
		if found {
			mergeFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(gsIdx), i)
			continue
		}
		h.appendIntGroupSlot(o.groupStateAt(i))
		h.packedKeys = append(h.packedKeys, k)
		copyFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(newIdx), i)
	}
}

// migrateToGenericMap converts int/compact group key state to the generic
// map[string]*groupState path. No-op if already using the generic path.
func (h *HashAggregate) migrateToGenericMap() {
	// Materialize SoA accumulators before migration needs gs.accs
	h.materializeFlatAccums()
	if h.useCompactGroupKey {
		h.migrateCompactToGeneric()
		return
	}
	if h.usePackedGroupKey {
		// Migrate packed composite group key → generic path. Keys are
		// re-encoded in processRow's binary format ([null-flag]
		// [appendColumnValue bytes] per column) — NOT serializeKey's text
		// format — because the generic path keeps inserting after this
		// migration runs (a null group key triggers it mid-consume, and
		// MergeSink can pair a migrated side with a natively-generic side).
		// A text-format index entry would never match the binary key of the
		// same logical group, silently duplicating groups.
		h.strGroupIndex = newStrHashTable(h.numIntGroups)
		h.strGroupStates = make([]*groupState, 0, h.numIntGroups)
		h.serializedKeys = make([]string, 0, h.numIntGroups)
		h.serializedKeyBytes = 0
		h.keys = make([][]any, 0, h.numIntGroups)
		for i, gs := range h.intGroupStates {
			k := h.packedKeys[i]
			ext := gs.ensureExtras()
			if ext.keyValues == nil {
				vals := make([]any, len(h.packedLayout))
				for j, f := range h.packedLayout {
					vals[j] = f.get(k)
				}
				ext.keyValues = vals
			}
			h.keyBuf = h.keyBuf[:0]
			for j, f := range h.packedLayout {
				h.keyBuf = appendIntKeyRowFormat(h.keyBuf, f.get(k), h.groupColTypes[j])
			}
			key := string(h.keyBuf)
			h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
			h.strGroupStates = append(h.strGroupStates, gs)
			h.serializedKeys = append(h.serializedKeys, key)
			h.serializedKeyBytes += int64(len(key))
			h.keys = append(h.keys, ext.keyValues)
		}
		h.usePackedGroupKey = false
		h.intGroupStates = nil
		h.numIntGroups = 0
		h.packedIdx = nil
		h.packedTwoLevel = nil
		h.packedKeys = nil
		return
	}
	if !h.useIntGroupKey {
		return
	}
	// Migrate int group key → generic path (binary key format — see the
	// packed-key branch comment).
	h.strGroupIndex = newStrHashTable(h.numIntGroups)
	h.strGroupStates = make([]*groupState, 0, h.numIntGroups)
	h.serializedKeys = make([]string, 0, h.numIntGroups)
	h.serializedKeyBytes = 0
	h.keys = make([][]any, 0, h.numIntGroups)
	for gi, gs := range h.intGroupStates {
		intKey := h.intKeys[gi]
		if gs == nil {
			// Deferred single-int state: reify for the generic path.
			gs = h.gsPool.alloc()
			gs.intKey = intKey
		}
		// Lazily construct keyValues for groups that deferred boxing
		ext := gs.ensureExtras()
		if ext.keyValues == nil {
			ext.keyValues = []any{intKey}
		}
		key := string(appendIntKeyRowFormat(h.keyBuf[:0], intKey, h.groupColTypes[0]))
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
		h.serializedKeyBytes += int64(len(key))
		h.keys = append(h.keys, ext.keyValues)
	}
	h.useIntGroupKey = false
	h.intGroupStates = nil
	h.numIntGroups = 0
	h.intGroupIndex = nil
	h.intTwoLevel = nil
	h.intKeys = nil
}
