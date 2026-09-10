// This file holds distinct-value tracking and group-state adoption.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// distinctColType resolves the input column type for aggregate i from the
// live batch, for choosing the distinct-set representation. Unresolvable
// columns fall back to the string representation (safe for any type).
func (h *HashAggregate) distinctColType(b *batch.RecordBatch, i int) batch.TypeID {
	if i < len(h.aggColIdx) {
		if idx := h.aggColIdx[i]; idx >= 0 && idx < len(b.Columns) {
			return b.Columns[idx].Type
		}
	}
	return batch.TypeString
}

// distinctFirstSighting reports whether this group is seeing aggregate i's
// argument value for the FIRST time, recording it if so — SQL's
// `AGG(DISTINCT x)` for every aggregate whose state is not the set itself
// (#703).
//
// A NULL argument answers true and records nothing: NULL is not part of any
// aggregate's input, and every arm of updateGroup skips it on its own, so
// answering false here would only move that skip earlier — while putting NULL
// in the set would make the FIRST null suppress a later real value under a
// representation where they collide.
//
// The key is the value at its EXACT type: the int set for the int-class
// columns, and otherwise appendColumnValue's encoding — the same one the
// group key uses (ADR-0023), so a DECIMAL dedupes on its unscaled integer at
// its own scale and never through a float. A second argument (CORR, COVAR_*,
// MIN_BY, MAX_BY) is appended to the same key, because PostgreSQL's DISTINCT
// dedupes an aggregate's whole ARGUMENT LIST rather than its first argument.
func (h *HashAggregate) distinctFirstSighting(ext *groupStateExtras, i int, b *batch.RecordBatch, row int) bool {
	if ext == nil || i >= len(ext.distinctSets) {
		return true
	}
	ds := ext.distinctSets[i]
	if ds == nil {
		return true
	}
	idx := h.aggColIdx[i]
	if idx < 0 || idx >= len(b.Columns) {
		return true
	}
	v := b.Columns[idx]
	if v.Nulls.IsNullFast(row) {
		return true
	}
	idx2 := -1
	if i < len(h.aggColIdx2) {
		idx2 = h.aggColIdx2[i]
	}
	if ds.ints != nil && idx2 < 0 {
		if ds.addInt(intColValue(v, row)) {
			h.distinctBytes += 16
			return true
		}
		return false
	}
	h.keyBuf = appendColumnValue(h.keyBuf[:0], v, row, v.Type)
	if idx2 >= 0 && idx2 < len(b.Columns) {
		v2 := b.Columns[idx2]
		if v2.Nulls.IsNullFast(row) {
			return true
		}
		h.keyBuf = appendColumnValue(h.keyBuf, v2, row, v2.Type)
	}
	if ds.addStr(h.keyBuf) {
		h.distinctBytes += int64(len(h.keyBuf)) + 48
		return true
	}
	return false
}

// intColValue reads an int-class column value widened to int64.
func intColValue(v *batch.Vector, row int) int64 {
	switch v.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row])
	default:
		return v.Int64Data[row]
	}
}

// groupCount returns the number of group slots across the key modes. Used
// as the emptiness gate for state adoption — a partially-drained aggregate
// (freed slots still occupy the slices) reports non-zero and is not adopted
// into.
func (h *HashAggregate) groupCount() int {
	return h.numIntGroups + len(h.strGroupStates)
}

// groupStateAt returns the AoS state for int-keyed slot i, or nil when the
// path deferred it (single-int / packed, where intGroupStates is empty).
func (h *HashAggregate) groupStateAt(i int) *groupState {
	if i < len(h.intGroupStates) {
		return h.intGroupStates[i]
	}
	return nil
}

// appendIntGroupSlot claims the next int-keyed group slot and returns its
// index. A nil gs on a path that never materialized state leaves
// intGroupStates empty (numIntGroups alone tracks the count); anything else
// stores gs, padding the slice so slot indices stay aligned.
func (h *HashAggregate) appendIntGroupSlot(gs *groupState) int32 {
	idx := int32(h.numIntGroups)
	h.numIntGroups++
	if gs == nil && len(h.intGroupStates) == 0 {
		return idx
	}
	for len(h.intGroupStates) < int(idx) {
		h.intGroupStates = append(h.intGroupStates, nil)
	}
	h.intGroupStates = append(h.intGroupStates, gs)
	return idx
}

// adoptStateFrom moves o's entire group state (and the schema-resolution
// and key-mode fields it depends on — deterministic given the shared config
// and input schema, so overwriting is safe even when h already resolved)
// into an EMPTY h. O(1): slice/pointer moves, no per-group work. o's state
// is zeroed so its Close releases only its (still-intact) tracking charge
// and emits nothing.
func (h *HashAggregate) adoptStateFrom(o *HashAggregate) {
	// Resolution state.
	h.groupColIdx = o.groupColIdx
	h.aggColIdx = o.aggColIdx
	h.aggColIdx2 = o.aggColIdx2
	h.aggInputTypes = o.aggInputTypes
	h.aggInputMeta = o.aggInputMeta
	// COPIED, not aliased, for mergeSinkState's reason: the scale upgrade
	// there writes into h.aggInputDecScale on every later merge, and sharing
	// the slice with the clone it was adopted from would write back into a
	// state o still owns until its Close.
	//
	// The comparison happens BEFORE the overwrite, and for mergeSinkState's
	// reason one seam over: an empty primary reaches the adopt path instead of
	// the upgrade loop, so without this the same two clones at two scales came
	// back 25.50 through here.
	for i, have := range h.aggInputDecScale {
		if i < len(o.aggInputDecScale) && have != 0 &&
			o.aggInputDecScale[i] != 0 && have != o.aggInputDecScale[i] {
			h.decScaleConflict = true
		}
	}
	h.aggInputDecScale = append([]int(nil), o.aggInputDecScale...)
	h.aggBoxedMinMax = o.aggBoxedMinMax
	h.hasBoxedMinMax = o.hasBoxedMinMax
	// The operator-wide cross-scale latch travels with the state it describes.
	if o.decScaleConflict {
		h.decScaleConflict = true
	}
	h.groupColTypes = o.groupColTypes
	h.groupColMeta = o.groupColMeta
	h.aggUpdaters = o.aggUpdaters
	h.aggUpdatersNoNull = o.aggUpdatersNoNull
	// Per-batch updater-selection scratch, normally sized by resolveIndices.
	// h skipped resolveIndices when its warmup batch was fully filtered, and
	// adopting o.resolved below suppresses it forever — yet the post-merge
	// consume paths (pressure-collapse serial continuation, spilled-partition
	// replay) index this scratch unconditionally (#279: SF100 Q18 join-8,
	// index-out-of-range at the first post-adoption consumeBatch).
	h.batchUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
	h.batchAggKernels = o.batchAggKernels
	h.aggF64Extract = o.aggF64Extract
	h.aggF64Extract2 = o.aggF64Extract2
	h.resolved = o.resolved
	h.needsDistinct = o.needsDistinct
	h.needsExtra = o.needsExtra
	h.simpleAggs = o.simpleAggs
	h.inputSchema = o.inputSchema
	// Key mode + state.
	h.useIntGroupKey = o.useIntGroupKey
	h.intGroupIndex = o.intGroupIndex
	h.intTwoLevel = o.intTwoLevel
	h.intGroupStates = o.intGroupStates
	h.numIntGroups = o.numIntGroups
	h.intGroupKeyCol = o.intGroupKeyCol
	h.intKeys = o.intKeys
	h.intFlatAccs = o.intFlatAccs
	// The adopted slices live in o's off-heap reservations: take ownership
	// so o.Close (the pipeline closes merged clones) doesn't unmap them
	// out from under h.
	if o.offheap != nil {
		if h.offheap == nil {
			h.offheap = o.offheap
		} else {
			h.offheap.AdoptFrom(o.offheap)
		}
		o.offheap = nil
	}
	h.usePackedGroupKey = o.usePackedGroupKey
	h.packedIdx = o.packedIdx
	h.packedTwoLevel = o.packedTwoLevel
	h.packedLayout = o.packedLayout
	h.packedKeys = o.packedKeys
	h.useCompactGroupKey = o.useCompactGroupKey
	h.compactKeys = o.compactKeys
	h.useStrGroupKey = o.useStrGroupKey
	h.strGroupKeyCol = o.strGroupKeyCol
	h.strNullGroupIdx = o.strNullGroupIdx
	h.useGenericSoA = o.useGenericSoA
	h.strGroupIndex = o.strGroupIndex
	h.strGroupStates = o.strGroupStates
	h.deferGenericKeyBoxing = o.deferGenericKeyBoxing
	h.genKeyIdx = o.genKeyIdx
	h.genKeyNext = o.genKeyNext
	h.keys = o.keys
	h.serializedKeys = o.serializedKeys
	h.gsPool = o.gsPool
	h.freeGroupIDs = o.freeGroupIDs
	h.drainK = o.drainK
	h.nextDrainPartition = o.nextDrainPartition
	// State byte counters travel with the state.
	h.serializedKeyBytes = o.serializedKeyBytes
	h.compactKeyBytes = o.compactKeyBytes
	h.distinctBytes = o.distinctBytes
	h.extraStateBytes = o.extraStateBytes
	h.extrasAccsCount = o.extrasAccsCount

	// Zero o's moved state (o keeps its trackedGroupMem so Close releases
	// the charge it made while accumulating).
	o.intGroupIndex = nil
	o.intTwoLevel = nil
	o.intGroupStates = nil
	o.numIntGroups = 0
	o.intKeys = nil
	o.intFlatAccs = nil
	o.packedIdx = nil
	o.packedTwoLevel = nil
	o.packedKeys = nil
	o.compactKeys = nil
	o.strGroupIndex = nil
	o.strGroupStates = nil
	o.strNullGroupIdx = -1
	o.genKeyIdx = nil
	o.genKeyNext = nil
	o.keys = nil
	o.serializedKeys = nil
	o.gsPool = groupStatePool{}
	o.freeGroupIDs = nil
	o.resetStateByteCounters()
}
