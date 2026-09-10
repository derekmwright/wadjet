// This file holds row-wise group creation and accumulator updates.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

func (h *HashAggregate) processRow(b *batch.RecordBatch, row int) {
	// Serialize group key using binary encoding (fixed-width for numeric types).
	// Each column is prefixed by a 1-byte null flag (0=value, 1=null).
	h.keyBuf = h.keyBuf[:0]
	for i, idx := range h.groupColIdx {
		if idx < 0 {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		h.keyBuf = append(h.keyBuf, 0) // not-null flag
		h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[i])
	}

	// Use open-addressing string hash table to avoid GC overhead of map[string].
	groupIdx, found := h.strIndexForRow().GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
	if found {
		h.updateGroup(h.strGroupStates[groupIdx], b, row)
		return
	}

	// New group
	keyVals := make([]any, len(h.GroupByCols))
	for i, idx := range h.groupColIdx {
		if idx >= 0 {
			keyVals[i] = b.Columns[idx].GetValue(row)
		}
	}
	gs := h.gsPool.alloc()
	ext := gs.ensureExtras()
	ext.keyValues = keyVals
	ext.accs = make([]kernel.Accumulator, len(h.Aggs))
	h.extrasAccsCount += int64(len(h.Aggs))
	if h.needsDistinct {
		ext.distinctSets = make([]*distinctSet, len(h.Aggs))
	}
	if h.needsExtra {
		ext.extraState = make([]any, len(h.Aggs))
		h.extraStateBytes += int64(len(h.Aggs)) * 80
	}
	h.initGroupState(ext, b)
	h.strGroupStates = append(h.strGroupStates, gs)
	h.keys = append(h.keys, keyVals)
	h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
	h.serializedKeyBytes += int64(len(h.keyBuf))

	// Keep the SoA flat accumulator length in sync with strGroupStates.
	// processRow is called from the null-key branch of consumeBatchStrGroup
	// (and other str-group paths) AFTER the SoA fast paths are configured;
	// without this extra slot, the hash table maps a new key to gsIdx N but
	// fa.count is still len N, so the next batch's scatterCountStar indexes
	// out of bounds. Manifested as Q21 SF1's mysterious panic.
	h.appendFlatAccumSlot()

	h.updateGroup(gs, b, row)
}

// processRowGroupingSets inserts a row into each grouping set. The key is
// prefixed with the set index byte, and only columns in the set are serialized.
// Columns not in the set are stored as nil in keyValues so the output path
// can emit NULLs for excluded columns.
func (h *HashAggregate) processRowGroupingSets(b *batch.RecordBatch, row int) {
	for setIdx, colIndices := range h.GroupingSets {
		h.keyBuf = h.keyBuf[:0]
		h.keyBuf = append(h.keyBuf, byte(setIdx)) // set prefix

		// Serialize only the columns in this set
		for _, ci := range colIndices {
			idx := h.groupColIdx[ci]
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1) // null flag
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

		groupIdx, found := h.strIndexForRow().GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
		if found {
			h.updateGroup(h.strGroupStates[groupIdx], b, row)
			continue
		}

		// New group — store all GroupByCols values, NULLing excluded columns
		inSet := make(map[int]bool, len(colIndices))
		for _, ci := range colIndices {
			inSet[ci] = true
		}
		keyVals := make([]any, len(h.GroupByCols))
		for i, idx := range h.groupColIdx {
			if inSet[i] && idx >= 0 {
				keyVals[i] = b.Columns[idx].GetValue(row)
			}
			// columns not in set stay nil
		}
		gs := h.gsPool.alloc()
		gs.setID = int32(setIdx)
		ext := gs.ensureExtras()
		ext.keyValues = keyVals
		ext.accs = make([]kernel.Accumulator, len(h.Aggs))
		h.extrasAccsCount += int64(len(h.Aggs))
		if h.needsDistinct {
			ext.distinctSets = make([]*distinctSet, len(h.Aggs))
		}
		if h.needsExtra {
			ext.extraState = make([]any, len(h.Aggs))
			h.extraStateBytes += int64(len(h.Aggs)) * 80
		}
		h.initGroupState(ext, b)
		h.strGroupStates = append(h.strGroupStates, gs)
		h.keys = append(h.keys, keyVals)
		h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
		h.serializedKeyBytes += int64(len(h.keyBuf))
		h.updateGroup(gs, b, row)
	}
}

// initGroupState fills a freshly allocated group's distinct sets and
// extraState, one entry per aggregate. Shared by processRow and
// processRowGroupingSets: they used to carry a copy each, and a copy that
// falls behind hands updateGroup a nil state to type-assert (the panic
// AggVarState hit on its first distributed run).
//
// Callers must have allocated ext.distinctSets / ext.extraState already —
// whether either is needed is decided once, at resolution, by
// h.needsDistinct / h.needsExtra.
func (h *HashAggregate) initGroupState(ext *groupStateExtras, b *batch.RecordBatch) {
	for i, agg := range h.Aggs {
		// The DISTINCT set of an aggregate that is not COUNT (#703). Allocated
		// here, beside COUNT(DISTINCT)'s, so one merge rule
		// (mergeSinkState/distinctSet.mergeFrom) serves both.
		if agg.Distinct && ext.distinctSets != nil && ext.distinctSets[i] == nil {
			// A TWO-ARGUMENT aggregate dedupes on the pair, so its key is the
			// two encodings appended — a string set whatever the first
			// column's type is. distinctFirstSighting builds the same key.
			typ := h.distinctColType(b, i)
			if i < len(h.aggColIdx2) && h.aggColIdx2[i] >= 0 {
				typ = batch.TypeString
			}
			ext.distinctSets[i] = newDistinctSetFor(typ)
			h.distinctBytes += 48
		}
		switch agg.Func {
		case AggCountDistinct, AggApproxDistinct:
			if ext.distinctSets != nil && ext.distinctSets[i] == nil {
				ext.distinctSets[i] = newDistinctSetFor(h.distinctColType(b, i))
				h.distinctBytes += 48
			}
		case AggStringAgg:
			sep := agg.Separator
			if sep == "" {
				sep = ","
			}
			ext.extraState[i] = &stringAggState{sep: sep, sorted: agg.Distinct}
		case AggStddev, AggVariance, AggStddevPop, AggVarPop,
			AggVarState, AggVarStateMerge:
			ext.extraState[i] = &varianceState{}
		case AggBoolAnd, AggBoolOr:
			// Deliberately left nil: the state is seeded by the first
			// non-NULL input (updateGroup), so a group that never sees one
			// finalizes to NULL per SQL — an eager identity value (true for
			// AND) answered `true` over an all-NULL group.
		case AggCorr, AggCovarSamp, AggCovarPop,
			AggCovarState, AggCovarStateMerge:
			ext.extraState[i] = &covarianceState{}
		case AggPercentileCont, AggPercentileDisc, AggMode, AggMedian:
			ext.extraState[i] = &collectState{}
		case AggOhlcv, AggOhlcvState, AggOhlcvStateMerge:
			// The carrier is resolved at Consume from the vectors, so a
			// group minted before the first batch is seen would carry the
			// zero domain. resolveIndices runs first (h.resolved), so
			// aggOhlcvDom is already filled here.
			dom := ohlcvDomain{}
			if i < len(h.aggOhlcvDom) {
				dom = h.aggOhlcvDom[i]
			}
			// The declared ROW is stamped on at construction, by the forms
			// that READ the raw columns, and travels with the values through
			// the encoding — so a merge stage and the coordinator's fold
			// finish the bar against the declaration it was COMPUTED for
			// rather than one they had to be told (#965).
			//
			// The MERGE form stamps NOTHING. Its input is the encoded state
			// column, a STRING, so any declaration it derived would be a
			// guess — and a guessed one is worse than none: it wrote a
			// DECIMAL bar's digits into FLOAT64 children and tripped the #361
			// silent-write guard. It adopts the first partial's instead.
			st := &ohlcvState{dom: dom}
			if agg.Func != AggOhlcvStateMerge {
				st.fields = h.ohlcvFields(i)
			}
			ext.extraState[i] = st
		case AggMinBy:
			ext.extraState[i] = &minMaxByState{isMin: true}
		case AggMaxBy:
			ext.extraState[i] = &minMaxByState{isMin: false}
		case AggMin, AggMax:
			// Only the container form takes an extraState; scalar MIN/MAX
			// stay on the Accumulator, whose slot is already allocated.
			if i < len(h.aggBoxedMinMax) && h.aggBoxedMinMax[i] {
				ext.extraState[i] = &containerMinMaxState{isMin: agg.Func == AggMin}
			}
		}
	}
}

// updateGroup updates a group's accumulators with values from a single row.
// updateGroup is only ever called on groups whose extras was allocated by
// processRow / processRowGroupingSets, so gs.extras is non-nil here and we
// bind it to a local for compactness.
func (h *HashAggregate) updateGroup(gs *groupState, b *batch.RecordBatch, row int) {
	ext := gs.extras
	for i, agg := range h.Aggs {
		// SQL's DISTINCT, for every aggregate whose state is not itself the
		// set: the row is folded in the FIRST time this group sees its value
		// and skipped afterwards (#703). COUNT(DISTINCT)/APPROX_DISTINCT keep
		// their own arms below — their answer IS the set's size.
		if agg.Distinct && !h.distinctFirstSighting(ext, i, b, row) {
			continue
		}
		// MIN/MAX over a container is the one aggregate whose FUNC does not
		// decide its path — the input type does. Kept ahead of the switch,
		// behind one bool, so the ordinary MIN/MAX arm below stays exactly
		// what it was for every scalar type (#426).
		if h.hasBoxedMinMax && i < len(h.aggBoxedMinMax) && h.aggBoxedMinMax[i] {
			idx := h.aggColIdx[i]
			// extraState can be short on a state adopted from a clone that
			// resolved differently; a missing slot means no value observed,
			// which finalizes NULL — never an index panic on the emit path.
			if idx < 0 || i >= len(ext.extraState) {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			if st, ok := ext.extraState[i].(*containerMinMaxState); ok {
				before := st.memBytes()
				st.observe(v, row)
				h.extraStateBytes += st.memBytes() - before
			}
			continue
		}
		switch agg.Func {
		case AggCountDistinct:
			// COUNT(DISTINCT): hash the value, add to set
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			if ds := ext.distinctSets[i]; ds.ints != nil {
				if ds.addInt(intColValue(v, row)) {
					h.distinctBytes += 16
				}
			} else {
				h.keyBuf = appendColumnValue(h.keyBuf[:0], v, row, v.Type)
				// addStr probes zero-copy and copies into the set's arena
				// only on insert — no per-row string allocation.
				if ds.addStr(h.keyBuf) {
					h.distinctBytes += int64(len(h.keyBuf)) + 48
				}
			}

		case AggStringAgg:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			state := ext.extraState[i].(*stringAggState)
			state.parts = append(state.parts, fmt.Sprint(v.GetValue(row)))

		case AggBoolAnd, AggBoolOr:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			val := v.GetValue(row)
			boolVal := false
			switch tv := val.(type) {
			case bool:
				boolVal = tv
			case int64:
				boolVal = tv != 0
			case float64:
				boolVal = tv != 0
			}
			// The first non-NULL input seeds the state; nil means "no
			// input yet" so an all-NULL group finalizes to NULL (SQL's
			// rule). mergeSinkState and the finalize write share the
			// convention.
			if current, ok := ext.extraState[i].(bool); ok {
				if agg.Func == AggBoolAnd {
					ext.extraState[i] = current && boolVal
				} else {
					ext.extraState[i] = current || boolVal
				}
			} else {
				ext.extraState[i] = boolVal
			}

		case AggStddev, AggVariance, AggStddevPop, AggVarPop, AggVarState:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			extract := h.aggF64Extract[i]
			if extract == nil {
				continue
			}
			ext.extraState[i].(*varianceState).update(extract(v, row))

		case AggVarStateMerge:
			// One input row is one upstream partial's encoded (count, mean,
			// M2) triple. Combining them pairwise is the whole point of the
			// column: re-aggregating finished STDDEV values here would be
			// the standard deviation of a handful of near-identical numbers.
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			s, ok := v.GetValue(row).(string)
			if !ok {
				continue
			}
			if partial, ok := decodeVarianceState(s); ok {
				ext.extraState[i].(*varianceState).merge(&partial)
			}

		case AggApproxDistinct:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			if ds := ext.distinctSets[i]; ds.ints != nil {
				if ds.addInt(intColValue(v, row)) {
					h.distinctBytes += 16
				}
			} else {
				h.keyBuf = appendColumnValue(h.keyBuf[:0], v, row, v.Type)
				// addStr probes zero-copy and copies into the set's arena
				// only on insert — no per-row string allocation.
				if ds.addStr(h.keyBuf) {
					h.distinctBytes += int64(len(h.keyBuf)) + 48
				}
			}

		case AggCovarStateMerge:
			// One input row is one upstream partial's encoded sextuple —
			// combined pairwise, never re-correlated. A CORR of per-task
			// CORR values is the correlation of a handful of numbers that
			// have nothing to do with the question.
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			s, ok := v.GetValue(row).(string)
			if !ok {
				continue
			}
			if partial, ok := decodeCovarianceState(s); ok {
				ext.extraState[i].(*covarianceState).merge(&partial)
			}

		case AggCorr, AggCovarSamp, AggCovarPop, AggCovarState:
			idx1 := h.aggColIdx[i]
			idx2 := h.aggColIdx2[i]
			if idx1 < 0 || idx2 < 0 {
				continue
			}
			v1 := b.Columns[idx1]
			v2 := b.Columns[idx2]
			if v1.Nulls.IsNullFast(row) || v2.Nulls.IsNullFast(row) {
				continue
			}
			e1, e2 := h.aggF64Extract[i], h.aggF64Extract2[i]
			if e1 == nil || e2 == nil {
				continue
			}
			ext.extraState[i].(*covarianceState).update(e1(v1, row), e2(v2, row))

		case AggPercentileCont, AggPercentileDisc, AggMedian:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			extract := h.aggF64Extract[i]
			if extract == nil {
				continue
			}
			ext.extraState[i].(*collectState).values = append(ext.extraState[i].(*collectState).values, extract(v, row))

		case AggMode:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			extract := h.aggF64Extract[i]
			if extract == nil {
				continue
			}
			ext.extraState[i].(*collectState).values = append(ext.extraState[i].(*collectState).values, extract(v, row))

		case AggOhlcv, AggOhlcvState:
			// ohlcv(ts, price, volume). InputCol is the PRICE, InputCol2 the
			// instant, InputCol3 the volume — see logical.parseAggExtraArgs
			// for why the arguments are repointed onto MIN_BY's slots.
			//
			// A row is SKIPPED when ANY of the three is NULL. That is
			// PostgreSQL's rule for a multi-argument aggregate, measured on
			// 17.11: regr_count(y,x) over (1,1),(2,NULL),(NULL,3),(4,4) is 2.
			px, tsi, voli := h.aggColIdx[i], h.aggColIdx2[i], h.aggColIdx3[i]
			if px < 0 || tsi < 0 || voli < 0 {
				continue
			}
			pv, tv, vv := b.Columns[px], b.Columns[tsi], b.Columns[voli]
			if pv.Nulls.IsNullFast(row) || tv.Nulls.IsNullFast(row) || vv.Nulls.IsNullFast(row) {
				continue
			}
			ext.extraState[i].(*ohlcvState).observe(
				ohlcvReader{tsVec: tv, pxVec: pv, volVec: vv}, row)

		case AggOhlcvStateMerge:
			// One input row is one upstream partial's encoded bar. Folding
			// them is the whole point of the column: re-aggregating FINISHED
			// bars here would take a MAX of two ROWs, which is not a bar.
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			s, ok := v.GetString(row)
			if !ok {
				continue
			}
			// One step, shared with MergeOhlcvStates, so the merge stage and
			// the encoded/encoded face cannot drift apart (round-2 P3).
			absorbEncodedOhlcvState(ext.extraState[i].(*ohlcvState), s)

		case AggMinBy, AggMaxBy:
			idx1 := h.aggColIdx[i]
			idx2 := h.aggColIdx2[i]
			if idx1 < 0 || idx2 < 0 {
				continue
			}
			v1 := b.Columns[idx1] // return column
			v2 := b.Columns[idx2] // comparison column
			if v1.Nulls.IsNullFast(row) || v2.Nulls.IsNullFast(row) {
				continue
			}
			extract2 := h.aggF64Extract2[i]
			if extract2 == nil {
				continue
			}
			state := ext.extraState[i].(*minMaxByState)
			cmpVal := extract2(v2, row)
			if !state.hasValue ||
				(state.isMin && kernel.CompareFloat64(cmpVal, state.bestCmp) < 0) ||
				(!state.isMin && kernel.CompareFloat64(cmpVal, state.bestCmp) > 0) {
				before := boxContainerMemBytes(state.bestVal)
				state.hasValue = true
				state.bestCmp = cmpVal
				state.bestVal = v1.GetValue(row)
				h.extraStateBytes += boxContainerMemBytes(state.bestVal) - before
			}

		default:
			updater := h.batchUpdaters[i]
			if updater == nil {
				continue
			}
			idx := h.aggColIdx[i]
			if idx >= 0 {
				updater(&ext.accs[i], b.Columns[idx], row)
			} else {
				// COUNT(*) — pass nil vec
				updater(&ext.accs[i], nil, row)
			}
		}
	}
}
