// This file holds clone adoption and partial aggregate state merging.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// CloneSink returns a new HashAggregate with the same configuration but fresh state.
// Used by parallel pipeline execution: each worker gets its own cloned sink.
func (h *HashAggregate) CloneSink() SinkSource {
	clone := &HashAggregate{
		GroupByCols: h.GroupByCols,
		// A clone EMITS: the morsel-parallel path finalizes each one and the
		// primary merges their state, so a clone that did not carry the
		// published names would publish its keys under their hidden slots
		// and every consumer above would read NULL.
		GroupByOutNames: h.GroupByOutNames,
		GroupByAll:      h.GroupByAll, // clones must resolve the same key set, not fall into the scalar path
		GroupByColIdx:   h.GroupByColIdx,
		Aggs:            h.Aggs,
		NullGroupCols:   h.NullGroupCols,
		GroupingSets:    h.GroupingSets,
		// A clone emits, so it needs the GROUPING(...) columns too: without
		// them its output schema is one column short of the primary's and
		// the merge reads past the end.
		GroupingCalls:     h.GroupingCalls,
		GroupingCallNames: h.GroupingCallNames,
		// No spill manager — partial aggregates are small enough
		strNullGroupIdx: -1, // defensive: Init sets it, but the zero value is a VALID slot
	}
	// Partitioned clones own disjoint 1/k slices of the key space, so the
	// NDV presize divides cleanly across them (the pipeline sets the
	// divisor before cloning). Non-partitioned clones can each see the
	// full key set — presizing every clone to full NDV would k× the
	// table memory, so they keep organic growth.
	if h.cloneNDVDivisor > 1 {
		clone.GroupNDVHint = h.GroupNDVHint
		clone.cloneNDVDivisor = h.cloneNDVDivisor
	}
	// A clone of a bounded sink is bounded: the owner tears the whole
	// aggregate down on the cap, clones included. Passing the cap unchanged
	// over-states what one clone can hold (clones own disjoint 1/k slices),
	// which keeps the ceiling an upper bound — the safe direction.
	clone.epochByteCap = h.epochByteCap
	// Same for the row bound: a clone consumes a SUBSET of the parent's
	// input, so the parent's bound bounds it too. Deliberately not divided
	// by the clone count — morsel clones take dynamic row slices, and a
	// bound that over-states only ever keeps the adaptive path.
	clone.inputRowBound = h.inputRowBound
	return clone
}

// MergeSink merges another HashAggregate's partial state into this one.
// Called after all parallel workers finish to combine partial aggregates.
//
// After the state merge, h's group footprint has grown by the clone's
// state; reconcileGroupMemory recharges h so the shared-tracker reservation
// follows the state (morsel-parallel clones charge a tracking-only
// SpillManager view; their own charge is released at clone Close, AFTER
// this recharge, so the tracker never under-reports in between). No-op when
// h.Spill is nil — the single-process planner path.
func (h *HashAggregate) MergeSink(other SinkSource) {
	o := other.(*HashAggregate)
	// Partitioned-disjoint adoption: keys never overlap across sinks, so
	// re-inserting the clone's groups into the primary's table (a serial
	// O(total groups) rehash) is pure waste — keep the clone's state and
	// stream it during Next().
	if h.PartitionedDisjoint && o.PartitionedDisjoint {
		// The clone keeps its state AND its drained runs: normally
		// mergeSinkState hands drainedRuns to the primary, but an adopted
		// partition finalizes its own runs itself — without this transfer
		// they were orphaned and every drained group silently vanished.
		o.partialSpillFiles = append(o.partialSpillFiles, o.drainedRuns...)
		o.drainedRuns = nil
		h.adoptedPartitions = append(h.adoptedPartitions, o)
		return
	}
	h.mergeSinkState(o)
	h.reconcileGroupMemory()
}

func (h *HashAggregate) mergeSinkState(o *HashAggregate) {
	// EVERY run the clone wrote belongs to the primary now, whatever merge
	// path the remaining in-memory state takes — and there are two lists,
	// because there are two ways a clone drains.
	//
	// drainedRuns is the PartialDrainBytes bound, the one a clone is designed
	// to take. partialSpillFiles is what spillPartialState appends to, and a
	// clone reaches THAT through SpillSome: a clone registers as an accounted
	// operator like any other aggregate (Consume does it whenever Spill is
	// non-nil and canUseExternalMerge holds — a tracking-only view is still
	// non-nil), so SpillManager.RequestRelief can ask it for bytes on a peer's
	// behalf and it drains a whole run in answer.
	//
	// Only the first list was transferred. The clone's own Close then dropped
	// the second, and every group in those runs vanished from the answer with
	// no error anywhere — 5000 rows in, 1100 out, on a shape whose totals a
	// COUNT(*) makes obvious and a SUM does not (#790). The partitioned
	// adoption branch above does not have the bug: it keeps the clone and
	// finalizes it, which is why it moves drainedRuns INTO partialSpillFiles
	// rather than away from them.
	if len(o.drainedRuns) > 0 {
		h.partialSpillFiles = append(h.partialSpillFiles, o.drainedRuns...)
		o.drainedRuns = nil
	}
	if len(o.partialSpillFiles) > 0 {
		h.partialSpillFiles = append(h.partialSpillFiles, o.partialSpillFiles...)
		o.partialSpillFiles = nil
	}
	// The LEGACY raw-row pair, for the same reason. A clone that took the
	// raw-row branch owns spilled row FILES and the in-memory buffer that has
	// not reached one yet, and Close removes the files and drops the buffer —
	// so leaving them here loses every row in them, not merely a partial
	// aggregate. The bytes ride along because the primary now owns the
	// tracker charge for them and its own flush threshold has to see them.
	//
	// Production cannot reach this today: a clone's tracking-only view makes
	// ShouldSpillFor false, SpillSome refuses anything that is not
	// canUseExternalMerge, and DrainOnHeapPressure refuses both, so the
	// pressure branch never fires for a clone. The TEST knobs on this branch
	// do reach it — forcedDrainDue ignores ShouldSpillFor — and so would any
	// future change that gives a clone a spill-capable manager. It is 49% row
	// loss when reached (4000 rows in, 2048 out, at Workers=8 with the drain
	// forced and partitioned aggregation off), and the invariance oracle runs
	// with WADJET_PARTITIONED_AGG=0, which is the arm that takes this branch.
	if len(o.spillFiles) > 0 {
		h.spillFiles = append(h.spillFiles, o.spillFiles...)
		o.spillFiles = nil
	}
	if len(o.spillBuffer) > 0 {
		h.spillBuffer = append(h.spillBuffer, o.spillBuffer...)
		h.spillBufferBytes += o.spillBufferBytes
		o.spillBuffer, o.spillBufferBytes = nil, 0
	}

	// When the parent (h) was never fed a batch — runParallel's warmup batch
	// gets consumed when present, but if the warmup row group is fully
	// filtered out (e.g. shipdate range outside the data window) then
	// resolveIndices was never called on h. That leaves h.groupColTypes
	// empty, which forces outputSchema to fall back to TypeString for every
	// GROUP BY column. The string-typed output column then stores group
	// keys (int64 from gs.keyValues) as their decimal string form, and the
	// downstream HAVING / projection comparison against an int literal
	// silently produces zero matches even though the merged accumulator
	// had the right value. Inherit the worker's resolved schema metadata
	// on the first non-empty merge so the parent's output schema matches
	// the workers'.
	if len(h.groupColTypes) == 0 && len(o.groupColTypes) > 0 {
		h.groupColTypes = o.groupColTypes
		h.groupColMeta = o.groupColMeta
		if len(h.groupColIdx) == 0 {
			h.groupColIdx = o.groupColIdx
		}
		if len(h.aggColIdx) == 0 {
			h.aggColIdx = o.aggColIdx
		}
		if len(h.aggColIdx2) == 0 {
			h.aggColIdx2 = o.aggColIdx2
		}
		// GroupByAll (DISTINCT) parents have no plan-time column list at
		// all — the clones resolved it from the first batch's schema.
		// Without inheriting it, outputSchema emits zero group columns
		// while the merged states hold full key tuples (index panic).
		if len(h.GroupByCols) == 0 && len(o.GroupByCols) > 0 {
			h.GroupByCols = o.GroupByCols
			// The key POSITIONS travel with the list they address (#1022).
			h.GroupByColIdx = o.GroupByColIdx
		}
	}
	// Same inheritance for the aggregate inputs, and for the same reason:
	// MIN/MAX/MIN_BY/MAX_BY re-declare their output from the type (and, for
	// DECIMAL and the containers, the full column metadata) they observed at
	// Consume. A parent that consumed nothing observed nothing, so without
	// this it emits the value under whatever the planner guessed. The
	// GROUP BY block above cannot carry it: a SCALAR aggregate has no group
	// columns at all, and MIN_BY's ungrouped form is exactly the shape the
	// parallel merge produces.
	if len(h.aggInputTypes) == 0 && len(o.aggInputTypes) > 0 {
		h.aggInputTypes = o.aggInputTypes
		h.aggInputMeta = o.aggInputMeta
		h.aggInputDecScale = append([]int(nil), o.aggInputDecScale...)
		h.aggBoxedMinMax = o.aggBoxedMinMax
		h.hasBoxedMinMax = o.hasBoxedMinMax
		if len(h.aggColIdx) == 0 {
			h.aggColIdx = o.aggColIdx
		}
	}
	// A clone that latched a cross-scale batch hands the finding to the
	// primary: the flag is a property of the INPUT the operator saw, and the
	// primary saw none of it.
	if o.decScaleConflict {
		h.decScaleConflict = true
	}
	// Reconcile DECIMAL scale on every clone merge: prefer the first nonzero scale
	// because a clone with no non-NULL input reports zero (#455). Inherited metadata
	// must be copied, not aliased, so reconciliation cannot mutate that clone.
	// Latch differing nonzero scales here as well as in Consume: separate clones may
	// each observe only one scale and otherwise hide the grouped conflict (#685).
	// Known boundary: zero also represents real DECIMAL(p,0), so scale 0 followed by 2
	// upgrades here (5 + 1.00 becomes 1.05), while ungrouped adoptDecScale conflicts.
	// The first-nonzero rule cannot distinguish absent scale from genuine zero (#455).
	// See docs/internals/grouped-decimal-clone-scale-merge.md for the design.
	for i := range h.aggInputDecScale {
		if i >= len(o.aggInputDecScale) {
			continue
		}
		switch {
		case h.aggInputDecScale[i] == 0:
			h.aggInputDecScale[i] = o.aggInputDecScale[i]
		case o.aggInputDecScale[i] != 0 && o.aggInputDecScale[i] != h.aggInputDecScale[i]:
			h.decScaleConflict = true
		}
	}

	// Scalar aggregate fast path: merge batch accumulators directly.
	// The parent (h) is created by CloneSink and never consumes a batch
	// itself, so isScalarAgg / scalarAccs / batchAggKernels stay zero on h
	// even when the workers (o) all resolved as scalar. Adopt the worker's
	// scalar wiring on the first scalar merge so Next() takes the scalar
	// finalization path instead of falling through to the empty-input
	// "emit zeros" fallback.
	if o.isScalarAgg {
		if !h.isScalarAgg {
			h.isScalarAgg = true
			h.scalarAccs = make([]kernel.Accumulator, len(o.scalarAccs))
			h.batchAggKernels = o.batchAggKernels
			h.aggColIdx = o.aggColIdx
		}
		for i := range h.scalarAccs {
			h.scalarAccs[i].Merge(&o.scalarAccs[i])
		}
		return
	}

	// Int-keyed SoA fast path: merge flat accumulators directly without
	// materializing per-group Accumulator structs or migrating to generic map.
	if h.useIntGroupKey && o.useIntGroupKey && h.intFlatAccs != nil && o.intFlatAccs != nil {
		h.mergeIntGroupSoA(o)
		return
	}

	// Packed-key SoA fast path: merge via one probe per source group.
	if h.usePackedGroupKey && o.usePackedGroupKey && h.intFlatAccs != nil && o.intFlatAccs != nil {
		h.mergePackedGroupSoA(o)
		return
	}

	// Empty-primary adoption. A primary that just whole-state-drained
	// (spillFullState → resetGroupStateAfterSpill) has intFlatAccs == nil
	// until its next Consume lazily rebuilds — a barrier merge landing in
	// that window used to fall through to the migrate path below and
	// materialize BOTH sides into the generic map. On SF100 Q17 (GROUP BY
	// l_partkey, ~20M keys, 8 clone partials) that fallback allocated a
	// second full copy of every partial (migrateToGenericMap 14.2 GB +
	// materializeFlatAccums 9.6 GB cum in the worker heap profiles) at the
	// moment memory was already critical, and one such merge poisoned all
	// later ones by nil'ing the SoA arrays (2026-07-03 postmortem,
	// morsel-agg-partials-v2.md §3.C). When the primary is empty, adopting
	// the clone's state wholesale is O(1) and mode-preserving.
	if h.groupCount() == 0 && o.groupCount() > 0 {
		h.adoptStateFrom(o)
		return
	}

	// Drain-to-runs fallback for simple aggregates: instead of migrating an
	// SoA-capable side into the generic map, write its state as canonical
	// partial-state runs (sorted by binary sortKey — the format is
	// instance-independent) and let Finalize's existing k-way merge combine
	// them with the primary's in-memory state. O(state) disk I/O instead of
	// O(state) heap at the barrier. Falls through to the legacy in-memory
	// merge on any write error — correctness never depends on disk.
	if h.simpleAggs && len(h.GroupingSets) == 0 && o.canUseExternalMerge() &&
		h.Spill != nil && h.Spill.SpillDir() != "" {
		if paths, err := o.drainStateToRuns(h.Spill.SpillDir()); err == nil {
			h.partialSpillFiles = append(h.partialSpillFiles, paths...)
			return
		}
	}

	// Normalize both sides to the generic map path so merge is uniform.
	h.migrateToGenericMap()
	o.migrateToGenericMap()

	// Typed-generic sinks don't maintain strGroupIndex during consume;
	// the dedup loop below probes it, so rebuild from serializedKeys.
	h.ensureStrGroupIndexForMerge()

	// migrateToGenericMap → materializeFlatAccums on both sides, so every
	// group's extras is allocated and extras.accs is populated. Iterate
	// strGroupStates, not o.keys — deferred-boxing generic sinks never
	// populate o.keys (serializedKeys carries the identity).
	for i := range o.strGroupStates {
		key := o.serializedKeys[i]
		oGS := o.strGroupStates[i]
		oExt := oGS.extras

		newIdx := int32(len(h.strGroupStates))
		var gsIdx int32
		var found bool
		if int32(i) == o.strNullGroupIdx {
			gsIdx, found = h.mergeNullGroupSlot(key, newIdx)
		} else {
			gsIdx, found = h.strGroupIndex.GetOrInsert([]byte(key), newIdx)
		}
		if found {
			gs := h.strGroupStates[gsIdx]
			ext := gs.extras
			for j := range ext.accs {
				ext.accs[j].Merge(&oExt.accs[j])
			}
			// STDDEV/VARIANCE/CORR/COVAR/STRING_AGG/MEDIAN/PERCENTILE/
			// MODE/MIN_BY/MAX_BY/BOOL_AND/BOOL_OR keep their state in
			// extraState, not in accs. Without this merge a group split
			// across morsel-parallel clones kept only the FIRST clone's
			// partial — the primary starts empty, adopts clone 1 wholesale
			// (adoptStateFrom), and every later clone's state was dropped
			// here. STDDEV(o_totalprice) over 15000 rows answered from
			// 3750 of them (#339): a plausible-looking number, wrong in the
			// fourth digit, that no row count or NULL check can catch.
			h.mergeExtraState(ext, oExt)
			// COUNT(DISTINCT) state lives in distinctSets, not accs. Without
			// this merge, parallel workers' partial distinct sets aren't
			// combined and COUNT(DISTINCT) under-counts whenever a group is
			// split across workers (test: Q16 missing the cnt=6 row at
			// position 2 because half the suppliers were on a different
			// worker than the other half).
			for j := range ext.distinctSets {
				if oExt.distinctSets == nil || j >= len(oExt.distinctSets) || oExt.distinctSets[j] == nil {
					continue
				}
				if ext.distinctSets[j] == nil {
					ext.distinctSets[j] = oExt.distinctSets[j]
					h.distinctBytes += oExt.distinctSets[j].memBytes()
					continue
				}
				before := ext.distinctSets[j].memBytes()
				ext.distinctSets[j].mergeFrom(oExt.distinctSets[j])
				h.distinctBytes += ext.distinctSets[j].memBytes() - before
			}
		} else {
			h.strGroupStates = append(h.strGroupStates, oGS)
			if oExt.accs != nil {
				h.extrasAccsCount += int64(len(oExt.accs))
			}
			if oExt.extraState != nil {
				h.extraStateBytes += int64(len(oExt.extraState)) * 80
				// The flat charge above is sized for a scalar box; a
				// container MIN/MAX's retained value (or MIN_BY/MAX_BY's
				// bestVal, which shares the same container-shaped output
				// per aggSpecOutputType) is a whole copied structure and
				// needs its own accounting on top, the same as observe/
				// merge do for a group already in h.
				for _, st := range oExt.extraState {
					switch st := st.(type) {
					case *containerMinMaxState:
						h.extraStateBytes += st.memBytes()
					case *minMaxByState:
						h.extraStateBytes += boxContainerMemBytes(st.bestVal)
					}
				}
			}
			for _, ds := range oExt.distinctSets {
				h.distinctBytes += ds.memBytes()
			}
			h.keys = append(h.keys, oExt.keyValues)
			h.serializedKeys = append(h.serializedKeys, key)
			h.serializedKeyBytes += int64(len(key))
		}
	}
}

// mergeExtraState folds one group's extraState from a clone (src) into the
// primary's (dst). Every kind is combined by its own algebra: the variance
// and covariance families pairwise (see varianceState.merge), the
// value-collecting kinds by concatenation, MIN_BY/MAX_BY by keeping the
// better comparison value, and the boolean kinds by their operator.
//
// Missing or short slices are tolerated rather than indexed blindly: a
// clone that never consumed a row for this group has no state to give.
func (h *HashAggregate) mergeExtraState(dst, src *groupStateExtras) {
	if dst == nil || src == nil || src.extraState == nil || dst.extraState == nil {
		return
	}
	for j := range dst.extraState {
		if j >= len(src.extraState) || src.extraState[j] == nil || j >= len(h.Aggs) {
			continue
		}
		switch s := src.extraState[j].(type) {
		case *varianceState:
			if d, ok := dst.extraState[j].(*varianceState); ok {
				d.merge(s)
			} else {
				dst.extraState[j] = s
			}
		case *covarianceState:
			if d, ok := dst.extraState[j].(*covarianceState); ok {
				d.merge(s)
			} else {
				dst.extraState[j] = s
			}
		case *stringAggState:
			// Concatenation order across parallel clones is the order the
			// clones merge in, which is as defined as STRING_AGG without an
			// ORDER BY ever is.
			if d, ok := dst.extraState[j].(*stringAggState); ok {
				d.parts = append(d.parts, s.parts...)
			} else {
				dst.extraState[j] = s
			}
		case *collectState:
			// PERCENTILE/MEDIAN/MODE sort or tally the pooled values at
			// finalize, so appending is exact.
			if d, ok := dst.extraState[j].(*collectState); ok {
				d.values = append(d.values, s.values...)
			} else {
				dst.extraState[j] = s
			}
		case *ohlcvState:
			d, ok := dst.extraState[j].(*ohlcvState)
			if !ok || d == nil {
				dst.extraState[j] = s
				continue
			}
			if d.n == 0 {
				d.dom = s.dom
				if len(d.fields) != len(OhlcvFieldNames) {
					d.fields = s.fields
				}
			}
			d.merge(s)
		case *minMaxByState:
			d, ok := dst.extraState[j].(*minMaxByState)
			if !ok {
				dst.extraState[j] = s
				h.extraStateBytes += boxContainerMemBytes(s.bestVal)
				continue
			}
			if !s.hasValue {
				continue
			}
			if !d.hasValue || (d.isMin && kernel.CompareFloat64(s.bestCmp, d.bestCmp) < 0) || (!d.isMin && kernel.CompareFloat64(s.bestCmp, d.bestCmp) > 0) {
				before := boxContainerMemBytes(d.bestVal)
				d.bestVal, d.bestCmp, d.hasValue = s.bestVal, s.bestCmp, true
				h.extraStateBytes += boxContainerMemBytes(d.bestVal) - before
			}
		case *containerMinMaxState:
			d, ok := dst.extraState[j].(*containerMinMaxState)
			if !ok {
				dst.extraState[j] = s
				h.extraStateBytes += s.memBytes()
				continue
			}
			before := d.memBytes()
			d.merge(s)
			h.extraStateBytes += d.memBytes() - before
		case bool:
			d, ok := dst.extraState[j].(bool)
			if !ok {
				dst.extraState[j] = s
				continue
			}
			if h.Aggs[j].Func == AggBoolAnd {
				dst.extraState[j] = d && s
			} else {
				dst.extraState[j] = d || s
			}
		}
	}
}
