// This file holds flat accumulators, count-array planning, and scatter dispatch.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// scatterBatchAggs is Phase 2 for every SoA consume path: extend the flat
// accumulators to the batch's final group count in ONE step per array, then
// run one typed scatter pass per aggregate.
//
// The growth used to happen one group at a time inside the hash-probe loop
// (flatAccumArrays.appendGroup, 11 nil-checks + up to 3 appends per NEW group
// PER AGGREGATE). In the near-unique-key regime that is ~33 branch
// evaluations per row for three aggregates, all of it redundant: the arrays
// are append-in-place off-heap slices, so the extension itself is a length
// bump plus a memclr.
//
// Aggregates whose entire accumulator is a SHARED count array (a duplicate
// COUNT over an identical non-null predicate) are skipped — the owner's pass
// already wrote the array they read. SUM/AVG sharers still run, through the
// count-free kernels.
func (h *HashAggregate) scatterBatchAggs(nGroups int, b *batch.RecordBatch, gi []int32, sel []uint32, iterLen int) {
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].growTo(nGroups)
	}
	for i := range h.Aggs {
		fa := &h.intFlatAccs[i]
		ci := h.aggColIdx[i]
		if h.Aggs[i].Func == AggCount && fa.count == nil {
			continue // shared count IS this aggregate's whole state
		}
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
		}
	}
}

// appendFlatAccumSlot extends every flat accumulator by exactly one
// zero-initialized slot. Used by the two paths that mint a group outside the
// batch-oriented growth above: processRow (generic per-row insert) and
// strNullGroupSlot (created mid-loop, before the batch's group count is
// known).
func (h *HashAggregate) appendFlatAccumSlot() {
	for ai := range h.intFlatAccs {
		fa := &h.intFlatAccs[ai]
		fa.growTo(flatAccumLen(fa) + 1)
	}
}

// flatAccumLen reports the group-slot length of a flat accumulator. count is
// the natural probe, but MIN/MAX aggregates have none and count-sharing
// aggregates borrow one, so fall back to the first live array.
func flatAccumLen(fa *flatAccumArrays) int {
	switch {
	case fa.count != nil:
		return len(fa.count)
	case fa.sumI64 != nil:
		return len(fa.sumI64)
	case fa.sumF64 != nil:
		return len(fa.sumF64)
	case fa.sumDec != nil:
		return len(fa.sumDec)
	case fa.hasMin != nil:
		return len(fa.hasMin)
	case fa.hasMax != nil:
		return len(fa.hasMax)
	}
	return 0
}

// initFlatAccums initializes SoA accumulator arrays for the intGroupKey fast path.
// Called once from resolveIndices when useIntGroupKey is true.
// offheapReg returns the aggregate's off-heap registry, creating it on
// first use when the platform path is available. nil (heap fallback in
// every memory.Offheap constructor) otherwise.
func (h *HashAggregate) offheapReg() *memory.OffheapRegistry {
	if h.offheap == nil && memory.OffheapAvailable() {
		h.offheap = memory.NewOffheapRegistry()
	}
	return h.offheap
}

// aggNeedsCount reports whether an aggregate's flat state includes a row
// count. MIN/MAX do not: no scatter kernel writes their count[], and every
// consumer (finalizeKernelAcc, writeAccToColumn, the partial-spill
// emitAcc/readAcc format) drives MIN/MAX purely off HasMin/HasMax. The array
// was 8 bytes per group per aggregate of permanent zeroes.
func aggNeedsCount(fn AggFunc) bool {
	return fn == AggSum || fn == AggAvg || fn == AggCount
}

// planCountArrays decides, per aggregate, which aggregate's count[] it reads.
// Result[i] == i means "owns its own"; result[i] == j < i means "shares j's".
//
// Two aggregates may share a count array only when every row increments both
// counts or neither — i.e. their count kernels run over an identical
// predicate. That holds exactly when:
//
//   - both are COUNT(*) (no input column: every row with a live group index
//     counts), or
//   - both read the SAME input column AND both count kernels are guaranteed
//     to fire for that column's type.
//
// The type guard matters: scatterFlatAggUpdate's SUM/AVG dispatch has no case
// for e.g. Bool or String, so SUM over such a column silently increments
// nothing while COUNT over it increments every non-null row. Restricting
// sharing to the numeric set the SUM/AVG switches actually handle keeps the
// two predicates identical.
//
// MIN/MAX never participate — they have no count at all (aggNeedsCount).
//
// NOT shared: aggregates over different columns, even when the data happens
// to have no nulls in either. Null-ness is a per-batch property, so that
// equality isn't provable at plan time. ClickBench Q33's three aggregates
// (COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)) fall in exactly that
// bucket and each keep their own count.
func (h *HashAggregate) planCountArrays(b *batch.RecordBatch) []int32 {
	plan := make([]int32, len(h.Aggs))
	for i := range plan {
		plan[i] = int32(i)
	}
	// classOf returns a shareable class key, or ok=false for "never shares".
	classOf := func(i int) (int, bool) {
		agg := h.Aggs[i]
		if !aggNeedsCount(agg.Func) {
			return 0, false
		}
		ci := h.aggColIdx[i]
		if ci < 0 {
			// COUNT(*): counts every row of every group.
			return -1, agg.Func == AggCount
		}
		if agg.Func != AggCount && !isFlatSumType(b.Columns[ci].Type) {
			return 0, false
		}
		return ci, true
	}
	owners := make(map[int]int32, len(h.Aggs))
	for i := range h.Aggs {
		key, ok := classOf(i)
		if !ok {
			continue
		}
		if owner, seen := owners[key]; seen {
			plan[i] = owner
			continue
		}
		owners[key] = int32(i)
	}
	return plan
}

// aggIntExact reports whether an aggregate over an INTEGER column accumulates
// in the Int128 carrier because PostgreSQL answers it in numeric (#784).
//
//	SUM(int2/int4) -> bigint    exact in int64; here only when the DECLARATION
//	                            says numeric, which is AVG's decomposed SUM leg
//	SUM(int8)      -> numeric   an int64 sum WRAPS past 2^63
//	AVG(int*)      -> numeric   the float64 mean loses integer digits past 2^53
//
// Taken from the live server (`pg_typeof(sum(c_i32))` = bigint,
// `pg_typeof(sum(c_i64))` = numeric, `pg_typeof(avg(c_i32))` = numeric): the
// two SUM rules differ because int4's sum has a wider integer type to grow
// into and int8's does not. WHICH input types those rules cover is
// IntegerAccOutputType's answer, not a list repeated here: the window
// operator and both planner declarations ask the same function, and a list
// that drifted from it would be a carrier disagreeing with a declaration.
//
// It is the ONE predicate every accumulation path consults, so the flat
// scatter arrays, the row updaters, the batch kernels, the spill run's latched
// encodings and the output schema cannot disagree about which carrier a value
// is in — the disagreement class ADR-0027 decision 3 exists for.
// The DECLARATION is the second half of the test, and it is what keeps this
// off the plumbing. A MERGE stage re-aggregates a partial COUNT as a SUM over
// an int64 column (buildFragmentAggregate) and declares int64 for it: that
// column is the fold's row count, not a user's SUM(int8), and giving it the
// numeric carrier would put the total in SumDec while the emit reads SumI64 —
// the two-carrier disagreement in its purest form. So the carrier follows the
// declared output type, and a planner that could not resolve the input at all
// keeps the float64 it always had, with the accumulator agreeing.
func aggIntExact(agg AggColumn, typ batch.TypeID) bool {
	// The INT32 class is in scope too, and only the DECLARATION lets it in. A
	// user's SUM(int4) declares bigint and keeps its int64 array; the SUM LEG
	// of a decomposed AVG(int4) declares numeric, because AVG(int*) is numeric
	// and its sum has to be the same carrier. Deciding that from the input
	// type alone made the partial's carrier depend on whether a task SAW A
	// BATCH: one that did emitted int64 (the SUM(int4) rule) and one that did
	// not emitted the spec's DECIMAL, and ADR-0010's shuffle type guard
	// refused the read. `SELECT AVG(c_i32) FROM typemx WHERE id < 3` — any
	// predicate selective enough to leave a scan task empty — was a hard DAG
	// failure on a query the single-process path answers (#784, review round
	// 2 B2).
	if agg.Func != AggSum && agg.Func != AggAvg {
		return false
	}
	if !integerAccInput(typ) {
		return false
	}
	return agg.OutputType == parquet.TypeDecimal
}

// isFlatSumType reports whether scatterFlatAggUpdate's SUM/AVG dispatch has a
// case for this column type (and therefore increments count for it).
func isFlatSumType(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration,
		batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate,
		batch.TypeFloat64, batch.TypeFloat32, batch.TypeDecimal:
		return true
	}
	return false
}

func (h *HashAggregate) initFlatAccums(b *batch.RecordBatch) {
	// Flat accumulator arrays: same sizing principle as the group-state pool
	// above. InputRowHint overshoots for low-cardinality GROUP BY (Q12: 7
	// groups but 50M-row InputRowHint would preAlloc 2M × 8B × nAggs here).
	// Cap at 64K slots; ensureCapacity doubles organically for high-cardinality
	// aggregates.
	nAggs := len(h.Aggs)
	h.intFlatAccs = make([]flatAccumArrays, nAggs)
	const flatInitCap = 64 * 1024
	initCap := 4096
	reg := h.offheapReg()
	if h.InputRowHint > int64(initCap)*8 {
		est := int(h.InputRowHint / 8)
		if est > flatInitCap {
			est = flatInitCap
		}
		initCap = est
	}

	// Count-array plan: which aggregates need a count[] of their own, and
	// which can read another aggregate's. See planCountArrays for the
	// safety argument.
	countPlan := h.planCountArrays(b)

	for i, agg := range h.Aggs {
		fa := &h.intFlatAccs[i]
		fa.countFrom = countPlan[i]
		if fa.countFrom == int32(i) && aggNeedsCount(agg.Func) {
			fa.count = memory.Offheap[int64](reg, initCap)
		}

		ci := h.aggColIdx[i]
		if ci < 0 {
			continue // COUNT(*) only needs count
		}
		typ := b.Columns[ci].Type

		switch agg.Func {
		case AggSum, AggAvg:
			switch {
			// PostgreSQL answers SUM(int8) and AVG(int2/int4/int8) in
			// numeric, so those accumulate in the Int128 carrier at scale 0
			// (#784). The int64 sum they used to take WRAPS past 2^63 and the
			// float64 AVG loses integer digits past 2^53 — both silent. SUM
			// over the int32 class keeps its exact int64 array: it declares
			// bigint, which is what PostgreSQL declares, and no realistic row
			// count can overflow it.
			case aggIntExact(agg, typ):
				fa.sumDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				fa.decScale = 0
			case typ == batch.TypeFloat64 || typ == batch.TypeFloat32:
				fa.sumF64 = memory.Offheap[float64](reg, initCap)
				fa.isFloat = true
			case typ == batch.TypeDecimal:
				fa.sumDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				fa.decScale = b.Columns[ci].DecimalData.Scale
			case typ == batch.TypeInt64 || typ == batch.TypeTimestamp || typ == batch.TypeDuration:
				if agg.Func == AggAvg {
					// Overflow-safe AVG: float64 accumulation, matching
					// scatterFlatAggUpdate's AggAvg case and the row kernels.
					// Reached now only where #784's rule does NOT apply —
					// TIMESTAMP and DURATION, which are wadjet's own
					// int-backed types with no PostgreSQL integer to follow,
					// and an INT64 whose declared output is not numeric
					// (a MERGE stage's re-aggregated partial count).
					fa.sumF64 = memory.Offheap[float64](reg, initCap)
					fa.isFloat = true
				} else {
					fa.sumI64 = memory.Offheap[int64](reg, initCap)
				}
			default: // int32-class: exact int64 sum, cannot overflow
				fa.sumI64 = memory.Offheap[int64](reg, initCap)
			}
		case AggCount:
			// count[] is all we need
		case AggMin:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.minF64 = memory.Offheap[float64](reg, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.minDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				// The SUM/AVG arm above always set this; the min/max arms
				// did not, and loadAccFromFlat finalizes MinDec through
				// ToFloat64(DecScale) — so a scale-4 column would have
				// answered 10000x too large the moment the scatter started
				// writing anything at all.
				fa.decScale = b.Columns[ci].DecimalData.Scale
			default:
				fa.minI64 = memory.Offheap[int64](reg, initCap)
			}
			fa.hasMin = memory.Offheap[bool](reg, initCap)
		case AggMax:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.maxF64 = memory.Offheap[float64](reg, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.maxDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				fa.decScale = b.Columns[ci].DecimalData.Scale
			default:
				fa.maxI64 = memory.Offheap[int64](reg, initCap)
			}
			fa.hasMax = memory.Offheap[bool](reg, initCap)
		}
	}

	h.groupIndexBuf = make([]int32, batch.DefaultBatchSize)

	// The flags were just resolved from this batch's column types; latch them
	// now, because the drain that needs them may run after the arrays holding
	// them have been cleared (latchAggEncodings).
	h.latchAggEncodings()
}

// loadAccFromFlat fills dst from a flatAccumArrays row at index gi. countArr
// is the aggregate's resolved count array (countArrayOf) — nil for MIN/MAX,
// or the owner's array when this aggregate shares one. The
// SoA-direct counterpart to materializing per-group accs into extras.accs:
// callers build a stack-local Accumulator (which the compiler sees value-
// typed and proves doesn't escape) and pass it to a finalizer that doesn't
// retain it. At SF100 Q17 scale (20M groups) this saves a multi-GB heap
// pass through materializeFlatAccums.
func loadAccFromFlat(fa *flatAccumArrays, countArr []int64, gi int, dst *kernel.Accumulator) {
	dst.Count = 0
	if countArr != nil {
		dst.Count = countArr[gi]
	}
	dst.IsFloat = fa.isFloat
	dst.IsDecimal = fa.isDecimal
	dst.DecScale = fa.decScale
	dst.DecOverflow = fa.sumDecOverflow
	dst.IntOverflow = fa.sumIntOverflow
	if fa.sumI64 != nil {
		dst.SumI64 = fa.sumI64[gi]
	}
	if fa.sumF64 != nil {
		dst.SumF64 = fa.sumF64[gi]
	}
	if fa.sumDec != nil {
		dst.SumDec = fa.sumDec[gi]
	}
	if fa.minI64 != nil {
		dst.MinI64 = fa.minI64[gi]
		dst.HasMin = fa.hasMin[gi]
	}
	if fa.maxI64 != nil {
		dst.MaxI64 = fa.maxI64[gi]
		dst.HasMax = fa.hasMax[gi]
	}
	if fa.minF64 != nil {
		dst.MinF64 = fa.minF64[gi]
		dst.HasMin = fa.hasMin[gi]
	}
	if fa.maxF64 != nil {
		dst.MaxF64 = fa.maxF64[gi]
		dst.HasMax = fa.hasMax[gi]
	}
	if fa.minDec != nil {
		dst.MinDec = fa.minDec[gi]
		dst.HasMin = fa.hasMin[gi]
	}
	if fa.maxDec != nil {
		dst.MaxDec = fa.maxDec[gi]
		dst.HasMax = fa.hasMax[gi]
	}
}

// materializeFlatAccums converts SoA flat arrays back to per-group Accumulator
// structs for output (Next) and merge (MergeSink). Called once after all input
// is consumed. O(groups) — negligible compared to the O(rows) hot loop.
func (h *HashAggregate) materializeFlatAccums() {
	if h.intFlatAccs == nil {
		return
	}
	// This is the one funnel that CLEARS the flat arrays, so it is where the
	// spill format's encoding flags have to be preserved: a drain landing
	// after it (the migrating batch's, MergeSink's normalization) reads the
	// latch instead of the nil arrays.
	h.latchAggEncodings()
	nAggs := len(h.Aggs)
	// String GROUP BY and generic SoA use strGroupStates with SoA flat accumulators.
	if h.useStrGroupKey || h.useGenericSoA {
		for gi, gs := range h.strGroupStates {
			if gs == nil {
				// Deferred state (typed-generic / str paths): reify for
				// the migration/merge cold path.
				gs = h.gsPool.alloc()
				h.strGroupStates[gi] = gs
			}
			ext := gs.ensureExtras()
			if ext.accs == nil {
				ext.accs = make([]kernel.Accumulator, nAggs)
				h.extrasAccsCount += int64(nAggs)
			}
			for ai := range h.intFlatAccs {
				fa := &h.intFlatAccs[ai]
				// Resolve the SHARED count array before the bound check.
				// flatAccumLen probes an aggregate's OWN arrays, and a
				// count-sharing COUNT owns none of them (count nil by
				// definition, no sum, no min/max) — so it measured 0 for
				// every group, the guard below skipped it unconditionally,
				// and its accumulator stayed at the zero value. Sharing is
				// by COLUMN CLASS (planCountArrays), so that is every COUNT
				// that is not the FIRST count-needing aggregate over its
				// column: `SUM(v), COUNT(v)`, `AVG(v), COUNT(v)`, a second
				// COUNT(*), the second and third of three COUNT(v). Each
				// emitted 0 while the aggregate it shares with emitted the
				// right number, on whichever groups happened to reach this
				// materialize (#402).
				ca := countArrayOf(h.intFlatAccs, ai)
				// Defensive: a gi that wasn't appended to the SoA arrays
				// (can happen when compact-to-generic migration runs with
				// no rows consumed, leaving intFlatAccs cap=0 while
				// strGroupStates carries migrated entries) would otherwise
				// index a zero-length array and panic. Leave the
				// accumulator at its zero value so downstream kernels
				// emit identity output rather than crashing the worker.
				// Both bounds, because the two arrays are the same length
				// whenever both are live: growTo extends every array the
				// aggregate owns to the same n each batch, and the shared
				// count array is grown by its OWNER on the same schedule.
				// The only aggregate whose own length can lag is a sharer
				// that owns no arrays at all, and `gi >= len(ca)` is what
				// keeps it in.
				if gi >= flatAccumLen(fa) && gi >= len(ca) {
					continue
				}
				acc := &ext.accs[ai]
				acc.Count = 0
				if gi < len(ca) {
					acc.Count = ca[gi]
				}
				acc.IsFloat = fa.isFloat
				acc.IsDecimal = fa.isDecimal
				acc.DecScale = fa.decScale
				acc.DecOverflow = fa.sumDecOverflow
				acc.IntOverflow = fa.sumIntOverflow
				if fa.sumI64 != nil {
					acc.SumI64 = fa.sumI64[gi]
				}
				if fa.sumF64 != nil {
					acc.SumF64 = fa.sumF64[gi]
				}
				if fa.sumDec != nil {
					acc.SumDec = fa.sumDec[gi]
				}
				if fa.minI64 != nil {
					acc.MinI64 = fa.minI64[gi]
					acc.HasMin = fa.hasMin[gi]
				}
				if fa.maxI64 != nil {
					acc.MaxI64 = fa.maxI64[gi]
					acc.HasMax = fa.hasMax[gi]
				}
				if fa.minF64 != nil {
					acc.MinF64 = fa.minF64[gi]
					acc.HasMin = fa.hasMin[gi]
				}
				if fa.maxF64 != nil {
					acc.MaxF64 = fa.maxF64[gi]
					acc.HasMax = fa.hasMax[gi]
				}
				if fa.minDec != nil {
					acc.MinDec = fa.minDec[gi]
					acc.HasMin = fa.hasMin[gi]
				}
				if fa.maxDec != nil {
					acc.MaxDec = fa.maxDec[gi]
					acc.HasMax = fa.hasMax[gi]
				}
			}
		}
		h.intFlatAccs = nil
		h.groupIndexBuf = nil
		return
	}
	// Single-int and packed keys defer per-group state entirely: numIntGroups is
	// the count and intGroupStates is empty. Reify the slice here — the
	// migration/merge cold paths this feeds all read intGroupStates.
	if len(h.intGroupStates) < h.numIntGroups {
		reified := make([]*groupState, h.numIntGroups)
		copy(reified, h.intGroupStates)
		h.intGroupStates = reified
	}
	for gi, gs := range h.intGroupStates {
		if gs == nil {
			// Deferred state (single-int and packed-key paths): reify for
			// the migration/merge cold path that needs boxed extras. The
			// single-int key rides along so post-reify readers of
			// gs.intKey stay correct; composite keys stay in their SoA.
			gs = h.gsPool.alloc()
			if h.useIntGroupKey && gi < len(h.intKeys) {
				gs.intKey = h.intKeys[gi]
			}
			h.intGroupStates[gi] = gs
		}
		ext := gs.ensureExtras()
		if ext.accs == nil {
			ext.accs = make([]kernel.Accumulator, nAggs)
			h.extrasAccsCount += int64(nAggs)
		}
		for ai := range h.intFlatAccs {
			fa := &h.intFlatAccs[ai]
			acc := &ext.accs[ai]
			acc.Count = 0
			if ca := countArrayOf(h.intFlatAccs, ai); ca != nil {
				acc.Count = ca[gi]
			}
			acc.IsFloat = fa.isFloat
			acc.IsDecimal = fa.isDecimal
			acc.DecScale = fa.decScale
			acc.DecOverflow = fa.sumDecOverflow
			acc.IntOverflow = fa.sumIntOverflow
			if fa.sumI64 != nil {
				acc.SumI64 = fa.sumI64[gi]
			}
			if fa.sumF64 != nil {
				acc.SumF64 = fa.sumF64[gi]
			}
			if fa.sumDec != nil {
				acc.SumDec = fa.sumDec[gi]
			}
			if fa.minI64 != nil {
				acc.MinI64 = fa.minI64[gi]
				acc.HasMin = fa.hasMin[gi]
			}
			if fa.maxI64 != nil {
				acc.MaxI64 = fa.maxI64[gi]
				acc.HasMax = fa.hasMax[gi]
			}
			if fa.minF64 != nil {
				acc.MinF64 = fa.minF64[gi]
				acc.HasMin = fa.hasMin[gi]
			}
			if fa.maxF64 != nil {
				acc.MaxF64 = fa.maxF64[gi]
				acc.HasMax = fa.hasMax[gi]
			}
			if fa.minDec != nil {
				acc.MinDec = fa.minDec[gi]
				acc.HasMin = fa.hasMin[gi]
			}
			if fa.maxDec != nil {
				acc.MaxDec = fa.maxDec[gi]
				acc.HasMax = fa.hasMax[gi]
			}
		}
	}
	// Free flat arrays — no longer needed after materialization
	h.intFlatAccs = nil
	h.groupIndexBuf = nil
}

// rebuildFlatAccums re-creates SoA flat accumulator arrays from materialized
// per-group Accumulator structs. Called when intFlatAccs was cleared by
// materializeFlatAccums (during parallel merge) but the fast path is
// re-enabled for processing spilled rows in Finalize.
func (h *HashAggregate) rebuildFlatAccums(b *batch.RecordBatch) {
	h.initFlatAccums(b)

	var groups []*groupState
	nGroups := h.numIntGroups
	if h.useStrGroupKey || h.useGenericSoA {
		groups = h.strGroupStates
		nGroups = len(groups)
	} else {
		groups = h.intGroupStates
	}

	// Size every array to the group count first, so slots survive even for
	// group slices that were left deferred (empty) by the SoA paths.
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].growTo(nGroups)
	}
	for gi, gs := range groups {
		if gs == nil {
			continue
		}
		for ai := range h.intFlatAccs {
			fa := &h.intFlatAccs[ai]
			// extras may be nil if rebuild runs before any materialize/Group
			// path allocated them; treat those as the "no accumulators yet"
			// case the original `gs.accs == nil` branch handled.
			if gs.extras == nil || gs.extras.accs == nil || ai >= len(gs.extras.accs) {
				continue
			}
			acc := &gs.extras.accs[ai]
			if fa.count != nil {
				fa.count[gi] = acc.Count
			}
			if fa.sumI64 != nil {
				fa.sumI64[gi] = acc.SumI64
			}
			if fa.sumF64 != nil {
				fa.sumF64[gi] = acc.SumF64
			}
			if fa.sumDec != nil {
				fa.sumDec[gi] = acc.SumDec
			}
			if acc.DecOverflow {
				fa.sumDecOverflow = true
			}
			if acc.IntOverflow {
				fa.sumIntOverflow = true
			}
			if fa.minI64 != nil {
				fa.minI64[gi] = acc.MinI64
				fa.hasMin[gi] = acc.HasMin
			}
			if fa.maxI64 != nil {
				fa.maxI64[gi] = acc.MaxI64
				fa.hasMax[gi] = acc.HasMax
			}
			if fa.minF64 != nil {
				fa.minF64[gi] = acc.MinF64
				fa.hasMin[gi] = acc.HasMin
			}
			if fa.maxF64 != nil {
				fa.maxF64[gi] = acc.MaxF64
				fa.hasMax[gi] = acc.HasMax
			}
			if fa.minDec != nil {
				fa.minDec[gi] = acc.MinDec
				fa.hasMin[gi] = acc.HasMin
			}
			if fa.maxDec != nil {
				fa.maxDec[gi] = acc.MaxDec
				fa.hasMax[gi] = acc.HasMax
			}
		}
	}
}
