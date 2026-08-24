package exec

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// flatAccumArrays stores accumulator state in SoA (Struct of Arrays) layout
// for better cache locality during grouped aggregation. One per aggregate.
//
// With AoS layout (groupState.accs[]), accessing 2M group accumulators requires
// pointer-chasing through scattered heap objects (~192MB working set for 2 aggs).
// With SoA layout, each accumulator field is a contiguous array (~16MB per field),
// fitting in L3 cache and enabling sequential memory access.
type flatAccumArrays struct {
	// count is nil for aggregates that neither write nor read a row count
	// (MIN/MAX — their scatter kernels touch only min/max + hasMin/hasMax,
	// and their finalizers read only HasMin/HasMax) and for aggregates that
	// SHARE another aggregate's count array (see countFrom).
	count  []int64        // used by SUM, AVG, COUNT
	sumI64 []int64        // SUM/AVG for integer types
	sumF64 []float64      // SUM/AVG for float types
	sumDec []batch.Int128 // SUM/AVG for decimal types
	minI64 []int64
	maxI64 []int64
	minF64 []float64
	maxF64 []float64
	minDec []batch.Int128
	maxDec []batch.Int128
	hasMin []bool
	hasMax []bool

	// countFrom is the index (within the owning []flatAccumArrays) of the
	// aggregate whose count array this aggregate reads. It equals the
	// aggregate's own index when it owns its count (or has none). Sharers
	// keep count nil and resolve through countArrayOf; their scatter runs
	// the *NoCount kernel so the shared array is incremented exactly once
	// per batch. Set by initFlatAccums, which is also where the sharing
	// legality analysis lives. Travels with intFlatAccs through adoption,
	// merge and the drain cursor.
	countFrom int32

	isFloat   bool
	isDecimal bool
	decScale  int
	// sumDecOverflow marks that at least one group's DECIMAL SUM left the
	// 128-bit range in this operator. Per COLUMN rather than per group: one
	// wrapped sum fails the whole query (#455), so a bool is the whole
	// answer and a []bool would be a per-group array of zeroes. Merged by
	// mergeFlatAccs and copied onto every accumulator loadAccFromFlat
	// builds, which is where the emit path reads it.
	sumDecOverflow bool
}

// countArrayOf resolves the count array an aggregate reads, following the
// count-sharing link. Returns nil for aggregates with no count at all.
func countArrayOf(accs []flatAccumArrays, ai int) []int64 {
	return accs[accs[ai].countFrom].count
}

// growTo extends every live array to length n, zero-filling the new slots.
// Replaces the old per-new-group appendGroup(): the hash-probe loop now only
// counts groups, and each accumulator array is extended once per batch. Arrays
// the aggregate doesn't use stay nil (nil means "field absent", not "empty").
//
// Zero is the identity element for every operator here — COUNT/SUM start at 0,
// MIN/MAX start at 0 with hasMin/hasMax false so the scatter kernels seed from
// the first input — so a freshly grown slot behaves exactly like the old
// appendGroup'd one.
func (fa *flatAccumArrays) growTo(n int) {
	fa.count = growZeros(fa.count, n)
	fa.sumI64 = growZeros(fa.sumI64, n)
	fa.sumF64 = growZeros(fa.sumF64, n)
	fa.sumDec = growZeros(fa.sumDec, n)
	fa.minI64 = growZeros(fa.minI64, n)
	fa.maxI64 = growZeros(fa.maxI64, n)
	fa.minF64 = growZeros(fa.minF64, n)
	fa.maxF64 = growZeros(fa.maxF64, n)
	fa.minDec = growZeros(fa.minDec, n)
	fa.maxDec = growZeros(fa.maxDec, n)
	fa.hasMin = growZeros(fa.hasMin, n)
	fa.hasMax = growZeros(fa.hasMax, n)
}

// growZeros extends a live (non-nil) array to length n, zero-filling the new
// tail. A nil array means the aggregate has no such field and stays nil.
// Capacity growth is multiplicative (ensureAppendCap) so the heap-backed
// fallback doesn't realloc-and-copy every batch; off-heap arrays never
// realloc at all (their reservation caps at 512M elements).
func growZeros[T any](s []T, n int) []T {
	if s == nil || len(s) >= n {
		return s
	}
	if cap(s) < n {
		s = ensureAppendCap(s, n-len(s))
	}
	old := len(s)
	s = s[:n]
	clear(s[old:])
	return s
}

// clearGroup zeros every non-nil SoA field at index idx. Used by the
// partial-drain path to reset a reclaimed slot before it's recycled
// through HashAggregate.freeGroupIDs. Caller must guarantee idx is a
// valid index into the arrays (i.e., len(count) > idx, etc.).
//
// Zero is the identity element for the operators we run here: COUNT starts
// at 0, SUM at 0, MIN/MAX at 0 with hasMin/hasMax false (which makes the
// scatter kernels treat the first input as the seed). So a clearGroup'd
// slot behaves identically to a freshly grown slot for subsequent scatter
// updates.
//
// A shared count array (see countFrom) is zeroed by whichever aggregate owns
// it; sharers hold count == nil and skip it. Zeroing is idempotent, so the
// order aggregates are visited in doesn't matter.
func (fa *flatAccumArrays) clearGroup(idx int) {
	if fa.count != nil {
		fa.count[idx] = 0
	}
	if fa.sumI64 != nil {
		fa.sumI64[idx] = 0
	}
	if fa.sumF64 != nil {
		fa.sumF64[idx] = 0
	}
	if fa.sumDec != nil {
		fa.sumDec[idx] = batch.Int128{}
	}
	if fa.minI64 != nil {
		fa.minI64[idx] = 0
	}
	if fa.maxI64 != nil {
		fa.maxI64[idx] = 0
	}
	if fa.minF64 != nil {
		fa.minF64[idx] = 0
	}
	if fa.maxF64 != nil {
		fa.maxF64[idx] = 0
	}
	if fa.minDec != nil {
		fa.minDec[idx] = batch.Int128{}
	}
	if fa.maxDec != nil {
		fa.maxDec[idx] = batch.Int128{}
	}
	if fa.hasMin != nil {
		fa.hasMin[idx] = false
	}
	if fa.hasMax != nil {
		fa.hasMax[idx] = false
	}
}

// ensureAppendCap pre-grows s (≥2x) to absorb n more appends. The group-key side arrays (key values,
// chain links, group states) are appended once per NEW group inside the
// hash-lookup loops; Go's builtin append growth drops to ~1.25x for large
// slices, which turned those appends into a realloc-and-copy treadmill at
// high group cardinality (ClickBench Q33: growslice was 22% of the
// profile — more than the aggregation itself).
func ensureAppendCap[T any](s []T, n int) []T {
	if cap(s)-len(s) >= n {
		return s
	}
	newCap := len(s) + n
	if double := cap(s) * 2; double > newCap {
		newCap = double
	}
	ns := make([]T, len(s), newCap)
	copy(ns, s)
	return ns
}

// --- Generic scatter functions for SoA aggregate updates ---
//
// Each operates on contiguous arrays, eliminating per-row function pointer
// overhead and pointer-chasing through groupState objects.
//
// gi[i] = group index for iteration index i, or -1 to skip (null GROUP BY key).
// sel: if non-nil, data is accessed at sel[i]; if nil, data is accessed at i.
// n: number of direct rows (used when sel is nil).

func scatterSumInt[T ~int32 | ~int64](sumArr, countArr []int64, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		if !hasNulls {
			for si := range sel {
				row := int(sel[si])
				if idx := gi[si]; idx >= 0 {
					sumArr[idx] += int64(data[row])
					countArr[idx]++
				}
			}
		} else {
			for si := range sel {
				row := int(sel[si])
				if idx := gi[si]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += int64(data[row])
					countArr[idx]++
				}
			}
		}
	} else {
		if !hasNulls {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 {
					sumArr[idx] += int64(data[row])
					countArr[idx]++
				}
			}
		} else {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += int64(data[row])
					countArr[idx]++
				}
			}
		}
	}
}

// scatterSumIntNoCount is scatterSumInt without the count increment, for an
// aggregate that shares another aggregate's count array (the owner's kernel
// already incremented it over the identical non-null predicate). Keeping the
// count-less loop separate rather than nil-checking countArr per row is the
// same typed-kernel rule the rest of this file follows.
func scatterSumIntNoCount[T ~int32 | ~int64](sumArr []int64, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		if !hasNulls {
			for si := range sel {
				if idx := gi[si]; idx >= 0 {
					sumArr[idx] += int64(data[sel[si]])
				}
			}
		} else {
			for si := range sel {
				row := int(sel[si])
				if idx := gi[si]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += int64(data[row])
				}
			}
		}
	} else {
		if !hasNulls {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 {
					sumArr[idx] += int64(data[row])
				}
			}
		} else {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += int64(data[row])
				}
			}
		}
	}
}

func scatterSumFloat[T ~float32 | ~float64 | ~int64](sumArr []float64, countArr []int64, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		if !hasNulls {
			for si := range sel {
				row := int(sel[si])
				if idx := gi[si]; idx >= 0 {
					sumArr[idx] += float64(data[row])
					countArr[idx]++
				}
			}
		} else {
			for si := range sel {
				row := int(sel[si])
				if idx := gi[si]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += float64(data[row])
					countArr[idx]++
				}
			}
		}
	} else {
		if !hasNulls {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 {
					sumArr[idx] += float64(data[row])
					countArr[idx]++
				}
			}
		} else {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += float64(data[row])
					countArr[idx]++
				}
			}
		}
	}
}

// scatterSumFloatNoCount is scatterSumFloat for a count-sharing aggregate.
func scatterSumFloatNoCount[T ~float32 | ~float64 | ~int64](sumArr []float64, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		if !hasNulls {
			for si := range sel {
				if idx := gi[si]; idx >= 0 {
					sumArr[idx] += float64(data[sel[si]])
				}
			}
		} else {
			for si := range sel {
				row := int(sel[si])
				if idx := gi[si]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += float64(data[row])
				}
			}
		}
	} else {
		if !hasNulls {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 {
					sumArr[idx] += float64(data[row])
				}
			}
		} else {
			for row := 0; row < n; row++ {
				if idx := gi[row]; idx >= 0 && !nulls.IsNullFast(row) {
					sumArr[idx] += float64(data[row])
				}
			}
		}
	}
}

// scatterSumDecimal returns whether any group's sum left the Int128 range —
// the caller ORs it into flatAccumArrays.sumDecOverflow, and the emit path
// turns that into a query error rather than writing the wrapped value (#455).
func scatterSumDecimal(sumArr []batch.Int128, countArr []int64, data []batch.Int128, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) bool {
	hasNulls := nulls.HasNulls()
	overflow := false
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				sum, ok := sumArr[idx].AddChecked(data[row])
				sumArr[idx] = sum
				overflow = overflow || !ok
				countArr[idx]++
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				sum, ok := sumArr[idx].AddChecked(data[row])
				sumArr[idx] = sum
				overflow = overflow || !ok
				countArr[idx]++
			}
		}
	}
	return overflow
}

// scatterSumDecimalNoCount is scatterSumDecimal for a count-sharing aggregate.
func scatterSumDecimalNoCount(sumArr []batch.Int128, data []batch.Int128, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) bool {
	hasNulls := nulls.HasNulls()
	overflow := false
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				sum, ok := sumArr[idx].AddChecked(data[row])
				sumArr[idx] = sum
				overflow = overflow || !ok
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				sum, ok := sumArr[idx].AddChecked(data[row])
				sumArr[idx] = sum
				overflow = overflow || !ok
			}
		}
	}
	return overflow
}

func scatterCount(countArr []int64, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				countArr[idx]++
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				countArr[idx]++
			}
		}
	}
}

func scatterCountStar(countArr []int64, gi []int32, n int) {
	for i := 0; i < n; i++ {
		if idx := gi[i]; idx >= 0 {
			if int(idx) >= len(countArr) {
				// Diagnostic: surface state that lets us debug Q21's panic
				// instead of the bare "index out of range" runtime error.
				maxGI := int32(-1)
				for _, g := range gi[:n] {
					if g > maxGI {
						maxGI = g
					}
				}
				panic(fmt.Sprintf(
					"scatterCountStar: gi[%d]=%d out of range, len(countArr)=%d, n=%d, max(gi)=%d",
					i, idx, len(countArr), n, maxGI))
			}
			countArr[idx]++
		}
	}
}

func scatterMinInt[T ~int32 | ~int64](minArr []int64, hasMin []bool, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := int64(data[row])
				if !hasMin[idx] || v < minArr[idx] {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := int64(data[row])
				if !hasMin[idx] || v < minArr[idx] {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	}
}

func scatterMinDecimal(minArr []batch.Int128, hasMin []bool, data []batch.Int128, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := data[row]
				if !hasMin[idx] || v.Less(minArr[idx]) {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := data[row]
				if !hasMin[idx] || v.Less(minArr[idx]) {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	}
}

func scatterMaxDecimal(maxArr []batch.Int128, hasMax []bool, data []batch.Int128, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := data[row]
				if !hasMax[idx] || maxArr[idx].Less(v) {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := data[row]
				if !hasMax[idx] || maxArr[idx].Less(v) {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	}
}

func scatterMinFloat[T ~float32 | ~float64](minArr []float64, hasMin []bool, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMin[idx] || kernel.CompareFloat64(v, minArr[idx]) < 0 {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMin[idx] || kernel.CompareFloat64(v, minArr[idx]) < 0 {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	}
}

func scatterMaxInt[T ~int32 | ~int64](maxArr []int64, hasMax []bool, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := int64(data[row])
				if !hasMax[idx] || v > maxArr[idx] {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := int64(data[row])
				if !hasMax[idx] || v > maxArr[idx] {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	}
}

func scatterMaxFloat[T ~float32 | ~float64](maxArr []float64, hasMax []bool, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMax[idx] || kernel.CompareFloat64(v, maxArr[idx]) > 0 {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMax[idx] || kernel.CompareFloat64(v, maxArr[idx]) > 0 {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	}
}

// scatterFlatAggUpdate dispatches to the right scatter function for one aggregate.
// Type switch runs once per aggregate per batch, not per row.
//
// fa.count == nil means this aggregate shares an earlier aggregate's count
// array (initFlatAccums proved the two receive identical increments), so the
// count-free kernels run and the owner's pass does the counting. AggCount
// sharers have no state of their own at all and are skipped by the caller.
func scatterFlatAggUpdate(fa *flatAccumArrays, gi []int32, fn AggFunc, col *batch.Vector, sel []uint32, n int) {
	if fa.count == nil && (fn == AggSum || fn == AggAvg) {
		scatterFlatAggUpdateNoCount(fa, gi, fn, col, sel, n)
		return
	}
	switch fn {
	case AggAvg:
		// AVG over int64-class accumulates float64 (overflow-safe; the
		// result is a float mean anyway). Layout must match initFlatAggs.
		switch col.Type {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			scatterSumFloat(fa.sumF64, fa.count, col.Int64Data, gi, &col.Nulls, sel, n)
		case batch.TypeInt32, batch.TypePort, batch.TypeDate:
			scatterSumInt(fa.sumI64, fa.count, col.Int32Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat64:
			scatterSumFloat(fa.sumF64, fa.count, col.Float64Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat32:
			scatterSumFloat(fa.sumF64, fa.count, col.Float32Data, gi, &col.Nulls, sel, n)
		case batch.TypeDecimal:
			if scatterSumDecimal(fa.sumDec, fa.count, col.DecimalData.Data, gi, &col.Nulls, sel, n) {
				fa.sumDecOverflow = true
			}
		}
	case AggSum:
		switch col.Type {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			scatterSumInt(fa.sumI64, fa.count, col.Int64Data, gi, &col.Nulls, sel, n)
		case batch.TypeInt32, batch.TypePort, batch.TypeDate:
			scatterSumInt(fa.sumI64, fa.count, col.Int32Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat64:
			scatterSumFloat(fa.sumF64, fa.count, col.Float64Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat32:
			scatterSumFloat(fa.sumF64, fa.count, col.Float32Data, gi, &col.Nulls, sel, n)
		case batch.TypeDecimal:
			if scatterSumDecimal(fa.sumDec, fa.count, col.DecimalData.Data, gi, &col.Nulls, sel, n) {
				fa.sumDecOverflow = true
			}
		}
	case AggCount:
		scatterCount(fa.count, gi, &col.Nulls, sel, n)
	case AggMin:
		switch col.Type {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			scatterMinInt(fa.minI64, fa.hasMin, col.Int64Data, gi, &col.Nulls, sel, n)
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			scatterMinInt(fa.minI64, fa.hasMin, col.Int32Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat64:
			scatterMinFloat(fa.minF64, fa.hasMin, col.Float64Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat32:
			scatterMinFloat(fa.minF64, fa.hasMin, col.Float32Data, gi, &col.Nulls, sel, n)
		case batch.TypeDecimal:
			// initFlatAccums allocates minDec for a DECIMAL column and every
			// later stage of the SoA path — merge, copy, load, partial
			// spill — reads it, but nothing ever WROTE it: this switch had
			// no DECIMAL arm and no default. So `SELECT g, MIN(dec_col) ...
			// GROUP BY g` left hasMin all false and finalized as NULL on
			// every group, while the scalar `SELECT MIN(dec_col)` answered
			// correctly through kernel.ResolveBatchMin's own DECIMAL arm.
			// Both arms of the two-path gate take this code, so both
			// answered NULL and agreed (#417).
			scatterMinDecimal(fa.minDec, fa.hasMin, col.DecimalData.Data, gi, &col.Nulls, sel, n)
		}
	case AggMax:
		switch col.Type {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			scatterMaxInt(fa.maxI64, fa.hasMax, col.Int64Data, gi, &col.Nulls, sel, n)
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			scatterMaxInt(fa.maxI64, fa.hasMax, col.Int32Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat64:
			scatterMaxFloat(fa.maxF64, fa.hasMax, col.Float64Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat32:
			scatterMaxFloat(fa.maxF64, fa.hasMax, col.Float32Data, gi, &col.Nulls, sel, n)
		case batch.TypeDecimal:
			scatterMaxDecimal(fa.maxDec, fa.hasMax, col.DecimalData.Data, gi, &col.Nulls, sel, n)
		}
	}
}

// scatterFlatAggUpdateNoCount is the SUM/AVG dispatch for an aggregate that
// shares another aggregate's count array. Same layout rules as
// scatterFlatAggUpdate — only the count increment is dropped.
func scatterFlatAggUpdateNoCount(fa *flatAccumArrays, gi []int32, fn AggFunc, col *batch.Vector, sel []uint32, n int) {
	if fn == AggAvg {
		switch col.Type {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			scatterSumFloatNoCount(fa.sumF64, col.Int64Data, gi, &col.Nulls, sel, n)
		case batch.TypeInt32, batch.TypePort, batch.TypeDate:
			scatterSumIntNoCount(fa.sumI64, col.Int32Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat64:
			scatterSumFloatNoCount(fa.sumF64, col.Float64Data, gi, &col.Nulls, sel, n)
		case batch.TypeFloat32:
			scatterSumFloatNoCount(fa.sumF64, col.Float32Data, gi, &col.Nulls, sel, n)
		case batch.TypeDecimal:
			if scatterSumDecimalNoCount(fa.sumDec, col.DecimalData.Data, gi, &col.Nulls, sel, n) {
				fa.sumDecOverflow = true
			}
		}
		return
	}
	switch col.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		scatterSumIntNoCount(fa.sumI64, col.Int64Data, gi, &col.Nulls, sel, n)
	case batch.TypeInt32, batch.TypePort, batch.TypeDate:
		scatterSumIntNoCount(fa.sumI64, col.Int32Data, gi, &col.Nulls, sel, n)
	case batch.TypeFloat64:
		scatterSumFloatNoCount(fa.sumF64, col.Float64Data, gi, &col.Nulls, sel, n)
	case batch.TypeFloat32:
		scatterSumFloatNoCount(fa.sumF64, col.Float32Data, gi, &col.Nulls, sel, n)
	case batch.TypeDecimal:
		if scatterSumDecimalNoCount(fa.sumDec, col.DecimalData.Data, gi, &col.Nulls, sel, n) {
			fa.sumDecOverflow = true
		}
	}
}
