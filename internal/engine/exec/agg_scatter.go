package exec

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// flatAccumArrays stores accumulator state in SoA (Struct of Arrays) layout
// for better cache locality during grouped aggregation. One per aggregate.
//
// With AoS layout (groupState.accs[]), accessing 2M group accumulators requires
// pointer-chasing through scattered heap objects (~192MB working set for 2 aggs).
// With SoA layout, each accumulator field is a contiguous array (~16MB per field),
// fitting in L3 cache and enabling sequential memory access.
type flatAccumArrays struct {
	count  []int64       // used by SUM, AVG, COUNT
	sumI64 []int64       // SUM/AVG for integer types
	sumF64 []float64     // SUM/AVG for float types
	sumDec []batch.Int128 // SUM/AVG for decimal types
	minI64 []int64
	maxI64 []int64
	minF64 []float64
	maxF64 []float64
	minDec []batch.Int128
	maxDec []batch.Int128
	hasMin []bool
	hasMax []bool

	isFloat   bool
	isDecimal bool
	decScale  int
}

// appendGroup extends all non-nil arrays by one zero-initialized element.
func (fa *flatAccumArrays) appendGroup() {
	fa.count = append(fa.count, 0)
	if fa.sumI64 != nil {
		fa.sumI64 = append(fa.sumI64, 0)
	}
	if fa.sumF64 != nil {
		fa.sumF64 = append(fa.sumF64, 0)
	}
	if fa.sumDec != nil {
		fa.sumDec = append(fa.sumDec, batch.Int128{})
	}
	if fa.minI64 != nil {
		fa.minI64 = append(fa.minI64, 0)
	}
	if fa.maxI64 != nil {
		fa.maxI64 = append(fa.maxI64, 0)
	}
	if fa.minF64 != nil {
		fa.minF64 = append(fa.minF64, 0)
	}
	if fa.maxF64 != nil {
		fa.maxF64 = append(fa.maxF64, 0)
	}
	if fa.minDec != nil {
		fa.minDec = append(fa.minDec, batch.Int128{})
	}
	if fa.maxDec != nil {
		fa.maxDec = append(fa.maxDec, batch.Int128{})
	}
	if fa.hasMin != nil {
		fa.hasMin = append(fa.hasMin, false)
	}
	if fa.hasMax != nil {
		fa.hasMax = append(fa.hasMax, false)
	}
}

// clearGroup zeros every non-nil SoA field at index idx. Used by the
// partial-drain path to reset a reclaimed slot before it's recycled
// through HashAggregate.freeGroupIDs. Caller must guarantee idx is a
// valid index into the arrays (i.e., len(count) > idx, etc.).
//
// Zero is the identity element for the operators we run here: COUNT starts
// at 0, SUM at 0, MIN/MAX at 0 with hasMin/hasMax false (which makes the
// scatter kernels treat the first input as the seed). So a clearGroup'd
// slot behaves identically to a fresh appendGroup'd slot for subsequent
// scatter updates.
func (fa *flatAccumArrays) clearGroup(idx int) {
	fa.count[idx] = 0
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

// ensureCapacity pre-grows all non-nil arrays so that n additional
// appendGroup calls won't trigger growslice reallocation. Called
// before the hash lookup phase with n = batch row count as an upper
// bound on new groups this batch can create.
func (fa *flatAccumArrays) ensureCapacity(n int) {
	fa.count = ensureSliceCap(fa.count, n)
	if fa.sumI64 != nil {
		fa.sumI64 = ensureSliceCap(fa.sumI64, n)
	}
	if fa.sumF64 != nil {
		fa.sumF64 = ensureSliceCapF64(fa.sumF64, n)
	}
	if fa.sumDec != nil {
		fa.sumDec = ensureSliceCapInt128(fa.sumDec, n)
	}
	if fa.minI64 != nil {
		fa.minI64 = ensureSliceCap(fa.minI64, n)
	}
	if fa.maxI64 != nil {
		fa.maxI64 = ensureSliceCap(fa.maxI64, n)
	}
	if fa.minF64 != nil {
		fa.minF64 = ensureSliceCapF64(fa.minF64, n)
	}
	if fa.maxF64 != nil {
		fa.maxF64 = ensureSliceCapF64(fa.maxF64, n)
	}
	if fa.minDec != nil {
		fa.minDec = ensureSliceCapInt128(fa.minDec, n)
	}
	if fa.maxDec != nil {
		fa.maxDec = ensureSliceCapInt128(fa.maxDec, n)
	}
	if fa.hasMin != nil {
		fa.hasMin = ensureSliceCapBool(fa.hasMin, n)
	}
	if fa.hasMax != nil {
		fa.hasMax = ensureSliceCapBool(fa.hasMax, n)
	}
}

// ensureSliceCap grows the capacity of s to fit n more appends without reallocation.
// Uses multiplicative growth (at least 2x) to amortize allocation cost. Without this,
// additive growth by batch size (2048) causes a reallocation nearly every batch when
// only a few new groups are created per batch.
func ensureSliceCap(s []int64, n int) []int64 {
	if cap(s)-len(s) >= n {
		return s
	}
	newCap := len(s) + n
	if double := cap(s) * 2; double > newCap {
		newCap = double
	}
	ns := make([]int64, len(s), newCap)
	copy(ns, s)
	return ns
}

// ensureAppendCap is ensureSliceCap for any element type: pre-grows s
// (≥2x) to absorb n more appends. The group-key side arrays (key values,
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

func ensureSliceCapF64(s []float64, n int) []float64 {
	if cap(s)-len(s) >= n {
		return s
	}
	newCap := len(s) + n
	if double := cap(s) * 2; double > newCap {
		newCap = double
	}
	ns := make([]float64, len(s), newCap)
	copy(ns, s)
	return ns
}

func ensureSliceCapInt128(s []batch.Int128, n int) []batch.Int128 {
	if cap(s)-len(s) >= n {
		return s
	}
	newCap := len(s) + n
	if double := cap(s) * 2; double > newCap {
		newCap = double
	}
	ns := make([]batch.Int128, len(s), newCap)
	copy(ns, s)
	return ns
}

func ensureSliceCapBool(s []bool, n int) []bool {
	if cap(s)-len(s) >= n {
		return s
	}
	newCap := len(s) + n
	if double := cap(s) * 2; double > newCap {
		newCap = double
	}
	ns := make([]bool, len(s), newCap)
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

func scatterSumDecimal(sumArr []batch.Int128, countArr []int64, data []batch.Int128, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				sumArr[idx] = sumArr[idx].Add(data[row])
				countArr[idx]++
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				sumArr[idx] = sumArr[idx].Add(data[row])
				countArr[idx]++
			}
		}
	}
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

func scatterMinFloat[T ~float32 | ~float64](minArr []float64, hasMin []bool, data []T, gi []int32, nulls *batch.Bitmap, sel []uint32, n int) {
	hasNulls := nulls.HasNulls()
	if sel != nil {
		for si := range sel {
			row := int(sel[si])
			if idx := gi[si]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMin[idx] || v < minArr[idx] {
					minArr[idx] = v
					hasMin[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMin[idx] || v < minArr[idx] {
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
				if !hasMax[idx] || v > maxArr[idx] {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	} else {
		for row := 0; row < n; row++ {
			if idx := gi[row]; idx >= 0 && !(hasNulls && nulls.IsNullFast(row)) {
				v := float64(data[row])
				if !hasMax[idx] || v > maxArr[idx] {
					maxArr[idx] = v
					hasMax[idx] = true
				}
			}
		}
	}
}

// scatterFlatAggUpdate dispatches to the right scatter function for one aggregate.
// Type switch runs once per aggregate per batch, not per row.
func scatterFlatAggUpdate(fa *flatAccumArrays, gi []int32, fn AggFunc, col *batch.Vector, sel []uint32, n int) {
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
			scatterSumDecimal(fa.sumDec, fa.count, col.DecimalData.Data, gi, &col.Nulls, sel, n)
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
			scatterSumDecimal(fa.sumDec, fa.count, col.DecimalData.Data, gi, &col.Nulls, sel, n)
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
		}
	}
}
