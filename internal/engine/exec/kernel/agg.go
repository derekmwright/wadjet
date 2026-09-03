package kernel

import (
	"math/bits"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Generic aggregate slice functions (monomorphized at compile time) ---

// sumSliceFloat32Widened totals a REAL column at DOUBLE width, widening each
// value before adding it. See the TypeFloat32 arm of ResolveBatchSum for why
// the widening has to happen per value rather than on the batch's own float32
// sum (#760).
func sumSliceFloat32Widened(data []float32, nulls *batch.Bitmap, sel []uint32, vecLen int) (float64, int64) {
	var sum float64
	var count int64
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					sum += float64(data[idx])
					count++
				}
			}
			return sum, count
		}
		for _, idx := range sel {
			sum += float64(data[idx])
		}
		return sum, int64(len(sel))
	}
	if nulls.HasNulls() {
		for i := 0; i < vecLen; i++ {
			if !nulls.IsNullFast(i) {
				sum += float64(data[i])
				count++
			}
		}
		return sum, count
	}
	for i := 0; i < vecLen; i++ {
		sum += float64(data[i])
	}
	return sum, int64(vecLen)
}

func sumSlice[T Numeric](data []T, nulls *batch.Bitmap, sel []uint32, vecLen int) (T, int64) {
	var sum T
	var count int64
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					sum += data[idx]
					count++
				}
			}
		} else {
			for _, idx := range sel {
				sum += data[idx]
			}
			count = int64(len(sel))
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					sum += data[i]
					count++
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				sum += data[i]
			}
			count = int64(vecLen)
		}
	}
	return sum, count
}

func countSlice(nulls *batch.Bitmap, sel []uint32, vecLen int) int64 {
	if sel != nil {
		if !nulls.HasNulls() {
			return int64(len(sel))
		}
		var count int64
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				count++
			}
		}
		return count
	}
	if !nulls.HasNulls() {
		return int64(vecLen)
	}
	var count int64
	for i := 0; i < vecLen; i++ {
		if !nulls.IsNullFast(i) {
			count++
		}
	}
	return count
}

// --- Row-level updaters (for grouped aggregation inner loop) ---
// Each updater has a null-checking variant and a NoNulls variant.
// The NoNulls variants skip the per-row null bitmap check, which
// eliminates a branch + memory load per row when the column has no nulls.

func sumRowInt64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		addInt64Checked(acc, vec.Int64Data[row])
	}
}
func sumRowInt64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	addInt64Checked(acc, vec.Int64Data[row])
}

func sumRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		addInt64Checked(acc, int64(vec.Int32Data[row]))
	}
}
func sumRowInt32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	addInt64Checked(acc, int64(vec.Int32Data[row]))
}

func sumRowFloat64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumF64 += vec.Float64Data[row]
		acc.Count++
		acc.IsFloat = true
	}
}
func sumRowFloat64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	acc.SumF64 += vec.Float64Data[row]
	acc.Count++
	acc.IsFloat = true
}

// REAL width, the row-at-a-time twin of the batched arm in ResolveBatchSum.
// The two must accumulate at the SAME width or one query answers two numbers
// depending on which path the operator took (#760).
func sumRowFloat32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumF64 += float64(vec.Float32Data[row])
		acc.Count++
		acc.IsFloat = true
	}
}
func sumRowFloat32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	acc.SumF64 += float64(vec.Float32Data[row])
	acc.Count++
	acc.IsFloat = true
}

func countRow(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.Count++
	}
}

func countRowStar(acc *Accumulator, _ *batch.Vector, _ int) {
	acc.Count++
}

func minRowInt64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.Int64Data[row]
		if !acc.HasMin || v < acc.MinI64 {
			acc.MinI64 = v
			acc.HasMin = true
		}
	}
}
func minRowInt64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := vec.Int64Data[row]
	if !acc.HasMin || v < acc.MinI64 {
		acc.MinI64 = v
		acc.HasMin = true
	}
}

func minRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := int64(vec.Int32Data[row])
		if !acc.HasMin || v < acc.MinI64 {
			acc.MinI64 = v
			acc.HasMin = true
		}
	}
}
func minRowInt32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := int64(vec.Int32Data[row])
	if !acc.HasMin || v < acc.MinI64 {
		acc.MinI64 = v
		acc.HasMin = true
	}
}

// The float MIN/MAX row and slice updaters below (#457) order by
// PostgreSQL's float total order — NaN sorts greatest, ADR-0012 item 8 —
// using `v < acc || acc != acc` (MIN) / `v > acc || v != v` (MAX) in place
// of a kernel.CompareFloat64 call: pointwise identical to
// CompareFloat64(v, acc) < 0 / > 0 except when v AND acc are both NaN,
// where the cheap form does a redundant "replace NaN with NaN" that
// CompareFloat64 would skip as a tie — harmless, since every NaN compares
// equal to every other regardless of payload and nothing here round-trips
// a NaN's bit pattern. Proved exhaustively over every boundary the order
// distinguishes by kernel.TestCheapNaNMinMaxFormsMatchCompareFloat64
// (agg_nan_test.go); a CompareFloat64 function call measured +59-102% on
// these hot per-element loops (BenchmarkScalarMinMaxFloat64/32,
// BenchmarkRowMinMaxFloat64, agg_nan_bench_test.go) for no behavioral
// difference a caller can observe.
func minRowFloat64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.Float64Data[row]
		if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
			acc.MinF64 = v
			acc.HasMin = true
			acc.IsFloat = true
		}
	}
}
func minRowFloat64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := vec.Float64Data[row]
	if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
		acc.MinF64 = v
		acc.HasMin = true
		acc.IsFloat = true
	}
}

func minRowFloat32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := float64(vec.Float32Data[row])
		if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
			acc.MinF64 = v
			acc.HasMin = true
			acc.IsFloat = true
		}
	}
}
func minRowFloat32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := float64(vec.Float32Data[row])
	if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
		acc.MinF64 = v
		acc.HasMin = true
		acc.IsFloat = true
	}
}

func maxRowInt64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.Int64Data[row]
		if !acc.HasMax || v > acc.MaxI64 {
			acc.MaxI64 = v
			acc.HasMax = true
		}
	}
}
func maxRowInt64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := vec.Int64Data[row]
	if !acc.HasMax || v > acc.MaxI64 {
		acc.MaxI64 = v
		acc.HasMax = true
	}
}

func maxRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := int64(vec.Int32Data[row])
		if !acc.HasMax || v > acc.MaxI64 {
			acc.MaxI64 = v
			acc.HasMax = true
		}
	}
}
func maxRowInt32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := int64(vec.Int32Data[row])
	if !acc.HasMax || v > acc.MaxI64 {
		acc.MaxI64 = v
		acc.HasMax = true
	}
}

// See minRowFloat64's comment for the `v > acc || v != v` cheap form.
func maxRowFloat64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.Float64Data[row]
		if !acc.HasMax || v > acc.MaxF64 || v != v {
			acc.MaxF64 = v
			acc.HasMax = true
			acc.IsFloat = true
		}
	}
}
func maxRowFloat64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := vec.Float64Data[row]
	if !acc.HasMax || v > acc.MaxF64 || v != v {
		acc.MaxF64 = v
		acc.HasMax = true
		acc.IsFloat = true
	}
}

func maxRowFloat32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := float64(vec.Float32Data[row])
		if !acc.HasMax || v > acc.MaxF64 || v != v {
			acc.MaxF64 = v
			acc.HasMax = true
			acc.IsFloat = true
		}
	}
}
func maxRowFloat32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	v := float64(vec.Float32Data[row])
	if !acc.HasMax || v > acc.MaxF64 || v != v {
		acc.MaxF64 = v
		acc.HasMax = true
		acc.IsFloat = true
	}
}

// --- Decimal row updaters ---

func sumRowDecimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		sum, ok := acc.SumDec.AddChecked(vec.DecimalData.Data[row])
		acc.SumDec = sum
		if !ok {
			acc.DecOverflow = true
		}
		acc.Count++
		acc.adoptDecScale(vec.DecimalData.Scale)
	}
}

func minRowDecimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.DecimalData.Data[row]
		// Outside the win check: a row that LOSES still contributed, and its
		// scale still has to agree with the one the accumulator counts in.
		acc.adoptDecScale(vec.DecimalData.Scale)
		if !acc.HasMin || v.Less(acc.MinDec) {
			acc.MinDec = v
			acc.HasMin = true
		}
	}
}

func maxRowDecimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.DecimalData.Data[row]
		// See minRowDecimal.
		acc.adoptDecScale(vec.DecimalData.Scale)
		if !acc.HasMax || acc.MaxDec.Less(v) {
			acc.MaxDec = v
			acc.HasMax = true
		}
	}
}

// addInt64Checked is the accumulate step for an INT64 sum that must be loud
// when it wraps (ADR-0012 item 9). The test is the standard two's-complement
// one: a sum whose sign differs from BOTH addends' is a wrap, and it costs two
// XORs and a branch that is never taken on real data.
func addInt64Checked(acc *Accumulator, v int64) {
	s := acc.SumI64 + v
	if (acc.SumI64^s)&(v^s) < 0 {
		acc.IntOverflow = true
	}
	acc.SumI64 = s
	acc.Count++
}

// --- Integer-into-Int128 updaters (#784) ---
//
// PostgreSQL answers SUM(int8) and AVG(int2/int4/int8) in numeric, so those
// accumulate exactly in the Int128 carrier at scale 0. The int64 sum they used
// to take WRAPS past 2^63 and the float64 AVG loses integer digits past 2^53,
// and both are silent — a different number wearing the right type, which
// ADR-0024 item 4 makes a 22003 rather than an answer. exec.aggIntExact is the
// single predicate that decides which aggregates take these; the scale is
// always 0, so adoptDecScale can never report a conflict against another
// integer batch of the same column.

func sumRowInt64Decimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		sumRowInt64DecimalNoNulls(acc, vec, row)
	}
}

func sumRowInt64DecimalNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	sum, ok := acc.SumDec.AddChecked(batch.Int128From(vec.Int64Data[row]))
	acc.SumDec = sum
	if !ok {
		acc.DecOverflow = true
	}
	acc.Count++
	acc.adoptDecScale(0)
}

func sumRowInt32Decimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		sumRowInt32DecimalNoNulls(acc, vec, row)
	}
}

func sumRowInt32DecimalNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	sum, ok := acc.SumDec.AddChecked(batch.Int128From(int64(vec.Int32Data[row])))
	acc.SumDec = sum
	if !ok {
		acc.DecOverflow = true
	}
	acc.Count++
	acc.adoptDecScale(0)
}

// ResolveRowSumIntExact returns the Int128 row updater for an INTEGER column,
// or nil for a type that has none — the caller then keeps its ordinary
// resolver, which is what makes this additive rather than a second dispatch
// nothing keeps in step with the first.
func ResolveRowSumIntExact(typ batch.TypeID, noNulls bool) RowAggUpdater {
	switch typ {
	case batch.TypeInt64:
		if noNulls {
			return sumRowInt64DecimalNoNulls
		}
		return sumRowInt64Decimal
	case batch.TypeInt32:
		if noNulls {
			return sumRowInt32DecimalNoNulls
		}
		return sumRowInt32Decimal
	}
	return nil
}

// ResolveBatchSumIntExact is ResolveRowSumIntExact's whole-vector form, for
// the ungrouped scalar fast path.
func ResolveBatchSumIntExact(typ batch.TypeID) BatchAggKernel {
	switch typ {
	case batch.TypeInt64:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			batchSumIntExact(acc, &vec.Nulls, sel, vecLen, func(row int) int64 { return vec.Int64Data[row] })
		}
	case batch.TypeInt32:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			batchSumIntExact(acc, &vec.Nulls, sel, vecLen, func(row int) int64 { return int64(vec.Int32Data[row]) })
		}
	}
	return nil
}

func batchSumIntExact(acc *Accumulator, nulls *batch.Bitmap, sel []uint32, vecLen int, at func(int) int64) {
	acc.adoptDecScale(0)
	add := func(row int) {
		sum, ok := acc.SumDec.AddChecked(batch.Int128From(at(row)))
		acc.SumDec = sum
		if !ok {
			acc.DecOverflow = true
		}
		acc.Count++
	}
	if sel != nil {
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				add(int(idx))
			}
		}
		return
	}
	for row := 0; row < vecLen; row++ {
		if !nulls.IsNullFast(row) {
			add(row)
		}
	}
}

// --- Resolve functions (called once at query init) ---

// ResolveRowSum returns a row-level sum updater for the given column type.
func ResolveRowSum(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return sumRowInt64
	case batch.TypeInt32, batch.TypePort, batch.TypeDate:
		return sumRowInt32
	case batch.TypeFloat64:
		return sumRowFloat64
	case batch.TypeFloat32:
		return sumRowFloat32
	case batch.TypeDecimal:
		return sumRowDecimal
	default:
		return nil
	}
}

// ResolveRowSumNoNulls returns a no-null-check sum updater.
func ResolveRowSumNoNulls(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return sumRowInt64NoNulls
	case batch.TypeInt32, batch.TypePort, batch.TypeDate:
		return sumRowInt32NoNulls
	case batch.TypeFloat64:
		return sumRowFloat64NoNulls
	case batch.TypeFloat32:
		return sumRowFloat32NoNulls
	case batch.TypeDecimal:
		return sumRowDecimal
	default:
		return nil
	}
}

// ResolveRowMin returns a row-level min updater for the given column type.
func ResolveRowMin(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return minRowInt64
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return minRowInt32
	case batch.TypeFloat64:
		return minRowFloat64
	case batch.TypeFloat32:
		return minRowFloat32
	case batch.TypeDecimal:
		return minRowDecimal
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return minRowString
	case batch.TypeCIDR:
		// PostgreSQL's inet order, not the stored text's byte order (#520):
		// see minCIDRUpdate.
		return minRowCIDR
	case batch.TypeBool:
		return minRowBool
	default:
		return nil
	}
}

// ResolveRowMinNoNulls returns a no-null-check min updater.
func ResolveRowMinNoNulls(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return minRowInt64NoNulls
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return minRowInt32NoNulls
	case batch.TypeFloat64:
		return minRowFloat64NoNulls
	case batch.TypeFloat32:
		return minRowFloat32NoNulls
	case batch.TypeDecimal:
		return minRowDecimal
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return minRowStringNoNulls
	case batch.TypeCIDR:
		return minRowCIDRNoNulls
	case batch.TypeBool:
		return minRowBoolNoNulls
	default:
		return nil
	}
}

// ResolveRowMax returns a row-level max updater for the given column type.
func ResolveRowMax(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return maxRowInt64
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return maxRowInt32
	case batch.TypeFloat64:
		return maxRowFloat64
	case batch.TypeFloat32:
		return maxRowFloat32
	case batch.TypeDecimal:
		return maxRowDecimal
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return maxRowString
	case batch.TypeCIDR:
		return maxRowCIDR
	case batch.TypeBool:
		return maxRowBool
	default:
		return nil
	}
}

// ResolveRowMaxNoNulls returns a no-null-check max updater.
func ResolveRowMaxNoNulls(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return maxRowInt64NoNulls
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return maxRowInt32NoNulls
	case batch.TypeFloat64:
		return maxRowFloat64NoNulls
	case batch.TypeFloat32:
		return maxRowFloat32NoNulls
	case batch.TypeDecimal:
		return maxRowDecimal
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return maxRowStringNoNulls
	case batch.TypeCIDR:
		return maxRowCIDRNoNulls
	case batch.TypeBool:
		return maxRowBoolNoNulls
	default:
		return nil
	}
}

// ResolveRowCount returns a row-level count updater.
// If countStar is true, counts all rows (including nulls).
func ResolveRowCount(countStar bool) RowAggUpdater {
	if countStar {
		return countRowStar
	}
	return countRow
}

// --- Batch-level resolve functions ---

// A note the three DECIMAL arms below share: acc.DecScale is adopted ONLY
// from a batch that actually contributed a value, exactly as the row updaters
// (sumRowDecimal/minRowDecimal/maxRowDecimal) already did inside their null
// guard. It is the scale the Int128 in SumDec/MinDec/MaxDec is counted in, and
// FinalSum/FinalMin/FinalMax render the accumulator as text at it — so a batch
// that added nothing but overwrote it makes the emit re-parse a right integer
// under a wrong scale, which is a different number written silently.
//
// The reachable case is not hypothetical: an ungrouped aggregate that consumed
// no rows emits an identity row (SUM/MIN/MAX -> NULL) whose DECIMAL vector has
// no input to take a scale from, so it ships scale 0. On the stage DAG each
// partial is its own task, a selective filter makes some of them match nothing,
// and that all-NULL scale-0 batch is one of the inputs the final aggregate
// merges. Adopting its scale turned SUM(a) WHERE id < 5 into 3824.00 where the
// answer is 38.24 — 10^scale too large, on SUM, AVG, MIN and MAX alike (#685).

// sumSliceExactInt64 sums an INT64 vector in 128 BITS and reports whether the
// batch's true total fits back into int64. It replaces the per-row branch the
// first cut of this check used.
//
// The 128-bit form is the CHEAPEST EXACT one measured, and it is cheaper for
// the shape of the work rather than the width. A per-row
// `over = over || (sum^s)&(v^s) < 0` puts a BRANCH on the loop-carried sum;
// this is an ADD/ADC pair whose carry feeds a SECOND accumulator and whose sign
// count feeds a THIRD, so three independent chains issue together.
// BenchmarkBatchSumInt64, medians of 7 runs at -benchtime=3000x:
//
//	unchecked base (no exactness at all)      517 ns
//	per-row branch (8d34890b)               1 257 ns   +143%
//	this                                      954 ns    +85%
//
// Every alternative the round-3 review named was measured and is slower or no
// better: the branchless `overBits |=` (~1 100), a magnitude probe proving the
// batch cannot wrap before running the untouched base loop (~1 300 — the probe
// is a second full pass and no cheaper than the sum), and two- and four-lane
// unrollings of this loop (~1 015). The residual is not a spelling problem: the
// base loop is already ISSUE-limited at about 1.1 cycles per row (load, add,
// index, branch), so any exactness at all costs roughly a cycle per row. What
// is left is a bounded cost on a kernel that, since #784, serves computed
// integer arguments and TIMESTAMP/DURATION sums — a bare SUM over an integer
// COLUMN takes the Int128 carrier or the widened int32 loop below.
//
// It is also the more correct reading. The per-row form was STICKY — a running
// total that left the int64 range and came back failed a query whose answer was
// representable — where this one asks the question once, of the number the
// batch actually produced. The cross-batch fold in ResolveBatchSum keeps the
// conservative reading, because there the running total IS the answer so far.
//
// (hi, lo) is the exact two's-complement 128-bit sum; it fits in int64 exactly
// when hi is int64(lo)'s sign extension.
func sumSliceExactInt64(data []int64, nulls *batch.Bitmap, sel []uint32, vecLen int, acc *Accumulator) (int64, int64) {
	var lo uint64
	var hi, count int64
	// The same four specialized loops sumSlice has, for the same reason: the
	// null predicate is resolved once per vector, not once per row.
	switch {
	case sel != nil && nulls.HasNulls():
		for _, idx := range sel {
			if nulls.IsNullFast(int(idx)) {
				continue
			}
			v := data[idx]
			var c uint64
			lo, c = bits.Add64(lo, uint64(v), 0)
			hi += (v >> 63) + int64(c)
			count++
		}
	case sel != nil:
		for _, idx := range sel {
			v := data[idx]
			var c uint64
			lo, c = bits.Add64(lo, uint64(v), 0)
			hi += (v >> 63) + int64(c)
		}
		count = int64(len(sel))
	case nulls.HasNulls():
		for i := 0; i < vecLen; i++ {
			if nulls.IsNullFast(i) {
				continue
			}
			v := data[i]
			var c uint64
			lo, c = bits.Add64(lo, uint64(v), 0)
			hi += (v >> 63) + int64(c)
			count++
		}
	default:
		var csum uint64
		var signs int64
		for _, v := range data[:vecLen] {
			var c uint64
			lo, c = bits.Add64(lo, uint64(v), 0)
			csum += c
			signs += v >> 63
		}
		hi += signs + int64(csum)
		count = int64(vecLen)
	}
	sum := int64(lo)
	if hi != sum>>63 {
		acc.IntOverflow = true
	}
	return sum, count
}

// sumSliceInt32Widened sums an INT32 vector into an INT64, which is the whole
// fix: sumSlice is generic over the column's own width, so the INT32 arm summed
// each batch in INT32 and `SUM(int4)` over four rows of 2 000 000 000 answered
// -589934592 for PostgreSQL's 8000000000 (review round 3, F1). The row path
// never had this — sumRowInt32 widens per row — so the two paths disagreed on
// the same query.
//
// No check inside the loop, and that is a bound rather than an omission: a
// batch holds at most batch.DefaultBatchSize rows, and 2^32 rows at 2^31 would
// be needed before a widened int32 sum could leave int64. The CROSS-BATCH fold
// is checked by the caller, which is where an int4 sum can genuinely overflow.
//
// The dense loop is unrolled four ways because the widening is what costs: an
// int32-width sum folds the load into ADDL, and a widened one cannot, so the
// extra sign-extending load needs the instruction-level parallelism to hide.
// BenchmarkBatchSumInt32, medians of 7 runs at -benchtime=3000x: summing at the
// column's width (wrong) 523 ns, the plain widened loop 887 ns, this 658 ns.
// Eight lanes measured no better than four.
func sumSliceInt32Widened(data []int32, nulls *batch.Bitmap, sel []uint32, vecLen int) (int64, int64) {
	var sum, count int64
	switch {
	case sel != nil && nulls.HasNulls():
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				sum += int64(data[idx])
				count++
			}
		}
	case sel != nil:
		for _, idx := range sel {
			sum += int64(data[idx])
		}
		count = int64(len(sel))
	case nulls.HasNulls():
		for i := 0; i < vecLen; i++ {
			if !nulls.IsNullFast(i) {
				sum += int64(data[i])
				count++
			}
		}
	default:
		d := data[:vecLen]
		var s0, s1, s2, s3 int64
		i := 0
		for ; i+4 <= len(d); i += 4 {
			s0 += int64(d[i])
			s1 += int64(d[i+1])
			s2 += int64(d[i+2])
			s3 += int64(d[i+3])
		}
		sum += s0 + s1 + s2 + s3
		for ; i < len(d); i++ {
			sum += int64(d[i])
		}
		count = int64(vecLen)
	}
	return sum, count
}

// foldInt64Checked adds a batch's sum into the accumulator's running total and
// latches a wrap. One check per BATCH, not per row.
func foldInt64Checked(acc *Accumulator, s, c int64) {
	total := acc.SumI64 + s
	if (acc.SumI64^total)&(s^total) < 0 {
		acc.IntOverflow = true
	}
	acc.SumI64 = total
	acc.Count += c
}

// ResolveBatchSum returns a batch-level sum kernel for the given column type.
func ResolveBatchSum(typ batch.TypeID) BatchAggKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			// The batch sum and the fold into the accumulator can each wrap,
			// and both are checked: an int64 sum that wrapped is a different
			// number wearing the right type, and ADR-0012 item 9 makes it a
			// 22003 rather than an answer.
			s, c := sumSliceExactInt64(vec.Int64Data, &vec.Nulls, sel, vecLen, acc)
			foldInt64Checked(acc, s, c)
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeDate:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			// WIDENED to int64 before the addition, and folded through the same
			// checked add the int64 arm uses: `SUM(int4)` is bigint, so the
			// batch may not be summed at the column's width (review round 3,
			// F1).
			s, c := sumSliceInt32Widened(vec.Int32Data, &vec.Nulls, sel, vecLen)
			foldInt64Checked(acc, s, c)
		}
	case batch.TypeFloat64:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			s, c := sumSlice(vec.Float64Data, &vec.Nulls, sel, vecLen)
			acc.SumF64 += s
			acc.Count += c
			acc.IsFloat = true
		}
	case batch.TypeFloat32:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			// Widened PER VALUE, not per batch sum (#760). PostgreSQL's
			// avg(real) is double precision and totals each value at that
			// width: over 0.1, 16777216 and -0.5 the per-value float8 total
			// is 16777215.6 and its average 5592405.2, which is what the
			// server answers. Summing the batch at float32 first absorbs the
			// 0.1 and averages 5592405.33 — which is what the single-process
			// path answered while the DAG, whose three workers each summed
			// ONE row, answered PostgreSQL's number. One query, two engines,
			// two numbers, reproducibly: not ADR-0013's legal float
			// nondeterminism.
			//
			// SUM(real) is real on the same server, and the REAL-width
			// DECLARATION narrows this total once at the store rather than
			// carrying a second accumulator — which is also what makes the
			// two engines agree there, because the narrowing happens on every
			// partial and again on the merge.
			s, c := sumSliceFloat32Widened(vec.Float32Data, &vec.Nulls, sel, vecLen)
			acc.SumF64 += s
			acc.Count += c
			acc.IsFloat = true
		}
	case batch.TypeDecimal:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			data := vec.DecimalData.Data
			nulls := &vec.Nulls
			overflow := false
			// SUM reads "did this batch contribute" off the count it already
			// maintains, so the loop is untouched.
			before := acc.Count
			if sel != nil {
				for _, idx := range sel {
					if !nulls.IsNullFast(int(idx)) {
						sum, ok := acc.SumDec.AddChecked(data[idx])
						acc.SumDec = sum
						overflow = overflow || !ok
						acc.Count++
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if !nulls.IsNullFast(i) {
						sum, ok := acc.SumDec.AddChecked(data[i])
						acc.SumDec = sum
						overflow = overflow || !ok
						acc.Count++
					}
				}
			}
			if overflow {
				acc.DecOverflow = true
			}
			if acc.Count != before {
				acc.adoptDecScale(vec.DecimalData.Scale)
			}
		}
	default:
		return nil
	}
}

// ResolveBatchCount returns a batch-level count kernel.
func ResolveBatchCount() BatchAggKernel {
	return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
		acc.Count += countSlice(&vec.Nulls, sel, vecLen)
	}
}

// ResolveBatchMin returns a batch-level min kernel for the given column type.
func ResolveBatchMin(typ batch.TypeID) BatchAggKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			minSliceInt64(acc, vec.Int64Data, &vec.Nulls, sel, vecLen)
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			minSliceInt32(acc, vec.Int32Data, &vec.Nulls, sel, vecLen)
		}
	case batch.TypeFloat64:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			minSliceFloat64(acc, vec.Float64Data, &vec.Nulls, sel, vecLen)
			acc.IsFloat = true
		}
	case batch.TypeFloat32:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			minSliceFloat32(acc, vec.Float32Data, &vec.Nulls, sel, vecLen)
			acc.IsFloat = true
		}
	case batch.TypeDecimal:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			data := vec.DecimalData.Data
			nulls := &vec.Nulls
			// MIN/MAX keep no count, so the flag is set in the guard body the
			// loop already runs. Asking a helper up front instead cost a
			// SECOND full bitmap pass, which on an all-NULL batch — the shape
			// this rule exists for — is the whole of the work (#685 review).
			took := false
			if sel != nil {
				for _, idx := range sel {
					if !nulls.IsNullFast(int(idx)) {
						v := data[idx]
						took = true
						if !acc.HasMin || v.Less(acc.MinDec) {
							acc.MinDec = v
							acc.HasMin = true
						}
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if !nulls.IsNullFast(i) {
						v := data[i]
						took = true
						if !acc.HasMin || v.Less(acc.MinDec) {
							acc.MinDec = v
							acc.HasMin = true
						}
					}
				}
			}
			if took {
				acc.adoptDecScale(vec.DecimalData.Scale)
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return minBatchString
	case batch.TypeCIDR:
		return minBatchCIDR
	case batch.TypeBool:
		return minBatchBool
	default:
		return nil
	}
}

// ResolveBatchMax returns a batch-level max kernel for the given column type.
func ResolveBatchMax(typ batch.TypeID) BatchAggKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			maxSliceInt64(acc, vec.Int64Data, &vec.Nulls, sel, vecLen)
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			maxSliceInt32(acc, vec.Int32Data, &vec.Nulls, sel, vecLen)
		}
	case batch.TypeFloat64:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			maxSliceFloat64(acc, vec.Float64Data, &vec.Nulls, sel, vecLen)
			acc.IsFloat = true
		}
	case batch.TypeFloat32:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			maxSliceFloat32(acc, vec.Float32Data, &vec.Nulls, sel, vecLen)
			acc.IsFloat = true
		}
	case batch.TypeDecimal:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			data := vec.DecimalData.Data
			nulls := &vec.Nulls
			// See ResolveBatchMin's DECIMAL arm.
			took := false
			if sel != nil {
				for _, idx := range sel {
					if !nulls.IsNullFast(int(idx)) {
						v := data[idx]
						took = true
						if !acc.HasMax || acc.MaxDec.Less(v) {
							acc.MaxDec = v
							acc.HasMax = true
						}
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if !nulls.IsNullFast(i) {
						v := data[i]
						took = true
						if !acc.HasMax || acc.MaxDec.Less(v) {
							acc.MaxDec = v
							acc.HasMax = true
						}
					}
				}
			}
			if took {
				acc.adoptDecScale(vec.DecimalData.Scale)
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return maxBatchString
	case batch.TypeCIDR:
		return maxBatchCIDR
	case batch.TypeBool:
		return maxBatchBool
	default:
		return nil
	}
}

// --- Batch-level min/max helper functions ---

func minSliceInt64(acc *Accumulator, data []int64, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := data[idx]
					if !acc.HasMin || v < acc.MinI64 {
						acc.MinI64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := data[idx]
				if !acc.HasMin || v < acc.MinI64 {
					acc.MinI64 = v
					acc.HasMin = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := data[i]
					if !acc.HasMin || v < acc.MinI64 {
						acc.MinI64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := data[i]
				if !acc.HasMin || v < acc.MinI64 {
					acc.MinI64 = v
					acc.HasMin = true
				}
			}
		}
	}
}

func minSliceInt32(acc *Accumulator, data []int32, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := int64(data[idx])
					if !acc.HasMin || v < acc.MinI64 {
						acc.MinI64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := int64(data[idx])
				if !acc.HasMin || v < acc.MinI64 {
					acc.MinI64 = v
					acc.HasMin = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := int64(data[i])
					if !acc.HasMin || v < acc.MinI64 {
						acc.MinI64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := int64(data[i])
				if !acc.HasMin || v < acc.MinI64 {
					acc.MinI64 = v
					acc.HasMin = true
				}
			}
		}
	}
}

// See minRowFloat64's comment for the `v < acc || acc != acc` cheap form.
func minSliceFloat64(acc *Accumulator, data []float64, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := data[idx]
					if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
						acc.MinF64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := data[idx]
				if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
					acc.MinF64 = v
					acc.HasMin = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := data[i]
					if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
						acc.MinF64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := data[i]
				if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
					acc.MinF64 = v
					acc.HasMin = true
				}
			}
		}
	}
}

func minSliceFloat32(acc *Accumulator, data []float32, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := float64(data[idx])
					if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
						acc.MinF64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := float64(data[idx])
				if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
					acc.MinF64 = v
					acc.HasMin = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := float64(data[i])
					if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
						acc.MinF64 = v
						acc.HasMin = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := float64(data[i])
				if !acc.HasMin || v < acc.MinF64 || acc.MinF64 != acc.MinF64 {
					acc.MinF64 = v
					acc.HasMin = true
				}
			}
		}
	}
}

func maxSliceInt64(acc *Accumulator, data []int64, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := data[idx]
					if !acc.HasMax || v > acc.MaxI64 {
						acc.MaxI64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := data[idx]
				if !acc.HasMax || v > acc.MaxI64 {
					acc.MaxI64 = v
					acc.HasMax = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := data[i]
					if !acc.HasMax || v > acc.MaxI64 {
						acc.MaxI64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := data[i]
				if !acc.HasMax || v > acc.MaxI64 {
					acc.MaxI64 = v
					acc.HasMax = true
				}
			}
		}
	}
}

func maxSliceInt32(acc *Accumulator, data []int32, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := int64(data[idx])
					if !acc.HasMax || v > acc.MaxI64 {
						acc.MaxI64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := int64(data[idx])
				if !acc.HasMax || v > acc.MaxI64 {
					acc.MaxI64 = v
					acc.HasMax = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := int64(data[i])
					if !acc.HasMax || v > acc.MaxI64 {
						acc.MaxI64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := int64(data[i])
				if !acc.HasMax || v > acc.MaxI64 {
					acc.MaxI64 = v
					acc.HasMax = true
				}
			}
		}
	}
}

// See minRowFloat64's comment for the `v > acc || v != v` cheap form.
func maxSliceFloat64(acc *Accumulator, data []float64, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := data[idx]
					if !acc.HasMax || v > acc.MaxF64 || v != v {
						acc.MaxF64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := data[idx]
				if !acc.HasMax || v > acc.MaxF64 || v != v {
					acc.MaxF64 = v
					acc.HasMax = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := data[i]
					if !acc.HasMax || v > acc.MaxF64 || v != v {
						acc.MaxF64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := data[i]
				if !acc.HasMax || v > acc.MaxF64 || v != v {
					acc.MaxF64 = v
					acc.HasMax = true
				}
			}
		}
	}
}

func maxSliceFloat32(acc *Accumulator, data []float32, nulls *batch.Bitmap, sel []uint32, vecLen int) {
	if sel != nil {
		if nulls.HasNulls() {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					v := float64(data[idx])
					if !acc.HasMax || v > acc.MaxF64 || v != v {
						acc.MaxF64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for _, idx := range sel {
				v := float64(data[idx])
				if !acc.HasMax || v > acc.MaxF64 || v != v {
					acc.MaxF64 = v
					acc.HasMax = true
				}
			}
		}
	} else {
		if nulls.HasNulls() {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					v := float64(data[i])
					if !acc.HasMax || v > acc.MaxF64 || v != v {
						acc.MaxF64 = v
						acc.HasMax = true
					}
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				v := float64(data[i])
				if !acc.HasMax || v > acc.MaxF64 || v != v {
					acc.MaxF64 = v
					acc.HasMax = true
				}
			}
		}
	}
}
