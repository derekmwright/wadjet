package kernel

import "github.com/derekmwright/wadjet/internal/engine/batch"

// --- Generic aggregate slice functions (monomorphized at compile time) ---

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
		acc.SumI64 += vec.Int64Data[row]
		acc.Count++
	}
}
func sumRowInt64NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	acc.SumI64 += vec.Int64Data[row]
	acc.Count++
}

func sumRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumI64 += int64(vec.Int32Data[row])
		acc.Count++
	}
}
func sumRowInt32NoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	acc.SumI64 += int64(vec.Int32Data[row])
	acc.Count++
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

// ResolveBatchSum returns a batch-level sum kernel for the given column type.
func ResolveBatchSum(typ batch.TypeID) BatchAggKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			s, c := sumSlice(vec.Int64Data, &vec.Nulls, sel, vecLen)
			acc.SumI64 += s
			acc.Count += c
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeDate:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
			s, c := sumSlice(vec.Int32Data, &vec.Nulls, sel, vecLen)
			acc.SumI64 += int64(s)
			acc.Count += c
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
			s, c := sumSlice(vec.Float32Data, &vec.Nulls, sel, vecLen)
			acc.SumF64 += float64(s)
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
