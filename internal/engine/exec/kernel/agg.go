package kernel

import "github.com/derekmwright/caelum/internal/engine/batch"

// --- Generic aggregate slice functions (monomorphized at compile time) ---

func sumSlice[T Numeric](data []T, nulls *batch.Bitmap, sel []uint16, vecLen int) (T, int64) {
	var sum T
	var count int64
	if sel != nil {
		if nulls.NullCount() > 0 {
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
		if nulls.NullCount() > 0 {
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

func minSlice[T Ordered](data []T, nulls *batch.Bitmap, sel []uint16, vecLen int) (T, bool) {
	var min T
	found := false
	iter := selOrRange(sel, vecLen)
	for _, i := range iter {
		if !nulls.IsNullFast(i) {
			v := data[i]
			if !found || v < min {
				min = v
				found = true
			}
		}
	}
	return min, found
}

func maxSlice[T Ordered](data []T, nulls *batch.Bitmap, sel []uint16, vecLen int) (T, bool) {
	var max T
	found := false
	iter := selOrRange(sel, vecLen)
	for _, i := range iter {
		if !nulls.IsNullFast(i) {
			v := data[i]
			if !found || v > max {
				max = v
				found = true
			}
		}
	}
	return max, found
}

func countSlice(nulls *batch.Bitmap, sel []uint16, vecLen int) int64 {
	if sel != nil {
		if nulls.NullCount() == 0 {
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
	if nulls.NullCount() == 0 {
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

// selOrRange converts a selection vector to []int, or generates 0..n-1.
// Used by min/max which iterate less frequently than sum.
func selOrRange(sel []uint16, n int) []int {
	if sel != nil {
		out := make([]int, len(sel))
		for i, idx := range sel {
			out[i] = int(idx)
		}
		return out
	}
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// --- Row-level updaters (for grouped aggregation inner loop) ---

func sumRowInt64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumI64 += vec.Int64Data[row]
		acc.Count++
	}
}

func sumRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumI64 += int64(vec.Int32Data[row])
		acc.Count++
	}
}

func sumRowFloat64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumF64 += vec.Float64Data[row]
		acc.Count++
		acc.IsFloat = true
	}
}

func sumRowFloat32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumF64 += float64(vec.Float32Data[row])
		acc.Count++
		acc.IsFloat = true
	}
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

func minRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := int64(vec.Int32Data[row])
		if !acc.HasMin || v < acc.MinI64 {
			acc.MinI64 = v
			acc.HasMin = true
		}
	}
}

func minRowFloat64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.Float64Data[row]
		if !acc.HasMin || v < acc.MinF64 {
			acc.MinF64 = v
			acc.HasMin = true
			acc.IsFloat = true
		}
	}
}

func minRowFloat32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := float64(vec.Float32Data[row])
		if !acc.HasMin || v < acc.MinF64 {
			acc.MinF64 = v
			acc.HasMin = true
			acc.IsFloat = true
		}
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

func maxRowInt32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := int64(vec.Int32Data[row])
		if !acc.HasMax || v > acc.MaxI64 {
			acc.MaxI64 = v
			acc.HasMax = true
		}
	}
}

func maxRowFloat64(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.Float64Data[row]
		if !acc.HasMax || v > acc.MaxF64 {
			acc.MaxF64 = v
			acc.HasMax = true
			acc.IsFloat = true
		}
	}
}

func maxRowFloat32(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := float64(vec.Float32Data[row])
		if !acc.HasMax || v > acc.MaxF64 {
			acc.MaxF64 = v
			acc.HasMax = true
			acc.IsFloat = true
		}
	}
}

// --- Resolve functions (called once at query init) ---

// ResolveRowSum returns a row-level sum updater for the given column type.
func ResolveRowSum(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		return sumRowInt64
	case batch.TypeInt32:
		return sumRowInt32
	case batch.TypeFloat64:
		return sumRowFloat64
	case batch.TypeFloat32:
		return sumRowFloat32
	default:
		return nil
	}
}

// ResolveRowMin returns a row-level min updater for the given column type.
func ResolveRowMin(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		return minRowInt64
	case batch.TypeInt32:
		return minRowInt32
	case batch.TypeFloat64:
		return minRowFloat64
	case batch.TypeFloat32:
		return minRowFloat32
	default:
		return nil
	}
}

// ResolveRowMax returns a row-level max updater for the given column type.
func ResolveRowMax(typ batch.TypeID) RowAggUpdater {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		return maxRowInt64
	case batch.TypeInt32:
		return maxRowInt32
	case batch.TypeFloat64:
		return maxRowFloat64
	case batch.TypeFloat32:
		return maxRowFloat32
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

// ResolveBatchSum returns a batch-level sum kernel for the given column type.
func ResolveBatchSum(typ batch.TypeID) BatchAggKernel {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
			s, c := sumSlice(vec.Int64Data, &vec.Nulls, sel, vecLen)
			acc.SumI64 += s
			acc.Count += c
		}
	case batch.TypeInt32:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
			s, c := sumSlice(vec.Int32Data, &vec.Nulls, sel, vecLen)
			acc.SumI64 += int64(s)
			acc.Count += c
		}
	case batch.TypeFloat64:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
			s, c := sumSlice(vec.Float64Data, &vec.Nulls, sel, vecLen)
			acc.SumF64 += s
			acc.Count += c
			acc.IsFloat = true
		}
	case batch.TypeFloat32:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
			s, c := sumSlice(vec.Float32Data, &vec.Nulls, sel, vecLen)
			acc.SumF64 += float64(s)
			acc.Count += c
			acc.IsFloat = true
		}
	default:
		return nil
	}
}

// ResolveBatchCount returns a batch-level count kernel.
func ResolveBatchCount() BatchAggKernel {
	return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
		acc.Count += countSlice(&vec.Nulls, sel, vecLen)
	}
}
