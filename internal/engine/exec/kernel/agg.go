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

// --- Decimal row updaters ---

func sumRowDecimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.SumDec = acc.SumDec.Add(vec.DecimalData.Data[row])
		acc.Count++
		acc.IsDecimal = true
		acc.DecScale = vec.DecimalData.Scale
	}
}

func minRowDecimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.DecimalData.Data[row]
		if !acc.HasMin || v.Less(acc.MinDec) {
			acc.MinDec = v
			acc.HasMin = true
			acc.IsDecimal = true
			acc.DecScale = vec.DecimalData.Scale
		}
	}
}

func maxRowDecimal(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		v := vec.DecimalData.Data[row]
		if !acc.HasMax || acc.MaxDec.Less(v) {
			acc.MaxDec = v
			acc.HasMax = true
			acc.IsDecimal = true
			acc.DecScale = vec.DecimalData.Scale
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
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
			s, c := sumSlice(vec.Int64Data, &vec.Nulls, sel, vecLen)
			acc.SumI64 += s
			acc.Count += c
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeDate:
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
	case batch.TypeDecimal:
		return func(acc *Accumulator, vec *batch.Vector, sel []uint16, vecLen int) {
			data := vec.DecimalData.Data
			nulls := &vec.Nulls
			if sel != nil {
				for _, idx := range sel {
					if !nulls.IsNullFast(int(idx)) {
						acc.SumDec = acc.SumDec.Add(data[idx])
						acc.Count++
					}
				}
			} else {
				for i := 0; i < vecLen; i++ {
					if !nulls.IsNullFast(i) {
						acc.SumDec = acc.SumDec.Add(data[i])
						acc.Count++
					}
				}
			}
			acc.IsDecimal = true
			acc.DecScale = vec.DecimalData.Scale
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
