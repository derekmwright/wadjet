package kernel

import (
	"fmt"
	"os"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

var debugStrKernel = os.Getenv("WADJET_DEBUG_STRKERNEL") == "1"

// Byte-backed and BOOL MIN/MAX kernels, and overflow-safe AVG kernels.
//
// MIN/MAX over string/bytes columns previously resolved to a nil updater —
// the aggregate silently skipped accumulation and returned NULL (ClickBench
// Q22, MIN(URL)). Comparisons use the zero-copy string view; only a new
// best value is copied out of the batch buffer (StringValue).
//
// The same nil updater kept answering NULL for the other three BytesColumn
// types — IPV6, CIDR and UUID — and for BOOL, long after STRING and BYTES
// were fixed, because the resolvers named two types where five share the
// storage (#417). The byte order is the one kernel/sort.go already gives all
// five, so MIN agrees with the first row of an ORDER BY over the same column.
// The accumulator records WHICH of the five it holds (Accumulator.StrType),
// because the raw bytes of an IPV6 or a UUID are not text and only round-trip
// into their own vector as []byte.
//
// AVG over int64-class columns previously shared the int64 SUM kernel; the
// running sum wrapped for large values (AVG(UserID) over hash-like IDs) and
// the mean came out garbage. AVG-designated kernels accumulate float64
// instead — the result is a float64 mean anyway, and the relative error of
// float64 accumulation is far below any cross-engine tolerance. SUM keeps
// its exact int64 semantics. int32-class inputs keep the exact int64 sum
// (they cannot overflow it). These kernels set IsFloat so finalize, merge,
// and partial-spill all route through the float sum.

func minRowString(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		minStrUpdate(acc, vec, row)
	}
}
func minRowStringNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	minStrUpdate(acc, vec, row)
}
func minStrUpdate(acc *Accumulator, vec *batch.Vector, row int) {
	acc.IsString = true
	acc.StrType = vec.Type
	v := vec.BytesData.UnsafeStringValue(row)
	if !acc.HasMin || v < acc.MinStr {
		acc.MinStr = vec.BytesData.StringValue(row)
		acc.HasMin = true
	}
}

func maxRowString(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		maxStrUpdate(acc, vec, row)
	}
}
func maxRowStringNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	maxStrUpdate(acc, vec, row)
}
func maxStrUpdate(acc *Accumulator, vec *batch.Vector, row int) {
	acc.IsString = true
	acc.StrType = vec.Type
	v := vec.BytesData.UnsafeStringValue(row)
	if debugStrKernel && len(v) > 64 {
		panic(fmt.Sprintf("maxStrUpdate oversized: row=%d len=%d offsets=%d vecLen=%d type=%v", row, len(v), len(vec.BytesData.Offsets), vec.Len, vec.Type))
	}
	if !acc.HasMax || v > acc.MaxStr {
		acc.MaxStr = vec.BytesData.StringValue(row)
		acc.HasMax = true
	}
}

func minBatchString(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
	acc.IsString = true
	acc.StrType = vec.Type
	nulls := &vec.Nulls
	if sel != nil {
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				minStrUpdate(acc, vec, int(idx))
			}
		}
		return
	}
	for i := 0; i < vecLen; i++ {
		if !nulls.IsNullFast(i) {
			minStrUpdate(acc, vec, i)
		}
	}
}

func maxBatchString(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
	acc.IsString = true
	acc.StrType = vec.Type
	nulls := &vec.Nulls
	if sel != nil {
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				maxStrUpdate(acc, vec, int(idx))
			}
		}
		return
	}
	for i := 0; i < vecLen; i++ {
		if !nulls.IsNullFast(i) {
			maxStrUpdate(acc, vec, i)
		}
	}
}

// BOOL MIN/MAX. PostgreSQL has no min(boolean)/max(boolean) — it offers
// bool_and/bool_or instead — so there is no PG answer to follow here; the
// order is DuckDB's and kernel/sort.go's, false < true, which is also what
// `ORDER BY bool_col` already returns. Answering is the extension; the NULL
// this replaces was not an answer at all.
//
// The value rides in MinI64/MaxI64 as 0/1 rather than in a slot of its own:
// the accumulator already has an integer channel with a merge and a finalize,
// and IsBool is what tells FinalMin to hand it back as a bool.

func minRowBool(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		minBoolUpdate(acc, vec, row)
	}
}
func minRowBoolNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	minBoolUpdate(acc, vec, row)
}
func minBoolUpdate(acc *Accumulator, vec *batch.Vector, row int) {
	acc.IsBool = true
	v := int64(0)
	if vec.BoolData[row] {
		v = 1
	}
	if !acc.HasMin || v < acc.MinI64 {
		acc.MinI64 = v
		acc.HasMin = true
	}
}

func maxRowBool(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		maxBoolUpdate(acc, vec, row)
	}
}
func maxRowBoolNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	maxBoolUpdate(acc, vec, row)
}
func maxBoolUpdate(acc *Accumulator, vec *batch.Vector, row int) {
	acc.IsBool = true
	v := int64(0)
	if vec.BoolData[row] {
		v = 1
	}
	if !acc.HasMax || v > acc.MaxI64 {
		acc.MaxI64 = v
		acc.HasMax = true
	}
}

func minBatchBool(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
	acc.IsBool = true
	nulls := &vec.Nulls
	if sel != nil {
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				minBoolUpdate(acc, vec, int(idx))
			}
		}
		return
	}
	for i := 0; i < vecLen; i++ {
		if !nulls.IsNullFast(i) {
			minBoolUpdate(acc, vec, i)
		}
	}
}

func maxBatchBool(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
	acc.IsBool = true
	nulls := &vec.Nulls
	if sel != nil {
		for _, idx := range sel {
			if !nulls.IsNullFast(int(idx)) {
				maxBoolUpdate(acc, vec, int(idx))
			}
		}
		return
	}
	for i := 0; i < vecLen; i++ {
		if !nulls.IsNullFast(i) {
			maxBoolUpdate(acc, vec, i)
		}
	}
}

func avgRowInt64AsFloat(acc *Accumulator, vec *batch.Vector, row int) {
	if !vec.Nulls.IsNullFast(row) {
		acc.IsFloat = true
		acc.SumF64 += float64(vec.Int64Data[row])
		acc.Count++
	}
}
func avgRowInt64AsFloatNoNulls(acc *Accumulator, vec *batch.Vector, row int) {
	acc.IsFloat = true
	acc.SumF64 += float64(vec.Int64Data[row])
	acc.Count++
}

// isAvgFloatAccumType reports whether AVG over this input type accumulates
// in float64 instead of the shared int64 SUM kernel.
func isAvgFloatAccumType(typ batch.TypeID) bool {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return true
	}
	return false
}

// ResolveRowAvg returns a row-level updater for AVG. Differs from
// ResolveRowSum only for int64-class inputs (float64 accumulation).
func ResolveRowAvg(typ batch.TypeID) RowAggUpdater {
	if isAvgFloatAccumType(typ) {
		return avgRowInt64AsFloat
	}
	return ResolveRowSum(typ)
}

// ResolveRowAvgNoNulls is the no-null-check variant of ResolveRowAvg.
func ResolveRowAvgNoNulls(typ batch.TypeID) RowAggUpdater {
	if isAvgFloatAccumType(typ) {
		return avgRowInt64AsFloatNoNulls
	}
	return ResolveRowSumNoNulls(typ)
}

// ResolveBatchAvg returns a batch-level kernel for AVG. Differs from
// ResolveBatchSum only for int64-class inputs (float64 accumulation).
func ResolveBatchAvg(typ batch.TypeID) BatchAggKernel {
	if !isAvgFloatAccumType(typ) {
		return ResolveBatchSum(typ)
	}
	return func(acc *Accumulator, vec *batch.Vector, sel []uint32, vecLen int) {
		acc.IsFloat = true
		data := vec.Int64Data
		nulls := &vec.Nulls
		sum := 0.0
		var count int64
		if sel != nil {
			for _, idx := range sel {
				if !nulls.IsNullFast(int(idx)) {
					sum += float64(data[idx])
					count++
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				if !nulls.IsNullFast(i) {
					sum += float64(data[i])
					count++
				}
			}
		}
		acc.SumF64 += sum
		acc.Count += count
	}
}
