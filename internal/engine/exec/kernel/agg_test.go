package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

func makeInt32Vec(vals []int32, nulls []int) *batch.Vector {
	v := batch.NewVector(batch.TypeInt32, len(vals))
	for i, val := range vals {
		v.Int32Data[i] = val
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return v
}

func makeFloat32Vec(vals []float32, nulls []int) *batch.Vector {
	v := batch.NewVector(batch.TypeFloat32, len(vals))
	for i, val := range vals {
		v.Float32Data[i] = val
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return v
}

func makeDecimalVec(vals []int64, nulls []int, scale int) *batch.Vector {
	v := batch.NewVectorWithScale(batch.TypeDecimal, len(vals), scale)
	for i, val := range vals {
		v.DecimalData.Data[i] = batch.Int128From(val)
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return v
}

// --- Row-level sum tests for all types ---

func TestSumRowInt32(t *testing.T) {
	vec := makeInt32Vec([]int32{10, 20, 30}, nil)
	k := ResolveRowSum(batch.TypeInt32)
	if k == nil {
		t.Fatal("expected non-nil for Int32")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.SumI64 != 60 {
		t.Fatalf("expected sum=60, got %d", acc.SumI64)
	}
}

func TestSumRowFloat64(t *testing.T) {
	vec := makeFloatVec([]float64{1.5, 2.5, 3.0}, nil)
	k := ResolveRowSum(batch.TypeFloat64)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.SumF64 != 7.0 {
		t.Fatalf("expected sum=7.0, got %f", acc.SumF64)
	}
	if !acc.IsFloat {
		t.Fatal("expected IsFloat true")
	}
}

func TestSumRowFloat32(t *testing.T) {
	vec := makeFloat32Vec([]float32{1.0, 2.0, 3.0}, nil)
	k := ResolveRowSum(batch.TypeFloat32)
	if k == nil {
		t.Fatal("expected non-nil for Float32")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.SumF64 != 6.0 {
		t.Fatalf("expected sum=6.0, got %f", acc.SumF64)
	}
	if !acc.IsFloat {
		t.Fatal("expected IsFloat true")
	}
}

func TestSumRowDecimal(t *testing.T) {
	vec := makeDecimalVec([]int64{100, 200, 300}, nil, 2)
	k := ResolveRowSum(batch.TypeDecimal)
	if k == nil {
		t.Fatal("expected non-nil for Decimal")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if !acc.IsDecimal {
		t.Fatal("expected IsDecimal true")
	}
	if acc.Count != 3 {
		t.Fatalf("expected count=3, got %d", acc.Count)
	}
}

func TestSumRowWithNulls(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, []int{1})
	k := ResolveRowSum(batch.TypeInt64)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.SumI64 != 40 { // 10+30
		t.Fatalf("expected sum=40, got %d", acc.SumI64)
	}
	if acc.Count != 2 {
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

func TestResolveRowSumUnsupported(t *testing.T) {
	k := ResolveRowSum(batch.TypeString)
	if k != nil {
		t.Fatal("expected nil for unsupported type")
	}
}

// --- Row-level min tests for all types ---

func TestMinRowInt32(t *testing.T) {
	vec := makeInt32Vec([]int32{30, 10, 50}, nil)
	k := ResolveRowMin(batch.TypeInt32)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MinI64 != 10 {
		t.Fatalf("expected min=10, got %d", acc.MinI64)
	}
}

func TestMinRowFloat64(t *testing.T) {
	vec := makeFloatVec([]float64{3.0, 1.5, 2.5}, nil)
	k := ResolveRowMin(batch.TypeFloat64)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MinF64 != 1.5 {
		t.Fatalf("expected min=1.5, got %f", acc.MinF64)
	}
}

func TestMinRowFloat32(t *testing.T) {
	vec := makeFloat32Vec([]float32{3.0, 1.5, 2.5}, nil)
	k := ResolveRowMin(batch.TypeFloat32)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MinF64 != 1.5 {
		t.Fatalf("expected min=1.5, got %f", acc.MinF64)
	}
}

func TestMinRowDecimal(t *testing.T) {
	vec := makeDecimalVec([]int64{300, 100, 200}, nil, 2)
	k := ResolveRowMin(batch.TypeDecimal)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if !acc.HasMin {
		t.Fatal("expected HasMin true")
	}
	if !acc.IsDecimal {
		t.Fatal("expected IsDecimal true")
	}
}

func TestMinRowWithNulls(t *testing.T) {
	vec := makeIntVec([]int64{30, 10, 50}, []int{1})
	k := ResolveRowMin(batch.TypeInt64)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MinI64 != 30 { // 10 is null, so min is 30
		t.Fatalf("expected min=30, got %d", acc.MinI64)
	}
}

func TestResolveRowMinUnsupported(t *testing.T) {
	if k := ResolveRowMin(batch.TypeString); k == nil {
		t.Fatal("string min is supported and must resolve")
	}
	if k := ResolveRowMin(batch.TypeArray); k != nil {
		t.Fatal("expected nil for unsupported type")
	}
}

// --- Row-level max tests for all types ---

func TestMaxRowInt32(t *testing.T) {
	vec := makeInt32Vec([]int32{30, 10, 50}, nil)
	k := ResolveRowMax(batch.TypeInt32)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MaxI64 != 50 {
		t.Fatalf("expected max=50, got %d", acc.MaxI64)
	}
}

func TestMaxRowFloat64(t *testing.T) {
	vec := makeFloatVec([]float64{3.0, 1.5, 2.5}, nil)
	k := ResolveRowMax(batch.TypeFloat64)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MaxF64 != 3.0 {
		t.Fatalf("expected max=3.0, got %f", acc.MaxF64)
	}
}

func TestMaxRowFloat32(t *testing.T) {
	vec := makeFloat32Vec([]float32{3.0, 1.5, 2.5}, nil)
	k := ResolveRowMax(batch.TypeFloat32)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.MaxF64 != 3.0 {
		t.Fatalf("expected max=3.0, got %f", acc.MaxF64)
	}
}

func TestMaxRowDecimal(t *testing.T) {
	vec := makeDecimalVec([]int64{100, 300, 200}, nil, 2)
	k := ResolveRowMax(batch.TypeDecimal)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if !acc.HasMax {
		t.Fatal("expected HasMax true")
	}
}

func TestResolveRowMaxUnsupported(t *testing.T) {
	if k := ResolveRowMax(batch.TypeString); k == nil {
		t.Fatal("string max is supported and must resolve")
	}
	if k := ResolveRowMax(batch.TypeArray); k != nil {
		t.Fatal("expected nil for unsupported type")
	}
}

// --- Accumulator FinalMin and FinalMax ---

func TestFinalMin(t *testing.T) {
	t.Run("NoMin", func(t *testing.T) {
		acc := Accumulator{}
		if acc.FinalMin() != nil {
			t.Fatal("expected nil for no min")
		}
	})

	t.Run("IntMin", func(t *testing.T) {
		acc := Accumulator{HasMin: true, MinI64: 5}
		if acc.FinalMin().(int64) != 5 {
			t.Fatalf("expected 5, got %v", acc.FinalMin())
		}
	})

	t.Run("FloatMin", func(t *testing.T) {
		acc := Accumulator{HasMin: true, MinF64: 1.5, IsFloat: true}
		if acc.FinalMin().(float64) != 1.5 {
			t.Fatalf("expected 1.5, got %v", acc.FinalMin())
		}
	})

	t.Run("DecimalMin", func(t *testing.T) {
		acc := Accumulator{HasMin: true, MinDec: batch.Int128From(100), IsDecimal: true, DecScale: 2}
		result := acc.FinalMin()
		if result == nil {
			t.Fatal("expected non-nil")
		}
	})
}

func TestFinalMax(t *testing.T) {
	t.Run("NoMax", func(t *testing.T) {
		acc := Accumulator{}
		if acc.FinalMax() != nil {
			t.Fatal("expected nil for no max")
		}
	})

	t.Run("IntMax", func(t *testing.T) {
		acc := Accumulator{HasMax: true, MaxI64: 100}
		if acc.FinalMax().(int64) != 100 {
			t.Fatalf("expected 100, got %v", acc.FinalMax())
		}
	})

	t.Run("FloatMax", func(t *testing.T) {
		acc := Accumulator{HasMax: true, MaxF64: 9.5, IsFloat: true}
		if acc.FinalMax().(float64) != 9.5 {
			t.Fatalf("expected 9.5, got %v", acc.FinalMax())
		}
	})

	t.Run("DecimalMax", func(t *testing.T) {
		acc := Accumulator{HasMax: true, MaxDec: batch.Int128From(500), IsDecimal: true, DecScale: 2}
		result := acc.FinalMax()
		if result == nil {
			t.Fatal("expected non-nil")
		}
	})
}

func TestFinalSumDecimal(t *testing.T) {
	acc := Accumulator{
		SumDec:    batch.Int128From(1000),
		Count:     3,
		IsDecimal: true,
		DecScale:  2,
	}
	result := acc.FinalSum()
	if result == nil {
		t.Fatal("expected non-nil")
	}
	// 1000 with scale 2 = 10.0
	if result.(float64) != 10.0 {
		t.Fatalf("expected 10.0, got %v", result)
	}
}

func TestFinalAvgDecimal(t *testing.T) {
	acc := Accumulator{
		SumDec:    batch.Int128From(1000),
		Count:     2,
		IsDecimal: true,
		DecScale:  2,
	}
	result := acc.FinalAvg()
	// 1000 / 2 with scale 2 = 10.0 / 2.0 = 5.0
	if result.(float64) != 5.0 {
		t.Fatalf("expected 5.0, got %v", result)
	}
}

// --- Batch-level sum tests for all types ---

func TestBatchSumInt32(t *testing.T) {
	vec := makeInt32Vec([]int32{10, 20, 30}, nil)
	k := ResolveBatchSum(batch.TypeInt32)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.SumI64 != 60 {
		t.Fatalf("expected sum=60, got %d", acc.SumI64)
	}
}

func TestBatchSumFloat32(t *testing.T) {
	vec := makeFloat32Vec([]float32{1.0, 2.0, 3.0}, nil)
	k := ResolveBatchSum(batch.TypeFloat32)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.SumF64 != 6.0 {
		t.Fatalf("expected sum=6.0, got %f", acc.SumF64)
	}
}

func TestBatchSumDecimal(t *testing.T) {
	vec := makeDecimalVec([]int64{100, 200, 300}, nil, 2)
	k := ResolveBatchSum(batch.TypeDecimal)
	if k == nil {
		t.Fatal("expected non-nil")
	}
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.Count != 3 {
		t.Fatalf("expected count=3, got %d", acc.Count)
	}
	if !acc.IsDecimal {
		t.Fatal("expected IsDecimal true")
	}
}

func TestBatchSumDecimalWithSel(t *testing.T) {
	vec := makeDecimalVec([]int64{100, 200, 300}, nil, 2)
	k := ResolveBatchSum(batch.TypeDecimal)
	var acc Accumulator
	sel := []uint32{0, 2}
	k(&acc, vec, sel, 3)
	if acc.Count != 2 {
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

func TestBatchSumDecimalWithNulls(t *testing.T) {
	vec := makeDecimalVec([]int64{100, 200, 300}, []int{1}, 2)
	k := ResolveBatchSum(batch.TypeDecimal)
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.Count != 2 {
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

func TestBatchSumUnsupported(t *testing.T) {
	k := ResolveBatchSum(batch.TypeString)
	if k != nil {
		t.Fatal("expected nil for unsupported type")
	}
}

// --- Batch-level count ---

func TestBatchCount(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, []int{1})
	k := ResolveBatchCount()
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.Count != 2 { // index 1 is null
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

func TestBatchCountWithSel(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40}, []int{1})
	k := ResolveBatchCount()
	var acc Accumulator
	sel := []uint32{0, 1, 3}
	k(&acc, vec, sel, 4)
	if acc.Count != 2 { // index 1 is null, so 0 and 3 = 2
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

func TestBatchCountNoNulls(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, nil)
	k := ResolveBatchCount()
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.Count != 3 {
		t.Fatalf("expected count=3, got %d", acc.Count)
	}
}

func TestBatchCountNoNullsWithSel(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, nil)
	k := ResolveBatchCount()
	var acc Accumulator
	sel := []uint32{0, 2}
	k(&acc, vec, sel, 3)
	if acc.Count != 2 {
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

// --- sumSlice with selection+nulls ---

func TestBatchSumInt64SelWithNulls(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40, 50}, []int{2})
	k := ResolveBatchSum(batch.TypeInt64)
	var acc Accumulator
	sel := []uint32{0, 2, 4}
	k(&acc, vec, sel, 5)
	// sel={0,2,4}, index 2 is null, so 10+50=60
	if acc.SumI64 != 60 {
		t.Fatalf("expected sum=60, got %d", acc.SumI64)
	}
	if acc.Count != 2 {
		t.Fatalf("expected count=2, got %d", acc.Count)
	}
}

// --- Sort compare tests for additional types ---

func TestSortCompareInt32(t *testing.T) {
	a := makeInt32Vec([]int32{30}, nil)
	b := makeInt32Vec([]int32{20}, nil)
	k := ResolveSortCompare(batch.TypeInt32)
	if k(a, 0, b, 0) != 1 {
		t.Fatal("expected 30 > 20")
	}
	if k(b, 0, a, 0) != -1 {
		t.Fatal("expected 20 < 30")
	}
	c := makeInt32Vec([]int32{30}, nil)
	if k(a, 0, c, 0) != 0 {
		t.Fatal("expected equal")
	}
}

func TestSortCompareFloat64(t *testing.T) {
	a := makeFloatVec([]float64{3.0}, nil)
	b := makeFloatVec([]float64{2.0}, nil)
	k := ResolveSortCompare(batch.TypeFloat64)
	if k(a, 0, b, 0) != 1 {
		t.Fatal("expected 3.0 > 2.0")
	}
	if k(b, 0, a, 0) != -1 {
		t.Fatal("expected 2.0 < 3.0")
	}
	c := makeFloatVec([]float64{3.0}, nil)
	if k(a, 0, c, 0) != 0 {
		t.Fatal("expected equal")
	}
}

func TestSortCompareFloat32(t *testing.T) {
	a := makeFloat32Vec([]float32{3.0}, nil)
	b := makeFloat32Vec([]float32{2.0}, nil)
	k := ResolveSortCompare(batch.TypeFloat32)
	if k(a, 0, b, 0) != 1 {
		t.Fatal("expected 3.0 > 2.0")
	}
	if k(b, 0, a, 0) != -1 {
		t.Fatal("expected 2.0 < 3.0")
	}
	c := makeFloat32Vec([]float32{3.0}, nil)
	if k(a, 0, c, 0) != 0 {
		t.Fatal("expected equal")
	}
}

func TestSortCompareString(t *testing.T) {
	a := makeStringVec([]string{"charlie"})
	b := makeStringVec([]string{"alice"})
	k := ResolveSortCompare(batch.TypeString)
	if k(a, 0, b, 0) != 1 {
		t.Fatal("expected charlie > alice")
	}
	if k(b, 0, a, 0) != -1 {
		t.Fatal("expected alice < charlie")
	}
	c := makeStringVec([]string{"charlie"})
	if k(a, 0, c, 0) != 0 {
		t.Fatal("expected equal")
	}
}

func TestSortCompareBool(t *testing.T) {
	makeBoolVec := func(val bool) *batch.Vector {
		v := batch.NewVector(batch.TypeBool, 1)
		v.BoolData[0] = val
		return v
	}
	k := ResolveSortCompare(batch.TypeBool)

	trueVec := makeBoolVec(true)
	falseVec := makeBoolVec(false)

	if k(trueVec, 0, falseVec, 0) != 1 {
		t.Fatal("expected true > false")
	}
	if k(falseVec, 0, trueVec, 0) != -1 {
		t.Fatal("expected false < true")
	}
	if k(trueVec, 0, trueVec, 0) != 0 {
		t.Fatal("expected equal")
	}
}

func TestSortCompareWithNullsBothNull(t *testing.T) {
	a := makeIntVec([]int64{10}, []int{0})
	b := makeIntVec([]int64{20}, []int{0})
	k := ResolveSortCompare(batch.TypeInt64)
	if k(a, 0, b, 0) != 0 {
		t.Fatal("expected both null = equal")
	}
}

func TestSortCompareStringWithNulls(t *testing.T) {
	a := makeStringVec([]string{"hello"})
	a.Nulls.SetNull(0)
	b := makeStringVec([]string{"world"})
	k := ResolveSortCompare(batch.TypeString)
	if k(a, 0, b, 0) != -1 {
		t.Fatal("null should sort before non-null")
	}
}

func TestSortCompareDefaultType(t *testing.T) {
	// The default arm used to hand back a closure that reported every pair
	// EQUAL. SortMergeJoin decides key equality with these kernels, so that
	// answer is not a degraded sort — it is a cross product presented as an
	// inner join, and it is what ARRAY/ROW/MAP/VECTOR got until #415. The
	// resolvers now refuse instead, which is what makes the `cmp == nil`
	// guard in sort_merge_join.go a live check.
	if k := ResolveSortCompare(99); k != nil { // nonexistent type
		t.Fatal("an unknown type must resolve to nil, not to an always-equal comparator")
	}
}

// --- Filter kernel edge cases ---

func TestFilterKernelAllOps(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, nil)

	tests := []struct {
		op    CompareOp
		val   int64
		count int
	}{
		{OpEq, 20, 1},
		{OpNe, 20, 2},
		{OpLt, 20, 1},
		{OpLe, 20, 2},
		{OpGt, 20, 1},
		{OpGe, 20, 2},
	}

	for _, tt := range tests {
		k := ResolveFilterKernel(batch.TypeInt64, tt.op, tt.val)
		out := make([]uint32, 0, 3)
		result := k(vec, nil, 3, out)
		if len(result) != tt.count {
			t.Fatalf("op=%d val=%d: expected %d matches, got %d", tt.op, tt.val, tt.count, len(result))
		}
	}
}

func TestFilterKernelFloat32(t *testing.T) {
	vec := makeFloat32Vec([]float32{1.0, 2.0, 3.0}, nil)
	k := ResolveFilterKernel(batch.TypeFloat32, OpGt, float64(1.5))
	out := make([]uint32, 0, 3)
	result := k(vec, nil, 3, out)
	if len(result) != 2 { // 2.0, 3.0
		t.Fatalf("expected 2 matches, got %d", len(result))
	}
}

func TestFilterKernelInt32(t *testing.T) {
	vec := makeInt32Vec([]int32{10, 20, 30}, nil)
	k := ResolveFilterKernel(batch.TypeInt32, OpEq, int64(20))
	out := make([]uint32, 0, 3)
	result := k(vec, nil, 3, out)
	if len(result) != 1 {
		t.Fatalf("expected 1 match, got %d", len(result))
	}
}

func TestFilterKernelUnsupported(t *testing.T) {
	k := ResolveFilterKernel(99, OpEq, int64(1))
	if k != nil {
		t.Fatal("expected nil for unsupported type")
	}
}

func TestFilterKernelStringWithSel(t *testing.T) {
	vec := makeStringVec([]string{"alice", "bob", "charlie"})
	k := ResolveFilterKernel(batch.TypeString, OpEq, "bob")
	sel := []uint32{0, 1}
	out := make([]uint32, 0, 2)
	result := k(vec, sel, 3, out)
	if len(result) != 1 || result[0] != 1 {
		t.Fatalf("expected [1], got %v", result)
	}
}

func TestFilterKernelDefaultCompareOp(t *testing.T) {
	// Test default case in resolveCompare
	cmp := resolveCompare[int64](CompareOp(99))
	if cmp(1, 2) {
		t.Fatal("default compare should return false")
	}
}

// --- Type conversion helpers ---

func TestToInt64(t *testing.T) {
	tests := []struct {
		input any
		want  int64
	}{
		{int64(42), 42},
		{int(10), 10},
		{int32(5), 5},
		{float64(3.7), 3},
		{"string", 0},
		{nil, 0},
	}
	for _, tt := range tests {
		got := toInt64(tt.input)
		if got != tt.want {
			t.Fatalf("toInt64(%v) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input any
		want  float64
	}{
		{float64(3.14), 3.14},
		{float32(2.5), 2.5},
		{int64(10), 10.0},
		{int(5), 5.0},
		{"string", 0},
		{nil, 0},
	}
	for _, tt := range tests {
		got := toFloat64(tt.input)
		if got != tt.want {
			t.Fatalf("toFloat64(%v) = %f, want %f", tt.input, got, tt.want)
		}
	}
}

func TestToString(t *testing.T) {
	if toString("hello") != "hello" {
		t.Fatal("expected hello")
	}
	if toString(42) != "" {
		t.Fatal("expected empty for non-string")
	}
}

// --- Network type parsing ---

func TestParseIPv4ToInt64(t *testing.T) {
	result := parseIPv4ToInt64("192.168.1.1")
	if result == 0 {
		t.Fatal("expected non-zero for valid IP")
	}

	result = parseIPv4ToInt64("not-an-ip")
	if result != 0 {
		t.Fatalf("expected 0 for invalid IP, got %d", result)
	}

	result = parseIPv4ToInt64("::1") // IPv6 address
	if result != 0 {
		t.Fatalf("expected 0 for IPv6, got %d", result)
	}
}

func TestParseMACToInt64(t *testing.T) {
	result := parseMACToInt64("aa:bb:cc:dd:ee:ff")
	if result == 0 {
		t.Fatal("expected non-zero for valid MAC")
	}

	result = parseMACToInt64("not-a-mac")
	if result != 0 {
		t.Fatalf("expected 0 for invalid MAC, got %d", result)
	}
}

func TestParseIPv6ToRawString(t *testing.T) {
	result := parseIPv6ToRawString("::1")
	if result == "" {
		t.Fatal("expected non-empty for valid IPv6")
	}

	result = parseIPv6ToRawString("not-an-ip")
	if result != "" {
		t.Fatal("expected empty for invalid IP")
	}
}

// --- Filter kernel for various types ---

func TestFilterKernelIPv4(t *testing.T) {
	v := batch.NewVector(batch.TypeIPv4, 2)
	v.SetValue(0, "192.168.1.1")
	v.SetValue(1, "10.0.0.1")

	k := ResolveFilterKernel(batch.TypeIPv4, OpEq, "192.168.1.1")
	if k == nil {
		t.Fatal("expected non-nil kernel")
	}
	out := make([]uint32, 0, 2)
	result := k(v, nil, 2, out)
	if len(result) != 1 || result[0] != 0 {
		t.Fatalf("expected [0], got %v", result)
	}
}

func TestFilterKernelMAC(t *testing.T) {
	v := batch.NewVector(batch.TypeMAC, 2)
	v.SetValue(0, "aa:bb:cc:dd:ee:ff")
	v.SetValue(1, "00:11:22:33:44:55")

	k := ResolveFilterKernel(batch.TypeMAC, OpEq, "aa:bb:cc:dd:ee:ff")
	if k == nil {
		t.Fatal("expected non-nil kernel")
	}
	out := make([]uint32, 0, 2)
	result := k(v, nil, 2, out)
	if len(result) != 1 || result[0] != 0 {
		t.Fatalf("expected [0], got %v", result)
	}
}

func TestFilterKernelPort(t *testing.T) {
	v := batch.NewVector(batch.TypePort, 3)
	v.Int32Data[0] = 80
	v.Int32Data[1] = 443
	v.Int32Data[2] = 22

	k := ResolveFilterKernel(batch.TypePort, OpEq, int64(443))
	out := make([]uint32, 0, 3)
	result := k(v, nil, 3, out)
	if len(result) != 1 || result[0] != 1 {
		t.Fatalf("expected [1], got %v", result)
	}
}

func TestFilterKernelDuration(t *testing.T) {
	v := batch.NewVector(batch.TypeDuration, 3)
	v.Int64Data[0] = 100
	v.Int64Data[1] = 200
	v.Int64Data[2] = 300

	k := ResolveFilterKernel(batch.TypeDuration, OpGt, int64(150))
	out := make([]uint32, 0, 3)
	result := k(v, nil, 3, out)
	if len(result) != 2 { // 200 and 300
		t.Fatalf("expected 2 matches, got %d", len(result))
	}
}

func TestFilterKernelDate(t *testing.T) {
	v := batch.NewVector(batch.TypeDate, 3)
	v.Int32Data[0] = 100
	v.Int32Data[1] = 200
	v.Int32Data[2] = 300

	k := ResolveFilterKernel(batch.TypeDate, OpLt, int64(200))
	out := make([]uint32, 0, 3)
	result := k(v, nil, 3, out)
	if len(result) != 1 { // 100
		t.Fatalf("expected 1 match, got %d", len(result))
	}
}
