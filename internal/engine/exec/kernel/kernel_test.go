package kernel

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func makeIntVec(vals []int64, nulls []int) *batch.Vector {
	v := batch.NewVector(batch.TypeInt64, len(vals))
	for i, val := range vals {
		v.Int64Data[i] = val
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return v
}

func makeFloatVec(vals []float64, nulls []int) *batch.Vector {
	v := batch.NewVector(batch.TypeFloat64, len(vals))
	for i, val := range vals {
		v.Float64Data[i] = val
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return v
}

func makeStringVec(vals []string) *batch.Vector {
	v := batch.NewVector(batch.TypeString, len(vals))
	for i, val := range vals {
		v.BytesData.Set(i, []byte(val))
	}
	return v
}

func TestSumSliceInt64(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40, 50}, nil)
	k := ResolveBatchSum(batch.TypeInt64)
	var acc Accumulator
	k(&acc, vec, nil, 5)
	if acc.SumI64 != 150 {
		t.Fatalf("expected sum=150, got %d", acc.SumI64)
	}
	if acc.Count != 5 {
		t.Fatalf("expected count=5, got %d", acc.Count)
	}
}

func TestSumSliceInt64WithNulls(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40, 50}, []int{1, 3})
	k := ResolveBatchSum(batch.TypeInt64)
	var acc Accumulator
	k(&acc, vec, nil, 5)
	if acc.SumI64 != 90 { // 10+30+50
		t.Fatalf("expected sum=90, got %d", acc.SumI64)
	}
	if acc.Count != 3 {
		t.Fatalf("expected count=3, got %d", acc.Count)
	}
}

func TestSumSliceFloat64(t *testing.T) {
	vec := makeFloatVec([]float64{1.5, 2.5, 3.0}, nil)
	k := ResolveBatchSum(batch.TypeFloat64)
	var acc Accumulator
	k(&acc, vec, nil, 3)
	if acc.SumF64 != 7.0 {
		t.Fatalf("expected sum=7.0, got %f", acc.SumF64)
	}
	if !acc.IsFloat {
		t.Fatal("expected IsFloat=true")
	}
}

func TestSumSliceWithSelection(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40, 50}, nil)
	k := ResolveBatchSum(batch.TypeInt64)
	var acc Accumulator
	sel := []uint16{0, 2, 4}
	k(&acc, vec, sel, 5)
	if acc.SumI64 != 90 { // 10+30+50
		t.Fatalf("expected sum=90, got %d", acc.SumI64)
	}
	if acc.Count != 3 {
		t.Fatalf("expected count=3, got %d", acc.Count)
	}
}

func TestRowSumInt64(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, nil)
	k := ResolveRowSum(batch.TypeInt64)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.SumI64 != 60 {
		t.Fatalf("expected sum=60, got %d", acc.SumI64)
	}
}

func TestRowMinMax(t *testing.T) {
	vec := makeIntVec([]int64{30, 10, 50, 20, 40}, nil)
	minK := ResolveRowMin(batch.TypeInt64)
	maxK := ResolveRowMax(batch.TypeInt64)
	var minAcc, maxAcc Accumulator
	for i := 0; i < 5; i++ {
		minK(&minAcc, vec, i)
		maxK(&maxAcc, vec, i)
	}
	if minAcc.MinI64 != 10 {
		t.Fatalf("expected min=10, got %d", minAcc.MinI64)
	}
	if maxAcc.MaxI64 != 50 {
		t.Fatalf("expected max=50, got %d", maxAcc.MaxI64)
	}
}

func TestRowCount(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, []int{1})
	k := ResolveRowCount(false)
	var acc Accumulator
	for i := 0; i < 3; i++ {
		k(&acc, vec, i)
	}
	if acc.Count != 2 {
		t.Fatalf("expected count=2, got %d", acc.Count)
	}

	// COUNT(*)
	kStar := ResolveRowCount(true)
	var acc2 Accumulator
	for i := 0; i < 3; i++ {
		kStar(&acc2, vec, i)
	}
	if acc2.Count != 3 {
		t.Fatalf("expected count=3, got %d", acc2.Count)
	}
}

func TestFilterKernelInt64(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40, 50}, nil)
	k := ResolveFilterKernel(batch.TypeInt64, OpGt, int64(25))
	out := make([]uint16, 0, 5)
	result := k(vec, nil, 5, out)
	if len(result) != 3 {
		t.Fatalf("expected 3 matches, got %d", len(result))
	}
	// Should match indices 2,3,4 (values 30,40,50)
	for i, idx := range result {
		expected := []uint16{2, 3, 4}
		if idx != expected[i] {
			t.Fatalf("expected index %d, got %d", expected[i], idx)
		}
	}
}

func TestFilterKernelFloat64(t *testing.T) {
	vec := makeFloatVec([]float64{1.0, 2.5, 3.0, 0.5}, nil)
	k := ResolveFilterKernel(batch.TypeFloat64, OpLe, float64(2.5))
	out := make([]uint16, 0, 4)
	result := k(vec, nil, 4, out)
	if len(result) != 3 { // 1.0, 2.5, 0.5
		t.Fatalf("expected 3 matches, got %d", len(result))
	}
}

func TestFilterKernelString(t *testing.T) {
	vec := makeStringVec([]string{"alice", "bob", "charlie"})
	k := ResolveFilterKernel(batch.TypeString, OpEq, "bob")
	out := make([]uint16, 0, 3)
	result := k(vec, nil, 3, out)
	if len(result) != 1 || result[0] != 1 {
		t.Fatalf("expected [1], got %v", result)
	}
}

func TestFilterKernelWithSelection(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30, 40, 50}, nil)
	k := ResolveFilterKernel(batch.TypeInt64, OpGt, int64(15))
	sel := []uint16{0, 2, 4} // only consider 10, 30, 50
	out := make([]uint16, 0, 3)
	result := k(vec, sel, 5, out)
	if len(result) != 2 { // 30, 50
		t.Fatalf("expected 2 matches, got %d", len(result))
	}
}

func TestFilterKernelWithNulls(t *testing.T) {
	vec := makeIntVec([]int64{10, 20, 30}, []int{1})
	k := ResolveFilterKernel(batch.TypeInt64, OpGt, int64(5))
	out := make([]uint16, 0, 3)
	result := k(vec, nil, 3, out)
	if len(result) != 2 { // 10, 30 (20 is null)
		t.Fatalf("expected 2 matches, got %d", len(result))
	}
}

func TestSortCompareInt64(t *testing.T) {
	a := makeIntVec([]int64{30}, nil)
	b := makeIntVec([]int64{20}, nil)
	k := ResolveSortCompare(batch.TypeInt64)
	if k(a, 0, b, 0) != 1 {
		t.Fatal("expected 30 > 20")
	}
	if k(b, 0, a, 0) != -1 {
		t.Fatal("expected 20 < 30")
	}
	if k(a, 0, a, 0) != 0 {
		t.Fatal("expected equal")
	}
}

func TestSortCompareWithNulls(t *testing.T) {
	a := makeIntVec([]int64{10}, []int{0})
	b := makeIntVec([]int64{20}, nil)
	k := ResolveSortCompare(batch.TypeInt64)
	if k(a, 0, b, 0) != -1 {
		t.Fatal("null should sort before non-null")
	}
	if k(b, 0, a, 0) != 1 {
		t.Fatal("non-null should sort after null")
	}
}

func TestAccumulatorFinals(t *testing.T) {
	// Int sum
	acc := Accumulator{SumI64: 100, Count: 5}
	if acc.FinalSum().(int64) != 100 {
		t.Fatalf("expected int64 sum=100, got %v", acc.FinalSum())
	}
	if acc.FinalAvg().(float64) != 20.0 {
		t.Fatalf("expected avg=20.0, got %v", acc.FinalAvg())
	}

	// Float sum
	fAcc := Accumulator{SumF64: 7.5, Count: 3, IsFloat: true}
	if fAcc.FinalSum().(float64) != 7.5 {
		t.Fatalf("expected float64 sum=7.5, got %v", fAcc.FinalSum())
	}

	// Zero count
	zAcc := Accumulator{}
	if zAcc.FinalSum() != nil {
		t.Fatal("expected nil for zero-count sum")
	}
	if zAcc.FinalAvg() != nil {
		t.Fatal("expected nil for zero-count avg")
	}
}

// --- Benchmarks ---

func benchVec(n int) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
		{Name: "category", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, n)
	cats := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < n; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].Float64Data[i] = float64(i) * 1.5
		b.Columns[1].Nulls.SetValid(i)
		b.Columns[2].BytesData.Set(i, []byte(cats[i%len(cats)]))
		b.Columns[2].Nulls.SetValid(i)
	}
	return b
}

func BenchmarkBatchSumInt64(b *testing.B) {
	bb := benchVec(2048)
	vec := bb.Columns[0]
	k := ResolveBatchSum(batch.TypeInt64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var acc Accumulator
		k(&acc, vec, nil, 2048)
	}
}

func BenchmarkBatchSumFloat64(b *testing.B) {
	bb := benchVec(2048)
	vec := bb.Columns[1]
	k := ResolveBatchSum(batch.TypeFloat64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var acc Accumulator
		k(&acc, vec, nil, 2048)
	}
}

func BenchmarkRowSumInt64(b *testing.B) {
	bb := benchVec(2048)
	vec := bb.Columns[0]
	k := ResolveRowSum(batch.TypeInt64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var acc Accumulator
		for j := 0; j < 2048; j++ {
			k(&acc, vec, j)
		}
	}
}

func BenchmarkFilterKernelInt64(b *testing.B) {
	bb := benchVec(2048)
	vec := bb.Columns[0]
	k := ResolveFilterKernel(batch.TypeInt64, OpGt, int64(1000))
	outSel := make([]uint16, 0, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k(vec, nil, 2048, outSel)
	}
}

func BenchmarkSortCompareInt64(b *testing.B) {
	bb := benchVec(2048)
	vec := bb.Columns[0]
	k := ResolveSortCompare(batch.TypeInt64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			k(vec, j, vec, 2047-j)
		}
	}
}
