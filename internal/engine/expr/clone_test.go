package expr

import (
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// constFloat64 is a test helper that implements Float64Expr and VecFloat64Expr
// for a constant value. Used to build expression trees in tests.
type constFloat64 struct{ val float64 }

func (c *constFloat64) Eval(*batch.RecordBatch, int) any              { return c.val }
func (c *constFloat64) EvalFloat64(_ *batch.RecordBatch, _ int) (float64, bool) { return c.val, true }
func (c *constFloat64) EvalFloat64Vec(_ *batch.RecordBatch, dst []float64, n int) bool {
	for i := 0; i < n; i++ {
		dst[i] = c.val
	}
	return false
}

// TestBinOpFloat64_CloneVec_ParallelSafety verifies that CloneVec creates
// independent scratch buffers so concurrent EvalFloat64Vec calls don't corrupt
// each other's results. This is a regression test for a data race where
// aggPreProject.Clone() shared BinOpFloat64 expression trees (with mutable
// vecBuf) across parallel pipeline workers.
func TestBinOpFloat64_CloneVec_ParallelSafety(t *testing.T) {
	// Build expression: a * (1 - b) * (1 + c)
	// This mirrors TPC-H Q1's sum_charge: l_extendedprice * (1 - l_discount) * (1 + l_tax)
	colA := &ColRef{Name: "a"}
	colB := &ColRef{Name: "b"}
	colC := &ColRef{Name: "c"}
	litOne := &constFloat64{val: 1.0}
	oneMinusB := &BinOpFloat64{Left: litOne, Right: colB, Op: "-"}
	onePlusC := &BinOpFloat64{Left: litOne, Right: colC, Op: "+"}
	aMulDisc := &BinOpFloat64{Left: colA, Right: oneMinusB, Op: "*"}
	fullExpr := &BinOpFloat64{Left: aMulDisc, Right: onePlusC, Op: "*"}

	// Create two clones (simulating two parallel workers)
	clone1 := fullExpr.CloneVec()
	clone2 := fullExpr.CloneVec()

	// Build two different batches
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
		{Name: "c", Type: parquet.TypeFloat64},
	}
	n := 1024
	b1 := batch.NewRecordBatch(schema, n)
	b2 := batch.NewRecordBatch(schema, n)
	for i := 0; i < n; i++ {
		b1.Columns[0].Float64Data[i] = 100.0           // a = 100
		b1.Columns[1].Float64Data[i] = 0.1             // b = 0.1
		b1.Columns[2].Float64Data[i] = 0.05            // c = 0.05
		b2.Columns[0].Float64Data[i] = 200.0           // a = 200
		b2.Columns[1].Float64Data[i] = 0.2             // b = 0.2
		b2.Columns[2].Float64Data[i] = 0.1             // c = 0.1
	}
	// Resolve column refs
	colA.EvalFloat64Vec(b1, make([]float64, n), n)
	colB.EvalFloat64Vec(b1, make([]float64, n), n)
	colC.EvalFloat64Vec(b1, make([]float64, n), n)

	// Expected: b1 = 100 * (1 - 0.1) * (1 + 0.05) = 100 * 0.9 * 1.05 = 94.5
	// Expected: b2 = 200 * (1 - 0.2) * (1 + 0.1)  = 200 * 0.8 * 1.1  = 176.0
	expected1 := 94.5
	expected2 := 176.0

	// Run both clones concurrently (many times to trigger race)
	const iters = 100
	dst1 := make([]float64, n)
	dst2 := make([]float64, n)

	var wg sync.WaitGroup
	var err1, err2 bool
	for iter := 0; iter < iters; iter++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			clone1.EvalFloat64Vec(b1, dst1, n)
			for i := 0; i < n; i++ {
				diff := dst1[i] - expected1
				if diff > 0.01 || diff < -0.01 {
					err1 = true
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			clone2.EvalFloat64Vec(b2, dst2, n)
			for i := 0; i < n; i++ {
				diff := dst2[i] - expected2
				if diff > 0.01 || diff < -0.01 {
					err2 = true
					return
				}
			}
		}()
		wg.Wait()
		if err1 || err2 {
			break
		}
	}

	if err1 {
		t.Errorf("clone1 produced wrong result: got %v, want %v", dst1[0], expected1)
	}
	if err2 {
		t.Errorf("clone2 produced wrong result: got %v, want %v", dst2[0], expected2)
	}

	// Verify the original expression tree is not corrupted
	origDst := make([]float64, n)
	fullExpr.EvalFloat64Vec(b1, origDst, n)
	if diff := origDst[0] - expected1; diff > 0.01 || diff < -0.01 {
		t.Errorf("original expression corrupted: got %v, want %v", origDst[0], expected1)
	}
}

// TestBinOpFloat64_CloneVec_Independence verifies that CloneVec creates a
// truly independent tree — mutating the clone's vecBuf doesn't affect the original.
func TestBinOpFloat64_CloneVec_Independence(t *testing.T) {
	colA := &ColRef{Name: "a"}
	litTwo := &constFloat64{val: 2.0}
	expr := &BinOpFloat64{Left: colA, Right: litTwo, Op: "*"}

	clone := expr.CloneVec()

	// Verify they're different pointers
	if clone == expr {
		t.Fatal("CloneVec returned same pointer")
	}

	// Verify children: colA should be shared (stateless), litTwo should be shared
	if clone.Left != expr.Left {
		t.Error("stateless ColRef should be shared between original and clone")
	}
	if clone.Right != expr.Right {
		t.Error("stateless LiteralFloat64 should be shared between original and clone")
	}

	// Nested BinOpFloat64 nodes should be cloned
	inner := &BinOpFloat64{Left: colA, Right: litTwo, Op: "+"}
	outer := &BinOpFloat64{Left: inner, Right: litTwo, Op: "*"}

	outerClone := outer.CloneVec()
	if outerClone.Left == inner {
		t.Fatal("nested BinOpFloat64 should be cloned, not shared")
	}
}
