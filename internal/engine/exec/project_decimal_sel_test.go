package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestADecimalProjectionUnderASelectionVectorUsesTheExactKernel is #825.
//
// Project's selection-vector branch sent every DECIMAL output straight to
// the boxed checked writer; only the non-sel branch consulted
// VecDecimalEval. So any DECIMAL projection under a live selection vector —
// i.e. any of them below a filter — paid the four-allocation text round
// trip per cell, at a second operator, by the same mechanism as #705. The
// two had to be fixed together or the second would survive the first.
//
// The observable is the boxed-cell counter rather than testing.AllocsPerRun:
// the counter names exactly which route a cell took, where an alloc count
// cannot separate the box from the pooled output batch.
func TestADecimalProjectionUnderASelectionVectorUsesTheExactKernel(t *testing.T) {
	const rows = 8
	in := &batch.RecordBatch{
		Schema: []parquet.Column{{Name: "a", Type: parquet.TypeDecimal, Precision: 18, Scale: 2}},
		Len:    rows,
	}
	col := batch.NewVectorWithScale(batch.TypeDecimal, rows, 2)
	for i := 0; i < rows; i++ {
		col.DecimalData.Data[i] = batch.Int128From(int64(i) * 100)
		col.Nulls.SetValid(i)
	}
	in.Columns = []*batch.Vector{col}
	// Half the rows selected: the shape a filter above a DECIMAL projection
	// produces.
	in.Sel = []uint32{1, 3, 5, 7}

	kernelRuns := 0
	p := NewProject([]ProjectColumn{{
		Name: "doubled", Type: parquet.TypeDecimal, Precision: 18, Scale: 2,
		Computed: true,
		// Stands in for expr.BinOpNumeric.EvalDecimalVec: writes carriers
		// straight into the output vector for rows 0..n-1, no box.
		VecDecimalEval: func(b *batch.RecordBatch, out *batch.Vector, n int) bool {
			kernelRuns++
			src := b.Columns[0]
			for i := 0; i < n; i++ {
				out.DecimalData.Data[i] = batch.Int128From(
					src.DecimalData.Data[i].ToInt64() * 2)
				out.Nulls.SetValid(i)
			}
			return true
		},
		// The boxed route, which must not run.
		Expr: func(b *batch.RecordBatch, row int) any {
			return b.Columns[0].GetValue(row)
		},
	}})
	if err := p.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	before := DecimalBoxedCells.Load()
	out, err := p.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if boxed := DecimalBoxedCells.Load() - before; boxed != 0 {
		t.Fatalf("a DECIMAL projection under a selection vector boxed %d cells; "+
			"the exact kernel is attached and must take them (#825)", boxed)
	}
	if kernelRuns != 1 {
		t.Fatalf("the exact kernel ran %d times, want 1", kernelRuns)
	}

	// And the answer is right: the selection must be compacted, not ignored.
	if got := out.ActiveLen(); got != len(in.Sel) {
		t.Fatalf("output has %d active rows, want %d", got, len(in.Sel))
	}
	want := []int64{200, 600, 1000, 1400}
	for i, w := range want {
		if got := out.Columns[0].DecimalData.Data[i].ToInt64(); got != w {
			t.Fatalf("row %d = %d, want %d — compacting the selection away must not "+
				"change which rows the projection describes", i, got, w)
		}
	}
}
