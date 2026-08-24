package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// twoDecimalBatch holds the two DECIMAL columns of the oracle fixture's shape:
// d_2 at scale 2 and d_4 at scale 4, equal on one row and ordered either way
// on the others, with a NULL on each side.
func twoDecimalBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d_2", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "d_4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}, 5)
	for i, u := range []int64{150, 200, -325, 0, 0} {
		b.Columns[0].DecimalData.Data[i] = batch.Int128From(u)
	}
	for i, u := range []int64{15000, 19999, -32501, 0, 100} {
		b.Columns[1].DecimalData.Data[i] = batch.Int128From(u)
	}
	b.Columns[0].Nulls.SetNull(3)
	b.Columns[1].Nulls.SetNull(4)
	return b
}

// #477: ColColFilter attaches its row-at-a-time fallback only when the two
// column TYPES differ (#375's mixed-type guard). Two DECIMALs share a TypeID,
// so that branch was skipped and the code went looking for a vectorized
// kernel that ResolveColColFilterKernel had no arm to give it. With no kernel
// and no fallback it FAILED THE QUERY:
//
//	ColColFilter: could not resolve kernel for d_2 0 d_4 (leftIdx=0, rightIdx=1)
//
// for every operator, on both execution paths.
func TestColColFilterComparesTwoDecimalColumns(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		op   CompareOp
		want []uint32
	}{
		// Row 0 is 1.50 against 1.5000 — the same number at two scales, which
		// is the pair that also settles that the unscaled integers are not
		// compared as if they were commensurable.
		{OpEq, []uint32{0}},
		{OpNe, []uint32{1, 2}},
		{OpLt, nil},
		{OpLe, []uint32{0}},
		{OpGt, []uint32{1, 2}},
		{OpGe, []uint32{0, 1, 2}},
	} {
		in := twoDecimalBatch(t)
		out, err := NewColColFilter("d_2", "d_4", tc.op).Execute(ctx, in)
		if err != nil {
			t.Fatalf("op %d: %v", tc.op, err)
		}
		if len(tc.want) == 0 {
			if out != nil && len(out.Sel) != 0 {
				t.Errorf("op %d: selected %v, want none", tc.op, out.Sel)
			}
			continue
		}
		if out == nil {
			t.Fatalf("op %d: selected nothing, want %v", tc.op, tc.want)
		}
		if len(out.Sel) != len(tc.want) {
			t.Fatalf("op %d: selected %v, want %v", tc.op, out.Sel, tc.want)
		}
		for i, w := range tc.want {
			if out.Sel[i] != w {
				t.Fatalf("op %d: selected %v, want %v", tc.op, out.Sel, tc.want)
			}
		}
	}
}
