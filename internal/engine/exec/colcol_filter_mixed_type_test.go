package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func mixedTypeBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "price", Type: parquet.TypeFloat64},
		{Name: "region", Type: parquet.TypeInt32},
	}, 4)
	copy(b.Columns[0].Float64Data, []float64{1.0, 2.5, 3.0, 4.0})
	copy(b.Columns[1].Int32Data, []int32{1, 2, 3, 9})
	return b
}

// Regression for #375: ColColFilter resolved its kernel from the LEFT
// column's type only, so comparing FLOAT64 against INT32 read the right
// vector's EMPTY Float64Data and panicked (`index out of range [i] with
// length 0`). The client-visible shape was a five-table join chain whose
// unqualified WHERE compared columns of different types (`o_totalprice <>
// r_regionkey`); the qualified spelling survived only because dotted names
// skip the vectorized kernel. A mixed-type comparison must run the row
// fallback, and one without a fallback must ERROR, never panic.
func TestColColFilterMixedTypesUsesRowFallback(t *testing.T) {
	f := NewColColFilter("price", "region", OpNe)
	f.RowFallback = func(b *batch.RecordBatch, row int) bool {
		p := b.Columns[0].Float64Data[row]
		r := float64(b.Columns[1].Int32Data[row])
		return p != r
	}
	out, err := f.Execute(context.Background(), mixedTypeBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	// Rows 0 (1.0 vs 1) and 2 (3.0 vs 3) are equal; rows 1 and 3 differ.
	if out == nil {
		t.Fatal("filter matched nothing; want rows 1 and 3")
	}
	if len(out.Sel) != 2 || out.Sel[0] != 1 || out.Sel[1] != 3 {
		t.Fatalf("Sel = %v, want [1 3]", out.Sel)
	}
}

func TestColColFilterMixedTypesWithoutFallbackErrors(t *testing.T) {
	f := NewColColFilter("price", "region", OpNe)
	out, err := f.Execute(context.Background(), mixedTypeBatch(t))
	if err == nil {
		t.Fatalf("want a mismatched-type error, got out=%v (pre-#375 this PANICKED instead)", out)
	}
	if !strings.Contains(err.Error(), "mismatched column types") {
		t.Fatalf("error %q does not name the mismatched types", err)
	}
}

// Same-type comparisons must still take the vectorized kernel: attaching a
// fallback does not disable it.
func TestColColFilterSameTypeStillVectorized(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
	}, 3)
	copy(b.Columns[0].Int64Data, []int64{1, 5, 3})
	copy(b.Columns[1].Int64Data, []int64{1, 2, 3})
	f := NewColColFilter("a", "b", OpEq)
	f.RowFallback = func(*batch.RecordBatch, int) bool {
		t.Fatal("row fallback ran for a same-type comparison")
		return false
	}
	out, err := f.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || len(out.Sel) != 2 || out.Sel[0] != 0 || out.Sel[1] != 2 {
		t.Fatalf("Sel = %v, want [0 2]", selOf(out))
	}
}

func selOf(b *batch.RecordBatch) []uint32 {
	if b == nil {
		return nil
	}
	return b.Sel
}
