package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Filter.Check is the row path's half of the #147 guard, and this pins the
// two things the operator owes it: the error reaches the caller instead of an
// empty batch, and it is asked ONCE rather than per batch.
//
// The guard cannot live in the Predicate — that returns bool and has nowhere
// to put an error — so the operator carries it. Without it a predicate over a
// name the batch does not carry is UNKNOWN on every row, which a WHERE turns
// into "no rows": indistinguishable from empty data, and the shape #653
// arrived as.
func TestFilterCheckRefusesBeforeFiltering(t *testing.T) {
	b := func() *batch.RecordBatch {
		nb := batch.NewRecordBatch([]parquet.Column{{Name: "a", Type: parquet.TypeInt64}}, 2)
		nb.Columns[0].SetValue(0, int64(1))
		nb.Columns[0].SetValue(1, int64(2))
		nb.Len = 2
		return nb
	}

	want := errors.New("filter column \"v\" does not exist in the input schema")
	calls := 0
	f := NewFilter(func(*batch.RecordBatch, int) bool { return true })
	f.Check = func(*batch.RecordBatch) error {
		calls++
		return want
	}
	if _, err := f.Execute(context.Background(), b()); !errors.Is(err, want) {
		t.Fatalf("Execute returned %v; the check's error must reach the caller", err)
	}

	// A passing check does not stand between the rows and the predicate, and
	// is not re-run on the next batch.
	calls = 0
	g := NewFilter(func(bb *batch.RecordBatch, row int) bool { return row == 0 })
	g.Check = func(*batch.RecordBatch) error { calls++; return nil }
	for i := 0; i < 3; i++ {
		out, err := g.Execute(context.Background(), b())
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
		if out == nil || len(out.Sel) != 1 {
			t.Fatalf("batch %d selected %v, want one row", i, out)
		}
	}
	if calls != 1 {
		t.Fatalf("the check ran %d times; it is a schema test and belongs on the first batch only", calls)
	}

	// Clone carries it: every clone reads the same schema.
	c := g.Clone().(*Filter)
	if c.Check == nil {
		t.Fatal("Clone dropped Check, so a parallel pipeline would answer where the original refuses")
	}
}
