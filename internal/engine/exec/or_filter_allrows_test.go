package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An OR branch that matches EVERY row returns the batch with a nil selection
// vector (the "all Len rows" convention). OrFilter used to read a nil Sel as
// zero rows, so `x IS NULL OR x IS NOT NULL` over a column with no nulls —
// {none} unioned with {all-as-nil-Sel} — answered zero rows instead of all.
// This is the operator-level regression for that (#622 soak follow-on).
func TestOrFilterUnionsAnAllRowsBranch(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
	rows := []map[string]any{{"s": "aaa"}, {"s": "bbb"}, {"s": "ccc"}}

	run := func(t *testing.T, left, right UnaryOperator, want int) {
		t.Helper()
		source := NewSliceSource(schema, rows)
		f := NewOrFilter(left, right)
		sink := &CollectSink{}
		pipe := &Pipeline{Source: source, Ops: []UnaryOperator{f}, Sink: sink}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(sink.Rows) != want {
			t.Fatalf("got %d rows, want %d", len(sink.Rows), want)
		}
	}

	// IS NULL (matches none) OR IS NOT NULL (matches all) = all rows.
	t.Run("null_or_notnull", func(t *testing.T) {
		run(t, NewNullCheckFilter("s", true), NewNullCheckFilter("s", false), 3)
	})
	// Order must not matter: all-rows branch on the left too.
	t.Run("notnull_or_null", func(t *testing.T) {
		run(t, NewNullCheckFilter("s", false), NewNullCheckFilter("s", true), 3)
	})
	// A never-matching comparison OR an all-rows branch = all rows.
	t.Run("nomatch_or_notnull", func(t *testing.T) {
		run(t, NewFilter(ColumnCompare("s", OpEq, "zzz")), NewNullCheckFilter("s", false), 3)
	})
	// Both branches match all rows: still all rows, not double-counted.
	t.Run("notnull_or_notnull", func(t *testing.T) {
		run(t, NewNullCheckFilter("s", false), NewNullCheckFilter("s", false), 3)
	})
	// Sanity: two genuine partial branches still union correctly.
	t.Run("partial_union", func(t *testing.T) {
		run(t, NewFilter(ColumnCompare("s", OpEq, "aaa")), NewFilter(ColumnCompare("s", OpEq, "ccc")), 2)
	})
}
