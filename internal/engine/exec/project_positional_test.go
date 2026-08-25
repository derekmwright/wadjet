package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ProjectColumn.SourceIdx names an input column by POSITION, for the one
// projection whose source cannot be named: the hidden-sort-key trim, whose
// output list may carry two columns of the same name. `SELECT abs(a), abs(b)`
// is such a list — PostgreSQL calls both columns `abs` and #513 made this
// engine agree — and copying by NAME gave the second output the first one's
// values, silently.
func TestProjectSourceIdxBeatsAName(t *testing.T) {
	schema := []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "__sortkey_0", Type: parquet.TypeInt64},
	}
	in := batch.NewRecordBatch(schema, 2)
	for i, vals := range [][]int64{{10, 20, 99}, {11, 21, 98}} {
		for j, v := range vals {
			in.Columns[j].SetValue(i, v)
		}
	}

	// The trim: keep the two visible columns, drop the hidden key. Both name
	// fields still say "x", exactly as hiddenSortTrimOp writes them.
	p := NewProject([]ProjectColumn{
		{Name: "x", SourceIdx: 0, SourceIdxSet: true, DirectCopy: "x", SourceCol: "x", Expr: ColumnRef("x")},
		{Name: "x", SourceIdx: 1, SourceIdxSet: true, DirectCopy: "x", SourceCol: "x", Expr: ColumnRef("x")},
	})
	if err := p.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := p.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(out.Columns) != 2 {
		t.Fatalf("output has %d columns, want 2", len(out.Columns))
	}
	want := [][]int64{{10, 20}, {11, 21}}
	for i := 0; i < out.Len; i++ {
		for j := range want[i] {
			got, ok := out.Columns[j].GetValue(i).(int64)
			if !ok || got != want[i][j] {
				t.Errorf("row %d col %d = %v, want %d — a positional source must not be "+
					"overridden by the same-name resolution", i, j, out.Columns[j].GetValue(i), want[i][j])
			}
		}
	}
}

// TestProjectSourceIdxOutOfRangeFallsBackToTheName: the positional source is
// an assertion about the input's shape, and a shape that does not match must
// degrade to the name-based resolution rather than read out of bounds.
func TestProjectSourceIdxOutOfRangeFallsBackToTheName(t *testing.T) {
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}}
	in := batch.NewRecordBatch(schema, 1)
	in.Columns[0].SetValue(0, int64(7))

	p := NewProject([]ProjectColumn{
		{Name: "a", SourceIdx: 5, SourceIdxSet: true, DirectCopy: "a", SourceCol: "a", Expr: ColumnRef("a")},
	})
	if err := p.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := p.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := out.Columns[0].GetValue(0); got != int64(7) {
		t.Errorf("got %v, want 7 — an out-of-range positional source must fall back to the name", got)
	}
}
