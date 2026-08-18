package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// selfJoinBatch mimics a join's output over two aliases of one table: the
// copy that sat on the PROBE side ships under the bare column name, and the
// build side's colliding copy is qualified with its alias.
func selfJoinBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "n_name", Type: parquet.TypeString},    // nation n1 (probe)
		{Name: "n2.n_name", Type: parquet.TypeString}, // nation n2 (build)
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Len = 2
	b.Columns[0].SetValue(0, "FRANCE")
	b.Columns[0].SetValue(1, "GERMANY")
	b.Columns[1].SetValue(0, "GERMANY")
	b.Columns[1].SetValue(1, "FRANCE")
	return b
}

// TestProject_SelfJoinQualifiedSource is the operator-level regression for
// #314. The worker's pre-aggregate projection (buildAggInputProjection) emits
// each GROUP BY column under the name the planner used — "n1.n_name" — but
// the join left that alias's column unqualified. Before the fix DirectCopy
// resolution was exact-match only, so the projection fell through to the
// per-row ColumnRef path, which misses the same way and returns nil for every
// row: Q07's supp_nation came back NULL in every result row while
// cust_nation, whose "n2.n_name" name did match, was correct.
func TestProject_SelfJoinQualifiedSource(t *testing.T) {
	in := selfJoinBatch(t)
	p := NewProject([]ProjectColumn{
		{Name: "n1.n_name", DirectCopy: "n1.n_name", SourceCol: "n1.n_name", Expr: ColumnRef("n1.n_name")},
		{Name: "n2.n_name", DirectCopy: "n2.n_name", SourceCol: "n2.n_name", Expr: ColumnRef("n2.n_name")},
	})
	out, err := p.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out.Schema[0].Type != parquet.TypeString {
		t.Errorf("n1.n_name type = %v, want String", out.Schema[0].Type)
	}
	want := [][2]string{{"FRANCE", "GERMANY"}, {"GERMANY", "FRANCE"}}
	for row, w := range want {
		if got := out.Columns[0].GetValue(row); got != w[0] {
			t.Errorf("row %d n1.n_name = %v, want %q (the unqualified probe-side column)", row, got, w[0])
		}
		if got := out.Columns[1].GetValue(row); got != w[1] {
			t.Errorf("row %d n2.n_name = %v, want %q", row, got, w[1])
		}
	}
}

// TestProject_BareSourceOverQualifiedColumn covers the other direction of the
// same fallback: a projection naming the bare column over a schema that only
// carries the qualified form.
func TestProject_BareSourceOverQualifiedColumn(t *testing.T) {
	schema := []parquet.Column{{Name: "n2.n_name", Type: parquet.TypeString}}
	in := batch.NewRecordBatch(schema, 1)
	in.Len = 1
	in.Columns[0].SetValue(0, "GERMANY")

	p := NewProject([]ProjectColumn{
		{Name: "cust", DirectCopy: "n_name", SourceCol: "n_name", Expr: ColumnRef("n_name")},
	})
	out, err := p.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := out.Columns[0].GetValue(0); got != "GERMANY" {
		t.Errorf("cust = %v, want GERMANY", got)
	}
}

// TestProject_AmbiguousBareSource: two qualified candidates for one bare
// source name is ambiguous, so the fallback refuses to guess. Nothing
// resolves, which makes the projection an error (issue #147's contract)
// rather than a silently wrong column.
func TestProject_AmbiguousBareSource(t *testing.T) {
	schema := []parquet.Column{
		{Name: "n1.n_name", Type: parquet.TypeString},
		{Name: "n2.n_name", Type: parquet.TypeString},
	}
	in := batch.NewRecordBatch(schema, 1)
	in.Len = 1

	p := NewProject([]ProjectColumn{
		{Name: "x", DirectCopy: "n_name", SourceCol: "n_name", Expr: ColumnRef("n_name")},
	})
	if _, err := p.Execute(context.Background(), in); err == nil {
		t.Fatal("want an error for an ambiguous bare source, got nil")
	}
}

// TestProject_UnresolvableSourceStillErrors keeps issue #147's guarantee: a
// projected plain column that resolves to nothing at all is an error, not an
// all-NULL column. The new fallback must not turn a typo into a silent pass.
func TestProject_UnresolvableSourceStillErrors(t *testing.T) {
	in := selfJoinBatch(t)
	p := NewProject([]ProjectColumn{
		{Name: "typo", DirectCopy: "n_nmae", SourceCol: "n_nmae", Expr: ColumnRef("n_nmae")},
	})
	if _, err := p.Execute(context.Background(), in); err == nil {
		t.Fatal("want an error for a column that resolves to nothing, got nil")
	}
}

// TestResolvePlainColumn_RowParentWins: "attrs.score" must keep extracting the
// ROW field even when an unrelated bare "score" column is in scope — the ROW
// check runs before the qualifier-strip fallback, and ROW field access stays
// on the per-row eval path (idx -1, ok true).
func TestResolvePlainColumn_RowParentWins(t *testing.T) {
	schema := []parquet.Column{
		{Name: "attrs", Type: parquet.TypeRow, Fields: []parquet.Column{{Name: "score", Type: parquet.TypeInt64}}},
		{Name: "score", Type: parquet.TypeInt64},
	}
	b := batch.NewRecordBatch(schema, 1)
	idx, ok := resolvePlainColumn(b, "attrs.score")
	if !ok {
		t.Fatal("attrs.score must be reachable through its ROW parent")
	}
	if idx != -1 {
		t.Errorf("idx = %d, want -1 (ROW field extraction is per-row eval, not a bulk copy of the bare %q column)", idx, "score")
	}
}
