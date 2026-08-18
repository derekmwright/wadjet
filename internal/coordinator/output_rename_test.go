package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestApplyOutputRenames verifies that the gather-side rename rewrites
// both gatherResult.columns and each batch's Schema column names. This
// is the data-plane half of the Bug A fix (the planner half is in
// q07_alias_test.go).
func TestApplyOutputRenames(t *testing.T) {
	schema := []parquet.Column{
		{Name: "n1.n_name", Type: parquet.TypeString},
		{Name: "substr(l_shipdate, 1, 4)", Type: parquet.TypeString},
		{Name: "revenue", Type: parquet.TypeFloat64},
	}
	b := batch.NewRecordBatch(schema, 1)
	gr := &gatherResult{
		batches: []*batch.RecordBatch{b},
		columns: []string{"n1.n_name", "substr(l_shipdate, 1, 4)", "revenue"},
	}
	// applyOutputRenames now PROJECTS — every desired output column needs an
	// entry, even pass-throughs. extractOutputRenames emits a self-rename
	// (From==To) for aggregate columns whose OutputCol already equals the
	// alias.
	renames := []physical.OutputRename{
		{From: "n1.n_name", To: "supp_nation"},
		{From: "substr(l_shipdate, 1, 4)", To: "l_year"},
		{From: "revenue", To: "revenue"},
	}

	applyOutputRenames(gr, renames)

	wantCols := []string{"supp_nation", "l_year", "revenue"}
	for i, w := range wantCols {
		if gr.columns[i] != w {
			t.Errorf("gr.columns[%d] = %q, want %q", i, gr.columns[i], w)
		}
		if gr.batches[0].Schema[i].Name != w {
			t.Errorf("schema[%d].Name = %q, want %q", i, gr.batches[0].Schema[i].Name, w)
		}
	}
}

// TestApplyOutputRenames_CaseInsensitive verifies the rename matches
// case-insensitively. The worker's expression compiler emits lowercase
// function names ("substr") even when the SELECT clause uses uppercase
// ("SUBSTR"). Match must tolerate this so the SUBSTR alias still binds.
func TestApplyOutputRenames_CaseInsensitive(t *testing.T) {
	schema := []parquet.Column{{Name: "substr(l_shipdate, 1, 4)", Type: parquet.TypeString}}
	gr := &gatherResult{
		batches: []*batch.RecordBatch{batch.NewRecordBatch(schema, 1)},
		columns: []string{"substr(l_shipdate, 1, 4)"},
	}
	renames := []physical.OutputRename{
		{From: "SUBSTR(L_SHIPDATE, 1, 4)", To: "l_year"},
	}
	applyOutputRenames(gr, renames)
	if gr.columns[0] != "l_year" {
		t.Errorf("got %q, want l_year", gr.columns[0])
	}
}

// TestApplyOutputRenames_SelfJoinQualifierFallback is the gather-side half of
// the #314 regression. A join qualifies only the BUILD side's colliding
// columns, so the two aliases of a self-joined table reach the gather as
// "n_name" (whichever copy sat on the probe) and "n2.n_name". The SELECT list
// names both by alias, so the rename source "n1.n_name" matches nothing
// exactly. Before the fix that single miss dropped the WHOLE projection to
// rename-only: "n2.n_name" became "cust_nation" but the first alias left the
// result under its raw worker name "n_name", with no "supp_nation" column at
// all.
func TestApplyOutputRenames_SelfJoinQualifierFallback(t *testing.T) {
	schema := []parquet.Column{
		{Name: "n_name", Type: parquet.TypeString},    // nation n1, probe side, unqualified
		{Name: "n2.n_name", Type: parquet.TypeString}, // nation n2, build side, qualified
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, "FRANCE")
	b.Columns[1].SetValue(0, "GERMANY")
	gr := &gatherResult{
		batches: []*batch.RecordBatch{b},
		columns: []string{"n_name", "n2.n_name"},
	}
	renames := []physical.OutputRename{
		{From: "n1.n_name", To: "supp_nation"},
		{From: "n2.n_name", To: "cust_nation"},
	}

	applyOutputRenames(gr, renames)

	want := []string{"supp_nation", "cust_nation"}
	for i, w := range want {
		if gr.columns[i] != w {
			t.Errorf("gr.columns[%d] = %q, want %q", i, gr.columns[i], w)
		}
		if gr.batches[0].Schema[i].Name != w {
			t.Errorf("schema[%d].Name = %q, want %q", i, gr.batches[0].Schema[i].Name, w)
		}
	}
	if got := gr.batches[0].Columns[0].GetValue(0); got != "FRANCE" {
		t.Errorf("supp_nation = %v, want FRANCE — the first alias must carry the probe-side column's values", got)
	}
	if got := gr.batches[0].Columns[1].GetValue(0); got != "GERMANY" {
		t.Errorf("cust_nation = %v, want GERMANY", got)
	}
}

// TestResolveRenameSource pins the resolution order: exact match wins over
// any fallback, a qualified source falls back to the bare column, a bare
// source falls back to a single qualified match, and an ambiguous bare source
// (2+ qualified candidates) resolves to nothing rather than guessing.
func TestResolveRenameSource(t *testing.T) {
	tests := []struct {
		name  string
		names []string
		from  string
		want  int
	}{
		{"exact", []string{"n_name", "n2.n_name"}, "n2.n_name", 1},
		{"exact beats fallback", []string{"n_name", "n1.n_name"}, "n1.n_name", 1},
		{"case insensitive", []string{"substr(x, 1, 4)"}, "SUBSTR(X, 1, 4)", 0},
		{"qualified to bare", []string{"n_name", "n2.n_name"}, "n1.n_name", 0},
		{"bare to qualified", []string{"l_orderkey", "n2.n_name"}, "n_name", 1},
		{"bare ambiguous", []string{"n1.n_name", "n2.n_name"}, "n_name", -1},
		{"missing", []string{"a", "b"}, "c", -1},
		{"qualified missing", []string{"a", "b"}, "t.c", -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveRenameSource(tt.names, tt.from); got != tt.want {
				t.Errorf("resolveRenameSource(%v, %q) = %d, want %d", tt.names, tt.from, got, tt.want)
			}
		})
	}
}

// TestApplyOutputRenames_NoOp guards the empty-renames + nil-result
// fast paths.
func TestApplyOutputRenames_NoOp(t *testing.T) {
	applyOutputRenames(nil, []physical.OutputRename{{From: "x", To: "y"}}) // must not panic
	gr := &gatherResult{columns: []string{"x"}}
	applyOutputRenames(gr, nil)
	if gr.columns[0] != "x" {
		t.Errorf("nil renames should leave columns unchanged, got %q", gr.columns[0])
	}
}
