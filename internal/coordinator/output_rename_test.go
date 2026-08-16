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
