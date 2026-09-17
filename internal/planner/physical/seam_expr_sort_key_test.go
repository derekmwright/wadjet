// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// TestHiddenSortTrimOpDropsMaterializedColumns covers the single-process half:
// there is no gather to project the result, so the pipeline needs its own
// trim. Without it the Sort hands __sortkey_N to the client next to the
// columns the query asked for.
func TestHiddenSortTrimOpDropsMaterializedColumns(t *testing.T) {
	visible := logical.Projection{Column: "n_name", Expr: "n_name", Alias: "n_name"}
	hidden := logical.Projection{Expr: "length(n_name)", Alias: "__sortkey_0", Hidden: true}

	plan := &logical.Node{Type: logical.NodeSort, Children: []*logical.Node{
		{Type: logical.NodeProject, Projections: []logical.Projection{visible, hidden}},
	}}
	op := HiddenSortTrimOp(plan)
	if op == nil {
		t.Fatal("no trim operator for a plan carrying a materialized sort column")
	}
	trim, ok := op.(*exec.Project)
	if !ok {
		t.Fatalf("trim operator is a %T, want *exec.Project", op)
	}
	if len(trim.Projections) != 1 || trim.Projections[0].Name != "n_name" {
		t.Errorf("trim projects %+v, want exactly the select list [n_name]", trim.Projections)
	}

	// Without a hidden column the plan must be left alone — a trim there
	// would be pure cost on every sorted query in the corpus.
	plain := &logical.Node{Type: logical.NodeSort, Children: []*logical.Node{
		{Type: logical.NodeProject, Projections: []logical.Projection{visible}},
	}}
	if HiddenSortTrimOp(plain) != nil {
		t.Error("plan with no materialized sort column grew a trim operator")
	}

	// An unexpanded star has no column list to trim to; the plan is left
	// alone rather than projected to nulls.
	star := &logical.Node{Type: logical.NodeSort, Children: []*logical.Node{
		{Type: logical.NodeProject, Projections: []logical.Projection{{Expr: "*", Column: "*"}, hidden}},
	}}
	if HiddenSortTrimOp(star) != nil {
		t.Error("unexpanded star grew a trim operator; it would project every row to nulls")
	}
}
