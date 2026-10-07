// SPDX-License-Identifier: MIT

package logical

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestOutputIDsFollowTheInstanceAndThePosition pins a node's ordered identity
// list (ADR-0047 stage 2): a scan answers for its instance by the column's
// place in the instance's FULL list (a pruned scan keeps its ordinals); a
// derived table's root answers for its own instance; a join is its arms in
// order and a semi join its left; a Filter passes its input through; a bare
// bound reference — a projection or a GROUP BY key — carries its binding; a
// computed item, an aggregate, an unexpanded star and a name the instance
// holds twice carry none.
func TestOutputIDsFollowTheInstanceAndThePosition(t *testing.T) {
	b := func(rel plansql.RelID, ord int) plansql.Binding { return plansql.Binding{Rel: rel, Ord: ord} }
	ref := func(tbl, col string, bd *plansql.Binding) *plansql.ColRef {
		return &plansql.ColRef{Table: tbl, Column: col, Bound: bd}
	}
	scan := &Node{Type: NodeScan, TableName: "t", ScanColumns: []string{"id", "F"}, Rel: 1, RelCols: []string{"id", "i", "f"}}
	derivedScan := &Node{Type: NodeScan, TableName: "t", ScanColumns: []string{"id", "f"}, Rel: 3, RelCols: []string{"id", "i", "f"}}
	derived := &Node{Type: NodeProject, Children: []*Node{derivedScan}, Rel: 2, RelCols: []string{"id", "x", "x2"},
		Projections: []Projection{
			{Column: "id", Expr: "id", ASTExpr: ref("", "id", &plansql.Binding{Rel: 3, Ord: 0})},
			{Column: "f", Alias: "x", Expr: "f", ASTExpr: ref("", "f", &plansql.Binding{Rel: 3, Ord: 2})},
		}}
	join := &Node{Type: NodeJoin, JoinType: "inner", Children: []*Node{scan, derived}}
	filter := &Node{Type: NodeFilter, Children: []*Node{join}}
	proj := &Node{Type: NodeProject, Children: []*Node{filter}, Projections: []Projection{
		{Column: "a.f", Expr: "a.f", ASTExpr: &plansql.ParenNode{Inner: ref("a", "f", &plansql.Binding{Rel: 1, Ord: 2})}},
		{Expr: "x + a.i", Alias: "v", ASTExpr: &plansql.BinaryOp{Op: "+", Left: ref("", "x", nil), Right: ref("a", "i", nil)}},
		{Column: "x", Alias: "o", Expr: "x", ASTExpr: ref("", "x", &plansql.Binding{Rel: 9, Ord: 0, Output: true})},
	}}
	agg := &Node{Type: NodeAggregate, Children: []*Node{filter}, GroupBy: []string{"b.x", "x + 1"},
		GroupByExprs: []plansql.Node{ref("b", "x", &plansql.Binding{Rel: 2, Ord: 1}), nil},
		AggExprs:     []AggExpr{{Func: "count", OutputCol: "c"}}}
	semi := &Node{Type: NodeJoin, JoinType: "semi", Children: []*Node{scan, derived}}
	star := &Node{Type: NodeProject, Children: []*Node{scan}, Projections: []Projection{{Column: "*", Expr: "*"}}}
	dup := &Node{Type: NodeScan, ScanColumns: []string{"k"}, Rel: 4, RelCols: []string{"k", "K"}}

	z := plansql.Binding{}
	for _, tc := range []struct {
		name string
		n    *Node
		want []plansql.Binding
	}{
		{"scan keeps its full-list ordinals", scan, []plansql.Binding{b(1, 0), b(1, 2)}},
		{"a derived root answers for its own instance", derived, []plansql.Binding{b(2, 0), b(2, 1)}},
		{"a join is its arms in order", join, []plansql.Binding{b(1, 0), b(1, 2), b(2, 0), b(2, 1)}},
		{"a filter passes its input", filter, []plansql.Binding{b(1, 0), b(1, 2), b(2, 0), b(2, 1)}},
		{"a bare bound item carries its binding, a computed one and an output binding none", proj, []plansql.Binding{b(1, 2), z, z}},
		{"a bound key carries its binding", agg, []plansql.Binding{b(2, 1), z, z}},
		{"a semi join is its left arm", semi, []plansql.Binding{b(1, 0), b(1, 2)}},
		{"a name the instance holds twice names no column", dup, []plansql.Binding{z}},
	} {
		got := tc.n.OutputIDs()
		if len(got) != len(tc.want) {
			t.Errorf("%s: %d positions %v, want %v", tc.name, len(got), got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: position %d is %v, want %v", tc.name, i, got[i], tc.want[i])
			}
		}
	}
	if ids := star.OutputIDs(); ids != nil {
		t.Errorf("an unexpanded star enumerated %v, want nil", ids)
	}
}
