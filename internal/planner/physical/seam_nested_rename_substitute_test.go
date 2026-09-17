// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestSubstituteNestedRenameRefs covers the #387 rewriter over constructed
// trees: reference substitution through single and chained renames, the
// copy-on-write identity for untouched expressions, qualifier dropping, and
// the decline cases (subquery-bearing nodes, unknown kinds).
func TestSubstituteNestedRenameRefs(t *testing.T) {
	renameProj := func(col, alias string, child *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeProject,
			Projections: []logical.Projection{{Column: col, Expr: col, Alias: alias}},
			Children:    []*logical.Node{child}}
	}
	scan := &logical.Node{Type: logical.NodeScan, TableName: "region"}
	child := renameProj("r_regionkey", "k", scan)
	chained := renameProj("b", "a", renameProj("r_regionkey", "b", scan))

	t.Run("binary op over a rename", func(t *testing.T) {
		in := &plansql.BinaryOp{Left: &plansql.ColRef{Column: "k"}, Op: "+",
			Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}
		out, ok := substituteNestedRenameRefs(in, child)
		if !ok {
			t.Fatal("rewrite declined")
		}
		if got := out.String(); !strings.EqualFold(got, "r_regionkey + 1") {
			t.Errorf("rewritten = %q, want r_regionkey + 1", got)
		}
		if in.Left.(*plansql.ColRef).Column != "k" {
			t.Error("input AST was mutated; the rewrite must be copy-on-write")
		}
	})
	t.Run("chained rename resolves to the scan column", func(t *testing.T) {
		out, ok := substituteNestedRenameRefs(&plansql.ColRef{Column: "a"}, chained)
		if !ok {
			t.Fatal("rewrite declined")
		}
		if got := out.(*plansql.ColRef).Column; got != "r_regionkey" {
			t.Errorf("resolved = %q, want r_regionkey", got)
		}
	})
	t.Run("qualifier drops with the rewrite", func(t *testing.T) {
		out, ok := substituteNestedRenameRefs(&plansql.ColRef{Table: "t", Column: "k"}, child)
		if !ok {
			t.Fatal("rewrite declined")
		}
		ref := out.(*plansql.ColRef)
		if ref.Column != "r_regionkey" || ref.Table != "" {
			t.Errorf("rewritten ref = %+v, want bare r_regionkey", ref)
		}
	})
	t.Run("untouched expression returns the same node", func(t *testing.T) {
		in := &plansql.BinaryOp{Left: &plansql.ColRef{Column: "r_name"}, Op: "+",
			Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}
		out, ok := substituteNestedRenameRefs(in, child)
		if !ok || out != plansql.Node(in) {
			t.Errorf("no reference resolves, the input node itself must come back (ok=%v)", ok)
		}
	})
	t.Run("subquery declines", func(t *testing.T) {
		in := &plansql.BinaryOp{Left: &plansql.ColRef{Column: "k"}, Op: "+",
			Right: &plansql.SubqueryNode{SQL: "select max(r_regionkey) from region"}}
		if _, ok := substituteNestedRenameRefs(in, child); ok {
			t.Error("a subquery-bearing expression must decline the rewrite")
		}
	})
}
