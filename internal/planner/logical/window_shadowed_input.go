// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// WindowShadowedInput returns the derived table a window reads — the first
// Project below it, through filters, sorts and limits — when that table, or a
// derived table it reads through Projects, filters, sorts and limits only,
// computes a column under the name of a column of its own input (`SELECT
// b * 2 AS b FROM t`); nil otherwise.
//
// Above such a window the shadowed SOURCE column does not exist: `b` is the
// derived table's computed column and a rename beside it (`b AS ob`) is the
// derived table's own column, as PostgreSQL scopes them. The stage DAG's
// window stage reads the table's DECLARED columns under their own names, so a
// walk that resolves a name from above the window stops AT it rather than
// respelling the name into the table's definitions — `ob` into the source
// `b`, or `b` into `b * 2` — which over that stream would read the computed
// column where the query named the source, or compute it a second time.
func WindowShadowedInput(w *Node) *Node {
	if w == nil || w.Type != NodeWindow || len(w.Children) != 1 {
		return nil
	}
	var top *Node
	for n := w.Children[0]; n != nil; n = n.Children[0] {
		switch n.Type {
		case NodeFilter, NodeSort, NodeLimit:
		case NodeProject:
			if n.SecurityBarrier {
				return nil
			}
			if top == nil {
				top = n
			}
			if len(n.Children) == 1 && projectShadowsItsInput(n) {
				return top
			}
		default:
			return nil
		}
		if len(n.Children) != 1 {
			return nil
		}
	}
	return nil
}

// projectShadowsItsInput reports whether a Project computes an item under the
// name of a column its input publishes.
func projectShadowsItsInput(p *Node) bool {
	var in map[string]bool
	for _, item := range p.Projections {
		if item.IsAgg || item.Column != "" || item.ASTExpr == nil || isColRefItem(item.ASTExpr) {
			continue
		}
		if in == nil {
			in = map[string]bool{}
			collectPublishedNames(p.Children[0], in)
		}
		if in[bareLower(projectionOutputName(item))] {
			return true
		}
	}
	return false
}

// isColRefItem reports whether a projection's expression is a column
// reference, parenthesized or not: a rename, not a computation.
func isColRefItem(n plansql.Node) bool {
	for {
		switch e := n.(type) {
		case *plansql.ColRef:
			return true
		case *plansql.ParenNode:
			n = e.Inner
		default:
			return false
		}
	}
}

// collectPublishedNames adds the bare, lower-cased names of every column a
// subtree publishes.
func collectPublishedNames(n *Node, out map[string]bool) {
	if n == nil {
		return
	}
	switch n.Type {
	case NodeScan:
		for _, c := range n.ScanColumns {
			out[bareLower(c)] = true
		}
		for c := range n.ScanColTypes {
			out[bareLower(c)] = true
		}
		return
	case NodeProject:
		for _, p := range n.Projections {
			out[bareLower(projectionOutputName(p))] = true
		}
		return
	case NodeAggregate:
		for _, g := range n.GroupBy {
			out[bareLower(g)] = true
		}
		for _, a := range n.AggExprs {
			out[bareLower(a.OutputCol)] = true
		}
		return
	case NodeWindow:
		for _, we := range n.WindowExprs {
			out[bareLower(we.OutputCol)] = true
		}
	}
	for _, c := range n.Children {
		collectPublishedNames(c, out)
	}
}

func bareLower(s string) string {
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		s = s[i+1:]
	}
	return strings.ToLower(s)
}

// WindowShadowedBlock reports whether p is the SELECT list of a window over a
// derived table that shadows its input (WindowShadowedInput) — the block a
// query reads that window through, `x` in `(SELECT id, SUM(b) OVER (…) AS w,
// b AS w3 FROM (SELECT id, b * 2 AS b FROM t) s) x`.
//
// Such a block is a relation of its own above the window: its items are named
// by the block (`x.w3`, `x.b`), and a walk that resolves a reference from
// above stops AT it rather than chasing `w3` into the window's input, where
// `b` is a name the other arm of a join may carry too. The stage DAG
// publishes the block's list onto the stage that terminates it, so the
// stream carries every item under the block's own name.
func WindowShadowedBlock(p *Node) bool {
	if p == nil || p.Type != NodeProject || p.SecurityBarrier || len(p.Children) != 1 {
		return false
	}
	for n := p.Children[0]; n != nil; n = n.Children[0] {
		switch n.Type {
		case NodeFilter, NodeSort, NodeLimit:
		case NodeWindow:
			// Windows of different specifications stack; any of them may be
			// the one over the shadowing table.
			if WindowShadowedInput(n) != nil {
				return true
			}
		default:
			return false
		}
		if len(n.Children) != 1 {
			return false
		}
	}
	return false
}
