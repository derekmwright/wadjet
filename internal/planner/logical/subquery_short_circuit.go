// SPDX-License-Identifier: MIT

package logical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// foldShortCircuitedSubqueries removes every subquery a constant decides is
// never evaluated, before any path resolves one (#1411 review r2 B1).
//
// PostgreSQL folds constants (eval_const_expressions) BEFORE it plans a
// sublink: `false AND EXISTS (…)` and `true OR EXISTS (…)` are constants
// there, and a top-level WHERE with a constant-false or NULL conjunct is a
// one-time false filter. The subquery is never planned or run, so whatever it
// would raise — a sample's 2202H, a 22003 its argument cannot hold, a 22012 —
// is not the statement's answer. Here the stage DAG resolves a filter's
// subqueries at plan time, before anything folds, and the single-process
// filter evaluates its connective left to right, so `EXISTS (…) AND false`
// ran the subquery first. Folding in the logical plan both paths consume
// makes the short-circuit the plan's, not an evaluation order's.
//
// Only a predicate that holds a subquery is rewritten, and only by the two
// identities that hold for every operand value — x AND false is false, x OR
// true is true — plus PostgreSQL's top-level rule that a NULL conjunct of a
// WHERE or HAVING passes no row. A connective whose constant arm does not
// decide it (`false OR EXISTS …`, `NULL OR EXISTS …`) keeps its subquery, as
// PostgreSQL does.
func foldShortCircuitedSubqueries(n *Node) {
	if n == nil {
		return
	}
	if n.Type == NodeFilter {
		filterFalse := filterIsConstantFalse(n)
		for i := range n.Predicates {
			p := &n.Predicates[i]
			if p.ASTExpr == nil || !holdsSubquery(p.ASTExpr) {
				continue
			}
			folded := p.ASTExpr
			if !filterFalse {
				folded = foldDecidedConnectives(p.ASTExpr, true)
			}
			if filterFalse || isConstantFalse(folded, true) {
				folded = &plansql.Lit{Value: "false", Kind: plansql.LitBool}
			}
			if folded != p.ASTExpr {
				*p = Predicate{Raw: folded.String(), ASTExpr: folded, FromPolicy: p.FromPolicy}
			}
		}
	}
	for _, c := range n.Children {
		foldShortCircuitedSubqueries(c)
	}
}

// foldDecidedConnectives folds an AND with a constant-false arm to false and
// an OR with a constant-true arm to true, innermost first. top is true while
// the node is a conjunct of the predicate's own AND chain, where a NULL
// conjunct is false (PostgreSQL's canonicalize_qual).
func foldDecidedConnectives(node plansql.Node, top bool) plansql.Node {
	switch v := node.(type) {
	case *plansql.ParenNode:
		inner := foldDecidedConnectives(v.Inner, top)
		if lit, ok := inner.(*plansql.Lit); ok {
			return lit
		}
		if inner != v.Inner {
			return &plansql.ParenNode{Inner: inner}
		}
	case *plansql.AndNode:
		l, r := foldDecidedConnectives(v.Left, top), foldDecidedConnectives(v.Right, top)
		if isConstantFalse(l, top) || isConstantFalse(r, top) {
			return &plansql.Lit{Value: "false", Kind: plansql.LitBool}
		}
		if l != v.Left || r != v.Right {
			return &plansql.AndNode{Left: l, Right: r}
		}
	case *plansql.OrNode:
		l, r := foldDecidedConnectives(v.Left, false), foldDecidedConnectives(v.Right, false)
		if isConstantTrue(l) || isConstantTrue(r) {
			return &plansql.Lit{Value: "true", Kind: plansql.LitBool}
		}
		if l != v.Left || r != v.Right {
			return &plansql.OrNode{Left: l, Right: r}
		}
	case *plansql.NotNode:
		if inner := foldDecidedConnectives(v.Inner, false); inner != v.Inner {
			return &plansql.NotNode{Inner: inner}
		}
	}
	return node
}

// isConstantFalse: a constant that is false — or NULL where nullIsFalse.
func isConstantFalse(node plansql.Node, nullIsFalse bool) bool {
	if !conjunctIsConstant(node) {
		return false
	}
	v, ok := evalConstantFilter(node)
	if !ok {
		return false
	}
	b, isBool := v.(bool)
	return (isBool && !b) || (v == nil && nullIsFalse)
}

func isConstantTrue(node plansql.Node) bool {
	if !conjunctIsConstant(node) {
		return false
	}
	v, ok := evalConstantFilter(node)
	b, isBool := v.(bool)
	return ok && isBool && b
}

// holdsSubquery reports whether an expression contains a subquery of any
// kind: scalar, EXISTS, or the right side of IN / ANY / ALL.
func holdsSubquery(node plansql.Node) bool {
	found := false
	plansql.RewriteExpr(node, func(x plansql.Node) (plansql.Node, bool) {
		switch v := x.(type) {
		case *plansql.SubqueryNode, *plansql.ExistsNode:
			found = true
		case *plansql.AnyAllExpr:
			// RewriteExpr does not descend into ANY / ALL.
			for _, val := range v.Values {
				if _, ok := val.(*plansql.SubqueryNode); ok {
					found = true
				}
			}
		}
		return nil, false
	})
	return found
}
