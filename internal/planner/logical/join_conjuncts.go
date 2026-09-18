// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// joinConjunct is one top-level AND term of an ON clause: the text the rest of
// the planner carries it as, and the parsed node it was split off.
type joinConjunct struct {
	text string
	expr plansql.Node
}

// splitJoinConjuncts splits an ON clause into its top-level AND terms ON THE
// AST, which is what tells an AND that JOINS two conditions from an AND that
// is PART OF one.
//
// splitOnAnd, which this replaces at the join sites, cuts the RENDERED text at
// every " AND ". BETWEEN carries one of its own: `ON a.x BETWEEN b.lo AND
// b.hi` was cut into `a.x BETWEEN b.lo` and `b.hi`, the first of which parses
// as nothing and the second as a literal, so the join was refused for a
// condition PostgreSQL evaluates and the same predicate in WHERE answers
// (#1178). A string literal containing ' AND ', a parenthesised OR, and a CASE
// with an AND inside it are the same cut on other spellings.
//
// A conjunct is rendered back from its node, which round-trips: QuoteIdent
// keeps a delimited or CamelCase name re-parseable (#731), and an OR conjunct
// is re-parenthesised so re-joining the terms with " AND " cannot change how
// they associate. An expression that does not parse at all keeps the textual
// split — the physical key parser still refuses it loudly there — and a
// condition with nothing to split keeps its ORIGINAL text, so the common case
// is byte-identical to what the planner carried before.
func splitJoinConjuncts(cond string) []joinConjunct {
	root := tryParseExpr(cond)
	if root == nil {
		var out []joinConjunct
		for _, part := range splitOnAnd(cond, strings.ToUpper(cond)) {
			out = append(out, joinConjunct{text: part})
		}
		return out
	}
	var nodes []plansql.Node
	flattenAndNodes(root, &nodes)
	if len(nodes) == 1 {
		return []joinConjunct{{text: cond, expr: root}}
	}
	out := make([]joinConjunct, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, joinConjunct{text: renderConjunct(n), expr: n})
	}
	return out
}

// flattenAndNodes collects the top-level AND terms of n. A ParenNode is
// looked THROUGH only when it wraps an AND — `(a AND b) AND c` is three terms
// — and kept otherwise, so `(a OR b)` stays one term WITH its parentheses.
func flattenAndNodes(n plansql.Node, out *[]plansql.Node) {
	switch e := n.(type) {
	case *plansql.AndNode:
		flattenAndNodes(e.Left, out)
		flattenAndNodes(e.Right, out)
		return
	case *plansql.ParenNode:
		if _, isAnd := e.Inner.(*plansql.AndNode); isAnd {
			flattenAndNodes(e.Inner, out)
			return
		}
	}
	*out = append(*out, n)
}

// renderConjunct renders one term so that joining the terms back with " AND "
// re-parses to the same tree. Only a bare OR needs help: AND binds tighter, so
// `x = y AND a OR b` would re-associate. NOT, BETWEEN, IN and the comparisons
// all bind tighter than AND already.
func renderConjunct(n plansql.Node) string {
	if _, isOr := n.(*plansql.OrNode); isOr {
		return "(" + n.String() + ")"
	}
	return n.String()
}
