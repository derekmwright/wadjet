// SPDX-License-Identifier: MIT

// This file holds the interval operand rule; ADR-0012 governs the execution contracts.
package expr

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// IsIntervalNode reports whether a parsed operand is an INTERVAL value: a
// literal, a CAST to INTERVAL, and the operators and choice constructs that
// answer one as PostgreSQL types them (#1713c) — interval ± interval, interval
// * number, number * interval, interval / number, unary minus of an interval,
// NULLIF whose first argument is one, GREATEST / LEAST over one, and a CASE
// whose every result — or a COALESCE whose every argument — is an interval, a
// quoted literal or NULL, one at least an interval (round 3: the plan declared
// those float8, so `COALESCE(INTERVAL '1 day', NULL)` and `CASE … END * 2` met
// the store guard). A quoted literal there is interval input, as PostgreSQL's
// select_common_type resolves it (typeIntervalUnknowns compiles it so).
// `date ± interval` is a TIMESTAMP, not an interval, and is not here.
//
// It is one rule for the plan (the declaration, the temporal validators) and
// the compile (an unknown literal beside an interval reads as an interval).
func IsIntervalNode(node plansql.Node) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return IsIntervalNode(n.Inner)
	case *plansql.IntervalLit:
		return true
	case *plansql.CastNode:
		return strings.EqualFold(strings.TrimSpace(n.TypeName), "interval")
	case *plansql.UnaryOp:
		return n.Op == "-" && IsIntervalNode(n.Inner)
	case *plansql.BinaryOp:
		l, r := IsIntervalNode(n.Left), IsIntervalNode(n.Right)
		switch n.Op {
		case "+", "-":
			// A quoted operand that reads as interval input beside an
			// interval is one (PostgreSQL resolves the unknown to interval;
			// intervalPair evaluates it so): `INTERVAL '1 day' + '1 hour'`.
			return (l && r) || (l && isQuotedIntervalText(n.Right)) || (r && isQuotedIntervalText(n.Left))
		case "*":
			return l != r
		case "/":
			return l && !r
		}
	case *plansql.CaseNode:
		results := make([]plansql.Node, 0, len(n.Whens)+1)
		for _, w := range n.Whens {
			results = append(results, w.Result)
		}
		if n.Else != nil {
			results = append(results, n.Else)
		}
		return intervalOrNullChoice(results)
	case *plansql.FuncCallNode:
		switch strings.ToLower(n.Name) {
		case "coalesce":
			return intervalOrNullChoice(n.Args)
		case "nullif":
			return len(n.Args) == 2 && IsIntervalNode(n.Args[0])
		case "greatest", "least":
			for _, a := range n.Args {
				if IsIntervalNode(a) {
					return true
				}
			}
		}
	}
	return false
}

// isQuotedIntervalText reports a quoted literal that reads as interval input.
func isQuotedIntervalText(n plansql.Node) bool {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	l, ok := n.(*plansql.Lit)
	return ok && l.Kind == plansql.LitString && intervalInputText(l.Value)
}

// intervalOrNullChoice reports a choice's branches that are each an interval,
// a quoted literal or NULL, at least one an interval: PostgreSQL's
// select_common_type answers interval for them.
func intervalOrNullChoice(branches []plansql.Node) bool {
	saw := false
	for _, b := range branches {
		switch {
		case IsIntervalNode(b):
			saw = true
		case isNullNode(b), isQuotedNode(b):
		default:
			return false
		}
	}
	return saw
}
