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
func IsIntervalNode(node plansql.Node) bool { return IsIntervalNodeWith(node, nil) }

// IsIntervalNodeWith is IsIntervalNode with one more PRODUCER, a node the
// caller's declarations type interval where this package cannot: the
// planner's `timestamp - timestamp` (#1713d round 6; PostgreSQL types it
// interval). The same composition rules apply over it, so a CASE, COALESCE,
// NULLIF, GREATEST, LEAST or interval arithmetic over a producer is one too.
// producer nil is IsIntervalNode.
func IsIntervalNodeWith(node plansql.Node, producer func(plansql.Node) bool) bool {
	if producer != nil && node != nil && producer(node) {
		return true
	}
	rec := func(k plansql.Node) bool { return IsIntervalNodeWith(k, producer) }
	switch n := node.(type) {
	case *plansql.ParenNode:
		return rec(n.Inner)
	case *plansql.IntervalLit:
		return true
	case *plansql.CastNode:
		return strings.EqualFold(strings.TrimSpace(n.TypeName), "interval")
	case *plansql.UnaryOp:
		return n.Op == "-" && rec(n.Inner)
	case *plansql.BinaryOp:
		l, r := rec(n.Left), rec(n.Right)
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
		return intervalOrNullChoice(results, producer)
	case *plansql.FuncCallNode:
		switch strings.ToLower(n.Name) {
		case "coalesce":
			return intervalOrNullChoice(n.Args, producer)
		case "nullif":
			return len(n.Args) == 2 && rec(n.Args[0])
		case "greatest", "least":
			for _, a := range n.Args {
				if rec(a) {
					return true
				}
			}
		}
	}
	return false
}

// IsQuotedIntervalText reports a quoted literal that reads as interval input
// (the planner's timestamp-difference producer leaves `ts - '1 day'` to the
// date arms, which refuse it 22007 as PostgreSQL does).
func IsQuotedIntervalText(n plansql.Node) bool { return isQuotedIntervalText(n) }

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
func intervalOrNullChoice(branches []plansql.Node, producer func(plansql.Node) bool) bool {
	saw := false
	for _, b := range branches {
		switch {
		case IsIntervalNodeWith(b, producer):
			saw = true
		case isNullNode(b), isQuotedNode(b):
		default:
			return false
		}
	}
	return saw
}
