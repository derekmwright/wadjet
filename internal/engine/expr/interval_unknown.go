// SPDX-License-Identifier: MIT

// This file holds the untyped literal beside an interval; ADR-0012 governs the execution contracts.
package expr

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// unknownAsInterval is a quoted literal beside an INTERVAL operand, typed as
// PostgreSQL's operator resolution types it: the unknown takes the interval
// type (`INTERVAL '1 day' = '24 hours'` is interval = interval, true), so it
// is compiled as CAST(literal AS INTERVAL) — text that is not interval input
// is 22007, as on PostgreSQL. It compared the literal's TEXT with the
// interval's rendering here. Any other node is returned unchanged.
func unknownAsInterval(n plansql.Node) plansql.Node {
	inner := n
	for {
		p, ok := inner.(*plansql.ParenNode)
		if !ok {
			break
		}
		inner = p.Inner
	}
	if l, ok := inner.(*plansql.Lit); ok && l.Kind == plansql.LitString {
		return &plansql.CastNode{Inner: l, TypeName: "INTERVAL"}
	}
	return n
}

// unknownBesideInterval types every quoted literal among nodes as INTERVAL
// when one of them is an interval and every other is a quoted literal or an
// interval (PostgreSQL's select_common_type / operator resolution: an unknown
// takes the one known type). Anything else — a column, a number — leaves the
// list as it is, for the operator's own resolution.
func unknownBesideInterval(nodes []plansql.Node) []plansql.Node {
	sawInterval := false
	for _, n := range nodes {
		switch {
		case IsIntervalNode(n):
			sawInterval = true
		case isQuotedNode(n), isNullNode(n):
		default:
			return nodes
		}
	}
	if !sawInterval {
		return nodes
	}
	out := make([]plansql.Node, len(nodes))
	for i, n := range nodes {
		out[i] = unknownAsInterval(n)
	}
	return out
}

func isQuotedNode(n plansql.Node) bool {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	l, ok := n.(*plansql.Lit)
	return ok && l.Kind == plansql.LitString
}

func isNullNode(n plansql.Node) bool {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	l, ok := n.(*plansql.Lit)
	return ok && l.Kind == plansql.LitNull
}

// typeIntervalUnknowns is node with every quoted literal PostgreSQL resolves
// to INTERVAL beside an interval operand compiled as that interval (#1713c):
// a comparison's two sides, an IN list with its probe, BETWEEN's three
// operands, a simple CASE's WHEN values against an interval subject, and the
// arguments of NULLIF, GREATEST and LEAST. A CASE whose SUBJECT is
// the quoted literal is not here: PostgreSQL resolves that subject to text
// and refuses `text = interval`. The node is copied, never mutated; one with
// nothing to type is returned as it is.
func typeIntervalUnknowns(node plansql.Node) plansql.Node {
	same := func(a, b []plansql.Node) bool {
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	switch n := node.(type) {
	case *plansql.CmpExpr:
		in := []plansql.Node{n.Left, n.Right}
		if out := unknownBesideInterval(in); !same(in, out) {
			c := *n
			c.Left, c.Right = out[0], out[1]
			return &c
		}
	case *plansql.InExpr:
		if len(n.Values) == 1 {
			if _, sub := n.Values[0].(*plansql.SubqueryNode); sub {
				return node
			}
		}
		in := append([]plansql.Node{n.Left}, n.Values...)
		if out := unknownBesideInterval(in); !same(in, out) {
			c := *n
			c.Left, c.Values = out[0], out[1:]
			return &c
		}
	case *plansql.BetweenExpr:
		in := []plansql.Node{n.Left, n.Low, n.High}
		if out := unknownBesideInterval(in); !same(in, out) {
			c := *n
			c.Left, c.Low, c.High = out[0], out[1], out[2]
			return &c
		}
	case *plansql.CaseNode:
		if n.Subject == nil || !IsIntervalNode(n.Subject) {
			return node
		}
		changed := false
		whens := make([]plansql.WhenClause, len(n.Whens))
		for i, w := range n.Whens {
			whens[i] = w
			if isQuotedNode(w.Cond) {
				whens[i].Cond = unknownAsInterval(w.Cond)
				changed = true
			}
		}
		if changed {
			c := *n
			c.Whens = whens
			return &c
		}
	case *plansql.FuncCallNode:
		switch strings.ToLower(n.Name) {
		case "nullif", "greatest", "least":
		default:
			return node
		}
		if out := unknownBesideInterval(n.Args); !same(n.Args, out) {
			c := *n
			c.Args = out
			return &c
		}
	}
	return node
}
