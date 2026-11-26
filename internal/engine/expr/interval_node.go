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
// NULLIF whose first argument is one, and GREATEST / LEAST over one.
// COALESCE is not here: over an interval it is refused today ("cannot store
// … into FLOAT64"), a loud answer this line does not change. `date ± interval` is a TIMESTAMP, not an interval, and is not here.
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
			return l && r
		case "*":
			return l != r
		case "/":
			return l && !r
		}
	case *plansql.FuncCallNode:
		switch strings.ToLower(n.Name) {
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
