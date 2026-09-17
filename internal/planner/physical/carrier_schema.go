// SPDX-License-Identifier: MIT

package physical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Which column references does a predicate or projection read? These walks
// collect them from the expression, with an optional stop at a computed column.
//
// The distributed caller checks whether its fragment receives those columns;
// that check lives in dagplan/carrier_schema.go after the split (ADR-0037 §6).
// An expression can name a column its input does not carry. That is the whole
// #653/#656 failure mode: `expr.ColRef.Eval` returns nil for a name it cannot
// resolve, the predicate is UNKNOWN on every row, and a WHERE admits only
// TRUE. Collecting the references supplies the caller with the names it needs
// to compare against the producer's output.
//
// Collection itself does not decide how a JOIN names its output. A join has
// two sides, with per-column origin rules that the executor resolves. The
// caller supplies that interpretation; the walks below only traverse the
// expression. A stop predicate lets a caller treat a computed expression as
// one published column instead of reading its original operands again.

// collectColRefs lists every column reference in an expression.
func collectColRefs(n plansql.Node) []*plansql.ColRef {
	return collectColRefsBelow(n, nil)
}

// collectColRefsBelow is collectColRefs with a STOP predicate: a node the
// predicate accepts is a column in its own right, and its children are not
// references at all.
//
// That distinction is the difference between a defect and a false refusal on
// a computed GROUP BY key. An aggregate stage emits its key under the key's
// own EXPRESSION TEXT, so `g + 1` is the column NAME and a HAVING spelled
// `g + 1 > 2` resolves against it exactly — while a walk that descends into
// the term sees a reference to `g`, which the aggregate's OUTPUT genuinely
// does not carry. Reading that as unresolvable refused
// `WITH a AS (SELECT g+1 AS gk, COUNT(*) AS n FROM t GROUP BY g+1 HAVING
// g+1 > 2) SELECT gk, n FROM a WHERE gk > 3` outright, on a plan whose
// fragment computes it correctly.
func collectColRefsBelow(n plansql.Node, stop func(plansql.Node) bool) []*plansql.ColRef {
	var out []*plansql.ColRef
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n != nil && stop != nil && stop(n) {
			return
		}
		switch e := n.(type) {
		case nil:
			return
		case *plansql.ColRef:
			out = append(out, e)
		case *plansql.BinaryOp:
			walk(e.Left)
			walk(e.Right)
		case *plansql.CmpExpr:
			walk(e.Left)
			walk(e.Right)
		case *plansql.AndNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.OrNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.NotNode:
			walk(e.Inner)
		case *plansql.UnaryOp:
			walk(e.Inner)
		case *plansql.ParenNode:
			walk(e.Inner)
		case *plansql.CastNode:
			walk(e.Inner)
		case *plansql.IsExpr:
			walk(e.Left)
		case *plansql.LikeExpr:
			walk(e.Left)
			walk(e.Pattern)
		case *plansql.BetweenExpr:
			walk(e.Left)
			walk(e.Low)
			walk(e.High)
		case *plansql.InExpr:
			walk(e.Left)
			for _, v := range e.Values {
				walk(v)
			}
		case *plansql.FuncCallNode:
			for _, a := range e.Args {
				walk(a)
			}
		case *plansql.CaseNode:
			walk(e.Subject)
			walk(e.Else)
			for _, w := range e.Whens {
				walk(w.Cond)
				walk(w.Result)
			}
		}
	}
	walk(n)
	return out
}
