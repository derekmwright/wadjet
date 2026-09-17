// SPDX-License-Identifier: MIT

package physical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Does the stage carrying a predicate or a projection have the COLUMNS to
// evaluate it?
//
// stageRunsFilterExprs answers a weaker question — does the fragment read the
// field at all — and a stage can pass that and still answer nothing, because
// the expression names a column its input does not carry. That is the whole
// #653/#656 failure mode: `expr.ColRef.Eval` returns nil for a name it cannot
// resolve, the predicate is UNKNOWN on every row, and a WHERE admits only
// TRUE. Every silent shape in the family looks identical at the stage-type
// level and different here.
//
// The check is deliberately partial. A JOIN's input is the QUALIFIED union of
// two sides, with per-column origin rules (BuildColOrigins, QualifyAllBuildCols)
// that only the executor resolves; asserting over it would produce false
// refusals, which are worse than a narrower gate. Join stages are therefore
// excluded and named as excluded, rather than silently passing.

// collectColRefs lists every column reference in an expression.
func collectColRefs(n plansql.Node) []*plansql.ColRef {
	return collectColRefsBelow(n, nil)
}

// collectColRefsBelow is CollectColRefs with a STOP predicate: a node the
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
