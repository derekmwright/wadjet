// SPDX-License-Identifier: MIT

package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// markScalarAnswer marks, in a subquery's plan — the one built to DECLARE its
// answer (scalarAnswerPlan) and the one that COMPUTES it
// (ExecuteSubquerySchema) — every integer CAST and every integral EXTRACT
// field of its SELECT lists, aggregate and window inputs, so the declaration
// walk (answerIntegerOperand) and the kernel (expr.integerOperand) read them
// as integer operands of numeric arithmetic. PostgreSQL types
// `CAST(o.b AS INTEGER) * x.m` and `extract(year FROM o.d) * x.m` numeric,
// and v0.25.3 declared a scalar subquery over either numeric; the same
// expressions in a query's own SELECT list keep this engine's recorded rules
// (N-10, ADR-0024 §2c). The expressions are REWRITTEN, never mutated: a CTE
// body's parsed tree is shared with every plan that reads it.
func markScalarAnswer(n *logical.Node) {
	if n == nil {
		return
	}
	for i := range n.Projections {
		n.Projections[i].ASTExpr = markAnswerExpr(n.Projections[i].ASTExpr)
	}
	for i := range n.AggExprs {
		n.AggExprs[i].InputExpr = markAnswerExpr(n.AggExprs[i].InputExpr)
	}
	for i := range n.WindowExprs {
		n.WindowExprs[i].InputExpr = markAnswerExpr(n.WindowExprs[i].InputExpr)
	}
	for _, c := range n.Children {
		markScalarAnswer(c)
	}
}

func markAnswerExpr(e plansql.Node) plansql.Node {
	if e == nil {
		return nil
	}
	var fn func(plansql.Node) (plansql.Node, bool)
	fn = func(node plansql.Node) (plansql.Node, bool) {
		switch v := node.(type) {
		case *plansql.CastNode:
			return &plansql.CastNode{Inner: plansql.RewriteExpr(v.Inner, fn), TypeName: v.TypeName,
				Column: v.Column, Answer: true}, true
		case *plansql.SubqueryNode:
			if !v.Array && expr.SubqueryAnswersIntegralExtract(v.SQL) {
				c := *v
				c.Answer = true
				return &c, true
			}
		case *plansql.FuncCallNode:
			if expr.IntegralExtractField(v.Name) && len(v.Args) == 1 && !v.Distinct && !v.Star {
				c := *v
				c.Answer = true
				return &c, true
			}
		}
		return nil, false
	}
	return plansql.RewriteExpr(e, fn)
}
