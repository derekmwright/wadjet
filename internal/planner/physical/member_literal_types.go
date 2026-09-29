// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// typeMemberLiterals gives the literal on the OUTER side of a membership
// against a subquery — `'2024-1-2' IN (SELECT d …)`, `= ANY`, NOT IN,
// `<> ALL` — the subquery's type IN THE PLAN, as the CAST expr.MemberProbe
// builds (a quoted literal; against a NUMERIC or integer set also a numeric
// constant or a constant numeric expression): PostgreSQL resolves the unknown
// constant to the set's type while it analyses the statement (#1372).
//
// Why the plan: the single-process arm evaluates the membership as
// InSubquery / CorrelatedInSubquery, and the DAG inlines the set as an IN list
// of the members' spellings (dagplan.materializeInSubquery), so a literal typed
// only where the single-process arm compiled it met the members as TEXT on the
// DAG (docs/adr/0012-divergences/comparison-membership.md names the spellings that missed there). As a CAST in the
// plan it reaches every carrier typed, read by the type's own input function.
//
// It runs from AnnotateScanColumns, which every entry calls and the optimizer
// calls again after its decorrelations, so a decorrelated body is typed too; a
// typed literal is a CAST no rule of expr.MemberProbe matches, so a second run
// changes nothing. A literal the type cannot read is the binder's plan-time
// 22P02 / 22007 (expr.CheckMemberProbe), and the CAST's own at run time
// anywhere else — the same refusal.
func (p *Planner) typeMemberLiterals(root *logical.Node) {
	if p == nil || root == nil {
		return
	}
	ctes := root.CTEs
	var node func(n *logical.Node)
	node = func(n *logical.Node) {
		if n == nil {
			return
		}
		for _, preds := range [][]logical.Predicate{n.Predicates, n.ScanPredicates} {
			for i := range preds {
				// The DAG reads a filter back from its TEXT (Raw), so a
				// typed literal is written into both.
				if preds[i].ASTExpr != nil && p.typeMemberLiteralsIn(ctes, preds[i].ASTExpr) {
					preds[i].Raw = preds[i].ASTExpr.String()
				}
			}
		}
		for i := range n.Projections {
			p.typeMemberLiteralsIn(ctes, n.Projections[i].ASTExpr)
		}
		for i := range n.LateralDualItems {
			p.typeMemberLiteralsIn(ctes, n.LateralDualItems[i].ASTExpr)
		}
		for i := range n.AggExprs {
			p.typeMemberLiteralsIn(ctes, n.AggExprs[i].InputExpr)
		}
		for i := range n.WindowExprs {
			p.typeMemberLiteralsIn(ctes, n.WindowExprs[i].InputExpr)
		}
		for _, g := range n.GroupByExprs {
			p.typeMemberLiteralsIn(ctes, g)
		}
		for _, c := range n.Children {
			node(c)
		}
	}
	node(root)
}

// typeMemberLiteralsIn types every membership's quoted-literal outer value in
// one expression, in place, and reports whether it changed anything.
func (p *Planner) typeMemberLiteralsIn(ctes []plansql.CTEDef, e plansql.Node) bool {
	changed := false
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n == nil {
			return
		}
		switch v := n.(type) {
		case *plansql.InExpr:
			if cast := p.memberLiteralCast(ctes, v.Left, v.Values); cast != nil {
				v.Left, changed = cast, true
			}
		case *plansql.AnyAllExpr:
			op := strings.TrimSpace(v.Op)
			mod := strings.ToUpper(v.Modifier)
			if (op == "=" && (mod == "ANY" || mod == "SOME")) || ((op == "<>" || op == "!=") && mod == "ALL") {
				if cast := p.memberLiteralCast(ctes, v.Left, v.Values); cast != nil {
					v.Left, changed = cast, true
				}
			}
		}
		for _, c := range exprOperands(n) {
			walk(c)
		}
	}
	walk(e)
	return changed
}

// memberLiteralCast is the typed node for a membership's outer operand, or
// nil when there is nothing to type: the operand is not one
// expr.MemberProbeCandidate accepts (a quoted literal, a numeric constant, a
// constant numeric expression), the membership is not against ONE subquery,
// or expr.MemberProbe does not type it for the body's declared type (a TEXT
// set keeps the text, as PostgreSQL does).
//
// A body that reads a CTE of the statement resolves its type against the
// statement's WITH list, which is seeded for this one question (as
// declaredOutputSchemaForPlan seeds it) and only when there is a literal to
// type: a plan with no quoted-literal membership leaves the planner as it
// found it.
func (p *Planner) memberLiteralCast(ctes []plansql.CTEDef, left plansql.Node, values []plansql.Node) plansql.Node {
	if !expr.MemberProbeCandidate(left) || len(values) != 1 {
		return nil
	}
	sq, ok := plansql.Unparen(values[0]).(*plansql.SubqueryNode)
	if !ok || sq.Array {
		return nil
	}
	if len(ctes) > 0 {
		saved := p.Ctes
		p.Ctes = ctes
		defer func() { p.Ctes = saved }()
	}
	col, ok := p.SubqueryOutputColumn(sq.SQL)
	if !ok {
		return nil
	}
	// A literal MemberProbe refuses (the 22003 of a NUMERIC literal no
	// DECIMAL(38,s) holds) is left as written: the binder refuses the
	// statement with the same error before any plan runs, and the compiler's
	// memberProbe refuses a plan-less one. Text the type cannot read (22P02)
	// is still CAST, and the CAST's input function refuses it.
	typed, ok, err := expr.MemberProbe(left, col.Type)
	if !ok || err != nil {
		return nil
	}
	return typed
}
