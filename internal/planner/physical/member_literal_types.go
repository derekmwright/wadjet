// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// typeMemberLiterals gives the quoted literal on the OUTER side of a
// membership against a subquery — `'2024-1-2' IN (SELECT d …)`, `= ANY`,
// NOT IN, `<> ALL` — the subquery's type IN THE PLAN, as the CAST
// expr.MemberLiteralCast names: PostgreSQL resolves the unknown constant to
// the set's type while it analyses the statement (#1372).
//
// WHY THE PLAN AND NOT THE EXECUTOR. Both arms consume this one logical plan,
// and they meet the membership in two different carriers: the single-process
// arm evaluates it as InSubquery / CorrelatedInSubquery, and the DAG
// materializes the set at plan time into an IN LIST of the members' literal
// spellings (dagplan.materializeInSubquery). The literal was typed only where
// the single-process arm COMPILED it, so on the DAG an unknown literal met
// unknown literals and compared as TEXT: `'2024-1-2'`, `'20240102'`,
// `'2001:DB8::1'`, a braced uuid, `'1_2'`, `'0x0C'` answered 0 rows there
// (NOT IN every row) where the single-process arms and PostgreSQL match —
// only a literal spelled exactly as a member renders matched. Written into
// the plan as a CAST, the literal reaches every carrier already typed, and the
// type's own input function reads it on every arm.
//
// It runs from AnnotateScanColumns, which every entry calls on the plan it
// built and the optimizer calls again on the plan its decorrelations extend,
// so a membership in a decorrelated body is typed too. A typed literal is no
// longer a quoted one, so a second run changes nothing. A literal the type
// cannot read is the binder's plan-time 22P02 / 22007 on a statement it
// validated (expr.CheckMemberLiteral), and the CAST's own at run time
// anywhere else — the same refusal.
func (p *Planner) typeMemberLiterals(root *logical.Node) {
	if p == nil || root == nil {
		return
	}
	// A body that reads a CTE of this statement resolves its type against
	// the statement's WITH list (the reason SubqueryOutputColumn's caller
	// seeds it, as declaredOutputSchemaForPlan does).
	if len(root.CTEs) > 0 {
		saved := p.Ctes
		p.Ctes = root.CTEs
		defer func() { p.Ctes = saved }()
	}
	var node func(n *logical.Node)
	node = func(n *logical.Node) {
		if n == nil {
			return
		}
		for _, preds := range [][]logical.Predicate{n.Predicates, n.ScanPredicates} {
			for i := range preds {
				// The DAG reads a filter back from its TEXT (Raw), so a
				// typed literal is written into both.
				if preds[i].ASTExpr != nil && p.typeMemberLiteralsIn(preds[i].ASTExpr) {
					preds[i].Raw = preds[i].ASTExpr.String()
				}
			}
		}
		for i := range n.Projections {
			p.typeMemberLiteralsIn(n.Projections[i].ASTExpr)
		}
		for i := range n.LateralDualItems {
			p.typeMemberLiteralsIn(n.LateralDualItems[i].ASTExpr)
		}
		for i := range n.AggExprs {
			p.typeMemberLiteralsIn(n.AggExprs[i].InputExpr)
		}
		for i := range n.WindowExprs {
			p.typeMemberLiteralsIn(n.WindowExprs[i].InputExpr)
		}
		for _, g := range n.GroupByExprs {
			p.typeMemberLiteralsIn(g)
		}
		for _, c := range n.Children {
			node(c)
		}
	}
	node(root)
}

// typeMemberLiteralsIn types every membership's quoted-literal outer value in
// one expression, in place, and reports whether it changed anything.
func (p *Planner) typeMemberLiteralsIn(e plansql.Node) bool {
	changed := false
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n == nil {
			return
		}
		switch v := n.(type) {
		case *plansql.InExpr:
			if cast := p.memberLiteralCast(v.Left, v.Values); cast != nil {
				v.Left, changed = cast, true
			}
		case *plansql.AnyAllExpr:
			op := strings.TrimSpace(v.Op)
			mod := strings.ToUpper(v.Modifier)
			if (op == "=" && (mod == "ANY" || mod == "SOME")) || ((op == "<>" || op == "!=") && mod == "ALL") {
				if cast := p.memberLiteralCast(v.Left, v.Values); cast != nil {
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
// nil when there is nothing to type: the operand is not a quoted literal,
// the membership is not against ONE subquery, or its declared type is not
// one the literal takes (a TEXT set keeps the text, as PostgreSQL does).
func (p *Planner) memberLiteralCast(left plansql.Node, values []plansql.Node) plansql.Node {
	lit, ok := plansql.Unparen(left).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString || len(values) != 1 {
		return nil
	}
	sq, ok := plansql.Unparen(values[0]).(*plansql.SubqueryNode)
	if !ok || sq.Array {
		return nil
	}
	col, ok := p.SubqueryOutputColumn(sq.SQL)
	if !ok {
		return nil
	}
	typed, ok := expr.MemberProbe(lit, col.Type)
	if !ok {
		return nil
	}
	return typed
}
