package physical

import (
	"errors"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ErrCorrelatedSubqueryDistributed marks a plan the stage DAG refuses because
// it contains a subquery correlated on the outer query's rows (#359). Such a
// subquery must re-execute once per outer row, and a worker fragment has no
// SubqueryRunner: before this refusal existed, a correlated EXISTS failed the
// task outright while a correlated SCALAR was mis-deferred to a producer stage
// whose dangling outer reference evaluated NULL — the query answered 0,
// silently, on a distributed deployment and correctly single-process.
//
// Only a NON-EQUI correlation reaches this refusal: equality correlations are
// decorrelated into joins by the logical optimizer (TPC-H Q17/Q20/Q22), which
// is why the silent half went unnoticed.
//
// The refusal is typed so the coordinator can route the query onto its local
// single-process pipeline — the engine that owns correlated-subquery
// semantics — instead of surfacing the error (see
// Coordinator.runCorrelatedLocal). A caller without a local engine reports it,
// which is still strictly better than a confident wrong answer. The real
// distributed algorithm for this shape is a dependent join / general
// decorrelation, a separate feature; this mirrors how INTERSECT/EXCEPT were
// refused (#346) until they grew distributed stages.
var ErrCorrelatedSubqueryDistributed = errors.New(
	"correlated subquery requires per-row execution, which the stage DAG does not support")

// refuseCorrelatedSubqueries walks the optimized logical plan and returns a
// typed refusal for the first per-row correlated subquery it finds in a filter
// predicate or SELECT-list projection. Scope is derived exactly the way the
// single-process pipeline derives it when it DECIDES correlation
// (collectTableAliases / collectOuterColumns over the expression's input
// subtree, plus the catalog-backed inner-column resolver of #334), so the two
// paths classify identically by construction: a subquery this pass calls
// uncorrelated is one the single-process engine executes once, and its
// plan-time deferral to a producer stage is sound.
func (p *Planner) refuseCorrelatedSubqueries(node *logical.Node) error {
	if node == nil {
		return nil
	}
	switch node.Type {
	case logical.NodeFilter:
		if len(node.Children) > 0 && len(node.Predicates) > 0 {
			outerTables := collectTableAliases(node.Children[0])
			outerCols := collectOuterColumns(node.Children[0])
			for _, pred := range node.Predicates {
				ast := pred.ASTExpr
				if ast == nil && pred.Raw != "" {
					ast, _ = plansql.ParseExpression(pred.Raw)
				}
				if err := p.refuseCorrelatedInExpr(ast, "filter predicate", outerTables, outerCols); err != nil {
					return err
				}
			}
		}
	case logical.NodeProject:
		if len(node.Children) > 0 {
			outerTables := collectTableAliases(node.Children[0])
			outerCols := collectOuterColumns(node.Children[0])
			for _, proj := range node.Projections {
				ast := proj.ASTExpr
				if ast == nil && proj.Expr != "" {
					ast, _ = plansql.ParseExpression(proj.Expr)
				}
				if err := p.refuseCorrelatedInExpr(ast, "SELECT-list projection", outerTables, outerCols); err != nil {
					return err
				}
			}
		}
	}
	for _, child := range node.Children {
		if err := p.refuseCorrelatedSubqueries(child); err != nil {
			return err
		}
	}
	return nil
}

// refuseCorrelatedInExpr checks every subquery embedded in one expression
// against the outer scope and returns the typed refusal for the first
// correlated one.
func (p *Planner) refuseCorrelatedInExpr(ast plansql.Node, site string, outerTables map[string]bool, outerCols map[string]string) error {
	if ast == nil || len(outerTables) == 0 {
		return nil
	}
	var found error
	visitExprSubqueries(ast, func(sql, construct string) {
		if found != nil {
			return
		}
		refs, err := plansql.FindCorrelatedRefsWithScope(sql, outerTables, outerCols, p.subqueryInnerColumns())
		if err != nil || len(refs) == 0 {
			return
		}
		found = fmt.Errorf("%w: %s in a %s reads outer %s per row"+
			" (equality correlations decorrelate into joins; this one does not);"+
			" the coordinator runs this query single-process",
			ErrCorrelatedSubqueryDistributed, construct, site, describeOuterRefs(refs))
	})
	return found
}

// refuseCorrelated parks a refusal found DURING stage generation, for
// PlanDistributed to return — walkStages has no error return, same shape as
// setOpErr / joinCondErr. First one wins.
func (p *Planner) refuseCorrelated(err error) {
	if p.correlatedErr == nil {
		p.correlatedErr = err
	}
}

// visitExprSubqueries walks an expression AST and calls visit for every
// embedded subquery, labelled by construct. It does not descend into the
// subquery SQL itself: correlation analysis (FindCorrelatedRefsWithScope)
// already recurses through nesting, so a two-deep correlation surfaces at the
// outermost subquery.
func visitExprSubqueries(n plansql.Node, visit func(sql, construct string)) {
	if n == nil {
		return
	}
	switch v := n.(type) {
	case *plansql.SubqueryNode:
		visit(v.SQL, "a scalar subquery")
	case *plansql.ExistsNode:
		visit(v.SQL, "an EXISTS subquery")
	case *plansql.InExpr:
		visitExprSubqueries(v.Left, visit)
		for _, val := range v.Values {
			if sq, ok := val.(*plansql.SubqueryNode); ok {
				visit(sq.SQL, "an IN subquery")
				continue
			}
			visitExprSubqueries(val, visit)
		}
	case *plansql.AnyAllExpr:
		visitExprSubqueries(v.Left, visit)
		for _, val := range v.Values {
			if sq, ok := val.(*plansql.SubqueryNode); ok {
				visit(sq.SQL, "an ANY/ALL subquery")
				continue
			}
			visitExprSubqueries(val, visit)
		}
	case *plansql.BinaryOp:
		visitExprSubqueries(v.Left, visit)
		visitExprSubqueries(v.Right, visit)
	case *plansql.UnaryOp:
		visitExprSubqueries(v.Inner, visit)
	case *plansql.CmpExpr:
		visitExprSubqueries(v.Left, visit)
		visitExprSubqueries(v.Right, visit)
	case *plansql.BetweenExpr:
		visitExprSubqueries(v.Left, visit)
		visitExprSubqueries(v.Low, visit)
		visitExprSubqueries(v.High, visit)
	case *plansql.LikeExpr:
		visitExprSubqueries(v.Left, visit)
		visitExprSubqueries(v.Pattern, visit)
	case *plansql.IsExpr:
		visitExprSubqueries(v.Left, visit)
	case *plansql.AndNode:
		visitExprSubqueries(v.Left, visit)
		visitExprSubqueries(v.Right, visit)
	case *plansql.OrNode:
		visitExprSubqueries(v.Left, visit)
		visitExprSubqueries(v.Right, visit)
	case *plansql.NotNode:
		visitExprSubqueries(v.Inner, visit)
	case *plansql.ParenNode:
		visitExprSubqueries(v.Inner, visit)
	case *plansql.FuncCallNode:
		for _, a := range v.Args {
			visitExprSubqueries(a, visit)
		}
	case *plansql.CaseNode:
		visitExprSubqueries(v.Subject, visit)
		for _, w := range v.Whens {
			visitExprSubqueries(w.Cond, visit)
			visitExprSubqueries(w.Result, visit)
		}
		visitExprSubqueries(v.Else, visit)
	case *plansql.CastNode:
		visitExprSubqueries(v.Inner, visit)
	case *plansql.TupleNode:
		for _, e := range v.Elements {
			visitExprSubqueries(e, visit)
		}
	case *plansql.ArrayLitNode:
		for _, e := range v.Elements {
			visitExprSubqueries(e, visit)
		}
	}
}

// describeOuterRefs renders correlated references for the refusal message.
func describeOuterRefs(refs []plansql.OuterRef) string {
	parts := make([]string, 0, len(refs))
	for _, r := range refs {
		parts = append(parts, r.Table+"."+r.Column)
	}
	if len(parts) == 1 {
		return "column " + parts[0]
	}
	return "columns " + strings.Join(parts, ", ")
}
