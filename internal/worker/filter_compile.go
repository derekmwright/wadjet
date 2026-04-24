package worker

import (
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// compileFilterExprs parses each scan-pushed filter SQL fragment and returns
// a slice of filter operators plus the union of bare column names they
// reference. The column set lets callers extend their parquet projection
// hint so filter inputs aren't pruned by the source.
//
// Subqueries in filter fragments have already been resolved to literals by
// the planner (see plan.go resolveFilterSubqueries) before FilterExprs is
// populated, so compile here runs with a nil subquery runner.
func compileFilterExprs(exprs []string) ([]exec.UnaryOperator, []string, error) {
	if len(exprs) == 0 {
		return nil, nil, nil
	}
	ops := make([]exec.UnaryOperator, 0, len(exprs))
	colSet := make(map[string]struct{})
	for _, s := range exprs {
		node, err := plansql.ParseExpression(s)
		if err != nil {
			return nil, nil, fmt.Errorf("parse filter %q: %w", s, err)
		}
		collectFilterColumns(node, colSet)
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, nil, fmt.Errorf("compile filter %q: %w", s, err)
		}
		ops = append(ops, exec.NewFilter(wrapCompiledFilter(compiled)))
	}
	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	return ops, cols, nil
}

func wrapCompiledFilter(e expr.Expr) exec.Predicate {
	if be, ok := e.(expr.BoolExpr); ok {
		return func(b *batch.RecordBatch, row int) bool {
			return be.EvalBool(b, row)
		}
	}
	return func(b *batch.RecordBatch, row int) bool {
		v := e.Eval(b, row)
		if v == nil {
			return false
		}
		if bv, ok := v.(bool); ok {
			return bv
		}
		return false
	}
}

// collectFilterColumns walks a SQL expression AST and records the bare
// column names referenced. Qualified table.column refs still collect just
// the column portion — the scan alias is implicit for single-table filter
// fragments produced by the planner.
func collectFilterColumns(n plansql.Node, out map[string]struct{}) {
	if n == nil {
		return
	}
	switch v := n.(type) {
	case *plansql.ColRef:
		if v.Column != "" {
			out[v.Column] = struct{}{}
		}
	case *plansql.BinaryOp:
		collectFilterColumns(v.Left, out)
		collectFilterColumns(v.Right, out)
	case *plansql.UnaryOp:
		collectFilterColumns(v.Inner, out)
	case *plansql.CmpExpr:
		collectFilterColumns(v.Left, out)
		collectFilterColumns(v.Right, out)
	case *plansql.InExpr:
		collectFilterColumns(v.Left, out)
		for _, val := range v.Values {
			collectFilterColumns(val, out)
		}
	case *plansql.BetweenExpr:
		collectFilterColumns(v.Left, out)
		collectFilterColumns(v.Low, out)
		collectFilterColumns(v.High, out)
	case *plansql.LikeExpr:
		collectFilterColumns(v.Left, out)
		collectFilterColumns(v.Pattern, out)
	case *plansql.IsExpr:
		collectFilterColumns(v.Left, out)
	case *plansql.AndNode:
		collectFilterColumns(v.Left, out)
		collectFilterColumns(v.Right, out)
	case *plansql.OrNode:
		collectFilterColumns(v.Left, out)
		collectFilterColumns(v.Right, out)
	case *plansql.NotNode:
		collectFilterColumns(v.Inner, out)
	case *plansql.ParenNode:
		collectFilterColumns(v.Inner, out)
	case *plansql.FuncCallNode:
		for _, a := range v.Args {
			collectFilterColumns(a, out)
		}
	case *plansql.CaseNode:
		collectFilterColumns(v.Subject, out)
		for _, w := range v.Whens {
			collectFilterColumns(w.Cond, out)
			collectFilterColumns(w.Result, out)
		}
		collectFilterColumns(v.Else, out)
	case *plansql.CastNode:
		collectFilterColumns(v.Inner, out)
	case *plansql.TupleNode:
		for _, e := range v.Elements {
			collectFilterColumns(e, out)
		}
	case *plansql.ArrayLitNode:
		for _, e := range v.Elements {
			collectFilterColumns(e, out)
		}
	case *plansql.AnyAllExpr:
		collectFilterColumns(v.Left, out)
		for _, val := range v.Values {
			collectFilterColumns(val, out)
		}
	}
}
