package worker

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// buildAggInputProjection returns a Project operator that materializes
// each aggregate's derived input expression into a named column that
// HashAggregate can look up by AggSpec.InputCol. Pass-through columns
// (GROUP BY keys, filter references, bare-column aggregate inputs) are
// included via DirectCopy so the output batch contains everything the
// downstream aggregate or filter needs.
//
// Returns (nil, nil) when no aggregate has a derived InputExpr — the
// caller skips inserting a Project in that case.
//
// The referenced-columns list is the union of all bare columns each
// derived expression reads; callers extend the source projection hint
// with these so parquet readers don't prune them.
func buildAggInputProjection(
	groupBy []string,
	aggs []distributed.AggSpec,
	filterCols []string,
) (*exec.Project, []string, error) {
	// Detect derived inputs. An aggregate can carry InputExpr explicitly;
	// a GROUP BY column is "derived" when parsing it yields anything
	// other than a bare column reference (e.g. SUBSTR(o_orderdate, 1, 4)
	// for Q09). Without projection, HashAggregate looks up the literal
	// expression string as a column name, misses, and buckets every row
	// into a nil group.
	hasDerived := false
	derivedGroupBy := make(map[string]plansql.Node)
	for _, a := range aggs {
		if a.InputExpr != "" {
			hasDerived = true
		}
	}
	for _, g := range groupBy {
		if g == "" {
			continue
		}
		node, err := plansql.ParseExpression(g)
		if err != nil {
			continue
		}
		if _, bare := node.(*plansql.ColRef); bare {
			continue
		}
		derivedGroupBy[g] = node
		hasDerived = true
	}
	if !hasDerived {
		return nil, nil, nil
	}

	projCols := make([]exec.ProjectColumn, 0, len(groupBy)+len(aggs)+len(filterCols))
	seen := make(map[string]bool)
	addPassthrough := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		projCols = append(projCols, exec.ProjectColumn{
			Name:       name,
			DirectCopy: name,
			SourceCol:  name,
			// Fallback for when DirectCopy resolution misses — without
			// this, Project.Execute panics invoking a nil Expr on any
			// column it can't resolve by name. ColumnRef lazily resolves
			// the column index on first call and returns nil if missing,
			// which HashAggregate tolerates (null row value).
			Expr: exec.ColumnRef(name),
		})
	}
	for _, c := range groupBy {
		if node, ok := derivedGroupBy[c]; ok {
			// Compile the expression once and emit a projection under
			// the same name HashAggregate expects.
			collectFilterColumns(node, nil)
			compiled, err := expr.Compile(node)
			if err != nil {
				return nil, nil, fmt.Errorf("compile group-by %q: %w", c, err)
			}
			if seen[c] {
				continue
			}
			seen[c] = true
			e := compiled
			projCols = append(projCols, exec.ProjectColumn{
				Name: c,
				Type: parquet.TypeString, // actual type resolved at first batch
				Expr: func(b *batch.RecordBatch, row int) any {
					return e.Eval(b, row)
				},
			})
			continue
		}
		addPassthrough(c)
	}
	for _, c := range filterCols {
		addPassthrough(c)
	}

	// Union of extra columns referenced by all derived expressions,
	// returned to the caller so scan projection includes them.
	extraColSet := make(map[string]struct{})
	for _, a := range aggs {
		if a.InputExpr == "" {
			addPassthrough(a.InputCol)
			continue
		}
		node, err := plansql.ParseExpression(a.InputExpr)
		if err != nil {
			return nil, nil, fmt.Errorf("parse agg input %q: %w", a.InputExpr, err)
		}
		collectFilterColumns(node, extraColSet)
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, nil, fmt.Errorf("compile agg input %q: %w", a.InputExpr, err)
		}
		if seen[a.InputCol] {
			// A prior agg already declared the same derived column.
			continue
		}
		seen[a.InputCol] = true
		e := compiled
		// The planner's answer, carried on the spec. Float64 is only the
		// default for an aggregate input, not a fact about it —
		// MAX(UPPER(c)) is a string (#310) and so is
		// MAX(COALESCE(a, b)) over two string columns (#333), and writing
		// either into a Float64 vector drops every value in silence. The
		// planner resolves it against the catalog; this side has the
		// expression's text and nothing else, so it can only carry the
		// answer. Zero = an older coordinator that did not declare one.
		inTyp := parquet.TypeID(a.InputType)
		if inTyp == 0 {
			inTyp = parquet.TypeFloat64
		}
		projCols = append(projCols, exec.ProjectColumn{
			Name: a.InputCol,
			Type: inTyp,
			Expr: func(b *batch.RecordBatch, row int) any {
				return e.Eval(b, row)
			},
		})
	}

	// Make sure all bare columns referenced by expressions are passed
	// through (HashAggregate doesn't need them, but filter columns are
	// captured separately).
	extra := make([]string, 0, len(extraColSet))
	for c := range extraColSet {
		extra = append(extra, c)
	}
	return exec.NewProject(projCols), extra, nil
}

// collectFilterColumns walks a SQL expression AST and records the bare
// column names referenced. Qualified table.column refs still collect just
// the column portion — the scan alias is implicit for single-table filter
// fragments produced by the planner.
func collectFilterColumns(n plansql.Node, out map[string]struct{}) {
	if n == nil || out == nil {
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

// buildSelectProjection compiles an OpProject spec into an exec.Project.
// Bare column references become passthrough copies (DirectCopy with a
// ColumnRef fallback); everything else compiles through the expression
// engine and evaluates per row. Output columns appear in spec order and are
// exactly the fragment's output schema — anything not listed is dropped,
// which is the point: the fragment emits the SELECT list, not the scan's
// input columns (#169).
func buildSelectProjection(specs []distributed.ProjectSpec) (*exec.Project, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("project: at least one projection required")
	}
	projCols := make([]exec.ProjectColumn, 0, len(specs))
	for _, p := range specs {
		node, err := plansql.ParseExpression(p.Expr)
		if err != nil {
			return nil, fmt.Errorf("parse projection %q: %w", p.Expr, err)
		}
		if ref, ok := node.(*plansql.ColRef); ok {
			src := ref.String()
			projCols = append(projCols, exec.ProjectColumn{
				Name:       p.Name,
				DirectCopy: src,
				SourceCol:  src,
				Expr:       exec.ColumnRef(src),
			})
			continue
		}
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, fmt.Errorf("compile projection %q: %w", p.Expr, err)
		}
		typ := parquet.TypeID(p.Type)
		if p.Type == 0 {
			typ = parquet.TypeString
		}
		e := compiled
		projCols = append(projCols, exec.ProjectColumn{
			Name: p.Name,
			// Plan-time inferred type: the output column doesn't exist in
			// the input schema, so exec.Project cannot resolve it there —
			// unless the alias happens to name a DIFFERENT input column,
			// which is the shadowing case Computed exists to reject (#327).
			Type:     typ,
			Computed: true,
			Expr: func(b *batch.RecordBatch, row int) any {
				return e.Eval(b, row)
			},
		})
	}
	return exec.NewProject(projCols), nil
}
