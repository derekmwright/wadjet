package worker

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildWindowKeyProjection compiles the PARTITION BY / window ORDER BY terms
// an OpWindow fragment must compute, into an operator that passes every input
// column through and APPENDS them.
//
// It cannot be exec.Project, which narrows the batch to its own list: a
// window emits every input column plus its own outputs, and this side has no
// input schema to enumerate a passthrough list from. physical's
// pre-aggregate projection is the same operator answering the same need for
// the hash aggregate, so it is reused rather than re-implemented (#585).
//
// A term that will not parse or compile fails the TASK. Skipping it would put
// exec.Window back where #585 found it — resolving a key name nothing
// produces — except that there the operator now refuses, so the only thing
// silence would buy is a worse error message.
func buildWindowKeyProjection(specs []distributed.ProjectSpec) (exec.UnaryOperator, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	cols := make([]exec.ProjectColumn, 0, len(specs))
	seen := make(map[string]bool, len(specs))
	for _, spec := range specs {
		if spec.Name == "" || seen[spec.Name] {
			continue
		}
		seen[spec.Name] = true
		node, err := plansql.ParseExpression(spec.Expr)
		if err != nil {
			return nil, fmt.Errorf("parse window key %q: %w", spec.Expr, err)
		}
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, fmt.Errorf("compile window key %q: %w", spec.Expr, err)
		}
		// The planner's declared type, for the reason the aggregate's
		// derived input carries one: this side has the expression's text and
		// no catalog, so a schema-blind inference would type
		// `PARTITION BY COALESCE(f, 0)` from its literal and truncate every
		// float key on the write (#379). Nil = an older coordinator; the
		// planner's own fallback for an unresolved key is STRING.
		outType := physical.ProjectionOutputType(node, parquet.TypeString)
		if spec.Type != nil {
			outType = parquet.TypeID(*spec.Type)
		}
		e := compiled
		pc := exec.ProjectColumn{
			Name:     spec.Name,
			Type:     outType,
			Computed: true,
			Expr: func(b *batch.RecordBatch, row int) any {
				return e.Eval(b, row)
			},
		}
		if ve, ok := compiled.(expr.VecExpr); ok {
			pc.VecEval = ve.EvalVec
		}
		cols = append(cols, pc)
	}
	if len(cols) == 0 {
		return nil, nil
	}
	return physical.NewComputedColumnsOp(cols), nil
}

// compileFilterExprs parses each scan-pushed filter SQL fragment and returns
// a slice of filter operators plus the union of bare column names they
// reference. The column set lets callers extend their parquet projection
// hint so filter inputs aren't pruned by the source.
//
// Subqueries in filter fragments have already been resolved to literals by
// the planner (see plan.go resolveFilterSubqueries) before FilterExprs is
// populated, so compile here runs with a nil subquery runner. A PER-ROW
// correlated subquery can never reach this compile: PlanDistributed refuses
// that shape (physical.ErrCorrelatedSubqueryDistributed) and the coordinator
// answers it on its local single-process pipeline (#359). The nil-runner
// compile error below is therefore a backstop, not a supported path — it
// fails the task loudly rather than letting a subquery silently evaluate
// wrong.
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
		ops = append(ops, exec.NewFilter(expr.FilterPredicate(compiled)))
	}
	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	return ops, cols, nil
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
// groupByTypes is the plan-time type of each derived key, keyed by its
// exact GroupByCols text (OpSpec.GroupByTypes). It overrides the
// schema-blind ProjectionOutputType inference below, which has no catalog
// and typed COALESCE(l_extendedprice, 0) Int64 from the literal alone —
// truncating every float group key on write (#379). Absent entries (bare
// keys, older coordinators) keep the inference.
func buildAggInputProjection(
	groupBy []string,
	aggs []distributed.AggSpec,
	filterCols []string,
	groupByTypes map[string]int,
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
			// The planner's declared type wins over the schema-blind
			// inference: with no catalog here, a polymorphic key like
			// COALESCE(l_extendedprice, 0) infers Int64 from its literal
			// and the float keys truncate on write (#379).
			outType := physical.ProjectionOutputType(node, parquet.TypeString)
			if t, ok := groupByTypes[c]; ok {
				outType = parquet.TypeID(t)
			}
			projCols = append(projCols, exec.ProjectColumn{
				Name: c,
				// The planner's rule for the same expression. Nothing
				// resolves this at the first batch — exec.Project types a
				// COMPUTED output from the declaration and never from the
				// value — so a blanket String held only for keys that really
				// are strings. CAST(l_shipdate AS DATE) evaluates to an
				// epoch-day number and grouped as the digits of that number
				// (#340). String stays the fallback for anything the rule
				// leaves undecided, which is what it was standing in for.
				Type: outType,
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
		// answer. Nil = an older coordinator that did not declare one —
		// a POINTER because a declared BOOL is TypeID zero (#371), which
		// the old plain-int convention read as undeclared.
		inTyp := parquet.TypeFloat64
		if a.InputType != nil {
			inTyp = parquet.TypeID(*a.InputType)
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
		// p.Type is a pointer exactly because a plain int couldn't tell a
		// declared BOOL (TypeID 0) apart from "never declared" (#445) — nil
		// is the only "not set" case; the fallback below stays STRING, which
		// is what an unresolved computed expression declared before.
		typ := parquet.TypeString
		if p.Type != nil {
			typ = parquet.TypeID(*p.Type)
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
