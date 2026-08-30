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
			outType = expr.DeclType{ID: parquet.TypeID(*spec.Type),
				Precision: spec.Precision, Scale: spec.Scale,
				DecKnown: spec.Precision > 0}
		}
		e := compiled
		pc := exec.ProjectColumn{
			Name:      spec.Name,
			Type:      outType.ID,
			Precision: outType.Precision,
			Scale:     outType.Scale,
			Computed:  true,
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
//
// scanSchema says the input is a BASE-TABLE SCAN's output, whose schema is the
// catalog's declaration — the one place a missing predicate column can only
// mean the planner named something the table does not have. See
// OpSpec.ScanSchemaFilter for why a filter above a JOIN cannot be checked the
// same way.
func compileFilterExprs(exprs []string, scanSchema bool) ([]exec.UnaryOperator, []string, error) {
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
		f := exec.NewFilter(expr.FilterPredicate(compiled))
		// The #147 guard for the ROW evaluator. A scan stage's filter text is
		// written by the planner against the catalog's schema, and every
		// modelling defect so far has arrived here as a name the scan's
		// batches do not carry — a renamed CTE column was the last one (#653).
		// Without this the predicate is UNKNOWN on every row and the task
		// answers zero rows in silence; with it the task fails and names the
		// column. Refs are declined for a subquery-bearing predicate, where a
		// name may legitimately resolve outside the batch.
		if refs, ok := expr.FilterColumnRefs(node); ok && scanSchema {
			f.Check = func(b *batch.RecordBatch) error {
				if err := expr.CheckFilterColumns(b, refs); err != nil {
					return fmt.Errorf("filter %q: %w", s, err)
				}
				return nil
			}
		}
		ops = append(ops, f)
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
// exact GroupByCols text (OpSpec.GroupByTypes), and groupByDecimal carries
// the (p,s) of its DECIMAL entries. Together they override the
// schema-blind ProjectionOutputType inference below, which has no catalog
// and typed COALESCE(l_extendedprice, 0) Int64 from the literal alone —
// truncating every float group key on write (#379). Absent entries (bare
// keys, older coordinators) keep the inference.
func buildAggInputProjection(
	groupBy []string,
	aggs []distributed.AggSpec,
	filterCols []string,
	groupByTypes map[string]int,
	groupByDecimal map[string]distributed.DecimalMeta,
) (*exec.Project, []string, error) {
	// Detect derived inputs. An aggregate can carry InputExpr explicitly;
	// a GROUP BY column is "derived" when parsing it yields anything
	// other than a bare column reference (e.g. SUBSTR(o_orderdate, 1, 4)
	// for Q09). Without projection, HashAggregate looks up the literal
	// expression string as a column name, misses, and buckets every row
	// into a nil group.
	hasDerived := false
	for _, a := range aggs {
		if a.InputExpr != "" {
			hasDerived = true
		}
	}
	derivedGroupBy, slots := derivedGroupKeys(groupBy, aggs, filterCols, groupByTypes)
	if len(derivedGroupBy) > 0 {
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
	for gi, c := range groupBy {
		if node, ok := derivedGroupBy[c]; ok {
			// Compile the expression once and emit a projection under
			// the same name HashAggregate expects.
			collectFilterColumns(node, nil)
			compiled, err := expr.Compile(node)
			if err != nil {
				return nil, nil, fmt.Errorf("compile group-by %q: %w", c, err)
			}
			// The HIDDEN SLOT, not the key's own text: this projection
			// NARROWS to its outputs, so a derived key named after its own
			// canonical text SHADOWS an input column the query spells the
			// same way — and the single-process pre-aggregate projection,
			// which APPENDS, was shadowed BY it. One name, two engines, two
			// different wrong answers. The key is published under its
			// canonical text by GroupByOutNames on the aggregate itself
			// (ADR-0026).
			slot := slots[gi]
			if seen[slot] {
				continue
			}
			seen[slot] = true
			e := compiled
			// The planner's declared type wins over the schema-blind
			// inference: with no catalog here, a polymorphic key like
			// COALESCE(l_extendedprice, 0) infers Int64 from its literal
			// and the float keys truncate on write (#379).
			outType := physical.ProjectionOutputType(node, parquet.TypeString)
			if t, ok := groupByTypes[c]; ok {
				outType = expr.Decl(parquet.TypeID(t))
				if m, ok := groupByDecimal[c]; ok && m.Precision > 0 {
					// A DECIMAL key's (p,s), without which the vector below
					// comes out at scale 0 and truncates every value written
					// into it — 12.7500 and 12.7501 collapsing into one group
					// holding 12 (ADR-0024 item 2, #379's shape one type
					// over).
					outType = expr.DeclDecimal(m.Precision, m.Scale)
				}
			}
			projCols = append(projCols, exec.ProjectColumn{
				Name: slot,
				// The planner's rule for the same expression. Nothing
				// resolves this at the first batch — exec.Project types a
				// COMPUTED output from the declaration and never from the
				// value — so a blanket String held only for keys that really
				// are strings. CAST(l_shipdate AS DATE) evaluates to an
				// epoch-day number and grouped as the digits of that number
				// (#340). String stays the fallback for anything the rule
				// leaves undecided, which is what it was standing in for.
				Type:      outType.ID,
				Precision: outType.Precision,
				Scale:     outType.Scale,
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
		inPrec, inScale := 0, 0
		if a.InputType != nil {
			inTyp = parquet.TypeID(*a.InputType)
			// A DECIMAL input's (p,s) rides with the TypeID for the reason a
			// DECIMAL group key's does: the materialized vector is built from
			// the declaration alone, and one at scale 0 truncates every value
			// — MAX(COALESCE(a, b)) answered 12 for 12.75 (ADR-0024 item 2).
			inPrec, inScale = a.InputPrecision, a.InputScale
		}
		projCols = append(projCols, exec.ProjectColumn{
			Name:      a.InputCol,
			Type:      inTyp,
			Precision: inPrec,
			Scale:     inScale,
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
			// The UNQUOTED spelling, which is the name a batch column
			// carries and the one expr.Compile resolves a ColRef by
			// (compile.go: `name := n.Column`). ColRef.String() re-quotes a
			// delimited identifier, so it named `"g + 1"` — quotes included
			// — for a column really called `g + 1`, and the projection
			// failed on a column the batch did carry (#656).
			src := ref.Column
			if ref.Table != "" {
				src = ref.Table + "." + ref.Column
			}
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
			Type: typ,
			// And a DECIMAL's (p,s) with it: the vector this fills is built
			// from the declaration alone, and one without a scale reads
			// every value back at 10^0 (ADR-0024 item 2).
			Precision: p.Precision,
			Scale:     p.Scale,
			Computed:  true,
			Expr: func(b *batch.RecordBatch, row int) any {
				return e.Eval(b, row)
			},
		})
	}
	return exec.NewProject(projCols), nil
}

// derivedGroupKeys splits a fragment's GROUP BY key list into the keys this
// fragment must COMPUTE and the column each key is RESOLVED by.
//
// A key is derived when parsing it yields anything but a bare column
// reference, and also when it IS a bare reference the planner marked derived:
// a ROW FIELD PATH (`c_row.b`) parses to a ColRef and names no column any
// stage emits, so HashAggregate could not look it up and the key serialized
// as NULL. groupByTypes is the planner's answer — derivedGroupKeyTypes
// records an entry for exactly the keys that must be computed here, and a
// bare column has none (#568).
//
// slots is parallel to groupBy: a bare key resolves by its own name, and a
// derived key by a hidden `__gb_expr_N` that no query can spell (the planner's
// reserved namespace). Naming the computed column after the key's own text
// instead put it in the user's namespace, where it shadowed — or was shadowed
// by — an input column of the same spelling, differently on each engine
// (ADR-0026).
func derivedGroupKeys(groupBy []string, aggs []distributed.AggSpec, filterCols []string,
	groupByTypes map[string]int) (map[string]plansql.Node, []string) {
	derived := make(map[string]plansql.Node)
	slots := make([]string, len(groupBy))
	// Every name this fragment BINDS, so a slot can be minted clear of all of
	// them. A slot is hidden only when nothing else answers to it, and there
	// are three ways for something to:
	//
	//   - a group key's own name;
	//   - an AGGREGATE'S ARGUMENT or output. A stored column may legitimately
	//     be called `__gb_expr_0` — the reservation binds where a user MINTS
	//     a name, not where one is read back, so such a column is never
	//     refused. `SUM(__gb_expr_0)` beside `GROUP BY g + 1` was answered
	//     from the KEY's slot, because this projection narrows and the key
	//     had already claimed the name: right keys, right row count, and the
	//     sum of a group key where the query asked for the sum of a column.
	//   - a filter column, for the same reason.
	//
	// And each slot excludes the ones already ISSUED, which is what stops two
	// derived keys landing in one column.
	taken := make(map[string]bool, len(groupBy)+len(aggs)+len(filterCols))
	for _, g := range groupBy {
		taken[g] = true
	}
	for _, a := range aggs {
		taken[a.InputCol] = true
		taken[a.InputCol2] = true
		taken[a.OutputCol] = true
	}
	for _, c := range filterCols {
		taken[c] = true
	}
	// Termination: the excluded set is finite and fixed before the first
	// call, plus at most one name per key, so a scan bounded by its size
	// cannot exhaust the family.
	limit := len(taken) + len(groupBy) + 2
	mint := func(i int) string {
		for n := i; n <= i+limit; n++ {
			name := physical.SlotName(physical.SlotGroupKey, n)
			if !taken[name] {
				taken[name] = true
				return name
			}
		}
		return physical.SlotName(physical.SlotGroupKey, i)
	}
	for i, g := range groupBy {
		slots[i] = g
		if g == "" {
			continue
		}
		node, err := plansql.ParseExpression(g)
		if err != nil {
			continue
		}
		if _, bare := node.(*plansql.ColRef); bare {
			if _, isDerived := groupByTypes[g]; !isDerived {
				continue
			}
		}
		derived[g] = node
		slots[i] = mint(i)
	}
	return derived, slots
}

// equalStringSlices reports whether two name lists are identical.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
