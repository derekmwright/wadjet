// This file holds projection plan for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func (p *Planner) buildFilter(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("filter has no child")
	}

	source, ops, sink, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	// Collect outer table aliases and columns for correlated subquery detection
	outerTables := collectTableAliases(node.Children[0])
	outerCols := collectOuterColumns(node.Children[0])

	// Scan-level filter pushdown: when the filter sits directly on a
	// catalog scan, eligible conjuncts move into the scan (dictionary-mask
	// evaluation, no materialization of filter-only columns) and only the
	// residue compiles into exec filter ops. See scan_filter_pushdown.go.
	preds := node.Predicates
	if css, ok := source.(*catalogScanSource); ok && len(ops) == 0 &&
		!node.PolicyFilter && !subtreeHasSecurityBarrier(node.Children[0]) {
		// NOT below a security projection. Scan-level pushdown evaluates the
		// predicate against the FILE, so pushing one that sits ABOVE a
		// barrier makes it read the STORED column — the in-process twin of
		// the DAG's single filter slot. `IN (SELECT id FROM t WHERE ssn =
		// '***')` compared the stored SSN against the mask and answered no
		// rows; `… WHERE bal > 300` over a masked `bal` answered exactly the
		// rows above the threshold (#859 round 3).
		//
		// The POLICY's own filter is exempt for the reason it always is: it
		// is supposed to read the row as stored, and it sits BELOW the
		// barrier, so pushing it into the scan is the same evaluation.
		preds = p.tryPushFilterIntoScan(ctx, node, css)
	}

	for _, pred := range preds {
		filter, err := p.buildFilterOp(pred, outerTables, outerCols)
		if err != nil {
			return nil, nil, nil, err
		}
		if filter != nil {
			ops = append(ops, filter)
		}
	}

	return source, ops, sink, nil
}

func (p *Planner) buildProject(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("project has no child")
	}

	child := node.Children[0]

	// A star sharing its SELECT list with other items — `SELECT t.*, ctid` is
	// how DataGrip opens a table — reaches the planner as a projection of the
	// literal column "*". logical.Optimize expands it (before column pruning,
	// which is what #315 turned on); this catches the unoptimized-plan case.
	p.expandStarProjections(ctx, node, child)
	if err := refuseUnexpandedStarBesideItems(node); err != nil {
		return nil, nil, nil, err
	}

	// If the child (or child chain through Filter/HAVING) leads to an Aggregate,
	// skip the projection when possible — the aggregate already produces correctly
	// named output columns (group-by cols + agg output cols).
	// Keep the projection when:
	//   1. Any non-aggregate projection has a complex AST expression (e.g., SUM(x) * 0.0001)
	//   2. Any projection renames a column via alias (e.g., l_suppkey AS supplier_no)
	if hasAggregateAncestor(child) {
		// A group key is decided against the AGGREGATE'S INPUT, which is
		// where a ROW column and its fields live — the aggregate's own
		// output carries neither.
		var elideKeyDecls colDecls
		if agg := findAggregateAncestor(child); agg != nil && len(agg.Children) == 1 {
			elideKeyDecls = inputColDecls(agg.Children[0])
		}
		needsProject := false
		for _, proj := range node.Projections {
			// A literal select item is NOT elidable: the aggregate's output
			// carries it as a synthetic __gb_expr_N key column, and only the
			// projection renames it to the select-list name.
			if proj.ASTExpr != nil && !proj.IsAgg && !isPlainGroupKey(proj.ASTExpr, elideKeyDecls) {
				needsProject = true
				break
			}
			// Aggregate projection with a wrapping scalar function
			// e.g., format_bytes(SUM(rx_bytes)) — the outer function must be
			// applied as a post-aggregate projection.
			if proj.IsAgg && proj.ASTExpr != nil {
				if fn, ok := proj.ASTExpr.(*plansql.FuncCallNode); ok {
					if !plansql.IsAggregate(fn.Name) {
						needsProject = true
						break
					}
				}
				if _, ok := proj.ASTExpr.(*plansql.BinaryOp); ok {
					needsProject = true
					break
				}
			}
			// Check for column rename on non-aggregate columns: alias differs
			// from source column/expression (aggregate columns already use
			// the alias as their OutputCol, so no rename needed).
			if !proj.IsAgg && proj.Alias != "" {
				src := proj.Column
				if src == "" {
					src = proj.Expr
				}
				if proj.Alias != src {
					needsProject = true
					break
				}
			}
		}
		if !needsProject {
			// Elide an aggregate's projection only when its output exactly matches the
			// projected columns in order; hidden HAVING outputs and unselected group keys
			// must not reach the client (#591). Unknown shapes, including grouping sets,
			// keep the projection. Do not look through a Window: it appends __win_N beyond
			// aggregateOutputNames' aggregate-only answer (#575). Sort and LIMIT add no
			// columns and are safe to look through.
			if names, ok := aggregateOutputNames(child); ok &&
				!wrapsAWindow(child) && namesMatchProjections(names, node.Projections) {
				return p.buildPipeline(ctx, child)
			}
		}
	}

	source, ops, sink, err := p.buildPipeline(ctx, child)
	if err != nil {
		return nil, nil, nil, err
	}

	aggNode := findAggregateAncestor(child)
	isOverAggregate := aggNode != nil

	// Which column a SELECT item that IS a derived GROUP BY key reads.
	// `SUBSTR(c_phone, 1, 2)` is computed below the aggregate and published
	// under one name; above the aggregate its source columns are gone, so
	// re-evaluating the expression there answers NULL for every row.
	//
	// The lookup is by plansql.ExprIdentity, not by rendered text. The two
	// spellings of one key differ in ways SQL does not distinguish —
	// `(g + 1)` against `g + 1`, `G + 1` against `g + 1` — and comparing the
	// renderings made which spelling was used decide whether the query
	// answered or came back with a NULL key column (#723).
	gbExprToSyn := groupKeyByIdentity(aggNode)

	// Catalog types of what feeds these projections, resolved once for the
	// whole list: a bare column reference inside a projection expression
	// decides its type from them (see nodeDeclaredType, #333). The second
	// map is for a SELECT expression that maps to a synthetic group column —
	// a rename of a value computed BELOW the aggregate, so it types against
	// the aggregate's input rather than its output.
	childColTypes := emittedColDecls(child)
	// A SELECT-list scalar subquery types against its OWN plan, not against
	// this projection's input columns (#874).
	childColTypes.subqueryDecl = p.subqueryOutputColumn
	var aggInputColTypes colDecls
	if isOverAggregate && len(aggNode.Children) > 0 {
		aggInputColTypes = inputColDecls(aggNode.Children[0])
		aggInputColTypes.subqueryDecl = p.subqueryOutputColumn

	}

	// When the aggregate below emits two output columns of one NAME, a
	// name-based DirectCopy resolves both projections to the FIRST such
	// column and collapses them to one value (#575). The fix pins each such
	// projection to the physical slot its TRUE PROVENANCE names, so
	// appearance order is never assumed to equal slot order — it does not
	// when an aggregate shares its alias with a group key (`SELECT COUNT(*)
	// AS k, k AS x GROUP BY k`) or when the select list orders aggregates
	// and keys differently from the aggregate's [keys…, aggs…] output.
	//
	// keySlotByName / aggSlotByName carry the ABSOLUTE indices of the
	// duplicated names in the child's output, split by class: a group-key
	// projection consumes key slots, an aggregate projection consumes
	// aggregate slots. Only built when the child is a clean
	// [group keys…, aggregates…] aggregate output (no grouping sets, no
	// elided-literal reordering); anything else leaves every reference on
	// the existing name path.
	keySlotByName := map[string][]int{}
	aggSlotByName := map[string][]int{}
	if isOverAggregate && aggNode != nil {
		// The EMITTED names, not the planner's spelling of them: the whole
		// point of this map is to find a name TWO columns of the operator's
		// output batch answer to, and a key the planner spells `x.a` is a
		// column the operator calls `a` (#968).
		if full, ok := aggregateEmittedOutputNames(child); ok {
			nAgg := len(aggNode.AggExprs)
			nKey := len(full) - nAgg
			clean := nKey >= 0
			for j := 0; clean && j < nAgg; j++ {
				if !strings.EqualFold(strings.TrimSpace(full[nKey+j]),
					strings.TrimSpace(aggNode.AggExprs[j].OutputCol)) {
					clean = false
				}
			}
			if clean {
				total := map[string]int{}
				for _, n := range full {
					total[strings.ToLower(strings.TrimSpace(n))]++
				}
				for i, n := range full {
					key := strings.ToLower(strings.TrimSpace(n))
					if total[key] < 2 {
						continue // unambiguous by name; leave it on the name path
					}
					if i < nKey {
						keySlotByName[key] = append(keySlotByName[key], i)
					} else {
						aggSlotByName[key] = append(aggSlotByName[key], i)
					}
				}
			}
		}
	}
	keySlotSeen := map[string]int{}
	aggSlotSeen := map[string]int{}

	var projCols []exec.ProjectColumn
	for _, proj := range node.Projections {
		colRef := proj.Column
		if colRef == "" {
			colRef = cleanExpr(proj.Expr)
		}
		name := proj.Alias
		if name == "" {
			name = colRef // use unqualified column name
		}
		// When projecting over an aggregate, aggregate columns should reference
		// their output column name (the alias), not the raw expression.
		if isOverAggregate && proj.IsAgg && proj.Alias != "" {
			colRef = proj.Alias
		}

		// Try to compile from AST expression first, fall back to ColumnRef
		var expression exec.Expression

		// When projecting over an aggregate, check if this SELECT expression
		// matches a GROUP BY expression that was pre-computed into a synthetic
		// column. If so, use a ColumnRef to the synthetic column instead of
		// re-evaluating the expression (the original columns are gone).
		var synSource string
		if isOverAggregate && proj.ASTExpr != nil && !proj.IsAgg {
			if synName, ok := gbExprToSyn[plansql.ExprIdentity(proj.ASTExpr)]; ok {
				expression = exec.ColumnRef(synName)
				synSource = synName
			}
		}

		// Handle aggregate projections with wrapping scalar functions,
		// e.g., format_bytes(SUM(rx_bytes)). Replace the inner aggregate
		// AST node with a ColRef to the aggregate output column, then
		// compile the modified AST as a scalar expression.
		var compiledExpr expr.Expr
		if expression == nil && proj.IsAgg && proj.ASTExpr != nil && isOverAggregate {
			innerAgg := plansql.FindNestedAggregate(proj.ASTExpr)
			if innerAgg != nil {
				outerFn, isFunc := proj.ASTExpr.(*plansql.FuncCallNode)
				if isFunc && !plansql.IsAggregate(outerFn.Name) {
					// Build the aggregate output column name
					aggOutputCol := strings.ToLower(innerAgg.Name) + "("
					if innerAgg.Distinct {
						aggOutputCol += "distinct "
					}
					if innerAgg.Star {
						aggOutputCol += "*"
					} else if len(innerAgg.Args) > 0 {
						var argStrs []string
						for _, a := range innerAgg.Args {
							argStrs = append(argStrs, a.String())
						}
						aggOutputCol += strings.Join(argStrs, ", ")
					}
					aggOutputCol += ")"
					// Replace inner aggregate with a column reference in the AST
					rewritten := replaceAggWithColRef(proj.ASTExpr, innerAgg, aggOutputCol)
					compiled, compErr := expr.CompileWithRunner(rewritten, p.subqueryRunner, p.subqueryBudgetOption())
					if expr.IsCompileRefusal(compErr) {
						return nil, nil, nil, compErr
					}
					if compErr == nil {
						expression = wrapExpr(compiled)
						compiledExpr = compiled
					}
				}
			}
		}

		// An expression OVER a group key — `(g + 1) * 2` — is not the key, so
		// nothing above resolves it as one, and the aggregate's output does
		// not carry `g` to rebuild it from. Re-point its group-key SUBTERMS
		// at the columns the aggregate publishes, which is what the DAG's
		// requoteAggOutputRefs does for the same shape; without it the whole
		// item evaluated to NULL for every row (#723).
		astExpr := proj.ASTExpr
		if isOverAggregate && astExpr != nil && !proj.IsAgg {
			astExpr = plansql.ReplaceGroupKeyRefs(astExpr, gbExprToSyn)
		}

		if expression == nil && astExpr != nil && !proj.IsAgg {
			// CSE within a single Project operator is unsafe: prevCol below
			// is the OUTPUT column name of an earlier projection, but at
			// runtime each ColumnRef is resolved against the INPUT batch's
			// schema — which doesn't yet have the earlier output column.
			// Pointing the duplicate at prevCol resolves to NULL at every
			// row (e.g. `SELECT 1 AS n, 0 AS a, 1 AS b` produced
			// {n: 1, a: 0, b: NULL} because the second `1` literal mapped
			// to ColumnRef("n") and n wasn't in the input — regression
			// surfaced by TestRecursiveCTE_Fibonacci).
			//
			// Safe CSE for SELECT-list duplicates would require either
			// (a) materialising shared expressions as a synthetic column
			// the Project then references, or (b) compiling each
			// projection independently. (b) is what we do — recompiling
			// a literal or already-compiled expression is cheap.
			outerTables := collectTableAliases(child)
			outerCols := collectOuterColumns(child)
			var compiled expr.Expr
			var compErr error
			if len(outerTables) > 0 {
				if len(outerCols) > 0 {
					compiled, compErr = expr.CompileWithScopeResolver(astExpr, p.subqueryRunner, outerTables, outerCols, p.subqueryInnerColumns(), p.subqueryDeclOption(), p.subqueryBudgetOption())
				} else {
					compiled, compErr = expr.CompileWithScope(astExpr, p.subqueryRunner, outerTables, p.subqueryDeclOption(), p.subqueryBudgetOption())
				}
			} else {
				// With the child's DECLARED column types in hand, so a pair
				// that cannot be exact fixed-point — a FLOAT column against a
				// fractional literal — keeps the vectorized float node it has
				// always compiled to instead of deferring the question to the
				// first batch (#555 review).
				compiled, compErr = expr.CompileWithColumnTypes(
					astExpr, p.subqueryRunner, childColTypes.types, p.subqueryBudgetOption())
			}
			// A name nothing implements has no input column to fall back to,
			// so the direct-copy path below would only re-report it as a
			// missing column. Propagate instead (#341).
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				expression = wrapExpr(compiled)
				compiledExpr = compiled
			}
		}
		isDirectCopy := expression == nil
		if expression == nil {
			expression = exec.ColumnRef(colRef)
		}

		// Infer output type: TypeString is the default, resolved at runtime from
		// input schema when column names match. For arithmetic expressions that
		// won't match an input column (e.g., nested aggregate rewrites like
		// __agg_0 * 0.0001), use TypeFloat64.
		outDecl := expr.Decl(parquet.TypeString)
		if proj.ASTExpr != nil && !proj.IsAgg {
			// A select expression mapped to a synthetic group column is a
			// RENAME of a value computed BELOW the aggregate — type it
			// against the aggregate's input, or the declared Float64
			// coerces the pre-projected int64 keys on the copy (#297).
			strictInt := strictIntArithCols(child)
			colTypes := childColTypes
			// The RESPELLED expression is the one that gets evaluated, so it
			// is the one to type. `(c_dec + 1) * 2` over `GROUP BY c_dec + 1`
			// names `c_dec` — a column the aggregate's output does not carry
			// — so typing the original left the DECIMAL key unresolved and
			// the whole term fell to the float rule, which is a scale-0
			// vector over exact fixed point (ADR-0024 item 2). Typing the
			// respelled form reads the key's own declared type instead.
			typeExpr := astExpr
			if isOverAggregate {
				if _, ok := gbExprToSyn[plansql.ExprIdentity(proj.ASTExpr)]; ok {
					// The WHOLE item is a key: a rename of a value computed
					// BELOW the aggregate, so it types against the
					// aggregate's input or the declared Float64 coerces the
					// pre-projected int64 keys on the copy (#297).
					strictInt = strictIntArithCols(aggNode.Children[0])
					colTypes = aggInputColTypes
					typeExpr = proj.ASTExpr
				}
			}
			outDecl = inferProjectionDeclType(typeExpr, outDecl.ID, strictInt, colTypes)
		}
		outType := outDecl.ID
		// The planner's declaration is the AUTHORITY for this projection's
		// arithmetic mode, and the compiled tree is told it here rather than
		// deriving its own. Two walks over two representations of one
		// expression is how a float came to be computed under an INT64
		// declaration and TRUNCATED into the vector (round-1 review, B3);
		// expr.StampArithMode is the seam that makes it one decision.
		if compiledExpr != nil {
			expr.StampArithMode(compiledExpr, outType == parquet.TypeInt64)
		}

		pc := exec.ProjectColumn{
			Name: name,
			Type: outType, // Will be resolved at runtime if input column matches
			Expr: expression,
			// A computed DECIMAL's (p,s): the output column exists in no
			// input schema, so exec.Project has nothing to read the scale
			// off and a scale-0 vector reads every value back a hundredfold
			// out (ADR-0024 item 2; #529, #555).
			Precision: outDecl.Precision,
			Scale:     outDecl.Scale,
		}
		// VECTOR-returning functions (embed()) need their output dimension
		// carried so the runtime sizes the output vector. Resolve it from the
		// registry at plan time (embed() derives it from the live provider).
		if outType == parquet.TypeVector {
			if fc, ok := proj.ASTExpr.(*plansql.FuncCallNode); ok {
				if dim, ok := expr.DefaultRegistry.VecReturnDim(fc.Name); ok {
					pc.Dimension = dim
				}
			}
		}
		// For column renames (e.g., l_suppkey AS supplier_no), record the
		// source column so Project.Execute can resolve the correct type.
		if name != colRef {
			pc.SourceCol = colRef
		}
		// A QUALIFIED reference names ONE SIDE, and the DECLARATION has to be
		// read off the column the VALUE comes from.
		//
		// `proj.Column` is the BARE name — the parser records `z.d92` as the
		// column `d92` — so where both join sides carry that name the value
		// was resolved through `z.d92` (the compiled expression keeps the
		// qualifier) and the type through the first bare `d92`, which is the
		// OTHER arm's. Over two tables holding `d92` at (9,2) and (18,4) that
		// rendered one arm's digits at the other arm's scale, silently, and
		// raised 22003 in the direction where the value does not fit —
		// `numeric field overflow: 1.1111 does not fit a DECIMAL at scale 2`
		// on a query PostgreSQL answers (#706).
		//
		// This is strictly more precise rather than a different rule:
		// `columnIndexFallback` resolves a qualified name with the same
		// ladder the value path uses — exact, then bare, then the
		// unambiguous suffix — so a stream that carries only the bare name
		// still resolves. Per-side resolution is what #551 gave set-op arms
		// and #653 gave filters; this is the projection's half of it.
		if cr, isRef := bareColRefOf(proj.ASTExpr); isRef && cr.Table != "" && !proj.IsAgg {
			pc.SourceCol = cr.Table + "." + cr.Column
		}
		// A ROW FIELD PATH records the WHOLE path, qualifier included, even
		// when the output name matches the field name. It is the only
		// spelling exec.Project can resolve the field's declaration from —
		// colRef here has already lost the `rw.` through cleanExpr — and it
		// is what carries the shape a bare TypeID cannot: a DECIMAL field's
		// (p,s) and a nested ROW/ARRAY/MAP field's own structure, which
		// colRefDeclaredType declines for the same reason it declines them
		// for a column (#568).
		if fp, ok := fieldPathRef(proj.ASTExpr, childColTypes); ok {
			pc.SourceCol = fp
		}
		// A SELECT item that maps to a synthetic GROUP BY key column reads
		// that column, so it is the source exec.Project must type from. The
		// planner's own declaration cannot carry a parameterized type
		// (colRefDeclaredType declines DECIMAL and the containers), which
		// left `SELECT rw.d ... GROUP BY rw.d` declaring STRING over a
		// DECIMAL key the aggregate had already emitted correctly (#568).
		if synSource != "" {
			pc.SourceCol = synSource
		}
		// Tell the runtime this output is computed, so it does not type the
		// output vector from an input column that merely shares the alias
		// (#327). A bare column reference — the only projection whose value
		// really does come from a same-named input — is excluded.
		pc.Computed = isComputedProjection(proj.ASTExpr)
		// For simple column references (no computed expression), use bulk vector
		// copy instead of per-row evaluation.
		if isDirectCopy {
			pc.DirectCopy = colRef
		}
		// Use vectorized column evaluation when the expression supports it.
		// VecExpr handles any output type (string, numeric, etc.) and is checked
		// before the Float64-specific paths.
		if compiledExpr != nil {
			if ve, ok := compiledExpr.(expr.VecExpr); ok {
				evalVec := ve.EvalVec
				pc.VecEval = func(b *batch.RecordBatch, out *batch.Vector, n int) {
					evalVec(b, out, n)
				}
			}
			// Exact fixed-point arithmetic into a DECIMAL output: the one
			// kernel that writes DecimalData, so it is the one projection
			// that may skip exec.Project's checked per-row box (ADR-0024
			// item 3, #555). Gated on the DECLARED type, because a node whose
			// runtime mode turns out not to be decimal writes nothing.
			if outType == parquet.TypeDecimal {
				if dv, ok := compiledExpr.(expr.DecimalVecExpr); ok {
					pc.VecDecimalEval = dv.EvalDecimalVec
				}
			}
		}
		// Use typed evaluation to avoid interface{} boxing in the inner loop.
		// Only safe when the output type is explicitly Float64 (arithmetic exprs),
		// not when resolved from input schema (could be Decimal, Timestamp, etc.).
		if compiledExpr != nil && outType == parquet.TypeFloat64 {
			if ve, ok := compiledExpr.(expr.VecFloat64Expr); ok {
				pc.VecFloat64Eval = ve.EvalFloat64Vec
				if binop, ok := ve.(*expr.BinOpFloat64); ok {
					pc.VecFloat64Clone = func() exec.VecFloat64Expression {
						return binop.CloneVec().EvalFloat64Vec
					}
				}
			}
			if fe, ok := compiledExpr.(expr.Float64Expr); ok {
				pc.Float64Eval = fe.EvalFloat64
			} else if ie, ok := compiledExpr.(expr.Int64Expr); ok {
				pc.Int64Eval = ie.EvalInt64
			}
		}
		// A plain direct copy of a DUPLICATED output column: pin it to the
		// next physical slot of its PROVENANCE class — aggregate projections
		// take aggregate slots, group-key references take key slots — so two
		// projections reading `u` read the two distinct `u` columns and an
		// aggregate never reads the group-key column it happens to share a
		// name with (#575). SourceIdx is exact and beats the name path.
		if isDirectCopy {
			key := strings.ToLower(strings.TrimSpace(colRef))
			seen, slots := keySlotSeen, keySlotByName
			if proj.IsAgg {
				seen, slots = aggSlotSeen, aggSlotByName
			}
			if idxs := slots[key]; len(idxs) > 0 {
				if k := seen[key]; k < len(idxs) {
					pc.SourceIdx = idxs[k]
					pc.SourceIdxSet = true
					seen[key] = k + 1
				}
			}
		}
		projCols = append(projCols, pc)
	}

	if len(projCols) > 0 {
		ops = append(ops, exec.NewProject(projCols))
	}

	return source, ops, sink, nil
}
