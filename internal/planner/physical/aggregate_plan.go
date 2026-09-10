// This file holds aggregate plan for the physical planner, governed by ADR-0026 and ADR-0027.
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
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func (p *Planner) buildAggregate(ctx context.Context, node *logical.Node) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	if len(node.Children) == 0 {
		return nil, nil, nil, fmt.Errorf("aggregate has no child")
	}

	// Bare COUNT(*) over a plain scan answers from the catalog manifest —
	// no scan pipeline at all (see metadata_count.go). Un-grouped MIN/MAX
	// (optionally alongside COUNT(*)) answers the same way from parquet
	// footer statistics (see metadata_minmax.go).
	if src, ok := p.tryBuildMetadataCount(ctx, node); ok {
		return src, nil, &exec.CollectSink{}, nil
	}
	if src, ok := p.tryBuildMetadataMinMax(ctx, node); ok {
		return src, nil, &exec.CollectSink{}, nil
	}

	childSource, childOps, _, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}

	// Detect aggregate inputs that are expressions (not simple column refs).
	// For each, compile the expression and add a pre-aggregate projection
	// that evaluates it into a synthetic column.
	// CSE: deduplicate identical expressions by their string representation.
	var preProjectCols []exec.ProjectColumn
	var preProjectMeta []parquet.Column
	syntheticNames := make(map[int]string) // agg index → synthetic column name
	exprDedup := make(map[string]string)   // expr string → synthetic column name
	// synDecl is the DECLARATION each synthetic column is materialized under —
	// the same triple the stage DAG carries as AggSpec.InputType/Precision/
	// Scale. The aggregate's OUTPUT declaration is read off it below, so this
	// path and the DAG declare the same thing for the same query, identity row
	// included (#685; see aggOutputFromInputDecl).
	synDecl := make(map[string]parquet.Column)

	// The declarations of what feeds the aggregate, resolved once: they are
	// what tells a ROW FIELD PATH apart from a table-qualified column, and a
	// field path is NOT a simple column reference however much it looks like
	// one. exec.HashAggregate resolves its inputs by NAME through
	// columnIndexFallback, which has no ROW arm, so `MIN(rw.n)` failed with
	// `aggregate input "n" is not a column of its input` — cleanExpr having
	// dropped the `rw.` on the way. Routing it through the synthetic
	// pre-projection below is what materializes the field as a real column,
	// at its declared type (#568).
	//
	// emittedColDecls, not inputColDecls: the walk has to cross a DERIVED
	// TABLE. TPC-H Q08 is `SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0
	// END)` over `(SELECT …, l_extendedprice * (1 - l_discount) AS volume …)`,
	// and inputColTypes stops at that subquery's Project — so `volume`
	// decided nothing, the CASE declared its integer ELSE, and the branch's
	// DECIMAL text met an INT64 vector at the #361 store guard. It is the
	// same decline #529 hit one site over, where the SELECT list already
	// resolves through emittedColDecls (see TestDecimalDecidesThroughParens
	// AndDerivedTables), and the same walk declaredOutputSchema uses — so the
	// aggregate's input, the SELECT list and the plan-declared schema now
	// answer from one map.
	aggInputDecls := emittedColDecls(node.Children[0])

	for i, agg := range node.AggExprs {
		if agg.InputExpr != nil && (!isSimpleColRef(agg.InputExpr) || astIsFieldPath(agg.InputExpr, aggInputDecls)) {
			exprStr := agg.InputExpr.String()
			if existing, ok := exprDedup[exprStr]; ok {
				// Reuse previously compiled expression
				syntheticNames[i] = existing
				continue
			}
			synName := SlotName(SlotAggInput, i)
			// WITH THE OUTER SCOPE, exactly as the SELECT-list projection
			// site compiles its own expressions (see the CompileWith*
			// ladder above). Without it this site asked for none, so a
			// correlated subquery in an AGGREGATE ARGUMENT was compiled as
			// UNCORRELATED and run ONCE against no outer row: `SUM(CASE WHEN
			// EXISTS (SELECT 1 FROM y WHERE y.id = x.id * 2) THEN 1 ELSE 0
			// END)` read a query-wide constant FALSE and answered 0 for
			// PostgreSQL's 4, in silence, until v0.18.16 made the dangling
			// re-run loud (#734, ADR-0021 §1c). The identical expression one
			// level down — in a derived table's SELECT list — has always
			// answered, because that site does ask.
			aggOuterTables := collectTableAliases(node.Children[0])
			aggOuterCols := collectOuterColumns(node.Children[0])
			var compiled expr.Expr
			var compErr error
			if len(aggOuterTables) > 0 {
				compiled, compErr = expr.CompileWithScopeResolver(agg.InputExpr, p.subqueryRunner,
					aggOuterTables, aggOuterCols, p.subqueryInnerColumns(),
					p.subqueryDeclOption(), p.subqueryBudgetOption())
			} else {
				compiled, compErr = expr.CompileWithRunner(agg.InputExpr, p.subqueryRunner,
					p.subqueryDeclOption(), p.subqueryBudgetOption())
			}
			if expr.IsCompileRefusal(compErr) {
				return nil, nil, nil, compErr
			}
			if compErr == nil {
				aggDecl := inferProjectionDeclType(agg.InputExpr, parquet.TypeFloat64, nil, aggInputDecls)
				pc := exec.ProjectColumn{
					Name: synName,
					// Aggregate inputs are usually numeric, so Float64 is the
					// fallback — but MAX(UPPER(c)) is not, and declaring it
					// Float64 handed vecUpper a vector with no BytesData to
					// write into: the same process-killing mismatch as the
					// projection path (#310). MAX(COALESCE(a, b)) needs the
					// input's column types on top of that, or the polymorphic
					// declaration falls back to the same wrong Float64 (#333),
					// and a DECIMAL needs its (p,s) with the TypeID or the
					// materialized vector truncates at scale 0 (ADR-0024
					// item 2).
					Type:      aggDecl.ID,
					Precision: aggDecl.Precision,
					Scale:     aggDecl.Scale,
					Expr:      wrapExpr(compiled),
				}
				// Use general vectorized evaluation when available.
				if ve, ok := compiled.(expr.VecExpr); ok {
					evalVec := ve.EvalVec
					pc.VecEval = func(b *batch.RecordBatch, out *batch.Vector, n int) {
						evalVec(b, out, n)
					}
				}
				// Use vectorized float64 evaluation when available (entire column at once),
				// falling back to typed per-row eval.
				//
				// Gated on the DECLARED type, the way the SELECT-list
				// projection gates its own typed paths (buildProjectOp only
				// attaches them when outType is Float64). aggPreProject picks
				// its write route from WHICH eval is set, not from the
				// column, so a float writer on a non-float column writes into
				// a nil Float64Data — reached the moment a ROW field path
				// started taking this route, since a bare column reference
				// implements every one of these interfaces (#568).
				//
				// The gate applies to EVERY derived aggregate input, not only
				// to field paths, and that is deliberate: the mismatch it
				// prevents was always possible here — any expression whose
				// declared type is not Float64 while its compiled form
				// implements VecFloat64Expr had the same nil-slice write
				// available to it — and the two sites now state the same
				// rule. Nothing that was reaching a typed path with a
				// matching declared type loses it.
				if pc.Type == parquet.TypeFloat64 {
					if ve, ok := compiled.(expr.VecFloat64Expr); ok {
						pc.VecFloat64Eval = ve.EvalFloat64Vec
						if binop, ok := ve.(*expr.BinOpFloat64); ok {
							pc.VecFloat64Clone = func() exec.VecFloat64Expression {
								return binop.CloneVec().EvalFloat64Vec
							}
						}
					}
					if fe, ok := compiled.(expr.Float64Expr); ok {
						pc.Float64Eval = fe.EvalFloat64
					}
				}
				if pc.Type == parquet.TypeInt64 {
					if ie, ok := compiled.(expr.Int64Expr); ok {
						pc.Int64Eval = ie.EvalInt64
					}
				}
				// Exact fixed-point arithmetic into a DECIMAL aggregate
				// input, gated on the DECLARED type exactly like the paths
				// above and like the SELECT-list projection builder.
				//
				// The kernel existed and was reachable — BinOpNumeric
				// implements DecimalVecExpr and writes carriers straight
				// into out.DecimalData.Data — and it was attached in
				// exactly ONE place in the tree, the SELECT-list builder.
				// Every DECIMAL aggregate input therefore took the boxed
				// checked writer: 4.00 allocations per computed cell, at
				// SF1 48,026,572 for Q01's two computed columns over
				// 6,001,215 rows, 1804x the FLOAT64 arm's object count.
				// The mechanism is one round trip — Int128 →
				// FormatDecimal → FormatUint → any box →
				// SetComputedChecked → DecimalTextParts re-parse — so the
				// value is rendered to decimal TEXT and parsed back
				// between two exact kernels (#705). The BYTE ratio is a
				// different thing and is NOT a defect: the Int128 carrier
				// is 16 B against float64's 8, which is ADR-0024's
				// predicted cost.
				if pc.Type == parquet.TypeDecimal {
					if dv, ok := compiled.(expr.DecimalVecExpr); ok {
						pc.VecDecimalEval = dv.EvalDecimalVec
					}
				}
				// A ROW FIELD PATH declares the FIELD, wholesale: its (p,s),
				// its dimension, its nested shape. colRefDeclaredType above
				// declines every parameterized type — it can only answer a
				// TypeID — so MIN over a DECIMAL field fell back to the
				// Float64 default and a container field to Float64 outright
				// (#568).
				meta := parquet.Column{Name: synName, Type: pc.Type, Nullable: true,
					Precision: pc.Precision, Scale: pc.Scale}
				if fc, ok := aggInputDecls.field(fieldPathColRef(agg.InputExpr)); ok {
					meta = fc
					meta.Name, meta.Nullable = synName, true
					pc.Type = fc.Type
					pc.Dimension = fc.Dimension
					// The boxed route is the only null-correct one for a
					// field path: aggPreProject picks its writer from WHICH
					// eval is set, and the typed writers have no way to mark
					// a NULL field — MIN over a float field read the 0 they
					// leave behind instead of skipping the row.
					pc.VecEval, pc.VecFloat64Eval, pc.VecFloat64Clone = nil, nil, nil
					pc.Float64Eval, pc.Int64Eval = nil, nil
				}
				preProjectCols = append(preProjectCols, pc)
				preProjectMeta = append(preProjectMeta, meta)
				syntheticNames[i] = synName
				exprDedup[exprStr] = synName
				synDecl[synName] = meta
			}
		}
	}

	var aggCols []exec.AggColumn
	for i, agg := range node.AggExprs {
		fn := parseAggFunc(agg.Func)
		if agg.Distinct && fn == exec.AggCount {
			fn = exec.AggCountDistinct
		}
		// Preserve the table qualifier — NormalizeIdentRef only strips the
		// quotes off a delimited identifier, the way the GROUP BY keys below
		// are normalized and the way the stage-DAG AggSpec carries
		// agg.InputCol verbatim. cleanExpr here would drop the qualifier
		// ("t2.c2" -> "c2"), and a bare name binds to the FIRST column of
		// that name in the input schema. Over a join whose two sides share a
		// bare column name, that is the wrong table's column: BOOL_OR(t2.c2)
		// over `t5, t1 FULL OUTER JOIN t2 ON t1.c7` read t5.c2 (all NULL) and
		// answered NULL, and an always-true WHERE that reorders the cross
		// join flipped which wrong column it read (t1.c2, FALSE) — a
		// TLP-Aggregate self-consistency violation (#622). exec.HashAggregate's
		// columnIndexFallback still resolves the qualified name against a
		// scan that emits the column bare via its qualified->bare fallback.
		inputCol := plansql.NormalizeIdentRef(strings.TrimSpace(agg.InputCol))
		// Use synthetic column name if the input was an expression
		if synName, ok := syntheticNames[i]; ok {
			inputCol = synName
		}
		// Prefer the resolved declaration (MIN/MAX carry their input
		// column's type) so this pipeline and the stage DAG declare the
		// same thing for the same query. exec.HashAggregate overrides
		// MIN/MAX from the vector it observes anyway, so this only decides
		// the type of the identity row an empty input produces — which is
		// exactly where the two paths would otherwise disagree.
		outType, outTypeKnown := aggSpecOutputType(node, agg)
		if !outTypeKnown {
			outType = aggOutputType(agg.Func, agg.Distinct)
		}
		// The arguments past the first arrive as their own fields
		// (logical/agg_extra_args.go). They used to be recovered by
		// splitting InputCol on a comma, which only worked if something
		// had packed them there — nothing had, because the SELECT parser
		// dropped every argument after the first (#353).
		ac := exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			InputCol2:  agg.InputCol2,
			InputCol3:  agg.InputCol3,
			Separator:  agg.Separator,
			Percentile: agg.Percentile,
			OutputCol:  agg.OutputCol,
			OutputType: outType,
			// DISTINCT for every aggregate PostgreSQL accepts it for, not
			// only COUNT (#703). COUNT spells it as its own AggFunc above —
			// its state IS the set — so the flag would be redundant there;
			// MIN/MAX are unaffected by de-duplication and carry it only so
			// the two paths declare the same spec.
			Distinct: agg.Distinct && fn != exec.AggCountDistinct,
		}
		// And the (p,s) a bare TypeID cannot carry, for the same reason: it
		// decides what the identity row of an EMPTY input declares, which is
		// the one output no observed vector can type. On the DAG that row is
		// a whole partial task's .wshf file (#685); here it is the zero-row
		// result's schema, and the two paths declare the same thing only if
		// both read this function.
		if m, known := aggSpecOutputDecimal(node, agg); known {
			ac.OutputPrecision, ac.OutputScale = m.Precision, m.Scale
		}
		// A ROW-valued aggregate's FIELDS, which a bare TypeID cannot carry
		// either. Same function the stage spec uses, so the two paths declare
		// one bar (#965).
		if fields, ok := aggOhlcvOutputFields(node, agg); ok {
			ac.OutputFields = fields
		}
		// A COMPUTED argument is declared from the projection this path
		// materializes it under, which is the DAG's rule read off the local
		// equivalent of AggSpec.InputType/Precision/Scale. Without it a
		// zero-row SUM(a * (1 - b)) declared float64 here and DECIMAL there.
		if synName, ok := syntheticNames[i]; ok {
			if d, ok := synDecl[synName]; ok {
				if t, prec, sc, known := aggOutputFromInputDecl(
					agg.Func, agg.Distinct, d.Type, d.Precision, d.Scale,
					aggInputIsWideInteger(agg.InputExpr, aggInputDecls)); known {
					ac.OutputType = t
					ac.OutputPrecision, ac.OutputScale = prec, sc
				}
			}
		}
		aggCols = append(aggCols, ac)
	}

	// Catalog types of the aggregate's input, for typing derived GROUP BY
	// key expressions (see nodeDeclaredType, #333). Resolved once.
	aggChildStrictInt := strictIntArithCols(node.Children[0])
	aggChildColTypes := inputColDecls(node.Children[0])

	groupByCols := make([]string, len(node.GroupBy))
	for i, gb := range node.GroupBy {
		// Preserve table qualifiers for self-join disambiguation (e.g., n1.n_name vs n2.n_name).
		// The aggregate operator resolves qualified names with fallback to unqualified.
		// Delimited identifiers lose their quotes here: the operator matches
		// the batch column name itself (Zeek's flat id.orig_h).
		groupByCols[i] = plansql.NormalizeIdentRef(strings.TrimSpace(gb))
	}

	// Literal group keys (GROUP BY 1, URL — the positional ref resolves to
	// the literal select item) are constant per row: they cannot affect
	// grouping, but as synthetic key columns they widen every serialized
	// key and force the multi-column generic path over the single-column
	// fast paths (ClickBench Q35 vs Q34). Elide them from the key set and
	// re-attach the constant as a post-aggregate column under the same
	// synthetic name the downstream projection expects. Kept out of
	// grouping-sets plans (set indices reference key positions), and only
	// when a non-literal key remains — GROUP BY over literals alone must
	// still emit zero rows on empty input, which one retained key
	// preserves.
	var litPostOps []exec.UnaryOperator
	litElided := map[int]bool{}
	// The names this aggregate publishes its keys under, resolved once. The
	// literal elision below, the derived-key materialization further down,
	// aggregateOutputNames and the projection above all read them from here —
	// one rule, so the two engines' aggregate output schemas cannot drift
	// apart (#723).
	keyOuts := groupKeyOutputs(node)
	if len(node.GroupByExprs) == len(node.GroupBy) && len(node.GroupingSets) == 0 {
		nonLit := 0
		for _, gbExpr := range node.GroupByExprs {
			if gbExpr == nil {
				nonLit++
				continue
			}
			if _, isLit := gbExpr.(*plansql.Lit); !isLit {
				nonLit++
			}
		}
		if nonLit > 0 {
			for i, gbExpr := range node.GroupByExprs {
				if gbExpr == nil {
					continue
				}
				if _, isLit := gbExpr.(*plansql.Lit); !isLit {
					continue
				}
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr != nil {
					continue
				}
				litDecl := inferProjectionDeclType(gbExpr, parquet.TypeString, aggChildStrictInt, aggChildColTypes)
				litPostOps = append(litPostOps, &aggPreProject{computed: []exec.ProjectColumn{{
					Name:      keyOuts[i].Name,
					Type:      litDecl.ID,
					Precision: litDecl.Precision,
					Scale:     litDecl.Scale,
					Expr:      wrapExpr(compiled),
				}}})
				litElided[i] = true
			}
		}
	}

	// Handle GROUP BY expressions (e.g., SUBSTR(c_phone, 1, 2)).
	// Compile expression-valued GROUP BY entries into pre-aggregate projections
	// so the aggregate can group by the computed result.
	if len(node.GroupByExprs) == len(node.GroupBy) {
		for i, gbExpr := range node.GroupByExprs {
			if litElided[i] {
				continue
			}
			// keyOuts is the single answer to "is this key derived", shared
			// with aggregateOutputNames and with the projection above, so
			// the schema this materialization produces and the schema they
			// describe cannot disagree (ADR-0026).
			if gbExpr != nil && i < len(keyOuts) && keyOuts[i].Derived {
				// The HIDDEN SLOT, not the key's own text. The
				// pre-aggregate projection APPENDS this column to the input
				// batch and every consumer resolves by name, so a slot
				// spelled like an input column the query already carries is
				// shadowed BY it — `GROUP BY g + 1` over a table that also
				// has a column called "g + 1" grouped by the column. The
				// key is PUBLISHED under its canonical text by
				// GroupByOutNames below, which is what keeps the two
				// engines' output schemas equal (#720, ADR-0026).
				synName := keyOuts[i].Slot
				compiled, compErr := expr.CompileWithRunner(gbExpr, p.subqueryRunner, p.subqueryDeclOption(), p.subqueryBudgetOption())
				if expr.IsCompileRefusal(compErr) {
					return nil, nil, nil, compErr
				}
				if compErr == nil {
					gbDecl := derivedGroupKeyDecl(node.GroupBy[i], gbExpr, node.Children[0])
					pc := exec.ProjectColumn{
						Name: synName,
						// Numeric expressions (abs(x), x-1, …) must get a
						// numeric synthetic column: SetValue on a String
						// vector mangles float group keys.
						Type: gbDecl.ID,
						// And a DECIMAL key needs its (p,s) with the type:
						// a scale-0 vector TRUNCATES every value on the way
						// in, so `GROUP BY COALESCE(a, b)` collapsed 12.75
						// and 12.7501 into one group holding 12 (ADR-0024
						// item 2).
						Precision: gbDecl.Precision,
						Scale:     gbDecl.Scale,
						Expr:      wrapExpr(compiled),
					}
					// Batched evaluation when available — beyond the vec
					// kernels themselves, FuncCall.EvalVec is where the
					// per-batch input memo for expensive scalar functions
					// (regexp family, ClickBench Q29's GROUP BY key) lives;
					// the per-row Expr path bypasses it.
					if ve, ok := compiled.(expr.VecExpr); ok {
						pc.VecEval = ve.EvalVec
					}
					// Same rule the aggregate INPUT takes above: a ROW field
					// path declares the whole field, and its value is
					// written through the boxed route so a NULL field stays
					// NULL (#568).
					meta := parquet.Column{Name: synName, Type: pc.Type, Nullable: true,
						Precision: pc.Precision, Scale: pc.Scale}
					if fc, ok := aggChildColTypes.field(fieldPathColRef(gbExpr)); ok {
						meta = fc
						meta.Name, meta.Nullable = synName, true
						pc.Type = fc.Type
						pc.Dimension = fc.Dimension
						pc.VecEval = nil
					}
					preProjectCols = append(preProjectCols, pc)
					preProjectMeta = append(preProjectMeta, meta)
					groupByCols[i] = synName
				}
			}
		}
	}

	// If we have expression inputs or GROUP BY expressions, build a
	// pass-through projection that keeps all input columns and adds
	// the computed ones.
	if len(preProjectCols) > 0 {
		childOps = append(childOps, &aggPreProject{computed: preProjectCols, meta: preProjectMeta})
	}

	// Compact literal-elided entries out of the key set.
	if len(litElided) > 0 {
		kept := groupByCols[:0:0]
		for i, c := range groupByCols {
			if !litElided[i] {
				kept = append(kept, c)
			}
		}
		groupByCols = kept
	}

	hashAgg := exec.NewHashAggregate(groupByCols, aggCols)
	// A DERIVED key resolves by its hidden slot and publishes under its own
	// canonical text. Only then do the two engines' aggregate output schemas
	// match, which is what lets one HAVING predicate — rewritten once, in the
	// logical plan — be evaluable on both (ADR-0026). Set only when a key
	// really is derived, so every other shape keeps exec's own naming rule
	// (the qualifier strip, and the ambiguity exception for `GROUP BY
	// n1.n_name, n2.n_name`).
	if outNames, derived := publishedGroupKeyNames(keyOuts, litElided); derived {
		hashAgg.GroupByOutNames = outNames
	}
	if est := findScanRowEstimate(node.Children[0]); est > 0 {
		hashAgg.InputRowHint = est
	}
	if ndv := groupKeyNDVEstimate(node.Children[0], groupByCols); ndv > 0 {
		hashAgg.GroupNDVHint = ndv
	}
	if sm := p.getSpillManager(); sm != nil {
		hashAgg.Spill = sm
	}

	// For GROUPING SETS: single-pass mode — convert the sets' terms to key
	// POSITIONS.
	//
	// A term is looked up against `node.GroupBy`, the key list as the query
	// wrote it, and NOT against `groupByCols`, which is what the aggregate
	// actually groups on: a DERIVED key's entry there is its hidden
	// `__gb_expr_N` slot (ADR-0026 §2), which no grouping set can be spelled
	// with. Keyed on the materialized name, `ROLLUP (g + 1)` found nothing,
	// produced an EMPTY set, and every set collapsed to the grand total.
	var postOps []exec.UnaryOperator
	if len(node.GroupingSets) > 0 || len(node.GroupingCalls) > 0 {
		keyIndex := make(map[string]int, len(node.GroupBy))
		for i, c := range node.GroupBy {
			// FIRST wins. The key list is deduped upstream, so a repeat should
			// not arrive — but last-wins is the wrong reading if one ever does,
			// and it is what pointed both sets of `ROLLUP (g, g)` at position 1
			// and left position 0 grouped on nothing.
			if _, taken := keyIndex[strings.ToLower(strings.TrimSpace(c))]; !taken {
				keyIndex[strings.ToLower(strings.TrimSpace(c))] = i
			}
		}
		for i, c := range groupByCols {
			// The materialized spelling too, so a set written against a name
			// the pre-projection did not move still resolves.
			if _, taken := keyIndex[strings.ToLower(c)]; !taken {
				keyIndex[strings.ToLower(c)] = i
			}
		}
		if len(node.GroupingSets) > 0 {
			sets := make([][]int, len(node.GroupingSets))
			for i, set := range node.GroupingSets {
				indices := make([]int, 0, len(set))
				for _, col := range set {
					if idx, ok := keyIndex[strings.ToLower(strings.TrimSpace(col))]; ok {
						indices = append(indices, idx)
					}
				}
				sets[i] = indices
			}
			hashAgg.GroupingSets = sets
		}

		// GROUPING(a[, b, ...]): the same name→key-position map, but the
		// ARGUMENT ORDER is preserved and an unresolvable argument is an
		// error rather than a silently dropped bit. Dropping one would shift
		// every bit below it and answer a different number (#804); the
		// grouping-set loop above can drop a term because a set is a SET.
		for _, call := range node.GroupingCalls {
			positions := make([]int, 0, len(call.Args))
			for _, arg := range call.Args {
				idx, ok := keyIndex[strings.ToLower(strings.TrimSpace(arg))]
				if !ok {
					return nil, nil, nil, sqlerr.New("42803",
						"arguments to GROUPING must be grouping expressions of the associated query level: %q is not a group key of this aggregate", arg)
				}
				positions = append(positions, idx)
			}
			hashAgg.GroupingCalls = append(hashAgg.GroupingCalls, positions)
			hashAgg.GroupingCallNames = append(hashAgg.GroupingCallNames, call.OutputCol)
		}
	}
	if len(node.GroupingSets) == 0 && len(node.GroupingSetNulls) > 0 {
		hashAgg.NullGroupCols = node.GroupingSetNulls
	}

	// Elided literal keys re-attach as constant columns on the aggregate's
	// output, under the synthetic names the projection maps to.
	postOps = append(postOps, litPostOps...)

	// The aggregate acts as both sink and source
	// We need to run childSource -> childOps -> hashAgg(sink), then hashAgg(source) -> collectSink
	return &aggSourceAdapter{
		childSource: childSource,
		childOps:    childOps,
		agg:         hashAgg,
	}, postOps, &exec.CollectSink{}, nil
}

// isSimpleColRef returns true if the AST node is a simple column reference
// (no arithmetic, function calls, etc).
// replaceAggWithColRef returns a copy of the AST with the target aggregate
// function node replaced by a ColRef to the given column name.
func replaceAggWithColRef(node plansql.Node, target *plansql.FuncCallNode, colName string) plansql.Node {
	if node == nil {
		return nil
	}
	if fn, ok := node.(*plansql.FuncCallNode); ok && fn == target {
		return &plansql.ColRef{Column: colName}
	}
	switch n := node.(type) {
	case *plansql.FuncCallNode:
		newArgs := make([]plansql.Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = replaceAggWithColRef(a, target, colName)
		}
		return &plansql.FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  replaceAggWithColRef(n.Left, target, colName),
			Op:    n.Op,
			Right: replaceAggWithColRef(n.Right, target, colName),
		}
	case *plansql.ParenNode:
		return &plansql.ParenNode{Inner: replaceAggWithColRef(n.Inner, target, colName)}
	case *plansql.CastNode:
		return &plansql.CastNode{Inner: replaceAggWithColRef(n.Inner, target, colName), TypeName: n.TypeName}
	default:
		return node
	}
}
