package logical

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// BuildFromSelect constructs a logical plan from a parsed SELECT query.
func BuildFromSelect(info *plansql.SelectInfo) (*Node, error) {
	return BuildFromSelectWithCTEs(info, info.CTEs)
}

// BuildFromSelectWithCTEs constructs a logical plan, resolving CTE references
// to inline sub-plans instead of table scans.
func BuildFromSelectWithCTEs(info *plansql.SelectInfo, ctes []plansql.CTEDef) (*Node, error) {
	// Handle set operations (UNION, INTERSECT, EXCEPT)
	if info.Union != nil {
		return buildSetOpPlan(info, ctes)
	}

	// Where an aggregate or grouping operation may APPEAR, before anything is
	// planned. Every arm of a set operation reaches this through its own
	// recursive call, and a subquery through its own, so the scan stays
	// level-local.
	if err := checkAggregatePlacement(info); err != nil {
		return nil, err
	}

	plan, err := buildFromClause(info, ctes)
	if err != nil {
		return nil, err
	}

	// WHERE clause
	if info.Where != "" {
		preds := []Predicate{{Raw: info.Where, ASTExpr: info.WhereExpr}}
		plan = NewFilter(plan, preds)
	}

	// Check if we have aggregates
	hasAgg := false
	for _, col := range info.Columns {
		if col.IsAgg {
			hasAgg = true
			break
		}
	}
	// An aggregate in ORDER BY makes the query aggregated too, exactly as one
	// in the SELECT list or a HAVING clause does — PostgreSQL's
	// parseCheckAggregates reads the sort clause along with the rest (#811).
	//
	// This was read off the SELECT list alone, so `SELECT 1 FROM t ORDER BY
	// MAX(id)` built no Aggregate at all: the ORDER BY term was then a name
	// nothing carried, it was dropped, and the query returned every row where
	// PostgreSQL returns ONE. The `SELECT id ...` spelling returned every row
	// where PostgreSQL raises 42803 — the refusal that check lives in
	// physical.checkUngrouped and was unreachable for the same reason.
	orderByAggs := orderByAggregates(info)
	if len(orderByAggs) > 0 {
		hasAgg = true
	}

	// Track columns that need AST rewriting for nested aggregates.
	// Key: column index, Value: rewritten AST with aggregate replaced by ColRef.
	nestedAggRewrites := map[int]plansql.Node{}
	// Map aggregate expression string → synthetic name (for ORDER BY resolution)
	aggSyntheticNames := map[string]string{}

	// What an aggregate below PUBLISHES, for the WINDOW above it to be spelled
	// against (#737). Both are empty when the query has no aggregate, and the
	// respell is then a no-op.
	//
	// groupKeyRefs maps a computed GROUP BY key's identity to the column the
	// aggregate publishes its value under; winAggRefs maps an aggregate CALL
	// inside a window's own spec to the aggregate output that computes it.
	// A window evaluates its argument, its PARTITION BY and its ORDER BY over
	// the aggregate's OUTPUT rows, where both of those are NAMES — `g` and the
	// aggregate's input columns are gone — which is the same rule ADR-0026 §3
	// states for HAVING, one operator over.
	var groupKeyRefs map[string]string
	winAggRefs := map[string]string{}

	// GROUP BY / aggregation
	// GROUPING(...) anywhere in the SELECT list or HAVING (#804). Every call
	// gets a hidden aggregate output slot: the operator that assigned a row
	// its grouping set is the only thing that can answer the question, and
	// giving the plain-GROUP-BY case its own constant-folded spelling would
	// be a second mechanism that a nested call could not use. groupingSlots
	// maps a SELECT-list index whose column IS a bare call to its slot; a
	// call nested inside a larger expression is substituted into that
	// expression instead, through the machinery that already does this for
	// nested aggregates. Declared out here because the projection loop below
	// is what consumes them.
	var groupingCalls []GroupingCall
	groupingSlots := map[int]string{}
	groupingSlotFor := map[string]string{}

	// allocGroupingSlot validates one GROUPING call's arguments and returns
	// the slot its bitmask is published under, reusing the slot when the same
	// call appears more than once.
	allocGroupingSlot := func(fn *plansql.FuncCallNode) (string, error) {
		args := make([]string, 0, len(fn.Args))
		for _, a := range fn.Args {
			args = append(args, cleanExpr(plansql.Unparen(a).String()))
		}
		if len(args) == 0 {
			return "", sqlerr.New(groupingErrSQLState, "GROUPING requires at least one argument")
		}
		if err := checkGroupingArgs(args, info); err != nil {
			return "", err
		}
		key := strings.ToLower(fn.String())
		if slot, ok := groupingSlotFor[key]; ok {
			return slot, nil
		}
		slot := plansql.SlotName(plansql.SlotGrouping, len(groupingCalls))
		groupingCalls = append(groupingCalls, GroupingCall{Args: args, OutputCol: slot})
		groupingSlotFor[key] = slot
		return slot, nil
	}

	if hasAgg || len(info.GroupBy) > 0 {
		var aggs []AggExpr
		aggCounter := 0

		for i, col := range info.Columns {
			if col.IsAgg {
				// GROUPING() is not an aggregate: it reads which grouping SET
				// produced the row, which only the operator that assigned the
				// set knows. It gets a hidden aggregate output slot (or, with
				// a plain GROUP BY, the constant 0 — every key is grouped in
				// every row) and the projection below reads that (#804).
				if fn := bareGroupingCall(col.ASTExpr); fn != nil {
					slot, err := allocGroupingSlot(fn)
					if err != nil {
						return nil, err
					}
					groupingSlots[i] = slot
					continue
				}

				// The constant-arithmetic aggregate lift used to run HERE,
				// and it is gone: it ran before any type was known, so the
				// most it could see was the literal's SPELLING, and lifting on
				// that alone is not an identity over a float column. It lives
				// in const_arith_agg_typed.go now, inside logical.Optimize,
				// where the column's type is on the scan (#850, round-1 B1).

				// Find all aggregates in this expression (handles multi-aggregate
				// expressions like MAX(x) - MIN(x)).
				var allAggs []*plansql.FuncCallNode
				if col.ASTExpr != nil {
					allAggs = plansql.FindAllAggregates(col.ASTExpr)
				}

				// Detect nested aggregate: top-level is not a direct aggregate,
				// or there are multiple aggregates in the expression.
				isNested := false
				if len(allAggs) > 1 {
					isNested = true
				} else if col.ASTExpr != nil {
					if fn, topLevel := col.ASTExpr.(*plansql.FuncCallNode); !topLevel {
						isNested = true
					} else if topLevel && !plansql.IsAggregate(fn.Name) {
						isNested = true
					}
				}

				if isNested && len(allAggs) > 0 {
					// Register each aggregate with its own synthetic name.
					// Identical aggregates (same textual form) across select
					// items share ONE synthetic column — after the const-arith
					// rewrite a 90-expression query references the same
					// SUM(col)/COUNT(col) pair 90 times; without dedup each
					// reference computed its own copy.
					replacements := map[string]string{}
					for _, agg := range allAggs {
						aggKey := strings.ToLower(agg.String())
						if existing, ok := aggSyntheticNames[aggKey]; ok {
							replacements[aggKey] = existing
							continue
						}
						// A GROUPING(...) nested in a larger expression —
						// `GROUPING(g) + 1`, `CASE WHEN GROUPING(g) = 1 ...`,
						// `SUM(v) + GROUPING(g)`. It is not an aggregate and
						// must not become one: it substitutes its bitmask
						// slot into the surrounding expression the same way a
						// real nested aggregate substitutes its output (#804).
						if strings.EqualFold(agg.Name, "grouping") {
							slot, err := allocGroupingSlot(agg)
							if err != nil {
								return nil, err
							}
							replacements[aggKey] = slot
							aggSyntheticNames[aggKey] = slot
							continue
						}
						syntheticName := plansql.SlotName(plansql.SlotNestedAgg, aggCounter)
						aggCounter++

						aggInputCol := ""
						var aggInputExpr plansql.Node
						if len(agg.Args) > 0 {
							aggInputCol = cleanExpr(agg.Args[0].String())
							aggInputExpr = agg.Args[0]
						}
						funcName := strings.ToLower(agg.Name)
						if funcName == "count" && (aggInputCol == "*" || aggInputCol == "") {
							aggInputCol = ""
						}

						ae := AggExpr{
							Func:      funcName,
							InputCol:  aggInputCol,
							OutputCol: syntheticName,
							Distinct:  agg.Distinct,
							InputExpr: aggInputExpr,
						}
						if err := parseAggExtraArgs(&ae, agg.Args); err != nil {
							return nil, err
						}
						aggs = append(aggs, ae)

						replacements[aggKey] = syntheticName
						aggSyntheticNames[aggKey] = syntheticName
					}
					nestedAggRewrites[i] = plansql.ReplaceAllAggregates(col.ASTExpr, replacements)
				} else {
					// Simple non-nested single aggregate
					inputCol := cleanExpr(col.AggArg)
					if col.AggFunc == "count" && (inputCol == "*" || inputCol == "") {
						inputCol = ""
					}
					outputCol := col.Alias
					if outputCol == "" {
						outputCol = col.Expr
					}
					ae := AggExpr{
						Func:      col.AggFunc,
						InputCol:  inputCol,
						OutputCol: outputCol,
						Distinct:  col.AggDistinct,
						InputExpr: col.AggArgExpr,
					}
					if err := parseAggExtraArgs(&ae, col.AggArgs); err != nil {
						return nil, err
					}
					aggs = append(aggs, ae)
					aggCounter++
				}
			}
		}

		// Add hidden aggregates from HAVING that aren't in the SELECT list.
		// e.g., SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
		// needs SUM(l_quantity) computed even though it's not in SELECT.
		havingReplacements := map[string]string{}
		if info.HavingExpr != nil {
			havingAggs := plansql.FindAllAggregates(info.HavingExpr)
			for _, hAgg := range havingAggs {
				hKey := strings.ToLower(hAgg.String())
				// GROUPING in HAVING — `HAVING GROUPING(g) = 0` — is the same
				// substitution as in the SELECT list, and it must happen here
				// or the loop below mints an AggExpr for a function no
				// aggregate kernel implements: the column came back empty and
				// the HAVING matched NO rows, silently (#804).
				if strings.EqualFold(hAgg.Name, "grouping") {
					slot, err := allocGroupingSlot(hAgg)
					if err != nil {
						return nil, err
					}
					havingReplacements[hKey] = slot
					continue
				}
				aggInputCol := ""
				var aggInputExpr plansql.Node
				if len(hAgg.Args) > 0 {
					aggInputCol = cleanExpr(hAgg.Args[0].String())
					aggInputExpr = hAgg.Args[0]
				}
				funcName := strings.ToLower(hAgg.Name)
				if funcName == "count" && (aggInputCol == "*" || aggInputCol == "") {
					aggInputCol = ""
				}
				// Reuse an identical SELECT-list aggregate by NORMALIZED fields, not rendered text.
				// Decline reuse when a group key or another aggregate shares its output name:
				// by-name lookup reads the FIRST column, not necessarily this aggregate (#785, ADR-0026 §3a).
				// Instead compute it again under its own collision-free __having_N slot.
				// Leave the SELECT list unchanged: duplicate output names are legal, and their
				// publishing consumers distinguish them by CLASS and POSITION (#575).
				// See docs/internals/having-aggregate-reuse-identity.md for the design.
				found := false
				if len(hAgg.Args) <= 1 {
					for _, existing := range aggs {
						if existing.InputCol2 != "" || existing.InputCol3 != "" || existing.Separator != "" || existing.Percentile != 0 {
							continue
						}
						if strings.EqualFold(existing.Func, funcName) &&
							strings.EqualFold(existing.InputCol, aggInputCol) &&
							existing.Distinct == hAgg.Distinct {
							if aggOutputNameIsShared(existing.OutputCol, info.GroupBy, aggs) {
								continue
							}
							found = true
							havingReplacements[hKey] = existing.OutputCol
							break
						}
					}
				}
				if !found {
					synName := plansql.SlotName(plansql.SlotHaving, aggCounter)
					aggCounter++
					ae := AggExpr{
						Func:      funcName,
						InputCol:  aggInputCol,
						OutputCol: synName,
						Distinct:  hAgg.Distinct,
						InputExpr: aggInputExpr,
					}
					if err := parseAggExtraArgs(&ae, hAgg.Args); err != nil {
						return nil, err
					}
					aggs = append(aggs, ae)
					havingReplacements[hKey] = synName
				}
			}
		}

		// The same hoist for an aggregate named only in ORDER BY (#811).
		//
		// It is COMPUTED even though nothing reads its value: PostgreSQL
		// computes it too, and an aggregate that would raise (a SUM that
		// overflows, a CAST inside its argument) has to raise here as well.
		// Its output column is dropped with the rest of the planner's own
		// slots, so nothing reaches the client.
		//
		// The sort key itself is settled in resolveOrderBy: with no GROUP BY
		// the aggregate emits exactly ONE row, so the ORDER BY is provably a
		// no-op and the term is dropped rather than materialized. WITH a
		// GROUP BY it is not a no-op, and that case is still refused (#597).
		for _, oAgg := range orderByAggs {
			if strings.EqualFold(oAgg.Name, "grouping") {
				continue // not an aggregate; it has its own slot family
			}
			if _, err := reuseOrAddAggregate(oAgg, &aggs, &aggCounter); err != nil {
				return nil, err
			}
		}

		// The same hoist for an aggregate inside a WINDOW's own spec.
		//
		// `SUM(COUNT(*)) OVER ()` and `ROW_NUMBER() OVER (ORDER BY COUNT(*))`
		// run the window over the aggregate's OUTPUT rows, so the call inside
		// the spec names a column the aggregate publishes and is not something
		// the window can compute. Left as text it named nothing: the ORDER BY
		// key was NULL on every row and the window answered in an arbitrary
		// order, and the argument answered NULL — both silently, and both on
		// every arm (#737).
		//
		// An identical aggregate the SELECT list already computes is REUSED,
		// exactly as HAVING reuses one; anything else is hoisted into the
		// nested-aggregate slot family, which is where an aggregate inside an
		// expression has lived since #610.
		for _, col := range info.Columns {
			for _, term := range windowSpecTerms(col) {
				for _, wAgg := range plansql.FindAllAggregates(term) {
					wKey := strings.ToLower(wAgg.String())
					if _, done := winAggRefs[wKey]; done {
						continue
					}
					if existing, ok := aggSyntheticNames[wKey]; ok {
						winAggRefs[wKey] = existing
						continue
					}
					name, err := reuseOrAddAggregate(wAgg, &aggs, &aggCounter)
					if err != nil {
						return nil, err
					}
					winAggRefs[wKey] = name
					aggSyntheticNames[wKey] = name
				}
			}
		}

		if len(info.GroupingSets) > 0 {
			// GROUPING SETS / CUBE / ROLLUP: one aggregate pass over the union
			// of every set's terms, with the sets as metadata.
			allGroupCols := make([]string, len(info.GroupBy))
			for i, gb := range info.GroupBy {
				allGroupCols[i] = cleanExpr(gb)
			}

			aggNode := buildGroupingSets(plan, allGroupCols, aggs, info.GroupingSets)
			aggNode.GroupingCalls = groupingCalls
			// The keys' PARSED forms travel with them here too. A grouping-set
			// term is a GROUP BY term and nothing about the construct changes
			// what `g + 1` means: it is arithmetic one of the engines has to
			// materialize, and every consumer above resolves it by identity
			// (ADR-0026). Left off, `buildAggregate` saw only the text, could
			// not tell a derived key from an input column, and refused the
			// query — while the DAG answered a plain GROUP BY (#778).
			aggNode.GroupByExprs = info.GroupByExprs
			plan = aggNode
			groupKeyRefs = computedGroupKeyRefs(aggNode)
		} else {
			// Simple GROUP BY
			var groupBy []string
			for _, gb := range info.GroupBy {
				groupBy = append(groupBy, cleanExpr(gb))
			}
			aggNode := NewAggregate(plan, groupBy, aggs)
			aggNode.GroupByExprs = info.GroupByExprs
			// GROUPING(...) under a plain GROUP BY is always 0 — every key is
			// grouped in every row — but it takes the SAME slot as it does
			// over grouping sets rather than a constant-folded spelling of
			// its own. One mechanism: a nested call substitutes a column
			// reference either way, and there is no second path to get wrong.
			aggNode.GroupingCalls = groupingCalls
			plan = aggNode
			groupKeyRefs = computedGroupKeyRefs(aggNode)
		}

		// HAVING clause (must come after Aggregate)
		if info.Having != "" && info.HavingExpr != nil {
			var rewritten plansql.Node
			if len(havingReplacements) > 0 {
				rewritten = plansql.ReplaceAllAggregates(info.HavingExpr, havingReplacements)
			} else {
				rewritten = rewriteHavingExpr(info.HavingExpr, info.Columns)
			}
			// Spell the predicate against what the aggregate PUBLISHES.
			// Below the aggregate `g + 1` is arithmetic over `g`; above it,
			// it is the NAME of the one column carrying that value, and `g`
			// is gone. A predicate left as arithmetic evaluated UNKNOWN on
			// every row, and a filter admits only TRUE, so the query
			// answered with no rows at all where PostgreSQL answers five —
			// on BOTH execution paths, silently (#720).
			rewritten = plansql.ReplaceGroupKeyRefs(rewritten, groupKeyRefs)
			preds := []Predicate{{Raw: rewritten.String(), ASTExpr: rewritten}}
			plan = NewFilter(plan, preds)
		}
	}

	// Nested window functions: a window call wrapped inside a larger
	// expression — SUM(x) OVER (...) + 1, COALESCE(LAG(x) OVER (...), 0), a
	// window in a CASE branch. The parser only flags a window column when the
	// window is the WHOLE select expression (col.IsWindow), so those bare ones
	// are already in info.Windows. Here we extract windows embedded deeper into
	// their own NodeWindow output columns and rewrite the surrounding
	// expression to reference them, so the outer arithmetic/function is
	// evaluated OVER the window's result instead of being silently dropped
	// (#610).
	var nestedWinExprs []WindowExpr
	nestedWinRewrites := map[int]plansql.Node{}
	winCounter := 0
	for i, col := range info.Columns {
		if col.IsWindow || col.ASTExpr == nil {
			continue
		}
		wfns := plansql.FindAllWindowFuncs(col.ASTExpr)
		if len(wfns) == 0 {
			continue
		}
		replacements := map[*plansql.WindowFuncNode]string{}
		for _, wfn := range wfns {
			syntheticName := plansql.SlotName(plansql.SlotWindowOutput, winCounter)
			winCounter++
			nestedWinExprs = append(nestedWinExprs, windowExprFromNode(wfn, syntheticName))
			replacements[wfn] = syntheticName
		}
		nestedWinRewrites[i] = plansql.ReplaceWindowFuncs(col.ASTExpr, replacements)
	}

	// A BARE window column — one whose whole SELECT expression is the window
	// call — writes its result into a slot of its own, exactly as the nested
	// case above does, and the SELECT list reads THAT slot.
	//
	// It used to write under the user's ALIAS, and exec.Window APPENDS its
	// output to the input batch, so a query whose alias happened to spell an
	// input column's name handed the projection two columns called `s`. The
	// projection resolves by NAME and took the first: `SELECT id, SUM(a) OVER
	// () AS s FROM decpair` came back with decpair.s — the TEXT column — on
	// BOTH execution paths, silently, and `AS a` came back with the window's
	// own ARGUMENT column (#694). Provenance is the only thing that
	// distinguishes them, and the synthetic name IS the provenance.
	//
	// A window with no alias at all was the same defect one step further
	// along: nothing named the output, the projection asked for "", and the
	// single-process path answered NULL while the DAG dropped the column from
	// the result entirely.
	bareWinOutput := map[int]string{}
	if len(info.Windows) > 0 || len(nestedWinExprs) > 0 {
		var winExprs []WindowExpr
		for i, col := range info.Columns {
			if !col.IsWindow || col.WindowSpec == nil {
				continue
			}
			ws := *col.WindowSpec
			var orderBy []OrderExpr
			for _, ob := range ws.OrderBy {
				orderBy = append(orderBy, OrderExpr{
					Column:     cleanExpr(ob.Column),
					Desc:       ob.Desc,
					NullsFirst: ob.NullsFirst,
				})
			}
			partBy := make([]string, len(ws.PartitionBy))
			for j, p := range ws.PartitionBy {
				partBy[j] = cleanExpr(p)
			}
			syntheticName := plansql.SlotName(plansql.SlotWindowOutput, winCounter)
			winCounter++
			bareWinOutput[i] = syntheticName
			we := WindowExpr{
				Func:        ws.FuncName,
				InputCol:    cleanExpr(ws.Args),
				OutputCol:   syntheticName,
				PartitionBy: partBy,
				OrderBy:     orderBy,
				InputExpr:   windowArgNode(col.ASTExpr),
			}
			if ws.Frame != nil {
				we.Frame = convertFrame(ws.Frame)
			}
			winExprs = append(winExprs, we)
		}
		winExprs = append(winExprs, nestedWinExprs...)
		// Spell every term the window will EVALUATE against what the producer
		// below it PUBLISHES. Above an aggregate `g + 1` is the NAME of one
		// column and `COUNT(*)` is the name of another; rebuilding either as an
		// expression reads input columns the aggregate does not emit (#737).
		// With no aggregate below, both maps are empty and this is a no-op.
		for i := range winExprs {
			winExprs[i].InputCol = respellOverAggregate(winExprs[i].InputCol, winAggRefs, groupKeyRefs)
			for j := range winExprs[i].PartitionBy {
				winExprs[i].PartitionBy[j] = respellOverAggregate(
					winExprs[i].PartitionBy[j], winAggRefs, groupKeyRefs)
			}
			for j := range winExprs[i].OrderBy {
				before := winExprs[i].OrderBy[j].Column
				after := respellOverAggregate(before, winAggRefs, groupKeyRefs)
				winExprs[i].OrderBy[j].Column = after
				// …and WHICH map re-spelled it, which is the CLASS the name
				// itself can no longer carry once the aggregate emits it
				// twice. Asking the aggregate map ALONE is the test: a term
				// that names an aggregate CALL is re-spelled by it, and a
				// group-key reference is not (#968).
				if after != before && respellOverAggregate(before, winAggRefs, nil) == after {
					winExprs[i].OrderBy[j].NamesAggregateOutput = true
				}
			}
		}
		plan = NewWindow(plan, winExprs)
	}

	// When ORDER BY references a nested aggregate, sort BEFORE Project
	// so the sort operates on raw numeric aggregate values rather than
	// post-formatted strings.
	sortBeforeProject := false
	if len(nestedAggRewrites) > 0 && len(info.OrderBy) > 0 {
		for _, ob := range info.OrderBy {
			colLower := strings.ToLower(cleanExpr(ob.Column))
			// Check if ORDER BY matches any aggregate expression
			for _, sc := range info.Columns {
				if !sc.IsAgg {
					continue
				}
				var aggExpr string
				if sc.AggFunc == "count" && (sc.AggArg == "*" || sc.AggArg == "") {
					aggExpr = "count(*)"
				} else {
					aggExpr = strings.ToLower(sc.AggFunc) + "(" + strings.ToLower(sc.AggArg) + ")"
				}
				if aggExpr == colLower {
					sortBeforeProject = true
					break
				}
			}
			if sortBeforeProject {
				break
			}
		}
	}

	// Build Sort before Project when ORDER BY references nested aggregates.
	// Also outside resolveOrderBy's reach (#320): this Sort runs BELOW the
	// projection, on the aggregate's own output, so the select-list names it
	// would resolve against are not the ones it reads. resolveOrderByPreProject
	// maps each term to the aggregate's synthetic output instead.
	if sortBeforeProject {
		var orderExprs []OrderExpr
		for _, ob := range info.OrderBy {
			orderExprs = append(orderExprs, OrderExpr{
				Column:     resolveOrderByPreProject(cleanExpr(ob.Column), info.Columns, aggSyntheticNames),
				Desc:       ob.Desc,
				NullsFirst: ob.NullsFirst,
			})
		}
		plan = NewSort(plan, orderExprs)
	}

	// PROJECT (SELECT columns)
	var projectNode *Node
	if !isStarOnly(info.Columns) {
		var projections []Projection
		for i, col := range info.Columns {
			if col.IsWindow {
				// Window column: read the window's own output SLOT and
				// publish it under the name the SELECT list asked for. The
				// slot, not the alias, because an input column may be spelled
				// like the alias and the projection resolves by name (#694).
				src := bareWinOutput[i]
				name := windowOutputName(col)
				if src == "" {
					// No window node was built for this column, which means
					// the parse said IsWindow and the builder disagreed. Keep
					// the old spelling rather than projecting nothing.
					src = name
				}
				projections = append(projections, Projection{
					Expr:          src,
					Alias:         name,
					Column:        src,
					SlotSource:    src,
					PublishedName: plansql.OutputColumnName(col),
				})
				continue
			}
			// GROUPING(...) reads the aggregate's hidden bitmask slot by name
			// (the window-column pattern), or is the constant 0 when a plain
			// GROUP BY leaves every key grouped in every row (#804).
			if slot, ok := groupingSlots[i]; ok {
				projections = append(projections, Projection{
					Expr:       slot,
					Alias:      groupingOutputName(col),
					Column:     slot,
					SlotSource: slot,
				})
				continue
			}
			p := Projection{
				Expr:    col.Expr,
				Alias:   col.Alias,
				IsAgg:   col.IsAgg,
				ASTExpr: col.ASTExpr,
				// PostgreSQL's name for the column, which is not always the
				// name the planner resolves it by (#732).
				PublishedName: plansql.OutputColumnName(col),
			}
			// For nested aggregates, use the rewritten AST that references
			// the synthetic aggregate output column. The projection is no
			// longer an aggregate — it's a regular expression that references
			// the aggregate output column.
			if rewritten, ok := nestedAggRewrites[i]; ok {
				p.ASTExpr = rewritten
				p.IsAgg = false
			}
			// A nested window column (#610): the window has been extracted into
			// a NodeWindow output column and the projection now evaluates the
			// surrounding expression over that column's ColRef.
			if rewritten, ok := nestedWinRewrites[i]; ok {
				p.ASTExpr = rewritten
			}
			if col.ColumnRef != "" {
				p.Column = col.ColumnRef
			}
			projections = append(projections, p)
		}
		plan = NewProject(plan, projections)
		projectNode = plan
	}

	// DISTINCT
	if info.Distinct {
		plan = NewDistinct(plan)
	}

	// ORDER BY (skip if already sorted before project). resolveOrderBy
	// materializes any term the Sort's input does not carry and rejects the
	// ones it cannot — an ORDER BY that resolves to nothing is an error, not
	// a silently arbitrary order (#320).
	if len(info.OrderBy) > 0 && !sortBeforeProject {
		child, orderExprs, err := resolveOrderBy(plan, projectNode, info)
		if err != nil {
			return nil, err
		}
		plan = NewSort(child, orderExprs)
	}

	// LIMIT / OFFSET
	limitNode, err := buildLimitNode(plan, info)
	if err != nil {
		return nil, err
	}
	plan = limitNode

	// Store CTE definitions on the root node so the physical planner
	// can resolve CTE references in scalar subqueries.
	if len(ctes) > 0 {
		plan.CTEs = ctes
	}

	return plan, nil
}

// isStarOnly reports whether a SELECT list is the IDENTITY of its input, so no
// projection has to be built for it.
//
// A BARE `*` is: it stands for every column of every FROM item, in order, which
// is exactly what the node below publishes. A QUALIFIED `o.*` is NOT — it names
// ONE relation, and over a join the input carries the others too. Reading the
// two the same way is the whole of #979: `SELECT o.*` built no Project, so
// `ExpandStarProjections` (which only ever rewrites a Project) never saw the
// star, and the query published the entire join — `id, order_id, product,
// amount, o.id, customer, total` where PostgreSQL 17 publishes o's three, with
// a column literally called `o.id` among them. The same list with one more item
// beside it has always been right, because that one has a projection.
func isStarOnly(cols []plansql.SelectColumn) bool {
	return len(cols) == 1 && cols[0].Star && cols[0].TableRef == ""
}

// windowOutputName is plansql.WindowOutputName. The rule lives there because
// the parser's positional-ORDER-BY resolvers need it too and cannot import the
// planner; see that function for the five namers it keeps in agreement.
func windowOutputName(col plansql.SelectColumn) string {
	return plansql.WindowOutputName(col)
}

func cleanExpr(s string) string {
	return strings.TrimSpace(s)
}

// resolveOrderByColumn resolves an ORDER BY expression to the matching SELECT
// column's output name. This handles cases like ORDER BY SUM(x) DESC when the
// SELECT has SUM(x) AS total — the sort key must use "total", not "sum(x)".
func resolveOrderByColumn(col string, selectCols []plansql.SelectColumn) string {
	colLower := strings.ToLower(col)
	// 1. Direct alias match (case-insensitive)
	for _, sc := range selectCols {
		if strings.ToLower(sc.Alias) == colLower {
			return sc.Alias
		}
	}
	// 2. Expression match (case-insensitive)
	for _, sc := range selectCols {
		if strings.ToLower(sc.Expr) == colLower {
			if sc.Alias != "" {
				return sc.Alias
			}
			return sc.Expr
		}
	}
	// 3. Aggregate function match: ORDER BY sum(x) matches SELECT sum(x) AS alias
	for _, sc := range selectCols {
		if !sc.IsAgg {
			continue
		}
		var aggExpr string
		if sc.AggFunc == "count" && (sc.AggArg == "*" || sc.AggArg == "") {
			aggExpr = "count(*)"
		} else {
			aggExpr = sc.AggFunc + "(" + strings.ToLower(sc.AggArg) + ")"
		}
		if aggExpr == colLower {
			if sc.Alias != "" {
				return sc.Alias
			}
			return sc.Expr
		}
	}
	return col
}

// resolveOrderByPreProject resolves an ORDER BY expression to a column name
// available at the Aggregate output level (before projection). This maps
// aggregate expressions to their synthetic names and aliases to underlying
// column names.
func resolveOrderByPreProject(col string, selectCols []plansql.SelectColumn, aggSynthetic map[string]string) string {
	colLower := strings.ToLower(col)

	// 1. Check if it matches a known aggregate with a synthetic name
	if synthName, ok := aggSynthetic[colLower]; ok {
		return synthName
	}

	// 2. Check aggregate function pattern: sum(x) → synthetic name
	for _, sc := range selectCols {
		if !sc.IsAgg {
			continue
		}
		var aggExpr string
		if sc.AggFunc == "count" && (sc.AggArg == "*" || sc.AggArg == "") {
			aggExpr = "count(*)"
		} else {
			aggExpr = strings.ToLower(sc.AggFunc) + "(" + strings.ToLower(sc.AggArg) + ")"
		}
		if aggExpr == colLower {
			// Check if this aggregate has a synthetic name
			if synthName, ok := aggSynthetic[aggExpr]; ok {
				return synthName
			}
			// Non-nested aggregate: use alias or expression
			if sc.Alias != "" {
				return sc.Alias
			}
			return sc.Expr
		}
	}

	// 3. Direct alias → resolve to underlying column name
	for _, sc := range selectCols {
		if strings.ToLower(sc.Alias) == colLower {
			if sc.ColumnRef != "" {
				return sc.ColumnRef
			}
			return sc.Alias
		}
	}

	// 4. Expression match
	for _, sc := range selectCols {
		if strings.ToLower(sc.Expr) == colLower {
			if sc.ColumnRef != "" {
				return sc.ColumnRef
			}
			return sc.Expr
		}
	}

	return col
}

// rewriteHavingExpr rewrites aggregate function calls in a HAVING expression
// to column references that match the aggregate output column names.
func rewriteHavingExpr(expr plansql.Node, cols []plansql.SelectColumn) plansql.Node {
	return rewriteExpr(expr, cols)
}

func rewriteExpr(node plansql.Node, cols []plansql.SelectColumn) plansql.Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *plansql.FuncCallNode:
		// Only an AGGREGATE stands for a column the aggregate produced. This
		// arm used to rewrite EVERY function call that way, so a HAVING over
		// an ordinary scalar function of a grouped column — `HAVING
		// COALESCE(f, FALSE)`, `HAVING starts_with(name, 'A')` — became a
		// reference to a column literally named "coalesce(f, false)", which
		// no batch has. As a bare predicate that filter silently admitted NO
		// GROUP; with a comparison around it the query failed with `filter
		// column "coalesce(f, false)" does not exist in the input schema`
		// (#592's sweep of the bare-boolean class).
		//
		// The bug was only reachable here because this whole function is the
		// fallback for a HAVING that names no aggregate at all — one that
		// does takes ReplaceAllAggregates above, which has always asked
		// IsAggregate. A scalar call's ARGUMENTS still get walked: an
		// aggregate can hide inside one, `HAVING ABS(MAX(c)) > 1`.
		if !plansql.IsAggregate(n.Name) {
			args := make([]plansql.Node, len(n.Args))
			for i, a := range n.Args {
				args[i] = rewriteExpr(a, cols)
			}
			out := *n
			out.Args = args
			return &out
		}
		funcStr := n.String()
		colName := funcStr // default: use the expression string as column name
		for _, col := range cols {
			if col.IsAgg && col.ASTExpr != nil && strings.EqualFold(col.ASTExpr.String(), funcStr) {
				if col.Alias != "" {
					colName = col.Alias
				} else {
					colName = col.Expr
				}
				break
			}
		}
		return &plansql.ColRef{Column: colName}
	case *plansql.AndNode:
		return &plansql.AndNode{
			Left:  rewriteExpr(n.Left, cols),
			Right: rewriteExpr(n.Right, cols),
		}
	case *plansql.OrNode:
		return &plansql.OrNode{
			Left:  rewriteExpr(n.Left, cols),
			Right: rewriteExpr(n.Right, cols),
		}
	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Op:    n.Op,
			Left:  rewriteExpr(n.Left, cols),
			Right: rewriteExpr(n.Right, cols),
		}
	case *plansql.ParenNode:
		return &plansql.ParenNode{
			Inner: rewriteExpr(n.Inner, cols),
		}
	case *plansql.NotNode:
		return &plansql.NotNode{
			Inner: rewriteExpr(n.Inner, cols),
		}
	case *plansql.CastNode:
		return &plansql.CastNode{
			Inner:    rewriteExpr(n.Inner, cols),
			TypeName: n.TypeName,
		}
	default:
		// Literals, ColRef, etc. — pass through unchanged
		return node
	}
}

// windowExprFromNode builds a logical WindowExpr directly from a parsed
// WindowFuncNode, for a window function extracted out of a larger expression
// (#610). It mirrors the WindowSpec→WindowExpr conversion the bare top-level
// path performs above, reading the func name, argument list, PARTITION BY /
// ORDER BY keys and frame straight off the AST node.
// windowSpecTerms returns the parsed terms a SELECT-list item's WINDOWS will
// EVALUATE — the function's arguments, the PARTITION BY keys and the ORDER BY
// keys — for both spellings: a BARE window column, whose spec the parser keeps
// as text, and a window NESTED inside a larger expression, whose spec is
// already an AST.
//
// The window's own OUTPUT is deliberately not a term: it is what the operator
// computes, not what it reads.
func windowSpecTerms(col plansql.SelectColumn) []plansql.Node {
	var out []plansql.Node
	add := func(text string) {
		text = strings.TrimSpace(text)
		if text == "" || text == "*" {
			return
		}
		if parsed, err := plansql.ParseExpression(text); err == nil && parsed != nil {
			out = append(out, parsed)
		}
	}
	if col.IsWindow && col.WindowSpec != nil {
		add(col.WindowSpec.Args)
		for _, p := range col.WindowSpec.PartitionBy {
			add(p)
		}
		for _, ob := range col.WindowSpec.OrderBy {
			add(ob.Column)
		}
		return out
	}
	if col.ASTExpr == nil {
		return nil
	}
	for _, wfn := range plansql.FindAllWindowFuncs(col.ASTExpr) {
		if wfn.Func != nil {
			out = append(out, wfn.Func.Args...)
		}
		out = append(out, wfn.PartitionBy...)
		for _, ob := range wfn.OrderBy {
			if ob.Expr != nil {
				out = append(out, ob.Expr)
			}
		}
	}
	return out
}

// reuseOrAddAggregate returns the column an aggregate CALL is published under,
// reusing an identical aggregate the SELECT list already computes and adding a
// hidden one when there is none.
//
// The match is on the NORMALIZED fields — function, input column, DISTINCT —
// and never on rendered text: an AggExpr's InputCol has had `count(*)`'s star
// emptied by the time it is stored, while the AST still renders `count(*)`, so
// a text key made `HAVING COUNT(*) > 1` beside `COUNT(*) AS c` count twice.
func reuseOrAddAggregate(call *plansql.FuncCallNode, aggs *[]AggExpr, counter *int) (string, error) {
	inputCol := ""
	var inputExpr plansql.Node
	if len(call.Args) > 0 {
		inputCol = cleanExpr(call.Args[0].String())
		inputExpr = call.Args[0]
	}
	funcName := strings.ToLower(call.Name)
	if funcName == "count" && (inputCol == "*" || inputCol == "") {
		inputCol = ""
	}
	if len(call.Args) <= 1 {
		for _, existing := range *aggs {
			if existing.InputCol2 != "" || existing.InputCol3 != "" || existing.Separator != "" || existing.Percentile != 0 {
				continue
			}
			if strings.EqualFold(existing.Func, funcName) &&
				strings.EqualFold(existing.InputCol, inputCol) &&
				existing.Distinct == call.Distinct {
				return existing.OutputCol, nil
			}
		}
	}
	name := plansql.SlotName(plansql.SlotNestedAgg, *counter)
	*counter++
	ae := AggExpr{
		Func:      funcName,
		InputCol:  inputCol,
		OutputCol: name,
		Distinct:  call.Distinct,
		InputExpr: inputExpr,
	}
	if err := parseAggExtraArgs(&ae, call.Args); err != nil {
		return "", err
	}
	*aggs = append(*aggs, ae)
	return name, nil
}

// respellOverAggregate rewrites one window-spec term so it names what the
// aggregate below PUBLISHES: an aggregate call becomes the output column that
// computes it, and a computed GROUP BY key becomes the column the key's value
// is published under.
//
// The result is RENDERED, which for a published key means a DELIMITED
// identifier: a computed key is published under its own canonical text, so
// `g + 1` names one column and re-parsing it bare would read it back as
// arithmetic over a `g` the aggregate does not emit — ADR-0026 §2c's rule in
// the direction a window's key resolver reads. `physical.resolveWindowKeys`
// strips the delimiters and binds the name; without them it materialized the
// key by EVALUATING it and ordered by NULL on every row.
func respellOverAggregate(term string, aggRefs, keyRefs map[string]string) string {
	if term == "" || term == "*" || (len(aggRefs) == 0 && len(keyRefs) == 0) {
		return term
	}
	parsed, err := plansql.ParseExpression(term)
	if err != nil || parsed == nil {
		return term
	}
	out := parsed
	if len(aggRefs) > 0 {
		out = plansql.ReplaceAllAggregates(out, aggRefs)
	}
	if len(keyRefs) > 0 {
		out = plansql.ReplaceGroupKeyRefs(out, keyRefs)
	}
	if out == nil || out == parsed {
		return term
	}
	return out.String()
}

// windowArgNode returns the AST of a window function's FIRST argument, for
// WindowExpr.InputExpr. It takes the SELECT item rather than the
// WindowFuncNode so both construction sites — the bare SELECT-list window,
// which holds a plansql.WindowSpec with no node on it, and the nested one,
// which holds the node itself — can ask one function.
//
// nil for `COUNT(*)`, for a zero-argument rank function, and for anything that
// is not a window node: a consumer must treat a missing node as "unknown", not
// as a narrower answer.
func windowArgNode(n plansql.Node) plansql.Node {
	wfn, ok := n.(*plansql.WindowFuncNode)
	if !ok || wfn.Func == nil || wfn.Func.Star || len(wfn.Func.Args) == 0 {
		return nil
	}
	return wfn.Func.Args[0]
}

func windowExprFromNode(wfn *plansql.WindowFuncNode, outputCol string) WindowExpr {
	inputCol := ""
	if wfn.Func.Star {
		inputCol = "*"
	} else if len(wfn.Func.Args) > 0 {
		args := make([]string, len(wfn.Func.Args))
		for i, a := range wfn.Func.Args {
			args[i] = a.String()
		}
		inputCol = cleanExpr(strings.Join(args, ", "))
	}
	partBy := make([]string, len(wfn.PartitionBy))
	for i, pb := range wfn.PartitionBy {
		partBy[i] = cleanExpr(pb.String())
	}
	var orderBy []OrderExpr
	for _, ob := range wfn.OrderBy {
		orderBy = append(orderBy, OrderExpr{
			Column:     cleanExpr(ob.Expr.String()),
			Desc:       ob.Desc,
			NullsFirst: ob.NullsFirst,
		})
	}
	we := WindowExpr{
		Func:        wfn.Func.Name,
		InputCol:    inputCol,
		OutputCol:   outputCol,
		PartitionBy: partBy,
		OrderBy:     orderBy,
		InputExpr:   windowArgNode(wfn),
	}
	if wfn.Frame != nil {
		we.Frame = convertFrame(wfn.Frame)
	}
	return we
}

// convertFrame converts a SQL WindowFrame to a logical WindowFrameSpec.
func convertFrame(f *plansql.WindowFrame) *WindowFrameSpec {
	spec := &WindowFrameSpec{}
	switch f.Mode {
	case plansql.FrameRows:
		spec.Mode = "rows"
	case plansql.FrameRange:
		spec.Mode = "range"
	}
	spec.Start = convertBound(f.Start)
	if f.End != nil {
		spec.End = convertBound(*f.End)
	} else {
		spec.End = WindowBound{Type: "current_row"}
	}
	return spec
}

func convertBound(b plansql.FrameBound) WindowBound {
	wb := WindowBound{}
	switch b.Type {
	case plansql.BoundUnboundedPreceding:
		wb.Type = "unbounded_preceding"
	case plansql.BoundPreceding:
		wb.Type = "preceding"
		if b.Offset != nil {
			if lit, ok := b.Offset.(*plansql.Lit); ok {
				v, _ := strconv.Atoi(lit.Value)
				wb.Offset = v
			}
		}
	case plansql.BoundCurrentRow:
		wb.Type = "current_row"
	case plansql.BoundFollowing:
		wb.Type = "following"
		if b.Offset != nil {
			if lit, ok := b.Offset.(*plansql.Lit); ok {
				v, _ := strconv.Atoi(lit.Value)
				wb.Offset = v
			}
		}
	case plansql.BoundUnboundedFollowing:
		wb.Type = "unbounded_following"
	}
	return wb
}

// buildGroupingSets creates a single aggregate node that processes all grouping
// sets in one pass. The HashAggregate inserts each row once per set with a
// set-prefixed key, avoiding N rescans of the input.
func buildGroupingSets(inputPlan *Node, allGroupCols []string, aggs []AggExpr, sets [][]string) *Node {
	if len(sets) == 0 {
		return NewAggregate(inputPlan, allGroupCols, aggs)
	}

	// Normalize set columns
	cleanSets := make([][]string, len(sets))
	for i, set := range sets {
		cs := make([]string, len(set))
		for j, c := range set {
			cs[j] = cleanExpr(c)
		}
		cleanSets[i] = cs
	}

	// Single aggregate over all group columns, with GroupingSets metadata
	aggNode := NewAggregate(inputPlan, allGroupCols, aggs)
	aggNode.GroupingSets = cleanSets
	return aggNode
}

// buildSetOpPlan constructs a logical plan for a set operation (UNION, INTERSECT, EXCEPT).
func buildSetOpPlan(info *plansql.SelectInfo, ctes []plansql.CTEDef) (*Node, error) {
	op := info.Union.Op
	if op == "" {
		op = plansql.SetOpUnion // backwards compat
	}

	leftPlan, err := BuildFromSelectWithCTEs(info.Union.Left, ctes)
	if err != nil {
		return nil, fmt.Errorf("building %s left side: %w", op, err)
	}
	rightPlan, err := BuildFromSelectWithCTEs(info.Union.Right, ctes)
	if err != nil {
		return nil, fmt.Errorf("building %s right side: %w", op, err)
	}

	var plan *Node
	switch op {
	case plansql.SetOpIntersect:
		plan = NewIntersect(leftPlan, rightPlan, info.Union.All)
	case plansql.SetOpExcept:
		plan = NewExcept(leftPlan, rightPlan, info.Union.All)
	default:
		plan = NewUnion(leftPlan, rightPlan, info.Union.All)
	}

	// ORDER BY on the overall set operation. Deliberately outside
	// resolveOrderBy's reach (#320): a set operation's output names come from
	// its branches, which are planned independently here, so there is no
	// SELECT list to resolve a term against and no projection to materialize
	// one onto. A term that names something the union does not emit still
	// reaches the sort as written.
	//
	// The POSITION an ordinal was written as rides along, through the same
	// orderExprFor every other sort key is built by. A set operation's result
	// columns are its leftmost arm's names and two of them may be the SAME
	// string — `SELECT order_id AS amount, amount FROM lat_item UNION SELECT
	// id, total FROM lat_ord ORDER BY 1, 2 DESC` publishes `amount` twice —
	// so with the position dropped both keys reached the sort spelled
	// `amount`, bound the first of them, and key 2 was never applied on any
	// arm (#1022).
	if len(info.OrderBy) > 0 {
		leftmost := info
		for leftmost.Union != nil {
			leftmost = leftmost.Union.Left
		}
		var orderExprs []OrderExpr
		for _, ob := range info.OrderBy {
			// A position the PARSER could not count, because the leftmost
			// arm's list carries a `*`: ResolveOrdinalSortKeys names it once
			// the star has expanded, against the set operation's own result
			// columns (projectOutputNamesBelow descends a set-op node to its
			// leftmost arm — #982).
			if pos, ok := deferredOrdinal(ob, leftmost.Columns); ok {
				orderExprs = append(orderExprs, OrderExpr{
					Column: cleanExpr(ob.Column), Position: pos,
					Desc: ob.Desc, NullsFirst: ob.NullsFirst,
				})
				continue
			}
			orderExprs = append(orderExprs, orderExprFor(cleanExpr(ob.Column), ob))
		}
		plan = NewSort(plan, orderExprs)
	}

	// LIMIT / OFFSET on the overall set operation
	return buildLimitNode(plan, info)
}

// buildLimitNode wraps plan in a Limit for the statement's LIMIT and OFFSET.
//
// OFFSET applies whether or not a LIMIT accompanies it. It used to be read
// only inside the `if info.Limit != ""` branch, so `ORDER BY 1 OFFSET 5`
// returned all 25 rows instead of 20 — a paginating client asking for any
// page but the first got the whole table, and the first page still looked
// right (#337).
func buildLimitNode(plan *Node, info *plansql.SelectInfo) (*Node, error) {
	if info.Limit == "" && info.Offset == "" {
		return plan, nil
	}
	limit := NoLimit
	if info.Limit != "" {
		n, err := strconv.Atoi(info.Limit)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT: %w", err)
		}
		limit = n
	}
	offset := 0
	if info.Offset != "" {
		n, err := strconv.Atoi(info.Offset)
		if err != nil {
			return nil, fmt.Errorf("invalid OFFSET: %w", err)
		}
		offset = n
	}
	// `OFFSET 0` with no LIMIT skips nothing and bounds nothing.
	if limit == NoLimit && offset == 0 {
		return plan, nil
	}
	return NewLimit(plan, limit, offset), nil
}

// buildFromClause plans a SELECT's FROM list: every comma item's own subtree,
// then the explicit JOINs attached to the item each one follows.
//
// It is a function rather than an inline block because the three
// decorrelations need the SAME assembly for the subquery they lower. Building
// the inner side out of `NewScan(info.Tables[0].Name)` instead was the source
// of a defect class of its own — a derived table has no name a Scan can hold,
// and neither does a CTE reference, so the semi/anti join's build side became
// a scan of a table the catalog has never heard of and answered NOTHING
// (#571, #535). Declining those shapes made them right and SLOW — one re-read
// of the inner relation per outer row (#852); this is what makes them right
// and fast. See decorrelated_inner_plan.go.
func buildFromClause(info *plansql.SelectInfo, ctes []plansql.CTEDef) (*Node, error) {
	var plan *Node
	// FROM clause — build scan nodes (or CTE sub-plans)
	if len(info.Joins) > 0 {
		// Build join tree
		if len(info.Tables) == 0 {
			return nil, fmt.Errorf("no tables in FROM clause")
		}
		// Comma-separated FROM entries beyond the first parse into
		// info.Tables (the parser only emits JoinInfo for explicit JOIN
		// syntax). Each is a FROM ITEM, and an explicit JOIN extends the
		// item it follows — JoinInfo.FromItem says which. Build every item's
		// own subtree first, then cross-join the items left to right;
		// pushdownPredicates, liftWhereEquiPredsIntoJoins and reorderJoins
		// recover the real join conditions from WHERE.
		//
		// Dropping the extra tables silently returned wrong results (#281).
		// Folding them in BEFORE the explicit joins — which is what this did
		// until #593/#594 — was the next wrong answer: `FROM a JOIN b ON …,
		// c` planned as `(a × c) ⋈ b` rather than `(a ⋈ b) × c`, so a real
		// cross product sat under the equi-join (60,175 × 2,000 rows on the
		// SF0.01 fixture, an OOM kill at 30 GB, #593) and the WHERE equality
		// between b and c straddled that join's two sides, where its key
		// pair resolves against neither and the query answers zero rows with
		// no error (#594).
		items := make([]*Node, len(info.Tables))
		for i := range info.Tables {
			// &info.Tables[i], not the range VALUE: the sub-block parse is
			// memoized on the TableRef the parser owns (ADR-0032), and a copy
			// carries the memo nowhere.
			item, err := resolveTableOrCTE(&info.Tables[i], ctes)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		// crossFold merges items[0..k] into items[k], left-deep, leaving nil
		// behind. LATERAL needs it: its right side may reference EVERY
		// preceding FROM item, not just the one it extends.
		crossFold := func(k int) *Node {
			var acc *Node
			for i := 0; i <= k; i++ {
				if items[i] == nil {
					continue
				}
				if acc == nil {
					acc = items[i]
				} else {
					acc = NewJoin(acc, items[i], "cross", "")
				}
				items[i] = nil
			}
			items[k] = acc
			return acc
		}

		for joinIdx, join := range info.Joins {
			// FromItem is non-decreasing across Joins, so the slot a join
			// names is never one an earlier crossFold emptied.
			idx := join.FromItem
			if idx < 0 || idx >= len(items) {
				idx = len(items) - 1
			}
			if join.Lateral && strings.HasPrefix(join.RightTable, "(") {
				// LATERAL subquery: decorrelate by extracting correlated
				// WHERE predicates and moving them to the join condition.
				left := crossFold(idx)
				// A body with NO FROM clause has no relation to decorrelate
				// against: it is a PROJECTION OVER THE OUTER ROW and is
				// lowered as one (lateral_dual_body.go, #1033).
				if body, perr := lateralBodySelect(join); perr == nil && lateralDualBody(body) {
					lowered, lerr := buildTableLessLateralJoin(info, left, join, ctes)
					if lerr != nil {
						return nil, lerr
					}
					items[idx] = lowered
					continue
				}
				right, joinCond, empty, hiddenCols, err := buildLateralSubquery(info, left, join, ctes)
				if err != nil {
					return nil, err
				}
				// Cross join with correlated predicates → inner join
				// (cross join skips key parsing in the physical planner)
				jt := join.Type
				if joinCond != "" && strings.EqualFold(strings.TrimSpace(jt), "cross join") {
					jt = "join"
				}
				// An UNGROUPED aggregate over an empty input still yields one
				// row, so an outer row the lateral matches nothing for
				// SURVIVES in PostgreSQL — see lateralEmptyInput (#767 part 1).
				//
				// The join's OWN `ON` decides what happens to that row NEXT,
				// and it has to keep deciding: PostgreSQL evaluates the
				// lateral per outer row, THEN applies the join condition. A
				// repair that forces LEFT and defaults the COUNT without
				// looking at the ON keeps rows the ON rejects and prints 0
				// for a count of 2. See lateralEmptyInputPlan for the three
				// cases and which of them this can express.
				plan := lateralEmptyInputPlan(jt, empty,
					lateralJoinNullExtendsAfter(info.Joins, joinIdx))
				var emptyDefaults []LateralEmptyDefault
				switch plan {
				case lateralPadOnly, lateralPadThenFilter:
					// The empty-input value rides on the COLUMN, once, for
					// every consumer: a star, a derived star, a CTE, the
					// wire, a scalar subquery's substitution and an EXISTS.
					// The REFERENCE rewrite this replaced could reach none of
					// those, and could not tell a pad from a matched NULL
					// either — it wrapped every reference in COALESCE(…, 0),
					// so `NULLIF(COUNT(*), 2)` read 0 on a matched row that
					// counted 2.
					emptyDefaults = empty.defaults
				}
				if err := refuseUnorderedLateralOn(plan, empty, join.RightAlias,
					join.CondExpr, join.Condition); err != nil {
					return nil, err
				}
				switch plan {
				case lateralPadThenFilter:
					// The lateral yields a row for every outer row (LEFT on
					// the CORRELATION alone, defaults applied), and the
					// written ON then filters — which for an INNER join is
					// exactly a WHERE, so it moves there and is defaulted
					// with everything else.
					jt = "left"
					joinCond = empty.correlationCond
					andIntoWhere(info, empty.onResidual, empty.onResidualExpr)
				case lateralPadOnly:
					jt = "left"
				case lateralNoRepair:
					// Left as written. See lateralEmptyInputPlan.
				}
				right.LateralSubtree = true
				lat := NewJoin(left, right, jt, joinCond)
				// The correlation key this lowering MATERIALIZED is the
				// join's to key on and nobody else's to see: the enclosing
				// query never named it, so a `SELECT *` over the join would
				// publish a column PostgreSQL does not have, under a name no
				// user can spell. The join drops it from its OUTPUT — the one
				// place below every star, derived star and CTE star, so the
				// trim cannot be reached around (ADR-0026 3c).
				lat.HiddenJoinCols = hiddenCols
				lat.LateralEmptyDefaults = emptyDefaults
				if len(emptyDefaults) > 0 {
					lat.LateralPadMarker = empty.padMarker
				}
				items[idx] = lat
			} else {
				rightRef := &plansql.TableRef{
					Name:  join.RightTable,
					Alias: join.RightAlias,
				}
				if join.RightTableRef != nil {
					rightRef = join.RightTableRef
				}
				right, err := resolveTableOrCTE(rightRef, ctes)
				if err != nil {
					return nil, err
				}
				// The join's left is its own FROM item — UNLESS its ON clause
				// references an earlier comma item, which SQL scopes it to see
				// (a JOIN's ON may name any relation to its left in the FROM
				// list). `FROM a, b JOIN c ON a.k = c.k` must put a in the
				// join's left subtree, or the key naming a resolves to nothing
				// and the join answers no rows — the #593/#594 failure mode,
				// reached here by an ON rather than a WHERE. Fold only the
				// referenced case, so a join whose ON stays within its own two
				// sides keeps the later comma items as siblings (the shape the
				// #593/#594 builder fix restored).
				left := items[idx]
				switch {
				case join.Lateral:
					left = crossFold(idx)
				case onRefsEarlierItem(join, items, idx):
					// A QUALIFIED reference to an earlier comma item.
					left = crossFold(idx)
				case isInnerOrCrossJoin(join.Type) && !onConfinedToOwnSides(join, items[idx], right):
					// An INNER/cross join whose ON is NOT provably confined to
					// its own two sides — a bare (unqualified) cross-item key
					// is the common case — may reference an earlier comma item
					// that onRefsEarlierItem cannot see without a qualifier.
					// main folded every comma item in first, which made such
					// ONs resolve; restore that here. OUTER joins deliberately
					// do NOT take this path: folding preceding items into a
					// preserved side changes which rows survive.
					left = crossFold(idx)
				}
				items[idx] = NewJoin(left, right, join.Type, join.Condition)
			}
		}
		for _, item := range items {
			if item == nil {
				continue
			}
			if plan == nil {
				plan = item
			} else {
				plan = NewJoin(plan, item, "cross", "")
			}
		}
	} else if len(info.Tables) > 0 {
		var err error
		plan, err = resolveTableOrCTE(&info.Tables[0], ctes)
		if err != nil {
			return nil, err
		}
		// Comma-join FROM list (see the explicit-join branch above).
		for i := 1; i < len(info.Tables); i++ {
			right, err := resolveTableOrCTE(&info.Tables[i], ctes)
			if err != nil {
				return nil, err
			}
			plan = NewJoin(plan, right, "cross", "")
		}
	} else {
		// Table-less SELECT (e.g., SELECT CURRENT_DATE, SELECT 1+1).
		// Use a single-row dual source so the projection evaluates once.
		plan = &Node{Type: NodeDual}
	}

	return plan, nil
}

// scopeCTEs supplies enclosing CTEs followed by the nested block's OWN WITH items (#684).
// resolveTableOrCTE's first-match walk retains enclosing precedence; ctes[:i]
// lets each item see ONLY earlier definitions, preventing self-recursion (#771).
// This deliberately differs from PostgreSQL when an inner WITH shadows an outer one.
// Do not reverse the search alone: Planner.cteCache is statement-wide and keyed by NAME,
// so the single path would still read the outer materialization while the DAG differed.
// Correct shadowing requires scope-aware cache identity AND reversed lookup;
// TestAWithInsideASubqueryBlockIsInScopeThere pins this divergence.
// See docs/internals/nested-with-scope-precedence.md for the design.
func scopeCTEs(outer, own []plansql.CTEDef) []plansql.CTEDef {
	if len(own) == 0 {
		return outer
	}
	out := make([]plansql.CTEDef, 0, len(outer)+len(own))
	out = append(out, outer...)
	return append(out, own...)
}

// resolveTableOrCTE checks whether a table reference matches a CTE name.
//
// `table` and `ctes` are held by POINTER into the caller's own AST, not by
// value, because a nested block's parse is MEMOIZED on the reference: the
// binder validated that very tree and may have rewritten its terms — a bare
// GROUP BY name it bound to an input column rather than to a SELECT alias
// (#851) — and a copy would be planned from a tree that never heard the
// answer. See plansql's sub_block.go.
func resolveTableOrCTE(table *plansql.TableRef, ctes []plansql.CTEDef) (*Node, error) {
	nameLower := strings.ToLower(table.Name)
	for i := range ctes {
		cte := &ctes[i]
		if cte.Name == nameLower {
			// Recursive CTEs are materialized by the physical planner via
			// fixed-point iteration (materializeRecursiveCTE). Don't expand
			// the body here — that would cause infinite recursion on the
			// self-reference. Just create a tagged scan that buildPipeline
			// resolves from cteCache.
			if cte.Recursive {
				node := NewScan(cte.Name, table.Alias)
				node.CTEName = cte.Name
				// The DEFINITION rides on the reference, so the block this
				// reference sits in can be materialized where it is planned
				// rather than only at the statement root (#1047). See
				// Node.RecursiveCTE.
				node.RecursiveCTE = cte
				return node, nil
			}

			// The CTE body, from the memo the binder validated.
			selectInfo, err := cte.BodySelect()
			if err != nil {
				return nil, fmt.Errorf("parsing CTE %q: %w", cte.Name, err)
			}
			// EARLIER CTEs only. A non-recursive CTE's own name is NOT in
			// scope inside its own body — PostgreSQL's rule and the SQL
			// standard's: `WITH t AS (SELECT * FROM t) SELECT * FROM t`
			// reads the BASE table inside, and a WITH item that is not yet
			// defined is 42P01 there with a DETAIL naming it. Passing the
			// WHOLE list made a CTE that shadows a base table read ITSELF —
			// `WITH decpair AS (SELECT id, a * 2 AS dv FROM decpair)` was
			// `unknown column "a"` on every arm — and, when nothing else
			// answers to the name, re-entered this function without bound and
			// took the PROCESS DOWN with a stack overflow, which Go cannot
			// recover from and any pgwire client can reach (#771).
			//
			// LATER CTEs are excluded for the same reason: PostgreSQL refuses
			// a forward reference rather than resolving it.
			plan, err := BuildFromSelectWithCTEs(selectInfo, scopeCTEs(ctes[:i], selectInfo.CTEs))
			if err != nil {
				return nil, fmt.Errorf("building plan for CTE %q: %w", cte.Name, err)
			}

			// Tag the sub-plan so the physical planner can detect CTE subtrees
			// and materialize multi-referenced CTEs.
			plan.CTEName = cte.Name

			// A CTE reference is a NAMED SCOPE, exactly as a derived table's
			// alias is, and the enclosing query writes `c.col` for its OUTPUT
			// columns. The name a REFERENCE gives it rides alongside, because
			// `FROM c AS x` makes `x` the only spelling the enclosing query
			// can use. physical.subtreeNamesRelation reads both off this node
			// so the DAG's alias resolvers answer `c.gk` the way they answer
			// a derived table's `x.gk` (#653).
			//
			// The scope is NOT stamped onto the scans below, which is what
			// setSubtreeAlias does for a derived table: Node.OuterTableID
			// would then answer `c` for every scan in the body, so two
			// relations comma-joined INSIDE the CTE would share one identity
			// and a predicate spanning them would be attributed to one of
			// them and pushed there (issue #281's q18 CTE spelling).
			if table.Alias != "" && !strings.EqualFold(table.Alias, cte.Name) {
				plan.CTERefAlias = table.Alias
			}

			// The explicit column list renames the CTE's OUTPUT columns. It
			// wraps the finished plan rather than rewriting the body's SELECT
			// aliases, because a CTE's body SQL is re-read by consumers that
			// would not see the rewrite — the cte cache and the physical
			// binder's own view (validate.go's b.ctes).
			// A list over a `SELECT *` body is DEFERRED to the pass that knows
			// the star's width rather than dropped (column_alias_defer.go);
			// every other spelling is renamed here as before.
			if wrapped, ok := deferColumnAliasesOverStar(plan, selectInfo,
				cte.Columns, cte.Name, "WITH query"); ok {
				return wrapped, nil
			}
			renamed, err := applyColumnAliasProject(plan, selectInfo, cte.Columns, cte.Name, "WITH query")
			if err != nil {
				return nil, err
			}
			return renamed, nil
		}
	}
	// Check for derived table (subquery in FROM): name starts with "("
	if strings.HasPrefix(table.Name, "(") {
		// The derived body, from the memo the binder validated.
		selectInfo, err := table.SubSelect()
		if err != nil {
			return nil, fmt.Errorf("parsing derived table: %w", err)
		}
		// The COLUMN-ALIAS LIST renames the derived table's columns
		// positionally: `(SELECT s, n FROM t) AS b(kk, nn)` publishes kk and
		// nn. Only the CTE arm honoured it, so on a derived table the names
		// resolved to nothing and an EXISTS or IN over one answered ZERO ROWS
		// with no error (#613).
		aliasName := table.Alias
		if aliasName == "" {
			aliasName = "subquery"
		}
		if err := applyColumnAliases(selectInfo, table.ColumnAliases, aliasName, "table"); err != nil {
			return nil, err
		}
		plan, err := BuildFromSelectWithCTEs(selectInfo, scopeCTEs(ctes, selectInfo.CTEs))
		if err != nil {
			return nil, fmt.Errorf("building plan for derived table: %w", err)
		}
		// A list over a `SELECT *` body: applyColumnAliases above declined it
		// because the star's width is not countable there, so it is DEFERRED
		// to the pass that knows it (column_alias_defer.go). The wrapper goes
		// on BEFORE the alias stamps, so DerivedAlias lands on the relation
		// the enclosing query actually sees.
		if wrapped, ok := deferColumnAliasesOverStar(plan, selectInfo,
			table.ColumnAliases, aliasName, "table"); ok {
			plan = wrapped
		}
		// Apply alias as table alias on the root scan if available
		if table.Alias != "" {
			setSubtreeAlias(plan, table.Alias)
			// And on the subtree ROOT, the way a CTE records CTEName. The
			// stamp above reaches the scans, which is what a bare reference
			// resolves through; it cannot say what the ENCLOSING query calls
			// this arm when the subtree holds two scans or an inner derived
			// table of its own (#751, #773).
			plan.DerivedAlias = table.Alias
		}
		return plan, nil
	}

	// Check for table function (e.g., read_json('url'), read_csv('path'))
	if table.IsFunction {
		node := NewScan(table.Name, table.Alias)
		node.IsTableFunc = true
		node.FuncName = strings.ToLower(table.Name)
		node.FuncArgs = table.FuncArgs
		node.FuncNamedArgs = table.FuncNamedArgs
		node.WithOrdinality = table.WithOrdinality
		node.FuncColAliases = table.ColumnAliases
		return node, nil
	}

	// A qualified name resolves to the table when the qualifier names this
	// server's own schema (public) or catalog.schema (wadjet.public) — the
	// spelling PostgreSQL clients use by default. Any other qualifier names
	// something this server does not have, and saying so beats scanning a
	// table the statement never asked for.
	if q := strings.ToLower(table.Qualifier); q != "" &&
		q != expr.SessionSchema &&
		q != expr.SessionCatalog+"."+expr.SessionSchema &&
		q != expr.SessionCatalog {
		return nil, fmt.Errorf("unknown schema %q: this server has one schema, %q, in database %q",
			table.Qualifier, expr.SessionSchema, expr.SessionCatalog)
	}

	node := NewScan(table.Name, table.Alias)
	if table.SampleMethod != "" {
		node.SampleMethod = strings.ToUpper(table.SampleMethod)
		pct, _ := strconv.ParseFloat(table.SamplePercent, 64)
		node.SamplePercent = pct
	}
	return node, nil
}

// setSubtreeAlias records a DERIVED TABLE's alias on every Scan in its
// subtree, so that a reference qualified by it (`u.a`) can be recognized as
// naming this scope — see Node.DerivedAliases and physical.derivedScopeBareName.
//
// A scan that answers to a name the QUERY wrote keeps it. The alias inside the
// derived table is what tells one arm of a self-join from the other, and
// overwriting it made `(SELECT n1.n_name AS a, n2.n_name AS b FROM nation n1
// JOIN nation n2 ON …) u` plan as two scans both called `u`, after which
// nothing downstream could say which `n_name` was which — 25 groups where
// PostgreSQL 17 answers 5 (#489).
//
// A scan answering only to its own TABLE NAME still takes the derived alias,
// which keeps every other plan spelled exactly as before: `(SELECT … FROM
// nation) u` scans `nation AS u` today and after this change.
func setSubtreeAlias(n *Node, alias string) {
	// The alias REPLACES a scan's own only when the subtree holds ONE
	// relation. With two or more, every unaliased scan took the derived
	// table's alias and the relations became indistinguishable: `SELECT c
	// FROM (SELECT t0.c1 AS c FROM t0, t2, t1 WHERE t0.c1 IS NOT NULL) x`
	// bound `t0.c1` to whichever relation was planned last and answered t2's
	// values — a silent wrong answer, and NULL for rows the query's own WHERE
	// says are not null (#843). The CTE arm has always declined to stamp for
	// the same reason (#281's q18 spelling); the derived arm is that comment's
	// own hazard, reached by a FROM list rather than a comma join inside a
	// CTE.
	//
	// DerivedAliases is appended either way: it RECORDS that this subtree is
	// reachable as `alias` without claiming the scan is called that, which is
	// what the alias resolvers read (#751, #773).
	single := countScans(n) == 1
	setSubtreeAliasWalk(n, alias, single)
}

func setSubtreeAliasWalk(n *Node, alias string, replace bool) {
	if n.Type == NodeScan {
		if replace && (n.TableAlias == "" || strings.EqualFold(n.TableAlias, n.TableName)) {
			n.TableAlias = alias
		}
		n.DerivedAliases = append(n.DerivedAliases, alias)
	}
	for _, c := range n.Children {
		setSubtreeAliasWalk(c, alias, replace)
	}
}

func countScans(n *Node) int {
	if n == nil {
		return 0
	}
	total := 0
	if n.Type == NodeScan {
		total = 1
	}
	for _, c := range n.Children {
		total += countScans(c)
	}
	return total
}

// applyColumnAliases renames subquery outputs POSITIONALLY: fewer aliases rename
// only a prefix; more aliases than columns refuse with PostgreSQL's 42P10.
// Leave a SELECT list containing a star alone: its width needs later catalog expansion,
// so neither renaming nor arity refusal is knowable here.
// Rewrite the derived table's OWN SELECT aliases, not an extra rename Project.
// CTEs use applyColumnAliasProject because consumers re-read their body SQL
// and would not see a rewritten SELECT list.
// See docs/internals/derived-column-alias-prefix.md for the design.
func applyColumnAliases(info *plansql.SelectInfo, aliases []string, relName, kind string) error {
	if len(aliases) == 0 || info == nil {
		return nil
	}
	// A set operation's output names come from its LEFTMOST arm, which is
	// where PostgreSQL applies the list too.
	cols := info
	for cols.Union != nil {
		cols = cols.Union.Left
	}
	for _, c := range cols.Columns {
		if c.Star {
			return nil
		}
	}
	if len(aliases) > len(cols.Columns) {
		return sqlerr.New("42P10",
			"%s %q has %d columns available but %d columns specified",
			kind, relName, len(cols.Columns), len(aliases))
	}
	for i, name := range aliases {
		cols.Columns[i].Alias = name
	}
	return nil
}

// applyColumnAliasProject is applyColumnAliases' Project-on-top form, for a
// CTE. Both obey the same PostgreSQL arity rules — a shorter list renames a
// PREFIX, a longer one is 42P10 — and differ only in where the rename is
// written and in what PostgreSQL CALLS the relation in the refusal: a derived
// table is a `table`, a CTE is a `WITH query` (measured live).
func applyColumnAliasProject(plan *Node, info *plansql.SelectInfo, aliases []string, relName, kind string) (*Node, error) {
	if len(aliases) == 0 || info == nil {
		return plan, nil
	}
	cols := info
	for cols.Union != nil {
		cols = cols.Union.Left
	}
	for _, c := range cols.Columns {
		if c.Star {
			return plan, nil
		}
	}
	if len(aliases) > len(cols.Columns) {
		return nil, sqlerr.New("42P10",
			"%s %q has %d columns available but %d columns specified",
			kind, relName, len(cols.Columns), len(aliases))
	}
	srcNames := getOutputColNames(cols)
	projections := make([]Projection, 0, len(srcNames))
	for i, srcName := range srcNames {
		outName := srcName
		if i < len(aliases) {
			outName = aliases[i]
		}
		projections = append(projections, Projection{Column: srcName, Alias: outName, Expr: srcName})
	}
	return NewProject(plan, projections), nil
}

// countOutputCols returns the number of output columns from a select info.
func countOutputCols(plan *Node, info *plansql.SelectInfo) int {
	if info != nil {
		return len(info.Columns)
	}
	return 0
}

// getOutputColNames returns the output column names from a select info.
func getOutputColNames(info *plansql.SelectInfo) []string {
	names := make([]string, len(info.Columns))
	for i, col := range info.Columns {
		if col.Alias != "" {
			names[i] = col.Alias
		} else if col.ColumnRef != "" {
			names[i] = col.ColumnRef
		} else {
			names[i] = col.Expr
		}
	}
	return names
}

// buildLateralSubquery decorrelates a LATERAL subquery join by:
// 1. Collecting left-side table aliases
// 2. Parsing the subquery and splitting WHERE into correlated vs local predicates
// 3. Building the inner plan with only local predicates
// 4. Returning the inner plan and the combined join condition
func buildLateralSubquery(outer *plansql.SelectInfo, left *Node, join plansql.JoinInfo, ctes []plansql.CTEDef) (*Node, string, lateralEmptyInput, []string, error) {
	// Collect left-side table aliases to detect correlated references
	leftAliases := collectLogicalAliases(left)

	// Parse the subquery
	inner := join.RightTable[1 : len(join.RightTable)-1]
	parsed, err := plansql.Parse(inner)
	if err != nil {
		return nil, "", lateralEmptyInput{}, nil, fmt.Errorf("parsing LATERAL subquery: %w", err)
	}
	subInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, "", lateralEmptyInput{}, nil, fmt.Errorf("extracting SELECT from LATERAL subquery: %w", err)
	}

	// THE FROM ITEM'S COLUMN-ALIAS LIST renames the body's items POSITIONALLY,
	// before anything else reads them — the correlation lowering below INJECTS
	// items into this list, and a rename applied after that would rename the
	// wrong positions. It is PostgreSQL's rule and the one
	// `plansql.OverlayColumnAliases` states for a derived table; without it
	// `LATERAL (…) l(w)` renamed a column nothing carried and `l.w` answered
	// NULL (round-2 review, P2). A list over a body whose width is not knowable
	// — one with a star — is left alone, and `RefuseUnappliedColumnAliasLists`
	// raises PostgreSQL's 42P10 for it.
	if err := applyLateralItemAliases(subInfo, join); err != nil {
		return nil, "", lateralEmptyInput{}, nil, err
	}

	// Split WHERE clause into correlated and local predicates
	var correlatedParts []string
	var localParts []string
	if subInfo.Where != "" {
		parts := splitANDPredicates(subInfo.Where)
		for _, p := range parts {
			if referencesAliases(p, leftAliases) {
				correlatedParts = append(correlatedParts, p)
			} else {
				localParts = append(localParts, p)
			}
		}
	}

	// Rebuild the inner plan with only local WHERE predicates.
	// Always clear WhereExpr — it's the AST for the original full WHERE and
	// would conflict with the modified Where string.
	subInfo.WhereExpr = nil
	if len(localParts) > 0 {
		subInfo.Where = strings.Join(localParts, " AND ")
	} else {
		subInfo.Where = ""
	}

	// Add correlated inner columns to GROUP BY for every aggregated LATERAL.
	// A block that GROUPS is an aggregate even without a SELECT-list aggregate (#1008),
	// matching BuildFromSelect's hasAgg || len(info.GroupBy) > 0 rule.
	// Otherwise a minted key can name another group's value or type (#615).
	// hasAgg still means the list calls an aggregate: lateralEmptyInputOf separately
	// checks GroupBy because only an UNGROUPED aggregate yields one row on empty input.
	// See docs/internals/lateral-grouping-classification.md for the design.
	hasAgg := false
	for _, col := range subInfo.Columns {
		if col.IsAgg {
			hasAgg = true
			break
		}
	}
	aggregates := hasAgg || len(subInfo.GroupBy) > 0
	// What an EMPTY inner input means for this lateral, decided BEFORE the
	// key injection below adds a GROUP BY of its own. See lateralEmptyInput.
	empty := lateralEmptyInputOf(subInfo, hasAgg, len(correlatedParts) > 0)
	// keyRename maps a correlated inner column to the name the subquery's
	// SELECT list publishes it under, when the two differ. See the comment on
	// lateralPublishedKeyName below.
	var keyRename map[string]string
	// mintedKeys maps a correlated inner column to the HIDDEN SLOT this
	// lowering materialized it into, for the GroupByPublish stamp below.
	var mintedKeys map[string]string
	// injectedSlots are the slots this lowering added as OUTPUT COLUMNS of
	// the lateral — the ones the enclosing query never asked for and must
	// never see. The join drops them from its output (Node.HiddenJoinCols);
	// a slot that only RENAMES a column the list already carries is not one
	// of them, because no column was added.
	var injectedSlots []string
	if len(correlatedParts) > 0 {
		// The inner correlation key must be SELECTED, for non-aggregated laterals too,
		// and GROUPED only when the subquery aggregates (#591, #767 part 2).
		// A key absent from the projection resolves to a degenerate join key; SELECT *
		// or an explicitly published key needs no injection.
		// An alias of another value cannot stand in for the key: a second same-name key
		// would read the wrong value. Use a collision-free hidden slot (#785, ADR-0026 §3a);
		// the earlier alias-shadowing boundary is recorded in the design.
		// Use ONE shared reserved-slot allocator for the lateral, never a per-key namer;
		// seed it with the subquery's bound names and the outer scope (ADR-0026 2a).
		// See docs/internals/lateral-correlation-key-publication.md for the design.
		alloc := plansql.NewSlotAllocator(append(lateralScopeNames(subInfo),
			outerScopeNames(left)...)...)
		var injected []plansql.SelectColumn
		for _, cp := range correlatedParts {
			innerCol := extractInnerColumn(cp, leftAliases)
			if innerCol == "" {
				continue
			}
			// The join keys on the name the subquery PUBLISHES for the key (#767).
			// If the key's SOURCE is selected under an alias, record that published name and
			// rewrite the equality. Another value ALIASED to the key name is not the key;
			// that collision needs a hidden slot (ADR-0026 3a), never name-only substitution.
			// Decide a collision FIRST, before accepting the list's own published key name.
			// A colliding aggregate takes the FULL mint: inject the slot as an output item and
			// key the join on that slot. The aggregate publishes it, the projection carries it,
			// the shuffle can name it, and the join drops it on output on every path.
			// See docs/internals/lateral-published-key-collisions.md for the design.
			collides := aggregates && lateralKeyNameCollides(subInfo.Columns, innerCol)
			published := false
			if pub, ok := lateralPublishedKeyName(subInfo.Columns, innerCol); ok && !collides {
				if keyRename == nil {
					keyRename = map[string]string{}
				}
				keyRename[strings.ToLower(strings.TrimSpace(innerCol))] = pub
				published = true
			}
			if aggregates {
				// Add to GROUP BY if not already present
				found := false
				for _, g := range subInfo.GroupBy {
					if strings.EqualFold(g, innerCol) {
						found = true
						break
					}
				}
				if !found {
					subInfo.GroupBy = append(subInfo.GroupBy, innerCol)
				}
			}
			// The aggregate's key output must not collide with a user aggregate alias (#956).
			// The join's published key name and the aggregate's own key name are distinct
			// contracts: a minted aggregate key must survive the projection and shuffle.
			// A name-only mint stamps the aggregate's published key without injecting another
			// output when the list already carries the key and the join names its publication.
			// Do not rename without a collision: an elided projection exposes the aggregate's
			// name directly and would leave the join/shuffle key absent.
			// A name-only mint is safe only if the list publishes the key under ANOTHER name;
			// same-name ambiguity (PostgreSQL 42702, engine superset) must not become missing rows.
			// See docs/internals/lateral-aggregate-key-output-names.md for the design.
			if published {
				continue
			}
			if !collides && (lateralSelectsColumn(subInfo.Columns, innerCol) ||
				lateralSelectsColumn(injected, innerCol)) {
				// The list publishes the key under its OWN name (or through a
				// star), so nothing needs materializing -- but the promoted
				// equality still names it the way the SUBQUERY wrote it, and
				// what the lateral EMITS is the bare column. On the DAG those
				// are two names: the join's output filter carries `t.g`, the
				// build stream carries `g`, and a fragment whose build side is
				// empty writes a file WITHOUT the qualified copy while one with
				// rows writes it -- `declares 3 columns [k g c] where an earlier
				// file ... declared 4 [k g c t.g]` (ADR-0010, #767's DAG half).
				//
				// Spelling it against the LATERAL's own alias is the same rule
				// the alias case above takes: the join keys on the name the
				// subquery publishes.
				if bare := lateralBareKeyName(innerCol); bare != "" && bare != innerCol {
					if keyRename == nil {
						keyRename = map[string]string{}
					}
					keyRename[strings.ToLower(strings.TrimSpace(innerCol))] = bare
				}
				continue
			}
			col, ok := lateralKeySelectItem(innerCol)
			if !ok {
				continue
			}
			// Publish the injected key under a HIDDEN SLOT (ADR-0026 3a), not its source name:
			// a user alias may hold another value under that name (#956, #767).
			// __key_N belongs to plansql/reserved_slots: query text cannot mint or shadow it.
			// Respell the promoted equality to the slot. An aggregated lateral PUBLISHES
			// that slot while RESOLVING the source column via Node.GroupByPublish.
			// See docs/internals/lateral-hidden-key-publication.md for the design.
			slot, allocated := alloc.Next(plansql.SlotCorrKey)
			if !allocated {
				// An exhausted family has no known SQL. Publishing under the
				// source column's own name is what this did before the slot
				// existed -- right for every shape but the two above, and
				// never a name invented from nothing.
				injected = append(injected, col)
				continue
			}
			col.Alias = slot
			injected = append(injected, col)
			injectedSlots = append(injectedSlots, slot)
			if keyRename == nil {
				keyRename = map[string]string{}
			}
			if mintedKeys == nil {
				mintedKeys = map[string]string{}
			}
			keyRename[strings.ToLower(strings.TrimSpace(innerCol))] = slot
			mintedKeys[strings.ToLower(strings.TrimSpace(innerCol))] = slot
			if aggregates {
				// ONLY over an aggregate. There the projection sits ABOVE the
				// operator that republishes the key under the slot, so a
				// reference to the slot resolves. Without one, the injected
				// item is a SIBLING in the SAME projection and nothing has
				// computed it yet: `CASE WHEN order_id > 1 …` re-spelled to
				// `__key_0` read NULL from the projection's INPUT and every
				// row took the ELSE arm.
				respellKeyRefsToSlot(subInfo, innerCol, slot)
			}
		}
		// The marker a padded row is recognised by: the name the lateral
		// PUBLISHES its correlation key under — the slot where one was
		// minted, the list's own name otherwise. The join keys on it, so it
		// is NULL exactly on the rows the LEFT pad manufactured.
		for _, pub := range keyRename {
			empty.padMarker = pub
			break
		}
		if empty.padMarker == "" && len(injected) > 0 {
			empty.padMarker = injected[0].Alias
		}
		// QUALIFIED by the lateral's own alias where it has one: the join
		// emits a build column that collides with a probe column under
		// `alias.name`, and the PROBE's column of that name is a different
		// one — a user's stored `__key_0` (ADR-0012). The operator matches
		// the qualified spelling first and falls back to a bare name only
		// when exactly one column carries it.
		if empty.padMarker != "" && join.RightAlias != "" &&
			!strings.Contains(empty.padMarker, ".") {
			empty.padMarker = join.RightAlias + "." + empty.padMarker
		}
		// The defaults are built HERE and not in lateralEmptyInputOf because
		// each one is an expression OVER the marker, and the marker is what
		// the loop above just decided.
		if empty.ungroupedAggregate {
			empty.defaults = lateralEmptyDefaults(subInfo, empty.padMarker)
		}
		// Keys first, mirroring the order buildAggregate emits them in, so
		// the projection above stays elidable in the ordinary shape.
		subInfo.Columns = append(injected, subInfo.Columns...)
	}

	if err := refuseDecorrelatedWindow(subInfo, correlatedParts, leftAliases); err != nil {
		return nil, "", lateralEmptyInput{}, nil, err
	}

	right, err := BuildFromSelectWithCTEs(subInfo, scopeCTEs(ctes, subInfo.CTEs))
	if err != nil {
		return nil, "", lateralEmptyInput{}, nil, fmt.Errorf("building LATERAL subquery plan: %w", err)
	}
	// An AGGREGATED lateral groups on the key, and an aggregate publishes a
	// group key under the key's own text -- which is the collision the slot
	// exists to avoid, one operator lower. Record the slot as the key's
	// PUBLISHED name; the aggregate keeps RESOLVING it by the source column.
	markLateralAggregate(right, mintedKeys)
	// A COLUMN-ALIAS LIST over a `SELECT *` BODY is REFUSED, which is what the
	// documentation and ADR-0021 §1l already said and what the code did not do:
	// `applyLateralItemAliases` declined a star body because this layer cannot
	// count it, and DROPPED the list, so `LATERAL (SELECT * FROM i WHERE
	// i.order_id = u.id) l(w)` answered four NULLs for PostgreSQL's 1,2,3,4 —
	// the exact failure `column_alias_defer.go` exists to prevent, on the one
	// FROM item never wired into it (round-2 review, B2).
	//
	// REFUSED and not deferred, unlike the CTE and derived-table arms, because
	// a decorrelated LATERAL JOINS on the column its correlated predicate
	// names: the list renames that column's POSITION like any other, and the
	// join would then key on a name nothing carries and answer no rows. Loud
	// beats plausible, and the sentence now describes the code.
	if err := refuseLateralAliasListOverStar(outer, subInfo, join); err != nil {
		return nil, "", lateralEmptyInput{}, nil, err
	}
	if join.RightAlias != "" {
		setSubtreeAlias(right, join.RightAlias)
	}

	// Normalize correlated equalities so the outer reference is on the left
	// and the inner reference on the right. This ensures parseJoinKeys in
	// the physical planner assigns probe keys (left child = outer table)
	// and build keys (right child = inner table) correctly.
	for i, part := range correlatedParts {
		correlatedParts[i] = normalizeCorrelatedEquality(part, leftAliases)
	}
	// …and then spell the INNER side with the name the subquery publishes,
	// which normalization has just put on the right of the equality.
	for i, part := range correlatedParts {
		correlatedParts[i] = renameCorrelatedInnerRef(part, keyRename, join.RightAlias)
	}

	// The correlation the DECORRELATION produced and the ON the QUERY WROTE
	// are returned apart, because the empty-input repair may keep only the
	// first as the join's condition and has to move the second (see
	// lateralEmptyInput and the caller). Concatenating them is what discarded
	// the written ON.
	corrCond := strings.Join(correlatedParts, " AND ")
	empty.onResidual = ""
	// A condition that FOLDS to a constant TRUE rejects nothing, so it is not
	// a residual at all — it is `ON true` under another spelling, and the
	// repair that makes the join LEFT on the correlation alone IS its
	// semantics. Testing the TEXT for "true" made `ON 1 = 1` a residual, which
	// refused a query PostgreSQL answers `Carol, 0`, while `ON true` answered
	// it; both are one constant and they fold the same way.
	if join.Condition != "" && !onFoldsToTrue(join.CondExpr, join.Condition) {
		empty.onResidual = join.Condition
		empty.onResidualExpr = join.CondExpr
	}
	joinCond := corrCond
	if empty.onResidual != "" {
		if joinCond != "" {
			joinCond += " AND "
		}
		joinCond += empty.onResidual
	}
	empty.correlationCond = corrCond

	return right, joinCond, empty, injectedSlots, nil
}

// collectLogicalAliases collects table names and aliases from scan nodes.
func collectLogicalAliases(n *Node) map[string]bool {
	aliases := make(map[string]bool)
	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeScan {
			// Every name an outer scope may qualify with, derived-table
			// aliases included — this set decides whether a LATERAL
			// subquery's predicate references the left side (#489).
			for _, name := range n.ScopeNames() {
				aliases[strings.ToLower(name)] = true
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(n)
	return aliases
}

// splitANDPredicates splits a WHERE expression on top-level AND boundaries,
// respecting parentheses nesting.
func splitANDPredicates(where string) []string {
	var parts []string
	depth := 0
	inStr := false
	start := 0
	upper := strings.ToUpper(where)

	for i := 0; i < len(where); i++ {
		ch := where[i]
		if inStr {
			if ch == '\'' {
				if i+1 < len(where) && where[i+1] == '\'' {
					i++
				} else {
					inStr = false
				}
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		if depth == 0 && i+4 <= len(upper) && upper[i:i+3] == "AND" {
			// Ensure it's a word boundary (not part of an identifier)
			before := i == 0 || where[i-1] == ' ' || where[i-1] == ')'
			after := i+3 >= len(where) || where[i+3] == ' ' || where[i+3] == '('
			if before && after {
				parts = append(parts, strings.TrimSpace(where[start:i]))
				start = i + 3
			}
		}
	}
	if start < len(where) {
		parts = append(parts, strings.TrimSpace(where[start:]))
	}
	return parts
}

// referencesAliases returns true if the expression contains a qualified
// column reference (alias.column) where alias is in the given set.
func referencesAliases(expr string, aliases map[string]bool) bool {
	lower := strings.ToLower(expr)
	for alias := range aliases {
		// Look for alias.column pattern, ensuring it's a word boundary
		// (not a substring of an identifier like "o" matching "order_id")
		prefix := alias + "."
		idx := strings.Index(lower, prefix)
		if idx >= 0 {
			// Check that the character before is not alphanumeric/underscore
			if idx == 0 || !isIdentChar(lower[idx-1]) {
				return true
			}
		}
	}
	return false
}

// isIdentChar returns true if the byte is a valid identifier character.
func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// lateralKeySelectItem builds the select item that publishes a correlation key
// injected into an aggregated LATERAL subquery's GROUP BY. The expression is
// parsed rather than assembled so the item carries the same AST a written
// `SELECT order_id` would, which is what the projection and the join key
// resolution below it both read.
func lateralKeySelectItem(innerCol string) (plansql.SelectColumn, bool) {
	node, err := plansql.ParseExpression(innerCol)
	if err != nil || node == nil {
		return plansql.SelectColumn{}, false
	}
	col := plansql.SelectColumn{Expr: node.String(), ASTExpr: node}
	if ref, ok := node.(*plansql.ColRef); ok {
		col.ColumnRef = ref.Column
		col.TableRef = ref.Table
	}
	return col, true
}

// lateralKeyNameCollides reports whether the name an AGGREGATE would publish
// the correlation key under — the source column's bare text, which is
// exec.PublishedGroupKeyNames' qualifier strip — is also the output name of
// an AGGREGATE in the same SELECT list.
//
// That is #956's shape in the spelling where the list carries the key itself:
// `SELECT t.g AS gk, MAX(t.id) AS g … WHERE t.g = d.k GROUP BY t.g` makes the
// aggregate emit two columns called `g`, and the projection above resolves by
// name.
func lateralKeyNameCollides(cols []plansql.SelectColumn, innerCol string) bool {
	bare := lateralBareKeyName(innerCol)
	if bare == "" {
		bare = strings.TrimSpace(innerCol)
	}
	for _, c := range cols {
		if !c.IsAgg && !c.IsWindow {
			continue
		}
		if strings.EqualFold(plansql.OutputColumnName(c), bare) {
			return true
		}
	}
	return false
}

// refuseDecorrelatedWindow refuses a LATERAL whose window frame decorrelation changes.
// Moving the correlated filter into the join makes the window see the WHOLE inner relation.
// Allow only windows whose PARTITION BY carries the correlation key, so each row
// still reads exactly its correlated group; otherwise refuse 0A000.
// Per-outer-row window evaluation is not expressed by this lowering.
// See docs/internals/decorrelated-window-frame-boundary.md for the design.
func refuseDecorrelatedWindow(info *plansql.SelectInfo, correlatedParts []string, leftAliases map[string]bool) error {
	if info == nil || len(correlatedParts) == 0 {
		return nil
	}
	keys := make(map[string]bool, len(correlatedParts))
	for _, cp := range correlatedParts {
		inner := extractInnerColumn(cp, leftAliases)
		if inner == "" {
			continue
		}
		keys[strings.ToLower(strings.TrimSpace(inner))] = true
		if bare := lateralBareKeyName(inner); bare != "" {
			keys[strings.ToLower(bare)] = true
		}
	}
	for _, c := range info.Columns {
		if !c.IsWindow || c.WindowSpec == nil {
			continue
		}
		partitioned := false
		for _, pb := range c.WindowSpec.PartitionBy {
			p := strings.ToLower(strings.TrimSpace(pb))
			if keys[p] {
				partitioned = true
				break
			}
			if bare := lateralBareKeyName(pb); bare != "" && keys[strings.ToLower(bare)] {
				partitioned = true
				break
			}
		}
		if partitioned {
			continue
		}
		return sqlerr.New("0A000",
			"window function %q inside a LATERAL subquery correlated on %s is not supported: "+
				"the correlation is evaluated as a join, so the window would be computed over the "+
				"whole inner relation rather than over the correlated rows, which is a different "+
				"answer — add the correlation column to the window's PARTITION BY, or compute the "+
				"window outside the LATERAL",
			plansql.WindowOutputName(c), strings.Join(sortedKeyNames(keys), ", "))
	}
	return nil
}

// sortedKeyNames renders a correlation-key set in a stable order for a message.
func sortedKeyNames(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// lateralBareKeyName is the unqualified column a correlated equality's inner
// side names, or "" when it names something this cannot take apart.
func lateralBareKeyName(innerCol string) string {
	node, err := plansql.ParseExpression(innerCol)
	if err != nil || node == nil {
		return ""
	}
	ref, ok := node.(*plansql.ColRef)
	if !ok {
		return ""
	}
	return ref.Column
}

// respellKeyRefsToSlot binds the subquery's own key references to the aggregate's
// published SLOT, retaining each site's text identity and position.
// Walk the block: SELECT items, HAVING and its ORDER BY read the output and take
// the slot; WHERE and GROUP BY read the INPUT and retain the source column.
// RewriteExpr reaches references in CASE, casts and functions, stopping at aggregates.
// Do not walk WINDOW specs: refuseDecorrelatedWindow must read the original
// PARTITION BY to recognize the correlation key and decide frame preservation.
// Plant ColRef.Slot provenance to distinguish planner references from user names
// (ADR-0025 rule 1).
// See docs/internals/lateral-key-slot-reference-scope.md for the design.
func respellKeyRefsToSlot(info *plansql.SelectInfo, innerCol, slot string) {
	bare := lateralBareKeyName(innerCol)
	if bare == "" {
		bare = strings.TrimSpace(innerCol)
	}
	respell := func(n plansql.Node) plansql.Node {
		if n == nil {
			return nil
		}
		return plansql.RewriteExpr(n, func(x plansql.Node) (plansql.Node, bool) {
			ref, ok := x.(*plansql.ColRef)
			if !ok || !strings.EqualFold(ref.Column, bare) {
				return nil, false
			}
			return &plansql.ColRef{Column: slot, Slot: true}, true
		})
	}
	for i := range info.Columns {
		c := &info.Columns[i]
		if c.Star || c.IsAgg || c.IsWindow || c.ASTExpr == nil {
			continue
		}
		rewritten := respell(c.ASTExpr)
		if rewritten == c.ASTExpr {
			continue
		}
		c.ASTExpr = rewritten
		c.Expr = rewritten.String()
		if ref, ok := rewritten.(*plansql.ColRef); ok {
			c.ColumnRef = ref.Column
			c.TableRef = ""
		}
	}
	// HAVING and the subquery's own ORDER BY run ABOVE the aggregate, over
	// what it PUBLISHES — so they take the slot with the select list. The
	// WHERE and the GROUP BY do not: both are resolved against the
	// aggregate's INPUT, where the source column is the only name there is.
	if info.HavingExpr != nil {
		if rewritten := respell(info.HavingExpr); rewritten != info.HavingExpr {
			info.HavingExpr = rewritten
			info.Having = rewritten.String()
		}
	}
	for i := range info.OrderBy {
		if info.OrderBy[i].Expr == nil {
			continue
		}
		rewritten := respell(info.OrderBy[i].Expr)
		if rewritten == info.OrderBy[i].Expr {
			continue
		}
		info.OrderBy[i].Expr = rewritten
		info.OrderBy[i].Column = rewritten.String()
	}
}

// outerScopeNames lists the names the OUTER side of a lateral join already
// binds, so a minted slot never takes one of them.
//
// It is the second half of the seed, and it exists because a name is only a
// slot if nothing else answers to it. The lateral's own text was seeded from
// the start; the outer side was not, and an outer relation may STORE a column
// called `__key_0` — reading is not minting, so the reservation admits one
// (ADR-0012). The join drops what it MINTED by identity rather than by name,
// so a stored one is safe either way; this makes the two names differ in the
// first place, which is the stronger property.
//
// What it can see, walking the built outer subtree: each scan's ScanColumns
// where the physical annotation has already run, every projection's alias and
// column, and each scan's scope names. Where the annotation has NOT run — the
// ordinary case at this point in the build — the scan's stored columns are
// invisible here, and the identity-based drop is what covers that.
func outerScopeNames(n *Node) []string {
	if n == nil {
		return nil
	}
	var out []string
	var walk func(*Node)
	walk = func(cur *Node) {
		if cur == nil {
			return
		}
		switch cur.Type {
		case NodeScan:
			out = append(out, cur.ScanColumns...)
			out = append(out, cur.ScopeNames()...)
		case NodeProject:
			for _, pr := range cur.Projections {
				if pr.Alias != "" {
					out = append(out, pr.Alias)
				}
				if pr.Column != "" {
					out = append(out, pr.Column)
				}
			}
		}
		for _, c := range cur.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}

// lateralScopeNames seeds the key allocator with SELECT aliases/references and GROUP BY.
// It runs before catalog annotation and cannot see unreferenced stored __key_0 columns;
// reading a stored reserved name is allowed (ADR-0012).
// Such a column can share the output only if named in SELECT (then it is seeded),
// or selected by star (then lateralSelectsColumn reports the key already published
// and nothing is injected or minted). Other lists cannot publish the collision.
// See docs/internals/lateral-slot-allocator-visible-names.md for the design.
func lateralScopeNames(info *plansql.SelectInfo) []string {
	if info == nil {
		return nil
	}
	out := make([]string, 0, len(info.Columns)*2+len(info.GroupBy))
	for _, c := range info.Columns {
		if c.Alias != "" {
			out = append(out, c.Alias)
		}
		if c.ColumnRef != "" {
			out = append(out, c.ColumnRef)
		}
		if e := strings.TrimSpace(c.Expr); e != "" {
			out = append(out, e)
		}
	}
	out = append(out, info.GroupBy...)
	return out
}

// markLateralAggregate marks the aggregate a decorrelated LATERAL built and
// records, on it, the hidden slot each minted correlation key is PUBLISHED
// under.
//
// The aggregate keeps RESOLVING the key by the source column it groups on —
// `GroupBy` is untouched — and publishes it under the slot, which is
// ADR-0026 §2's pair of names in the direction §3a asks for. Without it the
// aggregate emits the key under the source column's stripped name, beside an
// aggregate output the query aliased the same way, and the first match wins
// (#956).
//
// Only the OUTERMOST aggregate of the subquery is stamped: the walk descends
// through the nodes that leave an aggregate's own output visible and stops at
// the first one it finds. A nested block's aggregate is a different relation
// whose key of that name is a different value, and stamping it would publish
// somebody else's column under this lateral's slot.
func markLateralAggregate(n *Node, minted map[string]string) {
	if n == nil {
		return
	}
	switch n.Type {
	case NodeAggregate:
		// The mark is made whether or not a key was minted: it says WHOSE
		// aggregate this is, and the stage's naming rule is scoped by that.
		n.LateralAggregate = true
		if len(minted) == 0 {
			return
		}
		if len(n.GroupByPublish) < len(n.GroupBy) {
			grown := make([]string, len(n.GroupBy))
			copy(grown, n.GroupByPublish)
			n.GroupByPublish = grown
		}
		for i, gb := range n.GroupBy {
			if slot, ok := minted[strings.ToLower(strings.TrimSpace(gb))]; ok {
				n.GroupByPublish[i] = slot
			}
		}
		return
	case NodeProject, NodeFilter, NodeSort, NodeLimit, NodeDistinct, NodeWindow:
		if len(n.Children) > 0 {
			markLateralAggregate(n.Children[0], minted)
		}
	}
}

// lateralPublishedKeyName returns the name a LATERAL subquery's SELECT list
// publishes innerCol under, when that name is NOT innerCol's own.
//
// The decorrelation promotes the correlated equality into the join condition,
// where it names the INNER column — so the join can only key on it if the
// subquery's output carries a column of that name. `SELECT t.g AS gg …` does
// carry the value and does not carry the name, which is the gap this closes.
//
// ok=false covers three things, all of which need no rewrite: the list does
// not publish the key at all (lateralSelectsColumn's caller injects it), it
// publishes it under its own name, or it publishes it through a star.
//
// The item's SOURCE has to be the key. An item whose ALIAS merely matches the
// key's name (`SELECT amount AS order_id`) publishes a DIFFERENT value under
// that name, and keying on it would answer a plausible wrong number where the
// engine answers an obvious zero today — protocol item 8's rule, and the
// boundary pinned as `boundary_inner_alias_shadowing_the_key_answers_nothing`.
func lateralPublishedKeyName(cols []plansql.SelectColumn, innerCol string) (string, bool) {
	inner := strings.TrimSpace(innerCol)
	bare := inner
	if node, err := plansql.ParseExpression(innerCol); err == nil {
		if ref, ok := node.(*plansql.ColRef); ok {
			bare = ref.Column
		}
	}
	for _, c := range cols {
		if c.Star || c.IsAgg || c.Alias == "" {
			continue
		}
		sourceIsKey := strings.EqualFold(strings.TrimSpace(c.Expr), inner) ||
			(c.ColumnRef != "" && (strings.EqualFold(c.ColumnRef, inner) ||
				strings.EqualFold(c.ColumnRef, bare)))
		if !sourceIsKey {
			continue
		}
		if strings.EqualFold(c.Alias, bare) {
			return "", false // published under its own name
		}
		return c.Alias, true
	}
	return "", false
}

// renameCorrelatedInnerRef rewrites a NORMALIZED correlated equality's inner
// side — the right of the `=`, which normalizeCorrelatedEquality has just put
// there — to the name the subquery publishes, qualified by the lateral's own
// alias when it has one so the reference cannot bind to an outer column of the
// same name.
func renameCorrelatedInnerRef(part string, keyRename map[string]string, rightAlias string) string {
	if len(keyRename) == 0 {
		return part
	}
	eq := strings.Index(part, "=")
	if eq <= 0 || part[eq-1] == '!' || part[eq-1] == '<' || part[eq-1] == '>' {
		return part
	}
	inner := strings.TrimSpace(part[eq+1:])
	pub, ok := keyRename[strings.ToLower(inner)]
	if !ok {
		return part
	}
	if rightAlias != "" {
		pub = rightAlias + "." + pub
	}
	return strings.TrimSpace(part[:eq]) + " = " + pub
}

// lateralSelectsColumn reports whether a subquery's select list already
// publishes innerCol, so an injected key is never selected twice. A star
// publishes everything the subquery can see, the key included.
func lateralSelectsColumn(cols []plansql.SelectColumn, innerCol string) bool {
	bare := strings.TrimSpace(innerCol)
	if node, err := plansql.ParseExpression(innerCol); err == nil {
		if ref, ok := node.(*plansql.ColRef); ok {
			bare = ref.Column
		}
	}
	for _, c := range cols {
		if c.Star {
			return true
		}
		if strings.EqualFold(strings.TrimSpace(c.Expr), strings.TrimSpace(innerCol)) {
			return true
		}
		// The item's SOURCE, never its ALIAS. A NAME is not a value: an item
		// that merely answers to the key's name publishes whatever IT reads —
		// `MAX(t.id) AS g` beside `WHERE t.g = d.k` (#956) and
		// `amount AS order_id` beside `WHERE order_id = o.id` (#767's mirror)
		// both looked like they published the key, so no key was materialized
		// and the join had nothing to key on. Both are minted into a hidden
		// slot by the caller now.
		if !c.IsAgg && c.ColumnRef != "" && strings.EqualFold(c.ColumnRef, bare) {
			return true
		}
	}
	return false
}

// extractInnerColumn extracts the inner (non-outer) column from a correlated
// equality predicate like "order_id = o.id". Returns the unqualified inner column.
func extractInnerColumn(expr string, outerAliases map[string]bool) string {
	eqIdx := strings.Index(expr, "=")
	if eqIdx < 0 {
		return ""
	}
	if eqIdx > 0 && (expr[eqIdx-1] == '!' || expr[eqIdx-1] == '<' || expr[eqIdx-1] == '>') {
		return ""
	}
	left := strings.TrimSpace(expr[:eqIdx])
	right := strings.TrimSpace(expr[eqIdx+1:])

	if referencesAliases(left, outerAliases) {
		return right
	}
	if referencesAliases(right, outerAliases) {
		return left
	}
	return ""
}

// normalizeCorrelatedEquality ensures the outer reference is on the left
// side of an equality predicate. This is needed so parseJoinKeys assigns
// probe keys to the outer (left) child and build keys to the inner (right).
func normalizeCorrelatedEquality(expr string, outerAliases map[string]bool) string {
	eqIdx := strings.Index(expr, "=")
	if eqIdx < 0 {
		return expr
	}
	// Skip != and >=, <=
	if eqIdx > 0 && (expr[eqIdx-1] == '!' || expr[eqIdx-1] == '<' || expr[eqIdx-1] == '>') {
		return expr
	}

	left := strings.TrimSpace(expr[:eqIdx])
	right := strings.TrimSpace(expr[eqIdx+1:])

	leftIsOuter := referencesAliases(left, outerAliases)
	rightIsOuter := referencesAliases(right, outerAliases)

	// If inner is on left and outer is on right, swap
	if rightIsOuter && !leftIsOuter {
		return right + " = " + left
	}
	return expr
}

// onRefsEarlierItem reports whether an explicit join's ON clause references a
// relation belonging to a FROM item BEFORE the one it extends. SQL scopes a
// join's ON over every FROM item to its left, so `FROM a, b JOIN c ON a.k =
// c.k` is legal and a must be in the join's left input. The builder attaches a
// join to the single item it follows by default (the #593/#594 fix), so this
// is what pulls the earlier items back in when the ON actually needs them —
// and only then, leaving the common case (an ON within its own two sides) with
// its later comma items still siblings.
//
// Detection is by relation QUALIFIER, which is all that is resolvable at build
// time: scans carry their alias/name here, but not yet their columns
// (AnnotateScanColumns runs in the physical planner). A BARE cross-item ON
// carries no qualifier for this to match, so it is handled separately: an
// inner/cross join whose ON is not provably confined to its own two sides
// folds its preceding items in via onConfinedToOwnSides, which is main's
// original fold-comma-first behaviour restored for exactly that case.
func onRefsEarlierItem(join plansql.JoinInfo, items []*Node, idx int) bool {
	if idx <= 0 {
		return false
	}
	quals := condQualifiers(join)
	if len(quals) == 0 {
		return false
	}
	for i := 0; i < idx; i++ {
		if items[i] == nil {
			continue
		}
		for name := range liftRelationAliases(items[i]) {
			if quals[name] {
				return true
			}
		}
	}
	return false
}

// isInnerOrCrossJoin reports whether a join type is an inner or cross join,
// for which folding preceding comma items into the left input is always
// semantically safe (a cross join commutes with an inner join, so the extra
// relations only widen the left before the same equi-join runs). Outer joins
// are excluded: which rows an outer join preserves depends on what its left
// input IS, so folding earlier items in would change the answer.
func isInnerOrCrossJoin(joinType string) bool {
	jt := strings.ToLower(strings.TrimSpace(joinType))
	if jt == "" || jt == "join" || jt == "inner" || jt == "inner join" {
		return true
	}
	return strings.Contains(jt, "cross")
}

// onConfinedToOwnSides reports whether every column reference in a join's ON
// clause is QUALIFIED by a relation the join itself exposes — its own FROM
// item (ownItem) or its right table (right). When it is, the ON cannot name an
// earlier comma item and the join needs no preceding items folded in. When it
// is not — a bare column, or a qualifier naming neither side — the reference
// MIGHT be an earlier comma item, which at build time (before scan columns are
// annotated) cannot be ruled out, so the caller folds conservatively. It
// returns false for an unparseable ON, which also folds: the safe direction,
// since folding an inner/cross join's left never changes its answer.
func onConfinedToOwnSides(join plansql.JoinInfo, ownItem, right *Node) bool {
	expr := join.CondExpr
	if expr == nil {
		expr = tryParseExpr(join.Condition)
	}
	if expr == nil {
		return false
	}
	own := liftRelationAliases(ownItem)
	for a := range liftRelationAliases(right) {
		own[a] = true
	}
	confined := true
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if !confined || n == nil {
			return
		}
		switch e := n.(type) {
		case *plansql.ColRef:
			if e.Table == "" || !own[strings.ToLower(e.Table)] {
				confined = false
			}
		case *plansql.CmpExpr:
			walk(e.Left)
			walk(e.Right)
		case *plansql.AndNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.OrNode:
			walk(e.Left)
			walk(e.Right)
		case *plansql.BinaryOp:
			walk(e.Left)
			walk(e.Right)
		case *plansql.UnaryOp:
			walk(e.Inner)
		case *plansql.NotNode:
			walk(e.Inner)
		case *plansql.ParenNode:
			walk(e.Inner)
		case *plansql.CastNode:
			walk(e.Inner)
		case *plansql.FuncCallNode:
			for _, a := range e.Args {
				walk(a)
			}
		case *plansql.InExpr:
			walk(e.Left)
			for _, v := range e.Values {
				walk(v)
			}
		case *plansql.BetweenExpr:
			walk(e.Left)
			walk(e.Low)
			walk(e.High)
		case *plansql.LikeExpr:
			walk(e.Left)
			walk(e.Pattern)
		case *plansql.IsExpr:
			walk(e.Left)
		case *plansql.CaseNode:
			walk(e.Subject)
			for _, w := range e.Whens {
				walk(w.Cond)
				walk(w.Result)
			}
			walk(e.Else)
		case *plansql.Lit, *plansql.IntervalLit, nil:
			// literals reference no relation
		default:
			// An ON node this walk does not model might hide a bare or
			// foreign reference; fold rather than assume it is confined.
			confined = false
		}
	}
	walk(expr)
	return confined
}

// condQualifiers returns the lower-cased relation qualifiers a join condition
// references — {"a", "c"} for `a.k = c.k`. It reads the parsed AST when the
// parser kept one and falls back to re-parsing the text. It reuses
// collectASTColumnRefs — which already emits every qualified reference as a
// lower-cased "table.column" entry — and keeps the table halves, so it covers
// every expression node that walker does (AND/OR/CASE/func/IN/…) rather than a
// hand-picked subset.
func condQualifiers(join plansql.JoinInfo) map[string]bool {
	expr := join.CondExpr
	if expr == nil {
		expr = tryParseExpr(join.Condition)
	}
	refs := make(map[string]bool, 8)
	collectASTColumnRefs(expr, refs)
	out := make(map[string]bool, 4)
	for ref := range refs {
		if i := strings.IndexByte(ref, '.'); i > 0 {
			out[ref[:i]] = true
		}
	}
	return out
}

// computedGroupKeyRefs maps the identity of each COMPUTED group key to the
// column name the aggregate publishes it under, for rewriting an expression
// written above the aggregate into one it can evaluate.
//
// Bare column keys are left out on purpose. Their value is published under
// the input column's own name, so a reference to one already resolves; a ROW
// FIELD PATH is a *ColRef too and resolves through the same dotted spelling
// on both engines. Rewriting those would only re-point a resolution that
// works.

// aggOutputNameIsShared detects an output name shared with a group key or another
// aggregate; HAVING must not reuse it because ColumnIndex reads the FIRST match (#785).
// Compare keys as the resolver does: exact spelling, then a qualified key's BARE part
// (#968, ADR-0026 §3a). A whitespace-only cleanExpr comparison misses qualified collisions.
// A conservative true costs one extra computation under __having_N, never wrong rows;
// use the bare test rather than approximating exec.PublishedGroupKeyNames.
// See docs/internals/having-shared-aggregate-name-test.md for the design.
func aggOutputNameIsShared(out string, groupBy []string, aggs []AggExpr) bool {
	if out == "" {
		return false
	}
	for _, gb := range groupBy {
		key := cleanExpr(gb)
		if strings.EqualFold(key, out) {
			return true
		}
		if _, bare, ok := plansql.SplitIdentRef(key); ok && bare != "" &&
			strings.EqualFold(bare, out) {
			return true
		}
	}
	n := 0
	for _, a := range aggs {
		if strings.EqualFold(a.OutputCol, out) {
			n++
		}
	}
	return n > 1
}

func computedGroupKeyRefs(agg *Node) map[string]string {
	if agg == nil || len(agg.GroupByExprs) != len(agg.GroupBy) {
		return nil
	}
	var refs map[string]string
	for i, e := range agg.GroupByExprs {
		if e == nil {
			continue
		}
		if _, isRef := e.(*plansql.ColRef); isRef {
			continue
		}
		if _, isLit := e.(*plansql.Lit); isLit {
			continue
		}
		id := plansql.ExprIdentity(e)
		if id == "" {
			continue
		}
		if refs == nil {
			refs = make(map[string]string, len(agg.GroupByExprs))
		}
		if _, taken := refs[id]; !taken {
			refs[id] = agg.GroupBy[i]
		}

	}
	return refs
}
