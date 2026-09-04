package logical

import (
	"fmt"
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
				// Reuse an identical aggregate the SELECT list already
				// computes, so HAVING references its output column instead
				// of adding a second copy under a synthetic name. Match on
				// the NORMALIZED fields rather than on rendered text: the
				// old key rebuilt "count()" from an AggExpr whose InputCol
				// the normalization above had already emptied, and compared
				// it against the AST's "count(*)", so `SELECT a, COUNT(*)
				// AS c ... HAVING COUNT(*) > 1` never matched — it counted
				// twice and leaked the second count as __having_N.
				found := false
				if len(hAgg.Args) <= 1 {
					for _, existing := range aggs {
						if existing.InputCol2 != "" || existing.Separator != "" || existing.Percentile != 0 {
							continue
						}
						if strings.EqualFold(existing.Func, funcName) &&
							strings.EqualFold(existing.InputCol, aggInputCol) &&
							existing.Distinct == hAgg.Distinct {
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
				winExprs[i].OrderBy[j].Column = respellOverAggregate(
					winExprs[i].OrderBy[j].Column, winAggRefs, groupKeyRefs)
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
					Expr:       src,
					Alias:      name,
					Column:     src,
					SlotSource: src,
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

func isStarOnly(cols []plansql.SelectColumn) bool {
	return len(cols) == 1 && cols[0].Star
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
			if existing.InputCol2 != "" || existing.Separator != "" || existing.Percentile != 0 {
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
	if len(info.OrderBy) > 0 {
		var orderExprs []OrderExpr
		for _, ob := range info.OrderBy {
			orderExprs = append(orderExprs, OrderExpr{
				Column:     cleanExpr(ob.Column),
				Desc:       ob.Desc,
				NullsFirst: ob.NullsFirst,
			})
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
				right, joinCond, empty, err := buildLateralSubquery(left, join, ctes)
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
				switch lateralEmptyInputPlan(jt, empty,
					lateralJoinNullExtendsAfter(info.Joins, joinIdx)) {
				case lateralPadThenFilter:
					// The lateral yields a row for every outer row (LEFT on
					// the CORRELATION alone, defaults applied), and the
					// written ON then filters — which for an INNER join is
					// exactly a WHERE, so it moves there and is defaulted
					// with everything else.
					jt = "left"
					joinCond = empty.correlationCond
					andIntoWhere(info, empty.onResidual, empty.onResidualExpr)
					applyLateralEmptyInputDefaults(info, join.RightAlias, empty)
				case lateralPadOnly:
					jt = "left"
					applyLateralEmptyInputDefaults(info, join.RightAlias, empty)
				case lateralNoRepair:
					// Left as written. See lateralEmptyInputPlan.
				}
				items[idx] = NewJoin(left, right, jt, joinCond)
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
			plan, err := BuildFromSelectWithCTEs(selectInfo, ctes[:i])
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
		plan, err := BuildFromSelectWithCTEs(selectInfo, ctes)
		if err != nil {
			return nil, fmt.Errorf("building plan for derived table: %w", err)
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

// applyColumnAliases renames a subquery's output columns positionally, the way
// `(SELECT …) AS b(kk, nn)` and `WITH c(kk, nn) AS (…)` do.
//
// PostgreSQL's arity rules, measured live on postgres:17-alpine over a
// two-column derived table:
//
//	AS b(kk, nn)         → columns kk, nn
//	AS b(kk)             → columns kk, n — FEWER aliases rename a PREFIX
//	AS b(kk, nn, extra)  → 42P10 `table "b" has 2 columns available but
//	                       3 columns specified`
//
// The CTE arm used to apply the list only when the counts matched EXACTLY and
// drop it in silence otherwise, so both of the mismatches above answered under
// the wrong names.
//
// A subquery whose SELECT list carries a `*` is left alone: the star's width
// is a catalog question this layer cannot ask (ExpandStarProjections answers
// it later), so neither the rename nor the arity refusal can be made
// truthfully here. Guessing would rename the wrong columns, which is a wrong
// answer rather than a missing one.
//
// It rewrites the DERIVED TABLE's own SELECT ALIASES rather than stacking a
// rename Project above the finished plan. `AS b(kk)` means exactly what
// `SELECT s AS kk` means, and the spelling that already worked on every path
// is the one with the alias inside. A CTE takes applyColumnAliasProject
// instead, because its body SQL is re-read by consumers a rewritten SELECT
// list would be invisible to.
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
func buildLateralSubquery(left *Node, join plansql.JoinInfo, ctes []plansql.CTEDef) (*Node, string, lateralEmptyInput, error) {
	// Collect left-side table aliases to detect correlated references
	leftAliases := collectLogicalAliases(left)

	// Parse the subquery
	inner := join.RightTable[1 : len(join.RightTable)-1]
	parsed, err := plansql.Parse(inner)
	if err != nil {
		return nil, "", lateralEmptyInput{}, fmt.Errorf("parsing LATERAL subquery: %w", err)
	}
	subInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, "", lateralEmptyInput{}, fmt.Errorf("extracting SELECT from LATERAL subquery: %w", err)
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

	// For aggregated LATERAL subqueries, add the correlated inner column
	// to GROUP BY so the aggregate applies per-group rather than globally.
	// e.g., SELECT COUNT(*) FROM t WHERE t.id = o.id
	//     → SELECT t.id, COUNT(*) FROM t GROUP BY t.id
	hasAgg := false
	for _, col := range subInfo.Columns {
		if col.IsAgg {
			hasAgg = true
			break
		}
	}
	// What an EMPTY inner input means for this lateral, decided BEFORE the
	// key injection below adds a GROUP BY of its own. See lateralEmptyInput.
	empty := lateralEmptyInputOf(subInfo, hasAgg, len(correlatedParts) > 0)
	if len(correlatedParts) > 0 {
		// The key must be SELECTED — and, for an aggregated subquery, grouped.
		// The rewrite above promotes the correlated equality into the join
		// condition, so the join keys on the inner column — and a column the
		// subquery's select list does not publish is not there to key on. It
		// used to be there by accident: buildProject elided every projection
		// over an aggregate, so the aggregate's raw output (keys first, then
		// aggregates) reached the join and the key leaked through. Once that
		// elision became conditional on the shapes matching (c55492d1, #591)
		// the projection was kept, the key was genuinely gone, and
		// exec.HashJoin resolved its build key to index -1 — which it treats
		// as an unresolvable-but-matchable null key, so every build row
		// serialized the same degenerate key, nothing equalled the probe's
		// real value, and the query answered zero rows (a LEFT JOIN LATERAL
		// answered every aggregate NULL, which is worse).
		//
		// That reasoning never depended on the aggregate, but the gate did:
		// it read `hasAgg && …`, so a NON-aggregated LATERAL whose projection
		// narrows away the correlated column got no injection and hit the
		// identical degenerate key. `JOIN LATERAL (SELECT amount FROM item
		// WHERE order_id = o.id)` answered ZERO rows and its LEFT twin
		// answered every amount NULL, on the single-process path, where
		// PostgreSQL 17 answers four rows and five (#767 part 2). It was
		// invisible because every LATERAL test in the tree writes `SELECT *`,
		// which publishes the key by definition — and lateralSelectsColumn
		// still declines to inject there and where the list names the key
		// under its own name, so the controls are unchanged.
		//
		// It declines in one case where it should not, and that is a stated
		// boundary rather than an oversight: it matches the key's name
		// against a select item's ALIAS as well as its source column, so
		// `SELECT amount AS order_id` looks like it publishes `order_id` and
		// gets no injection — zero rows for PostgreSQL's four, here and at
		// this arc's base. Matching the published COLUMN instead would inject
		// a second `order_id` beside the aliased one, and `li.order_id` would
		// then read the KEY where PostgreSQL reads the amount: a plausible
		// wrong number for an obvious zero, which protocol item 8 refuses.
		// The key has to be published under a name nothing can collide with —
		// a hidden slot — which is #785's territory (ADR-0026 §3a). Pinned as
		// `boundary_inner_alias_shadowing_the_key_answers_nothing`.
		//
		// The GROUP BY half stays gated on hasAgg: a subquery with no
		// aggregate has nothing to group.
		var injected []plansql.SelectColumn
		for _, cp := range correlatedParts {
			innerCol := extractInnerColumn(cp, leftAliases)
			if innerCol == "" {
				continue
			}
			if hasAgg {
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
			if lateralSelectsColumn(subInfo.Columns, innerCol) || lateralSelectsColumn(injected, innerCol) {
				continue
			}
			if col, ok := lateralKeySelectItem(innerCol); ok {
				injected = append(injected, col)
			}
		}
		// Keys first, mirroring the order buildAggregate emits them in, so
		// the projection above stays elidable in the ordinary shape.
		subInfo.Columns = append(injected, subInfo.Columns...)
	}

	right, err := BuildFromSelectWithCTEs(subInfo, ctes)
	if err != nil {
		return nil, "", lateralEmptyInput{}, fmt.Errorf("building LATERAL subquery plan: %w", err)
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

	// The correlation the DECORRELATION produced and the ON the QUERY WROTE
	// are returned apart, because the empty-input repair may keep only the
	// first as the join's condition and has to move the second (see
	// lateralEmptyInput and the caller). Concatenating them is what discarded
	// the written ON.
	corrCond := strings.Join(correlatedParts, " AND ")
	empty.onResidual = ""
	if join.Condition != "" && !strings.EqualFold(strings.TrimSpace(join.Condition), "true") {
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

	return right, joinCond, empty, nil
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
		if c.Alias != "" && strings.EqualFold(c.Alias, bare) {
			return true
		}
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
