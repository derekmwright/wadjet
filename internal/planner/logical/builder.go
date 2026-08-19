package logical

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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

	var plan *Node

	// FROM clause — build scan nodes (or CTE sub-plans)
	if len(info.Joins) > 0 {
		// Build join tree
		if len(info.Tables) == 0 {
			return nil, fmt.Errorf("no tables in FROM clause")
		}
		var err error
		plan, err = resolveTableOrCTE(info.Tables[0], ctes)
		if err != nil {
			return nil, err
		}
		// Comma-separated FROM entries beyond the first parse into
		// info.Tables (the parser only emits JoinInfo for explicit JOIN
		// syntax). Fold them in as cross joins; pushdownPredicates and
		// reorderJoins recover the real join conditions from WHERE.
		// Dropping them silently returned wrong results (issue #281).
		for _, t := range info.Tables[1:] {
			right, err := resolveTableOrCTE(t, ctes)
			if err != nil {
				return nil, err
			}
			plan = NewJoin(plan, right, "cross", "")
		}

		for _, join := range info.Joins {
			if join.Lateral && strings.HasPrefix(join.RightTable, "(") {
				// LATERAL subquery: decorrelate by extracting correlated
				// WHERE predicates and moving them to the join condition.
				right, joinCond, err := buildLateralSubquery(plan, join, ctes)
				if err != nil {
					return nil, err
				}
				// Cross join with correlated predicates → inner join
				// (cross join skips key parsing in the physical planner)
				jt := join.Type
				if joinCond != "" && strings.EqualFold(strings.TrimSpace(jt), "cross join") {
					jt = "join"
				}
				plan = NewJoin(plan, right, jt, joinCond)
			} else {
				rightRef := plansql.TableRef{
					Name:  join.RightTable,
					Alias: join.RightAlias,
				}
				if join.RightTableRef != nil {
					rightRef = *join.RightTableRef
				}
				right, err := resolveTableOrCTE(rightRef, ctes)
				if err != nil {
					return nil, err
				}
				plan = NewJoin(plan, right, join.Type, join.Condition)
			}
		}
	} else if len(info.Tables) > 0 {
		var err error
		plan, err = resolveTableOrCTE(info.Tables[0], ctes)
		if err != nil {
			return nil, err
		}
		// Comma-join FROM list (see the explicit-join branch above).
		for _, t := range info.Tables[1:] {
			right, err := resolveTableOrCTE(t, ctes)
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

	// Track columns that need AST rewriting for nested aggregates.
	// Key: column index, Value: rewritten AST with aggregate replaced by ColRef.
	nestedAggRewrites := map[int]plansql.Node{}
	// Map aggregate expression string → synthetic name (for ORDER BY resolution)
	aggSyntheticNames := map[string]string{}

	// GROUP BY / aggregation
	if hasAgg || len(info.GroupBy) > 0 {
		var aggs []AggExpr
		aggCounter := 0

		for i, col := range info.Columns {
			if col.IsAgg {
				// Skip GROUPING() — it's computed during output, not a real aggregate
				if col.AggFunc == "grouping" {
					continue
				}

				// Algebraic constant-arithmetic rewrite: SUM(x+k) →
				// SUM(x)+k*COUNT(x), AVG(x+k) → AVG(x)+k, MIN/MAX(x±k) →
				// MIN/MAX(x)±k, etc. Turns N expression-aggregates over one
				// column into one shared aggregate plus post-projection
				// arithmetic — ClickBench Q30 (90 × SUM(col + k)) went from
				// evaluating 90 full-column expression passes per batch to
				// a single SUM + COUNT.
				if col.ASTExpr != nil {
					if rw := rewriteConstArithAggs(col.ASTExpr); rw != nil {
						col.ASTExpr = rw
					}
				}

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
						syntheticName := fmt.Sprintf("__agg_%d", aggCounter)
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
				// Check if this aggregate already exists in the SELECT-derived aggs
				found := false
				for _, existing := range aggs {
					existingKey := strings.ToLower(existing.Func + "(")
					if existing.Distinct {
						existingKey += "distinct "
					}
					existingKey += existing.InputCol + ")"
					if existingKey == hKey {
						found = true
						havingReplacements[hKey] = existing.OutputCol
						break
					}
				}
				if !found {
					synName := fmt.Sprintf("__having_%d", aggCounter)
					aggCounter++
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

		if len(info.GroupingSets) > 0 {
			// GROUPING SETS / CUBE / ROLLUP: build multiple aggregate passes
			// connected by UNION ALL.
			allGroupCols := make([]string, len(info.GroupBy))
			for i, gb := range info.GroupBy {
				allGroupCols[i] = cleanExpr(gb)
			}

			plan = buildGroupingSets(plan, allGroupCols, aggs, info.GroupingSets)
		} else {
			// Simple GROUP BY
			var groupBy []string
			for _, gb := range info.GroupBy {
				groupBy = append(groupBy, cleanExpr(gb))
			}
			aggNode := NewAggregate(plan, groupBy, aggs)
			aggNode.GroupByExprs = info.GroupByExprs
			plan = aggNode
		}

		// HAVING clause (must come after Aggregate)
		if info.Having != "" && info.HavingExpr != nil {
			var rewritten plansql.Node
			if len(havingReplacements) > 0 {
				rewritten = plansql.ReplaceAllAggregates(info.HavingExpr, havingReplacements)
			} else {
				rewritten = rewriteHavingExpr(info.HavingExpr, info.Columns)
			}
			preds := []Predicate{{Raw: rewritten.String(), ASTExpr: rewritten}}
			plan = NewFilter(plan, preds)
		}
	}

	// WINDOW functions (after GROUP BY/HAVING, before PROJECT)
	if len(info.Windows) > 0 {
		var winExprs []WindowExpr
		for _, ws := range info.Windows {
			var orderBy []OrderExpr
			for _, ob := range ws.OrderBy {
				orderBy = append(orderBy, OrderExpr{
					Column:     cleanExpr(ob.Column),
					Desc:       ob.Desc,
					NullsFirst: ob.NullsFirst,
				})
			}
			partBy := make([]string, len(ws.PartitionBy))
			for i, p := range ws.PartitionBy {
				partBy[i] = cleanExpr(p)
			}
			we := WindowExpr{
				Func:        ws.FuncName,
				InputCol:    cleanExpr(ws.Args),
				OutputCol:   ws.Alias,
				PartitionBy: partBy,
				OrderBy:     orderBy,
			}
			if ws.Frame != nil {
				we.Frame = convertFrame(ws.Frame)
			}
			winExprs = append(winExprs, we)
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
				// Window column: reference the window output by alias
				projections = append(projections, Projection{
					Expr:   col.WindowSpec.Alias,
					Alias:  col.WindowSpec.Alias,
					Column: col.WindowSpec.Alias,
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
		// This is an aggregate call — find the matching output column name
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
	default:
		// Literals, ColRef, etc. — pass through unchanged
		return node
	}
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

// resolveTableOrCTE checks whether a table reference matches a CTE name.
func resolveTableOrCTE(table plansql.TableRef, ctes []plansql.CTEDef) (*Node, error) {
	nameLower := strings.ToLower(table.Name)
	for _, cte := range ctes {
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

			// Parse the CTE body SQL
			parsed, err := plansql.Parse(cte.SQL)
			if err != nil {
				return nil, fmt.Errorf("parsing CTE %q: %w", cte.Name, err)
			}
			selectInfo, err := plansql.ExtractSelect(parsed)
			if err != nil {
				return nil, fmt.Errorf("extracting SELECT from CTE %q: %w", cte.Name, err)
			}
			// Pass the same CTE defs so CTEs can reference earlier CTEs
			plan, err := BuildFromSelectWithCTEs(selectInfo, ctes)
			if err != nil {
				return nil, fmt.Errorf("building plan for CTE %q: %w", cte.Name, err)
			}

			// Tag the sub-plan so the physical planner can detect CTE subtrees
			// and materialize multi-referenced CTEs.
			plan.CTEName = cte.Name

			// If the CTE has an explicit column list, wrap with a rename projection
			if len(cte.Columns) > 0 {
				// Count output columns from the plan
				outCols := countOutputCols(plan, selectInfo)
				if len(cte.Columns) == outCols {
					var projections []Projection
					srcNames := getOutputColNames(selectInfo)
					for i, newName := range cte.Columns {
						srcName := srcNames[i]
						projections = append(projections, Projection{
							Column: srcName,
							Alias:  newName,
							Expr:   srcName,
						})
					}
					plan = NewProject(plan, projections)
				}
			}

			return plan, nil
		}
	}
	// Check for derived table (subquery in FROM): name starts with "("
	if strings.HasPrefix(table.Name, "(") {
		// Strip outer parens to get inner SQL
		inner := table.Name[1 : len(table.Name)-1]
		parsed, err := plansql.Parse(inner)
		if err != nil {
			return nil, fmt.Errorf("parsing derived table: %w", err)
		}
		selectInfo, err := plansql.ExtractSelect(parsed)
		if err != nil {
			return nil, fmt.Errorf("extracting SELECT from derived table: %w", err)
		}
		plan, err := BuildFromSelectWithCTEs(selectInfo, ctes)
		if err != nil {
			return nil, fmt.Errorf("building plan for derived table: %w", err)
		}
		// Apply alias as table alias on the root scan if available
		if table.Alias != "" {
			setSubtreeAlias(plan, table.Alias)
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

// setSubtreeAlias sets the table alias on all Scan nodes in a subtree,
// used to alias derived table output so columns can be referenced by the alias.
func setSubtreeAlias(n *Node, alias string) {
	if n.Type == NodeScan {
		n.TableAlias = alias
	}
	for _, c := range n.Children {
		setSubtreeAlias(c, alias)
	}
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
func buildLateralSubquery(left *Node, join plansql.JoinInfo, ctes []plansql.CTEDef) (*Node, string, error) {
	// Collect left-side table aliases to detect correlated references
	leftAliases := collectLogicalAliases(left)

	// Parse the subquery
	inner := join.RightTable[1 : len(join.RightTable)-1]
	parsed, err := plansql.Parse(inner)
	if err != nil {
		return nil, "", fmt.Errorf("parsing LATERAL subquery: %w", err)
	}
	subInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, "", fmt.Errorf("extracting SELECT from LATERAL subquery: %w", err)
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
	if hasAgg && len(correlatedParts) > 0 {
		for _, cp := range correlatedParts {
			innerCol := extractInnerColumn(cp, leftAliases)
			if innerCol != "" {
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
		}
	}

	right, err := BuildFromSelectWithCTEs(subInfo, ctes)
	if err != nil {
		return nil, "", fmt.Errorf("building LATERAL subquery plan: %w", err)
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

	// Build the join condition from correlated predicates + original ON clause
	var condParts []string
	condParts = append(condParts, correlatedParts...)
	if join.Condition != "" && !strings.EqualFold(strings.TrimSpace(join.Condition), "true") {
		condParts = append(condParts, join.Condition)
	}
	joinCond := strings.Join(condParts, " AND ")

	return right, joinCond, nil
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
			aliases[strings.ToLower(n.TableName)] = true
			if n.TableAlias != "" {
				aliases[strings.ToLower(n.TableAlias)] = true
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
