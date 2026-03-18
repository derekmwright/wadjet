package logical

import (
	"fmt"
	"strconv"
	"strings"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
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

		for _, join := range info.Joins {
			right, err := resolveTableOrCTE(plansql.TableRef{
				Name:  join.RightTable,
				Alias: join.RightAlias,
			}, ctes)
			if err != nil {
				return nil, err
			}
			plan = NewJoin(plan, right, join.Type, join.Condition)
		}
	} else if len(info.Tables) > 0 {
		var err error
		plan, err = resolveTableOrCTE(info.Tables[0], ctes)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("no FROM clause")
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
					// Register each aggregate with its own synthetic name
					replacements := map[string]string{}
					for _, agg := range allAggs {
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

						aggs = append(aggs, AggExpr{
							Func:      funcName,
							InputCol:  aggInputCol,
							OutputCol: syntheticName,
							Distinct:  agg.Distinct,
							InputExpr: aggInputExpr,
						})

						aggKey := strings.ToLower(agg.String())
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
					aggs = append(aggs, AggExpr{
						Func:      col.AggFunc,
						InputCol:  inputCol,
						OutputCol: outputCol,
						Distinct:  col.AggDistinct,
						InputExpr: col.AggArgExpr,
					})
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
					aggs = append(aggs, AggExpr{
						Func:      funcName,
						InputCol:  aggInputCol,
						OutputCol: synName,
						Distinct:  hAgg.Distinct,
						InputExpr: aggInputExpr,
					})
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

	// Build Sort before Project when ORDER BY references nested aggregates
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
	}

	// DISTINCT
	if info.Distinct {
		plan = NewDistinct(plan)
	}

	// ORDER BY (skip if already sorted before project)
	if len(info.OrderBy) > 0 && !sortBeforeProject {
		var orderExprs []OrderExpr
		for _, ob := range info.OrderBy {
			orderExprs = append(orderExprs, OrderExpr{
				Column:     resolveOrderByColumn(cleanExpr(ob.Column), info.Columns),
				Desc:       ob.Desc,
				NullsFirst: ob.NullsFirst,
			})
		}
		plan = NewSort(plan, orderExprs)
	}

	// LIMIT
	if info.Limit != "" {
		limit, err := strconv.Atoi(info.Limit)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT: %w", err)
		}
		offset := 0
		if info.Offset != "" {
			offset, err = strconv.Atoi(info.Offset)
			if err != nil {
				return nil, fmt.Errorf("invalid OFFSET: %w", err)
			}
		}
		plan = NewLimit(plan, limit, offset)
	}

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
	s = strings.TrimSpace(s)
	// Remove table qualifiers for simple references: "e.user_id" -> "user_id"
	if parts := strings.SplitN(s, ".", 2); len(parts) == 2 {
		return parts[1]
	}
	return s
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

// buildGroupingSets creates a plan that runs multiple aggregate passes
// (one per grouping set) and combines them with UNION ALL.
// Columns not present in a given grouping set produce NULL in the output.
func buildGroupingSets(inputPlan *Node, allGroupCols []string, aggs []AggExpr, sets [][]string) *Node {
	if len(sets) == 0 {
		return NewAggregate(inputPlan, allGroupCols, aggs)
	}

	// Build one aggregate node per grouping set.
	var plans []*Node
	for _, set := range sets {
		setMap := make(map[string]bool, len(set))
		for _, c := range set {
			setMap[cleanExpr(c)] = true
		}

		// The GROUP BY for this set is only the columns in this set
		var setCols []string
		for _, c := range set {
			setCols = append(setCols, cleanExpr(c))
		}

		// Build aggregate for this grouping set
		aggNode := NewAggregate(inputPlan, setCols, aggs)

		// Mark which columns from allGroupCols are NOT in this set
		// (they need to be NULL). We do this via the GroupingSetNulls field.
		var nullCols []string
		for _, c := range allGroupCols {
			if !setMap[c] {
				nullCols = append(nullCols, c)
			}
		}
		aggNode.GroupingSetNulls = nullCols

		plans = append(plans, aggNode)
	}

	// Combine with UNION ALL
	if len(plans) == 1 {
		return plans[0]
	}
	result := plans[0]
	for i := 1; i < len(plans); i++ {
		result = NewUnion(result, plans[i], true) // UNION ALL
	}
	return result
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

	// ORDER BY on the overall set operation
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

	// LIMIT on the overall set operation
	if info.Limit != "" {
		limit, err := strconv.Atoi(info.Limit)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT: %w", err)
		}
		offset := 0
		if info.Offset != "" {
			offset, err = strconv.Atoi(info.Offset)
			if err != nil {
				return nil, fmt.Errorf("invalid OFFSET: %w", err)
			}
		}
		plan = NewLimit(plan, limit, offset)
	}

	return plan, nil
}

// resolveTableOrCTE checks whether a table reference matches a CTE name.
func resolveTableOrCTE(table plansql.TableRef, ctes []plansql.CTEDef) (*Node, error) {
	nameLower := strings.ToLower(table.Name)
	for _, cte := range ctes {
		if cte.Name == nameLower {
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
		return node, nil
	}

	return NewScan(table.Name, table.Alias), nil
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
