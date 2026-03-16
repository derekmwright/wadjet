package logical

import (
	"fmt"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/caelum/internal/planner/sql"
)

// BuildFromSelect constructs a logical plan from a parsed SELECT query.
func BuildFromSelect(info *plansql.SelectInfo) (*Node, error) {
	return buildFromSelectWithCTEs(info, info.CTEs)
}

// buildFromSelectWithCTEs constructs a logical plan, resolving CTE references
// to inline sub-plans instead of table scans.
func buildFromSelectWithCTEs(info *plansql.SelectInfo, ctes []plansql.CTEDef) (*Node, error) {
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

	// GROUP BY / aggregation
	if hasAgg || len(info.GroupBy) > 0 {
		var aggs []AggExpr

		for _, col := range info.Columns {
			if col.IsAgg {
				outputCol := col.Alias
				if outputCol == "" {
					outputCol = col.Expr
				}
				inputCol := cleanExpr(col.AggArg)
				// Handle COUNT(*)
				if col.AggFunc == "count" && (inputCol == "*" || inputCol == "") {
					inputCol = ""
				}
				// Skip GROUPING() — it's computed during output, not a real aggregate
				if col.AggFunc == "grouping" {
					continue
				}
				aggs = append(aggs, AggExpr{
					Func:      col.AggFunc,
					InputCol:  inputCol,
					OutputCol: outputCol,
					Distinct:  col.AggDistinct,
					InputExpr: col.AggArgExpr,
				})
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
			plan = NewAggregate(plan, groupBy, aggs)
		}

		// HAVING clause (must come after Aggregate)
		if info.Having != "" && info.HavingExpr != nil {
			rewritten := rewriteHavingExpr(info.HavingExpr, info.Columns)
			preds := []Predicate{{Raw: info.Having, ASTExpr: rewritten}}
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

	// PROJECT (SELECT columns)
	if !isStarOnly(info.Columns) {
		var projections []Projection
		for _, col := range info.Columns {
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

	// ORDER BY
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

	leftPlan, err := buildFromSelectWithCTEs(info.Union.Left, ctes)
	if err != nil {
		return nil, fmt.Errorf("building %s left side: %w", op, err)
	}
	rightPlan, err := buildFromSelectWithCTEs(info.Union.Right, ctes)
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
			plan, err := buildFromSelectWithCTEs(selectInfo, ctes)
			if err != nil {
				return nil, fmt.Errorf("building plan for CTE %q: %w", cte.Name, err)
			}

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
	return NewScan(table.Name, table.Alias), nil
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
