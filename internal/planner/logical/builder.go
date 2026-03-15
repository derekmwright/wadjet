package logical

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	plansql "github.com/derekmwright/caelum/internal/planner/sql"
)

// BuildFromSelect constructs a logical plan from a parsed SELECT query.
func BuildFromSelect(info *plansql.SelectInfo) (*Node, error) {
	return buildFromSelectWithCTEs(info, info.CTEs)
}

// buildFromSelectWithCTEs constructs a logical plan, resolving CTE references
// to inline sub-plans instead of table scans.
func buildFromSelectWithCTEs(info *plansql.SelectInfo, ctes []plansql.CTEDef) (*Node, error) {
	// Handle UNION queries
	if info.Union != nil {
		return buildUnionPlan(info, ctes)
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
		var groupBy []string

		for _, gb := range info.GroupBy {
			groupBy = append(groupBy, cleanExpr(gb))
		}

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
				aggs = append(aggs, AggExpr{
					Func:      col.AggFunc,
					InputCol:  inputCol,
					OutputCol: outputCol,
					Distinct:  col.AggDistinct,
				})
			}
		}

		plan = NewAggregate(plan, groupBy, aggs)

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
					Column: cleanExpr(ob.Column),
					Desc:   ob.Desc,
				})
			}
			partBy := make([]string, len(ws.PartitionBy))
			for i, p := range ws.PartitionBy {
				partBy[i] = cleanExpr(p)
			}
			winExprs = append(winExprs, WindowExpr{
				Func:        ws.FuncName,
				InputCol:    cleanExpr(ws.Args),
				OutputCol:   ws.Alias,
				PartitionBy: partBy,
				OrderBy:     orderBy,
			})
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
				Column: cleanExpr(ob.Column),
				Desc:   ob.Desc,
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
// e.g., COUNT(*) > 5 becomes a comparison against the "count(*)" output column.
func rewriteHavingExpr(expr sqlparser.Expr, cols []plansql.SelectColumn) sqlparser.Expr {
	return rewriteExpr(expr, cols)
}

func rewriteExpr(node sqlparser.Expr, cols []plansql.SelectColumn) sqlparser.Expr {
	switch n := node.(type) {
	case *sqlparser.FuncExpr:
		// This is an aggregate call — find the matching output column name
		funcStr := sqlparser.String(n)
		colName := funcStr // default: use the expression string as column name
		for _, col := range cols {
			if col.IsAgg && strings.EqualFold(sqlparser.String(col.ASTExpr), funcStr) {
				if col.Alias != "" {
					colName = col.Alias
				} else {
					colName = col.Expr
				}
				break
			}
		}
		return &sqlparser.ColName{
			Name: sqlparser.NewColIdent(colName),
		}
	case *sqlparser.AndExpr:
		return &sqlparser.AndExpr{
			Left:  rewriteExpr(n.Left, cols),
			Right: rewriteExpr(n.Right, cols),
		}
	case *sqlparser.OrExpr:
		return &sqlparser.OrExpr{
			Left:  rewriteExpr(n.Left, cols),
			Right: rewriteExpr(n.Right, cols),
		}
	case *sqlparser.ComparisonExpr:
		return &sqlparser.ComparisonExpr{
			Operator: n.Operator,
			Left:     rewriteExpr(n.Left, cols),
			Right:    rewriteExpr(n.Right, cols),
		}
	case *sqlparser.ParenExpr:
		return &sqlparser.ParenExpr{
			Expr: rewriteExpr(n.Expr, cols),
		}
	case *sqlparser.NotExpr:
		return &sqlparser.NotExpr{
			Expr: rewriteExpr(n.Expr, cols),
		}
	default:
		// Literals, ColName, etc. — pass through unchanged
		return node
	}
}

// buildUnionPlan constructs a logical plan for a UNION query.
// It recursively builds plans for the left and right sides, wraps them
// in a NewUnion node, and then applies any ORDER BY / LIMIT from the outer query.
func buildUnionPlan(info *plansql.SelectInfo, ctes []plansql.CTEDef) (*Node, error) {
	leftPlan, err := buildFromSelectWithCTEs(info.Union.Left, ctes)
	if err != nil {
		return nil, fmt.Errorf("building UNION left side: %w", err)
	}
	rightPlan, err := buildFromSelectWithCTEs(info.Union.Right, ctes)
	if err != nil {
		return nil, fmt.Errorf("building UNION right side: %w", err)
	}

	plan := NewUnion(leftPlan, rightPlan, info.Union.All)

	// ORDER BY on the overall UNION
	if len(info.OrderBy) > 0 {
		var orderExprs []OrderExpr
		for _, ob := range info.OrderBy {
			orderExprs = append(orderExprs, OrderExpr{
				Column: cleanExpr(ob.Column),
				Desc:   ob.Desc,
			})
		}
		plan = NewSort(plan, orderExprs)
	}

	// LIMIT on the overall UNION
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
// If it does, the CTE body is parsed and planned as a sub-plan.
// Otherwise, a regular Scan node is returned.
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
			return plan, nil
		}
	}
	return NewScan(table.Name, table.Alias), nil
}
