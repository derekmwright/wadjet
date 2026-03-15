package logical

import (
	"fmt"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/caelum/internal/planner/sql"
)

// BuildFromSelect constructs a logical plan from a parsed SELECT query.
func BuildFromSelect(info *plansql.SelectInfo) (*Node, error) {
	var plan *Node

	// FROM clause — build scan nodes
	if len(info.Joins) > 0 {
		// Build join tree
		if len(info.Tables) == 0 {
			return nil, fmt.Errorf("no tables in FROM clause")
		}
		plan = NewScan(info.Tables[0].Name, info.Tables[0].Alias)

		for _, join := range info.Joins {
			right := NewScan(join.RightTable, join.RightAlias)
			plan = NewJoin(plan, right, join.Type, join.Condition)
		}
	} else if len(info.Tables) > 0 {
		plan = NewScan(info.Tables[0].Name, info.Tables[0].Alias)
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
				})
			}
		}

		plan = NewAggregate(plan, groupBy, aggs)
	}

	// PROJECT (SELECT columns)
	if !isStarOnly(info.Columns) {
		var projections []Projection
		for _, col := range info.Columns {
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
