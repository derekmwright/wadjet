package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// checkAggregatePlacement enforces aggregate/grouping placement BEFORE planning,
// at THIS query level (#804). WHERE runs before grouping and cannot read either;
// JOIN conditions also reject aggregates, and aggregate calls cannot nest (42803).
// Use PostgreSQL's placement messages; SUM(GROUPING(g)) is forbidden nesting.
// A subquery is its own level: FindAllAggregates does not descend into it, so an
// aggregate inside a WHERE subquery is legal at that subquery's level.
// Skip WINDOW columns: SUM(COUNT(*)) OVER () is legal, and the builder already
// hoists aggregates from the window's spec terms.
// See docs/internals/query-level-aggregate-placement.md for the design.
func checkAggregatePlacement(info *plansql.SelectInfo) error {
	if info.WhereExpr != nil {
		if found := plansql.FindAllAggregates(info.WhereExpr); len(found) > 0 {
			return aggPlacementError(found[0], "WHERE")
		}
	}
	for _, j := range info.Joins {
		if j.CondExpr == nil {
			continue
		}
		if found := plansql.FindAllAggregates(j.CondExpr); len(found) > 0 {
			return aggPlacementError(found[0], "JOIN conditions")
		}
	}
	if err := checkSubqueryAggregatePlacement(info); err != nil {
		return err
	}

	// Nesting, at every position an aggregate IS legal. FindAllAggregates
	// stops at the outermost call, so the arguments are scanned separately.
	for _, col := range info.Columns {
		if col.IsWindow || col.ASTExpr == nil {
			continue
		}
		if err := checkNoNestedAggregate(col.ASTExpr); err != nil {
			return err
		}
	}
	if err := checkNoNestedAggregate(info.HavingExpr); err != nil {
		return err
	}
	for _, ob := range info.OrderBy {
		if err := checkNoNestedAggregate(ob.Expr); err != nil {
			return err
		}
	}
	return nil
}

// checkSubqueryAggregatePlacement applies placement rules at each contained
// subquery's OWN level, including subqueries taken apart by decorrelation (#809, #601).
// Refuse that level's misplaced aggregates here with 42803 on both distribution arms.
// An aggregate naming a relation the subquery does NOT provide is left to the runner:
// without schema, this check cannot resolve a bare name to decide query-level ownership.
// PostgreSQL can accept outer-level aggregates; our standalone runner still refuses
// those shapes at the subquery level. Do not turn that boundary into an earlier refusal.
// See docs/internals/subquery-aggregate-placement-boundary.md for the design.
func checkSubqueryAggregatePlacement(info *plansql.SelectInfo) error {
	var sqls []string
	collectSubquerySQL(info.WhereExpr, &sqls)
	collectSubquerySQL(info.HavingExpr, &sqls)
	for _, col := range info.Columns {
		collectSubquerySQL(col.ASTExpr, &sqls)
	}
	for _, ob := range info.OrderBy {
		collectSubquerySQL(ob.Expr, &sqls)
	}
	for _, j := range info.Joins {
		collectSubquerySQL(j.CondExpr, &sqls)
	}
	for _, sql := range sqls {
		parsed, err := plansql.Parse(sql)
		if err != nil {
			continue // not this check's business; the executor reports it
		}
		sub, err := plansql.ExtractSelect(parsed)
		if err != nil || sub == nil {
			continue
		}
		own := subqueryOwnRelations(sub)
		if sub.WhereExpr != nil {
			for _, fn := range plansql.FindAllAggregates(sub.WhereExpr) {
				if aggregateBelongsToLevel(fn, own) {
					return aggPlacementError(fn, "WHERE")
				}
			}
		}
		for _, j := range sub.Joins {
			if j.CondExpr == nil {
				continue
			}
			for _, fn := range plansql.FindAllAggregates(j.CondExpr) {
				if aggregateBelongsToLevel(fn, own) {
					return aggPlacementError(fn, "JOIN conditions")
				}
			}
		}
		// A subquery of a subquery is another level, asked the same way.
		if err := checkSubqueryAggregatePlacement(sub); err != nil {
			return err
		}
	}
	return nil
}

// subqueryOwnRelations is every name the subquery's own FROM answers to.
func subqueryOwnRelations(info *plansql.SelectInfo) map[string]bool {
	own := make(map[string]bool, len(info.Tables)+len(info.Joins))
	add := func(name, alias string) {
		if name != "" {
			own[strings.ToLower(name)] = true
		}
		if alias != "" {
			own[strings.ToLower(alias)] = true
		}
	}
	for _, t := range info.Tables {
		add(t.Name, t.Alias)
	}
	for _, j := range info.Joins {
		add(j.RightTable, j.RightAlias)
	}
	return own
}

// aggregateBelongsToLevel reports whether this aggregate is the subquery's
// OWN — which here means it names no relation the subquery does not provide.
// See checkSubqueryAggregatePlacement's boundary for why the test is that way
// round and not "every reference is one of ours".
func aggregateBelongsToLevel(fn *plansql.FuncCallNode, own map[string]bool) bool {
	foreign := false
	for _, arg := range fn.Args {
		walkColRefs(arg, func(c *plansql.ColRef) {
			if c.Table != "" && !own[strings.ToLower(c.Table)] {
				foreign = true
			}
		})
	}
	return !foreign
}

// walkColRefs visits every column reference in an expression.
func walkColRefs(node plansql.Node, visit func(*plansql.ColRef)) {
	switch n := node.(type) {
	case nil:
		return
	case *plansql.ColRef:
		visit(n)
	case *plansql.ParenNode:
		walkColRefs(n.Inner, visit)
	case *plansql.NotNode:
		walkColRefs(n.Inner, visit)
	case *plansql.UnaryOp:
		walkColRefs(n.Inner, visit)
	case *plansql.AndNode:
		walkColRefs(n.Left, visit)
		walkColRefs(n.Right, visit)
	case *plansql.OrNode:
		walkColRefs(n.Left, visit)
		walkColRefs(n.Right, visit)
	case *plansql.BinaryOp:
		walkColRefs(n.Left, visit)
		walkColRefs(n.Right, visit)
	case *plansql.CmpExpr:
		walkColRefs(n.Left, visit)
		walkColRefs(n.Right, visit)
	case *plansql.IsExpr:
		walkColRefs(n.Left, visit)
	case *plansql.LikeExpr:
		walkColRefs(n.Left, visit)
		walkColRefs(n.Pattern, visit)
	case *plansql.BetweenExpr:
		walkColRefs(n.Left, visit)
		walkColRefs(n.Low, visit)
		walkColRefs(n.High, visit)
	case *plansql.InExpr:
		walkColRefs(n.Left, visit)
		for _, v := range n.Values {
			walkColRefs(v, visit)
		}
	case *plansql.CastNode:
		walkColRefs(n.Inner, visit)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			walkColRefs(a, visit)
		}
	case *plansql.CaseNode:
		walkColRefs(n.Subject, visit)
		for _, w := range n.Whens {
			walkColRefs(w.Cond, visit)
			walkColRefs(w.Result, visit)
		}
		walkColRefs(n.Else, visit)
	}
}

// collectSubquerySQL gathers the SQL text of every subquery this expression
// contains, at THIS level only — a subquery's own nested ones are collected
// when it is itself examined.
func collectSubquerySQL(node plansql.Node, out *[]string) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *plansql.SubqueryNode:
		*out = append(*out, n.SQL)
	case *plansql.ExistsNode:
		*out = append(*out, n.SQL)
	case *plansql.ParenNode:
		collectSubquerySQL(n.Inner, out)
	case *plansql.NotNode:
		collectSubquerySQL(n.Inner, out)
	case *plansql.UnaryOp:
		collectSubquerySQL(n.Inner, out)
	case *plansql.AndNode:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Right, out)
	case *plansql.OrNode:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Right, out)
	case *plansql.BinaryOp:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Right, out)
	case *plansql.CmpExpr:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Right, out)
	case *plansql.IsExpr:
		collectSubquerySQL(n.Left, out)
	case *plansql.LikeExpr:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Pattern, out)
	case *plansql.BetweenExpr:
		collectSubquerySQL(n.Left, out)
		collectSubquerySQL(n.Low, out)
		collectSubquerySQL(n.High, out)
	case *plansql.InExpr:
		collectSubquerySQL(n.Left, out)
		for _, v := range n.Values {
			collectSubquerySQL(v, out)
		}
	case *plansql.CastNode:
		collectSubquerySQL(n.Inner, out)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			collectSubquerySQL(a, out)
		}
	case *plansql.CaseNode:
		collectSubquerySQL(n.Subject, out)
		for _, w := range n.Whens {
			collectSubquerySQL(w.Cond, out)
			collectSubquerySQL(w.Result, out)
		}
		collectSubquerySQL(n.Else, out)
	}
}

func checkNoNestedAggregate(expr plansql.Node) error {
	if expr == nil {
		return nil
	}
	for _, fn := range plansql.FindAllAggregates(expr) {
		for _, arg := range fn.Args {
			if len(plansql.FindAllAggregates(arg)) > 0 {
				return sqlerr.New(groupingErrSQLState, "aggregate function calls cannot be nested")
			}
		}
	}
	return nil
}

// aggPlacementError words the refusal the way PostgreSQL does: a GROUPING call
// is a "grouping operation", everything else an "aggregate function".
func aggPlacementError(fn *plansql.FuncCallNode, clause string) error {
	kind := "aggregate functions"
	if strings.EqualFold(fn.Name, "grouping") {
		kind = "grouping operations"
	}
	return sqlerr.New(groupingErrSQLState, "%s are not allowed in %s", kind, clause)
}
