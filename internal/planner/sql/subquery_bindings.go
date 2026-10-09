// SPDX-License-Identifier: MIT

package sql

// A CORRELATED SUBQUERY'S BODY STAYS UNBOUND (ADR-0047 stage 3).
//
// The binder binds an expression subquery's memoized body with the enclosing
// scope, and the planners plan that body — for an UNCORRELATED subquery. A
// correlated one is run per outer row from its text with the outer values
// substituted (expr.CorrelatedScalarSubquery.buildSQL), and decorrelated from
// a private parse whose keys are spelled by name (stage 4): neither reads a
// binding. A body bound and accepted by the binding rules would then run by
// the spelling rules it was not checked by — `(SELECT max(x.v + 1) FROM ss_i
// x GROUP BY x.v + 1 HAVING v + 1 > o.i …)` passes the bound grouping check
// and its per-row text answers NULL — so the binder clears a correlated
// body's bindings (ClearBindings) and judges it by spelling, as at 542b4f37.

// ClearBindings removes every binding the binder recorded on a block's tree:
// each column reference's (ColRef.Bound) and each FROM item's instance
// (TableRef.Rel, RelCols).
func ClearBindings(info *SelectInfo) {
	visitBlockRefs(info, 0, map[*SelectInfo]bool{}, func(c *ColRef, _ int) { c.Bound = nil },
		func(t *TableRef) { t.Rel, t.RelCols = 0, nil })
}

// visitBlockRefs calls ref for every column reference of info's tree, with
// the nesting depth of the block it is written in relative to info (an
// expression subquery is one level down; a derived table's and a WITH item's
// body are blocks of their own, whose references do not reach the enclosing
// levels, and are visited at depth 0 of their own), and table for every FROM
// item.
func visitBlockRefs(info *SelectInfo, depth int, seen map[*SelectInfo]bool, ref func(*ColRef, int), table func(*TableRef)) {
	if info == nil || seen[info] {
		return
	}
	seen[info] = true
	if info.Union != nil {
		visitBlockRefs(info.Union.Left, depth, seen, ref, table)
		visitBlockRefs(info.Union.Right, depth, seen, ref, table)
	}
	visit := func(n Node) {
		if n == nil {
			return
		}
		WalkColRefs(n, func(c *ColRef) { ref(c, depth) })
		ForEachSubquery(n, func(sq Node) {
			var body *SelectInfo
			switch q := sq.(type) {
			case *SubqueryNode:
				body, _ = q.Select()
			case *ExistsNode:
				body, _ = q.Select()
			}
			visitBlockRefs(body, depth+1, seen, ref, table)
		})
	}
	for i := range info.Columns {
		visit(info.Columns[i].ASTExpr)
		visit(info.Columns[i].AggArgExpr)
	}
	visit(info.WhereExpr)
	visit(info.HavingExpr)
	visit(info.QualifyExpr)
	for _, g := range info.GroupByExprs {
		visit(g)
	}
	for i := range info.OrderBy {
		visit(info.OrderBy[i].Expr)
	}
	from := func(t *TableRef) {
		if t == nil {
			return
		}
		if table != nil {
			table(t)
		}
		if sub, err := t.SubSelect(); err == nil && sub != nil {
			visitBlockRefs(sub, 0, seen, ref, table)
		}
	}
	for i := range info.Tables {
		from(&info.Tables[i])
	}
	for i := range info.Joins {
		visit(info.Joins[i].CondExpr)
		from(info.Joins[i].RightTableRef)
	}
	for i := range info.CTEs {
		if body, err := info.CTEs[i].BodySelect(); err == nil && body != nil {
			visitBlockRefs(body, 0, seen, ref, table)
		}
	}
}
