package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// checkAggregatePlacement enforces PostgreSQL's placement rules for aggregate
// and grouping operations AT THIS QUERY LEVEL, before anything is planned:
//
//	SELECT g FROM t WHERE SUM(h) > 1 GROUP BY g
//	  ERROR: aggregate functions are not allowed in WHERE            (42803)
//	SELECT g FROM t WHERE GROUPING(g) = 0 GROUP BY ROLLUP(g)
//	  ERROR: grouping operations are not allowed in WHERE            (42803)
//	SELECT a.g FROM t a JOIN t b ON SUM(a.h) = 0 GROUP BY a.g
//	  ERROR: aggregate functions are not allowed in JOIN conditions  (42803)
//	SELECT SUM(GROUPING(g)) FROM t GROUP BY ROLLUP(g)
//	  ERROR: aggregate function calls cannot be nested               (42803)
//
// (every message and SQLSTATE transcribed from PostgreSQL 17.11).
//
// WHERE runs BEFORE grouping, so no aggregate's output and no grouping-set
// membership exists there to read; an aggregate call in that position is a
// question the query cannot ask. Both were answered SILENTLY before this
// check — `WHERE SUM(h) > 1` and `WHERE GROUPING(g) = 0` each returned ZERO
// ROWS, because the reference resolved to nothing and a filter admits only
// TRUE — and `SUM(GROUPING(g))` aggregated over a column nothing populated
// and returned a column of NULLs. A wrong number in place of an error is the
// regression the correctness protocol's rule 8 forbids, and #804's parser
// widening reached two of these positions, so the rule that covers them is
// one rule, not a GROUPING special case.
//
// Scope is deliberately THIS query level:
//
//   - A subquery is its own level and its aggregates are legal there —
//     `WHERE h > (SELECT AVG(h) FROM t)` is ordinary SQL, and PostgreSQL
//     accepts it. plansql.FindAllAggregates does not descend into subquery
//     nodes, which is what makes the scan level-local.
//   - A WINDOW column is skipped: `SUM(COUNT(*)) OVER ()` is legal in
//     PostgreSQL (a window function OVER an aggregate), and the builder
//     already hoists aggregates out of a window's own spec terms. Refusing
//     it here would invent a rule PostgreSQL does not have.
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

// checkSubqueryAggregatePlacement applies the level-local rule to the
// SUBQUERIES this level contains, at THEIR level (#809, #601).
//
// The scan above deliberately does not descend into a subquery, and the
// reason it gives is right — a subquery is its own level, and `WHERE h >
// (SELECT AVG(h) FROM t)` is ordinary SQL. What it left uncovered is the
// subquery's OWN level, which nothing else reaches when the planner takes the
// subquery apart rather than running it: `SELECT b.w_i32 FROM numwidth b
// WHERE SUM(b.w_i32) > 0` is refused by PostgreSQL with 42803, and by this
// engine too when the subquery is EXECUTED (its Runner plans it, and the scan
// above fires at that level) — but a decorrelated IN builds the inner plan
// straight from the parsed subquery, so the aggregate reached a Filter and
// `a.w_i32 NOT IN (that)` answered every row of numwidth in silence. The DAG
// half of #809 is the same gap wearing the other hat: `WHERE h > (SELECT
// AVG(x.h) FROM collslot x WHERE SUM(x.h) > 0)` reached the worker as filter
// TEXT and failed with "subqueries require a SubqueryRunner" and no SQLSTATE
// at all, while the single-process arm gave PostgreSQL's 42803.
//
// THE BOUNDARY, and it is where PostgreSQL and this engine really differ: an
// aggregate inside a subquery may belong to the OUTER level, and PostgreSQL
// accepts it there — measured live, `HAVING (SELECT MAX(d.k) FROM typemx_dim
// d WHERE d.k = SUM(typemx.g)) > 0` answers rows. This engine does not answer
// that shape on ANY path, at this arc's base or at its tip: the subquery is
// re-run standalone and refused at its own level with the same 42803. So the
// test is not "does it belong to this level" — nothing here has a schema to
// resolve a bare name with — but "does it name a relation this subquery does
// NOT provide". An aggregate that does is left to the runner, exactly as
// before, so nothing that could one day answer is refused earlier because of
// this; everything else is the subquery's own and is refused HERE, where the
// error carries PostgreSQL's SQLSTATE and reaches both distribution arms.
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
