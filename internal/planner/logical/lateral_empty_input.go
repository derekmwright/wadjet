package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// What an EMPTY inner input means for a LATERAL subquery — #767 part 1.
//
// PostgreSQL evaluates a LATERAL subquery ONCE PER OUTER ROW. An UNGROUPED
// aggregate over an empty input still yields exactly one row, so an outer row
// the lateral matches nothing for SURVIVES, with `COUNT` reading 0 and every
// other aggregate reading NULL.
//
// buildLateralSubquery decorrelates by promoting the correlated equality into
// the join condition and injecting the correlated inner column into the
// subquery's GROUP BY, which turns "one row per outer row" into "one row per
// GROUP THAT EXISTS". An outer row with no matching inner rows then has no
// group, so an INNER join DROPS it:
//
//	SELECT o.customer, s.item_count, s.total_amount
//	FROM lat_ord o JOIN LATERAL (
//	  SELECT COUNT(*) AS item_count, SUM(amount) AS total_amount
//	  FROM lat_item WHERE order_id = o.id) s ON true
//
// PostgreSQL 17 answers THREE rows over the fixture, the third being the order
// with no items at `item_count = 0, total_amount = NULL`. This engine answered
// two, in silence; written `LEFT JOIN LATERAL` it answered three and gave that
// row `item_count = NULL`, which is a different wrong answer to the same
// question.
//
// Two things restore it, and both are decided here rather than at the join:
//
//   - the join is a LEFT join whatever the query wrote, because the lateral
//     side produces a row for every outer row and only the DECORRELATION made
//     that conditional;
//   - `COUNT` reads 0 on the padded rows. NULL is right for every other
//     aggregate (`SUM` of nothing IS NULL in PostgreSQL) and the LEFT pad
//     already gives it; COUNT is the one whose empty-input value is not NULL,
//     so its references are wrapped in `COALESCE(…, 0)`.
//
// A subquery the QUERY grouped is untouched: `GROUP BY x` over an empty input
// yields NO row in PostgreSQL either, so an outer row with no match is
// correctly dropped by an inner join.
type lateralEmptyInput struct {
	// ungroupedAggregate is the whole trigger: the lateral's SELECT list
	// holds an aggregate and the QUERY wrote no GROUP BY of its own.
	ungroupedAggregate bool
	// countOutputs are the output names whose empty-input value is 0 rather
	// than NULL — the COUNT family.
	countOutputs []string
}

// lateralEmptyInputOf reads the subquery as WRITTEN, before the correlation
// key is injected into its SELECT list and GROUP BY.
func lateralEmptyInputOf(info *plansql.SelectInfo, hasAgg, correlated bool) lateralEmptyInput {
	if !hasAgg || !correlated || len(info.GroupBy) > 0 {
		return lateralEmptyInput{}
	}
	out := lateralEmptyInput{ungroupedAggregate: true}
	for _, col := range info.Columns {
		if !col.IsAgg || !strings.EqualFold(col.AggFunc, "count") {
			continue
		}
		name := col.Alias
		if name == "" {
			name = col.Expr
		}
		if name != "" {
			out.countOutputs = append(out.countOutputs, name)
		}
	}
	return out
}

// applyLateralEmptyInputDefaults rewrites the ENCLOSING query's references to
// the lateral's COUNT outputs into `COALESCE(<ref>, 0)`, so a NULL-padded
// outer row reads the 0 an ungrouped COUNT over an empty input has.
//
// It rewrites the SELECT list, WHERE, HAVING and ORDER BY, which is every
// place the enclosing query can name one. `SELECT *` is the boundary and is
// pinned rather than described: a star expands to the raw column and reads
// NULL where PostgreSQL reads 0, because the expansion happens in a later
// pass over the plan's own schema and there is nothing here to rewrite.
func applyLateralEmptyInputDefaults(info *plansql.SelectInfo, alias string, empty lateralEmptyInput) {
	if len(empty.countOutputs) == 0 {
		return
	}
	names := make(map[string]bool, len(empty.countOutputs))
	for _, n := range empty.countOutputs {
		names[strings.ToLower(n)] = true
	}
	alias = strings.ToLower(alias)
	rewrite := func(n plansql.Node) plansql.Node {
		return coalesceLateralCountRefs(n, alias, names)
	}
	for i := range info.Columns {
		if info.Columns[i].ASTExpr == nil {
			continue
		}
		rewritten := rewrite(info.Columns[i].ASTExpr)
		if rewritten == info.Columns[i].ASTExpr {
			continue
		}
		info.Columns[i].ASTExpr = rewritten
		info.Columns[i].Expr = rewritten.String()
	}
	if info.WhereExpr != nil {
		info.WhereExpr = rewrite(info.WhereExpr)
		info.Where = info.WhereExpr.String()
	}
	if info.HavingExpr != nil {
		info.HavingExpr = rewrite(info.HavingExpr)
		info.Having = info.HavingExpr.String()
	}
	for i := range info.OrderBy {
		if info.OrderBy[i].Expr == nil {
			continue
		}
		rewritten := rewrite(info.OrderBy[i].Expr)
		if rewritten == info.OrderBy[i].Expr {
			continue
		}
		info.OrderBy[i].Expr = rewritten
		info.OrderBy[i].Column = rewritten.String()
	}
}

// coalesceLateralCountRefs returns node with every reference to one of the
// lateral's COUNT outputs wrapped in COALESCE(…, 0). It returns the SAME node
// when nothing matched, so a caller can tell a rewrite from a no-op.
func coalesceLateralCountRefs(node plansql.Node, alias string, names map[string]bool) plansql.Node {
	if node == nil {
		return nil
	}
	changed := false
	var walk func(plansql.Node) plansql.Node
	wrap := func(c *plansql.ColRef) plansql.Node {
		changed = true
		return &plansql.FuncCallNode{
			Name: "coalesce",
			Args: []plansql.Node{c, &plansql.Lit{Value: "0", Kind: plansql.LitNumber}},
		}
	}
	walk = func(n plansql.Node) plansql.Node {
		switch e := n.(type) {
		case nil:
			return nil
		case *plansql.ColRef:
			// Qualified by the lateral's own alias, or bare — the enclosing
			// query may write either, and the lateral's output names are the
			// ones it invented.
			if !names[strings.ToLower(e.Column)] {
				return e
			}
			if e.Table == "" || strings.EqualFold(e.Table, alias) {
				return wrap(e)
			}
			return e
		case *plansql.ParenNode:
			return &plansql.ParenNode{Inner: walk(e.Inner)}
		case *plansql.NotNode:
			return &plansql.NotNode{Inner: walk(e.Inner)}
		case *plansql.UnaryOp:
			return &plansql.UnaryOp{Op: e.Op, Inner: walk(e.Inner)}
		case *plansql.AndNode:
			return &plansql.AndNode{Left: walk(e.Left), Right: walk(e.Right)}
		case *plansql.OrNode:
			return &plansql.OrNode{Left: walk(e.Left), Right: walk(e.Right)}
		case *plansql.BinaryOp:
			return &plansql.BinaryOp{Left: walk(e.Left), Op: e.Op, Right: walk(e.Right)}
		case *plansql.CmpExpr:
			return &plansql.CmpExpr{Left: walk(e.Left), Op: e.Op, Right: walk(e.Right)}
		case *plansql.IsExpr:
			return &plansql.IsExpr{Left: walk(e.Left), Not: e.Not, Check: e.Check}
		case *plansql.LikeExpr:
			return &plansql.LikeExpr{Left: walk(e.Left), Not: e.Not, Pattern: walk(e.Pattern)}
		case *plansql.BetweenExpr:
			return &plansql.BetweenExpr{Left: walk(e.Left), Not: e.Not, Low: walk(e.Low), High: walk(e.High)}
		case *plansql.CastNode:
			return &plansql.CastNode{Inner: walk(e.Inner), TypeName: e.TypeName}
		case *plansql.FuncCallNode:
			args := make([]plansql.Node, len(e.Args))
			for i, a := range e.Args {
				args[i] = walk(a)
			}
			return &plansql.FuncCallNode{Name: e.Name, Args: args, Distinct: e.Distinct, Star: e.Star}
		case *plansql.CaseNode:
			cn := &plansql.CaseNode{}
			if e.Subject != nil {
				cn.Subject = walk(e.Subject)
			}
			for _, w := range e.Whens {
				cn.Whens = append(cn.Whens, plansql.WhenClause{Cond: walk(w.Cond), Result: walk(w.Result)})
			}
			if e.Else != nil {
				cn.Else = walk(e.Else)
			}
			return cn
		}
		return n
	}
	out := walk(node)
	if !changed {
		return node
	}
	return out
}
