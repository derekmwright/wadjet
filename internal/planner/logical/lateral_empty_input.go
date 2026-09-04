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
	// correlationCond is the condition the DECORRELATION produced (the
	// promoted correlated equalities), and onResidual / onResidualExpr are
	// the `ON` the QUERY WROTE, `true` excluded. They are kept apart because
	// the repair may make the join LEFT on the correlation ALONE and move the
	// written ON somewhere the padded row still has to pass it.
	correlationCond string
	onResidual      string
	onResidualExpr  plansql.Node
}

// lateralEmptyInputPlan is which of the three shapes this lateral join is, and
// it exists because the join's OWN `ON` is part of the semantics rather than
// decoration.
//
// PostgreSQL evaluates the lateral subquery ONCE PER OUTER ROW — an ungrouped
// aggregate over an empty input still yields one row — and THEN applies the
// join condition to that (outer row, lateral row) pair, with the join's kind
// deciding what happens to a pair the condition rejects. Three cases follow:
//
//   - No written ON (or `ON true`): the condition rejects nothing, so making
//     the join LEFT on the correlation and defaulting the COUNT outputs IS
//     the semantics, for the INNER and the LEFT spelling alike.
//     (lateralPadOnly)
//
//   - A written ON on an INNER join: the padded row must still be TESTED. An
//     inner join's ON and a WHERE are the same filter, so the join becomes
//     LEFT on the CORRELATION alone — giving every outer row its lateral row
//     — and the ON moves into the enclosing WHERE, where the same default
//     substitution reaches it. `ON s.n = 0` then keeps the unmatched row,
//     which is what PostgreSQL does and what the decorrelation alone cannot.
//     (lateralPadThenFilter)
//
//   - A written ON on an OUTER join: a pair the ON rejects must be KEPT with
//     the lateral side NULL, which needs the lateral columns nulled per
//     column rather than filtered — a CASE per output over a schema this pass
//     does not have. NOT REPAIRED: the join is left exactly as it was written
//     and answers what it answered before this repair existed, which for
//     every ON that an unmatched outer row would fail is PostgreSQL's answer.
//     The one shape it still gets wrong — an ON the DEFAULT row would pass,
//     `LEFT JOIN LATERAL … ON s.n = 0` — is pinned in the census with
//     PostgreSQL's answer beside it. (lateralNoRepair)
//
// A fourth condition cuts across all three and is checked first: if any join
// LATER in the FROM clause is a RIGHT or a FULL join, nothing is repaired at
// all. Such a join manufactures rows in which the lateral's columns are NULL,
// and neither the COALESCE nor the moved ON can tell those from rows the
// lateral produced. See the comment on that branch.
//
// A forced LEFT plus an unconditional default, with no case analysis at all,
// is what turned six PostgreSQL-correct answers into wrong ones: `ON s.n > 5`
// answered three rows for PostgreSQL's none, and printed 0 for counts of 2.
type lateralEmptyInputCase int

const (
	lateralNoRepair lateralEmptyInputCase = iota
	lateralPadOnly
	lateralPadThenFilter
)

func lateralEmptyInputPlan(joinType string, empty lateralEmptyInput, laterNullExtends bool) lateralEmptyInputCase {
	if !empty.ungroupedAggregate {
		return lateralNoRepair
	}
	if laterNullExtends {
		// A RIGHT or FULL join further along the FROM clause MANUFACTURES
		// rows in which the lateral's columns are NULL. Neither half of this
		// repair can tell such a row from one the lateral itself produced:
		// the COALESCE would read a manufactured NULL as 0, and an ON moved
		// into the enclosing WHERE would DELETE the manufactured row instead
		// of leaving it alone. Both are wrong, and both were measured wrong
		// (`... JOIN LATERAL (…) s ON s.n > 1 RIGHT JOIN c ON …` lost the
		// unmatched right row; `… ON true RIGHT JOIN …` printed n=0 where
		// PostgreSQL prints NULL).
		//
		// The repair's rewrites live in the ENCLOSING query — the SELECT
		// list, the WHERE — and so they see the whole FROM clause's result,
		// while what they are entitled to speak about is the LATERAL's own
		// output. While those two are the same relation the repair is sound;
		// a later RIGHT or FULL join is exactly what separates them.
		// Expressing it would need the default applied at the lateral's own
		// output, before the later join sees it, which is a plan-level change
		// rather than a SelectInfo rewrite.
		//
		// So: decline, and leave the query exactly as written. That is what
		// this engine answered before the repair existed, and it is
		// PostgreSQL's answer for every one of these shapes but the ungrouped
		// empty-input row itself, which is pinned as the boundary.
		return lateralNoRepair
	}
	if empty.onResidual == "" {
		return lateralPadOnly
	}
	// An INNER or CROSS join's ON is a filter; anything preserving a side is
	// not, and this pass cannot express the null-fill that would need.
	switch strings.ToLower(strings.TrimSpace(joinType)) {
	case "join", "inner join", "inner", "cross join", "cross", "":
		if empty.onResidualExpr == nil {
			// No AST to move; leave the join as written rather than drop the
			// condition.
			return lateralNoRepair
		}
		return lateralPadThenFilter
	}
	return lateralNoRepair
}

// lateralJoinNullExtendsAfter reports whether any join AFTER position ji in
// the flat, left-deep join list can NULL-EXTEND what is produced to its left.
// A RIGHT or FULL join is exactly that and nothing else is: an INNER join
// only filters, a LEFT join only extends the side it is joining ON.
func lateralJoinNullExtendsAfter(joins []plansql.JoinInfo, ji int) bool {
	if ji < 0 || ji+1 > len(joins) {
		return false
	}
	for _, j := range joins[ji+1:] {
		switch strings.ToLower(strings.TrimSpace(j.Type)) {
		case "right", "right join", "right outer", "right outer join",
			"full", "full join", "full outer", "full outer join":
			return true
		}
	}
	return false
}

// andIntoWhere conjoins one more predicate onto the enclosing query's WHERE.
func andIntoWhere(info *plansql.SelectInfo, raw string, expr plansql.Node) {
	if expr == nil {
		return
	}
	if info.WhereExpr == nil {
		info.WhereExpr = expr
		info.Where = raw
		return
	}
	info.WhereExpr = &plansql.AndNode{Left: info.WhereExpr, Right: expr}
	info.Where = info.WhereExpr.String()
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
		col := &info.Columns[i]
		if col.ASTExpr != nil {
			if rewritten := rewrite(col.ASTExpr); rewritten != col.ASTExpr {
				col.ASTExpr = rewritten
				col.Expr = rewritten.String()
			}
		}
		// An AGGREGATE's argument is a SECOND place the column lives, and the
		// one the builder reads for `SUM(s.n)`: SelectColumn.ASTExpr is the
		// whole item, AggArgExpr / AggArgs / AggArg are what buildAggregate
		// takes the input from. Rewriting only the first left `SUM(s.n)`
		// reading the LEFT join's NULL pad — Carol at NULL for PostgreSQL's
		// 0 on the single-process arm, and a hard `column "s.n" does not
		// exist in the input schema` on both DAG arms.
		if !col.IsAgg {
			continue
		}
		if col.AggArgExpr != nil {
			if rewritten := rewrite(col.AggArgExpr); rewritten != col.AggArgExpr {
				col.AggArgExpr = rewritten
				col.AggArg = rewritten.String()
			}
		}
		for j := range col.AggArgs {
			if col.AggArgs[j] == nil {
				continue
			}
			if rewritten := rewrite(col.AggArgs[j]); rewritten != col.AggArgs[j] {
				col.AggArgs[j] = rewritten
			}
		}
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
//
// The arm list is the whole contract, and a MISSING arm is silent: the
// default case returns the node unwalked, so a reference under it keeps
// reading the LEFT join's NULL. `WHERE s.n IN (0, 2)` dropped the unmatched
// outer row for PostgreSQL's three, because InExpr had no arm while
// BetweenExpr and IsExpr did.
//
// Every plansql node that can CONTAIN a column reference is here:
// ColRef, ParenNode, NotNode, UnaryOp, AndNode, OrNode, BinaryOp, CmpExpr,
// IsExpr, LikeExpr, BetweenExpr, InExpr, AnyAllExpr, CastNode, FuncCallNode,
// CaseNode, ArrayLitNode, TupleNode and WindowFuncNode — every node type in
// internal/planner/sql that holds another node, StarNode and the two
// text-carrying ones excepted.
// SubqueryNode and ExistsNode are deliberately NOT walked: they carry SQL
// TEXT, not a tree, and a lateral output is not in their scope.
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
		case *plansql.InExpr:
			vals := make([]plansql.Node, len(e.Values))
			for i, v := range e.Values {
				vals[i] = walk(v)
			}
			return &plansql.InExpr{Left: walk(e.Left), Not: e.Not, Values: vals}
		case *plansql.AnyAllExpr:
			out := *e
			out.Left = walk(e.Left)
			if e.Values != nil {
				vals := make([]plansql.Node, len(e.Values))
				for i, v := range e.Values {
					vals[i] = walk(v)
				}
				out.Values = vals
			}
			return &out
		case *plansql.ArrayLitNode:
			els := make([]plansql.Node, len(e.Elements))
			for i, v := range e.Elements {
				els[i] = walk(v)
			}
			return &plansql.ArrayLitNode{Elements: els}
		case *plansql.TupleNode:
			els := make([]plansql.Node, len(e.Elements))
			for i, v := range e.Elements {
				els[i] = walk(v)
			}
			return &plansql.TupleNode{Elements: els}
		case *plansql.WindowFuncNode:
			out := *e
			if e.Func != nil {
				if fn, ok := walk(e.Func).(*plansql.FuncCallNode); ok {
					out.Func = fn
				}
			}
			if e.PartitionBy != nil {
				pb := make([]plansql.Node, len(e.PartitionBy))
				for i, v := range e.PartitionBy {
					pb[i] = walk(v)
				}
				out.PartitionBy = pb
			}
			if e.OrderBy != nil {
				ob := make([]plansql.WindowOrderBy, len(e.OrderBy))
				copy(ob, e.OrderBy)
				for i := range ob {
					ob[i].Expr = walk(ob[i].Expr)
				}
				out.OrderBy = ob
			}
			return &out
		}
		return n
	}
	out := walk(node)
	if !changed {
		return node
	}
	return out
}
