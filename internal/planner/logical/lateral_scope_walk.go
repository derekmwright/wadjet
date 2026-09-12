// This file holds the ONE walk arc C1's scope questions are asked of,
// governed by ADR-0021.
package logical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ONE QUESTION, ONE WALK.
//
// Three sites used to ask "does this expression read the outer row / read this
// name / hold a window over the outer row", each with its own test, and the
// three disagreed — which is why four review rounds oscillated between a
// refusal that was too wide and one that was too narrow:
//
//   - the outer-read test walked with `plansql.RewriteExpr`, which does not
//     descend into an AGGREGATE call and treats a WINDOW call as another scope;
//   - the star-list read test was a second `RewriteExpr` walk over the
//     enclosing block only, so a read inside `SUM(l.w) OVER ()`, inside
//     `HAVING MAX(l.w) > 2`, one block up through a star, or in a SECOND
//     lateral's body was invisible and the rename was dropped — four spellings
//     answering plausible NULLs under a sentence promising a refusal;
//   - the window refusal switched on `SelectColumn.IsWindow`, a per-ITEM flag
//     the parser sets only when the item IS a window call, so
//     `(SUM(u.id) OVER ()) + 1` and a window inside a CASE were lowered as
//     projections and answered NULLs.
//
// `walkExprNodes` is the walk all three now use. It visits EVERY node of an
// expression — a function call's arguments whether or not the function is an
// aggregate, a window call's arguments, PARTITION BY, ORDER BY and frame
// offsets, a CASE's subject, conditions, results and ELSE, both sides of every
// operator, a cast's inner, an IN/BETWEEN/LIKE/ANY's operands, an array or
// tuple's elements — and stops at a SUBQUERY or an EXISTS, which are their own
// scope and resolve their own names.
//
// It is exhaustive over `plansql`'s node types by construction: every type that
// carries a child has an arm, and the arms that carry none (a literal, a star,
// a placeholder, an interval) fall through. A missing arm is a silently lost
// read, which is exactly what the three tests above each lost a different piece
// of.
func walkExprNodes(n plansql.Node, visit func(plansql.Node)) {
	if n == nil {
		return
	}
	visit(n)
	each := func(ns []plansql.Node) {
		for _, x := range ns {
			walkExprNodes(x, visit)
		}
	}
	switch t := n.(type) {
	case *plansql.ParenNode:
		walkExprNodes(t.Inner, visit)
	case *plansql.BinaryOp:
		walkExprNodes(t.Left, visit)
		walkExprNodes(t.Right, visit)
	case *plansql.UnaryOp:
		walkExprNodes(t.Inner, visit)
	case *plansql.CmpExpr:
		walkExprNodes(t.Left, visit)
		walkExprNodes(t.Right, visit)
	case *plansql.AndNode:
		walkExprNodes(t.Left, visit)
		walkExprNodes(t.Right, visit)
	case *plansql.OrNode:
		walkExprNodes(t.Left, visit)
		walkExprNodes(t.Right, visit)
	case *plansql.NotNode:
		walkExprNodes(t.Inner, visit)
	case *plansql.IsExpr:
		walkExprNodes(t.Left, visit)
	case *plansql.LikeExpr:
		walkExprNodes(t.Left, visit)
		walkExprNodes(t.Pattern, visit)
	case *plansql.BetweenExpr:
		walkExprNodes(t.Left, visit)
		walkExprNodes(t.Low, visit)
		walkExprNodes(t.High, visit)
	case *plansql.InExpr:
		walkExprNodes(t.Left, visit)
		each(t.Values)
	case *plansql.AnyAllExpr:
		walkExprNodes(t.Left, visit)
		each(t.Values)
	case *plansql.CastNode:
		walkExprNodes(t.Inner, visit)
	case *plansql.FuncCallNode:
		// AN AGGREGATE'S ARGUMENTS ARE VISITED. `RewriteExpr` returns an
		// aggregate call untouched, which is right for a REWRITE — rebinding a
		// name inside one changes what it aggregates — and wrong for a
		// question about what it READS.
		each(t.Args)
	case *plansql.CaseNode:
		walkExprNodes(t.Subject, visit)
		for _, w := range t.Whens {
			walkExprNodes(w.Cond, visit)
			walkExprNodes(w.Result, visit)
		}
		walkExprNodes(t.Else, visit)
	case *plansql.ArrayLitNode:
		each(t.Elements)
	case *plansql.TupleNode:
		each(t.Elements)
	case *plansql.WindowFuncNode:
		if t.Func != nil {
			each(t.Func.Args)
		}
		each(t.PartitionBy)
		for _, ob := range t.OrderBy {
			walkExprNodes(ob.Expr, visit)
		}
		if t.Frame != nil {
			walkExprNodes(t.Frame.Start.Offset, visit)
			if t.Frame.End != nil {
				walkExprNodes(t.Frame.End.Offset, visit)
			}
		}
	case *plansql.SubqueryNode, *plansql.ExistsNode:
		// Its own scope: it resolves its own names, and its text is not a
		// child node this walk could enter anyway.
	}
}

// walkBlockExprs visits every EXPRESSION one query block evaluates: its SELECT
// items (the item, an aggregate's arguments and a window call's own parts), its
// WHERE, HAVING and QUALIFY, its GROUP BY and ORDER BY terms, and every join's
// ON condition.
//
// It is the block-level half of the one walk: `walkExprNodes` says what an
// expression reads, and this says which expressions a block has.
func walkBlockExprs(info *plansql.SelectInfo, visit func(plansql.Node)) {
	walkBlockValueExprs(info, visit)
	if info == nil {
		return
	}
	for _, o := range info.OrderBy {
		walkExprNodes(o.Expr, visit)
	}
}

// walkBlockValueExprs is walkBlockExprs WITHOUT the ORDER BY.
//
// ONE of this arc's two questions excludes a sort term, and only one. "Is this
// body CORRELATED" does: `LATERAL (SELECT 7 AS v ORDER BY u.id)` yields at most
// one row, so its sort is the identity whatever it names, and the same holds
// for a SIBLING lateral's own sort term, which is why the alias-list read test
// asks a sibling body through this function too.
//
// "Does the query read a name the alias list introduces" does NOT exclude it
// any more (f5b0c8f5, round-5 review B2): the rename is dropped on the lowered
// path, so `… l(w) ORDER BY l.w DESC` bound nothing and answered PostgreSQL's
// rows in the opposite order. lateralAliasNameRead therefore asks the ENCLOSING
// block's OrderBy in a second loop of its own, where PostgreSQL's binding rule
// for an output alias can be applied to the whole term.
func walkBlockValueExprs(info *plansql.SelectInfo, visit func(plansql.Node)) {
	if info == nil {
		return
	}
	if info.Union != nil {
		walkBlockValueExprs(info.Union.Left, visit)
		walkBlockValueExprs(info.Union.Right, visit)
	}
	for i := range info.Columns {
		c := &info.Columns[i]
		walkExprNodes(c.ASTExpr, visit)
		walkExprNodes(c.AggArgExpr, visit)
		for _, a := range c.AggArgs {
			walkExprNodes(a, visit)
		}
		if c.WindowSpec != nil {
			walkWindowSpecTerms(c.WindowSpec, visit)
		}
	}
	walkExprNodes(info.WhereExpr, visit)
	walkExprNodes(info.HavingExpr, visit)
	walkExprNodes(info.QualifyExpr, visit)
	for _, g := range info.GroupByExprs {
		walkExprNodes(g, visit)
	}
	for i := range info.Joins {
		walkExprNodes(info.Joins[i].CondExpr, visit)
	}
}

// walkWindowSpecTerms visits a `plansql.WindowSpec`'s PARTITION BY and ORDER BY
// terms, which that struct carries as TEXT rather than as an AST, by parsing
// each one. A term that does not parse is reported to the caller as an opaque
// marker node, so a caller asking "does this read anything" answers yes rather
// than silently no.
func walkWindowSpecTerms(w *plansql.WindowSpec, visit func(plansql.Node)) {
	if w == nil {
		return
	}
	term := func(s string) {
		node, err := plansql.ParseExpression(s)
		if err != nil || node == nil {
			// Unparseable: hand the caller a bare reference to the text, which
			// every caller treats as a read it cannot rule out.
			visit(&plansql.ColRef{Column: s})
			return
		}
		walkExprNodes(node, visit)
	}
	for _, pb := range w.PartitionBy {
		if pb != "" {
			term(pb)
		}
	}
	for _, ob := range w.OrderBy {
		if ob.Column != "" {
			term(ob.Column)
		}
	}
}
