// This file holds the TABLE-LESS LATERAL lowering, governed by ADR-0021.
package logical

import (
	"errors"
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A LATERAL body with NO FROM clause is a PROJECTION OVER THE OUTER ROW.
//
// `FROM u, LATERAL (SELECT e(u) AS v) l` yields exactly one row per outer row
// whose columns are functions of that row, so the relation the body ranges
// over is the outer row itself and the lowering is `π(u.*, e(u) AS v)(u)` —
// no dependent join, no correlation key, nothing to decorrelate.
//
// The lowering it USED to get was the correlation machinery, which reads the
// body's WHERE and promotes the equalities it finds there into join keys. A
// body with no WHERE has no correlated part at all, so `u.id` in the SELECT
// list was promoted nowhere: `BuildFromSelect` built `Project(Dual, [u.id AS
// v])`, the Dual carries no `u`, and every projected value came back NULL
// under the STRING default an unresolvable reference falls to — three NULLs
// and OID 25 where PostgreSQL 17.11 answers 1, 2, 3 as bigint (#1033).
// COUNT(*) was right throughout: the ROWS were produced and only the VALUES
// were lost. The position is ADR-0021 §1l.

// lateralDualBody reports whether a LATERAL body is one this lowering owns: no
// FROM clause at all — the `NodeDual` shape BuildFromSelect gives a table-less
// SELECT — AND a body that actually READS THE OUTER ROW.
//
// BOTH HALVES ARE LOAD-BEARING. A table-less body that names no column is not
// correlated at all: nothing about it depends on the outer row, the ordinary
// build produces `Project(Dual, …)`, the cross join with its one row is exactly
// PostgreSQL's answer, and it needs no lowering. Classifying by the FROM clause
// alone took twelve such shapes — `(SELECT 7 AS v LIMIT 1)`, `(SELECT DISTINCT
// 7 AS v)`, `(SELECT COUNT(*) AS c)`, `(SELECT ROW_NUMBER() OVER () AS v)`,
// `(SELECT 1 AS v UNION ALL SELECT 2)`, `LEFT JOIN … ON false`, and the rest —
// from PostgreSQL's own rows to a 0A000 refusal, 60 cells across five arms
// (round-2 review, B1). The refusals are right for a CORRELATED body, where the
// base answered NULLs; they are a loss of working SQL for an uncorrelated one.
// The whole table is `arc_c1_body_class_two_path_test.go`.
func lateralDualBody(info *plansql.SelectInfo) bool {
	return info != nil && len(info.Tables) == 0 && len(info.Joins) == 0 &&
		lateralBodyReadsOuterRow(info)
}

// lateralBodyReadsOuterRow reports whether a TABLE-LESS body names any column.
//
// In such a body every column reference IS an outer reference: the body has no
// relation of its own, so there is nothing else a name could resolve to. That
// is what makes this test exact rather than a heuristic — it does not have to
// know which relations are outside, only that a name is read at all.
//
// A SUBQUERY is opaque: its own column references belong to its own FROM.
// `(SELECT (SELECT MAX(id) FROM t) AS v)` reads no outer column, and walking
// into it would have said it does.
//
// The join's written ON is deliberately NOT part of this: `LEFT JOIN LATERAL
// (SELECT 7 AS v) l ON u.id > 1` names the outer row in the JOIN, not in the
// body, and PostgreSQL evaluates the body once per outer row regardless.
func lateralBodyReadsOuterRow(info *plansql.SelectInfo) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		return lateralBodyReadsOuterRow(info.Union.Left) ||
			lateralBodyReadsOuterRow(info.Union.Right)
	}
	// The body's OWN output names are not outer columns. A sort or a group term
	// may name one — this parser resolves `ORDER BY 1` to the item's alias —
	// and `(SELECT 7 AS v ORDER BY 1)` reads nothing at all.
	own := map[string]bool{}
	for i := range info.Columns {
		if a := strings.ToLower(strings.TrimSpace(info.Columns[i].Alias)); a != "" {
			own[a] = true
		}
	}
	reads := func(n plansql.Node) bool { return exprReadsAColumnOutside(n, own) }
	for i := range info.Columns {
		c := &info.Columns[i]
		if reads(c.ASTExpr) || reads(c.AggArgExpr) {
			return true
		}
		for _, a := range c.AggArgs {
			if reads(a) {
				return true
			}
		}
		if c.WindowSpec != nil && windowSpecReadsAColumn(c.WindowSpec, own) {
			return true
		}
	}
	if reads(info.WhereExpr) || reads(info.HavingExpr) || reads(info.QualifyExpr) {
		return true
	}
	for _, g := range info.GroupByExprs {
		if reads(g) {
			return true
		}
	}
	// A SORT TERM IS DELIBERATELY NOT ASKED. A table-less body yields at most
	// one row, so its ORDER BY is the identity whatever it names — and
	// `(SELECT 7 AS v ORDER BY u.id)` is a shape the base answered exactly as
	// PostgreSQL does (round-2 review, B1). Counting the term made the body
	// "correlated", and a sort is not a projection, so it was refused.
	return false
}

// windowSpecReadsAColumn is lateralBodyReadsOuterRow over a window's own
// partition, order and frame terms, which `plansql.WindowSpec` carries as TEXT
// rather than as an AST.
//
// EACH TERM IS PARSED AND RESOLVED, not tested for being non-empty. Treating
// any non-empty term as a column read made `OVER (ORDER BY 1)` and
// `OVER (PARTITION BY 1)` "reads the outer row" — an integer LITERAL — and a
// window body is not a projection, so the shape was refused where the base
// answered PostgreSQL's own rows (round-2 review, B1). A false positive here is
// not a lost optimization: `lateralDualBody` returning true is what ARMS the
// refusal, so it costs the answer.
func windowSpecReadsAColumn(w *plansql.WindowSpec, own map[string]bool) bool {
	if w == nil {
		return false
	}
	for _, pb := range w.PartitionBy {
		if termReadsAColumnOutside(pb, own) {
			return true
		}
	}
	for _, ob := range w.OrderBy {
		if termReadsAColumnOutside(ob.Column, own) {
			return true
		}
	}
	if w.Frame != nil {
		if exprReadsAColumnOutside(w.Frame.Start.Offset, own) {
			return true
		}
		if w.Frame.End != nil && exprReadsAColumnOutside(w.Frame.End.Offset, own) {
			return true
		}
	}
	return false
}

// termReadsAColumnOutside parses one TEXT term and asks the AST whether it
// reads a column that is not the body's own output.
//
// A term this cannot parse is treated as a read, which is the safe side for a
// term whose shape is unknown: the lowering declines and the base path answers.
func termReadsAColumnOutside(term string, own map[string]bool) bool {
	term = strings.TrimSpace(term)
	if term == "" {
		return false
	}
	node, err := plansql.ParseExpression(term)
	if err != nil {
		return true
	}
	return exprReadsAColumnOutside(node, own)
}

// exprReadsAColumnOutside reports whether an expression tree holds a column
// reference to something OTHER than the body's own output names.
//
// A subquery is OPAQUE — its references are its own FROM's — and it needs no
// special case here: `plansql.SubqueryNode` carries its body as TEXT rather
// than as child nodes, so the walk cannot enter one.
func exprReadsAColumnOutside(n plansql.Node, own map[string]bool) bool {
	if n == nil {
		return false
	}
	found := false
	plansql.RewriteExpr(n, func(x plansql.Node) (plansql.Node, bool) {
		ref, ok := x.(*plansql.ColRef)
		if !ok {
			return nil, false
		}
		if ref.Table == "" && own[strings.ToLower(strings.TrimSpace(ref.Column))] {
			return nil, false
		}
		found = true
		return nil, false
	})
	return found
}

// refuseUnloweredTableLessLateral names the one body class or join shape that
// keeps this lateral out of the projection lowering, or nil when none does.
//
// WHAT IS NOT LOWERED IS REFUSED, LOUDLY. An aggregate, a GROUP BY, a window,
// DISTINCT, a set operation, a sort or a limit, a CTE, a star and a subquery
// in an item are not projections over the outer row, and an outer join whose
// condition does not fold to true pads rows this shape cannot express. Each of
// them answered plausible NULLs before; each is 0A000 now.
//
// Every class here is a shape PostgreSQL ANSWERS, so the SQLSTATE is 0A000 and
// not a syntax or a name error: what this engine cannot do is compute it, not
// read it.
func refuseUnloweredTableLessLateral(info *plansql.SelectInfo, join plansql.JoinInfo) error {
	refuse := func(what string) error {
		return sqlerr.New("0A000",
			"a LATERAL subquery with no FROM clause is computed as a projection over the "+
				"outer row, and %s is not one: %s. Give the subquery a FROM clause, or move "+
				"the computation into the enclosing query",
			what, strings.TrimSpace(join.RightTable))
	}
	if info.Union != nil {
		return refuse("a set operation")
	}
	if info.Distinct {
		return refuse("DISTINCT")
	}
	if len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 {
		return refuse("a GROUP BY")
	}
	if info.Having != "" || info.HavingExpr != nil {
		return refuse("a HAVING clause")
	}
	if info.Qualify != "" || info.QualifyExpr != nil {
		return refuse("a QUALIFY clause")
	}
	if info.Limit != "" || info.Offset != "" {
		return refuse("a LIMIT or OFFSET")
	}
	if len(info.CTEs) > 0 {
		return refuse("a WITH clause")
	}
	if len(info.Columns) == 0 {
		return refuse("an empty SELECT list")
	}
	for _, col := range info.Columns {
		switch {
		case col.Star:
			return refuse("a star")
		case col.IsAgg:
			return refuse("an aggregate")
		case col.IsWindow:
			return refuse("a window function")
		case col.ASTExpr == nil:
			return refuse("an item this planner did not parse into an expression")
		case exprCarriesSubqueryNode(col.ASTExpr):
			return refuse("a subquery in the SELECT list")
		}
	}
	// An OUTER join pads the rows its condition rejects, and a table-less body
	// always offers exactly one row — so the two shapes this lowering can
	// express are an inner/cross join (the condition is a filter above the
	// projection, which for an inner join is a WHERE) and an outer join whose
	// condition folds to TRUE (nothing is ever padded). Anything else needs
	// the pad, which a projection cannot manufacture.
	if !lateralDualInnerJoin(join.Type) && !onFoldsToTrue(join.CondExpr, join.Condition) {
		return refuse("an outer join whose ON condition does not fold to true")
	}
	if !lateralDualInnerJoin(join.Type) && info.Where != "" {
		return refuse("an outer join over a body with a WHERE clause")
	}
	return nil
}

// lateralDualInnerJoin reports whether this join keeps every row its condition
// accepts and pads none — the shape whose ON is exactly a WHERE.
func lateralDualInnerJoin(joinType string) bool {
	switch strings.ToLower(strings.TrimSpace(joinType)) {
	case "", "join", "inner", "inner join", "cross", "cross join":
		return true
	}
	return false
}

// exprCarriesSubqueryNode reports whether an expression tree holds a subquery
// or an EXISTS. A projection over the outer row is compiled by
// internal/engine/expr, which evaluates no second query.
func exprCarriesSubqueryNode(n plansql.Node) bool {
	found := false
	plansql.RewriteExpr(n, func(x plansql.Node) (plansql.Node, bool) {
		switch x.(type) {
		case *plansql.SubqueryNode, *plansql.ExistsNode:
			found = true
		}
		return nil, false
	})
	return found
}

// buildTableLessLateralJoin lowers `left CROSS JOIN LATERAL (SELECT … )` whose
// body has no FROM clause into `Join(left, Project(Dual, items), "cross")`
// carrying the body's items on the join, and moves the body's WHERE and the
// written ON into the ENCLOSING query's WHERE — which for an inner join is
// exactly where they already apply, and is the same move
// `lateralPadThenFilter` makes for the empty-input repair.
//
// info is the ENCLOSING block, and it is mutated: that is how the two
// predicates reach a place that runs ABOVE the projection this lowering
// produces, rather than a join condition there is no join operator to hold.
//
// The JOIN NODE stays in the tree, with the Dual still underneath, for two
// reasons that are one decision. The declaration walks read the right
// subtree's projections through `inputColDecls`, which the Dual's
// `LateralOuterScope` answers with the OUTER row's columns — so `v` declares
// what `u.id` declares, at every one of those walks at once. And the stage DAG
// hands any plan containing a Dual to the coordinator's in-process pipeline
// (physical.ErrTableLessSelectDistributed, #806), which is what keeps all five
// arms answering through one engine rather than needing the distributed
// single-row source that refusal exists for.
func buildTableLessLateralJoin(info *plansql.SelectInfo, left *Node,
	join plansql.JoinInfo, ctes []plansql.CTEDef) (*Node, error) {

	inner := join.RightTable[1 : len(join.RightTable)-1]
	parsed, err := plansql.Parse(inner)
	if err != nil {
		return nil, fmt.Errorf("parsing LATERAL subquery: %w", err)
	}
	subInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extracting SELECT from LATERAL subquery: %w", err)
	}
	if err := refuseUnloweredTableLessLateral(subInfo, join); err != nil {
		return nil, err
	}

	// The body's own WHERE is a predicate over the outer row and constants —
	// there is no inner relation for it to name — so on an inner join it is
	// the enclosing WHERE under another spelling. Move it whole rather than
	// splitting it into "correlated" and "local" parts: that split exists to
	// decide which half becomes a join key, and here neither half can.
	if subInfo.WhereExpr != nil {
		andIntoWhere(info, subInfo.Where, subInfo.WhereExpr)
	}
	subInfo.Where, subInfo.WhereExpr = "", nil
	if join.CondExpr != nil && !onFoldsToTrue(join.CondExpr, join.Condition) {
		andIntoWhere(info, join.Condition, join.CondExpr)
	}

	// THE FROM ITEM'S COLUMN-ALIAS LIST renames the body's items POSITIONALLY,
	// which is PostgreSQL's rule and the one `plansql.OverlayColumnAliases`
	// already states for a derived table. The lowering publishes each item
	// under its OWN alias, so without this `LATERAL (SELECT u.id AS v) l(w)`
	// renamed a column nothing carried and `l.w` answered NULL — #1033's own
	// headline shape under a second spelling (round-2 review, P2/B2ii). A list
	// LONGER than the body is PostgreSQL's 42P10 and is raised by
	// RefuseUnappliedColumnAliasLists, which sees the unapplied list.
	if err := applyLateralItemAliases(subInfo, join); err != nil {
		return nil, err
	}
	// A ONE-ROW SORT IS THE IDENTITY. The body has no FROM clause, so it yields
	// at most one row and its ORDER BY cannot reorder anything; dropping it is
	// what keeps the body's root a Project, which is what this lowering reads
	// its items from. (A LIMIT or OFFSET, where the order WOULD matter, is
	// refused above.)
	subInfo.OrderBy = nil

	right, err := BuildFromSelectWithCTEs(subInfo, ctes)
	if err != nil {
		return nil, fmt.Errorf("building LATERAL subquery plan: %w", err)
	}
	if join.RightAlias != "" {
		setSubtreeAlias(right, join.RightAlias)
	}
	right.LateralSubtree = true
	// The Dual's input scope IS the outer row, which is what lets every
	// declaration walk type `u.id` in the body's SELECT list. Stamped on the
	// Dual and not on the Project because `inputColDecls` asks a node what its
	// CHILD publishes, and the Dual is that child.
	stampLateralOuterScope(right, left)

	lat := NewJoin(left, right, "cross", "")
	lat.LateralDualItems = append([]Projection(nil), right.Projections...)
	lat.LateralDualAlias = join.RightAlias
	return lat, nil
}

// stampLateralOuterScope records the outer subtree on the Dual at the root of
// a table-less LATERAL body. The pointer is to the SAME nodes the join's left
// child holds, so the scan annotation physical.AnnotateScanColumns stamps
// there is visible here without a second pass.
func stampLateralOuterScope(n *Node, outer *Node) {
	for cur := n; cur != nil; {
		if cur.Type == NodeDual {
			cur.LateralOuterScope = outer
			return
		}
		if len(cur.Children) != 1 {
			return
		}
		cur = cur.Children[0]
	}
}

// lateralBodySelect parses a LATERAL join's parenthesised right side, so the
// caller can ask what SHAPE the body is before choosing a lowering. The parse
// is thrown away: each lowering re-parses and MUTATES its own copy — the
// table-less one moves the body's WHERE into the enclosing block, and the
// decorrelating one rewrites the SELECT list — so neither may hold the
// memoized tree the binder validated (ADR-0032).
func lateralBodySelect(join plansql.JoinInfo) (*plansql.SelectInfo, error) {
	if !strings.HasPrefix(join.RightTable, "(") || !strings.HasSuffix(join.RightTable, ")") {
		return nil, errNotALateralBody
	}
	parsed, err := plansql.Parse(join.RightTable[1 : len(join.RightTable)-1])
	if err != nil {
		return nil, err
	}
	return plansql.ExtractSelect(parsed)
}

// errNotALateralBody says the right side of this join is not a parenthesised
// SELECT, so there is no body to classify.
var errNotALateralBody = errors.New("lateral right side is not a subquery")

// applyLateralItemAliases renames a LATERAL body's items to the FROM item's
// COLUMN-ALIAS LIST — `LATERAL (…) l(w, x)` — POSITIONALLY, which is
// PostgreSQL's rule and the one `plansql.OverlayColumnAliases` already states
// for a derived table.
//
// Both lateral lowerings call it, and both call it FIRST: the decorrelating one
// injects correlation keys into the same list, and a rename applied after that
// would rename the wrong positions. Without it the lowering published each item
// under its own alias and the list renamed a column nothing carried, so
// `SELECT l.w FROM … LATERAL (SELECT u.id AS v) l(w)` answered three NULLs —
// #1033's own headline shape under a second spelling (round-2 review, P2).
//
// A body whose width is not knowable — one carrying a star — is left alone:
// `RefuseUnappliedColumnAliasLists` raises PostgreSQL's 42P10 for a list the
// expansion could not apply, and guessing the width here is what that refusal
// exists to prevent.
func applyLateralItemAliases(info *plansql.SelectInfo, join plansql.JoinInfo) error {
	if info == nil || join.RightTableRef == nil {
		return nil
	}
	aliases := join.RightTableRef.ColumnAliases
	if len(aliases) == 0 {
		return nil
	}
	for i := range info.Columns {
		if info.Columns[i].Star {
			return nil
		}
	}
	if len(aliases) > len(info.Columns) {
		return sqlerr.New("42P10",
			"table %q has %d columns available but %d columns specified",
			join.RightAlias, len(info.Columns), len(aliases))
	}
	for i, name := range aliases {
		info.Columns[i].Alias = name
		info.Columns[i].PublishedName = name
	}
	return nil
}
