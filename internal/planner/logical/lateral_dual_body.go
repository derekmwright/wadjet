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
// relation of its own, so there is nothing else a name could resolve to — except
// the body's OWN output names, which a group or sort term may spell.
//
// It asks THE ONE WALK (lateral_scope_walk.go), which descends into an
// aggregate's arguments, a window call's arguments and parts, a CASE and every
// operator, and stops at a subquery. Three sites used to ask this question three
// different ways and disagree; that is what four review rounds of oscillation
// between a too-wide and a too-narrow refusal were made of.
func lateralBodyReadsOuterRow(info *plansql.SelectInfo) bool {
	if info == nil {
		return false
	}
	own := lateralBodyOwnNames(info)
	found := false
	walkLateralBodyTerms(info, func(n plansql.Node) {
		if ref, ok := n.(*plansql.ColRef); ok && !isOwnName(ref, own) {
			found = true
		}
	})
	return found
}

// walkLateralBodyTerms visits every expression of a table-less LATERAL body that
// decides what the body READS — `walkBlockExprs` minus the ORDER BY.
//
// A SORT TERM IS DELIBERATELY NOT ASKED. A table-less body yields at most one
// row, so its ORDER BY is the identity whatever it names, and
// `(SELECT 7 AS v ORDER BY u.id)` is a shape the base answers exactly as
// PostgreSQL does. The lowering drops the sort rather than refusing the body.
func walkLateralBodyTerms(info *plansql.SelectInfo, visit func(plansql.Node)) {
	if info == nil {
		return
	}
	if info.Union != nil {
		walkLateralBodyTerms(info.Union.Left, visit)
		walkLateralBodyTerms(info.Union.Right, visit)
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
}

// lateralBodyOwnNames is the set of output names a body publishes; they are not
// outer columns however a term spells them.
func lateralBodyOwnNames(info *plansql.SelectInfo) map[string]bool {
	own := map[string]bool{}
	for i := range info.Columns {
		if a := strings.ToLower(strings.TrimSpace(info.Columns[i].Alias)); a != "" {
			own[a] = true
		}
	}
	return own
}

// isOwnName reports whether a reference names the body's own output rather than
// anything outside it. Only a BARE reference can: a qualified one names a
// relation, and a table-less body is not one.
func isOwnName(ref *plansql.ColRef, own map[string]bool) bool {
	return ref.Table == "" && own[strings.ToLower(strings.TrimSpace(ref.Column))]
}

// lateralBodyHasOuterWindow reports whether ANY window call in a table-less body
// — the whole item, or one nested in an expression or a CASE — reads the outer
// row.
//
// It replaces the `SelectColumn.IsWindow` flag the refusal used to switch on.
// The parser sets that only when the item IS a window call, so
// `(SELECT (SUM(u.id) OVER ()) + 1 AS v)` and a window inside a CASE were judged
// ordinary projections and lowered — and a window call evaluated in a projection
// has no frame to evaluate over, so every value came back NULL (round-4 review,
// B3).
func lateralBodyHasOuterWindow(info *plansql.SelectInfo) bool {
	if info == nil {
		return false
	}
	own := lateralBodyOwnNames(info)
	found := false
	walkLateralBodyTerms(info, func(n plansql.Node) {
		if found {
			return
		}
		w, ok := n.(*plansql.WindowFuncNode)
		if !ok {
			return
		}
		walkExprNodes(w, func(x plansql.Node) {
			if ref, ok := x.(*plansql.ColRef); ok && !isOwnName(ref, own) {
				found = true
			}
		})
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
	// A WINDOW CALL ANYWHERE IN AN ITEM, not just as the whole item: the one
	// walk finds it inside an expression, inside a CASE, inside another call.
	// Switching on `SelectColumn.IsWindow` — a flag the parser sets only when
	// the item IS a window call — lowered `(SUM(u.id) OVER ()) + 1` as an
	// ordinary projection, where a window has no frame to evaluate over and
	// every value came back NULL (round-4 review, B3).
	if lateralBodyHasOuterWindow(info) {
		return refuse("a window function")
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

// refuseLateralAliasListOverStar refuses a LATERAL FROM item's column-alias
// list over a body whose SELECT list holds a STAR — but ONLY when the enclosing
// query READS a name the list introduces.
//
// The width of a star is not knowable in the builder, and the two other FROM
// items that carry a list — a CTE and a derived table — DEFER the rename to the
// pass that knows it. A LATERAL cannot: the decorrelation JOINS on the column
// its correlated predicate names, and a deferred positional rename could take
// that column and leave the join keying on a name nothing carries — measured as
// ZERO ROWS for the two-alias spelling, which is a silent wrong answer traded
// for a silent wrong answer.
//
// THE TRIGGER IS THE READ, NOT THE LIST. PostgreSQL applies a SHORT list to the
// first k columns of the star's expansion and leaves the rest under their own
// names, so a query that never mentions a renamed name is unaffected by the
// rename: `SELECT u.id … l(w)` and `SELECT l.amount … l(w)` answer PostgreSQL's
// rows at every base, and refusing them on the PRESENCE of the list was ten
// cells right → refused (round-3 review, B2). Only a query that reads `l.w` —
// the name the list introduces and the expansion cannot be counted to produce —
// is refused.
func refuseLateralAliasListOverStar(outer *plansql.SelectInfo,
	info *plansql.SelectInfo, join plansql.JoinInfo) error {

	if info == nil || join.RightTableRef == nil || len(join.RightTableRef.ColumnAliases) == 0 {
		return nil
	}
	cols := info
	for cols.Union != nil && cols.Union.Left != nil {
		cols = cols.Union.Left
	}
	star := false
	for i := range cols.Columns {
		if cols.Columns[i].Star {
			star = true
			break
		}
	}
	if !star {
		return nil
	}
	read := lateralAliasNameRead(outer, join)
	if read == "" {
		return nil
	}
	name := join.RightAlias
	if name == "" {
		name = "subquery"
	}
	return sqlerr.New("0A000",
		"the query reads %q, a name the column-alias list on table %q introduces over a "+
			"LATERAL subquery whose SELECT list holds a `*`: the width of the star is not "+
			"known where the rename must be made, and a LATERAL is run as a join on the "+
			"column its correlated predicate names, so a positional rename could take that "+
			"column and leave the join with no key. Name the subquery's columns instead",
		read, name)
}

// lateralAliasNameRead is the first name the FROM item's column-alias list
// introduces that the ENCLOSING query actually reads, or "" when it reads none.
//
// It asks THE ONE WALK (lateral_scope_walk.go) over every expression the
// enclosing block evaluates, so a read inside an AGGREGATE (`HAVING MAX(l.w) >
// 2`) or inside a WINDOW call (`SUM(l.w) OVER ()`) is seen — `RewriteExpr`
// enters neither, and four spellings of "the query reads w" were invisible, so
// the rename was dropped and the query answered plausible NULLs under a
// sentence promising a refusal (round-4 review, B2).
//
// Two more positions the block's own expressions do not hold:
//
//   - A LATER FROM ITEM'S BODY. A second lateral may read the first one's
//     renamed column (`…, LATERAL (SELECT l.w + 100 AS z) m`), and that body is
//     parsed text hanging off the join rather than an expression of this block.
//   - ONE BLOCK UP, through a STAR. When the enclosing block itself selects a
//     star, every name it holds — the renamed ones included — is republished to
//     whatever reads that block, which this layer cannot see. A star is
//     therefore treated as a read of every name the list introduces: the
//     alternative is `SELECT x.w FROM (SELECT * … l(w)) x` answering NULL.
//
// A SORT TERM IS NOT ASKED, for the reason walkBlockValueExprs gives: it decides
// the order and never the values, and `… l(w) ORDER BY l.w` answers at main
// because the base path applies the rename before the sort. It keeps answering.
//
// A reference qualified by the lateral's own alias is certainly a read; a BARE
// reference of the same name is treated as one too, because the alternative is
// to answer it from a column the rename was supposed to have replaced.
func lateralAliasNameRead(outer *plansql.SelectInfo, join plansql.JoinInfo) string {
	if outer == nil || join.RightTableRef == nil {
		return ""
	}
	want := map[string]string{}
	var first string
	for _, a := range join.RightTableRef.ColumnAliases {
		if a = strings.TrimSpace(a); a != "" {
			want[strings.ToLower(a)] = a
			if first == "" {
				first = a
			}
		}
	}
	if len(want) == 0 {
		return ""
	}
	alias := strings.ToLower(strings.TrimSpace(join.RightAlias))
	hit := ""
	see := func(n plansql.Node) {
		if hit != "" {
			return
		}
		ref, ok := n.(*plansql.ColRef)
		if !ok {
			return
		}
		t := strings.ToLower(strings.TrimSpace(ref.Table))
		if t != "" && t != alias {
			return
		}
		if a, ok := want[strings.ToLower(strings.TrimSpace(ref.Column))]; ok {
			hit = a
		}
	}
	walkBlockValueExprs(outer, see)
	if hit != "" {
		return hit
	}
	// A STAR in the enclosing block republishes this lateral's names — but only
	// a star that COVERS this lateral. `SelectColumn.Star` is set for a
	// QUALIFIED star too, and `SELECT u.*` republishes the OUTER relation's
	// columns and never `l`'s: keying on the flag alone refused a query that
	// reads no name the list introduces (round-5 review, B1).
	for i := range outer.Columns {
		if !outer.Columns[i].Star {
			continue
		}
		q := strings.ToLower(strings.TrimSpace(outer.Columns[i].TableRef))
		if q == "" || q == alias {
			return first
		}
	}
	// A LATER FROM item's body, and only a reference in it that resolves to
	// THIS lateral. A sibling whose OWN output column is called `w` reads its
	// own `w`, not this list's, so the bare spelling is not enough there.
	for i := range outer.Joins {
		other := outer.Joins[i]
		if !other.Lateral || other.RightTable == join.RightTable || alias == "" {
			continue
		}
		body, err := lateralBodySelect(other)
		if err != nil || body == nil {
			continue
		}
		walkBlockExprs(body, func(n plansql.Node) {
			if hit != "" {
				return
			}
			ref, ok := n.(*plansql.ColRef)
			if !ok || !strings.EqualFold(strings.TrimSpace(ref.Table), join.RightAlias) {
				return
			}
			if a, ok := want[strings.ToLower(strings.TrimSpace(ref.Column))]; ok {
				hit = a
			}
		})
		if hit != "" {
			return hit
		}
	}
	return ""
}
