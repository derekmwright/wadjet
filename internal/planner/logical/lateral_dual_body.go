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

// lateralDualBody reports whether a LATERAL body has no FROM clause at all —
// the `NodeDual` shape BuildFromSelect gives a table-less SELECT.
func lateralDualBody(info *plansql.SelectInfo) bool {
	return info != nil && len(info.Tables) == 0 && len(info.Joins) == 0
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
	if len(info.OrderBy) > 0 {
		return refuse("an ORDER BY")
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
