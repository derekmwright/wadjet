// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// scalarBodyIsReproduced reports whether a correlated scalar subquery's body
// means what tryDecorrelateScalarSubquery builds for it: ONE ungrouped
// aggregate over the body's filtered rows, which the rewrite computes per
// correlation key and joins back. Any clause that changes which row, or how
// many rows, the body answers has no place in that plan and was dropped
// (#1701): a GROUP BY or grouping construct (the body answers one row per
// group, or none, or raises 21000 for several), HAVING (it can answer no
// row), LIMIT 0 or an OFFSET (no row), a set operation, a window or QUALIFY.
// Such a body declines the rewrite and the scalar subquery runs per outer
// row, the path every non-aggregate body already takes, which answers
// PostgreSQL's rows over the #1701 matrix.
//
// What it keeps decorrelating, because over one ungrouped aggregate row they
// cannot change the answer: ORDER BY, DISTINCT, and a positive literal LIMIT
// (existsInvariantLimit's rule).
func scalarBodyIsReproduced(info *plansql.SelectInfo) bool {
	if info == nil || info.Union != nil {
		return false
	}
	if len(info.GroupBy) > 0 || len(info.GroupByExprs) > 0 || info.GroupingSets != nil {
		return false
	}
	if info.HavingExpr != nil || strings.TrimSpace(info.Having) != "" {
		return false
	}
	if strings.TrimSpace(info.Offset) != "" {
		return false
	}
	if lim := strings.TrimSpace(info.Limit); lim != "" && !existsInvariantLimit(lim) {
		return false
	}
	if info.QualifyExpr != nil || strings.TrimSpace(info.Qualify) != "" || len(info.Windows) > 0 {
		return false
	}
	return true
}

// emptyMatchValue is what an aggregate answers over NO input rows when that
// is not NULL. The rewrite's LEFT join pads an outer row whose key matches
// no inner row with NULL, which is the aggregate's empty-input value for
// every aggregate but COUNT: PostgreSQL answers 0 for a COUNT over an empty
// correlated match (#1701). ok=false for the NULL-on-empty aggregates.
// decline=true for an aggregate whose empty-input value is not NULL and is
// not spelled here (APPROX_DISTINCT, which PostgreSQL does not have to
// measure): the rewrite declines and the per-row path answers the engine's
// own empty-input value, as the uncorrelated body does.
func emptyMatchValue(aggFunc string) (empty plansql.Node, ok, decline bool) {
	switch strings.ToLower(aggFunc) {
	case "count":
		return &plansql.Lit{Value: "0", Kind: plansql.LitNumber}, true, false
	case "approx_distinct":
		return nil, false, true
	}
	return nil, false, false
}

// coalesceSlot wraps the aggregate's output slot, wherever the rewritten
// select expression reads it, in COALESCE(slot, empty) so an unmatched outer
// row sees the aggregate's empty-input value instead of the join's NULL pad.
func coalesceSlot(expr plansql.Node, slot string, empty plansql.Node) plansql.Node {
	return plansql.RewriteExpr(expr, func(n plansql.Node) (plansql.Node, bool) {
		if ref, ok := n.(*plansql.ColRef); ok && ref.Table == "" && ref.Column == slot {
			return &plansql.FuncCallNode{Name: "coalesce", Args: []plansql.Node{ref, empty}}, true
		}
		return nil, false
	})
}
