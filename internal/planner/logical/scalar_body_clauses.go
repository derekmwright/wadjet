// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// scalarBodyIsReproduced reports whether a correlated scalar subquery's body
// can mean what tryDecorrelateScalarSubquery builds for it: ONE aggregate
// over the body's filtered rows, which the rewrite computes per correlation
// key and joins back. Any clause that changes which row, or how many rows,
// the body answers has no place in that plan and was dropped (#1701): a GROUP
// BY on anything but the correlation key or a grouping construct (the body
// answers one row per group, or none, or raises 21000 for several), HAVING
// (it can answer no row), LIMIT 0 or an OFFSET (no row), a set operation, a
// window or QUALIFY. Such a body declines the rewrite and the scalar subquery
// runs per outer row, the path every non-aggregate body already takes, which
// answers PostgreSQL's rows over the #1701 matrix.
//
// grouped=true marks a plain GROUP BY, which the caller keeps only when
// groupedOnCorrelationKey holds once the correlation keys are known.
//
// What it keeps decorrelating, because over one aggregate row they cannot
// change the answer: ORDER BY, DISTINCT, and a positive literal LIMIT
// (existsInvariantLimit's rule).
func scalarBodyIsReproduced(info *plansql.SelectInfo) (ok, grouped bool) {
	if info == nil || info.Union != nil {
		return false, false
	}
	if info.GroupingSets != nil {
		return false, false
	}
	grouped = len(info.GroupBy) > 0 || len(info.GroupByExprs) > 0
	if grouped && len(info.GroupByExprs) != len(info.GroupBy) {
		return false, false
	}
	if info.HavingExpr != nil || strings.TrimSpace(info.Having) != "" {
		return false, false
	}
	if strings.TrimSpace(info.Offset) != "" {
		return false, false
	}
	if lim := strings.TrimSpace(info.Limit); lim != "" && !existsInvariantLimit(lim) {
		return false, false
	}
	if info.QualifyExpr != nil || strings.TrimSpace(info.Qualify) != "" || len(info.Windows) > 0 {
		return false, false
	}
	return true, grouped
}

// scalarKeyCol is the inner side of a correlated equality as written.
type scalarKeyCol struct{ Qual, Col string }

// groupedOnCorrelationKey reports whether a grouped body's GROUP BY terms are
// EXACTLY the inner columns of its correlated equalities: each term a plain
// column reference (parentheses allowed) that names the body's own relation,
// and the set of terms equal to the set of inner key columns. Over the rows
// one outer row matches, every such term holds one value, so the body forms
// one group when a row matches and none when nothing does: the aggregate the
// rewrite computes per key, and the LEFT join's NULL pad for the no-group
// case (PostgreSQL 17.11 answers NULL there, COUNT included, so no COALESCE).
//
// Anything else declines, measured or not shown right: an expression of the
// key (`x.k + 1`; `x.k::text` splits equal keys that print differently), a
// constant beside the key, an enclosing column (`GROUP BY o.k`), or a strict
// subset of two correlation keys. own is the body's FROM namespace (nil when
// unknown), which decides an unqualified term. Over a body reading more than
// one relation (single=false) a term and its key must carry the same
// qualifier, since two relations can publish the same column name.
func groupedOnCorrelationKey(info *plansql.SelectInfo, innerTables map[string]bool, own map[string]bool, keys []scalarKeyCol, single bool) bool {
	norm := func(q, c string) scalarKeyCol {
		if single {
			q = ""
		}
		return scalarKeyCol{Qual: strings.ToLower(q), Col: strings.ToLower(c)}
	}
	want := make(map[scalarKeyCol]bool, len(keys))
	for _, k := range keys {
		if !single && k.Qual == "" {
			return false
		}
		want[norm(k.Qual, k.Col)] = true
	}
	got := make(map[scalarKeyCol]bool, len(keys))
	for _, g := range info.GroupByExprs {
		ref, ok := unparen(g).(*plansql.ColRef)
		if !ok || ref.Column == "" {
			return false
		}
		if !bodyOwnsRef(ref, innerTables, own) {
			return false
		}
		k := norm(ref.Table, ref.Column)
		if !want[k] {
			return false
		}
		got[k] = true
	}
	return len(got) == len(want)
}

// bodyOwnsRef reports whether a column reference provably names the body's
// own relation: by a qualifier the body's FROM publishes, or, unqualified,
// by a name the body's complete FROM namespace supplies (innermost scope
// first, as PostgreSQL binds it).
func bodyOwnsRef(ref *plansql.ColRef, innerTables map[string]bool, own map[string]bool) bool {
	if ref.Table != "" {
		return innerTables[strings.ToLower(ref.Table)]
	}
	return own != nil && own[strings.ToLower(ref.Column)]
}

// bodyOwnColumns is the body's complete FROM namespace, or nil when an item
// cannot be named exactly (bodyOuterColumns' catalog walk).
func bodyOwnColumns(info *plansql.SelectInfo, ctes []plansql.CTEDef, annotate func(*Node)) map[string]bool {
	if info == nil || annotate == nil {
		return nil
	}
	catalog := func(table string) []string {
		n := &Node{Type: NodeScan, TableName: table}
		annotate(n)
		return n.ScanColumns
	}
	return plansql.FromClauseColumns(info, plansql.CTEColumns(scopeCTEs(ctes, info.CTEs), catalog))
}

// rewritesEveryAggregate reports whether the rewrite reproduces the select
// expression's aggregates: it computes ONE aggregate per key and
// ReplaceAggregate puts its slot wherever an aggregate call sits in an
// arithmetic, cast or function-argument position. Two different aggregates
// (`max(c) - min(c)`: both calls read the one slot) or an aggregate where
// ReplaceAggregate does not reach (under a CASE) answered silently wrong, so
// the rewrite declines unless every aggregate call is the same call and the
// rewritten expression holds no aggregate (#1701).
func rewritesEveryAggregate(selectAST, rewritten plansql.Node) bool {
	aggs := plansql.FindAllAggregates(selectAST)
	if len(aggs) == 0 {
		return false
	}
	first := aggs[0].String()
	for _, a := range aggs[1:] {
		if a.String() != first {
			return false
		}
	}
	return len(plansql.FindAllAggregates(rewritten)) == 0
}

func unparen(n plansql.Node) plansql.Node {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			return n
		}
		n = p.Inner
	}
}

// emptyMatchValue is what an aggregate answers over NO input rows when that
// is not NULL. The rewrite's LEFT join pads an outer row whose key matches
// no inner row with NULL, which is the aggregate's empty-input value for
// every aggregate but COUNT and APPROX_DISTINCT: PostgreSQL answers 0 for a
// COUNT over an empty correlated match (#1701), and APPROX_DISTINCT, which
// PostgreSQL does not have, is answered as COUNT(DISTINCT x) is (#1691).
// ok=false for the NULL-on-empty aggregates.
func emptyMatchValue(aggFunc string) (empty plansql.Node, ok bool) {
	switch strings.ToLower(aggFunc) {
	case "count", "approx_distinct":
		return &plansql.Lit{Value: "0", Kind: plansql.LitNumber}, true
	}
	return nil, false
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
