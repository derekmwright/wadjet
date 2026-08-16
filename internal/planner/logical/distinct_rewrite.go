package logical

import (
	"regexp"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// rewriteDistinctAsGroupBy rewrites a top-level SELECT DISTINCT into an
// aggregate-free GROUP BY over the projection's expressions:
//
//	Distinct → Project[e1 [AS a1], …] → child
//	⇒ Project[e1 [AS a1], …] → Aggregate{GroupBy: [e1, …]} → child
//
// This is the tree BuildFromSelect produces for the equivalent
// `SELECT e1 … GROUP BY e1, …`, so DISTINCT rides the distributed aggregate
// machinery (fused partial dedup at the scan → hash-partition exchange →
// sharded final merge over disjoint key ranges) instead of the
// coordinator-side dedup fallback, which funnels every pre-dedup row to a
// single node — and which cannot project expression output at the gather.
//
// Scope:
//   - Only the root-path Distinct (ancestors Sort/Limit only) is rewritten.
//     Semi/anti build-side dedup Distincts sit under joins and keep their
//     dedicated handling.
//   - Aggregate projections (SELECT DISTINCT a, SUM(b) …), SELECT DISTINCT *
//     and subquery expressions fall through to the coordinator dedup
//     (MergeInfo.HasDistinct).
func rewriteDistinctAsGroupBy(n *Node) *Node {
	if n == nil {
		return n
	}
	switch n.Type {
	case NodeSort, NodeLimit:
		if len(n.Children) == 1 {
			n.Children[0] = rewriteDistinctAsGroupBy(n.Children[0])
		}
		return n
	case NodeDistinct:
		if len(n.Children) != 1 {
			return n
		}
		proj := n.Children[0]
		if proj.Type != NodeProject || len(proj.Children) != 1 {
			return n
		}
		groupBy := make([]string, 0, len(proj.Projections))
		groupByExprs := make([]plansql.Node, 0, len(proj.Projections))
		seen := make(map[string]bool, len(proj.Projections))
		for _, p := range proj.Projections {
			key, ast, ok := projectionGroupKey(p)
			if !ok {
				return n
			}
			if !seen[key] {
				seen[key] = true
				groupBy = append(groupBy, key)
				groupByExprs = append(groupByExprs, ast)
			}
		}
		if len(groupBy) == 0 {
			return n
		}
		agg := NewAggregate(proj.Children[0], groupBy, nil)
		agg.GroupByExprs = groupByExprs
		proj.Children = []*Node{agg}
		// The Distinct may be the plan root, which carries the CTE
		// definitions the physical planner resolves against.
		proj.CTEs = n.CTEs
		return proj
	}
	return n
}

// subqueryRe conservatively detects a subquery inside an expression. False
// positives only cost the sharded path (the query falls back to coordinator
// dedup); false negatives would group by an unevaluable expression.
var subqueryRe = regexp.MustCompile(`(?i)\bselect\b`)

// projectionGroupKey returns the GROUP BY key string and AST for a
// projection, and whether the projection can serve as a group key.
func projectionGroupKey(p Projection) (string, plansql.Node, bool) {
	if p.IsAgg {
		return "", nil, false
	}
	if p.Expr == "" || subqueryRe.MatchString(p.Expr) {
		return "", nil, false
	}
	ast := p.ASTExpr
	if ast == nil {
		parsed, err := plansql.ParseExpression(p.Expr)
		if err != nil {
			return "", nil, false
		}
		ast = parsed
	}
	if _, isSub := ast.(*plansql.SubqueryNode); isSub {
		return "", nil, false
	}
	// Bare column references group by the column name; expressions group
	// by their raw text — the same strings BuildFromSelect records for an
	// explicit GROUP BY, which the worker-side derived-group-by projection
	// (buildAggInputProjection) evaluates before the partial aggregate.
	if ref, ok := ast.(*plansql.ColRef); ok {
		return ref.String(), ast, true
	}
	return p.Expr, ast, true
}
