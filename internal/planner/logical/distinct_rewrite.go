package logical

import (
	"regexp"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// rewriteDistinctAsGroupBy rewrites every user SELECT DISTINCT into an
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
// It runs over the WHOLE tree, not just the root path, because the DAG has
// no execution for NodeDistinct at all: walkStages passes it through as a
// passthrough node and emits no stage (#163). Two compensations hid that —
// this rewrite for the root Distinct, and the coordinator's post-gather
// dedup keyed off MergeInfo.HasDistinct — and a DISTINCT inside a derived
// table whose consumer is an aggregate fell between them, so
// `SELECT COUNT(*) FROM (SELECT DISTINCT c FROM t) u` counted every raw row
// on the DAG and the deduplicated rows single-process (#466). Rewriting the
// Distinct wherever it sits gives it a stage wherever it sits.
//
// Scope:
//   - A Distinct marked BuildSideDedup is planner-inserted (semi/anti build
//     dedup, decorrelated semijoin key source), carries no user-visible
//     semantics, and has dedicated physical handling. Left alone.
//   - Aggregate projections (SELECT DISTINCT a, SUM(b) …) and subquery
//     expressions still fall through. On the root path the coordinator dedup
//     (MergeInfo.HasDistinct) answers them; anywhere else the DAG refuses the
//     query rather than dropping the DISTINCT silently — see
//     physical.refuseUnstageableDistinct.
func rewriteDistinctAsGroupBy(n *Node) *Node {
	if n == nil {
		return n
	}
	for i, child := range n.Children {
		n.Children[i] = rewriteDistinctAsGroupBy(child)
	}
	if n.Type != NodeDistinct || n.BuildSideDedup || len(n.Children) != 1 {
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
	if len(n.CTEs) > 0 {
		proj.CTEs = n.CTEs
	}
	return proj
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
