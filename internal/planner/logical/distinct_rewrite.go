package logical

import (
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
//   - `SELECT DISTINCT *` has no Project below it at all — a bare-star
//     select list produces none — so it takes the branch below into
//     rewriteStarDistinct, which reads the group keys off the relation.
//   - Aggregate projections (SELECT DISTINCT a, SUM(b) …) and subquery
//     expressions still fall through: neither has a group key. On the root
//     path the coordinator dedup (MergeInfo.HasDistinct) answers them;
//     anywhere else PlanDistributed refuses the query rather than dropping
//     the DISTINCT silently (physical.refuseUnstageableDistinct) and the
//     coordinator answers it on its local single-process pipeline
//     (Coordinator.runDistinctLocal).
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
		return rewriteStarDistinct(n)
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

// rewriteStarDistinct handles the one user DISTINCT that reaches the rewrite
// with no Project below it: `SELECT DISTINCT *` (and `SELECT DISTINCT t.*`).
// A bare-star select list produces no NodeProject at all, so the Distinct
// sits directly on the relation and the projection-driven path above has no
// projections to read.
//
// Its group keys are the relation's own columns, taken from the same source
// ExpandStarProjections reads when a star SHARES its select list with
// another item (Node.ScanColumns, populated by physical.AnnotateScanColumns
// — see star_expansion.go). Without this the shape was declined here and
// then refused by physical.refuseUnstageableDistinct off the root path, so
// `SELECT COUNT(*) FROM (SELECT DISTINCT * FROM supplier) u` — which
// PostgreSQL and the pre-#466 DAG both answer 100 — came back as an error.
//
// Declaring the keys also fixes what the column pruner did to the shape: a
// Distinct names no columns, so `SELECT COUNT(*) FROM (SELECT DISTINCT *
// FROM lineitem) u` pruned the scan down to one column and deduplicated on
// THAT (14979 distinct l_orderkeys, not 60000 distinct rows), and over a
// join it pruned to zero columns and failed with the schemaless-batch guard
// (#277). The group keys are required columns, so the pruner keeps them.
func rewriteStarDistinct(n *Node) *Node {
	groupBy, ok := starDistinctGroupKeys(n.Children[0])
	if !ok {
		return n
	}
	groupByExprs := make([]plansql.Node, 0, len(groupBy))
	for _, col := range groupBy {
		groupByExprs = append(groupByExprs, &plansql.ColRef{Column: col})
	}
	agg := NewAggregate(n.Children[0], groupBy, nil)
	agg.GroupByExprs = groupByExprs
	// The Distinct may be the plan root, which carries the CTE definitions
	// the physical planner resolves against.
	if len(n.CTEs) > 0 {
		agg.CTEs = n.CTEs
	}
	return agg
}

// starDistinctGroupKeys returns the output columns of a star DISTINCT's
// input relation, in schema order, or ok=false when they are not knowable
// here.
//
// It descends only through nodes that neither add nor drop columns — a
// Filter (the WHERE of `SELECT DISTINCT * FROM t WHERE …`) and a
// both-sides-emitting Join — down to catalog-annotated scans. Anything else
// (a Project, a nested Distinct or Aggregate, a set operation, a semi/anti
// join that emits one side only) means the column set is not the scans'
// concatenation, and guessing it would silently change which rows
// deduplicate against which.
//
// A name appearing in two scans is also declined: the group key would be
// ambiguous, and collapsing two columns into one over-deduplicates.
func starDistinctGroupKeys(n *Node) ([]string, bool) {
	var cols []string
	seen := map[string]bool{}
	var walk func(*Node) bool
	walk = func(cur *Node) bool {
		if cur == nil {
			return false
		}
		switch cur.Type {
		case NodeScan:
			if len(cur.ScanColumns) == 0 {
				return false
			}
			for _, col := range cur.ScanColumns {
				if seen[col] {
					return false
				}
				seen[col] = true
				cols = append(cols, col)
			}
			return true
		case NodeFilter:
			if len(cur.Children) != 1 {
				return false
			}
			return walk(cur.Children[0])
		case NodeJoin:
			switch cur.JoinType {
			case "inner", "left", "right", "full", "cross", "":
			default:
				return false
			}
			if len(cur.Children) != 2 {
				return false
			}
			return walk(cur.Children[0]) && walk(cur.Children[1])
		}
		return false
	}
	if !walk(n) || len(cols) == 0 {
		return nil, false
	}
	return cols, true
}

// projectionGroupKey returns the GROUP BY key string and AST for a
// projection, and whether the projection can serve as a group key.
func projectionGroupKey(p Projection) (string, plansql.Node, bool) {
	if p.IsAgg {
		return "", nil, false
	}
	if p.Expr == "" {
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
	// A subquery projection has no group key — decided on the AST, never on
	// the expression TEXT. A `(?i)\bselect\b` pre-check used to run first,
	// and it fired on any string literal containing the word: `SELECT
	// DISTINCT n_name, 'x select y' FROM nation` inside a derived table was
	// declined here and then REFUSED by refuseUnstageableDistinct, so a
	// query PostgreSQL answers came back as an error on both paths.
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
