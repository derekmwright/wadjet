package logical

// AggScopePreservingWrapper is THE shared list preserving an aggregate's OWN
// output names (ADR-0026 §4): Filter, Sort, Limit and Window; Window only appends.
// physical.aggregateUnderOutput, findAggregateAncestor, groupKeysPublishedBelow
// and AggregateOverGroupRows must read this list (#774); the physical wrapper
// only delegates. TestAggScopePreservingWrapperIsReadByEveryWalk covers all four.
// Aggregate replaces the schema and is the target, not a wrapper; Project handling
// belongs to each caller. NodeDistinct is deliberately absent: DISTINCT over grouped
// queries lowers to another Aggregate, so adding it would admit an untested path.
// See docs/internals/aggregate-scope-preserving-wrappers.md for the design.
func AggScopePreservingWrapper(t NodeType) bool {
	switch t {
	case NodeFilter, NodeSort, NodeLimit, NodeWindow:
		return true
	}
	return false
}

// AggregateOverGroupRows returns the Aggregate whose GROUP rows this Project's
// input carries, or nil when the input is something else.
//
// It is NOT AggregateBelowProject, and the difference is the fourth reader
// #774 was hiding in. AggregateBelowProject answers "which aggregate STAGE
// does this Project sit directly on top of", for two consumers that then map
// the SELECT list onto that stage — and a Sort or a WINDOW between the two
// emits a stage of its own, so stopping at one is right there.
//
// The question HERE is only about the ROWS: below any of the wrappers above,
// there is still exactly one row per group and the aggregate's input columns
// are gone. `SELECT g + 1 AS k, ROW_NUMBER() OVER (ORDER BY g + 1) AS rn FROM
// t GROUP BY g + 1` wrapped in a derived table and filtered `WHERE k > 3`
// substituted `k` away to `(g + 1)` and pushed it below the Project, where it
// met the WINDOW's output — which carries the key under its published NAME and
// no `g` at all. The predicate was UNKNOWN on every row and a filter admits
// only TRUE: zero rows on all four arms where PostgreSQL answers four (#774).
func AggregateOverGroupRows(n *Node) *Node {
	if n == nil || n.Type != NodeProject {
		return nil
	}
	for c := n; c != nil && len(c.Children) == 1; c = c.Children[0] {
		switch child := c.Children[0]; {
		case child == nil:
			return nil
		case child.Type == NodeAggregate:
			return child
		case AggScopePreservingWrapper(child.Type):
			// HAVING, a window, a sort, a LIMIT: still one row per group.
		default:
			return nil
		}
	}
	return nil
}
