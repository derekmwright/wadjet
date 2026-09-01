package logical

// AggScopePreservingWrapper reports whether a node standing between an
// aggregate and a consumer above it leaves the aggregate's OWN output columns
// visible, under their own names.
//
// This is THE list. ADR-0026 §4 states it once and names the walks that read
// it, because every one of them is asking a consumer's version of the same
// question and every one of them had grown its own answer:
//
//   - physical.aggregateUnderOutput — the gather's OutputRenames;
//   - physical.findAggregateAncestor — the single-process projection;
//   - physical.groupKeysPublishedBelow — whether an aggregate DIRECTLY BELOW
//     already publishes a key, so the one above must not re-materialize it;
//   - AggregateOverGroupRows (this package) — whether a Project's INPUT rows
//     are one per GROUP, which decides whether a predicate above it may be
//     substituted below (#774).
//
// It lives here rather than in `physical` because the fourth reader is in this
// package and `physical` imports `logical`, not the other way round. The
// physical package's `aggScopePreservingWrapper` is a thin delegation, and
// `TestAggScopePreservingWrapperIsReadByEveryWalk` drives all four.
//
// A WINDOW is on the list: exec.Window APPENDS its output to its input and
// renames nothing, so every column the aggregate published is still there
// under its own name. A Filter (HAVING), a Sort and a LIMIT are on it for the
// same reason — they drop or reorder ROWS and rename no column.
//
// An Aggregate is not: it replaces its child's schema with its own keys and
// outputs, which is why it is the walks' TARGET rather than a wrapper. Neither
// is a Project: what a Project does to the schema is the caller's own
// question, so each walk keeps its own rule for it. NodeDistinct is
// deliberately absent — rewriteDistinctAsGroupBy lowers a DISTINCT above a
// grouped query into a second Aggregate, so the node does not stand there, and
// admitting a kind no fixture produces would put an untested path on the
// default route (correctness protocol, method 10).
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
