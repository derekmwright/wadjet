package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// aggScopePreservingWrapper is read by THREE walks, and this is what keeps
// that a checked fact rather than a sentence in an ADR.
//
// Each of them answers a consumer's version of one question — "does this node
// leave the aggregate's own output columns visible under their own names?" —
// and each had grown its own hardcoded list:
//
//   - `aggregateUnderOutput`, for the gather's OutputRenames;
//   - `findAggregateAncestor`, for the single-process projection;
//   - `groupKeysPublishedBelow`, for whether an aggregate DIRECTLY BELOW
//     already publishes a key, so the one above must not re-materialize it.
//
// ADR-0026 §4's first statement said "both walks read one list, so they cannot
// disagree" while the THIRD still had its own — and the shape that finds it is
// `SELECT DISTINCT g + 1 AS k, ROW_NUMBER() OVER (…) FROM t GROUP BY g + 1`,
// which lowers to two aggregates keyed alike with a WINDOW between them: the
// outer one did not see the inner's key, materialized it again over a schema
// with no `g`, and collapsed the table into one NULL group.
//
// So the invariant this test carries is not "the list is right" — it is "there
// are exactly three readers and every one of them reads it", plus a
// completeness check that fails when the logical package gains a node type.
func TestAggScopePreservingWrapperIsReadByEveryWalk(t *testing.T) {
	// The list, stated once here so a change to it is a change to a test.
	want := map[logical.NodeType]bool{
		logical.NodeFilter: true, // HAVING: drops rows, renames nothing
		logical.NodeSort:   true, // reorders rows
		logical.NodeLimit:  true, // drops rows
		logical.NodeWindow: true, // APPENDS its outputs, renames none

		logical.NodeScan:      false, // a leaf: the aggregate is not below it
		logical.NodeProject:   false, // each caller decides its own Project rule
		logical.NodeAggregate: false, // the TARGET, not a wrapper
		logical.NodeJoin:      false, // two arms: the output is a union of both
		logical.NodeUnion:     false, // as Join, and re-rooted onto the first arm
		logical.NodeIntersect: false,
		logical.NodeExcept:    false,
		logical.NodeDual:      false,
		// A DISTINCT above a grouped query is lowered to a second Aggregate
		// by rewriteDistinctAsGroupBy, so it does not stand between one and a
		// SELECT list. It is false rather than absent so that a lowering which
		// stops doing that fails here instead of silently widening the walks.
		logical.NodeDistinct: false,
	}
	names := nodeTypeConstNames(t)
	if len(want) != len(names) {
		t.Fatalf("the logical package declares %d node types (%v) and this table has %d — "+
			"a new node type has to be decided here, because all three walks read this list",
			len(names), names, len(want))
	}
	for typ, w := range want {
		if got := aggScopePreservingWrapper(typ); got != w {
			t.Errorf("aggScopePreservingWrapper(%v) = %v, want %v", typ, got, w)
		}
	}

	// And the readers. Each is driven over a one-child wrapper of the given
	// type above an Aggregate, and must find that aggregate exactly when the
	// predicate says the wrapper preserves its scope.
	//
	// Only the single-child kinds are probed: a one-child Join or Union is not
	// that node at all, and a probe built from one reports a descent no real
	// plan performs — the mistake `TestScopePreservingWrapperMatchesTheRename-
	// Walk`'s first cut made.
	agg := func() *logical.Node {
		return &logical.Node{Type: logical.NodeAggregate,
			GroupBy: []string{"g + 1"},
			Children: []*logical.Node{{Type: logical.NodeScan,
				TableName: "t", ScanColumns: []string{"g"}}}}
	}
	for typ, w := range want {
		switch typ {
		case logical.NodeJoin, logical.NodeUnion, logical.NodeIntersect,
			logical.NodeExcept, logical.NodeScan, logical.NodeDual,
			logical.NodeProject, logical.NodeAggregate:
			continue // not single-child wrappers, or the caller's own rule
		}
		wrapped := &logical.Node{Type: typ, Children: []*logical.Node{agg()}}

		if found := aggregateUnderOutput(wrapped); (found != nil) != w {
			t.Errorf("aggregateUnderOutput through %v found=%v, want %v — the gather's walk "+
				"stopped reading aggScopePreservingWrapper", typ, found != nil, w)
		}
		if found := findAggregateAncestor(wrapped); (found != nil) != w {
			t.Errorf("findAggregateAncestor through %v found=%v, want %v — the single-process "+
				"walk stopped reading aggScopePreservingWrapper", typ, found != nil, w)
		}
		// groupKeysPublishedBelow returns a (possibly empty) map when it
		// reaches an aggregate and nil when it declines, which is the same
		// yes/no one level down.
		if reached := groupKeysPublishedBelow(wrapped) != nil; reached != w {
			t.Errorf("groupKeysPublishedBelow through %v reached=%v, want %v — the THIRD walk "+
				"stopped reading aggScopePreservingWrapper", typ, reached, w)
		}
	}
}
