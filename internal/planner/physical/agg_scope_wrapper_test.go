package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// aggScopePreservingWrapper is read by FIVE walks, and this is what keeps
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
//   - `logical.AggregateOverGroupRows`, for whether a Project's INPUT rows are
//     one per GROUP — which is what decides whether a predicate above the
//     Project may be substituted below it.
//   - `aggregateOutputNames`, for the names the aggregate below publishes —
//     which is what pins each projection to the physical SLOT its provenance
//     names when two outputs share one name (#575).
//
// ADR-0026 §4's first statement said "both walks read one list, so they cannot
// disagree" while the THIRD still had its own — and the shape that finds it is
// `SELECT DISTINCT g + 1 AS k, ROW_NUMBER() OVER (…) FROM t GROUP BY g + 1`,
// which lowers to two aggregates keyed alike with a WINDOW between them: the
// outer one did not see the inner's key, materialized it again over a schema
// with no `g`, and collapsed the table into one NULL group.
//
// The FOURTH was found the same way, by a review counting them, and it was in
// the OTHER package: `readsAnAggregate` asked its question through
// `AggregateBelowProject`, whose list is Filter-only. With a WINDOW between the
// aggregate and the Project, `WHERE k > 3` over `SELECT g + 1 AS k, ROW_NUMBER()
// OVER (…) … GROUP BY g + 1` substituted `k` away to `(g + 1)` and pushed it
// below the Project, where it met a schema with no `g`: UNKNOWN on every row,
// zero rows on all four arms where PostgreSQL answers four (#774). The list
// therefore moved to `logical` — `physical` imports `logical`, so a shared list
// can only live there — and `aggScopePreservingWrapper` is now a delegation.
// `AggregateBelowProject` keeps its narrower list ON PURPOSE and says so: its
// two callers map a SELECT list onto the aggregate's own STAGE, and a Sort or a
// window between them emits a stage of its own.
//
// The FIFTH was found by the next review, one round later, and it was inside
// this package all along: `aggregateOutputNames` descended NodeFilter ALONE
// while the call site that guards it (`isOverAggregate`, i.e.
// `findAggregateAncestor`) reads the full list. With a WINDOW between, the two
// disagreed — the guard said an aggregate is below, the walk said it could not
// model that — so #575's duplicate-name slot pinning was silently skipped and
// `SELECT COUNT(*) AS g, g AS x, ROW_NUMBER() OVER (ORDER BY g) ... GROUP BY g`
// published the KEY's value under the aggregate's alias on the SINGLE-process
// path.
//
// So the invariant this test carries is: **these five NAMED readers agree
// with the list, and the list covers every node type the logical package
// declares.**
//
// It does NOT — and cannot — discover a SIXTH reader. It drives the five by
// name, so a new walk that grows its own hardcoded Filter/Sort/Limit list
// passes here in silence; a review proved that by adding one and watching this
// test go green. The commit that introduced it said "a fourth reader with its
// own list fails here", and that was an overclaim twice over — the fourth
// existed already, the fifth did too, and a review counting them is what found
// each one. The CENSUS of candidates — every function in this package that is
// even arguably asking this question, with a recorded decision for each,
// including the two that were MEASURED and deliberately left alone
// (`resolveSortKeyColumn`, `aggregateUnderWindow`) — is in ADR-0026 §4. It is a
// record and not a test on purpose: the honest count is 29 functions in this
// package carrying a Filter/Sort/Limit case, so a source-level allowlist would
// have thirty entries and would drift.
//
// A source-level guard was considered and is NOT here on purpose. Twelve
// functions in this package carry a `case logical.NodeFilter, logical.NodeSort,
// logical.NodeLimit` clause and exactly ONE of them is asking this question;
// the rest ask what a node EMITS, what its input TYPES are, what a set-op arm
// declares, or whether a stage forwards its columns, and requiring them to read
// `aggScopePreservingWrapper` would be wrong. A grep guard therefore needs an
// eleven-entry allowlist that drifts with every new walk — the
// enumerate-the-kinds shape ADR-0025 records as having been wrong twice. What
// finds a fourth reader is a review counting them, which is how this one was
// found.
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
			"a new node type has to be decided here, because all four walks read this list",
			len(names), names, len(want))
	}
	for typ, w := range want {
		if got := aggScopePreservingWrapper(typ); got != w {
			t.Errorf("aggScopePreservingWrapper(%v) = %v, want %v", typ, got, w)
		}
		// The physical predicate is a DELEGATION to the logical one, which is
		// where the list lives so both packages can read it. Asserted rather
		// than assumed: a copy is exactly what #774 was.
		if got := logical.AggScopePreservingWrapper(typ); got != w {
			t.Errorf("logical.AggScopePreservingWrapper(%v) = %v, want %v", typ, got, w)
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
		// The fourth reader starts at a PROJECT and asks about its input, so
		// the probe puts one on top of the wrapper.
		overProject := &logical.Node{Type: logical.NodeProject,
			Children: []*logical.Node{wrapped}}
		if found := logical.AggregateOverGroupRows(overProject); (found != nil) != w {
			t.Errorf("logical.AggregateOverGroupRows through %v found=%v, want %v — the FOURTH "+
				"walk stopped reading AggScopePreservingWrapper (#774)", typ, found != nil, w)
		}
		// The fifth answers the aggregate's OUTPUT NAMES through the wrapper.
		if _, ok := aggregateOutputNames(wrapped); ok != w {
			t.Errorf("aggregateOutputNames through %v ok=%v, want %v — the FIFTH walk stopped "+
				"reading aggScopePreservingWrapper (#575 under a window)", typ, ok, w)
		}
	}
}
