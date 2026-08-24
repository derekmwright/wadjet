package physical

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrDistinctDistributed marks a plan the stage DAG refuses because it
// carries a DISTINCT the DAG has no stage for.
//
// walkStages treats NodeDistinct as a passthrough and emits nothing (#163).
// Two things compensate, and each covers only part of the space:
//
//   - logical.rewriteDistinctAsGroupBy turns Distinct(Project) into a
//     GroupBy aggregate, wherever it sits, so it gets a real stage. It
//     declines a projection it cannot turn into a group key — an aggregate
//     projection (SELECT DISTINCT a, SUM(b) …) or one containing a subquery.
//   - The coordinator deduplicates the gather result when
//     logical.ExtractMergeInfo reports HasDistinct, which it can only see
//     for a Distinct on the ROOT path (below Limit/Sort/Project chains).
//
// A Distinct that both decline is executed by nobody, and the DAG answers
// with every pre-dedup row: `SELECT COUNT(*) FROM (SELECT DISTINCT c FROM t) u`
// returned the raw count distributed and the right one single-process (#466).
// Refusing is the #308 position — a deterministic loud failure beats a
// silently different answer — and it is a refusal the rewrite is expected to
// make unreachable for every shape it handles.
//
// The refusal is a HANDOFF, not the query's outcome: Coordinator.ExecuteSQL
// matches this error and answers on the coordinator-local single-process
// pipeline, which applies a Distinct wherever it sits (runDistinctLocal, the
// same move #359 makes for correlated subqueries). A caller with no local
// engine reports it. What the refusal buys either way is that nothing
// carrying DISTINCT semantics reaches walkStages.
var ErrDistinctDistributed = errors.New(
	"DISTINCT in this position has no distributed stage")

// refuseUnstageableDistinct returns a typed refusal for the first DISTINCT
// that would reach stage generation with nothing to execute it.
//
// Scope mirrors the two compensations exactly. The root path — the chain of
// Limit / Sort / Project nodes descending from the plan root — is where the
// coordinator's post-gather dedup can see a Distinct, so one there is fine.
// (A star DISTINCT reaches neither branch: logical.rewriteStarDistinct
// lowers it wherever it sits, reading its group keys off the scan's catalog
// annotation, so this refusal never sees one whose columns are knowable.)
// Anywhere else, an unmarked Distinct that survived the rewrite is a dropped
// DISTINCT. A BuildSideDedup Distinct is planner-inserted and carries no
// user-visible semantics, so it never refuses.
func refuseUnstageableDistinct(root *logical.Node) error {
	// Walk the root path first, and hand off to the off-path check below it.
	n := root
	for n != nil {
		switch n.Type {
		case logical.NodeLimit, logical.NodeSort, logical.NodeProject:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		case logical.NodeDistinct:
			// Visible to ExtractMergeInfo — the coordinator dedups it.
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return offPathDistinct(n)
		}
	}
	return nil
}

// offPathDistinct reports the first unmarked Distinct anywhere in a subtree
// the coordinator's post-gather dedup cannot see.
func offPathDistinct(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeDistinct && !n.BuildSideDedup {
		return fmt.Errorf("%w: a DISTINCT feeding an aggregate or a join is"+
			" executed by being rewritten into a GROUP BY, and this one%s"+
			" could not be — neither the stage DAG nor the coordinator's"+
			" post-gather dedup would apply it",
			ErrDistinctDistributed, whyRewriteDeclined(n))
	}
	for _, child := range n.Children {
		if err := offPathDistinct(child); err != nil {
			return err
		}
	}
	return nil
}

// whyRewriteDeclined names the projection rewriteDistinctAsGroupBy could not
// turn into a group key, so the refusal points at the term to change.
func whyRewriteDeclined(d *logical.Node) string {
	if len(d.Children) != 1 || d.Children[0].Type != logical.NodeProject {
		return ""
	}
	for _, p := range d.Children[0].Projections {
		name := p.Alias
		if name == "" {
			name = p.Column
		}
		if p.IsAgg {
			return fmt.Sprintf(" (%q is an aggregate)", name)
		}
		if p.Expr == "" {
			return fmt.Sprintf(" (%q has no expression to group by)", name)
		}
	}
	return ""
}
