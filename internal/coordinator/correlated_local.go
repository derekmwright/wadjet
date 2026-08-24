package coordinator

import (
	"context"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// runCorrelatedLocal executes a query the stage DAG refused
// (physical.ErrCorrelatedSubqueryDistributed) on the coordinator-local
// single-process pipeline — the engine that owns correlated-subquery
// semantics, where a correlated reference compiles against a real
// SubqueryRunner and re-executes per outer row.
//
// A ROUTE, not a fallback; see runRefusedLocal for the guards and the
// reasoning they encode. Before the refusal, a correlated scalar answered 0
// silently on the DAG (#359).
func (c *Coordinator) runCorrelatedLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"correlated subquery", &c.localCorrelated)
}

// runDistinctLocal executes a query the stage DAG refused
// (physical.ErrDistinctDistributed) on the coordinator-local single-process
// pipeline, which applies a Distinct wherever it sits.
//
// The refusal covers the shapes logical.rewriteDistinctAsGroupBy cannot turn
// into a GROUP BY (an aggregate projection has no group key; a subquery
// projection has no evaluable one) and that sit off the root path, where the
// coordinator's post-gather dedup cannot see them either. Refusing them beat
// dropping them (#466), but the query still has an answer and one engine in
// this process can compute it — so route it there instead of handing the
// client an error, exactly as #359 does for correlated subqueries.
func (c *Coordinator) runDistinctLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"DISTINCT with no distributed stage", &c.localDistinct)
}

// CorrelatedLocalRoutes reports how many refused correlated-subquery plans
// were routed to the coordinator-local pipeline. Exposed for tests and
// observability — a distributed suite asserting DAG engagement uses it to
// prove the refusal fires ONLY for the correlated shapes.
func (c *Coordinator) CorrelatedLocalRoutes() int64 {
	return c.localCorrelated.Load()
}

// DistinctLocalRoutes reports how many plans refused for an unstageable
// DISTINCT were routed to the coordinator-local pipeline. Separate from
// CorrelatedLocalRoutes so a suite can assert which refusal fired.
func (c *Coordinator) DistinctLocalRoutes() int64 {
	return c.localDistinct.Load()
}
