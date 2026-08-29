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

// runInSubqueryLocal executes a query the stage DAG refused
// (physical.ErrInSubqueryDistributed) on the coordinator-local single-process
// pipeline, where expr.InSubquery resolves the set once under resolveMu and
// caches it.
//
// The refusal covers what the planner's materialization cannot inline: a set
// past the row bound, or a value with no literal spelling that survives the
// round trip through the filter's text. Everything it CAN inline never
// reaches here — the predicate becomes a literal list and the DAG runs it
// like any other filter. Refusing beat shipping the subquery to a worker that
// has no SubqueryRunner (#524), and routing beats handing the client an error,
// exactly as #359 does for correlated subqueries.
func (c *Coordinator) runInSubqueryLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"IN subquery with no distributed stage", &c.localInSubquery)
}

// runScalarProjectionLocal executes a query the stage DAG refused
// (physical.ErrScalarSubqueryProjectionDistributed) on the coordinator-local
// single-process pipeline, where a SELECT-list subquery compiles against a
// real SubqueryRunner.
//
// The DAG's scalar-producer machinery covers predicates only; a subquery in
// the SELECT list reached the worker as expression text and failed every task
// (#659). Refusing at plan time and routing here turns a hard failure into
// the answer, exactly as #359 does for correlated subqueries.
func (c *Coordinator) runScalarProjectionLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"SELECT-list subquery with no distributed stage", &c.localScalarProjection)
}

// ScalarProjectionLocalRoutes reports how many plans refused for a SELECT-list
// subquery were routed to the coordinator-local pipeline (#659).
func (c *Coordinator) ScalarProjectionLocalRoutes() int64 {
	return c.localScalarProjection.Load()
}

// InSubqueryLocalRoutes reports how many plans refused for an unmaterializable
// IN-subquery were routed to the coordinator-local pipeline. Separate from the
// other two counters so a suite can assert WHICH refusal fired.
func (c *Coordinator) InSubqueryLocalRoutes() int64 {
	return c.localInSubquery.Load()
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
