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

// runGroupingSetsLocal executes a query the stage DAG refused
// (physical.ErrGroupingSetsDistributed) on the coordinator-local
// single-process pipeline, whose HashAggregate is the only operator in the
// process that knows what a grouping set is.
//
// The DAG carries no representation for GROUPING SETS / ROLLUP / CUBE at all —
// no Stage field, no wire tag, no worker read — so it ran the UNION of the
// sets' terms as a plain GROUP BY and answered the cross product where
// PostgreSQL answers the sets, and no grand total where PostgreSQL has one.
// Silently, and for plain column keys as much as computed ones. Refusing beat
// that (#778); routing beats handing the client an error, exactly as #359 does
// for correlated subqueries and #466 for DISTINCT.
func (c *Coordinator) runGroupingSetsLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"GROUPING SETS with no distributed stage", &c.localGroupingSets)
}

// runGroupKeyLocal executes a query the stage DAG refused
// (physical.ErrGroupKeyDistributed) on the coordinator-local single-process
// pipeline, which is the engine that keeps a derived GROUP BY key's RESOLUTION
// name and its PUBLISHED name apart — a hidden `__gb_expr_N` slot and
// `exec.HashAggregate.GroupByOutNames` (ADR-0026 §2).
//
// `Stage.GroupByCols` is one field for both, and the worker re-derives "is this
// key derived?" by parsing the text, so two shapes answered wrongly and in
// silence: a key an aggregate DIRECTLY BELOW already publishes (the DISTINCT
// lowering, one NULL group), and a derived key sharing its published name with
// one of the aggregate's own outputs (the KEY's value under the aggregate's
// alias). Refusing beat both (#736); routing beats handing the client an error,
// exactly as #359 does for correlated subqueries.
func (c *Coordinator) runGroupKeyLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"GROUP BY key with no distributed published name", &c.localGroupKey)
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

// runUnreachableOutputLocal executes a query the stage DAG refused
// (physical.ErrUnreachableGatherOutput) on the coordinator-local
// single-process pipeline, where every Project is a real operator.
//
// The refusal covers the shapes no stage can compute the SELECT list for and
// the gather cannot either — today, a window wrapped in an expression ONE
// LEVEL DOWN, whose defining AST extractOutputRenames never sees. The DAG
// answered those with the producer's raw columns under their source names,
// which is a wrong RESULT SET rather than a wrong value; routing here is what
// turns it into the right one (#656 F2).
func (c *Coordinator) runUnreachableOutputLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"SELECT list no stage computes", &c.localUnreachableOutput)
}

// runTableLessLocal executes a query the stage DAG refused
// (physical.ErrTableLessSelectDistributed) on the coordinator-local
// single-process pipeline, whose DualSource produces the one row a SELECT
// with no FROM is.
//
// The DAG emits a `dual` stage with no dependencies and no ScanFiles, and the
// dispatcher's task-input builder requires one or the other — so every
// table-less SELECT past pgwire's synthetic-answer list FAILED on the DAG with
// "stage dual-0 has no dependencies and no ScanFiles" (#806). Routing beats
// handing the client an error, exactly as #359 does for correlated subqueries;
// and unlike those, this one gives up no parallelism, because the answer is
// one row.
func (c *Coordinator) runTableLessLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	return c.runRefusedLocal(ctx, queryID, logicalPlan, planStr, start, refusal,
		"table-less SELECT with no distributed stage", &c.localTableLess)
}

// TableLessLocalRoutes reports how many plans refused for a table-less SELECT
// were routed to the coordinator-local pipeline (#806). Separate from the
// other counters so a suite can assert WHICH refusal fired.
func (c *Coordinator) TableLessLocalRoutes() int64 {
	return c.localTableLess.Load()
}

// UnreachableOutputLocalRoutes reports how many plans refused for an
// uncomputed SELECT list were routed to the coordinator-local pipeline.
func (c *Coordinator) UnreachableOutputLocalRoutes() int64 {
	return c.localUnreachableOutput.Load()
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

// GroupingSetsLocalRoutes reports how many plans refused for GROUPING SETS /
// ROLLUP / CUBE were routed to the coordinator-local pipeline. Separate from
// the others so a suite can assert WHICH refusal fired — a gate that only
// checked the rows would pass just as happily if the DAG had started answering
// them by accident (#778).
func (c *Coordinator) GroupingSetsLocalRoutes() int64 {
	return c.localGroupingSets.Load()
}

// GroupKeyLocalRoutes reports how many plans refused for a GROUP BY key whose
// published name a stage cannot carry were routed to the coordinator-local
// pipeline (#736). Separate from the others so a suite can assert WHICH refusal
// fired — and, just as importantly, that an ordinary computed key did NOT fire
// it and stayed distributed.
func (c *Coordinator) GroupKeyLocalRoutes() int64 {
	return c.localGroupKey.Load()
}
