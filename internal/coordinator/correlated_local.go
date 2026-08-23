package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// runCorrelatedLocal executes a query the stage DAG refused
// (physical.ErrCorrelatedSubqueryDistributed) on the coordinator-local
// single-process pipeline — the engine that owns correlated-subquery
// semantics, where a correlated reference compiles against a real
// SubqueryRunner and re-executes per outer row.
//
// This is a ROUTE, not a fallback: the DAG produced no plan, so there is no
// second engine to retry on and every failure here is the query's outcome,
// reported to the client. That is the whole point of the refusal — before it,
// a correlated scalar answered 0 silently on the DAG (#359), and a wrong
// answer is strictly worse than an error.
//
// Unlike tryLocalFastPath there is no byte-threshold gate: these plans are
// unestimable (residual subquery text) and the alternative is refusing
// outright. Capacity is still bounded — the pipeline runs under the same
// memory budget (pipeline breakers spill past it) and the collect sink's
// result budget, both derived from the fast-path threshold or its default
// when the fast path is disabled. Past those bounds the query FAILS with the
// budget error rather than degrading the coordinator; a deployment that needs
// larger correlated results raises --local-fastpath-bytes.
func (c *Coordinator) runCorrelatedLocal(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time, refusal error) (*SQLResult, error) {
	select {
	case c.localSem <- struct{}{}:
		defer func() { <-c.localSem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("correlated-subquery local execution queue: %w", ctx.Err())
	}
	c.localCorrelated.Add(1)
	c.logger.Info("correlated subquery: stage DAG refused the plan, executing on the coordinator-local single-process pipeline",
		"query", queryID, "refusal", refusal)

	base := c.localFastPathBytes()
	if base <= 0 {
		base = DefaultLocalFastPathBytes
	}
	planner := physical.NewPlanner(c.catalog)
	planner.MemoryBudget = 8 * base
	planner.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	planner.LateMaterialization = c.config.LateMaterialization
	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		return nil, fmt.Errorf("correlated subquery requires single-process execution (%v); local planning failed: %w", refusal, err)
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}
	pipeline := physPlan.Pipeline
	sink, ok := pipeline.Sink.(*exec.CollectSink)
	if !ok {
		pipeline.Close()
		return nil, fmt.Errorf("correlated subquery requires single-process execution (%v); local pipeline produced no collectable result", refusal)
	}
	sink.SkipFinalizeToRows = true
	sink.MaxBytes = c.localResultBudget(base)
	if err := pipeline.Run(ctx); err != nil {
		pipeline.Close()
		if errors.Is(err, exec.ErrCollectBudget) {
			return nil, fmt.Errorf("correlated subquery executes single-process on the coordinator and its result exceeded the local budget (%d bytes); "+
				"narrow the query or raise --local-fastpath-bytes: %w", sink.MaxBytes, err)
		}
		return nil, fmt.Errorf("correlated-subquery local execution: %w", err)
	}
	defer pipeline.Close()
	batches := sink.Batches()
	var totalRows int64
	for _, b := range batches {
		totalRows += int64(b.ActiveLen())
	}
	columns := make([]string, 0, len(sink.Schema()))
	for _, col := range sink.Schema() {
		columns = append(columns, col.Name)
	}
	c.logger.Info("correlated subquery answered on the local pipeline",
		"query", queryID, "rows", totalRows, "elapsed", time.Since(start))
	return &SQLResult{
		QueryID:   queryID,
		Columns:   columns,
		Batches:   batches,
		TotalRows: totalRows,
		Elapsed:   time.Since(start),
		Plan:      planStr,
		// The sink's schema, not the batches': a zero-row result has no
		// batch to read the types off, and pgwire still has to declare
		// them. Without this the coord path fell back to OID 25 (text) for
		// every column of an empty correlated-subquery result while the
		// same query through the embedded API declared real OIDs.
		Schema: sink.Schema(),
	}, nil
}

// CorrelatedLocalRoutes reports how many refused correlated-subquery plans
// were routed to the coordinator-local pipeline. Exposed for tests and
// observability — a distributed suite asserting DAG engagement uses it to
// prove the refusal fires ONLY for the correlated shapes.
func (c *Coordinator) CorrelatedLocalRoutes() int64 {
	return c.localCorrelated.Load()
}
