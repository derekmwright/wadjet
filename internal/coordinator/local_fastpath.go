package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// DefaultLocalFastPathBytes is the default routing threshold: a query whose
// total post-pruning catalog scan bytes stay under this executes in-process
// on the coordinator instead of as a distributed stage DAG. The DAG's fixed
// costs (task dispatch + object-store materialization per stage boundary)
// are independent of data size, so small queries pay a latency floor the
// local pipeline doesn't have.
const DefaultLocalFastPathBytes = 64 << 20

// defaultLocalFastPathConcurrency bounds simultaneous local executions so a
// burst of interactive queries cannot monopolize coordinator CPU/memory.
// Overflow routes to the DAG — degraded latency, never a queue.
const defaultLocalFastPathConcurrency = 4

// localFastPathBytes resolves Config.LocalFastPathBytes: <=0 = disabled.
func (c *Coordinator) localFastPathBytes() int64 {
	if v := c.config.LocalFastPathBytes; v > 0 {
		return v
	}
	return 0
}

// LocalFastPathHits reports how many queries executed on the local fast
// path. Exposed for tests and observability.
func (c *Coordinator) LocalFastPathHits() int64 {
	return c.localHits.Load()
}

// LocalFastPathBails reports how many local executions bailed out over the
// result budget and re-dispatched as DAG queries.
func (c *Coordinator) LocalFastPathBails() int64 {
	return c.localBails.Load()
}

// localResultBudget bounds the materialized result of a local fast-path
// execution. Scan input is bounded by the routing threshold, but join
// output is not — a misrouted blow-up (e.g. a many-to-many self join) would
// otherwise grow coordinator heap without limit. Past the budget the local
// run aborts and the query re-dispatches as a DAG, whose gather spills
// oversized results to scratch. 8× the routing threshold: proportional to
// what routing declared "small", far above any well-routed result.
func (c *Coordinator) localResultBudget(threshold int64) int64 {
	if c.localResultBudgetOverride > 0 {
		return c.localResultBudgetOverride
	}
	return 8 * threshold
}

// tryLocalFastPath routes a small query onto the coordinator-local
// single-process pipeline. Returns (result, true) when the query was
// executed locally; (nil, false) when the caller should proceed with the
// distributed DAG — because the fast path is disabled, the concurrency cap
// is saturated, the plan's input size is unestimable or over threshold, or
// local planning/execution failed (any local error falls back to the DAG,
// which is the reliability path).
//
// The logical plan passed here is the final optimized plan — identical to
// what PlanDistributed would consume — so both paths see the same RLS row
// filters and rewrites, and answer identically by construction.
func (c *Coordinator) tryLocalFastPath(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time) (*SQLResult, bool) {
	threshold := c.localFastPathBytes()
	if threshold <= 0 {
		return nil, false
	}
	select {
	case c.localSem <- struct{}{}:
		defer func() { <-c.localSem }()
	default:
		c.logger.Debug("local fast path saturated, routing to DAG", "query", queryID)
		return nil, false
	}

	planner := physical.NewPlanner(c.catalog)
	estBytes, ok := planner.EstimatePlanScanBytes(ctx, logicalPlan)
	if !ok || estBytes > threshold {
		return nil, false
	}

	// Pipeline breakers spill past the budget; the budget only bounds
	// resident operator state, so a misestimate degrades to disk instead
	// of coordinator OOM.
	planner.MemoryBudget = 8 * threshold
	planner.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		c.logger.Warn("local fast path plan failed, routing to DAG",
			"query", queryID, "error", err)
		return nil, false
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}
	pipeline := physPlan.Pipeline
	sink, ok := pipeline.Sink.(*exec.CollectSink)
	if !ok {
		pipeline.Close()
		c.logger.Warn("local fast path: unexpected sink type, routing to DAG",
			"query", queryID)
		return nil, false
	}
	// Keep the result columnar: Finalize would otherwise box every row into
	// map[string]any and release the batches. The collected batches are
	// self-contained (Consume Detach()es them and BytesColumn owns its
	// arena), so they safely outlive pipeline.Close and stream to the wire
	// one batch at a time like the gather path's result.
	sink.SkipFinalizeToRows = true
	// Adaptive bail-out: the scan input is bounded by the estimate, the
	// RESULT is not (join blow-up). Past the budget the run aborts here
	// and the caller re-dispatches as a DAG query — reads are idempotent,
	// and the DAG gather spills oversized results to scratch.
	sink.MaxBytes = c.localResultBudget(threshold)
	if err := pipeline.Run(ctx); err != nil {
		pipeline.Close()
		if errors.Is(err, exec.ErrCollectBudget) {
			c.localBails.Add(1)
			c.logger.Info("local fast path result over budget, re-dispatching as DAG",
				"query", queryID, "budget_bytes", sink.MaxBytes)
		} else {
			c.logger.Warn("local fast path execution failed, routing to DAG",
				"query", queryID, "error", err)
		}
		return nil, false
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

	c.localHits.Add(1)
	c.logger.Info("query executed on local fast path",
		"query", queryID, "est_scan_bytes", estBytes,
		"rows", totalRows, "elapsed", time.Since(start))
	return &SQLResult{
		QueryID:   queryID,
		Columns:   columns,
		Batches:   batches,
		TotalRows: totalRows,
		Elapsed:   time.Since(start),
		Plan:      planStr,
	}, true
}
