package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// FastPathStrict makes a local execution FAILURE final instead of retrying
// the query on the distributed DAG (#308). Retry is kept only for failures
// that say nothing about the query's meaning — the deliberate result-budget
// bail-out, a local memory budget a worker fleet would not hit, and
// object-store unavailability. Every other execution error is deterministic:
// re-running it on the DAG either reproduces it (wasted work) or, when the
// two paths disagree, silently returns rows the local path considered
// impossible to produce. That second case is precisely the class of defect
// #312 was — a fallback would have shipped its wrong answer instead of
// surfacing anything. Kill switch: WADJET_FASTPATH_STRICT=0 restores the
// unconditional fallback.
//
// Routing and PLANNING failures are unaffected: no query semantics have been
// evaluated at that point, and the DAG legitimately covers plan shapes the
// local pipeline declines.
var FastPathStrict = optswitch.Register("fastpath-strict", "WADJET_FASTPATH_STRICT",
	"report a local fast-path execution failure instead of retrying it on the DAG")

// localOutcome is what a local execution failure means for the query.
type localOutcome int

const (
	// retryOnDAG: the failure says nothing about the query's meaning.
	retryOnDAG localOutcome = iota
	// reportToClient: the failure IS the query's outcome.
	reportToClient
)

// classifyLocalFailure decides whether a failed local execution may be
// retried on the DAG. ctxErr is the statement context's error, which
// outranks everything: a caller who cancelled is not asking for a second
// attempt. Pure so every branch is testable without a cluster.
func classifyLocalFailure(err, ctxErr error, strict bool) localOutcome {
	switch {
	case ctxErr != nil:
		// Cancelled or timed out. Re-dispatching runs the whole query
		// again under a context that is already dead, and reports the
		// cancellation as a DAG failure — which is how a client's stop
		// button surfaced as "native DAG: context canceled".
		return reportToClient
	case errors.Is(err, exec.ErrCollectBudget):
		// By design: the result outgrew the local budget and the DAG's
		// gather spills it to scratch. Reads are idempotent.
		return retryOnDAG
	case errors.Is(err, memory.ErrMemoryExceeded):
		// The coordinator budgets one process; the fleet has more.
		return retryOnDAG
	case errors.Is(err, objstore.ErrCircuitOpen):
		// S3 unavailable to THIS process — a worker may still reach it.
		return retryOnDAG
	case strict:
		return reportToClient
	}
	return retryOnDAG
}

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

// LocalFastPathStrictFailures reports how many local executions were
// reported to the client instead of retried on the DAG.
func (c *Coordinator) LocalFastPathStrictFailures() int64 {
	return c.localStrictFails.Load()
}

// tryLocalFastPath routes a small query onto the coordinator-local
// single-process pipeline. Returns handled=true when the fast path produced
// the query's definitive outcome — a result, or (under FastPathStrict) an
// execution error the DAG must not be given a second opinion on. It returns
// handled=false when the caller should proceed with the distributed DAG:
// the fast path is disabled, the concurrency cap is saturated, the plan's
// input size is unestimable or over threshold, local PLANNING failed, or
// execution failed in a way unrelated to the query's meaning.
//
// The logical plan passed here is the final optimized plan — identical to
// what PlanDistributed would consume — so both paths see the same RLS row
// filters and rewrites, and answer identically by construction.
func (c *Coordinator) tryLocalFastPath(ctx context.Context, queryID string, logicalPlan *logical.Node, planStr string, start time.Time) (*SQLResult, bool, error) {
	threshold := c.localFastPathBytes()
	if threshold <= 0 {
		return nil, false, nil
	}
	select {
	case c.localSem <- struct{}{}:
		defer func() { <-c.localSem }()
	default:
		c.logger.Debug("local fast path saturated, routing to DAG", "query", queryID)
		return nil, false, nil
	}

	// physical.NewPlannerForContext, not NewPlanner: ExecuteSQL/SubmitSQL
	// attach a per-statement ManifestSnapshot to ctx (#502) so this
	// planner shares the same pinned manifests as the scanAnnotator passes
	// and (if this bails out) the main distributed planner, instead of
	// re-reading the catalog for every table this fast-path attempt
	// touches.
	planner := physical.NewPlannerForContext(ctx, c.catalog)
	estBytes, ok := planner.EstimatePlanScanBytes(ctx, logicalPlan)
	if !ok || estBytes > threshold {
		return nil, false, nil
	}

	// Pipeline breakers spill past the budget; the budget only bounds
	// resident operator state, so a misestimate degrades to disk instead
	// of coordinator OOM.
	planner.MemoryBudget = 8 * threshold
	planner.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	planner.LateMaterialization = c.config.LateMaterialization
	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		c.logger.Warn("local fast path plan failed, routing to DAG",
			"query", queryID, "error", err)
		return nil, false, nil
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
		return nil, false, nil
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
		if classifyLocalFailure(err, ctx.Err(), FastPathStrict.On()) == reportToClient {
			// The DAG would either reproduce this or disagree with it, and
			// a disagreement is a defect to surface, not an answer to
			// serve. Logged at ERROR: this is the seam where the two paths
			// would have parted company.
			c.localStrictFails.Add(1)
			c.logger.Error("local fast path execution failed, reporting to client",
				"query", queryID, "error", err,
				"note", "set WADJET_FASTPATH_STRICT=0 to retry such failures on the DAG")
			return nil, true, err
		}
		if errors.Is(err, exec.ErrCollectBudget) {
			c.localBails.Add(1)
			c.logger.Info("local fast path result over budget, re-dispatching as DAG",
				"query", queryID, "budget_bytes", sink.MaxBytes)
		} else {
			c.logger.Warn("local fast path execution failed, routing to DAG",
				"query", queryID, "error", err)
		}
		return nil, false, nil
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
		// The sink's schema, not the batches': a zero-row result has no
		// batch to read it off, and the wire still has to declare types.
		Schema: sink.Schema(),
		// A plan property, applies whether or not this result has rows
		// (FIX 2, #457/#458 fold-in).
		WireUnconstrainedDecimal: sink.SchemaHintWireUnconstrainedDecimal,
	}, true, nil
}
