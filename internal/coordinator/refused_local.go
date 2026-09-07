package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// runRefusedLocal executes a query the stage DAG REFUSED — it produced no
// plan at all — on the coordinator-local single-process pipeline.
//
// This is a ROUTE, not a fallback: there is no second engine to retry on, so
// every failure here is the query's outcome and is reported to the client.
// That is the whole point of a typed refusal. Before #359 a per-row
// correlated scalar answered 0 on the DAG; before #466 a DISTINCT the DAG
// had no stage for was dropped and the raw pre-dedup rows came back. Both
// are wrong answers, and a wrong answer is strictly worse than an error —
// but an error is also worse than the right answer, which the local pipeline
// can produce for both classes.
//
// Unlike tryLocalFastPath there is no byte-threshold gate: a refused plan is
// often unestimable and the alternative is refusing outright. Capacity is
// still bounded — the pipeline runs under the same memory budget (pipeline
// breakers spill past it) and the collect sink's result budget, both derived
// from the fast-path threshold or its default when the fast path is
// disabled. Past those bounds the query FAILS with the budget error rather
// than degrading the coordinator; a deployment that needs larger results
// raises --local-fastpath-bytes.
//
// what names the construct in operator-facing messages; counter is the
// per-class route counter a distributed suite asserts engagement on.
func (c *Coordinator) runRefusedLocal(
	ctx context.Context,
	queryID string,
	logicalPlan *logical.Node,
	planStr string,
	start time.Time,
	refusal error,
	what string,
	counter *atomic.Int64,
) (*SQLResult, error) {
	select {
	case c.localSem <- struct{}{}:
		defer func() { <-c.localSem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("%s local execution queue: %w", what, ctx.Err())
	}
	counter.Add(1)
	c.logger.Info("stage DAG refused the plan, executing on the coordinator-local single-process pipeline",
		"query", queryID, "construct", what, "refusal", refusal)

	base := c.localFastPathBytes()
	if base <= 0 {
		base = DefaultLocalFastPathBytes
	}
	// physical.NewPlannerForContext: ctx carries the SAME per-statement
	// ManifestSnapshot ExecuteSQL/SubmitSQL attached (#502), so a table
	// PlanDistributed's failed attempt already read is not read again here.
	planner := physical.NewPlannerForContext(ctx, c.catalog)
	planner.MemoryBudget = 8 * base
	planner.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	planner.LateMaterialization = c.config.LateMaterialization
	planner.QueryLimits = c.resolveQueryLimits(ctx)
	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		if refusal, ok := authorizationRefusal(err); ok {
			return nil, refusal
		}
		return nil, fmt.Errorf("%s requires single-process execution (%v); local planning failed: %w", what, refusal, err)
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}
	pipeline := physPlan.Pipeline
	sink, ok := pipeline.Sink.(*exec.CollectSink)
	if !ok {
		pipeline.Close()
		return nil, fmt.Errorf("%s requires single-process execution (%v); local pipeline produced no collectable result", what, refusal)
	}
	sink.SkipFinalizeToRows = true
	sink.MaxBytes = c.localResultBudget(base)
	if err := pipeline.Run(ctx); err != nil {
		pipeline.Close()
		if errors.Is(err, exec.ErrCollectBudget) {
			return nil, fmt.Errorf("%s executes single-process on the coordinator and its result exceeded the local budget (%d bytes); "+
				"narrow the query or raise --local-fastpath-bytes: %w", what, sink.MaxBytes, err)
		}
		if refusal, ok := authorizationRefusal(err); ok {
			return nil, refusal
		}
		return nil, fmt.Errorf("%s local execution: %w", what, err)
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
	c.logger.Info("refused plan answered on the local pipeline",
		"query", queryID, "construct", what, "rows", totalRows, "elapsed", time.Since(start))
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
		// every column of an empty result while the same query through the
		// embedded API declared real OIDs.
		Schema: sink.Schema(),
		// A plan property, applies whether or not this result has rows
		// (FIX 2, #457/#458 fold-in).
		WireUnconstrainedDecimal: sink.SchemaHintWireUnconstrainedDecimal,
		StringLength:             sink.SchemaHintStringLength,
	}, nil
}

// authorizationRefusal returns the refusal itself when err carries SQLSTATE
// 42501, and reports whether it did — so a caller returns the shared
// decision's own sentence instead of its own routing narrative.
//
// The coordinator is the door a DEPLOYED server uses: `wadjet serve` puts
// pgwire and HTTP on this route, and every one of its wrappers names an
// internal route. A refused caller was told `table-less SELECT with no
// distributed stage local execution: permission denied for table "x"`, or
// `physical plan: permission denied for table "x"`, where the FROM-list denial
// on the same connection said `permission denied for table "x"` — one
// operation, two messages, and the difference names which internal route ran
// (ADR-0034 item 6; round-1 P1).
//
// It returns the innermost `*sqlerr.Error`, so a chain of wrappers collapses
// to the sentence the decision wrote — the table-function refusal reaches this
// route under two of them.
func authorizationRefusal(err error) (error, bool) {
	if sqlerr.StateOf(err) != "42501" {
		return nil, false
	}
	var refusal *sqlerr.Error
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return err, true
}
