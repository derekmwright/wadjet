// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// explainPlanSchema is EXPLAIN's one-column result, the same shape every other
// door answers EXPLAIN with: one row per line of plan text.
var explainPlanSchema = []parquet.Column{
	{Name: "plan", Type: parquet.TypeString},
}

// StagePlanTextForExplain renders the stage DAG this coordinator would
// dispatch for a plan, as EXPLAIN VERBOSE prints it.
//
// Both of this binary's doors call it, with the planner each already has, so
// the two answer the same text: the HTTP door (server.handleExplain) and the
// PostgreSQL wire door, which routes EXPLAIN here since the wire protocol has
// no stage planner of its own. It configures the stage planner exactly as
// ExecuteSQL does — the worker count, the broadcast threshold and the
// dynamic-filter switch are what decide the shape of the DAG, and an EXPLAIN
// that printed a plan built from other numbers would be describing a query
// this cluster would not run.
//
// A plan the distributed planner refuses renders as the local pipeline's own
// line rather than failing EXPLAIN: the statement's answer is a description,
// and "this would run in one process" is the true one for a shape the DAG
// declines.
func (c *Coordinator) StagePlanTextForExplain(ctx context.Context, planner *physical.Planner, logicalPlan *logical.Node) string {
	if c == nil || planner == nil {
		return "Single-stage local execution"
	}
	sp := dagplan.NewStagePlanner(planner)
	sp.WorkerCount = c.workers.Count()
	sp.BroadcastBytesThreshold = broadcastThresholdFromCluster(c.workers.MinWorkerPoolBudget())
	if c.config.BroadcastBytesOverride != 0 {
		sp.BroadcastBytesThreshold = c.config.BroadcastBytesOverride
	}
	sp.SortMergeJoinBytes = c.config.SortMergeJoinBytes
	sp.LateMaterialization = c.config.LateMaterialization
	sp.DynamicFiltersEnabled = c.config.DynamicFilters
	stages, err := sp.PlanDistributed(ctx, logicalPlan)
	if err != nil || len(stages) == 0 {
		return "Single-stage local execution"
	}
	return dagplan.PrettyPrintStages(stages)
}

// explainResult is EXPLAIN's answer on this coordinator: the logical plan, and
// for VERBOSE the stage DAG above.
//
// It exists because the PostgreSQL wire door used to answer EXPLAIN from the
// embedded database — which, once the local planner stopped emitting stages,
// meant a server about to dispatch a DAG printed "Single-stage local
// execution" over psql while its own HTTP door printed the DAG. Two doors of
// one binary answering one statement differently is the defect; routing
// EXPLAIN here is what makes them agree.
func (c *Coordinator) explainResult(ctx context.Context, planner *physical.Planner,
	logicalPlan *logical.Node, logicalText string, verbose bool,
) *SQLResult {
	text := logicalText
	if verbose {
		text += "\n\n-- Physical Plan --\n" + c.StagePlanTextForExplain(ctx, planner, logicalPlan)
	}
	lines := strings.Split(text, "\n")
	rows := make([]map[string]any, len(lines))
	for i, line := range lines {
		rows[i] = map[string]any{"plan": line}
	}
	return &SQLResult{
		Columns:   []string{"plan"},
		Schema:    explainPlanSchema,
		Batches:   []*batch.RecordBatch{batch.FromRows(explainPlanSchema, rows)},
		TotalRows: int64(len(rows)),
		Plan:      text,
	}
}
