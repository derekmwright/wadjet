// This file holds terminal gather dispatch and declared-column evaluation.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// dispatchGatherStage is terminal: dispatches a single Gather task to one
// worker, subscribes to its reply subject, and returns the assembled
// result. GatherOrdering + GatherLimit semantics are carried on the task
// so the worker can pre-sort; the receiver concatenates in arrival order.
func (c *Coordinator) dispatchGatherStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	depStage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (*gatherResult, error) {
	if len(stage.Dependencies) != 1 {
		return nil, fmt.Errorf("gather stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	// Dedicated reply subject: `wadjet.gather.<queryID>`. Avoids QueryResult
	// wildcard overlap (wadjet.results.<queryID>.>).
	replySubject := fmt.Sprintf("wadjet.gather.%s", queryID)

	// Ordered gather (#288 differential finding): a sort+limit that
	// fuseSortIntoPredecessor folded into a plan-time-Singleton compute
	// stage can still fan out at dispatch (broadcast-join probe-split), so
	// each task emits its own sorted ≤Limit-row output and a plain
	// streaming gather concatenates them — losing global order AND the
	// limit. When the upstream carries fused SortKeys, dispatch the gather
	// as a fragment that merge-sorts the pre-sorted inputs and applies the
	// limit worker-side: [ShuffleSource, OpSort{keys, limit}, GatherSink].
	// Standalone sort/merge_sort upstreams are excluded: they are true
	// single-task Singletons whose output is already globally ordered.
	var ordering []distributed.SortKeySpec
	switch depStage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin, physical.StageSortMergeJoin,
		"aggregate", "final_aggregate":
		for _, o := range depStage.SortKeys {
			ordering = append(ordering, distributed.SortKeySpec{Column: o.Column, Desc: o.Desc,
				NullsLast: distributed.NullsLastPtr(o.NullsLast), SlotPos: o.SlotPos})
		}
	}

	// Synthesize a task that runs the full SQL and streams its output.
	// Inputs alias: use the upstream stage ID so the worker's Inputs-based
	// source selection reads the upstream files.
	alias := stage.Dependencies[0]
	task := distributed.Task{
		ID:      uuid.New().String()[:8],
		QueryID: queryID,
		StageID: stage.ID,
		Type:    distributed.TaskTypeGather,
		SQLText: sql,
		// A plain scan is passed through rather than dispatched, so this
		// gather task does its reading — carry the upstream's pushed-down
		// bare LIMIT so it stops early (#311).
		RowLimit:     depStage.RowLimit,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		Inputs: map[string][]string{
			alias: flattenStageFiles(upstream),
		},
		ReplySubject: replySubject,
		CreatedAt:    time.Now(),
	}
	// A pass-through leaf scan hands its parquet keys to the CONSUMER, and
	// for a plain `SELECT … FROM t` that consumer is this gather task. So
	// the catalog's declared schema has to ride here too, or the read types
	// itself from files that cannot express nine of this engine's types —
	// and, where a file's stored type CONTRADICTS the catalog, decodes it
	// silently as whatever it holds (#423/#503). Empty for a gather over
	// stage output, which carries its own types in the WSHF payload; the
	// worker refuses a base-table read that arrives without it.
	scanTypes := wireColumnSpecs(upstream.ScanSchema)
	if len(scanTypes) == 0 {
		scanTypes = wireColumnSpecs(depStage.ScanSchema)
	}
	task.ColumnTypes = scanTypes
	if len(ordering) > 0 {
		task.Operators = []distributed.OpSpec{
			{
				Type:        distributed.OpShuffleSource,
				InputAlias:  alias,
				InputFiles:  task.Inputs[alias],
				InputBucket: c.config.ResultBucket,
				ColumnTypes: scanTypes,
			},
			{
				Type:         distributed.OpSort,
				SortKeySpecs: ordering,
				SortLimit:    depStage.Limit,
				HasSortLimit: depStage.HasLimit,
			},
			{
				Type:         distributed.OpGatherSink,
				ReplySubject: replySubject,
			},
		}
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	// Subscribe BEFORE publishing the task so the worker's batches and
	// terminal marker are not lost to a race. NATS does not buffer raw-
	// subject messages for late subscribers, so a goroutine that starts
	// subscribing after the publish can miss every reply (observed on
	// SF10: gather hung for 6+ minutes before the stage-level timeout
	// fired).
	c.logger.Info("gather: subscribing",
		"query_id", queryID, "reply_subject", replySubject)
	recv, err := subscribeGather(c.nc, replySubject, 1, c.workers, c.gatherResultBudget())
	if err != nil {
		return nil, fmt.Errorf("subscribing gather reply: %w", err)
	}
	// Remove unclaimed spill scratch on every path that returns before
	// wait() hands the result off. No-op after a successful wait().
	defer recv.discard()
	defer recv.registerWithDataPlane(c.dpSrv, queryID)()
	c.logger.Info("gather: publishing task",
		"query_id", queryID, "task_id", task.ID, "reply_subject", replySubject,
		"input_files", len(task.Inputs[alias]))
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing gather task: %w", err)
	}

	_ = workerCount // future: ordered gather dispatches one task per partition
	res, waitErr := recv.wait(ctx, gatherReceiveTimeout)
	c.logger.Info("gather: wait returned",
		"query_id", queryID, "msg_count", recv.msgCount.Load(),
		"err", waitErr)
	return res, waitErr
}

// evalDeclaredColumn materializes an expression at the type the PLAN declared
// for it, which for a column no catalog carries IS its runtime type
// (ADR-0025). It is exec.Project's own materialization — a vector of the
// declared type, SetValue per row — so the gather and the single-process
// projection build the same column from the same expression.
func evalDeclaredColumn(e expr.Expr, b *batch.RecordBatch, decl parquet.Column) *batch.Vector {
	v := batch.NewVectorWithScale(decl.Type, b.Len, decl.Scale)
	emit := func(row, dst int) { v.SetValue(dst, e.Eval(b, row)) }
	if b.Sel != nil {
		for i, src := range b.Sel {
			emit(int(src), i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			emit(i, i)
		}
	}
	return v
}
