package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// gatherReceiveTimeout bounds the coordinator's wait for worker terminal
// gather messages. Matches shuffleStageTimeout so long-running final
// aggregation stages don't falsely time out.
const gatherReceiveTimeout = 10 * time.Minute

// executeStageDAG is the native-DAG distributed executor. It walks the
// Exchange-annotated stage DAG in topological order, dispatches each
// stage via its Type-specific helper, records outputs in a per-query
// stageOutputs map, and terminates when it hits a Gather stage (which
// streams results to the coordinator directly).
//
// Gated by Coordinator.UseNativeDAG. When disabled, the legacy switch in
// ExecuteSQL handles dispatch. See spec:
// docs/superpowers/specs/2026-04-22-distribution-native-dag-execution-design.md
func (c *Coordinator) executeStageDAG(
	ctx context.Context,
	queryID, sql string,
	stages []physical.Stage,
	workerCount int,
) (*gatherResult, error) {
	if len(stages) == 0 {
		return nil, fmt.Errorf("executeStageDAG: empty stage list")
	}
	outputs := make(map[string]StageOutput, len(stages))

	for _, stage := range stages {
		inputs, err := collectInputs(stage, outputs)
		if err != nil {
			return nil, fmt.Errorf("stage %s: %w", stage.ID, err)
		}
		switch stage.Type {
		case physical.StageExchangeRepartition:
			out, err := c.dispatchShuffleStage(ctx, queryID, stage, inputs, workerCount)
			if err != nil {
				return nil, fmt.Errorf("shuffle stage %s: %w", stage.ID, err)
			}
			outputs[stage.ID] = out

		case physical.StageExchangeReplicate:
			out, err := c.dispatchReplicateStage(ctx, queryID, sql, stage, inputs)
			if err != nil {
				return nil, fmt.Errorf("replicate stage %s: %w", stage.ID, err)
			}
			outputs[stage.ID] = out

		case physical.StageExchangeGather:
			return c.dispatchGatherStage(ctx, queryID, sql, stage, inputs, workerCount)

		default:
			out, err := c.dispatchPipelineStage(ctx, queryID, sql, stage, inputs, workerCount)
			if err != nil {
				return nil, fmt.Errorf("pipeline stage %s: %w", stage.ID, err)
			}
			outputs[stage.ID] = out
		}
	}
	return nil, fmt.Errorf("executeStageDAG: plan for query %s terminated without Gather stage", queryID)
}

// dispatchShuffleStage executes a StageExchangeRepartition: reads upstream
// output, hash-partitions on stage.Exchange.Keys, writes one .wshf per
// (task, partition) to the query's shuffle prefix.
//
// Phase 3 scaffolding: wraps the existing runShuffleSide helper by adapting
// the upstream StageOutput's files into a synthetic source stage. The legacy
// helper expects a physical.Stage with ScanFiles populated — we satisfy
// that by flattening the upstream output.
//
// Known limitations (follow-up commits):
//   - Chained shuffles (Repartition consuming a previous Repartition's
//     partitioned output) use flattenStageFiles which loses partition
//     information. Correct end-to-end because the next shuffle rehashes,
//     but fails to exploit co-located partitions.
//   - Column pruning uses stage.Columns which may be over-approximated for
//     non-scan upstream stages.
func (c *Coordinator) dispatchShuffleStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if stage.Exchange == nil {
		return StageOutput{}, fmt.Errorf("repartition stage %s missing Exchange payload", stage.ID)
	}
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("repartition stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	depID := stage.Dependencies[0]
	upstream, ok := inputs[depID]
	if !ok {
		return StageOutput{}, fmt.Errorf("missing input for dep %s", depID)
	}
	sourceFiles := flattenStageFiles(upstream)
	if len(sourceFiles) == 0 {
		numParts := stage.Exchange.Count
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         make([][]string, numParts),
		}, nil
	}
	// Synthesize a source stage for runShuffleSide.
	synthetic := physical.Stage{
		ID:         stage.ID + "-src",
		Type:       physical.StageScan,
		ScanFiles:  sourceFiles,
		TableName:  stage.TableName, // may be empty for chained shuffles; worker falls back to generic scan
	}
	numParts := stage.Exchange.Count
	if numParts == 0 {
		numParts = workerCount * shufflePartitionMultiplier
	}
	shards, err := c.runShuffleSide(ctx, queryID, "stage-"+stage.ID, synthetic, stage.Exchange.Keys, numParts, workerCount)
	if err != nil {
		return StageOutput{}, err
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: numParts,
		Files:         shards,
	}, nil
}

// dispatchReplicateStage executes a StageExchangeReplicate: reads upstream
// output and materializes it into a single broadcast cache that all
// downstream workers read in full.
//
// Phase 3 scaffolding: delegates to preScanBuildTables when the upstream is
// a table scan (the common Phase 2a shape for build-side broadcasts); for
// stage-to-stage replicates the upstream files are already broadcast-shaped
// and we pass them through unchanged.
func (c *Coordinator) dispatchReplicateStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
) (StageOutput, error) {
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("replicate stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	// Pass-through: upstream already produced files; downstream workers
	// consume them as broadcast input. The legacy preScanBuildTables path
	// handles the source-table case via ExecuteSQL's explicit call.
	_ = ctx
	_ = queryID
	_ = sql
	return StageOutput{
		Kind:  OutputReplicated,
		Files: [][]string{flattenStageFiles(upstream)},
	}, nil
}

// dispatchPipelineStage executes a compute stage (scan/join/aggregate/etc.)
// by dispatching workerCount pipeline tasks that each read their assigned
// slice of upstream partitioned output and run the full SQL against it.
//
// Phase 3 scaffolding: wraps buildShufflePipelineTasks when the inputs look
// like a two-sided partitioned shuffle (build + probe). Leaf scan stages
// are passed through as StageOutput{Kind: OutputSinglePart, Files: scanFiles}
// without dispatching a task — they are consumed directly by the next
// Exchange stage. This matches the Phase 2a structure where leaf scans are
// not runtime-active stages.
//
// Known limitations: multi-step pipelines that need to run an intermediate
// join and emit WSHF-encoded partitioned output to a subsequent Exchange
// stage require worker-side SQL-fragment execution. That is a follow-up
// commit; the current path handles only the single-shuffle probe pipeline.
func (c *Coordinator) dispatchPipelineStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	_ = ctx
	_ = sql
	_ = workerCount
	// Leaf scan stage: pass through scan files as the stage's output. The
	// files are not yet materialized through a worker — downstream Exchange
	// stages treat ScanFiles as the upstream output.
	if stage.Type == physical.StageScan && len(inputs) == 0 {
		files := append([]string(nil), stage.ScanFiles...)
		return StageOutput{
			Kind:  OutputSinglePart,
			Files: [][]string{files},
		}, nil
	}
	// TODO(phase-3-followup): dispatch worker tasks for true intermediate
	// compute stages that produce .wshf output consumed by a downstream
	// Exchange. Requires worker support for partial-plan execution.
	return StageOutput{}, fmt.Errorf(
		"dispatchPipelineStage: non-leaf pipeline stage %q (type=%s) not yet implemented in native DAG",
		stage.ID, stage.Type,
	)
}

// dispatchGatherStage is terminal: dispatches a single Gather task to one
// worker, subscribes to its reply subject, and returns the assembled
// result. GatherOrdering + GatherLimit semantics are carried on the task
// so the worker can pre-sort; the receiver concatenates in arrival order.
func (c *Coordinator) dispatchGatherStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (*gatherResult, error) {
	if len(stage.Dependencies) != 1 {
		return nil, fmt.Errorf("gather stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	replySubject := distributed.QueryResultSubject(queryID) + ".gather"

	// Subscribe before publishing so we don't miss early messages.
	var ordering []distributed.SortKeySpec
	var limit int
	if stage.Exchange != nil {
		for _, o := range stage.Exchange.Ordering {
			ordering = append(ordering, distributed.SortKeySpec{Column: o.Column, Desc: o.Desc})
		}
	}

	// Synthesize a task that runs the full SQL and streams its output.
	// Inputs alias: use the upstream stage ID so the worker's Inputs-based
	// source selection reads the upstream files.
	alias := stage.Dependencies[0]
	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeGather,
		SQLText:      sql,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		Inputs: map[string][]string{
			alias: flattenStageFiles(upstream),
		},
		ReplySubject:   replySubject,
		GatherOrdering: ordering,
		GatherLimit:    limit,
		CreatedAt:      time.Now(),
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	// Kick off the receiver in a goroutine; publish the task; wait.
	type recvResult struct {
		r   *gatherResult
		err error
	}
	ch := make(chan recvResult, 1)
	go func() {
		res, err := runGatherReceiver(ctx, c.nc, replySubject, 1, gatherReceiveTimeout)
		ch <- recvResult{r: res, err: err}
	}()

	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing gather task: %w", err)
	}

	_ = workerCount // future: ordered gather dispatches one task per partition
	select {
	case rr := <-ch:
		return rr.r, rr.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
