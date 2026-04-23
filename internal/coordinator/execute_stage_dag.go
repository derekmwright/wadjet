package coordinator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"

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

	// Separate the terminal Gather from the DAG body: Gather is always run
	// last, on the coordinator's NATS reply subscription, and returns the
	// final result batches. Everything else is dispatched in waves where
	// each wave runs all ready sibling stages concurrently (sibling = all
	// dependencies already satisfied). Without concurrent waves, plans with
	// fan-out patterns like the multi-level merge_aggregate/merge_sort tree
	// (80+ independent siblings per query at SF10) serialize to one stage
	// per coordinator-to-worker round-trip — catastrophically slow.
	var gatherStage physical.Stage
	var hasGather bool
	pending := make(map[string]physical.Stage, len(stages))
	for _, s := range stages {
		if s.Type == physical.StageExchangeGather {
			if hasGather {
				return nil, fmt.Errorf("executeStageDAG: plan has multiple Gather stages")
			}
			gatherStage = s
			hasGather = true
			continue
		}
		pending[s.ID] = s
	}
	if !hasGather {
		return nil, fmt.Errorf("executeStageDAG: plan for query %s has no Gather stage", queryID)
	}

	outputs := make(map[string]StageOutput, len(stages))
	var outputsMu sync.Mutex
	wave := 0
	for len(pending) > 0 {
		wave++
		ready := make([]physical.Stage, 0)
		for _, s := range pending {
			allReady := true
			for _, depID := range s.Dependencies {
				if _, ok := outputs[depID]; !ok {
					allReady = false
					break
				}
			}
			if allReady {
				ready = append(ready, s)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("executeStageDAG: no ready stages but %d pending (cyclic dep or missing upstream?)", len(pending))
		}
		// Stable dispatch order for readable logs + reproducibility.
		sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })

		c.logger.Info("stage-DAG wave",
			"query", queryID, "wave", wave, "ready_stages", len(ready),
			"pending_after_wave", len(pending)-len(ready))

		g, gctx := errgroup.WithContext(ctx)
		waveResults := make(map[string]StageOutput, len(ready))
		var waveMu sync.Mutex
		for _, s := range ready {
			s := s
			g.Go(func() error {
				// collectInputs reads outputs only; it's not mutated during
				// the wave so no lock needed for reads.
				outputsMu.Lock()
				inputs, err := collectInputs(s, outputs)
				outputsMu.Unlock()
				if err != nil {
					return fmt.Errorf("stage %s collect inputs: %w", s.ID, err)
				}
				var out StageOutput
				switch s.Type {
				case physical.StageExchangeRepartition:
					out, err = c.dispatchShuffleStage(gctx, queryID, s, inputs, workerCount)
				case physical.StageExchangeReplicate:
					out, err = c.dispatchReplicateStage(gctx, queryID, sql, s, inputs)
				default:
					out, err = c.dispatchPipelineStage(gctx, queryID, sql, s, inputs, workerCount)
				}
				if err != nil {
					return fmt.Errorf("stage %s (%s): %w", s.ID, s.Type, err)
				}
				waveMu.Lock()
				waveResults[s.ID] = out
				waveMu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}

		// Publish this wave's outputs and retire its stages.
		outputsMu.Lock()
		for id, out := range waveResults {
			outputs[id] = out
		}
		outputsMu.Unlock()
		for id := range waveResults {
			delete(pending, id)
		}
	}

	// Terminal Gather.
	inputs, err := collectInputs(gatherStage, outputs)
	if err != nil {
		return nil, fmt.Errorf("gather stage %s: %w", gatherStage.ID, err)
	}
	return c.dispatchGatherStage(ctx, queryID, sql, gatherStage, inputs, workerCount)
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
	_ = sql
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
	// Compute stage: emit workerCount TaskTypeStage tasks, each reading its
	// slice of partitioned input and writing unpartitioned .wshf output.
	// The stage's output is collected into a Partitioned StageOutput where
	// partition p = the files produced by worker p — downstream Exchange
	// stages treat this as partitioned if they need to re-hash.
	return c.dispatchComputeStage(ctx, queryID, stage, inputs, workerCount)
}

// dispatchComputeStage handles non-leaf compute stages (hash_join,
// broadcast_join, aggregate, final_aggregate, sort, merge_sort) by
// emitting workerCount TaskTypeStage tasks. Each task's Inputs are
// sliced from upstream stage outputs via partitionFilesForWorker; each
// task writes unpartitioned .wshf output to a per-worker result key.
func (c *Coordinator) dispatchComputeStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1 // single-worker fallback for sparse test / bootstrap setups
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Debug("dispatchComputeStage entry",
		"stage_id", stage.ID, "stage_type", stage.Type,
		"deps", stage.Dependencies, "worker_count", workerCount,
		"inputs_aliases", len(inputs))

	// Build one task per worker. For joins, the two Inputs are keyed by
	// build/probe alias derived from stage.Dependencies and stage.BuildTableAlias.
	tasks := make([]distributed.Task, 0, workerCount)
	for w := 0; w < workerCount; w++ {
		taskInputs, err := buildTaskInputsForStage(stage, inputs, w, workerCount)
		if err != nil {
			return StageOutput{}, fmt.Errorf("stage %s worker %d: %w", stage.ID, w, err)
		}
		// Convert stage.AggSpecs → distributed.AggSpec.
		var aggs []distributed.AggSpec
		for _, a := range stage.AggSpecs {
			aggs = append(aggs, distributed.AggSpec{
				Func:      a.Func,
				InputCol:  a.InputCol,
				OutputCol: a.OutputCol,
			})
		}
		// Convert stage.SortKeys → distributed.SortKeySpec.
		var sorts []distributed.SortKeySpec
		for _, s := range stage.SortKeys {
			sorts = append(sorts, distributed.SortKeySpec{Column: s.Column, Desc: s.Desc})
		}
		t := distributed.Task{
			ID:              uuid.New().String()[:8],
			QueryID:         queryID,
			StageID:         stage.ID,
			Type:            distributed.TaskTypeStage,
			StageType:       stage.Type,
			JoinType:        stage.JoinType,
			JoinLeftKeys:    stage.JoinLeftKeys,
			JoinRightKeys:   stage.JoinRightKeys,
			BuildTableAlias: stage.BuildTableAlias,
			JoinFilter:      stage.JoinFilter,
			GroupByCols:     stage.GroupByCols,
			Aggregates:      aggs,
			SortKeys:        sorts,
			Limit:           stage.Limit,
			Inputs:          taskInputs,
			DataBucket:      c.config.ResultBucket,
			ResultBucket:    c.config.ResultBucket,
			ResultPrefix:    resultPrefix,
			CreatedAt:       time.Now(),
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
	}

	// Dispatch via scheduler + shuffle-side-style result collection.
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {
			StageID:    stage.ID,
			Type:       distributed.TaskTypeStage,
			TotalTasks: len(tasks),
		},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	subject := distributed.QueryResultSubject(stageQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
	}
	collected := make([]taskResult, 0, len(tasks))
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &r); err != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= len(tasks) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("stage %s: subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	c.logger.Info("dispatching compute stage",
		"stage_id", stage.ID, "stage_type", stage.Type,
		"tasks", len(tasks), "subject", subject)
	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("stage %s: publish: %w", stage.ID, err)
	}

	select {
	case <-done:
		c.logger.Info("compute stage complete", "stage_id", stage.ID, "tasks", len(tasks))
	case <-ctx.Done():
		mu.Lock()
		got := len(collected)
		mu.Unlock()
		return StageOutput{}, fmt.Errorf("stage %s: ctx done with %d/%d results: %w", stage.ID, got, len(tasks), ctx.Err())
	case <-time.After(shuffleStageTimeout):
		return StageOutput{}, fmt.Errorf("stage %s: timed out after %s", stage.ID, shuffleStageTimeout)
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()

	files := make([][]string, len(tasks))
	for i, r := range results {
		if r.err != "" {
			return StageOutput{}, fmt.Errorf("stage %s: task failed: %s", stage.ID, r.err)
		}
		files[i] = r.files
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         files,
	}, nil
}

// buildTaskInputsForStage maps a stage's Dependencies (upstream stage IDs)
// into Task.Inputs keyed by a per-stage alias convention:
//   - hash_join/broadcast_join: use stage.BuildTableAlias for the build
//     side (dep index 1) and "probe" for the other side (dep index 0).
//   - aggregate/sort/etc: use the single dep's ID as alias.
func buildTaskInputsForStage(stage physical.Stage, upstreams map[string]StageOutput, workerIdx, workerCount int) (map[string][]string, error) {
	inputs := make(map[string][]string)
	switch stage.Type {
	case physical.StageHashJoin, physical.StageBroadcastJoin:
		if len(stage.Dependencies) != 2 {
			return nil, fmt.Errorf("join stage %s expects 2 deps, got %d", stage.ID, len(stage.Dependencies))
		}
		probeDep := stage.LeftDepStage
		buildDep := stage.RightDepStage
		if probeDep == "" {
			probeDep = stage.Dependencies[0]
		}
		if buildDep == "" {
			buildDep = stage.Dependencies[1]
		}
		buildAlias := stage.BuildTableAlias
		if buildAlias == "" {
			buildAlias = "build"
		}
		probeAlias := "probe"
		if probeAlias == buildAlias {
			probeAlias = "probe_side"
		}
		inputs[buildAlias] = partitionFilesForWorker(upstreams[buildDep], workerIdx, workerCount)
		inputs[probeAlias] = partitionFilesForWorker(upstreams[probeDep], workerIdx, workerCount)
	default:
		if len(stage.Dependencies) == 0 {
			return nil, fmt.Errorf("stage %s has no dependencies and no ScanFiles", stage.ID)
		}
		// Single-input stages (aggregate/sort): use dep ID as alias.
		depID := stage.Dependencies[0]
		inputs[depID] = partitionFilesForWorker(upstreams[depID], workerIdx, workerCount)
	}
	return inputs, nil
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
	// Dedicated reply subject: `wadjet.gather.<queryID>`. Avoids QueryResult
	// wildcard overlap (wadjet.results.<queryID>.>).
	replySubject := fmt.Sprintf("wadjet.gather.%s", queryID)

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

	// Subscribe BEFORE publishing the task so the worker's batches and
	// terminal marker are not lost to a race. NATS does not buffer raw-
	// subject messages for late subscribers, so a goroutine that starts
	// subscribing after the publish can miss every reply (observed on
	// SF10: gather hung for 6+ minutes before the stage-level timeout
	// fired).
	recv, err := subscribeGather(c.nc, replySubject, 1)
	if err != nil {
		return nil, fmt.Errorf("subscribing gather reply: %w", err)
	}
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, fmt.Errorf("publishing gather task: %w", err)
	}

	_ = workerCount // future: ordered gather dispatches one task per partition
	return recv.wait(ctx, gatherReceiveTimeout)
}
