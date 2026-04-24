package coordinator

import (
	"context"
	"fmt"
	"strings"
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

	// Register parent queryID in the tracker so SubjectQueryActive replies
	// "active" for this query. Without this the worker's pre-execute
	// is-query-still-active probe (worker.go:402) gets "0" back and terms
	// the task message silently. Compute stages work because
	// dispatchComputeStage registers its own ephemeral stageQueryID; the
	// Gather task rides on the parent queryID which is ONLY registered by
	// the legacy ExecuteSQL code path that native-DAG bypasses at line 479.
	trackerStages := make(map[string]*StageInfo, len(stages))
	stageOrder := make([]string, 0, len(stages))
	for _, s := range stages {
		trackerStages[s.ID] = &StageInfo{
			StageID:      s.ID,
			Type:         distributed.TaskType(s.Type),
			TotalTasks:   s.Tasks,
			Dependencies: s.Dependencies,
		}
		stageOrder = append(stageOrder, s.ID)
	}
	c.tracker.Register(queryID, sql, trackerStages, stageOrder)
	c.tracker.Start(queryID)
	defer c.tracker.Delete(queryID)

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

	// Per-stage completion signal. Each stage goroutine closes its `done`
	// channel when its output is published into `outputs`. Dependent
	// stages block on the union of their dep signals before dispatching
	// their own work. This is a straight DAG scheduler — each stage
	// starts as soon as its own dependencies are satisfied, instead of
	// waiting for an entire wave to drain. Plans with parallel branches
	// of unequal length (e.g., two sides of a shuffle join where one
	// side has an extra filter stage) now overlap properly instead of
	// stalling on the longest path in each wave.
	done := make(map[string]chan struct{}, len(pending))
	for id := range pending {
		done[id] = make(chan struct{})
	}
	c.logger.Info("stage-DAG dispatch", "query", queryID, "stages", len(pending))

	g, gctx := errgroup.WithContext(ctx)
	for _, s := range pending {
		s := s
		g.Go(func() error {
			// Wait on each dependency's done signal. Deps not in the
			// `done` map are leaf stages / pre-computed outputs (e.g.,
			// the coordinator's initial inputs) and are treated as
			// immediately ready.
			for _, depID := range s.Dependencies {
				ch, tracked := done[depID]
				if !tracked {
					continue
				}
				select {
				case <-ch:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
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
			outputsMu.Lock()
			outputs[s.ID] = out
			outputsMu.Unlock()
			close(done[s.ID])
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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
	// Leaf scan stage.
	if stage.Type == physical.StageScan && len(inputs) == 0 {
		// Fused scan-aggregate: the planner marked this scan to produce
		// partial aggregates (saves the scan→aggregate round-trip in the
		// legacy single-pipeline executor). Under native-DAG we must
		// honor that signal by dispatching N partial-aggregate tasks —
		// each worker reads its slice of scan files, aggregates, and
		// emits a partial result. The downstream final_aggregate stage
		// merges across partial outputs.
		//
		// Without this path, a Singleton final_aggregate over a large
		// leaf scan ran on ONE worker reading all scan files serially —
		// at SF10 that was the dominant cost on Q01 (87s of 87s wall
		// time was the single-worker lineitem scan).
		if len(stage.FusedAggSpecs) > 0 {
			return c.dispatchScanAggregateStage(ctx, queryID, stage, workerCount)
		}
		// Plain scan with no fused aggregate: downstream Exchange stages
		// treat ScanFiles as the upstream output (pass-through).
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

// dispatchScanAggregateStage dispatches N partial-aggregate tasks, one
// per worker, each reading a disjoint slice of stage.ScanFiles. Each task
// runs a HashAggregate on its file slice and emits a partial result as
// its stage output. Downstream final_aggregate stages (which are almost
// always immediately adjacent) merge the N partial outputs into the
// logical full aggregate.
//
// Why scan-side fan-out: scan is the only leaf at plan time, so a
// Singleton-distributed final_aggregate reading "the scan's output"
// serialises to one worker reading every parquet file. Splitting the
// scan N ways + computing partial aggregates in parallel is the same
// map-reduce shape the legacy executor used (via emitMergeAggregateTree
// → scan-level fused-agg + multi-level merge tree) before native-DAG
// collapsed the tree. Now we re-introduce the fan-out, but without the
// extra merge-tree levels: one partial step, one final merge.
func (c *Coordinator) dispatchScanAggregateStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	workerCount int,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1
	}
	// AVG cannot merge across partials without decomposing into
	// sum+count pair — taking the average of per-partition averages
	// silently produces wrong values (unweighted mean). Until AVG
	// partial decomposition lands, fall back to a single task so the
	// whole aggregate runs on one worker and the merge step is a
	// pass-through. Loses parallelism for AVG queries (Q01 is the
	// notable one) but preserves correctness.
	for _, a := range stage.FusedAggSpecs {
		if strings.EqualFold(strings.TrimSpace(a.Func), "avg") {
			c.logger.Info("scan-aggregate AVG fallback: single-task",
				"stage_id", stage.ID, "func", a.Func)
			workerCount = 1
			break
		}
	}
	fileSets := splitFilesEvenly(stage.ScanFiles, workerCount)
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: 0,
			Files:         nil,
		}, nil
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchScanAggregateStage",
		"stage_id", stage.ID, "files", len(stage.ScanFiles),
		"tasks", actualTasks, "group_by", stage.FusedAggGroupBy)

	// Convert AggSpec → wire format once.
	var aggs []distributed.AggSpec
	for _, a := range stage.FusedAggSpecs {
		aggs = append(aggs, distributed.AggSpec{
			Func:      a.Func,
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
			InputExpr: a.InputExpr,
		})
	}

	tasks := make([]distributed.Task, 0, actualTasks)
	for w, files := range fileSets {
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      queryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeStage,
			StageType:    "aggregate",
			TableName:    stage.TableName,
			Columns:      stage.Columns,
			GroupByCols:  stage.FusedAggGroupBy,
			Aggregates:   aggs,
			// Propagate scan-pushed WHERE fragments. Without this the worker
			// aggregates every row in the file slice and ignores the query's
			// predicate — group counts match legacy but aggregate VALUES are
			// wrong. E.g. Q01's `WHERE l_shipdate <= '1998-09-02'` was being
			// silently dropped in native-DAG before this plumb.
			FilterExprs: append([]string(nil), stage.FilterExprs...),
			Inputs: map[string][]string{
				// Use the scan's table name as alias so the worker's
				// sourceForAlias opens these files via the parquet path.
				scanAliasForStage(stage): files,
			},
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
		_ = w
	}

	// Dispatch and collect (same pattern as dispatchComputeStage).
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.Register(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	for i := range tasks {
		tasks[i].QueryID = stageQueryID
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	collected := make([]taskResult, 0, len(tasks))
	var mu sync.Mutex
	done := make(chan struct{}, 1)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
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
		return StageOutput{}, fmt.Errorf("scan-agg stage %s subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("scan-agg stage %s publish: %w", stage.ID, err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		return StageOutput{}, ctx.Err()
	case <-time.After(shuffleStageTimeout):
		return StageOutput{}, fmt.Errorf("scan-agg stage %s timeout", stage.ID)
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()
	files := make([][]string, len(tasks))
	for i, r := range results {
		if r.err != "" {
			return StageOutput{}, fmt.Errorf("scan-agg stage %s: task failed: %s", stage.ID, r.err)
		}
		files[i] = r.files
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         files,
	}, nil
}

// scanAliasForStage returns the alias key used when handing scan-fused
// files to a worker's Inputs map. Falls back to the stage's TableName
// or scan ID so the worker can resolve the parquet source.
func scanAliasForStage(stage physical.Stage) string {
	if stage.ScanAlias != "" {
		return stage.ScanAlias
	}
	if stage.TableName != "" {
		return stage.TableName
	}
	return stage.ID
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
	// Task count derivation:
	//   - Singleton output: exactly 1 task. The stage produces one logical
	//     result; running workerCount tasks on the same unpartitioned input
	//     duplicates the output N× (caught on SF10 Q01: 18 rows instead of
	//     6 with workerCount=3).
	//   - Hash-partitioned output: one task per output partition. The
	//     planner's distribution.Count is authoritative; fall back to
	//     workerCount if unset.
	//   - Broadcast output: 1 task producing a file every consumer reads.
	//   - Anything else: correctness-first default of 1 task.
	numTasks := 1
	switch stage.Distribution.Kind {
	case physical.DistSingleton:
		numTasks = 1
	case physical.DistHashPartitioned:
		numTasks = stage.Distribution.Count
		if numTasks <= 0 {
			numTasks = workerCount
		}
	case physical.DistBroadcast:
		numTasks = 1
	default:
		// Unknown distribution — fall back to 1 rather than workerCount.
		numTasks = 1
	}
	// NB: stage.Tasks from the legacy planner encodes probe-split intent
	// (e.g., Tasks=workerCount for broadcast_join means "split probe files
	// across N workers"). That semantics assumes each task receives a
	// DIFFERENT slice of probe files — which only works if the coordinator
	// slices probe input per task. Native-DAG dispatch today hands every
	// Singleton-stage task the same full probe set (partitionFilesForWorker
	// on Singleton input returns the full list for every worker), so
	// honoring stage.Tasks=N duplicates the full scan/join N× and triples
	// the worker memory pressure. Until native-DAG grows probe-split
	// semantics, Distribution is the authoritative signal and stage.Tasks
	// is intentionally ignored here.
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchComputeStage",
		"stage_id", stage.ID, "stage_type", stage.Type,
		"deps", stage.Dependencies, "num_tasks", numTasks,
		"distribution_kind", stage.Distribution.Kind,
		"distribution_count", stage.Distribution.Count,
		"inputs_aliases", len(inputs))

	// Build one task per output partition. Input slicing uses numTasks as
	// the divisor so each task reads its share of the upstream partitioned
	// input — for Singleton stages every task reads the full upstream; for
	// Hash-partitioned N-task stages each task reads its N-th partition.
	tasks := make([]distributed.Task, 0, numTasks)
	for w := 0; w < numTasks; w++ {
		taskInputs, err := buildTaskInputsForStage(stage, inputs, w, numTasks)
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
				InputExpr: a.InputExpr,
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
			// Filters attached to a compute stage (HAVING on
			// aggregate/final_aggregate, residual predicates on
			// hash_join) reference OUTPUT columns and must run after
			// the stage's main operator. FilterExprs on compute stages
			// would otherwise silently drop — that's the root cause of
			// Q15 ignoring `WHERE total_revenue = (SELECT MAX...)` and
			// Q18 ignoring `HAVING SUM(l_quantity) > 300`.
			PostFilterExprs: append([]string(nil), stage.FilterExprs...),
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
	// Kind reflects what the planner said this stage produces. Single-
	// task Singleton stages label their output OutputSinglePart so
	// downstream consumers (partitionFilesForWorker) return the full file
	// list for every worker. Hash-partitioned stages produce Partitioned
	// output; broadcast produces Replicated; anything else falls back to
	// SinglePart (one worker consumed all input).
	kind := OutputSinglePart
	switch stage.Distribution.Kind {
	case physical.DistHashPartitioned:
		kind = OutputPartitioned
	case physical.DistBroadcast:
		kind = OutputReplicated
	}
	return StageOutput{
		Kind:          kind,
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
	c.logger.Info("gather: subscribing",
		"query_id", queryID, "reply_subject", replySubject)
	recv, err := subscribeGather(c.nc, replySubject, 1)
	if err != nil {
		return nil, fmt.Errorf("subscribing gather reply: %w", err)
	}
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
