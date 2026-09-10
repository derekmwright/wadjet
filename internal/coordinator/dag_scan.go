// This file holds scan aggregate and scan filter task dispatch.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

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
	// AVG cannot directly merge across partials (avg-of-averages is
	// arithmetically wrong) but it CAN be decomposed into per-partition
	// SUM and COUNT, which DO merge correctly. decomposeAvg expands
	// every AVG spec into (SUM, COUNT) twin specs with synthetic output
	// names — the worker's post-merge fold (avg_fold.go in package
	// worker) reconstructs AVG = SUM/COUNT after the final_aggregate
	// stage merges the partials. With this in place there's no
	// reason to fall back to single-task on AVG queries; Q01 SF10
	// drops from a single-worker scan of 60M rows to N-way fan-out.
	taskCount := workerCount
	capacity := c.workers.ClusterCapacity()
	var fileSets [][]string
	var affinity []string
	var shardCount int
	if len(stage.ScanFiles) == 1 {
		shardCount = scanShardCountForSingleFile(workerCount, capacity, stage.EstimatedBytes)
	}
	if shardCount > 1 {
		// Single-file row-group sharding for fused scan-aggregate. Each
		// shard task aggregates its row-group slice; the downstream
		// final_aggregate merges across the N partial outputs.
		fileSets = make([][]string, shardCount)
		for i := range fileSets {
			fileSets[i] = stage.ScanFiles
		}
		taskCount = shardCount
	} else {
		taskCount = scanFanOutTaskCount(workerCount, capacity, len(stage.ScanFiles))
		var bal *affineBalance
		fileSets, affinity, bal = affineFileSets(stage.ScanFiles, stage.ScanFileSizes, c.activeWorkerIDs(), taskCount)
		c.logAffineBalance(stage.ID, bal)
		if fileSets == nil {
			fileSets = splitFilesEvenly(stage.ScanFiles, taskCount)
		}
	}
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
		"tasks", actualTasks, "group_by", stage.FusedAggGroupBy,
		"shard_count", shardCount)

	// Convert AggSpec → wire format once. decomposeAvg expands AVG into
	// SUM+COUNT pairs so the partial fan-out can run in parallel; the
	// worker's avg-fold step (executor_stage.go in package worker) folds
	// the synthetic columns back into AVG after the downstream
	// final_aggregate stage merges the partials.
	aggs := wireAggSpecs(stage.FusedAggSpecs)
	aggs = c.decomposeOhlcvFor(decomposeCovar(decomposeVar(decomposeAvg(aggs))))

	tasks := make([]distributed.Task, 0, actualTasks)
	for shardIdx, files := range fileSets {
		t := distributed.Task{
			ID:               uuid.New().String()[:8],
			QueryID:          queryID,
			StageID:          stage.ID,
			Type:             distributed.TaskTypeStage,
			StageType:        "aggregate",
			AffinityWorkerID: affinityFor(affinity, shardIdx),
			TableName:        stage.TableName,
			Columns:          stage.Columns,
			GroupByCols:      stage.FusedAggGroupBy,
			Aggregates:       aggs,
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
		if shardCount > 1 {
			t.ScanShardIndex = shardIdx
			t.ScanShardCount = shardCount
		}
		// Fragment migration: dispatch via the multi-op fragment runner.
		// Two terminal sink shapes:
		//   - downstream Repartition Exchange absorbed by
		//     fuseScanAggregateShuffle → OpExchangeSender (each task's
		//     K aggregate rows hash-partition directly to the consumer's
		//     partition layout, skipping the standalone exchange-repartition).
		//   - otherwise → OpUnpartitionedSink (one .wshf per task;
		//     downstream Singleton final_aggregate reads them all).
		var sinkOp distributed.OpSpec
		if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
			sinkOp = distributed.OpSpec{
				Type:            distributed.OpExchangeSender,
				ShuffleKeys:     append([]string(nil), stage.Exchange.Keys...),
				ShuffleKeyTypes: wireKeyTypes(stage.Exchange.KeyTypes),
				NumPartitions:   stage.Exchange.Count,
			}
		} else {
			sinkOp = distributed.OpSpec{Type: distributed.OpUnpartitionedSink}
		}
		ops, ferr := buildScanAggregateFragment(stage, &t, files, aggs, shardIdx, shardCount, sinkOp)
		if ferr != nil {
			return StageOutput{}, fmt.Errorf("scan-agg stage %s: build fragment: %w", stage.ID, ferr)
		}
		t.Operators = ops
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		c.mu.Lock()
		qm := c.queryMetas[queryID]
		c.mu.Unlock()
		c.enrichTaskWithQueryContext(qm, &t)
		tasks = append(tasks, t)
	}

	// Dispatch and collect (same pattern as dispatchComputeStage).
	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.RegisterInternal(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	// Per-task admission estimate: the catalog-true scan bytes split
	// across tasks (files are split evenly; row-group shards each read
	// ~1/N of the file).
	perTaskEst := int64(0)
	if stage.EstimatedBytes > 0 {
		perTaskEst = stage.EstimatedBytes / int64(len(tasks))
	}
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
		tasks[i].EstimatedBytes = perTaskEst
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Scan-aggregate tasks sink to S3 (unpartitioned or exchange-sender),
	// never gather-fused — retry is always safe.
	retrier := newTaskRetrier(tasks, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("scan-agg task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
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
	if err := awaitStageProgress(ctx, done, progress, "scan-agg "+stage.ID); err != nil {
		return StageOutput{}, err
	}

	if f, failed := retrier.FirstError(); failed {
		return StageOutput{}, stageTaskFailure(f, fmt.Errorf("scan-agg stage %s: task %s failed after %d attempts: %s", stage.ID, f.TaskID, maxTaskAttempts, f.Message))
	}
	resultFiles := retrier.Files()
	if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0 {
		// Fused scan-aggregate + shuffle: each task produced N partition
		// files at "<prefix>partition=NNNN/<task>.wshf". Bucket all files
		// across all tasks by partition number so the downstream consumer
		// reads each partition's full set. Same shape as
		// dispatchScanFilterStage's fuseShuffle path.
		numParts := stage.Exchange.Count
		shardFiles := make([][]string, numParts)
		for _, taskFiles := range resultFiles {
			for _, f := range taskFiles {
				p, parseErr := parsePartitionFromPath(f)
				if parseErr != nil {
					return StageOutput{}, fmt.Errorf("scan-agg stage %s: parsing partition from %q: %w", stage.ID, f, parseErr)
				}
				if p < 0 || p >= numParts {
					return StageOutput{}, fmt.Errorf("scan-agg stage %s: partition %d out of range [0,%d) in %q", stage.ID, p, numParts, f)
				}
				shardFiles[p] = append(shardFiles[p], f)
			}
		}
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         shardFiles,
			Bytes:         retrier.TotalBytes(),
		}, nil
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         resultFiles,
		Bytes:         retrier.TotalBytes(),
	}, nil
}

// dispatchScanFilterStage runs a scan-and-filter pipeline fanned out across
// workers. Used when a leaf scan carries scan-pushed FilterExprs but no
// fused aggregate — each worker reads its slice of stage.ScanFiles, applies
// FilterExprs, and emits a partial output. Downstream stages consume the
// partitioned output the same way they consume the dispatchScanAggregateStage
// output: via partitionFilesForWorker.
//
// Pre-fan-out (single-task) was the bottleneck on Q04 SF10: scanning 600
// lineitem files in one task took >10m and tripped the new progress-based
// idle timeout (no completion signals can land for a 1-task stage). Fan-out
// makes scan-filter symmetric with scan-aggregate: workerCount tasks, each
// with a slice of files, each emits independently — completions stream in
// every minute or two, idle detection works as designed, and total wall
// drops linearly with worker count.
func (c *Coordinator) dispatchScanFilterStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if workerCount <= 0 {
		workerCount = 1
	}
	if len(stage.ScanFiles) == 0 {
		return StageOutput{Kind: OutputPartitioned, NumPartitions: 0, Files: nil}, nil
	}
	// Single-file sharding path: when the leaf scan has exactly one file
	// above the threshold, fan out N row-group shard tasks instead of one
	// whole-file task. Without this, a compacted single-file source
	// (e.g. SF10 partsupp = 691 MB) caps fan-out at fileCount=1, which
	// then cascades single-tasked through the entire downstream broadcast-
	// join chain (probe-split requires probe upstream to have ≥ 2 files).
	// The shards each emit one output file → the chain inherits parallelism
	// for free.
	capacity := c.workers.ClusterCapacity()
	var fileSets [][]string
	var affinity []string
	var shardCount int
	if len(stage.ScanFiles) == 1 {
		shardCount = scanShardCountForSingleFile(workerCount, capacity, stage.EstimatedBytes)
	}
	if shardCount > 1 {
		// One row-group shard per task, all reading the same single file.
		fileSets = make([][]string, shardCount)
		for i := range fileSets {
			fileSets[i] = stage.ScanFiles
		}
	} else {
		// Task count: prefer cluster capacity (workers × auto-tuned MaxConcurrent)
		// over raw worker count so each task stays under the per-worker memory
		// budget. With 3 workers × MaxConcurrent=2, that's 6 tasks instead of 3
		// — for SF10 lineitem (600 files) that's 100 files/task instead of 200,
		// which keeps each task's heap below the 32GB worker limit and avoids
		// the OOM-restart-then-idle-timeout pattern observed on Q04 SF10.
		taskCount := scanFanOutTaskCount(workerCount, capacity, len(stage.ScanFiles))
		var bal *affineBalance
		fileSets, affinity, bal = affineFileSets(stage.ScanFiles, stage.ScanFileSizes, c.activeWorkerIDs(), taskCount)
		c.logAffineBalance(stage.ID, bal)
		if fileSets == nil {
			fileSets = splitFilesEvenly(stage.ScanFiles, taskCount)
		}
	}
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		return StageOutput{Kind: OutputPartitioned, NumPartitions: 0, Files: nil}, nil
	}
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	c.logger.Info("dispatchScanFilterStage",
		"stage_id", stage.ID, "files", len(stage.ScanFiles),
		"tasks", actualTasks, "filters", len(stage.FilterExprs),
		"shard_count", shardCount,
		"df_emits", len(stage.EmitDynamicFilters),
		"df_consumes", len(stage.ConsumeDynamicFilters))

	// Fragment dispatch: when the planner's fuseScanShuffle pass absorbed a
	// downstream exchange-repartition into this scan, emit a 2-op (or 3-op
	// with filter) Operators[] pipeline so the worker streams the filtered
	// scan output directly into a partitioned shuffle sink. Saves one S3 PUT
	// + one S3 GET + one NATS round-trip vs the legacy scan→shuffle two-task
	// path. The pre-2026-05-03 narrow PR (#85) did this via stage.ShuffleKeys
	// on a single-op task, but that hit the worker's CollectSink memory blow-
	// up on Q05 SF10; the fragment path uses runStageScanPartitionedStreaming.
	fuseShuffle := stage.Exchange != nil && len(stage.Exchange.Keys) > 0 && stage.Exchange.Count > 0
	// Probe-side consume specs are identical for every task — materialize
	// once and log the attach outcome. attached < requested means the
	// build side produced no eligible partials for some FilterID and that
	// consume silently degraded to scan-without-filter (correct but worth
	// seeing in an A/B run).
	var consumeSpecs []distributed.DynamicFilterSpec
	if len(stage.ConsumeDynamicFilters) > 0 {
		consumeSpecs = dynamicFilterSpecsFromBuildStats(stage.ConsumeDynamicFilters, inputs, queryID, c.config.ResultBucket)
		deferred := 0
		for _, s := range consumeSpecs {
			if s.Deferred {
				deferred++
			}
		}
		c.logger.Info("dynamic_filter: consume specs attached to probe scan",
			"stage_id", stage.ID,
			"requested", len(stage.ConsumeDynamicFilters),
			"attached", len(consumeSpecs),
			"deferred", deferred)
	}
	tasks := make([]distributed.Task, 0, actualTasks)
	for shardIdx, files := range fileSets {
		t := distributed.Task{
			ID:        uuid.New().String()[:8],
			QueryID:   queryID,
			StageID:   stage.ID,
			Type:      distributed.TaskTypeStage,
			TableName: stage.TableName,
			// A bare LIMIT pushed onto this scan (#311): each task stops
			// pulling at n instead of reading its whole slice, and the
			// coordinator trims the union of tasks to the real limit.
			RowLimit:         stage.RowLimit,
			AffinityWorkerID: affinityFor(affinity, shardIdx),
			DataBucket:       c.config.ResultBucket,
			ResultBucket:     c.config.ResultBucket,
			ResultPrefix:     resultPrefix,
			CreatedAt:        time.Now(),
			// Emitter scans ride the priority lane for the same reason
			// they bypass the dispatch semaphore: their completion is what
			// makes the concurrently-running bulk scans cheap, and the
			// bulk fan-out must not be able to queue them out. Emitters
			// that ALSO consume (guarded re-emit mids) ride the lane's
			// "deep" class — a disjoint slot pool, because they may block
			// at finalize on a leaf emitter's bloom and must never occupy
			// the slots that leaf needs (lane deadlock otherwise).
			// IN-FLOW emitters (cascade mids above the dimension-class
			// cap, e.g. 15M-row customer) stay OFF both lanes: the lane's
			// extra-slots-above-MaxConcurrent contract is only memory-safe
			// for planner-bounded tiny scans, and an in-flow mid's
			// consumer is WAIT-blocked on its stat-dep, so normal
			// scheduling serves it correctly.
			Priority:     stageHasLaneEmit(stage),
			PriorityDeep: stageHasLaneEmit(stage) && len(stage.ConsumeDynamicFilters) > 0,
		}
		// Both branches emit fragment Operators[]. fuseShuffle terminates
		// in OpExchangeSender (writing partitioned shuffle output);
		// non-fuseShuffle terminates in OpUnpartitionedSink (one .wshf
		// per task) — same shape the legacy executeStageScan emitted.
		scanOp := distributed.OpSpec{
			Type:        distributed.OpScan,
			InputAlias:  scanAliasForStage(stage),
			InputFiles:  files,
			InputBucket: c.config.ResultBucket,
			Columns:     stage.Columns,
			// The catalog's types beside the catalog's names: a file written
			// before the declared-schema footer key existed cannot say what
			// its IPv4/UUID/MAC columns are, and without this the scan reads
			// them as the INT64/BYTE_ARRAY they are stored in (#423).
			ColumnTypes: wireColumnSpecs(stage.ScanSchema),
		}
		if shardCount > 1 {
			scanOp.ScanShardIndex = shardIdx
			scanOp.ScanShardCount = shardCount
		}
		// Build-side: each task emits a partial bloom+range artifact.
		// Source-side emits (legacy leaf-scan blooms) accumulate at the
		// scan; AtOutput emits (semi/anti build filters) ride the sink
		// OpSpec below so they observe the stage's post-filter OUTPUT.
		var outputEmits []distributed.DynamicFilterEmit
		if len(stage.EmitDynamicFilters) > 0 {
			var scanEmits []distributed.DynamicFilterEmit
			for _, e := range stage.EmitDynamicFilters {
				em := distributed.DynamicFilterEmit{
					FilterID:      e.FilterID,
					KeyColumn:     e.KeyColumn,
					KeyType:       e.KeyType,
					BloomBits:     e.BloomBits,
					AtOutput:      e.AtOutput,
					GuardConsumes: append([]string(nil), e.GuardConsumes...),
					// Count-in-key completeness target + upload prefix for
					// incremental partial publication: attach-on-arrival
					// consumers listing the prefix learn from any one
					// ".of<N>" partial how many the stage will produce.
					// StagePartials must equal the expectedTasks the
					// coordinator's own mergeCompleteBuildStats enforces
					// (len(tasks) below); PartialPrefix is stamped from the
					// same helper the consumer specs use, so emit and
					// consume agree by construction.
					StagePartials: actualTasks,
					PartialPrefix: dynamicFilterPartialPrefix(queryID, stage.ID),
				}
				if e.AtOutput {
					outputEmits = append(outputEmits, em)
				} else {
					scanEmits = append(scanEmits, em)
				}
			}
			scanOp.DynamicFilterEmits = scanEmits
		}
		// Probe-side: coordinator-merged stats from the upstream build-scan.
		// dynamicFilterSpecsFromBuildStats walks ConsumeDynamicFilters and
		// pulls the matching BuildStats out of inputs[SourceStageID].
		if len(consumeSpecs) > 0 {
			scanOp.DynamicFilters = consumeSpecs
		}
		t.Operators = append(t.Operators, scanOp)
		if len(stage.FilterExprs) > 0 {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type:       distributed.OpFilter,
				Predicates: append([]string(nil), stage.FilterExprs...),
				// This filter reads the SCAN's output, whose schema is the
				// catalog's, so a predicate column the batch does not carry
				// names nothing — see OpSpec.ScanSchemaFilter and #653.
				ScanSchemaFilter: true,
			})
		}
		// The ABAC security barrier projects here (masks applied, denied
		// columns dropped). The filter ABOVE it ran first and deliberately:
		// that slot carries the POLICY's own row filter, which reads the row
		// as stored — PostgreSQL's RLS ordering, and the same order the
		// single-process pipeline uses.
		if op, ok := projectOpFromSpecs(stage.SecurityProjectExprs); ok {
			t.Operators = append(t.Operators, op)
			// Then the predicates the USER wrote. They must see the mask, not
			// the stored column: before #859 round 2 they shared the slot
			// above and `WHERE bal > (SELECT MIN(bal) FROM t)` over a masked
			// `bal` returned exactly the rows whose hidden value was positive.
			if len(stage.PostSecurityFilterExprs) > 0 {
				t.Operators = append(t.Operators, distributed.OpSpec{
					Type:       distributed.OpFilter,
					Predicates: append([]string(nil), stage.PostSecurityFilterExprs...),
				})
			}
		}
		if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
			t.Operators = append(t.Operators, op)
		}
		// Scan-output projection (pruneScanOutputColumns): read the full
		// Columns set (pushed filters need it), ship only what consumers
		// declared. Placed after filters/projections, before the sink, so
		// the whole pipeline sees the full schema and only the payload
		// narrows.
		if len(stage.OutputColumns) > 0 {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type:          distributed.OpColumnPrune,
				OutputColumns: append([]string(nil), stage.OutputColumns...),
			})
		}
		if fuseShuffle {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type:               distributed.OpExchangeSender,
				ShuffleKeys:        append([]string(nil), stage.Exchange.Keys...),
				ShuffleKeyTypes:    wireKeyTypes(stage.Exchange.KeyTypes),
				NumPartitions:      stage.Exchange.Count,
				DynamicFilterEmits: outputEmits,
			})
		} else {
			t.Operators = append(t.Operators, distributed.OpSpec{
				Type:               distributed.OpUnpartitionedSink,
				DynamicFilterEmits: outputEmits,
			})
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

	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: len(tasks)},
	}
	c.tracker.RegisterInternal(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)

	// Per-task admission estimate: the catalog-true scan bytes split
	// across tasks (files are split evenly; row-group shards each read
	// ~1/N of the file).
	perTaskEst := int64(0)
	if stage.EstimatedBytes > 0 {
		perTaskEst = stage.EstimatedBytes / int64(len(tasks))
	}
	for i := range tasks {
		tasks[i].QueryID = stageQueryID
		tasks[i].Attempt = 1
		tasks[i].EstimatedBytes = perTaskEst
	}
	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, len(tasks))
	// Register with the per-query progress bridge so worker-emitted
	// TaskProgress messages also count as forward progress for this
	// stage's idle detection.
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Scan-filter tasks sink to S3 (unpartitioned or fused-shuffle
	// partition files), never gather-fused — retry is always safe.
	// Dynamic-filter partials are collected from the successful attempt's
	// notification, same as result files.
	retrier := newTaskRetrier(tasks, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("scan-filter task retry publish failed",
				"stage_id", stage.ID, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stage.ID, c.classifyFatalResult)
	defer c.watchStuckTasks(ctx, retrier)()
	sub, err := c.subscribeTaskResults(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if uerr := distributed.Unmarshal(msg.Data, &r); uerr != nil {
			return
		}
		c.noteTaskResult(r)
		allDone := retrier.Observe(r)
		select {
		case progress <- struct{}{}:
		default:
		}
		if allDone {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s subscribe: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("scan-filter stage %s publish: %w", stage.ID, err)
	}
	if err := awaitStageProgress(ctx, done, progress, "scan-filter "+stage.ID); err != nil {
		return StageOutput{}, err
	}

	if f, failed := retrier.FirstError(); failed {
		return StageOutput{}, stageTaskFailure(f, fmt.Errorf("scan-filter stage %s: task %s failed after %d attempts: %s", stage.ID, f.TaskID, maxTaskAttempts, f.Message))
	}
	resultFiles := retrier.Files()

	// Union per-task dynamic-filter partials into stage-level BuildStats.
	// Skipped if the stage didn't emit (no partials expected) — saves the
	// S3 round-trip for the common non-emit case. Completeness-enforced:
	// a filter missing any task's partial is withheld (an incomplete bloom
	// falsely rejects rows downstream).
	var buildStats map[string]*BuildStats
	if len(stage.EmitDynamicFilters) > 0 {
		merged := c.mergeCompleteBuildStats(ctx, queryID, stage.ID, retrier.DynamicFilterPartials(), len(tasks), lateAttachFilterIDs(stage.EmitDynamicFilters))
		buildStats = merged
	}

	if fuseShuffle {
		// Fused scan+shuffle: each task produced N partition files at
		// "<prefix>partition=NNNN/<task>.wshf". Bucket all files across
		// all tasks by partition number so the downstream consumer reads
		// each partition's full set. Same shape as runShuffleSide.
		numParts := stage.Exchange.Count
		shardFiles := make([][]string, numParts)
		for _, taskFiles := range resultFiles {
			for _, f := range taskFiles {
				p, parseErr := parsePartitionFromPath(f)
				if parseErr != nil {
					return StageOutput{}, fmt.Errorf("scan-filter stage %s: parsing partition from %q: %w", stage.ID, f, parseErr)
				}
				if p < 0 || p >= numParts {
					return StageOutput{}, fmt.Errorf("scan-filter stage %s: partition %d out of range [0,%d) in %q", stage.ID, p, numParts, f)
				}
				shardFiles[p] = append(shardFiles[p], f)
			}
		}
		out := StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         shardFiles,
			BuildStats:    buildStats,
			Bytes:         retrier.TotalBytes(),
		}
		out.PartitionRows, out.PartitionBytes = retrier.PartitionAccounting(numParts)
		return out, nil
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: len(tasks),
		Files:         resultFiles,
		BuildStats:    buildStats,
		Bytes:         retrier.TotalBytes(),
	}, nil
}

// stageHasLaneEmit reports whether any of the stage's dynamic-filter
// emits belongs on the priority lane. In-flow emits (planner-marked
// cascade mids riding normal scheduling) don't count; a stage with only
// in-flow emits dispatches as ordinary bulk work.
func stageHasLaneEmit(stage physical.Stage) bool {
	for _, e := range stage.EmitDynamicFilters {
		if !e.InFlow {
			return true
		}
	}
	return false
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
