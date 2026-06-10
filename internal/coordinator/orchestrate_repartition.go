package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// shuffleBuildThreshold gates the routing decision: if the largest build
// table's EstimatedBytes exceeds this, route to shuffle-distributed instead
// of probe-split. Declared as var so tests can lower it.
var shuffleBuildThreshold int64 = 4 * 1024 * 1024 * 1024 // 4 GB

// shufflePartitionMultiplier sets numPartitions = workerCount × this.
// 4 reduces hot-key skew impact while keeping file count bounded.
const shufflePartitionMultiplier = 4

// shuffleStageTimeout is the absolute upper bound a stage is allowed to run.
// It's intentionally generous because the actual stuck-stage detection is
// progress-based (see awaitStageProgress in execute_stage_dag.go): a stage is
// considered stuck only when no task has reported a result in
// stageIdleTimeout. The wall-clock cap here is the belt-and-suspenders for
// pathological cases where progress signals leak (lost subscription, NATS
// partition) and we still need to bail out.
const shuffleStageTimeout = 60 * time.Minute

// stageIdleTimeout is how long a stage may go without observing any task
// completion before it's declared stuck. Replaces the old fixed-wall-clock
// shuffleStageTimeout in the dispatch+collect loops: a stage that's slow but
// reporting completions every few minutes is fine; one that has gone silent
// for stageIdleTimeout is broken (worker crashed, deadlock, lost result
// publish).
//
// 30 min is sized for SF10 shuffle stages where each task uploads many
// partitions sequentially over a contended network. Observed wall time
// per task: 10–20 min for lineitem shuffle output. The previous 10 min
// fired before any task could complete on legitimately-long shuffles
// and surfaced as confusing "S3 PUT context deadline exceeded" errors —
// what actually happened was coord cancelling the query out from under
// the in-flight upload. shuffleStageTimeout (60 min absolute) still
// catches pathological cases where progress signals leak entirely.
const stageIdleTimeout = 30 * time.Minute

// ShuffleLayout describes the shard file layout produced by the two-sided
// shuffle stages. The caller (coordinator routing path) constructs probe
// pipeline tasks from this layout by assigning contiguous partition slices to
// each worker and populating PreScannedInputs with the corresponding shard files.
type ShuffleLayout struct {
	BuildAlias    string
	ProbeAlias    string
	NumPartitions int
	// BuildShardFiles[p] contains the S3 keys for build partition p.
	// May be nil/empty for a partition if no build rows hashed there.
	BuildShardFiles [][]string
	// ProbeShardFiles[p] contains the S3 keys for probe partition p.
	ProbeShardFiles [][]string
}

// orchestrateRepartition runs both shuffle stages (build side and probe side)
// in parallel and returns the resulting shard layout. The caller is responsible
// for dispatching the downstream probe pipeline tasks built from this layout.
func (c *Coordinator) orchestrateRepartition(
	ctx context.Context,
	queryID string,
	cand physical.ShuffleCandidate,
	stages []physical.Stage,
	workerCount int,
) (*ShuffleLayout, error) {
	numParts := workerCount * shufflePartitionMultiplier

	// Locate the scan stages for both sides.
	buildStage, probeStage, err := findShuffleScanStages(stages, cand)
	if err != nil {
		return nil, err
	}

	c.logger.Info("shuffle orchestrator: starting two-sided shuffle",
		"query_id", queryID,
		"build_alias", cand.BuildAlias,
		"probe_alias", cand.ProbeAlias,
		"num_partitions", numParts,
		"worker_count", workerCount,
		"build_bytes", cand.BuildBytes,
	)

	// Run build and probe shuffles in parallel. Failure of either cancels both.
	var buildShards, probeShards [][]string
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		shards, err := c.runShuffleSide(gctx, queryID, "build", buildStage, cand.BuildKeys, numParts, workerCount, nil)
		if err != nil {
			return fmt.Errorf("build-side shuffle for %s: %w", cand.BuildAlias, err)
		}
		buildShards = shards
		return nil
	})

	g.Go(func() error {
		shards, err := c.runShuffleSide(gctx, queryID, "probe", probeStage, cand.ProbeKeys, numParts, workerCount, nil)
		if err != nil {
			return fmt.Errorf("probe-side shuffle for %s: %w", cand.ProbeAlias, err)
		}
		probeShards = shards
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("orchestrateRepartition: %w", err)
	}

	c.logger.Info("shuffle orchestrator: both sides complete",
		"query_id", queryID,
		"num_partitions", numParts,
	)

	return &ShuffleLayout{
		BuildAlias:      cand.BuildAlias,
		ProbeAlias:      cand.ProbeAlias,
		NumPartitions:   numParts,
		BuildShardFiles: buildShards,
		ProbeShardFiles: probeShards,
	}, nil
}

// runShuffleSide dispatches workerCount TaskTypeShuffle tasks for one side
// (build or probe), each reading its file slice and hash-partitioning into
// numParts outputs. Waits for all tasks. Returns shardFiles[p] = slice of
// S3 keys for partition p (concatenated from each task's per-partition output).
func (c *Coordinator) runShuffleSide(
	ctx context.Context,
	parentQueryID string,
	sideName string, // "build" or "probe" — used in stage IDs and S3 prefix
	sourceStage physical.Stage,
	keys []string,
	numParts int,
	workerCount int,
	dynamicFilters []distributed.DynamicFilterSpec, // resolved bloom/range pushdowns from upstream build stat
) ([][]string, error) {
	ctx, cancel := context.WithTimeout(ctx, shuffleStageTimeout)
	defer cancel()

	// Convention A: resultPrefix always ends with "/" so that the worker's
	// "%spartition=%04d/%s.wshf" format string produces a valid S3 key with
	// the segment "partition=NNNN" cleanly separated. Never omit the slash.
	resultPrefix := fmt.Sprintf("queries/%s/shuffle/%s/", parentQueryID, sideName)
	shuffleQueryID := fmt.Sprintf("sh-%s-%s", sideName, parentQueryID)
	stageID := fmt.Sprintf("shuffle-%s", sideName)

	// Determine column projection. Use prunedScanColumns to avoid writing
	// columns that belong to sibling tables in the optimizer's over-approximated
	// stage.Columns. If pruning fails (catalog miss or empty columns), fall back
	// to nil which causes the worker to select all columns — correct but larger.
	cols := c.prunedScanColumns(ctx, sourceStage)
	// Note: intentionally accepting nil here (SELECT * fallback) rather than
	// returning an error for a catalog miss during shuffle setup.

	// Fan out shuffle tasks across the cluster's effective concurrency capacity
	// (sum of each worker's auto-tuned MaxConcurrent reported in heartbeats),
	// not just worker count. With 3 workers × MaxConcurrent=3 the cluster can
	// run 9 shuffle tasks at once; sizing to 3 leaves 6 task slots idle and
	// pegs each worker to a single fragment-pipeline. 2026-05-22 SF100 Q18
	// stage timeline showed exchange-repartition-15 wall = 3m09s with only
	// 3 tasks (workerCount), while workers had MaxConcurrent=3 → 6 task slots
	// sat idle for the whole stage. This mirrors scanFanOutTaskCount's policy
	// (used by dispatchScanAggregateStage et al) — see execute_stage_dag.go.
	capacity := c.workers.ClusterCapacity()
	taskCount := scanFanOutTaskCount(workerCount, capacity, len(sourceStage.ScanFiles))
	fileSets := splitFilesEvenly(sourceStage.ScanFiles, taskCount)
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		// No files — nothing to shuffle. Return empty partition layout.
		return make([][]string, numParts), nil
	}

	// Register an ephemeral tracker entry for this shuffle stage.
	trackerStages := map[string]*StageInfo{
		stageID: {
			StageID:    stageID,
			Type:       distributed.TaskTypeShuffle,
			TotalTasks: actualTasks,
		},
	}
	c.tracker.Register(shuffleQueryID, "", trackerStages, []string{stageID})
	c.tracker.Start(shuffleQueryID)
	defer c.tracker.Delete(shuffleQueryID)

	// Build tasks first (the retrier needs them registered), then subscribe
	// BEFORE publishing to avoid the result race.
	subject := distributed.QueryResultSubject(shuffleQueryID)
	done := make(chan struct{}, 1)

	tasks := make([]distributed.Task, actualTasks)
	for i, files := range fileSets {
		t := distributed.Task{
			ID:             uuid.New().String()[:8],
			QueryID:        shuffleQueryID,
			StageID:        stageID,
			Type:           distributed.TaskTypeShuffle,
			TableName:      sourceStage.TableName,
			Files:          files,
			Columns:        cols,
			ShuffleKeys:    keys,
			NumPartitions:  numParts,
			DataBucket:     c.config.ResultBucket,
			ResultBucket:   c.config.ResultBucket,
			ResultPrefix:   resultPrefix,
			CreatedAt:      time.Now(),
			DynamicFilters: dynamicFilters,
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		t.Attempt = 1
		tasks[i] = t
	}

	// Shuffle tasks are never gather-fused (their outputs are partition
	// files), so retry is always safe here: deterministic inputs, and a
	// retry overwrites the failed attempt's identically-named, identically-
	// contented partition files.
	retrier := newTaskRetrier(tasks, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("shuffle task retry publish failed",
				"side", sideName, "task_id", t.ID, "error", pubErr)
		}
	}, c.logger, stageID)
	defer c.watchStuckTasks(ctx, retrier)()

	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if unmarshalErr := distributed.Unmarshal(msg.Data, &r); unmarshalErr != nil {
			return
		}
		c.workers.MarkWorkerSeen(r.WorkerID)
		if c.workers.Liveness != nil {
			c.workers.Liveness.Remove(r.TaskID)
		}
		if retrier.Observe(r) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing for shuffle-%s results: %w", sideName, err)
	}
	defer sub.Unsubscribe()

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return nil, fmt.Errorf("publishing shuffle-%s tasks: %w", sideName, err)
	}

	// Wait for all tasks to complete.
	select {
	case <-done:
	case <-ctx.Done():
		return nil, fmt.Errorf("shuffle-%s timed out after %s: %w", sideName, shuffleStageTimeout, ctx.Err())
	}

	// Check for any task failures (terminal: retries exhausted).
	if taskID, errMsg, failed := retrier.FirstError(); failed {
		return nil, fmt.Errorf("shuffle-%s task %s failed after %d attempts: %s", sideName, taskID, maxTaskAttempts, errMsg)
	}

	// Bucket result files by partition. Each file has a path like:
	// <prefix>/partition=NNNN/<task-id>.wshf
	// We parse the partition number from the "partition=NNNN" path segment.
	// This format is fixed by the worker's executeShuffle (partitionedShuffleSink).
	shardFiles := make([][]string, numParts)
	for _, taskFiles := range retrier.Files() {
		for _, f := range taskFiles {
			p, parseErr := parsePartitionFromPath(f)
			if parseErr != nil {
				return nil, fmt.Errorf("shuffle-%s: parsing partition from %q: %w", sideName, f, parseErr)
			}
			if p < 0 || p >= numParts {
				return nil, fmt.Errorf("shuffle-%s: partition %d out of range [0,%d) in path %q", sideName, p, numParts, f)
			}
			shardFiles[p] = append(shardFiles[p], f)
		}
	}

	// Enumerate any shard files that were written to S3 but not reported in
	// ResultFiles (e.g., workers that produced zero rows for some partitions
	// may omit them — this is fine; nil slice for a partition is correct).
	// If the worker was thorough with ResultFiles we don't need List. Doing
	// a List pass here as a cross-check is safe but adds latency, so we skip
	// it. If a future bug shows missing shards, add a List-based reconciliation
	// here keyed on resultPrefix.

	c.logger.Info("shuffle side complete",
		"query_id", parentQueryID,
		"side", sideName,
		"tasks", actualTasks,
		"num_partitions", numParts,
	)

	return shardFiles, nil
}

// parsePartitionFromPath extracts the partition index from an S3 key that
// contains a segment of the form "partition=NNNN". Returns an error if no
// such segment is found or if NNNN is not a valid non-negative integer.
func parsePartitionFromPath(path string) (int, error) {
	const prefix = "partition="
	for _, seg := range strings.Split(path, "/") {
		if strings.HasPrefix(seg, prefix) {
			numStr := seg[len(prefix):]
			n, err := strconv.Atoi(numStr)
			if err != nil {
				return 0, fmt.Errorf("segment %q: %w", seg, err)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("no %q segment found", prefix)
}

// assignPartitionsToWorker returns the contiguous partition IDs assigned to
// worker w (zero-indexed) when there are workerCount workers and numParts
// total partitions. numParts must be divisible by workerCount; partsPerWorker
// = numParts / workerCount.
func assignPartitionsToWorker(w, workerCount, numParts int) []int {
	if workerCount <= 0 || numParts <= 0 || w < 0 || w >= workerCount {
		return nil
	}
	partsPerWorker := numParts / workerCount
	if partsPerWorker == 0 {
		// More workers than partitions — worker w gets partition w if w < numParts.
		if w < numParts {
			return []int{w}
		}
		return nil
	}
	start := w * partsPerWorker
	parts := make([]int, partsPerWorker)
	for i := range parts {
		parts[i] = start + i
	}
	return parts
}

// buildShufflePipelineTasks creates one probe pipeline task per worker.
// Each task gets PreScannedInputs populated with the shard files for the
// partitions assigned to that worker (contiguous slice of layout's partitions).
func buildShufflePipelineTasks(
	queryID, sql, resultBucket string,
	layout *ShuffleLayout,
	workerCount int,
) []distributed.Task {
	resultPrefix := fmt.Sprintf("queries/%s/pipeline-0/", queryID)
	tasks := make([]distributed.Task, 0, workerCount)
	for w := 0; w < workerCount; w++ {
		parts := assignPartitionsToWorker(w, workerCount, layout.NumPartitions)
		if len(parts) == 0 {
			continue
		}
		var buildFiles, probeFiles []string
		for _, p := range parts {
			buildFiles = append(buildFiles, layout.BuildShardFiles[p]...)
			probeFiles = append(probeFiles, layout.ProbeShardFiles[p]...)
		}
		tasks = append(tasks, distributed.Task{
			ID:               uuid.New().String()[:8],
			QueryID:          queryID,
			StageID:          "pipeline-0",
			Type:             distributed.TaskTypePipeline,
			SQLText:          sql,
			DataBucket:       resultBucket,
			ResultBucket:     resultBucket,
			ResultPrefix:     resultPrefix,
			PartialAggregate: true,
			PreScannedInputs: map[string][]string{
				layout.BuildAlias: buildFiles,
				layout.ProbeAlias: probeFiles,
			},
			CreatedAt: time.Now(),
		})
	}
	return tasks
}

// findShuffleScanStages locates the build and probe scan stages corresponding
// to the ShuffleCandidate's aliases.
func findShuffleScanStages(stages []physical.Stage, cand physical.ShuffleCandidate) (build, probe physical.Stage, err error) {
	var buildFound, probeFound bool
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		if s.ScanAlias == cand.BuildAlias {
			build = s
			buildFound = true
		}
		if s.ScanAlias == cand.ProbeAlias {
			probe = s
			probeFound = true
		}
	}
	if !buildFound {
		return physical.Stage{}, physical.Stage{}, fmt.Errorf("build scan stage for alias %q not found", cand.BuildAlias)
	}
	if !probeFound {
		return physical.Stage{}, physical.Stage{}, fmt.Errorf("probe scan stage for alias %q not found", cand.ProbeAlias)
	}
	return build, probe, nil
}
