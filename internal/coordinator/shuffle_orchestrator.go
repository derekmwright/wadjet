package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
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

// shuffleStageTimeout is the per-shuffle-stage timeout. Phase 1: no per-stage retry.
const shuffleStageTimeout = 10 * time.Minute

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

// orchestrateShuffleStages runs both shuffle stages (build side and probe side)
// in parallel and returns the resulting shard layout. The caller is responsible
// for dispatching the downstream probe pipeline tasks built from this layout.
func (c *Coordinator) orchestrateShuffleStages(
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
		shards, err := c.runShuffleSide(gctx, queryID, "build", buildStage, cand.BuildKeys, numParts, workerCount)
		if err != nil {
			return fmt.Errorf("build-side shuffle for %s: %w", cand.BuildAlias, err)
		}
		buildShards = shards
		return nil
	})

	g.Go(func() error {
		shards, err := c.runShuffleSide(gctx, queryID, "probe", probeStage, cand.ProbeKeys, numParts, workerCount)
		if err != nil {
			return fmt.Errorf("probe-side shuffle for %s: %w", cand.ProbeAlias, err)
		}
		probeShards = shards
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("orchestrateShuffleStages: %w", err)
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
) ([][]string, error) {
	ctx, cancel := context.WithTimeout(ctx, shuffleStageTimeout)
	defer cancel()

	resultPrefix := fmt.Sprintf("queries/%s/shuffle/%s", parentQueryID, sideName)
	shuffleQueryID := fmt.Sprintf("sh-%s-%s", sideName, parentQueryID)
	stageID := fmt.Sprintf("shuffle-%s", sideName)

	// Determine column projection. Use prunedScanColumns to avoid writing
	// columns that belong to sibling tables in the optimizer's over-approximated
	// stage.Columns. If pruning fails (catalog miss or empty columns), fall back
	// to nil which causes the worker to select all columns — correct but larger.
	cols := c.prunedScanColumns(ctx, sourceStage)
	// Note: intentionally accepting nil here (SELECT * fallback) rather than
	// returning an error for a catalog miss during shuffle setup.

	// Split source files across workers. splitFilesEvenly handles the case where
	// there are fewer files than workers by capping n at len(files), so
	// actualTasks ≤ workerCount.
	fileSets := splitFilesEvenly(sourceStage.ScanFiles, workerCount)
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

	// Subscribe for results before publishing to avoid the race.
	subject := distributed.QueryResultSubject(shuffleQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	collected := make([]taskResult, 0, actualTasks)
	var mu sync.Mutex
	done := make(chan struct{}, 1)

	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if unmarshalErr := distributed.Unmarshal(msg.Data, &r); unmarshalErr != nil {
			return
		}
		mu.Lock()
		if !r.Success {
			collected = append(collected, taskResult{err: r.Error})
		} else {
			collected = append(collected, taskResult{files: r.ResultFiles})
		}
		got := len(collected)
		mu.Unlock()
		if got >= actualTasks {
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

	// Build and publish all tasks at once.
	tasks := make([]distributed.Task, actualTasks)
	for i, files := range fileSets {
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      shuffleQueryID,
			StageID:      stageID,
			Type:         distributed.TaskTypeShuffle,
			TableName:    sourceStage.TableName,
			Files:        files,
			Columns:      cols,
			ShuffleKeys:  keys,
			NumPartitions: numParts,
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			CreatedAt:    time.Now(),
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		tasks[i] = t
	}

	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return nil, fmt.Errorf("publishing shuffle-%s tasks: %w", sideName, err)
	}

	// Wait for all tasks to complete.
	select {
	case <-done:
	case <-ctx.Done():
		return nil, fmt.Errorf("shuffle-%s timed out after %s: %w", sideName, shuffleStageTimeout, ctx.Err())
	}

	// Check for any task failures.
	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()

	for _, r := range results {
		if r.err != "" {
			return nil, fmt.Errorf("shuffle-%s task failed: %s", sideName, r.err)
		}
	}

	// Bucket result files by partition. Each file has a path like:
	// <prefix>/partition=NNNN/<task-id>.wshf
	// We parse the partition number from the "partition=NNNN" path segment.
	// This format is fixed by the worker's executeShuffle (partitionedShuffleSink).
	shardFiles := make([][]string, numParts)
	for _, r := range results {
		for _, f := range r.files {
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

