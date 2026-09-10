// This file holds shuffle and replication dispatch, projections, and materialization.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

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
	// Synthesize a source stage for runShuffleSide. EstimatedBytes carries
	// the upstream's worker-reported output size so the shuffle tasks get
	// per-task admission estimates (runShuffleSide splits it per task).
	// ScanTable/ScanColumns ride along when the upstream is a pass-through
	// leaf scan, so runShuffleSide's prunedScanColumns can narrow the
	// shuffle tasks' parquet reads — otherwise scan-absorbed legs shuffle
	// every column of the base table (memo §2 A2). Chained shuffles leave
	// both empty: WSHF inputs ignore column projection.
	synthTable := stage.TableName
	if synthTable == "" {
		synthTable = upstream.ScanTable
	}
	// ScanSchema follows synthTable: whichever stage owns the relation owns
	// the catalog's declared types for it, and the shuffle tasks need them
	// to read a pre-v0.18.0 file as anything but raw storage form (#423).
	synthSchema := stage.ScanSchema
	if len(synthSchema) == 0 {
		synthSchema = upstream.ScanSchema
	}
	synthetic := physical.Stage{
		ID:             stage.ID + "-src",
		Type:           physical.StageScan,
		ScanFiles:      sourceFiles,
		TableName:      synthTable,
		Columns:        upstream.ScanColumns,
		ScanSchema:     synthSchema,
		EstimatedBytes: upstream.Bytes,
	}
	if len(upstream.ScanFileSizes) == len(sourceFiles) {
		synthetic.ScanFileSizes = upstream.ScanFileSizes
	}
	// WSHF-input shuffle projection (fix 2 of the scan-output column leak):
	// stage outputs get no read-time narrowing, so without this a shuffle
	// over a wide upstream re-materializes every decoded column. Ship the
	// stage's declared Columns ∪ Keys as the task projection; the worker
	// prunes post-decode with intersection semantics. OutputColumns is the
	// carrier — runShuffleSide falls back to it when catalog-based pruning
	// yields nothing. Kill switch WADJET_WSHF_SHUFFLE_PRUNE=0.
	synthetic.OutputColumns = wshfShuffleProjection(stage, sourceFiles[0], synthetic.Columns)
	numParts := stage.Exchange.Count
	if numParts == 0 {
		numParts = workerCount * shufflePartitionMultiplier
	}
	// Forward any dynamic-filter specs the upstream (probe-side) leaf scan
	// resolved through pass-through. Each shuffle task gets a copy so its
	// parquet scan applies the bloom at row-group level.
	//
	// Eager dispatch: hand runShuffleSide this stage's feed so eligible
	// consumers can clear their barrier once the shuffle tasks are built.
	// nil when the flag is off. The empty-source early return above never
	// dispatches the feed — consumers just fall through on done[dep].
	var computedCols []distributed.ComputedColSpec
	for _, cc := range stage.Exchange.ComputedCols {
		computedCols = append(computedCols, distributed.ComputedColSpec{Name: cc.Name, Expr: cc.Expr})
	}
	// Widen the parquet read with the flag expressions' input columns; the
	// worker drops them from the payload after computing the flags.
	if len(stage.Exchange.ExtraReadCols) > 0 && len(synthetic.Columns) > 0 {
		synthetic.Columns = append(append([]string(nil), synthetic.Columns...), stage.Exchange.ExtraReadCols...)
	}
	// Sender-side partial aggregation (markExchangePartialAgg): thread the
	// planner-proven keys/specs into the shuffle tasks. Nil when unmarked.
	var partialAggKeys []string
	var partialAggSpecs []distributed.AggSpec
	if len(stage.Exchange.PartialAggSpecs) > 0 {
		partialAggKeys = stage.Exchange.PartialAggGroupBy
		for _, sp := range stage.Exchange.PartialAggSpecs {
			partialAggSpecs = append(partialAggSpecs, distributed.AggSpec{
				Func: sp.Func, InputCol: sp.InputCol, OutputCol: sp.OutputCol,
			})
		}
	}
	shards, shardStats, err := c.runShuffleSide(ctx, queryID, "stage-"+stage.ID, synthetic, stage.Exchange.Keys, stage.Exchange.KeyTypes, numParts, workerCount, upstream.DynamicFilters, computedCols, stage.Exchange.ExtraReadCols, partialAggKeys, partialAggSpecs, c.eagerFeedHandle(queryID, stage.ID))
	if err != nil {
		return StageOutput{}, err
	}
	return StageOutput{
		Kind:           OutputPartitioned,
		NumPartitions:  numParts,
		Files:          shards,
		Bytes:          shardStats.TotalBytes,
		PartitionRows:  shardStats.PartitionRows,
		PartitionBytes: shardStats.PartitionBytes,
	}, nil
}

// wshfShuffleProjection returns the task-level column projection for a
// repartition stage whose input is upstream STAGE OUTPUT (.wshf), or nil
// when ineligible. Scan pass-through legs (parquet inputs) keep the
// existing prunedScanColumns path — syntheticCols non-empty means that
// path owns projection. Eligibility mirrors pruneScanOutputColumns's
// caution: the stage must declare Columns and carry no computed-column
// machinery (ComputedCols expressions read inputs the declaration may
// omit; ExtraReadCols is a parquet-read widening that has no WSHF
// analogue). Exchange keys are unioned in defensively — the worker's
// partitioning sink hashes them.
//
// The declared set is over-approximated by planner convention (both join
// sides get the same union; "the reader ignores columns that don't
// exist"), so the worker applies it with intersection semantics against
// the decoded schema (wshfShufflePruneKeep) — never as a strict schema.
func wshfShuffleProjection(stage physical.Stage, firstSourceFile string, syntheticCols []string) []string {
	if !wshfShufflePrune {
		return nil
	}
	if len(syntheticCols) > 0 || len(stage.Columns) == 0 || stage.Exchange == nil {
		return nil
	}
	if len(stage.Exchange.ComputedCols) > 0 || len(stage.Exchange.ExtraReadCols) > 0 {
		return nil
	}
	if !strings.HasSuffix(firstSourceFile, ".wshf") {
		return nil
	}
	keep := append([]string(nil), stage.Columns...)
	seen := make(map[string]bool, len(keep)+len(stage.Exchange.Keys))
	for _, c := range keep {
		seen[c] = true
	}
	for _, k := range stage.Exchange.Keys {
		if !seen[k] {
			keep = append(keep, k)
			seen[k] = true
		}
	}
	return keep
}

// allWSHF reports whether every file in the list has the .wshf suffix.
// Native-DAG upstreams ALL write WSHF — the only path that would feed
// raw parquet into a replicate is a (legacy) plan that skipped the
// intermediate scan-filter stage, which the current planner never
// emits. When all upstreams are WSHF, the consolidation that
// materializeReplicate provides has no CPU payoff (mmap-walk a single
// 154 MB cache vs mmap-walk N WSHF chunks is essentially identical
// I/O cost), yet the serial single-task materialize burns wall time
// — 2 m 19 s on Q21 SF100 c9716f7 just to consolidate 5 WSHF files
// totalling ~150 MB (project_streaming_shuffle_sf100_win_2026-05-22
// follow-up profile analysis). Returns false for an empty list to
// preserve the existing "pass through" semantics — len() == 0 hits
// other guards.
func allWSHF(files []string) bool {
	if len(files) == 0 {
		return false
	}
	for _, f := range files {
		if !strings.HasSuffix(f, ".wshf") {
			return false
		}
	}
	return true
}

// dispatchReplicateStage executes a StageExchangeReplicate: reads upstream
// output and materializes it into a single broadcast cache that all
// downstream workers read in full.
//
// When the upstream has fewer than replicateMaterializeMinFiles files
// (or is already OutputReplicated, i.e. broadcast-shaped), we pass the
// file list through unchanged — every downstream task still reads the
// full set, but the consolidation overhead would not pay back. When the
// upstream is multi-file OutputSinglePart / OutputPartitioned, we spawn
// one scan task that reads every upstream file and writes a single WSHF
// cache. Downstream broadcast_join probe-split tasks all read that one
// cache instead of re-decoding the upstream parquet N times.
func (c *Coordinator) dispatchReplicateStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
) (StageOutput, error) {
	_ = sql
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("replicate stage %s expects 1 dep, got %d", stage.ID, len(stage.Dependencies))
	}
	upstream := inputs[stage.Dependencies[0]]
	upstreamFiles := flattenStageFiles(upstream)

	if upstream.Kind == OutputReplicated || len(upstreamFiles) < replicateMaterializeMinFiles || allWSHF(upstreamFiles) {
		// allWSHF bypass: native-DAG upstreams write WSHF, mmap-readable
		// downstream with no per-file decode cost. Materialize provides no
		// CPU savings here and adds a 2 m+ serial-task wall on Q21 SF100.
		// Pass through; downstream probe-split tasks each open all N
		// upstream files directly via cachedFileStreamSource (mmap, cheap).
		return replicatePassThrough(upstream, upstreamFiles), nil
	}

	cacheFiles, cacheBytes, err := replicateMaterialize(c, ctx, queryID, stage, stage.Dependencies[0], upstreamFiles, upstream)
	if err != nil {
		// Materialization failure must not break the query — fall back to
		// pass-through. Workers can still read the raw upstream files; we
		// just lose the consolidation benefit. Logged at Warn so an unhealthy
		// cluster is visible.
		//
		// The SAME pass-through as the bypass above, annotations included:
		// this branch runs over an upstream that is BY CONSTRUCTION
		// multi-file base parquet (the bypass took every other case), which
		// is exactly the input the #503 guard refuses to type from its own
		// footers. Handing it over bare turned "we lost the consolidation
		// benefit" into "the query fails".
		c.logger.Warn("dispatchReplicateStage: materialization failed, falling back to pass-through",
			"stage_id", stage.ID, "upstream_files", len(upstreamFiles), "err", err)
		return replicatePassThrough(upstream, upstreamFiles), nil
	}

	return StageOutput{
		Kind:  OutputReplicated,
		Files: [][]string{cacheFiles},
		Bytes: cacheBytes,
	}, nil
}

// replicatePassThrough is the replicate stage's output when the upstream's
// files are handed to every downstream reader as they are — the bypass above,
// and the fallback when materialization fails.
//
// It carries the upstream's SCAN ANNOTATIONS because a pass-through can be a
// pass-through of a pass-through: when the upstream was itself a leaf scan
// handed over as raw parquet, the CONSUMER of this replicate reads base-table
// files, and the catalog's types have to survive the hop. Dropping them left
// a broadcast join's build side typing an IPv4 column from a file that cannot
// say it is one (#423), and a file whose stored type contradicts the catalog
// decoding silently (#503) — and once #503's guard landed, it left the build
// side with an empty BuildColumnTypes and the whole query REFUSED. The fields
// are empty on a WSHF upstream, which is what they already mean.
//
// One function rather than two constructions, because two is how the fallback
// came to differ from the bypass in the first place.
func replicatePassThrough(upstream StageOutput, files []string) StageOutput {
	return StageOutput{
		Kind:          OutputReplicated,
		Files:         [][]string{files},
		Bytes:         upstream.Bytes,
		ScanTable:     upstream.ScanTable,
		ScanColumns:   append([]string(nil), upstream.ScanColumns...),
		ScanSchema:    upstream.ScanSchema,
		ScanFileSizes: append([]int64(nil), upstream.ScanFileSizes...),
	}
}

// materializeReplicate dispatches a single TaskTypeStage scan task that reads
// the upstream files and writes a consolidated WSHF that downstream readers
// can share. Returns the cache file paths (typically a single .wshf key).
//
// The consolidation task uses the same scan-stage handler as a leaf scan;
// it streams batches from upstream into writeUnpartitionedWSHF without
// running any operator. cachedFileStreamSource auto-detects parquet vs WSHF
// inputs, so the upstream's file kind is transparent.
func (c *Coordinator) materializeReplicate(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	upstreamID string,
	files []string,
	upstream StageOutput, // the task reads all of it; Bytes is the estimate
) ([]string, int64, error) {
	estBytes := upstream.Bytes
	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	task := distributed.Task{
		ID:             uuid.New().String()[:8],
		QueryID:        queryID,
		StageID:        stage.ID,
		Type:           distributed.TaskTypeStage,
		DataBucket:     c.config.ResultBucket,
		ResultBucket:   c.config.ResultBucket,
		ResultPrefix:   resultPrefix,
		EstimatedBytes: estBytes,
		CreatedAt:      time.Now(),
		// Fragment dispatch: stream upstream files through OpScan into an
		// unpartitioned WSHF sink. cachedFileStreamSource auto-detects
		// parquet vs WSHF, so the consolidation pattern (this caller's job)
		// drops in unchanged.
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpScan,
				InputAlias:  upstreamID,
				InputFiles:  files,
				InputBucket: c.config.ResultBucket,
				// The allWSHF bypass above already returned for stage
				// output, so a list that reaches here is base-table
				// parquet — a pass-through leaf scan being consolidated
				// into one broadcast file. It needs the catalog's types
				// and the read set that goes with them, or the WSHF this
				// task writes carries raw storage form to every join task
				// downstream (#423), and a file whose stored type
				// contradicts the catalog decodes silently (#503).
				Columns:     append([]string(nil), upstream.ScanColumns...),
				ColumnTypes: wireColumnSpecs(upstream.ScanSchema),
			},
			{
				Type: distributed.OpUnpartitionedSink,
			},
		},
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}
	c.mu.Lock()
	qm := c.queryMetas[queryID]
	c.mu.Unlock()
	c.enrichTaskWithQueryContext(qm, &task)

	stageQueryID := fmt.Sprintf("st-%s-%s", stage.ID, queryID)
	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeStage, TotalTasks: 1},
	}
	c.tracker.RegisterInternal(stageQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(stageQueryID)
	defer c.tracker.Delete(stageQueryID)
	task.QueryID = stageQueryID
	task.Attempt = 1

	subject := distributed.QueryResultSubject(stageQueryID)
	done := make(chan struct{}, 1)
	progress := make(chan struct{}, 1)
	defer stageProgressBridgeFromContext(ctx).Register(stage.ID, progress)()
	// Materialize sinks to S3 (OpUnpartitionedSink) — retry-safe.
	retrier := newTaskRetrier([]distributed.Task{task}, true, func(t distributed.Task) {
		if ctx.Err() != nil {
			return
		}
		if pubErr := c.scheduler.PublishTasks(ctx, []distributed.Task{t}); pubErr != nil {
			c.logger.Error("materialize task retry publish failed",
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
		return nil, 0, fmt.Errorf("subscribe materialize: %w", err)
	}
	defer sub.Unsubscribe()

	c.logger.Info("dispatchReplicateStage: materializing",
		"stage_id", stage.ID, "upstream_files", len(files), "task_id", task.ID)
	if err := c.scheduler.PublishTasks(ctx, []distributed.Task{task}); err != nil {
		return nil, 0, fmt.Errorf("publish materialize: %w", err)
	}
	if err := awaitStageProgress(ctx, done, progress, "replicate "+stage.ID); err != nil {
		return nil, 0, err
	}
	if f, failed := retrier.FirstError(); failed {
		return nil, 0, stageTaskFailure(f, fmt.Errorf("materialize task failed after %d attempts: %s", maxTaskAttempts, f.Message))
	}
	resultFiles := retrier.Files()[0]
	if len(resultFiles) == 0 {
		return nil, 0, fmt.Errorf("materialize task returned no files")
	}
	c.logger.Info("dispatchReplicateStage: materialized",
		"stage_id", stage.ID, "input_files", len(files), "cache_files", len(resultFiles))
	return resultFiles, retrier.TotalBytes(), nil
}
