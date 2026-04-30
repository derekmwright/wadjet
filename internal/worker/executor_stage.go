package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// executeStage dispatches a TaskTypeStage task (Phase 3 native-DAG
// single-operator stage fragment). The Task.StageType string matches
// physical.Stage.Type and selects which operator builder runs.
//
// Stage fragments are self-contained: one operator, inputs read from
// upstream stage output via Task.Inputs, output written to the sink
// selected by task fields (ShuffleKeys → partitionedShuffleSink,
// ReplySubject → gatherReplySink, else unpartitioned .wshf upload).
func (e *Executor) executeStage(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	switch task.StageType {
	case "scan", "filter_scan":
		return e.executeStageScan(ctx, task, result)
	case "hash_join", "broadcast_join":
		return e.executeStageHashJoin(ctx, task, result)
	case "aggregate", "final_aggregate":
		return e.executeStageAggregate(ctx, task, result)
	case "sort", "merge_sort":
		return e.executeStageSort(ctx, task, result)
	default:
		return fmt.Errorf("executeStage: unsupported StageType %q on task %s",
			task.StageType, task.ID)
	}
}

func (e *Executor) executeStageScan(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Inputs) != 1 {
		return fmt.Errorf("scan task %s: needs exactly 1 input, got %d", task.ID, len(task.Inputs))
	}
	var alias string
	var files []string
	for k, v := range task.Inputs {
		alias, files = k, v
		break
	}
	if len(files) == 0 {
		return e.writeStageOutput(ctx, task, nil, result)
	}
	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}
	filterOps, filterCols, err := compileFilterExprs(task.FilterExprs)
	if err != nil {
		return fmt.Errorf("scan task %s: filter compile: %w", task.ID, err)
	}
	// Ask the source for only the columns this stage's downstream
	// consumer needs, plus any filter-referenced columns.
	projCols := append([]string(nil), task.Columns...)
	if len(filterCols) > 0 {
		seen := make(map[string]bool, len(projCols))
		for _, c := range projCols {
			seen[c] = true
		}
		for _, c := range filterCols {
			if !seen[c] {
				seen[c] = true
				projCols = append(projCols, c)
			}
		}
	}
	src, err := e.sourceForAliasWithProjection(task.QueryID, bucket, alias, files, projCols)
	if err != nil {
		return fmt.Errorf("scan task %s: source: %w", task.ID, err)
	}
	// Row-group sharding: if the dispatcher fanned out a single-file scan
	// into N tasks, each task reads only its assigned row-group slice.
	// Cast safety: sourceForAliasWithProjection returns *cachedFileStreamSource
	// today; if a future call site returns a different source type the cast
	// fails and we silently read whole-file (functionally correct but wastes
	// the fan-out). Document with a log instead of erroring so a wiring
	// regression doesn't block queries.
	if task.ScanShardCount > 1 {
		if cs, ok := src.(*cachedFileStreamSource); ok {
			cs.SetShard(task.ScanShardIndex, task.ScanShardCount)
		} else {
			e.logger.Warn("scan task: shard params set but source is not cachedFileStreamSource; reading whole file",
				"task_id", task.ID, "shard_idx", task.ScanShardIndex, "shard_count", task.ScanShardCount)
		}
	}
	// SkipFinalizeToRows: writeStageOutput consumes Batches() directly;
	// the ToRows materialization Finalize would otherwise do held 21 GB
	// of live heap on Q18 SF10 (project_q18_sf10_native_dag_oom_2026-04-24).
	collect := &exec.CollectSink{SkipFinalizeToRows: true}
	pipeline := &exec.Pipeline{
		Source: src,
		Ops:    filterOps,
		Sink:   collect,
	}
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("scan task %s: pipeline: %w", task.ID, err)
	}
	return e.writeStageOutput(ctx, task, collect.Batches(), result)
}

// executeGatherStage is the native-DAG Gather task handler: reads all
// Inputs (the upstream stage's output files) and streams them to the
// coordinator's reply subject via gatherReplySink. No SQL, no physical
// plan — the upstream stage already produced the final result shape; the
// gather worker is just a pipe.
func (e *Executor) executeGatherStage(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	e.logger.Info("executeGatherStage: entry",
		"task_id", task.ID, "query_id", task.QueryID,
		"reply_subject", task.ReplySubject, "inputs_aliases", len(task.Inputs))
	if task.ReplySubject == "" {
		return fmt.Errorf("gather task %s: ReplySubject required", task.ID)
	}
	if e.nc == nil {
		return fmt.Errorf("gather task %s: NATS connection required", task.ID)
	}
	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	sink := newGatherReplySink(e.nc, task.ReplySubject, nil)
	if err := sink.Init(ctx); err != nil {
		return fmt.Errorf("gather task %s: sink init: %w", task.ID, err)
	}
	var totalRows, batchesPublished int64
	for alias, files := range task.Inputs {
		e.logger.Info("executeGatherStage: opening source",
			"task_id", task.ID, "alias", alias, "file_count", len(files))
		if len(files) == 0 {
			// Empty upstream: no data to gather. Skip this alias; the
			// sink.Finalize below still sends the terminal marker so
			// the coordinator's recv.wait() unblocks with an empty
			// result instead of waiting forever.
			continue
		}
		src, err := e.sourceForAlias(task.QueryID, bucket, alias, files)
		if err != nil {
			return fmt.Errorf("gather task %s: source for %q: %w", task.ID, alias, err)
		}
		if err := src.Init(ctx); err != nil {
			return fmt.Errorf("gather task %s: init source %q: %w", task.ID, alias, err)
		}
		for {
			b, err := src.Next(ctx)
			if err != nil {
				src.Close()
				return fmt.Errorf("gather task %s: next: %w", task.ID, err)
			}
			if b == nil {
				break
			}
			if err := sink.Consume(ctx, b); err != nil {
				src.Close()
				return fmt.Errorf("gather task %s: consume: %w", task.ID, err)
			}
			totalRows += int64(b.ActiveLen())
			batchesPublished++
		}
		src.Close()
	}
	e.logger.Info("executeGatherStage: finalizing",
		"task_id", task.ID, "reply_subject", task.ReplySubject,
		"batches_published", batchesPublished, "total_rows", totalRows)
	result.NumRows = totalRows
	if err := sink.Finalize(ctx); err != nil {
		e.logger.Error("executeGatherStage: finalize failed",
			"task_id", task.ID, "error", err)
		return err
	}
	e.logger.Info("executeGatherStage: complete",
		"task_id", task.ID, "total_rows", totalRows)
	return nil
}

// executeStageHashJoin builds exec.HashJoin from Task fields, reads the
// build side from Inputs[BuildTableAlias] and the probe side from the
// other Inputs entry, runs a build→probe pipeline, and writes the
// joined output via the sink selected from Task fields.
//
// Wire contract:
//   - task.Inputs[BuildTableAlias] — build-side S3 keys
//   - task.Inputs[<other>]         — probe-side S3 keys (exactly one other entry)
//   - task.JoinType / JoinLeftKeys / JoinRightKeys
//   - task.JoinFilter (optional, semi/anti)
//   - task.BuildRowHint, task.SemiAntiKeyOnly (planner decisions)
//   - task.BuildTableAlias
//   - Output sink: ShuffleKeys+NumPartitions → partitioned .wshf;
//     ReplySubject → gatherReplySink; else unpartitioned .wshf upload.
func (e *Executor) executeStageHashJoin(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Inputs) < 2 {
		return fmt.Errorf("hash_join task %s: needs 2 inputs, got %d", task.ID, len(task.Inputs))
	}
	if len(task.JoinLeftKeys) == 0 || len(task.JoinRightKeys) == 0 {
		return fmt.Errorf("hash_join task %s: JoinLeftKeys and JoinRightKeys required", task.ID)
	}
	buildAlias := task.BuildTableAlias
	if buildAlias == "" {
		return fmt.Errorf("hash_join task %s: BuildTableAlias required (even when synthetic) to pick build-side Input", task.ID)
	}
	buildFiles, ok := task.Inputs[buildAlias]
	if !ok {
		return fmt.Errorf("hash_join task %s: Inputs[%q] (build side) missing", task.ID, buildAlias)
	}
	var probeAlias string
	var probeFiles []string
	for k, v := range task.Inputs {
		if k != buildAlias {
			probeAlias = k
			probeFiles = v
			break
		}
	}
	if probeAlias == "" {
		return fmt.Errorf("hash_join task %s: no probe-side alias (only %q)", task.ID, buildAlias)
	}
	// Empty build side: inner/semi join produces no output; anti join
	// emits the entire probe. Short-circuit rather than erroring on
	// classifyInputFiles's empty-file-list check. Only inner/semi is
	// handled here; anti needs dedicated passthrough (not yet used by
	// the SF0.01 queries that hit this path).
	if len(buildFiles) == 0 {
		switch strings.ToLower(task.JoinType) {
		case "semi", "inner", "":
			return e.writeStageOutput(ctx, task, nil, result)
		}
	}
	// Empty probe: any join type produces no output rows.
	if len(probeFiles) == 0 {
		return e.writeStageOutput(ctx, task, nil, result)
	}

	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	buildSource, err := e.sourceForAlias(task.QueryID, bucket, buildAlias, buildFiles)
	if err != nil {
		return fmt.Errorf("hash_join task %s: build source: %w", task.ID, err)
	}
	probeSource, err := e.sourceForAlias(task.QueryID, bucket, probeAlias, probeFiles)
	if err != nil {
		return fmt.Errorf("hash_join task %s: probe source: %w", task.ID, err)
	}

	joinType := mapJoinTypeString(task.JoinType)
	hj := exec.NewHashJoin(joinType, task.JoinLeftKeys, task.JoinRightKeys)
	hj.BuildTableAlias = task.BuildTableAlias
	hj.QualifyAllBuildCols = task.QualifyAllBuildCols
	if task.BuildRowHint > 0 {
		hj.BuildRowHint = task.BuildRowHint
	}
	hj.SemiAntiKeyOnly = task.SemiAntiKeyOnly
	if task.JoinFilter != "" {
		hj.SemiAntiFilter = physical.BuildSemiAntiFilter(task.JoinFilter)
	}
	// Worker-level shared spill + tracker. All concurrent tasks on this
	// worker share the same budget; HashJoin.Spill triggers when cumulative
	// usage crosses the threshold, regardless of which task allocated.
	if e.sharedSpill != nil {
		hj.Spill = e.sharedSpill
		hj.MemTracker = e.sharedTracker
		// Partition-on-arrival is opt-in based on observed shared-pool
		// pressure at task entry. Below ~30% used, we let the join run the
		// legacy flat path; the per-batch partitioning allocation cost
		// (compactBatchForRows × 64 per source batch) isn't paid back when
		// no spill is actually going to fire. At SF10 with workers_per_node=2,
		// most broadcast-join builds at task entry are well under 30% pool
		// — pre-2026-04-30 unconditional partition-on-arrival turned a
		// quick 10MB build into 3,200 small allocations, GC-thrashed the
		// worker into OOM-kill, and regressed Q02 to 22m vs 12m baseline.
		//
		// When pressure IS already high at task entry (concurrent build
		// holding the pool, or a long-lived prior task), partition-on-
		// arrival pays for itself: spill becomes O(partition) eviction
		// instead of an O(total) flat→partitioned redistribute. If pressure
		// rises mid-build under the flat path, spillBuildBatches still
		// reactively converts to partitioned representation; the gate just
		// avoids paying upfront when the pool is plentiful.
		hj.PartitionOnArrival = exec.SharedPoolUnderPressure(e.sharedTracker)
	}
	// Close releases trackedMem to the shared tracker once the join is done.
	// Without this, a non-spilling broadcast_join leaks its reservation for
	// the lifetime of the worker process — phantom pool pressure inflates
	// across queries and triggers worker-side spill prematurely.
	defer hj.Close()

	if err := buildSource.Init(ctx); err != nil {
		return fmt.Errorf("hash_join task %s: init build source: %w", task.ID, err)
	}
	if err := hj.Build(ctx, buildSource); err != nil {
		buildSource.Close()
		return fmt.Errorf("hash_join task %s: building hash table: %w", task.ID, err)
	}
	buildSource.Close()
	hj.FixKeyAssignment()

	// Build the probe pipeline. FusedJoin entries execute BEFORE the primary
	// hash join because fuseJoinStages absorbs a leaf broadcast_join into
	// its CONSUMER — i.e. the absorbed join was originally upstream of the
	// primary in the data flow. The probe stream goes:
	//
	//   probeSource → fused[0].Probe → fused[1].Probe → … → primary.Probe
	//
	// Each fused join builds its own hash table (a broadcast cache,
	// replicated to every shard task) and probes batches passing through,
	// augmenting them with its build-side columns. The primary join sees
	// the wider probe schema after all fused joins have run.
	probeOps := make([]exec.UnaryOperator, 0, len(task.FusedJoins)+1)
	fusedJoins := make([]*exec.HashJoin, 0, len(task.FusedJoins))
	for i, fj := range task.FusedJoins {
		if len(fj.JoinLeftKeys) == 0 || len(fj.JoinRightKeys) == 0 {
			return fmt.Errorf("hash_join task %s: fused join %d missing keys", task.ID, i)
		}
		if len(fj.BuildFiles) == 0 {
			// An empty broadcast cache produces no output for inner/semi
			// joins; for anti, the entire probe passes through. Today we
			// treat empty as "no output" since SF0.01 / SF10 broadcast
			// chains all use inner joins. Revisit if anti chains land.
			return e.writeStageOutput(ctx, task, nil, result)
		}
		fjAlias := fj.BuildTableAlias
		if fjAlias == "" {
			fjAlias = fmt.Sprintf("fused_build_%d", i)
		}
		fjBuildSrc, err := e.sourceForAlias(task.QueryID, bucket, fjAlias, fj.BuildFiles)
		if err != nil {
			return fmt.Errorf("hash_join task %s: fused %d build source: %w", task.ID, i, err)
		}
		fjType := mapJoinTypeString(fj.JoinType)
		fjHJ := exec.NewHashJoin(fjType, fj.JoinLeftKeys, fj.JoinRightKeys)
		fjHJ.BuildTableAlias = fj.BuildTableAlias
		if fj.JoinFilter != "" {
			fjHJ.SemiAntiFilter = physical.BuildSemiAntiFilter(fj.JoinFilter)
		}
		if e.sharedSpill != nil {
			fjHJ.Spill = e.sharedSpill
			fjHJ.MemTracker = e.sharedTracker
			// Re-check pool pressure for each fused build — by the time the
			// primary build has run, the pool may have crossed the threshold
			// even if it was below at task entry.
			fjHJ.PartitionOnArrival = exec.SharedPoolUnderPressure(e.sharedTracker)
		}
		if err := fjBuildSrc.Init(ctx); err != nil {
			fjBuildSrc.Close()
			return fmt.Errorf("hash_join task %s: fused %d init build source: %w", task.ID, i, err)
		}
		if err := fjHJ.Build(ctx, fjBuildSrc); err != nil {
			fjBuildSrc.Close()
			return fmt.Errorf("hash_join task %s: fused %d building hash table: %w", task.ID, i, err)
		}
		fjBuildSrc.Close()
		fjHJ.FixKeyAssignment()
		fusedJoins = append(fusedJoins, fjHJ)
		probeOps = append(probeOps, fjHJ.Probe())
	}
	// Primary probe last — the original consumer-of-fused position.
	probeOps = append(probeOps, hj.Probe())
	defer func() {
		for _, fjHJ := range fusedJoins {
			fjHJ.Close()
		}
	}()

	// Probe pipeline with CollectSink; we post-process batches into the
	// configured output sink. CollectSink is memory-bounded by the build
	// spill semantics (per-worker partition of probe input).
	// SkipFinalizeToRows: we read Batches() below, never Rows. Without
	// this flag, Finalize materializes every probe row as map[string]any
	// — for Q18 SF10 join-8 (~60M probe rows) that's 15+ GB of pure waste.
	collect := &exec.CollectSink{SkipFinalizeToRows: true}
	pipeline := &exec.Pipeline{
		Source: probeSource,
		Ops:    probeOps,
		Sink:   collect,
	}
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("hash_join task %s: probe pipeline: %w", task.ID, err)
	}

	batches := collect.Batches()
	batches, err = applyPostFilter(ctx, task, batches)
	if err != nil {
		return fmt.Errorf("hash_join task %s: %w", task.ID, err)
	}
	batches = applyPostSort(ctx, task, batches)
	return e.writeStageOutput(ctx, task, batches, result)
}

// executeStageAggregate runs a HashAggregate over a single Input, writing
// the aggregated rows via writeStageOutput. Handles both "aggregate"
// (partial aggregate feeding a final_aggregate) and "final_aggregate"
// (merge partials from upstream partitions) — they share the same kernel;
// the planner decides which columns are pass-through vs. aggregated and
// encodes that in task.Aggregates + task.GroupByCols.
func (e *Executor) executeStageAggregate(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Inputs) != 1 {
		return fmt.Errorf("aggregate task %s: needs exactly 1 input, got %d", task.ID, len(task.Inputs))
	}
	var alias string
	var files []string
	for k, v := range task.Inputs {
		alias, files = k, v
		break
	}
	// Empty upstream is legal — a semi-join that matched nothing, or a
	// HAVING that rejected every partial, feeds the downstream aggregate
	// zero files. Grouped aggregates emit zero rows; scalar aggregates
	// emit identity values. Short-circuit via writeStageOutput(nil).
	if len(files) == 0 {
		return e.writeStageOutput(ctx, task, nil, result)
	}
	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}
	// Column projection hint: the aggregate only needs group-by columns +
	// each aggregate's input column. When ALL of those names exist in the
	// file schema the reader skips the rest — a big I/O win on wide tables
	// like lineitem (16 cols → typically 4-5 used). If any name is a
	// derived/expression output (e.g. "disc_price" for
	// sum(l_extendedprice*(1-l_discount))), the reader detects the missing
	// column and falls back to full-schema read so the downstream operator
	// can still compute the derivation.
	// Compile any scan-pushed filter fragments FIRST so we can extend the
	// projection hint with filter-referenced columns — otherwise the
	// projection may prune the very columns the filter needs (e.g. Q01's
	// l_shipdate is referenced only by WHERE, not by GROUP BY / aggs).
	filterOps, filterCols, err := compileFilterExprs(task.FilterExprs)
	if err != nil {
		return fmt.Errorf("aggregate task %s: filter compile: %w", task.ID, err)
	}
	// Build a Project operator when any aggregate has a derived input
	// expression (e.g. SUM(l_extendedprice * (1 - l_discount))). The
	// logical plan's implicit pre-projection is lost in the native-DAG
	// wire format, so without this the HashAggregate looks up a column
	// literally named by the expression string, finds nothing, and emits
	// nil aggregates.
	//
	// Skip project construction in merge mode — the partial stage already
	// computed the derived expression and emitted it under OutputCol; the
	// merge path rewrites InputCol = OutputCol below so HashAggregate
	// reads that pre-computed column directly.
	isMergeStage := task.StageType == "final_aggregate" || task.StageType == "merge_aggregate"
	var aggProject *exec.Project
	var aggExtraCols []string
	if !isMergeStage {
		aggProject, aggExtraCols, err = buildAggInputProjection(task.GroupByCols, task.Aggregates, filterCols)
		if err != nil {
			return fmt.Errorf("aggregate task %s: agg input project: %w", task.ID, err)
		}
	}
	projCols := aggProjectionCols(task.GroupByCols, task.Aggregates)
	// Extend the source column-projection hint with every column the
	// filter or a derived aggregate input reads, so the parquet reader
	// doesn't prune them from the batches it hands us.
	extend := func(cols []string) {
		seen := make(map[string]bool, len(projCols))
		for _, c := range projCols {
			seen[c] = true
		}
		for _, c := range cols {
			if !seen[c] {
				seen[c] = true
				projCols = append(projCols, c)
			}
		}
	}
	if len(filterCols) > 0 {
		extend(filterCols)
	}
	if len(aggExtraCols) > 0 {
		extend(aggExtraCols)
	}
	src, err := e.sourceForAliasWithProjection(task.QueryID, bucket, alias, files, projCols)
	if err != nil {
		return fmt.Errorf("aggregate task %s: source: %w", task.ID, err)
	}
	// Row-group sharding for fused scan-aggregate over a single compacted file.
	if task.ScanShardCount > 1 {
		if cs, ok := src.(*cachedFileStreamSource); ok {
			cs.SetShard(task.ScanShardIndex, task.ScanShardCount)
		}
	}

	// For final_aggregate / merge_aggregate the upstream "partial" stage
	// already computed the aggregate and emitted the result under OutputCol
	// (e.g. "total"). The raw InputCol ("v") isn't in the partial output, so
	// using it here makes the merge step look up a column that doesn't exist
	// and every group comes back with a nil aggregate value. The merge is
	// really "re-aggregate the partial results", so the input for that step
	// is the partial OutputCol.
	//
	// Function rewrites on merge:
	//   COUNT → SUM: counting rows at merge time would re-count the
	//     number of partial rows (usually == number of groups), not the
	//     total input rows. Summing partial counts is the correct merge.
	//   AVG   → UNSUPPORTED: AVG doesn't decompose into a single-column
	//     partial (you need the per-group SUM and COUNT). The dispatcher
	//     falls back to single-task scan-aggregate when AVG is present,
	//     which means this merge path never sees AVG across multiple
	//     partials. We still guard against it here.
	mergeMode := task.StageType == "final_aggregate" || task.StageType == "merge_aggregate"
	aggCols := make([]exec.AggColumn, len(task.Aggregates))
	for i, a := range task.Aggregates {
		inputCol := a.InputCol
		fn := parseAggFuncString(a.Func)
		if mergeMode {
			if a.OutputCol != "" {
				inputCol = a.OutputCol
			}
			if fn == exec.AggCount {
				fn = exec.AggSum
			}
		}
		aggCols[i] = exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			OutputCol:  a.OutputCol,
			OutputType: aggOutputTypeString(a.Func),
		}
	}
	hashAgg := exec.NewHashAggregate(task.GroupByCols, aggCols)
	if e.sharedSpill != nil {
		hashAgg.Spill = e.sharedSpill
	}
	// Close releases tracker reservations once draining is complete; see
	// HashJoin defer above for why this is required.
	defer hashAgg.Close()

	// Order: source → filters → derived-column projection → HashAggregate.
	// Filter first so expressions only evaluate on surviving rows.
	ops := filterOps
	if aggProject != nil {
		ops = append(ops, aggProject)
	}
	pipeline := &exec.Pipeline{
		Source: src,
		Ops:    ops,
		Sink:   hashAgg,
	}
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("aggregate task %s: pipeline: %w", task.ID, err)
	}
	// HashAggregate acts as a source after the sink run: drain its output
	// batches via Next.
	var outBatches []*batch.RecordBatch
	for {
		b, err := hashAgg.Next(ctx)
		if err != nil {
			return fmt.Errorf("aggregate task %s: next: %w", task.ID, err)
		}
		if b == nil {
			break
		}
		outBatches = append(outBatches, b)
	}
	outBatches, err = applyPostFilter(ctx, task, outBatches)
	if err != nil {
		return fmt.Errorf("aggregate task %s: %w", task.ID, err)
	}
	outBatches = applyPostSort(ctx, task, outBatches)
	return e.writeStageOutput(ctx, task, outBatches, result)
}

// aggProjectionCols returns the union of group-by columns and each
// aggregate spec's input column — the minimum set of column names the
// aggregate operator references. The returned list may include derived
// names (e.g. pre-projected expression outputs); the source treats those
// as a no-op because they won't match any file-schema column.
func aggProjectionCols(groupBy []string, aggs []distributed.AggSpec) []string {
	seen := make(map[string]bool, len(groupBy)+len(aggs))
	out := make([]string, 0, len(groupBy)+len(aggs))
	for _, c := range groupBy {
		if c != "" && !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	for _, a := range aggs {
		if a.InputCol != "" && !seen[a.InputCol] {
			seen[a.InputCol] = true
			out = append(out, a.InputCol)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// applyPostFilter runs task.PostFilterExprs against the given batches,
// returning the surviving rows. Used for HAVING (post-aggregate) and
// for residual predicates on hash_join output. Returns the input
// unchanged when PostFilterExprs is empty.
func applyPostFilter(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if len(task.PostFilterExprs) == 0 || len(batches) == 0 {
		return batches, nil
	}
	filterOps, _, err := compileFilterExprs(task.PostFilterExprs)
	if err != nil {
		return nil, fmt.Errorf("post-filter compile: %w", err)
	}
	if len(filterOps) == 0 {
		return batches, nil
	}
	for _, op := range filterOps {
		if err := op.Init(ctx); err != nil {
			return nil, fmt.Errorf("post-filter init: %w", err)
		}
	}
	out := make([]*batch.RecordBatch, 0, len(batches))
	for _, b := range batches {
		if b == nil {
			continue
		}
		cur := b
		for _, op := range filterOps {
			next, err := op.Execute(ctx, cur)
			if err != nil {
				return nil, fmt.Errorf("post-filter exec: %w", err)
			}
			if next == nil {
				cur = nil
				break
			}
			cur = next
		}
		if cur == nil {
			continue
		}
		if cur.ActiveLen() == 0 {
			continue
		}
		// Snapshot the selection vector before retaining the batch — Filter
		// operators reuse outSel across calls (see exec.Filter.selBuf), so
		// without copying, this batch's Sel would be clobbered by the next
		// iteration's filter call. CollectSink does the same snapshot for
		// the same reason. Without this, Q07's post-filter on join-10
		// retains rows the OR-WHERE rejected (extra empty group + extra
		// (FRANCE,FRANCE) and (GERMANY,GERMANY) groups in output).
		if cur.Sel != nil {
			selCopy := make([]uint32, len(cur.Sel))
			copy(selCopy, cur.Sel)
			cur.Sel = selCopy
		}
		out = append(out, cur)
	}
	return out, nil
}

// applyPostSort runs an in-process Sort on the given batches when the
// planner's fuseSortIntoPredecessor pass folded a Singleton sort into
// this stage. Returns the input unchanged when task.SortKeys is empty.
// Also honors task.Limit for Top-K truncation when set. Output batches
// replace the input batch slice; input batches are fed through Sort as
// a one-shot source.
func applyPostSort(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch) []*batch.RecordBatch {
	if len(task.SortKeys) == 0 || len(batches) == 0 {
		return batches
	}
	keys := make([]exec.SortKey, len(task.SortKeys))
	for i, k := range task.SortKeys {
		order := exec.Ascending
		if k.Desc {
			order = exec.Descending
		}
		keys[i] = exec.SortKey{Column: k.Column, Order: order}
	}
	sorter := exec.NewSort(keys)
	if err := sorter.Init(ctx); err != nil {
		return batches
	}
	for _, b := range batches {
		if b == nil {
			continue
		}
		if err := sorter.Consume(ctx, b); err != nil {
			return batches
		}
	}
	if err := sorter.Finalize(ctx); err != nil {
		return batches
	}
	if task.Limit > 0 {
		sorter.Truncate(task.Limit)
	}
	var out []*batch.RecordBatch
	for {
		b, err := sorter.Next(ctx)
		if err != nil || b == nil {
			break
		}
		out = append(out, b)
	}
	return out
}

// executeStageSort runs an in-memory Sort over a single Input, optionally
// truncating to task.Limit rows (Top-K), and writes the result via
// writeStageOutput. "merge_sort" stages consume pre-sorted partition
// streams — they use the same code path (a total sort is a correct merge
// output, just slower). Fine-tuned merge-sort is a follow-up.
func (e *Executor) executeStageSort(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Inputs) != 1 {
		return fmt.Errorf("sort task %s: needs exactly 1 input, got %d", task.ID, len(task.Inputs))
	}
	if len(task.SortKeys) == 0 {
		return fmt.Errorf("sort task %s: SortKeys required", task.ID)
	}
	var alias string
	var files []string
	for k, v := range task.Inputs {
		alias, files = k, v
		break
	}
	// Empty upstream is legal — a CTE / subquery / semi-join that matched
	// nothing, an aggregate that filtered out every row, etc. Sort of zero
	// rows is zero rows. Without this short-circuit the source classifier
	// errors out with "empty file list" and the whole query fails (Q15
	// CI failure 2026-04-25 — TestDistributedTPCH/Q15_Top_Supplier).
	if len(files) == 0 {
		return e.writeStageOutput(ctx, task, nil, result)
	}
	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}
	src, err := e.sourceForAlias(task.QueryID, bucket, alias, files)
	if err != nil {
		return fmt.Errorf("sort task %s: source: %w", task.ID, err)
	}

	keys := make([]exec.SortKey, len(task.SortKeys))
	for i, k := range task.SortKeys {
		order := exec.Ascending
		if k.Desc {
			order = exec.Descending
		}
		keys[i] = exec.SortKey{Column: k.Column, Order: order}
	}
	sorter := exec.NewSort(keys)
	// Close releases tracker reservation; see HashJoin defer for the
	// architectural rationale.
	defer sorter.Close()

	pipeline := &exec.Pipeline{
		Source: src,
		Sink:   sorter,
	}
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("sort task %s: pipeline: %w", task.ID, err)
	}
	if task.Limit > 0 {
		sorter.Truncate(task.Limit)
	}
	var outBatches []*batch.RecordBatch
	for {
		b, err := sorter.Next(ctx)
		if err != nil {
			return fmt.Errorf("sort task %s: next: %w", task.ID, err)
		}
		if b == nil {
			break
		}
		outBatches = append(outBatches, b)
	}
	return e.writeStageOutput(ctx, task, outBatches, result)
}

// aggOutputTypeString mirrors planner/physical.aggOutputType. The native
// AggSpec doesn't carry an output type; the worker derives it from the
// function name. Default (sum/min/max/avg) → float64, matching the
// coordinator's planner convention.
func aggOutputTypeString(funcName string) parquet.TypeID {
	switch strings.ToLower(strings.TrimSpace(funcName)) {
	case "count", "count_distinct", "approx_distinct":
		return parquet.TypeInt64
	case "string_agg":
		return parquet.TypeString
	case "bool_and", "every", "bool_or":
		return parquet.TypeBool
	default:
		return parquet.TypeFloat64
	}
}

// parseAggFuncString maps the canonical string form carried on
// distributed.AggSpec.Func into exec.AggFunc. Mirrors
// planner/physical.parseAggFunc; duplicated here to avoid importing the
// planner into the worker executor path.
func parseAggFuncString(s string) exec.AggFunc {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "sum":
		return exec.AggSum
	case "count":
		return exec.AggCount
	case "min":
		return exec.AggMin
	case "max":
		return exec.AggMax
	case "avg":
		return exec.AggAvg
	case "count_distinct":
		return exec.AggCountDistinct
	case "string_agg":
		return exec.AggStringAgg
	case "bool_and", "every":
		return exec.AggBoolAnd
	case "bool_or":
		return exec.AggBoolOr
	case "stddev", "stddev_samp":
		return exec.AggStddev
	case "variance", "var_samp":
		return exec.AggVariance
	default:
		return exec.AggSum
	}
}

// writeStageOutput dispatches produced batches to the sink selected by
// task fields. Three cases:
//   - task.ReplySubject set → gatherReplySink (stream to coordinator NATS)
//   - task.ShuffleKeys + NumPartitions set → partitionedShuffleSink, upload
//     each non-empty partition to <ResultPrefix>partition=NNNN/<taskID>.wshf
//   - neither → single unpartitioned .wshf upload to <ResultPrefix><taskID>.wshf
func (e *Executor) writeStageOutput(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch, result *distributed.ResultNotification) error {
	// Discover row count and schema.
	var totalRows int64
	var schema []parquet.Column
	for _, b := range batches {
		if b == nil {
			continue
		}
		totalRows += int64(b.ActiveLen())
		if schema == nil && len(b.Schema) > 0 {
			schema = b.Schema
		}
	}
	result.NumRows = totalRows

	// Gather reply: stream via NATS.
	if task.ReplySubject != "" {
		if e.nc == nil {
			return fmt.Errorf("stage task %s: ReplySubject set but executor has no NATS connection", task.ID)
		}
		sink := newGatherReplySink(e.nc, task.ReplySubject, schema)
		if err := sink.Init(ctx); err != nil {
			return fmt.Errorf("stage task %s: gather sink init: %w", task.ID, err)
		}
		for _, b := range batches {
			if b == nil || b.ActiveLen() == 0 {
				continue
			}
			if err := sink.Consume(ctx, b); err != nil {
				return fmt.Errorf("stage task %s: gather sink consume: %w", task.ID, err)
			}
		}
		return sink.Finalize(ctx)
	}

	// No output: nothing to write.
	if totalRows == 0 || schema == nil {
		return nil
	}

	// Partitioned shuffle output.
	if len(task.ShuffleKeys) > 0 && task.NumPartitions > 0 {
		return e.writePartitionedShuffle(ctx, task, batches, schema, result)
	}

	// Unpartitioned .wshf output.
	return e.writeUnpartitionedWSHF(ctx, task, batches, schema, result)
}

// writePartitionedShuffle hash-partitions all batches on task.ShuffleKeys
// into task.NumPartitions output files and uploads each non-empty partition
// to <ResultBucket>/<ResultPrefix>partition=NNNN/<TaskID>.wshf.
func (e *Executor) writePartitionedShuffle(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch, schema []parquet.Column, result *distributed.ResultNotification) error {
	spillDir := filepath.Join(e.spillDir, "stage-"+task.ID)
	if e.spillDir == "" {
		spillDir = filepath.Join(os.TempDir(), "stage-"+task.ID)
	}
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return fmt.Errorf("stage task %s: creating spill dir: %w", task.ID, err)
	}
	defer os.RemoveAll(spillDir)

	sink := newPartitionedShuffleSink(spillDir, task.ShuffleKeys, task.NumPartitions, schema)
	if err := sink.Init(ctx); err != nil {
		return fmt.Errorf("stage task %s: partitioned sink init: %w", task.ID, err)
	}
	defer sink.Close()

	for _, b := range batches {
		if b == nil || b.ActiveLen() == 0 {
			continue
		}
		if err := sink.Consume(ctx, b); err != nil {
			return fmt.Errorf("stage task %s: partitioned sink consume: %w", task.ID, err)
		}
	}
	if err := sink.Finalize(ctx); err != nil {
		return fmt.Errorf("stage task %s: partitioned sink finalize: %w", task.ID, err)
	}

	for p, localPath := range sink.PartitionFiles() {
		if localPath == "" {
			continue
		}
		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("stage task %s: opening partition %d: %w", task.ID, p, err)
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("stage task %s: stat partition %d: %w", task.ID, p, err)
		}
		key := fmt.Sprintf("%spartition=%04d/%s.wshf", task.ResultPrefix, p, task.ID)

		// S3 is the durable store; KV (if small enough) is a best-effort fast
		// cache. See writeUnpartitionedWSHF for the same rationale — KV's
		// 5-min TTL is shorter than long-running queries.
		_, uploadErr := e.store.Put(ctx, task.ResultBucket, key, f, fi.Size(), "application/octet-stream")
		f.Close()
		if uploadErr != nil {
			return fmt.Errorf("stage task %s: uploading partition %d: %w", task.ID, p, uploadErr)
		}
		result.ResultFiles = append(result.ResultFiles, key)
		result.SizeBytes += fi.Size()

		// Best-effort KV cache for small partitions. Read the local file we
		// already wrote to disk — cheaper than re-buffering. Failure is
		// non-fatal because S3 is now durable.
		if e.resultKV != nil && fi.Size() <= natsKVResultThreshold {
			if payload, readErr := os.ReadFile(localPath); readErr == nil {
				if _, putErr := e.resultKV.Put(ctx, natsKVKey(key), payload); putErr != nil {
					e.logger.Debug("KV cache write failed (S3 already durable)",
						"task_id", task.ID, "key", key,
						"payload_bytes", len(payload), "err", putErr)
				}
			}
		}

		// Same-worker fast path: hand the local file to the LocalStageCache
		// so a downstream task on this worker can mmap it directly. Adopt
		// renames the file out of the per-task spill dir into the cache's
		// per-query dir.
		if adopted := e.localCache.Adopt(task.QueryID, key, localPath); adopted == "" {
			_ = os.Remove(localPath)
		}
	}
	return nil
}

// writeUnpartitionedWSHF writes all batches to a single in-memory WSHF buffer
// and uploads it to <ResultBucket>/<ResultPrefix><TaskID>.wshf. Used when
// the stage's consumer treats the output as a single unpartitioned stream
// (e.g., aggregate feeding final_aggregate, or pipeline output to a
// downstream stage that re-partitions).
func (e *Executor) writeUnpartitionedWSHF(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch, schema []parquet.Column, result *distributed.ResultNotification) error {
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		return fmt.Errorf("stage task %s: writeHeader: %w", task.ID, err)
	}
	numChunks := uint32(0)
	for _, b := range batches {
		if b == nil {
			continue
		}
		active := b.ActiveLen()
		if active == 0 {
			continue
		}
		if err := sw.writeChunk(b.Columns, b.Sel, active); err != nil {
			return fmt.Errorf("stage task %s: writeChunk: %w", task.ID, err)
		}
		numChunks++
	}
	// Patch chunk count at offset 4.
	payload := buf.Bytes()
	payload[4] = byte(numChunks)
	payload[5] = byte(numChunks >> 8)
	payload[6] = byte(numChunks >> 16)
	payload[7] = byte(numChunks >> 24)

	key := fmt.Sprintf("%s%s.wshf", task.ResultPrefix, task.ID)

	// S3 is the durable store. NATS KV has a 5-minute TTL (coordinator.go
	// jetstream.KeyValueConfig) and a 1 GB bucket cap — entries can expire or
	// be evicted before downstream stages read them. Q02 SF10 2026-04-28 hit
	// exactly this: 10m57s query, KV-only outputs vanished at minute 5,
	// downstream `nats: key not found` + `object not found`. Always upload to
	// S3 first; KV is a best-effort fast-read cache below.
	_, err := e.store.Put(ctx, task.ResultBucket, key, bytes.NewReader(payload), int64(len(payload)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("stage task %s: uploading wshf: %w", task.ID, err)
	}
	result.ResultFiles = append(result.ResultFiles, key)
	result.SizeBytes += int64(len(payload))

	// Best-effort KV cache for small payloads. Downstream consumers check KV
	// first (~10ms) and fall back to S3 (~500ms) on miss; both are correct
	// reads now that S3 is durable.
	if e.resultKV != nil && len(payload) <= natsKVResultThreshold {
		kvKey := natsKVKey(key)
		if _, err := e.resultKV.Put(ctx, kvKey, payload); err != nil {
			e.logger.Debug("KV cache write failed (S3 already durable)",
				"task_id", task.ID, "key", key,
				"payload_bytes", len(payload), "err", err)
		}
	}

	// Same-worker fast path: adopt the local copy so a downstream task on
	// this worker can mmap it directly. Best-effort — failures fall back to
	// S3.
	e.cacheUnpartitionedLocal(task.QueryID, key, payload)
	return nil
}

// cacheUnpartitionedLocal writes payload to a temp file under the worker's
// spill directory and adopts it into the LocalStageCache. Best-effort — any
// I/O failure leaves the cache empty for this entry, and the consumer falls
// through to S3 as before. Caller must have already written payload durably
// to S3 (or KV) before invoking this.
func (e *Executor) cacheUnpartitionedLocal(queryID, key string, payload []byte) {
	if e.localCache == nil || e.spillDir == "" {
		return
	}
	tmp, err := os.CreateTemp(e.spillDir, "stage-unpart-*.wshf")
	if err != nil {
		return
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return
	}
	if adopted := e.localCache.Adopt(queryID, key, tmpPath); adopted == "" {
		_ = os.Remove(tmpPath)
	}
}

// mapJoinTypeString converts the canonical join-type string carried on
// task.JoinType into an exec.JoinType. Mirrors
// planner/physical.mapExecJoinType; duplicated here to avoid importing
// the planner package into the worker executor path.
func mapJoinTypeString(jt string) exec.JoinType {
	switch strings.ToLower(strings.TrimSpace(jt)) {
	case "left":
		return exec.LeftJoin
	case "right":
		return exec.RightJoin
	case "full":
		return exec.FullOuterJoin
	case "cross":
		return exec.CrossJoin
	case "semi":
		return exec.SemiJoin
	case "anti":
		return exec.AntiJoin
	default:
		return exec.InnerJoin
	}
}
