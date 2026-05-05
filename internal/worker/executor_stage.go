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
	// Fragment dispatch (multi-operator pipeline). Routes to the unified
	// executeFragment path when the planner emits an Operators[] chain
	// instead of a single StageType. Falls through to the per-StageType
	// switch when Operators is empty (legacy single-op stages, still in
	// active use until every shape has migrated).
	if len(task.Operators) > 0 {
		return e.executeFragment(ctx, task, result)
	}
	switch task.StageType {
	case "hash_join", "broadcast_join":
		return e.executeStageHashJoin(ctx, task, result)
	default:
		return fmt.Errorf("executeStage: unsupported StageType %q on task %s",
			task.StageType, task.ID)
	}
}


// uploadUnpartitionedSpill uploads a streaming sink's finalised file to S3,
// populates the NATS KV fast-read cache for small payloads, and adopts the
// file into the LocalStageCache for same-worker downstream tasks. Mirrors the
// post-write actions in writeUnpartitionedWSHF, but reads from disk instead of
// keeping the entire payload in heap.
func (e *Executor) uploadUnpartitionedSpill(ctx context.Context, task distributed.Task, sink *unpartitionedStageSink, result *distributed.ResultNotification) error {
	key := fmt.Sprintf("%s%s.wshf", task.ResultPrefix, task.ID)
	srcPath := sink.Path()

	stat, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("scan task %s: stat spill file: %w", task.ID, err)
	}
	size := stat.Size()

	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("scan task %s: open spill file: %w", task.ID, err)
	}
	if _, err := e.store.Put(ctx, task.ResultBucket, key, f, size, "application/octet-stream"); err != nil {
		f.Close()
		return fmt.Errorf("scan task %s: uploading wshf: %w", task.ID, err)
	}
	f.Close()

	result.ResultFiles = append(result.ResultFiles, key)
	result.SizeBytes += size

	// KV fast-read cache for small payloads (downstream stages on this query
	// hit KV first, fall through to S3 on miss). Skipped for large outputs
	// where the read cost would defeat the streaming-write savings.
	if e.resultKV != nil && size <= natsKVResultThreshold {
		payload, readErr := os.ReadFile(srcPath)
		if readErr == nil {
			kvKey := natsKVKey(key)
			if _, putErr := e.resultKV.Put(ctx, kvKey, payload); putErr != nil {
				e.logger.Debug("KV cache write failed (S3 already durable)",
					"task_id", task.ID, "key", key,
					"payload_bytes", len(payload), "err", putErr)
			}
		}
	}

	// Same-worker fast path: hand the spill file to the LocalStageCache. Adopt
	// uses os.Rename, moving the file out of the spill dir into the cache's
	// per-query directory; downstream tasks on this worker mmap it directly.
	// On Adopt failure (cross-device rename, etc.) the file stays in spillDir
	// and the deferred RemoveAll cleans it up — the durable S3 copy still
	// satisfies cross-worker reads.
	if e.localCache != nil && e.spillDir != "" {
		e.localCache.Adopt(task.QueryID, key, srcPath)
	}
	return nil
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

	sink := newGatherReplySink(e.nc, task.ReplySubject, result.WorkerID, nil)
	if err := sink.Init(ctx); err != nil {
		return fmt.Errorf("gather task %s: sink init: %w", task.ID, err)
	}
	// Always publish a terminal marker, even on error — the coord's gather
	// subscriber waits on the reply subject for the terminal message to
	// unblock recv.wait. Without this, any Consume / Next failure left the
	// coord blocked until query timeout (gather hang on Q grace_hash_join
	// SF1 was a 12 MB single-message publish that exceeded NATS's 8 MB
	// payload cap; the failed task returned without finalizing and the
	// coord waited 1m+ for batches that would never come). Finalize is
	// idempotent so the explicit success-path call below stays.
	defer func() { _ = sink.Finalize(ctx) }()
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
		// Partition-on-arrival policy:
		//   - Standalone (non-fused) join: opt-in based on observed
		//     shared-pool pressure at task entry. Below ~30% used, the
		//     legacy flat path skips the per-batch partitioning cost
		//     (compactBatchForRows × 64 per source batch). Without this
		//     gate, small broadcast joins (10 MB nation, region) burn
		//     thousands of micro-allocations for no spill benefit and
		//     regress wall — Q02 22m incident on 2026-04-30 morning.
		//   - Fused-chain join (this stage has FusedJoins entries): force
		//     PartitionOnArrival regardless of entry-time pressure. The
		//     chain holds N concurrent build hash tables × probe-split
		//     amplification; reactive spill arrives too late, the worker
		//     OOMs before the trigger fires. Q21 SF10 regression on
		//     2026-04-30 deploy: peak heap 19.8 GB on a 12 GB envelope.
		//     The amortization argument flips here too — a fused chain
		//     by definition has cumulative cache size large enough to
		//     pay back the partitioning cost.
		if len(task.FusedJoins) > 0 {
			hj.PartitionOnArrival = true
		} else {
			hj.PartitionOnArrival = exec.SharedPoolUnderPressure(e.sharedTracker)
		}
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
	// Output column projection: the planner sets stage.Columns to the columns
	// the downstream stage consumes from this join's output. The probe operator
	// uses OutputFilter to drop everything else, so the join writes a tight
	// schema to its WSHF instead of the full union of build+probe columns.
	// Empty Columns → nil filter → preserves "emit everything" semantics.
	// Qualified names (e.g. "n2.n_name" from self-joins) are handled by the
	// probe's lookup (see internal/engine/exec/join.go OutputSchema dot-strip).
	var outputFilter map[string]bool
	if len(task.Columns) > 0 {
		outputFilter = make(map[string]bool, len(task.Columns))
		for _, c := range task.Columns {
			outputFilter[c] = true
		}
	}
	// Defer cleanup BEFORE building any fused joins so that an error
	// partway through the loop still releases tracker reservations and
	// closes spill files for every fjHJ that was constructed. The earlier
	// shape (defer after the loop) leaked any fjHJ whose Init or Build
	// failed AFTER allocation but before the success-path append — and
	// since each fjHJ may have already reserved memory in the shared
	// tracker via Build's first batch, that's a phantom-pool-pressure
	// leak across the whole worker.
	defer func() {
		for _, fjHJ := range fusedJoins {
			fjHJ.Close()
		}
	}()
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
			// Force partition-on-arrival for every fused-chain build —
			// see the corresponding policy comment on the primary join
			// above. The chain holds N hash tables in memory simultaneously;
			// reactive spill (waiting for pool to cross 60%) arrives too late
			// once the FIRST cache has already grown unbounded. Proactive
			// partitioning + 60%-threshold-driven eviction keeps the per-
			// task working set bounded by partition_count × max_partition,
			// not by total_chain_cache_bytes.
			fjHJ.PartitionOnArrival = true
		}
		// Append BEFORE Init/Build so the deferred cleanup closes fjHJ on
		// any subsequent error. Build may reserve tracker memory on its
		// first batch even if it later fails — Close releases it.
		fusedJoins = append(fusedJoins, fjHJ)
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
		// No OutputFilter on fused probes: each fused probe must emit columns
		// that LATER probes in the chain (including the primary) consume —
		// e.g. probe-side keys for the next join. task.Columns reflects the
		// primary's NeededColumns only, so applying it to a fused probe would
		// silently drop those downstream-key columns mid-chain.
		probeOps = append(probeOps, fjHJ.Probe())
	}
	// Primary probe last — its output is what the stage emits. OutputFilter
	// here is safe: nothing downstream of the primary consumes intermediate
	// columns that aren't already in task.Columns.
	primaryProbe := hj.Probe()
	primaryProbe.OutputFilter = outputFilter
	probeOps = append(probeOps, primaryProbe)

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
		sink := newGatherReplySink(e.nc, task.ReplySubject, result.WorkerID, schema)
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

	return e.uploadPartitionedShuffleFiles(ctx, task, sink, result)
}

// uploadPartitionedShuffleFiles takes a finalised partitioned sink and uploads
// each non-empty partition file to S3, populates the KV fast-read cache for
// small payloads, and adopts the local file into the LocalStageCache. Shared
// between the legacy collect-then-partition path (writePartitionedShuffle) and
// the streaming-partition path (runStageScanPartitionedStreaming).
func (e *Executor) uploadPartitionedShuffleFiles(ctx context.Context, task distributed.Task, sink *partitionedShuffleSink, result *distributed.ResultNotification) error {
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
		if e.localCache != nil {
			if adopted := e.localCache.Adopt(task.QueryID, key, localPath); adopted == "" {
				_ = os.Remove(localPath)
			}
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
