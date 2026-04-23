package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	case "scan":
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
	return fmt.Errorf("executeStageScan: not yet implemented")
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
		src, err := e.sourceForAlias(bucket, alias, files)
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

	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	buildSource, err := e.sourceForAlias(bucket, buildAlias, buildFiles)
	if err != nil {
		return fmt.Errorf("hash_join task %s: build source: %w", task.ID, err)
	}
	probeSource, err := e.sourceForAlias(bucket, probeAlias, probeFiles)
	if err != nil {
		return fmt.Errorf("hash_join task %s: probe source: %w", task.ID, err)
	}

	joinType := mapJoinTypeString(task.JoinType)
	hj := exec.NewHashJoin(joinType, task.JoinLeftKeys, task.JoinRightKeys)
	hj.BuildTableAlias = task.BuildTableAlias
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
	}

	if err := buildSource.Init(ctx); err != nil {
		return fmt.Errorf("hash_join task %s: init build source: %w", task.ID, err)
	}
	if err := hj.Build(ctx, buildSource); err != nil {
		buildSource.Close()
		return fmt.Errorf("hash_join task %s: building hash table: %w", task.ID, err)
	}
	buildSource.Close()
	hj.FixKeyAssignment()

	// Probe pipeline with CollectSink; we post-process batches into the
	// configured output sink. CollectSink is memory-bounded by the build
	// spill semantics (per-worker partition of probe input).
	probe := hj.Probe()
	collect := &exec.CollectSink{}
	pipeline := &exec.Pipeline{
		Source: probeSource,
		Ops:    []exec.UnaryOperator{probe},
		Sink:   collect,
	}
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("hash_join task %s: probe pipeline: %w", task.ID, err)
	}

	batches := collect.Batches()
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
	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}
	src, err := e.sourceForAlias(bucket, alias, files)
	if err != nil {
		return fmt.Errorf("aggregate task %s: source: %w", task.ID, err)
	}

	aggCols := make([]exec.AggColumn, len(task.Aggregates))
	for i, a := range task.Aggregates {
		aggCols[i] = exec.AggColumn{
			Func:       parseAggFuncString(a.Func),
			InputCol:   a.InputCol,
			OutputCol:  a.OutputCol,
			OutputType: aggOutputTypeString(a.Func),
		}
	}
	hashAgg := exec.NewHashAggregate(task.GroupByCols, aggCols)
	if e.sharedSpill != nil {
		hashAgg.Spill = e.sharedSpill
	}

	pipeline := &exec.Pipeline{
		Source: src,
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
	outBatches = applyPostSort(ctx, task, outBatches)
	return e.writeStageOutput(ctx, task, outBatches, result)
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
	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}
	src, err := e.sourceForAlias(bucket, alias, files)
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

		// KV fast-path per partition file: if the partition fits, KV it
		// and skip S3. Partition sizes vary a lot within one stage (skew),
		// so this is a per-partition decision, not per-stage.
		if e.resultKV != nil && fi.Size() <= natsKVResultThreshold {
			payload := make([]byte, fi.Size())
			if _, readErr := io.ReadFull(f, payload); readErr == nil {
				f.Close()
				if _, putErr := e.resultKV.Put(ctx, natsKVKey(key), payload); putErr == nil {
					result.ResultFiles = append(result.ResultFiles, key)
					result.SizeBytes += fi.Size()
					_ = os.Remove(localPath)
					continue
				}
				// KV put failed — re-open for S3 upload.
				f, err = os.Open(localPath)
				if err != nil {
					return fmt.Errorf("stage task %s: reopen partition %d for S3: %w", task.ID, p, err)
				}
			} else {
				// Read failed — fall through and let S3 Put stream from file.
				f.Seek(0, 0)
			}
		}

		_, uploadErr := e.store.Put(ctx, task.ResultBucket, key, f, fi.Size(), "application/octet-stream")
		f.Close()
		if uploadErr != nil {
			return fmt.Errorf("stage task %s: uploading partition %d: %w", task.ID, p, uploadErr)
		}
		result.ResultFiles = append(result.ResultFiles, key)
		result.SizeBytes += fi.Size()
		_ = os.Remove(localPath)
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

	// KV fast-path: for small outputs, write only to NATS KV and skip the
	// S3 upload. Downstream cachedFileStreamSource checks KV first (via
	// natsKVKey(key)) and falls back to S3 on miss. This avoids the ~500ms
	// S3 round-trip per stage boundary for the many small intermediate
	// outputs in a typical TPC-H query (aggregates, merge-sort finals,
	// broadcast-join output).
	if e.resultKV != nil && len(payload) <= natsKVResultThreshold {
		kvKey := natsKVKey(key)
		if _, err := e.resultKV.Put(ctx, kvKey, payload); err == nil {
			result.ResultFiles = append(result.ResultFiles, key)
			result.SizeBytes += int64(len(payload))
			return nil
		}
		// KV put failed — fall through to S3.
	}

	_, err := e.store.Put(ctx, task.ResultBucket, key, bytes.NewReader(payload), int64(len(payload)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("stage task %s: uploading wshf: %w", task.ID, err)
	}
	result.ResultFiles = append(result.ResultFiles, key)
	result.SizeBytes += int64(len(payload))
	return nil
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
