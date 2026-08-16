package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// executeStage dispatches a TaskTypeStage task. Every stage now travels as
// a multi-operator fragment (task.Operators[]); this is just a thin wrapper
// around executeFragment.
func (e *Executor) executeStage(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Operators) == 0 {
		return fmt.Errorf("executeStage: empty Operators on task %s (StageType=%q) — every stage must dispatch as a fragment",
			task.ID, task.StageType)
	}
	return e.executeFragment(ctx, task, result)
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

	// Phase-B async upload: adopt + record + KV now, PUT in the background.
	// Adoption failure falls through to the synchronous body below.
	if root, asyncOK := e.asyncUploadEligible(&task); asyncOK {
		if job, ok := e.finishStageOutputAsync(ctx, &task, key, srcPath, size, false, result); ok {
			e.uploads.StartTask(root, task.ID, result.WorkerID, []uploadJob{job}, task.UploadPolicy)
			return nil
		}
	}

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

	sink := newGatherReplySink(e.nc, task.ReplySubject, result.WorkerID, nil).withDataPlane(e.dpClient)
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
		sink := newGatherReplySink(e.nc, task.ReplySubject, result.WorkerID, schema).withDataPlane(e.dpClient)
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
	// Per-partition accounting for coordinator-side skew detection. Rows
	// from the sink counters; bytes filled from the per-partition stats
	// below.
	result.PartitionRows = sink.PartitionRowCounts()
	result.PartitionBytes = make([]int64, len(result.PartitionRows))

	root, asyncOK := e.asyncUploadEligible(&task)
	var jobs []uploadJob
	defer func() {
		if len(jobs) > 0 {
			e.uploads.StartTask(root, task.ID, result.WorkerID, jobs, task.UploadPolicy)
		}
	}()
	for p, localPath := range sink.PartitionFiles() {
		if localPath == "" {
			continue
		}
		// Phase-B async upload: adopt + record + KV, PUT in the background.
		// Adoption failure falls through to the synchronous body below.
		if asyncOK {
			fi, statErr := os.Stat(localPath)
			if statErr != nil {
				return fmt.Errorf("stage task %s: stat partition %d: %w", task.ID, p, statErr)
			}
			result.PartitionBytes[p] = fi.Size()
			key := fmt.Sprintf("%spartition=%04d/%s.wshf", task.ResultPrefix, p, task.ID)
			if job, ok := e.finishStageOutputAsync(ctx, &task, key, localPath, fi.Size(), false, result); ok {
				jobs = append(jobs, job)
				continue
			}
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
		result.PartitionBytes[p] = fi.Size()

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

	// Phase-B async upload: stage the payload as a cache-adopted local file
	// (peers + background upload read it), record + KV now, PUT in the
	// background. Adoption failure falls through to the synchronous path.
	if root, asyncOK := e.asyncUploadEligible(&task); asyncOK {
		if adopted := e.cacheUnpartitionedLocal(task.QueryID, key, payload); adopted != "" {
			result.ResultFiles = append(result.ResultFiles, key)
			result.UploadPendingKeys = append(result.UploadPendingKeys, key)
			result.SizeBytes += int64(len(payload))
			if e.resultKV != nil && len(payload) <= natsKVResultThreshold {
				if _, kvErr := e.resultKV.Put(ctx, natsKVKey(key), payload); kvErr != nil {
					e.logger.Debug("KV cache write failed (upload pending, peers cover reads)",
						"task_id", task.ID, "key", key, "err", kvErr)
				}
			}
			e.uploads.StartTask(root, task.ID, result.WorkerID, []uploadJob{{
				bucket: task.ResultBucket, key: key, srcPath: adopted,
				compress: false, tmpDir: e.spillDir, size: int64(len(payload)),
			}}, task.UploadPolicy)
			return nil
		}
	}

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
// spill directory and adopts it into the LocalStageCache, returning the
// adopted path ("" on any failure). Best-effort for synchronous callers —
// an empty return leaves the cache cold and consumers fall through to S3.
// The Phase-B async path REQUIRES a non-empty return before skipping the
// synchronous upload (the adopted file is what the background upload
// reads).
func (e *Executor) cacheUnpartitionedLocal(queryID, key string, payload []byte) string {
	if e.localCache == nil || e.spillDir == "" {
		return ""
	}
	tmp, err := os.CreateTemp(e.spillDir, "stage-unpart-*.wshf")
	if err != nil {
		return ""
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return ""
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return ""
	}
	adopted := e.localCache.Adopt(queryID, key, tmpPath)
	if adopted == "" {
		_ = os.Remove(tmpPath)
	}
	return adopted
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
