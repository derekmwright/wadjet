package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

const inlineResultThreshold = 512 * 1024 // 512 KB — avoids S3 round-trip for small dimension tables and aggregation results

// natsKVResultThreshold is the max result size stored in NATS KV for
// cross-worker inter-stage transfer. Results below this threshold skip S3
// entirely, reducing inter-stage latency from ~500ms to ~10ms.
const natsKVResultThreshold = 4 * 1024 * 1024 // 4 MB — within NATS 8 MB max payload

// maxBufferedRows caps in-memory row accumulation during scan tasks to prevent
// unbounded memory growth. When this limit is reached, rows are flushed to the
// result file and the buffer is reused. Set to 0 for unlimited (legacy behavior).
const maxBufferedRows = 500_000

// Executor dispatches task types to the appropriate execution logic.
type Executor struct {
	store        objstore.Store
	js           jetstream.JetStream // for catalog access in pipeline tasks
	cache        *LRUCache
	resultStore  *ResultStore        // in-memory result passing between stages (nil = disabled)
	resultKV     jetstream.KeyValue  // NATS KV for cross-worker inter-stage results (nil = disabled)
	memoryBudget int64               // per-task memory budget in bytes (0 = unlimited)
	spillDir     string              // directory for spill files
	metrics      *metrics.Metrics
	logger       *slog.Logger
}

// NewExecutor creates a new task executor.
func NewExecutor(store objstore.Store, cache *LRUCache, js jetstream.JetStream) *Executor {
	return &Executor{store: store, js: js, cache: cache, logger: slog.Default()}
}

// SetMemoryBudget configures the per-task memory budget and spill directory.
func (e *Executor) SetMemoryBudget(budget int64, spillDir string) {
	e.memoryBudget = budget
	e.spillDir = spillDir
}

// SetResultStore attaches an in-memory result store for inter-stage result passing.
func (e *Executor) SetResultStore(rs *ResultStore) {
	e.resultStore = rs
}

// SetResultKV attaches a NATS KV store for cross-worker inter-stage result transfer.
// Results below natsKVResultThreshold are stored here instead of S3, reducing
// inter-stage latency from ~500ms (S3 round-trip) to ~10ms (NATS KV).
func (e *Executor) SetResultKV(kv jetstream.KeyValue) {
	e.resultKV = kv
}

// SetMetrics attaches Prometheus metrics for spill/memory tracking.
func (e *Executor) SetMetrics(m *metrics.Metrics) {
	e.metrics = m
}

// SetLogger sets the executor's logger.
func (e *Executor) SetLogger(l *slog.Logger) {
	e.logger = l
}

// newSpillManager creates a Tracker + SpillManager for a task if memory budget is configured.
// Returns nil, nil if budget is 0 (unlimited).
func (e *Executor) newSpillManager(taskID string) (*memory.SpillManager, *memory.Tracker) {
	if e.memoryBudget <= 0 {
		return nil, nil
	}

	tracker := memory.NewTracker(taskID, e.memoryBudget)

	dir := e.spillDir
	if dir == "" {
		dir = os.TempDir()
	}

	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		e.logger.Warn("failed to create spill manager, running without spill",
			"task_id", taskID, "error", err)
		return nil, tracker
	}

	return sm, tracker
}

// newSpillManagerScaled creates a Tracker + SpillManager with a budget scaled
// by the number of concurrent hash tables. Join tasks with N fused joins need
// N+1 hash tables in memory simultaneously during probing. Without scaling,
// each table gets 1/(N+1) of the budget, triggering premature spill.
func (e *Executor) newSpillManagerScaled(taskID string, joinCount int) (*memory.SpillManager, *memory.Tracker) {
	if e.memoryBudget <= 0 {
		return nil, nil
	}

	budget := e.memoryBudget
	if joinCount > 1 {
		budget = e.memoryBudget * int64(joinCount)
	}

	tracker := memory.NewTracker(taskID, budget)

	dir := e.spillDir
	if dir == "" {
		dir = os.TempDir()
	}

	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		e.logger.Warn("failed to create spill manager, running without spill",
			"task_id", taskID, "error", err)
		return nil, tracker
	}

	return sm, tracker
}

// Execute runs a task and returns the result notification.
func (e *Executor) Execute(ctx context.Context, task distributed.Task, workerID string) distributed.ResultNotification {
	start := time.Now()

	result := distributed.ResultNotification{
		TaskID:    task.ID,
		QueryID:   task.QueryID,
		StageID:   task.StageID,
		WorkerID:  workerID,
		Timestamp: time.Now(),
	}

	// Worker-side ABAC enforcement: validate column access policies before execution.
	if err := e.enforcePolicyDecision(task); err != nil {
		result.Duration = time.Since(start)
		result.Error = fmt.Sprintf("policy enforcement: %s", err)
		return result
	}

	peakTracker := newTaskPeakHeapTracker(ctx)

	var err error
	switch task.Type {
	case distributed.TaskTypePipeline:
		err = e.executePipeline(ctx, task, &result)
	case distributed.TaskTypeShuffle:
		err = e.executeShuffle(ctx, task, &result)
	default:
		err = fmt.Errorf("unsupported task type: %s", task.Type)
	}

	peakTracker.Stop()

	result.Duration = time.Since(start)
	if err != nil {
		result.Success = false
		result.Error = err.Error()
	} else {
		result.Success = true
	}

	// Ensure TaskStats is always populated (fallback for tasks without spill)
	if result.TaskStats == nil {
		result.TaskStats = &distributed.TaskStats{RSS: distributed.ProcessRSS()}
	}
	result.TaskStats.PeakHeapMB = peakTracker.PeakMB()

	return result
}

// enforcePolicyDecision validates ABAC column policies at the worker before
// task execution. If a denied column appears in the task's requested columns,
// the task is rejected. This provides defense-in-depth: the coordinator
// applies row filters at planning time, and the worker re-checks column
// policies at execution time.
func (e *Executor) enforcePolicyDecision(task distributed.Task) error {
	if len(task.PolicyDecisionJSON) == 0 {
		return nil
	}
	var sd auth.SerializedDecision
	if err := json.Unmarshal(task.PolicyDecisionJSON, &sd); err != nil {
		return fmt.Errorf("unmarshaling policy decision: %w", err)
	}
	if !sd.Allowed {
		return fmt.Errorf("access denied by policy")
	}

	// Check column-level policies for the task's target table
	tableName := task.TableName
	if tableName == "" {
		return nil // non-table tasks (aggregate, sort, etc.) don't need column checks
	}
	td, ok := sd.TableDecisions[tableName]
	if !ok || td == nil {
		return nil
	}
	if !td.Allowed {
		return fmt.Errorf("access denied for table %q: %s", tableName, td.Reason)
	}

	// Check each requested column against column-level decisions
	requestedCols := make(map[string]bool, len(task.Columns))
	for _, c := range task.Columns {
		requestedCols[c] = true
	}
	for _, cd := range td.Columns {
		if !cd.Allowed && requestedCols[cd.Column] {
			return fmt.Errorf("access denied for column %q in table %q", cd.Column, tableName)
		}
	}
	if e.logger != nil {
		e.logger.Debug("worker policy enforcement passed",
			"task_id", task.ID,
			"table", tableName,
			"columns", len(td.Columns),
		)
	}
	return nil
}

func (e *Executor) executePipeline(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if task.SQLText == "" {
		return fmt.Errorf("pipeline task missing SQL text")
	}
	if e.js == nil {
		return fmt.Errorf("pipeline task requires JetStream for catalog access")
	}

	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	// Create a catalog from NATS KV (same metadata the coordinator uses).
	// Wrap the object store with CachedStore so that scanners benefit from
	// the worker's cross-query LRU file cache instead of re-reading S3.
	kv, err := catalog.NewNATSKV(e.js)
	if err != nil {
		return fmt.Errorf("creating catalog KV: %w", err)
	}
	cachedStore := NewCachedStore(e.store, e.cache)
	cat := catalog.New(kv, cachedStore, bucket)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Parse SQL
	parsed, err := plansql.Parse(task.SQLText)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	// Build and optimize logical plan
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		return fmt.Errorf("logical plan: %w", err)
	}
	planner := physical.NewPlanner(cat)
	planner.AnnotateScanColumns(ctx, logicalPlan)
	scanAnnotator := func(plan *logical.Node) {
		planner.AnnotateScanColumns(ctx, plan)
	}
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	// Shuffle-distributed aggregate (spec: 2026-04-18-shuffle-distributed-
	// aggregate.md): when the coordinator pre-computed derived aggregate
	// subplans (e.g. Q17's decorrelated inner AVG-per-partkey), walk the
	// logical plan and replace each matching Aggregate subtree with a
	// synthetic scan of the cache files. Must run AFTER optimize (so the
	// decorrelator has landed the aggregate) and BEFORE physical planning
	// (so the scan substitution is visible to buildScan).
	precompAliasFiles := make(map[string][]string)
	if len(task.PreComputedAggregates) > 0 {
		sigs := make([]logical.PreComputedAggregate, 0, len(task.PreComputedAggregates))
		for i, pa := range task.PreComputedAggregates {
			alias := fmt.Sprintf("__precomp_agg_%d", i)
			aggOut := make([]string, len(pa.AggSpecs))
			for j, spec := range pa.AggSpecs {
				aggOut[j] = spec.OutputCol
			}
			sigs = append(sigs, logical.PreComputedAggregate{
				InputTable:     pa.InputTable,
				GroupByCols:    pa.GroupByCols,
				AggOutputCols:  aggOut,
				SyntheticAlias: alias,
			})
			precompAliasFiles[alias] = pa.CacheFiles
		}
		used, subErr := logical.SubstitutePreComputedAggregates(logicalPlan, sigs)
		if subErr != nil {
			return fmt.Errorf("substitute pre-computed aggregates: %w", subErr)
		}
		for alias := range precompAliasFiles {
			if !used[alias] {
				// Signature didn't match — fine, falls back to in-plan execution.
				// Drop the unused alias so it doesn't pollute StreamingSources.
				delete(precompAliasFiles, alias)
			}
		}
		e.logger.Info("pre-computed aggregate substitution",
			"sig_count", len(sigs), "matched", len(used), "unmatched_aliases_dropped", len(sigs)-len(used))
	}

	// Build standalone physical plan (single pipeline, no stages).
	// Set memory budget and spill directory so the planner can install spill
	// managers on pipeline-breaking operators. Without this, concurrent pipeline
	// tasks bypass memory tracking and risk OOM under multi-join pressure.
	if e.memoryBudget > 0 {
		planner.MemoryBudget = e.memoryBudget
	}
	if e.spillDir != "" {
		planner.SpillDir = e.spillDir
	}

	// Scan-split pipeline mode: create lazy streaming sources for pre-scanned
	// build-cache files. Each source downloads and parses files one at a time,
	// yielding batches on demand. This avoids materializing the entire build
	// side into memory — the hash join's grace spill handles memory pressure.
	if len(task.PreScannedInputs) > 0 || len(precompAliasFiles) > 0 {
		streamingSources := make(map[string]exec.Source, len(task.PreScannedInputs)+len(precompAliasFiles))
		for tableName, files := range task.PreScannedInputs {
			streamingSources[tableName] = newCachedFileStreamSource(e, bucket, files)
			e.logger.Debug("streaming pre-scanned input",
				"table", tableName, "files", len(files))
		}
		for alias, files := range precompAliasFiles {
			streamingSources[alias] = newCachedFileStreamSource(e, bucket, files)
			e.logger.Debug("streaming pre-computed aggregate",
				"alias", alias, "files", len(files))
		}
		planner.StreamingSources = streamingSources
	}

	// Probe-split pipeline mode: restrict scan files for the probe table.
	// Each worker reads its assigned partition of the probe table while
	// scanning build tables in full.
	if len(task.ScanFileFilter) > 0 {
		planner.ScanFileFilter = task.ScanFileFilter
		e.logger.Debug("probe-split scan file filter",
			"aliases", len(task.ScanFileFilter))
	}

	// Partial aggregate mode: strip top Sort+Limit so each worker produces
	// complete partial aggregates. The coordinator merges and applies final
	// ordering.
	if task.PartialAggregate {
		logicalPlan = logical.StripTopSortLimit(logicalPlan)
		e.logger.Debug("stripped top sort/limit for partial aggregate")
	}

	physPlan, err := planner.Plan(ctx, logicalPlan)
	if err != nil {
		return fmt.Errorf("physical plan: %w", err)
	}
	if physPlan.Cleanup != nil {
		defer physPlan.Cleanup()
	}

	pipeline := physPlan.Pipeline
	if pipeline == nil {
		return nil
	}

	// Build-cache pre-scan tasks (StageID == "build-cache-scan") read whole
	// tables that may not fit in worker memory. Replace the default CollectSink
	// with a streaming shuffle sink that writes each batch to a spill file as
	// it arrives, so memory stays bounded by one batch instead of growing to
	// the full table size. Without this, scanning partsupp at SF100 (~12GB)
	// peaks at ~24GB during serializeBatches and OOM-kills the worker.
	// Build-cache and aggregate-cache pre-scans both produce a cached .wshf
	// file that downstream probe tasks stream from via StreamingSources.
	// They share the streaming-sink path to produce the WSHF format that
	// cachedFileStreamSource expects; the default result path writes WSHC
	// (compressed) which would fail with "invalid shuffle magic".
	if (task.StageID == "build-cache-scan" || task.StageID == "aggregate-cache-compute") && e.spillDir != "" {
		return e.executeBuildCachePreScan(ctx, task, pipeline, result)
	}

	// Execute the pipeline — same path as standalone mode
	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("pipeline execution: %w", err)
	}

	// Collect results from the pipeline's sink
	collectSink, ok := pipeline.Sink.(*exec.CollectSink)
	if !ok {
		return fmt.Errorf("pipeline sink is not CollectSink")
	}
	batches := collectSink.Batches()
	var totalRows int64
	for _, b := range batches {
		totalRows += int64(b.ActiveLen())
	}

	e.logger.Info("pipeline task completed",
		"task_id", task.ID,
		"sql_length", len(task.SQLText),
		"rows", totalRows,
		"batches", len(batches),
	)

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, batches, result)
}

// executeBuildCachePreScan runs a build-cache pre-scan pipeline with a
// streaming sink that writes batches to a local spill file as they arrive,
// then uploads the file to S3. Avoids the OOM that the default CollectSink
// path triggers when scanning very large build tables.
func (e *Executor) executeBuildCachePreScan(ctx context.Context, task distributed.Task, pipeline *exec.Pipeline, result *distributed.ResultNotification) error {
	streamSink := newShuffleStreamSink(e.spillDir)

	// Replace CollectSink with the streaming sink. The pipeline doesn't care
	// what sink it has — it just calls Consume on each batch.
	//
	// IMPORTANT: do NOT call streamSink.Init here. Pipeline.Run will call
	// p.Sink.Init(ctx) itself; calling it twice would create two temp files
	// (the first one then orphaned and uploaded as a 0-byte blob).
	pipeline.Sink = streamSink

	// Make sure we always close the file handle and remove the spill file,
	// even on error paths. spillPath is captured AFTER Run (after Init).
	defer func() {
		_ = streamSink.Close()
		if path := streamSink.FilePath(); path != "" {
			_ = os.Remove(path)
		}
	}()

	if err := pipeline.Run(ctx); err != nil {
		return fmt.Errorf("build cache pre-scan pipeline: %w", err)
	}

	spillPath := streamSink.FilePath()
	totalRows := streamSink.NumRows()
	e.logger.Info("build cache pre-scan completed",
		"task_id", task.ID,
		"table_sql", task.SQLText,
		"rows", totalRows,
		"spill_file", spillPath,
	)

	result.NumRows = totalRows
	if totalRows == 0 {
		// Empty table: nothing to upload. Coordinator handles the no-rows case.
		return nil
	}
	if spillPath == "" {
		return fmt.Errorf("build cache pre-scan reported %d rows but no spill file path", totalRows)
	}

	// Re-open the spill file for upload (the writer keeps an fd, but the
	// streaming Put needs its own reader positioned at the start).
	f, err := os.Open(spillPath)
	if err != nil {
		return fmt.Errorf("opening spill file for upload: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat spill file: %w", err)
	}
	if fi.Size() == 0 {
		return fmt.Errorf("build cache pre-scan reported %d rows but spill file is empty", totalRows)
	}
	resultPath := task.ResultPrefix + task.ID + ".wshf"
	if _, err := e.store.Put(ctx, task.ResultBucket, resultPath, f, fi.Size(), "application/octet-stream"); err != nil {
		return fmt.Errorf("uploading build cache result to S3: %w", err)
	}
	result.ResultPath = resultPath
	result.SizeBytes = fi.Size()
	return nil
}

// executeShuffle reads source Parquet files from S3, hash-partitions every row
// on task.ShuffleKeys into task.NumPartitions output .wshf files, and uploads
// each non-empty partition file to S3 under
//
//	<ResultBucket>/<ResultPrefix>/partition=NNNN/<TaskID>.wshf
//
// Populated result fields: ResultFiles, NumRows, SizeBytes.
func (e *Executor) executeShuffle(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.ShuffleKeys) == 0 {
		return fmt.Errorf("shuffle task %s: ShuffleKeys must not be empty", task.ID)
	}
	if task.NumPartitions <= 0 {
		return fmt.Errorf("shuffle task %s: NumPartitions must be > 0, got %d", task.ID, task.NumPartitions)
	}
	if len(task.Files) == 0 {
		return fmt.Errorf("shuffle task %s: Files must not be empty", task.ID)
	}

	bucket := task.DataBucket
	if bucket == "" {
		bucket = task.ResultBucket
	}

	// Read all source Parquet files into batches. Uses the executor's
	// existing range-read + LRU cache path; no planner/catalog needed.
	batches, err := e.readParquetFilesConcurrentBatches(ctx, bucket, task.Files, task.Columns)
	if err != nil {
		return fmt.Errorf("shuffle task %s: reading source files: %w", task.ID, err)
	}
	if len(batches) == 0 {
		// Source files produced no rows — nothing to upload.
		return nil
	}

	// Extract schema from first non-empty batch.
	var schema []parquet.Column
	for _, b := range batches {
		if b != nil && len(b.Schema) > 0 {
			schema = b.Schema
			break
		}
	}
	if schema == nil {
		return fmt.Errorf("shuffle task %s: could not determine schema from source batches", task.ID)
	}

	// Set up the spill directory for the sink's partition files.
	spillDir := filepath.Join(e.spillDir, "shuffle-"+task.ID)
	if e.spillDir == "" {
		spillDir = filepath.Join(os.TempDir(), "shuffle-"+task.ID)
	}
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return fmt.Errorf("shuffle task %s: creating spill dir: %w", task.ID, err)
	}
	defer os.RemoveAll(spillDir)

	sink := newPartitionedShuffleSink(spillDir, task.ShuffleKeys, task.NumPartitions, schema)
	if err := sink.Init(ctx); err != nil {
		return fmt.Errorf("shuffle task %s: init sink: %w", task.ID, err)
	}
	defer sink.Close()

	// Feed all batches into the sink and count rows.
	var totalRows int64
	for _, b := range batches {
		if b == nil {
			continue
		}
		n := int64(b.ActiveLen())
		if n == 0 {
			continue
		}
		if err := sink.Consume(ctx, b); err != nil {
			return fmt.Errorf("shuffle task %s: consuming batch: %w", task.ID, err)
		}
		totalRows += n
	}

	if err := sink.Finalize(ctx); err != nil {
		return fmt.Errorf("shuffle task %s: finalizing sink: %w", task.ID, err)
	}

	// Upload each non-empty partition file to S3.
	partFiles := sink.PartitionFiles()
	for p, localPath := range partFiles {
		if localPath == "" {
			continue // empty partition
		}
		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("shuffle task %s: opening partition %d: %w", task.ID, p, err)
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("shuffle task %s: stat partition %d: %w", task.ID, p, err)
		}

		key := fmt.Sprintf("%spartition=%04d/%s.wshf", task.ResultPrefix, p, task.ID)
		_, uploadErr := e.store.Put(ctx, task.ResultBucket, key, f, fi.Size(), "application/octet-stream")
		f.Close()
		if uploadErr != nil {
			return fmt.Errorf("shuffle task %s: uploading partition %d: %w", task.ID, p, uploadErr)
		}

		result.ResultFiles = append(result.ResultFiles, key)
		// SizeBytes uses the local file size; the object store does not re-encode
		// .wshf data so this matches the uploaded byte count.
		result.SizeBytes += fi.Size()

		// Remove local file best-effort.
		if removeErr := os.Remove(localPath); removeErr != nil {
			e.logger.Warn("shuffle: failed to remove local partition file",
				"task_id", task.ID, "partition", p, "path", localPath, "error", removeErr)
		}
	}

	result.NumRows = totalRows

	e.logger.Info("shuffle task completed",
		"task_id", task.ID,
		"rows", totalRows,
		"partitions", len(result.ResultFiles),
		"size_bytes", result.SizeBytes,
	)
	return nil
}

// getFileData retrieves raw Parquet bytes with 3-tier caching:
// in-memory result store → LRU cache → object store (S3).
func (e *Executor) getFileData(ctx context.Context, bucket, path string) ([]byte, error) {
	// Tier 1: in-memory result store (same-worker, fastest)
	if e.resultStore != nil {
		if data, ok := e.resultStore.Get(path); ok {
			return data, nil
		}
	}

	// Tier 2: NATS KV result store (cross-worker, ~10ms vs ~500ms for S3)
	if e.resultKV != nil {
		kvKey := natsKVKey(path)
		if entry, kvErr := e.resultKV.Get(ctx, kvKey); kvErr == nil {
			data := entry.Value()
			// Populate LRU cache for subsequent reads
			e.cache.Put(bucket+"/"+path, data)
			return data, nil
		}
	}

	// Tier 3: LRU cache (cached S3 reads)
	cacheKey := bucket + "/" + path
	if data, ok := e.cache.Get(cacheKey); ok {
		return data, nil
	}

	// Tier 4: S3 object store (slowest, ~250-500ms)
	rc, _, err := e.store.Get(ctx, bucket, path)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	// Cache the file data
	e.cache.Put(cacheKey, data)
	return data, nil
}

// readParquetFileBatches reads a Parquet file directly into columnar RecordBatches,
// bypassing the map[string]any intermediate. One batch per row group.
// When the store supports range reads and column projection is active, uses
// lazy io.ReaderAt to fetch only the needed column chunks from S3 (5-10x I/O
// reduction on wide tables).
func (e *Executor) readParquetFileBatches(ctx context.Context, bucket, path string, selectedCols []string) ([]*batch.RecordBatch, error) {
	// Check LRU cache first — if the full file is cached, use it directly.
	cacheKey := bucket + "/" + path
	if data, ok := e.cache.Get(cacheKey); ok {
		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, err
		}
		return scan.ReadFileBatches(reader, reader.Schema().Columns, selectedCols)
	}

	// For column-pruned queries, use range reads to fetch only needed chunks.
	// This avoids downloading the full file when only a few columns are needed.
	if len(selectedCols) > 0 {
		if ras, ok := e.store.(objstore.ReaderAtStore); ok {
			ra, size, err := ras.GetReaderAt(ctx, bucket, path)
			if err == nil {
				defer ra.Close()
				reader, err := parquet.NewReader(ra, size)
				if err != nil {
					return nil, fmt.Errorf("opening parquet via range read: %w", err)
				}
				return scan.ReadFileBatches(reader, reader.Schema().Columns, selectedCols)
			}
			// Fall through to full download on GetReaderAt error.
		}
	}

	// Fallback: full file download + cache.
	data, err := e.getFileData(ctx, bucket, path)
	if err != nil {
		return nil, err
	}
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return scan.ReadFileBatches(reader, reader.Schema().Columns, selectedCols)
}

// readParquetFilesConcurrentBatches reads multiple Parquet files in parallel (up to 8
// goroutines), returning all batches concatenated in file order.
func (e *Executor) readParquetFilesConcurrentBatches(ctx context.Context, bucket string, files []string, selectedCols []string) ([]*batch.RecordBatch, error) {
	if len(files) == 0 {
		return nil, nil
	}
	if len(files) == 1 {
		return e.readParquetFileBatches(ctx, bucket, files[0], selectedCols)
	}

	type result struct {
		batches []*batch.RecordBatch
		err     error
	}
	results := make([]result, len(files))

	const maxConcurrency = 8
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, f := range files {
		wg.Add(1)
		go func(idx int, filePath string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			batches, err := e.readParquetFileBatches(ctx, bucket, filePath, selectedCols)
			results[idx] = result{batches: batches, err: err}
		}(i, f)
	}
	wg.Wait()

	var allBatches []*batch.RecordBatch
	for i, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("reading file %s: %w", files[i], r.err)
		}
		allBatches = append(allBatches, r.batches...)
	}
	return allBatches, nil
}

// readInputFilesBatches reads files that may be in binary shuffle format (.wshf)
// or Parquet format, auto-detecting based on file magic bytes.
func (e *Executor) readInputFilesBatches(ctx context.Context, bucket string, files []string, selectedCols []string) ([]*batch.RecordBatch, error) {
	if len(files) == 0 {
		return nil, nil
	}

	type result struct {
		batches []*batch.RecordBatch
		err     error
	}
	results := make([]result, len(files))

	const maxConcurrency = 8
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, f := range files {
		wg.Add(1)
		go func(idx int, filePath string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			data, err := e.getFileData(ctx, bucket, filePath)
			if err != nil {
				results[idx] = result{err: err}
				return
			}

			data, decErr := DecompressShuffleData(data)
			if decErr != nil {
				results[idx] = result{err: decErr}
				return
			}
			if isShuffleFormat(data) {
				batches, err := shuffleReadBatches(data)
				results[idx] = result{batches: batches, err: err}
			} else {
				reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					results[idx] = result{err: err}
					return
				}
				schema := reader.Schema().Columns
				batches, err := scan.ReadFileBatches(reader, schema, selectedCols)
				results[idx] = result{batches: batches, err: err}
			}
		}(i, f)
	}
	wg.Wait()

	var allBatches []*batch.RecordBatch
	for i, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("reading file %s: %w", files[i], r.err)
		}
		allBatches = append(allBatches, r.batches...)
	}
	return allBatches, nil
}

// readParquetFilesConcurrent reads multiple Parquet files in parallel (up to 8
// goroutines), returning all rows concatenated in file order. This significantly
// reduces latency for S3-backed reads where each GET is a network round-trip.
func (e *Executor) serializeBatches(batches []*batch.RecordBatch) ([]byte, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to serialize")
	}

	schema := batches[0].Schema

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		return nil, fmt.Errorf("writing header: %w", err)
	}
	for _, b := range batches {
		nRows := b.ActiveLen()
		if nRows == 0 {
			continue
		}
		if b.Sel != nil {
			if err := sw.writeChunk(b.Columns, b.Sel, nRows); err != nil {
				return nil, fmt.Errorf("writing chunk: %w", err)
			}
		} else {
			if err := sw.writeChunk(b.Columns, nil, nRows); err != nil {
				return nil, fmt.Errorf("writing chunk: %w", err)
			}
		}
	}

	// Patch chunk count
	data := buf.Bytes()
	if len(data) >= 8 {
		binary.LittleEndian.PutUint32(data[4:8], sw.numChunks)
	}

	// Compress for inter-node transfer
	return CompressShuffleData(data), nil
}

// writeBatchResult serializes batches and writes via inline/ResultStore/S3 tiering.
func (e *Executor) writeBatchResult(ctx context.Context, task distributed.Task, batches []*batch.RecordBatch, result *distributed.ResultNotification) error {
	data, err := e.serializeBatches(batches)
	if err != nil {
		return err
	}

	result.SizeBytes = int64(len(data))

	// Small result fast path: include inline
	if len(data) <= inlineResultThreshold {
		result.InlineData = data
		return nil
	}

	resultPath := task.ResultPrefix + task.ID + ".wshf"

	// Always write to S3 as the durable store. NATS KV has a 5-minute TTL
	// and 1 GB size cap — entries can expire or be evicted before downstream
	// stages read them (e.g., SF100 Q04 pipeline takes 11+ minutes).
	_, err = e.store.Put(ctx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("writing result to S3: %w", err)
	}
	result.ResultPath = resultPath

	// Also populate NATS KV as a fast read cache for cross-worker reads.
	// Workers check KV (tier 2, ~10ms) before falling back to S3 (~500ms).
	if e.resultKV != nil && len(data) <= natsKVResultThreshold {
		kvKey := natsKVKey(resultPath)
		e.resultKV.Put(ctx, kvKey, data) // best-effort; S3 is the source of truth
	}

	// Cache locally for same-node reads.
	if e.resultStore != nil {
		e.resultStore.Put(task.QueryID, resultPath, data)
	}
	return nil
}

// natsKVKey converts an S3 result path to a valid NATS KV key.
// NATS KV keys don't support '.' so we replace with '_'.
func natsKVKey(path string) string {
	return strings.ReplaceAll(path, ".", "_")
}

// batchSource wraps a slice of RecordBatches as an exec.Source.
func (e *Executor) collectTaskStats(spill *memory.SpillManager, tracker *memory.Tracker) *distributed.TaskStats {
	stats := &distributed.TaskStats{
		RSS: distributed.ProcessRSS(),
	}

	if spill != nil {
		files := spill.SpilledFiles()
		stats.SpillFiles = len(files)
		for _, f := range files {
			if info, err := os.Stat(f); err == nil {
				stats.SpillBytes += info.Size()
			}
		}
		if e.metrics != nil && stats.SpillFiles > 0 {
			e.metrics.SpillEvents.Add(float64(stats.SpillFiles))
			e.metrics.SpillBytesWritten.Add(float64(stats.SpillBytes))
		}
	}

	if tracker != nil {
		stats.MemUsed = tracker.Used()
		stats.MemBudget = tracker.Budget()
		if e.metrics != nil {
			e.metrics.MemoryBudgetBytes.Set(float64(stats.MemBudget))
			e.metrics.MemoryUsedBytes.Set(float64(stats.MemUsed))
		}
	}

	return stats
}

// aggregateNeededCols returns the minimal set of columns needed for an
// aggregate task: group-by columns + aggregate input columns. Extracts raw
// column references from expression strings (e.g., "substr(l_shipdate, 1, 4)"
// → "l_shipdate"). Returns nil (read all) if no columns are specified.

// readFileBytes reads a file from the object store into memory.
func (e *Executor) readFileBytes(ctx context.Context, bucket, path string) ([]byte, error) {
	reader, _, err := e.store.Get(ctx, bucket, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
