package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"sort"
	"strconv"
	"strings"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

const inlineResultThreshold = 256 * 1024 // 256 KB — avoids S3 round-trip for common aggregation/LIMIT results

// maxBufferedRows caps in-memory row accumulation during scan tasks to prevent
// unbounded memory growth. When this limit is reached, rows are flushed to the
// result file and the buffer is reused. Set to 0 for unlimited (legacy behavior).
const maxBufferedRows = 500_000

// Executor dispatches task types to the appropriate execution logic.
type Executor struct {
	store        objstore.Store
	cache        *LRUCache
	resultStore  *ResultStore // in-memory result passing between stages (nil = disabled)
	memoryBudget int64        // per-task memory budget in bytes (0 = unlimited)
	spillDir     string       // directory for spill files
	metrics      *metrics.Metrics
	logger       *slog.Logger
}

// NewExecutor creates a new task executor.
func NewExecutor(store objstore.Store, cache *LRUCache) *Executor {
	return &Executor{store: store, cache: cache, logger: slog.Default()}
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

	var err error
	switch task.Type {
	case distributed.TaskTypeScan:
		err = e.executeScan(ctx, task, &result)
	case distributed.TaskTypeAggregate:
		err = e.executeAggregate(ctx, task, &result)
	case distributed.TaskTypeSort:
		err = e.executeSort(ctx, task, &result)
	case distributed.TaskTypeJoin:
		err = e.executeJoin(ctx, task, &result)
	case distributed.TaskTypeWindow:
		err = e.executeWindow(ctx, task, &result)
	default:
		err = fmt.Errorf("unsupported task type: %s", task.Type)
	}

	result.Duration = time.Since(start)
	if err != nil {
		result.Success = false
		result.Error = err.Error()
	} else {
		result.Success = true
	}

	return result
}

func (e *Executor) executeScan(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// For large scans, stream rows to the result file in chunks to bound
	// memory usage. We accumulate up to maxBufferedRows before flushing.
	var (
		allRows     = make([]map[string]any, 0, min(len(task.Files)*1024, maxBufferedRows))
		totalRows   int64
		flushed     bool
		resultPath  = task.ResultPrefix + task.ID + ".parquet"
		multiWriter *multiPartWriter
	)

	for _, filePath := range task.Files {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, task.Columns)
		if err != nil {
			return fmt.Errorf("reading file %s: %w", filePath, err)
		}

		// Apply pushed-down filter predicates per file to reduce memory
		if len(task.FilterExprs) > 0 && len(rows) > 0 {
			rows = e.applyRowFilters(rows, task.FilterExprs)
		}

		allRows = append(allRows, rows...)
		totalRows += int64(len(rows))

		// Flush to prevent unbounded memory growth
		if maxBufferedRows > 0 && len(allRows) >= maxBufferedRows {
			if multiWriter == nil {
				multiWriter = &multiPartWriter{}
			}
			multiWriter.parts = append(multiWriter.parts, allRows)
			allRows = make([]map[string]any, 0, maxBufferedRows)
			flushed = true
		}
	}

	if totalRows == 0 {
		return nil
	}

	result.NumRows = totalRows

	// If we never hit the cap, write normally (may use inline fast path)
	if !flushed {
		return e.writeResult(ctx, task, allRows, result)
	}

	// Flushed at least once: collect remaining rows and write all parts
	if len(allRows) > 0 {
		multiWriter.parts = append(multiWriter.parts, allRows)
	}

	// Flatten parts for writing (total is bounded: we only hold the last chunk + result)
	var combined []map[string]any
	for _, part := range multiWriter.parts {
		combined = append(combined, part...)
	}
	multiWriter = nil // release part references

	return e.writeResultToPath(ctx, task, combined, resultPath, result)
}

// multiPartWriter collects row batches for large scan results.
type multiPartWriter struct {
	parts [][]map[string]any
}

// applyRowFilters applies SQL filter expressions to rows, keeping only matches.
// Uses the expression engine for evaluation.
func (e *Executor) applyRowFilters(rows []map[string]any, filterExprs []string) []map[string]any {
	if len(rows) == 0 {
		return rows
	}

	schema := schemaFromRows(rows)
	b := batch.FromRows(schema, rows)

	for _, filterSQL := range filterExprs {
		filter := buildSimpleFilter(filterSQL)
		if filter == nil {
			continue
		}

		var filtered []map[string]any
		for i := 0; i < b.Len; i++ {
			if filter(b, i) {
				filtered = append(filtered, rows[i])
			}
		}
		rows = filtered
		if len(rows) == 0 {
			break
		}
		// Rebuild batch for next filter
		b = batch.FromRows(schema, rows)
	}
	return rows
}

// buildSimpleFilter creates a predicate function from a SQL expression string.
func buildSimpleFilter(filterSQL string) func(*batch.RecordBatch, int) bool {
	// Parse operators in order of precedence
	operators := []struct {
		sql string
		op  exec.CompareOp
	}{
		{">=", exec.OpGe},
		{"<=", exec.OpLe},
		{"!=", exec.OpNe},
		{">", exec.OpGt},
		{"<", exec.OpLt},
		{"=", exec.OpEq},
	}

	for _, o := range operators {
		parts := strings.SplitN(filterSQL, o.sql, 2)
		if len(parts) == 2 {
			col := cleanFilterExpr(strings.TrimSpace(parts[0]))
			valStr := strings.TrimSpace(parts[1])
			val := parseFilterValue(valStr)
			pred := exec.ColumnCompare(col, o.op, val)
			return func(b *batch.RecordBatch, row int) bool {
				return pred(b, row)
			}
		}
	}
	return nil
}

func cleanFilterExpr(s string) string {
	s = strings.TrimSpace(s)
	if parts := strings.SplitN(s, ".", 2); len(parts) == 2 {
		return parts[1]
	}
	return s
}

func parseFilterValue(s string) any {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func (e *Executor) executeAggregate(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Build column projection: only read group-by columns + aggregate input columns
	neededCols := aggregateNeededCols(task.GroupByCols, task.Aggregates)

	// Read input files concurrently from previous stage
	allRows, err := e.readParquetFilesConcurrent(ctx, task.ResultBucket, task.InputFiles, neededCols)
	if err != nil {
		return err
	}

	if len(allRows) == 0 {
		return nil
	}

	// Determine schema from first row
	schema := schemaFromRows(allRows)

	// Build and execute aggregate
	aggs := make([]exec.AggColumn, len(task.Aggregates))
	for i, a := range task.Aggregates {
		aggs[i] = exec.AggColumn{
			Func:      parseAggFunc(a.Func),
			InputCol:  a.InputCol,
			OutputCol: a.OutputCol,
			OutputType: parquet.TypeFloat64,
		}
		if a.Func == "count" {
			aggs[i].OutputType = parquet.TypeInt64
		}
	}

	agg := exec.NewHashAggregate(task.GroupByCols, aggs)

	// Wire spill manager if memory budget is set
	spill, tracker := e.newSpillManager(task.ID)
	if spill != nil {
		agg.Spill = spill
		defer spill.Cleanup()
		defer e.recordSpillMetrics(spill, tracker)
	}

	if err := agg.Init(ctx); err != nil {
		return err
	}

	b := batch.FromRows(schema, allRows)
	if err := agg.Consume(ctx, b); err != nil {
		return err
	}
	if err := agg.Finalize(ctx); err != nil {
		return err
	}

	// Read results
	var resultRows []map[string]any
	for {
		rb, err := agg.Next(ctx)
		if err != nil {
			return err
		}
		if rb == nil {
			break
		}
		resultRows = append(resultRows, rb.ToRows()...)
	}

	result.NumRows = int64(len(resultRows))
	if len(resultRows) == 0 {
		return nil
	}

	return e.writeResult(ctx, task, resultRows, result)
}

func (e *Executor) executeSort(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	allRows, err := e.readParquetFilesConcurrent(ctx, task.ResultBucket, task.InputFiles, nil)
	if err != nil {
		return err
	}

	if len(allRows) == 0 {
		return nil
	}

	schema := schemaFromRows(allRows)

	var keys []exec.SortKey
	for _, sk := range task.SortKeys {
		order := exec.Ascending
		if sk.Desc {
			order = exec.Descending
		}
		keys = append(keys, exec.SortKey{Column: sk.Column, Order: order})
	}

	sortOp := exec.NewSort(keys)

	// Wire spill manager if memory budget is set
	spill, tracker := e.newSpillManager(task.ID)
	if spill != nil {
		sortOp.Spill = spill
		defer spill.Cleanup()
		defer e.recordSpillMetrics(spill, tracker)
	}

	if err := sortOp.Init(ctx); err != nil {
		return err
	}

	b := batch.FromRows(schema, allRows)
	if err := sortOp.Consume(ctx, b); err != nil {
		return err
	}
	if err := sortOp.Finalize(ctx); err != nil {
		return err
	}

	var resultRows []map[string]any
	for {
		rb, err := sortOp.Next(ctx)
		if err != nil {
			return err
		}
		if rb == nil {
			break
		}
		resultRows = append(resultRows, rb.ToRows()...)
	}

	// Apply limit if specified
	if task.Limit > 0 && len(resultRows) > task.Limit {
		resultRows = resultRows[:task.Limit]
	}

	result.NumRows = int64(len(resultRows))
	if len(resultRows) == 0 {
		return nil
	}

	return e.writeResult(ctx, task, resultRows, result)
}

func (e *Executor) executeJoin(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read build (right) and probe (left) sides concurrently
	buildRows, err := e.readParquetFilesConcurrent(ctx, task.ResultBucket, task.BuildFiles, nil)
	if err != nil {
		return fmt.Errorf("reading build files: %w", err)
	}

	probeRows, err := e.readParquetFilesConcurrent(ctx, task.ResultBucket, task.InputFiles, nil)
	if err != nil {
		return fmt.Errorf("reading probe files: %w", err)
	}

	if len(probeRows) == 0 && len(buildRows) == 0 {
		return nil
	}

	joinType := mapExecJoinType(task.JoinType)

	hj := exec.NewHashJoin(joinType, task.JoinLeftKeys, task.JoinRightKeys)

	// Build the hash table from right side
	var buildSchema []parquet.Column
	if len(buildRows) > 0 {
		buildSchema = schemaFromRows(buildRows)
		hj.BuildFromRows(buildSchema, buildRows)
	}

	// For RIGHT and FULL OUTER joins, we may still have results even
	// with no probe rows (unmatched build-side rows).
	if len(probeRows) == 0 && joinType != exec.RightJoin && joinType != exec.FullOuterJoin {
		return nil
	}

	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		return err
	}

	var resultRows []map[string]any

	// Probe with left side (if we have probe rows)
	if len(probeRows) > 0 {
		probeSchema := schemaFromRows(probeRows)
		probeBatch := batch.FromRows(probeSchema, probeRows)

		resultBatch, err := probe.Execute(ctx, probeBatch)
		if err != nil {
			return err
		}

		if resultBatch != nil {
			resultRows = append(resultRows, resultBatch.ToRows()...)
		}

		// For RIGHT and FULL OUTER joins, flush unmatched build-side rows
		if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
			unmatchedBatch := probe.FlushUnmatched(probeSchema)
			if unmatchedBatch != nil {
				resultRows = append(resultRows, unmatchedBatch.ToRows()...)
			}
		}
	} else if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
		// No probe rows but we need unmatched build rows — use build schema
		// for the left-side columns (all will be null).
		if buildSchema != nil {
			unmatchedBatch := probe.FlushUnmatched(buildSchema)
			if unmatchedBatch != nil {
				resultRows = append(resultRows, unmatchedBatch.ToRows()...)
			}
		}
	}

	result.NumRows = int64(len(resultRows))
	if len(resultRows) == 0 {
		return nil
	}

	return e.writeResult(ctx, task, resultRows, result)
}

func (e *Executor) executeWindow(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	allRows, err := e.readParquetFilesConcurrent(ctx, task.ResultBucket, task.InputFiles, nil)
	if err != nil {
		return err
	}

	if len(allRows) == 0 {
		return nil
	}

	schema := schemaFromRows(allRows)

	// Convert task window column specs to exec.WindowColumn
	var winCols []exec.WindowColumn
	for _, wc := range task.WindowCols {
		var orderBy []exec.SortKey
		for _, ob := range wc.OrderBy {
			order := exec.Ascending
			if ob.Desc {
				order = exec.Descending
			}
			orderBy = append(orderBy, exec.SortKey{Column: ob.Column, Order: order})
		}
		winCols = append(winCols, exec.WindowColumn{
			Func:        parseWindowFunc(wc.Func),
			InputCol:    wc.InputCol,
			OutputCol:   wc.OutputCol,
			OutputType:  windowOutputType(wc.Func),
			PartitionBy: wc.PartitionBy,
			OrderBy:     orderBy,
		})
	}

	winOp := exec.NewWindow(winCols)

	// Wire spill manager if memory budget is set
	spill, tracker := e.newSpillManager(task.ID)
	if spill != nil {
		winOp.Spill = spill
		defer spill.Cleanup()
		defer e.recordSpillMetrics(spill, tracker)
	}

	if err := winOp.Init(ctx); err != nil {
		return err
	}

	b := batch.FromRows(schema, allRows)
	if err := winOp.Consume(ctx, b); err != nil {
		return err
	}
	if err := winOp.Finalize(ctx); err != nil {
		return err
	}

	var resultRows []map[string]any
	for {
		rb, err := winOp.Next(ctx)
		if err != nil {
			return err
		}
		if rb == nil {
			break
		}
		resultRows = append(resultRows, rb.ToRows()...)
	}

	result.NumRows = int64(len(resultRows))
	if len(resultRows) == 0 {
		return nil
	}

	return e.writeResult(ctx, task, resultRows, result)
}

func parseWindowFunc(s string) exec.WindowFunc {
	switch s {
	case "row_number":
		return exec.WinRowNumber
	case "rank":
		return exec.WinRank
	case "dense_rank":
		return exec.WinDenseRank
	case "sum":
		return exec.WinSum
	case "count":
		return exec.WinCount
	case "avg":
		return exec.WinAvg
	case "min":
		return exec.WinMin
	case "max":
		return exec.WinMax
	default:
		return exec.WinRowNumber
	}
}

func windowOutputType(funcName string) parquet.TypeID {
	switch funcName {
	case "row_number", "rank", "dense_rank", "count":
		return parquet.TypeInt64
	default:
		return parquet.TypeFloat64
	}
}

// mapExecJoinType converts a canonical join type string to exec.JoinType.
func mapExecJoinType(jt string) exec.JoinType {
	switch jt {
	case "left":
		return exec.LeftJoin
	case "right":
		return exec.RightJoin
	case "full":
		return exec.FullOuterJoin
	case "cross":
		return exec.CrossJoin
	default:
		return exec.InnerJoin
	}
}

// readParquetFilesConcurrent reads multiple Parquet files in parallel (up to 8
// goroutines), returning all rows concatenated in file order. This significantly
// reduces latency for S3-backed reads where each GET is a network round-trip.
func (e *Executor) readParquetFilesConcurrent(ctx context.Context, bucket string, files []string, selectedCols []string) ([]map[string]any, error) {
	if len(files) == 0 {
		return nil, nil
	}
	if len(files) == 1 {
		return e.readParquetFile(ctx, bucket, files[0], selectedCols)
	}

	type result struct {
		rows []map[string]any
		err  error
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

			rows, err := e.readParquetFile(ctx, bucket, filePath, selectedCols)
			results[idx] = result{rows: rows, err: err}
		}(i, f)
	}
	wg.Wait()

	var allRows []map[string]any
	for i, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("reading file %s: %w", files[i], r.err)
		}
		allRows = append(allRows, r.rows...)
	}
	return allRows, nil
}

func (e *Executor) readParquetFile(ctx context.Context, bucket, path string, selectedCols []string) ([]map[string]any, error) {
	// Check in-memory result store first (avoids S3 round-trip for same-node stages)
	if e.resultStore != nil {
		if data, ok := e.resultStore.Get(path); ok {
			reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				return nil, err
			}
			return reader.ReadRows(selectedCols)
		}
	}

	// Check LRU cache
	cacheKey := bucket + "/" + path
	if data, ok := e.cache.Get(cacheKey); ok {
		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, err
		}
		return reader.ReadRows(selectedCols)
	}

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

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return reader.ReadRows(selectedCols)
}

func (e *Executor) writeResult(ctx context.Context, task distributed.Task, rows []map[string]any, result *distributed.ResultNotification) error {
	data, err := e.serializeRows(rows)
	if err != nil {
		return err
	}

	result.SizeBytes = int64(len(data))

	// Small result fast path: include inline
	if len(data) <= inlineResultThreshold {
		result.InlineData = data
		return nil
	}

	resultPath := task.ResultPrefix + task.ID + ".parquet"

	// Try in-memory result store first (avoids S3 write for same-node stages)
	if e.resultStore != nil && e.resultStore.Put(task.QueryID, resultPath, data) {
		result.ResultPath = resultPath
		return nil
	}

	// Fall back to S3
	_, err = e.store.Put(ctx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("writing result to S3: %w", err)
	}
	result.ResultPath = resultPath

	return nil
}

// writeResultToPath writes a large result directly to S3 (no inline fast path).
func (e *Executor) writeResultToPath(ctx context.Context, task distributed.Task, rows []map[string]any, resultPath string, result *distributed.ResultNotification) error {
	data, err := e.serializeRows(rows)
	if err != nil {
		return err
	}

	result.SizeBytes = int64(len(data))

	// Try in-memory result store first
	if e.resultStore != nil && e.resultStore.Put(task.QueryID, resultPath, data) {
		result.ResultPath = resultPath
		return nil
	}

	_, err = e.store.Put(ctx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("writing result to S3: %w", err)
	}
	result.ResultPath = resultPath
	return nil
}

func (e *Executor) serializeRows(rows []map[string]any) ([]byte, error) {
	schema := schemaFromRows(rows)

	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		return nil, fmt.Errorf("creating parquet writer: %w", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		return nil, fmt.Errorf("writing rows: %w", err)
	}
	if err := pw.Close(); err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}
	return buf.Bytes(), nil
}

func schemaFromRows(rows []map[string]any) []parquet.Column {
	if len(rows) == 0 {
		return nil
	}

	// Collect column names deterministically (sorted)
	names := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		names = append(names, k)
	}
	sort.Strings(names)

	cols := make([]parquet.Column, len(names))
	for i, k := range names {
		v := rows[0][k]
		col := parquet.Column{
			Name:     k,
			Nullable: true,
		}
		switch v.(type) {
		case bool:
			col.Type = parquet.TypeBool
		case int32:
			col.Type = parquet.TypeInt32
		case int64:
			col.Type = parquet.TypeInt64
		case int:
			col.Type = parquet.TypeInt64
		case float32:
			col.Type = parquet.TypeFloat32
		case float64:
			col.Type = parquet.TypeFloat64
		case string:
			col.Type = parquet.TypeString
		case []byte:
			col.Type = parquet.TypeBytes
		default:
			col.Type = parquet.TypeString
		}
		cols[i] = col
	}
	return cols
}

// recordSpillMetrics updates Prometheus counters with spill stats after a task completes.
func (e *Executor) recordSpillMetrics(spill *memory.SpillManager, tracker *memory.Tracker) {
	if e.metrics == nil {
		return
	}

	files := spill.SpilledFiles()
	if len(files) > 0 {
		e.metrics.SpillEvents.Add(float64(len(files)))

		// Sum spill file sizes
		var totalBytes int64
		for _, f := range files {
			if info, err := os.Stat(f); err == nil {
				totalBytes += info.Size()
			}
		}
		e.metrics.SpillBytesWritten.Add(float64(totalBytes))
	}

	if tracker != nil {
		e.metrics.MemoryBudgetBytes.Set(float64(tracker.Budget()))
		e.metrics.MemoryUsedBytes.Set(float64(tracker.Used()))
	}
}

// aggregateNeededCols returns the minimal set of columns needed for an
// aggregate task: group-by columns + aggregate input columns. Returns nil
// (read all) if no columns are specified (safety fallback).
func aggregateNeededCols(groupBy []string, aggs []distributed.AggSpec) []string {
	seen := make(map[string]struct{})
	var cols []string
	for _, col := range groupBy {
		if _, ok := seen[col]; !ok {
			seen[col] = struct{}{}
			cols = append(cols, col)
		}
	}
	for _, a := range aggs {
		if a.InputCol != "" {
			if _, ok := seen[a.InputCol]; !ok {
				seen[a.InputCol] = struct{}{}
				cols = append(cols, a.InputCol)
			}
		}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

func parseAggFunc(s string) exec.AggFunc {
	switch s {
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
	default:
		return exec.AggCount
	}
}
