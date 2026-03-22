package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
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
	"github.com/citc-tech/wadjet/internal/engine/scan"
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
	case distributed.TaskTypeShuffle:
		err = e.executeShuffle(ctx, task, &result)
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
	// Read all scan files concurrently (parallel S3 GETs)
	allBatches, err := e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.Files, task.Columns)
	if err != nil {
		return err
	}

	// Apply pushed-down filter predicates as selection vectors
	if len(task.FilterExprs) > 0 {
		var filtered []*batch.RecordBatch
		for _, b := range allBatches {
			b = e.applyBatchFilters(b, task.FilterExprs)
			if b != nil && b.ActiveLen() > 0 {
				filtered = append(filtered, b)
			}
		}
		allBatches = filtered
	}

	var totalRows int64
	for _, b := range allBatches {
		totalRows += int64(b.ActiveLen())
	}

	if totalRows == 0 {
		return nil
	}

	result.NumRows = totalRows
	return e.writeBatchResult(ctx, task, allBatches, result)
}

// applyBatchFilters applies SQL filter expressions to a RecordBatch using
// selection vectors, avoiding row-level map[string]any allocation.
func (e *Executor) applyBatchFilters(b *batch.RecordBatch, filterExprs []string) *batch.RecordBatch {
	for _, filterSQL := range filterExprs {
		filter := buildSimpleFilter(filterSQL)
		if filter == nil {
			continue
		}

		var sel []uint32
		if b.Sel != nil {
			sel = make([]uint32, 0, len(b.Sel))
			for _, idx := range b.Sel {
				if filter(b, int(idx)) {
					sel = append(sel, idx)
				}
			}
		} else {
			sel = make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				if filter(b, i) {
					sel = append(sel, uint32(i))
				}
			}
		}

		if len(sel) == 0 {
			return nil
		}
		b.Sel = sel
	}
	return b
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

	// Read input files directly into columnar batches
	allBatches, err := e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, neededCols)
	if err != nil {
		return err
	}

	if len(allBatches) == 0 {
		return nil
	}

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

	// Consume batches directly — no FromRows conversion
	for _, b := range allBatches {
		if err := agg.Consume(ctx, b); err != nil {
			return err
		}
	}
	if err := agg.Finalize(ctx); err != nil {
		return err
	}

	// Collect result batches
	var resultBatches []*batch.RecordBatch
	var totalRows int64
	for {
		rb, err := agg.Next(ctx)
		if err != nil {
			return err
		}
		if rb == nil {
			break
		}
		totalRows += int64(rb.ActiveLen())
		resultBatches = append(resultBatches, rb)
	}

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, resultBatches, result)
}

func (e *Executor) executeSort(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	allBatches, err := e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, nil)
	if err != nil {
		return err
	}

	if len(allBatches) == 0 {
		return nil
	}

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

	// Consume batches directly
	for _, b := range allBatches {
		if err := sortOp.Consume(ctx, b); err != nil {
			return err
		}
	}
	if err := sortOp.Finalize(ctx); err != nil {
		return err
	}

	var resultBatches []*batch.RecordBatch
	var totalRows int64
	for {
		rb, err := sortOp.Next(ctx)
		if err != nil {
			return err
		}
		if rb == nil {
			break
		}
		totalRows += int64(rb.ActiveLen())
		resultBatches = append(resultBatches, rb)

		// Apply limit: stop collecting once we have enough
		if task.Limit > 0 && totalRows >= int64(task.Limit) {
			break
		}
	}

	// Trim last batch if limit causes overshoot
	if task.Limit > 0 && totalRows > int64(task.Limit) {
		excess := totalRows - int64(task.Limit)
		last := resultBatches[len(resultBatches)-1]
		trimLen := int64(last.ActiveLen()) - excess
		if trimLen > 0 {
			sel := make([]uint32, trimLen)
			for i := range sel {
				sel[i] = uint32(i)
			}
			last.Sel = sel
		}
		totalRows = int64(task.Limit)
	}

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, resultBatches, result)
}

func (e *Executor) executeJoin(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read build (right) and probe (left) sides concurrently
	var buildBatches, probeBatches []*batch.RecordBatch
	var buildErr, probeErr error
	var rwg sync.WaitGroup
	rwg.Add(2)
	go func() {
		defer rwg.Done()
		buildBatches, buildErr = e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.BuildFiles, task.Columns)
	}()
	go func() {
		defer rwg.Done()
		probeBatches, probeErr = e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, task.Columns)
	}()
	rwg.Wait()
	if buildErr != nil {
		return fmt.Errorf("reading build files: %w", buildErr)
	}
	if probeErr != nil {
		return fmt.Errorf("reading probe files: %w", probeErr)
	}

	if len(probeBatches) == 0 && len(buildBatches) == 0 {
		return nil
	}

	joinType := mapExecJoinType(task.JoinType)

	// For inner/semi/anti joins with no build data, there can be no matches.
	if len(buildBatches) == 0 && joinType != exec.LeftJoin && joinType != exec.FullOuterJoin {
		return nil
	}

	hj := exec.NewHashJoin(joinType, task.JoinLeftKeys, task.JoinRightKeys)

	// Build the hash table from right side batches.
	// Build even with empty data to mark buildDone for the probe phase.
	if err := hj.Build(ctx, &batchSource{batches: buildBatches}); err != nil {
		return fmt.Errorf("building hash table: %w", err)
	}

	// For RIGHT and FULL OUTER joins, we may still have results even
	// with no probe rows (unmatched build-side rows).
	if len(probeBatches) == 0 && joinType != exec.RightJoin && joinType != exec.FullOuterJoin {
		return nil
	}

	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		return err
	}

	var resultBatches []*batch.RecordBatch
	var totalRows int64

	// Probe with left side batches
	for _, pb := range probeBatches {
		rb, err := probe.Execute(ctx, pb)
		if err != nil {
			return err
		}
		if rb != nil && rb.ActiveLen() > 0 {
			totalRows += int64(rb.ActiveLen())
			resultBatches = append(resultBatches, rb)
		}
	}

	// For RIGHT and FULL OUTER joins, flush unmatched build-side rows
	if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
		var probeSchema []parquet.Column
		if len(probeBatches) > 0 {
			probeSchema = probeBatches[0].Schema
		} else if len(buildBatches) > 0 {
			probeSchema = buildBatches[0].Schema
		}
		if probeSchema != nil {
			unmatchedBatch := probe.FlushUnmatched(probeSchema)
			if unmatchedBatch != nil && unmatchedBatch.ActiveLen() > 0 {
				totalRows += int64(unmatchedBatch.ActiveLen())
				resultBatches = append(resultBatches, unmatchedBatch)
			}
		}
	}

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, resultBatches, result)
}

func (e *Executor) executeWindow(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	allBatches, err := e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, nil)
	if err != nil {
		return err
	}

	if len(allBatches) == 0 {
		return nil
	}

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

	// Consume batches directly
	for _, b := range allBatches {
		if err := winOp.Consume(ctx, b); err != nil {
			return err
		}
	}
	if err := winOp.Finalize(ctx); err != nil {
		return err
	}

	var resultBatches []*batch.RecordBatch
	var totalRows int64
	for {
		rb, err := winOp.Next(ctx)
		if err != nil {
			return err
		}
		if rb == nil {
			break
		}
		totalRows += int64(rb.ActiveLen())
		resultBatches = append(resultBatches, rb)
	}

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, resultBatches, result)
}

// executeShuffle hash-partitions input batches by key columns and writes
// one output file per partition. Processes files one at a time to avoid
// holding the entire input in memory — only one file's batches plus the
// per-partition Parquet writer buffers are resident at any point.
func (e *Executor) executeShuffle(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.InputFiles) == 0 {
		return nil
	}

	numPartitions := task.NumPartitions
	if numPartitions <= 1 {
		numPartitions = 1
	}

	// Per-partition Parquet writers — initialized from first batch's schema.
	var (
		partWriters []*parquet.Writer
		partBufs    []*bytes.Buffer
		keyIdxs     []int
		totalRows   int64
	)

	// Process input files one at a time (streaming)
	for _, inputFile := range task.InputFiles {
		batches, err := e.readParquetFileBatches(ctx, task.ResultBucket, inputFile, task.Columns)
		if err != nil {
			return fmt.Errorf("reading shuffle input %s: %w", inputFile, err)
		}

		for _, b := range batches {
			if b.ActiveLen() == 0 {
				continue
			}

			// Lazy-initialize writers and key indices from first batch
			if partWriters == nil {
				schema := b.Schema
				keyIdxs = make([]int, len(task.ShuffleKeys))
				for i, key := range task.ShuffleKeys {
					found := false
					for j, col := range schema {
						if col.Name == key {
							keyIdxs[i] = j
							found = true
							break
						}
					}
					// Fallback: match suffix after "." for qualified names
					// (e.g., join output may have "n2.n_nationkey" for self-joins)
					if !found {
						for j, col := range schema {
							if dotIdx := strings.LastIndex(col.Name, "."); dotIdx >= 0 {
								if col.Name[dotIdx+1:] == key {
									keyIdxs[i] = j
									found = true
									break
								}
							}
						}
					}
					if !found {
						return fmt.Errorf("shuffle key %q not found in schema", key)
					}
				}

				partBufs = make([]*bytes.Buffer, numPartitions)
				partWriters = make([]*parquet.Writer, numPartitions)
				for pid := 0; pid < numPartitions; pid++ {
					partBufs[pid] = &bytes.Buffer{}
					pw, pwErr := parquet.NewWriter(partBufs[pid], parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
					if pwErr != nil {
						return fmt.Errorf("creating partition %d writer: %w", pid, pwErr)
					}
					partWriters[pid] = pw
				}
			}

			// Partition rows by hash
			nRows := b.ActiveLen()
			partitionRows := make([][]uint32, numPartitions)
			for ri := 0; ri < nRows; ri++ {
				rowIdx := ri
				if b.Sel != nil {
					rowIdx = int(b.Sel[ri])
				}

				h := uint64(14695981039346656037) // FNV-1a offset basis
				for _, ki := range keyIdxs {
					h = shuffleHashValue(b.Columns[ki], rowIdx, h)
				}
				pid := int(h % uint64(numPartitions))
				partitionRows[pid] = append(partitionRows[pid], uint32(rowIdx))
			}

			// Write partitioned rows immediately to Parquet writers
			for pid := 0; pid < numPartitions; pid++ {
				if len(partitionRows[pid]) == 0 {
					continue
				}
				pb := &batch.RecordBatch{
					Columns: b.Columns,
					Schema:  b.Schema,
					Len:     b.Len,
					Sel:     partitionRows[pid],
				}
				rows := pb.ToRows()
				if err := partWriters[pid].WriteRows(rows); err != nil {
					return fmt.Errorf("writing to partition %d: %w", pid, err)
				}
				totalRows += int64(len(rows))
			}
		}
		// batches from this file are now unreferenced — GC can reclaim
	}

	if partWriters == nil {
		return nil // no data
	}

	// Close writers and upload partition files
	resultFiles := make([]string, numPartitions)
	for pid := 0; pid < numPartitions; pid++ {
		if err := partWriters[pid].Close(); err != nil {
			return fmt.Errorf("closing partition %d writer: %w", pid, err)
		}
		data := partBufs[pid].Bytes()
		if len(data) == 0 {
			continue
		}

		partPath := fmt.Sprintf("%s%s-p%d.parquet", task.ResultPrefix, task.ID, pid)
		_, putErr := e.store.Put(ctx, task.ResultBucket, partPath,
			bytes.NewReader(data), int64(len(data)), "application/octet-stream")
		if putErr != nil {
			return fmt.Errorf("writing partition %d to store: %w", pid, putErr)
		}
		resultFiles[pid] = partPath
	}

	result.NumRows = totalRows
	result.ResultFiles = resultFiles
	result.ResultPath = task.ResultPrefix
	return nil
}

// shuffleHashValue hashes a single vector value into the running FNV-1a hash.
func shuffleHashValue(vec *batch.Vector, idx int, h uint64) uint64 {
	const fnvPrime = 1099511628211

	if vec.Nulls.IsNull(idx) {
		return (h ^ 0xFF) * fnvPrime // hash NULL distinctly
	}

	switch vec.Type {
	case batch.TypeInt64:
		v := uint64(vec.Int64Data[idx])
		h = (h ^ (v & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 8) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 16) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 24) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 32) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 40) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 48) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 56) & 0xFF)) * fnvPrime
	case batch.TypeInt32:
		v := uint64(vec.Int32Data[idx])
		h = (h ^ (v & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 8) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 16) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 24) & 0xFF)) * fnvPrime
	case batch.TypeFloat64:
		v := math.Float64bits(vec.Float64Data[idx])
		h = (h ^ (v & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 8) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 16) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 24) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 32) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 40) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 48) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 56) & 0xFF)) * fnvPrime
	case batch.TypeFloat32:
		v := uint64(math.Float32bits(vec.Float32Data[idx]))
		h = (h ^ (v & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 8) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 16) & 0xFF)) * fnvPrime
		h = (h ^ ((v >> 24) & 0xFF)) * fnvPrime
	case batch.TypeString, batch.TypeBytes:
		b := vec.BytesData.Value(idx)
		for _, c := range b {
			h = (h ^ uint64(c)) * fnvPrime
		}
	case batch.TypeBool:
		if vec.BoolData[idx] {
			h = (h ^ 1) * fnvPrime
		} else {
			h = (h ^ 0) * fnvPrime
		}
	default:
		// Fallback: hash as string
		b := vec.BytesData.Value(idx)
		for _, c := range b {
			h = (h ^ uint64(c)) * fnvPrime
		}
	}
	return h
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

// getFileData retrieves raw Parquet bytes with 3-tier caching:
// in-memory result store → LRU cache → object store (S3).
func (e *Executor) getFileData(ctx context.Context, bucket, path string) ([]byte, error) {
	// Check in-memory result store first (avoids S3 round-trip for same-node stages)
	if e.resultStore != nil {
		if data, ok := e.resultStore.Get(path); ok {
			return data, nil
		}
	}

	// Check LRU cache
	cacheKey := bucket + "/" + path
	if data, ok := e.cache.Get(cacheKey); ok {
		return data, nil
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
	return data, nil
}

// readParquetFileBatches reads a Parquet file directly into columnar RecordBatches,
// bypassing the map[string]any intermediate. One batch per row group.
func (e *Executor) readParquetFileBatches(ctx context.Context, bucket, path string, selectedCols []string) ([]*batch.RecordBatch, error) {
	data, err := e.getFileData(ctx, bucket, path)
	if err != nil {
		return nil, err
	}

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	schema := reader.Schema().Columns
	return scan.ReadFileBatches(reader, schema, selectedCols)
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
	data, err := e.getFileData(ctx, bucket, path)
	if err != nil {
		return nil, err
	}

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return reader.ReadRows(selectedCols)
}


// serializeBatches writes columnar batches directly to Parquet bytes.
func (e *Executor) serializeBatches(batches []*batch.RecordBatch) ([]byte, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to serialize")
	}

	schema := batches[0].Schema

	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		return nil, fmt.Errorf("creating parquet writer: %w", err)
	}
	for _, b := range batches {
		rows := b.ToRows()
		if err := pw.WriteRows(rows); err != nil {
			return nil, fmt.Errorf("writing rows: %w", err)
		}
	}
	if err := pw.Close(); err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}
	return buf.Bytes(), nil
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

	resultPath := task.ResultPrefix + task.ID + ".parquet"

	// Always write to S3 so results are visible to all workers.
	_, err = e.store.Put(ctx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("writing result to S3: %w", err)
	}
	result.ResultPath = resultPath

	// Also cache locally for same-node reads (avoids S3 round-trip
	// when the next stage runs on this worker).
	if e.resultStore != nil {
		e.resultStore.Put(task.QueryID, resultPath, data)
	}
	return nil
}

// batchSource wraps a slice of RecordBatches as an exec.Source.
type batchSource struct {
	batches []*batch.RecordBatch
	idx     int
}

func (s *batchSource) Init(_ context.Context) error { return nil }
func (s *batchSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}
func (s *batchSource) Close() error { return nil }

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
