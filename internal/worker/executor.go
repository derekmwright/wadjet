package worker

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
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
		// Use full expression compiler first — handles LIKE, IN,
		// column-to-column comparisons, and all other SQL predicates.
		filter := buildCompiledFilter(filterSQL)
		if filter == nil {
			// Fall back to simple col-op-literal parser
			filter = buildSimpleFilter(filterSQL)
			if filter == nil {
				continue
			}
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

// buildCompiledFilter uses the full SQL expression parser + compiler to create
// a filter function. Handles LIKE, IN, column-to-column comparisons, etc.
func buildCompiledFilter(filterSQL string) func(*batch.RecordBatch, int) bool {
	astNode, err := plansql.ParseExpression(filterSQL)
	if err != nil {
		return nil
	}
	compiled, err := expr.Compile(astNode)
	if err != nil {
		return nil
	}
	return func(b *batch.RecordBatch, row int) bool {
		v := compiled.Eval(b, row)
		if v == nil {
			return false
		}
		bv, ok := v.(bool)
		return ok && bv
	}
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
	// Build column projection: only read group-by columns + aggregate input columns.
	// For merge tasks, read all columns — partial aggregate output columns may have
	// expression-derived names (e.g., "substr(l_shipdate, 1, 4)") that don't decompose
	// into raw column references.
	var neededCols []string
	if !task.MergePartials {
		neededCols = aggregateNeededCols(task.GroupByCols, task.Aggregates)
	}

	// Read input files directly into columnar batches
	allBatches, err := e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, neededCols)
	if err != nil {
		return err
	}

	if len(allBatches) == 0 {
		return nil
	}

	// Pre-compute expression GROUP BY columns and aggregate inputs.
	// In standalone mode, the planner inserts aggPreProject for this. In distributed
	// mode, we evaluate expressions here and add them as new columns.
	// Skip for merge tasks — partial aggregate already materialized expression columns.
	if !task.MergePartials {
		exprCols := collectAggExpressions(task.GroupByCols, task.Aggregates)
		if len(exprCols) > 0 {
			for i, b := range allBatches {
				allBatches[i] = addComputedColumns(b, exprCols)
			}
		}
	}

	// Build and execute aggregate
	aggs := make([]exec.AggColumn, len(task.Aggregates))
	for i, a := range task.Aggregates {
		aggFunc := a.Func
		aggs[i] = exec.AggColumn{
			Func:       parseAggFunc(aggFunc),
			InputCol:   a.InputCol,
			OutputCol:  a.OutputCol,
			OutputType: parquet.TypeFloat64,
		}
		if aggFunc == "count" || aggFunc == "count_star" {
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

	// Apply HAVING / post-aggregate filter expressions
	if len(task.FilterExprs) > 0 && len(resultBatches) > 0 {
		resultBatches, totalRows = applyFilterExprs(resultBatches, task.FilterExprs)
	}

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, resultBatches, result)
}

func (e *Executor) executeSort(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// k-way merge: inputs are pre-sorted, merge without re-sorting
	if task.MergePreSorted && len(task.InputFiles) > 1 {
		return e.executeMergeSorted(ctx, task, result)
	}

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

// mergeRun represents a cursor over the pre-sorted batches from a single input file.
type mergeRun struct {
	batches  []*batch.RecordBatch
	batchIdx int
	rowIdx   int // position within current batch (respects Sel)
}

func (r *mergeRun) currentBatch() *batch.RecordBatch { return r.batches[r.batchIdx] }

func (r *mergeRun) currentRow() int {
	b := r.currentBatch()
	if b.Sel != nil {
		return int(b.Sel[r.rowIdx])
	}
	return r.rowIdx
}

func (r *mergeRun) advance() bool {
	b := r.currentBatch()
	r.rowIdx++
	if r.rowIdx >= b.ActiveLen() {
		r.batchIdx++
		r.rowIdx = 0
		return r.batchIdx < len(r.batches)
	}
	return true
}

func (r *mergeRun) exhausted() bool {
	return r.batchIdx >= len(r.batches)
}

// mergeHeap implements container/heap.Interface for k-way merge of sorted runs.
type mergeHeap struct {
	runs []*mergeRun
	less func(a, b *mergeRun) bool
}

func (h *mergeHeap) Len() int            { return len(h.runs) }
func (h *mergeHeap) Less(i, j int) bool  { return h.less(h.runs[i], h.runs[j]) }
func (h *mergeHeap) Swap(i, j int)       { h.runs[i], h.runs[j] = h.runs[j], h.runs[i] }
func (h *mergeHeap) Push(x any)          { h.runs = append(h.runs, x.(*mergeRun)) }
func (h *mergeHeap) Pop() any {
	old := h.runs
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.runs = old[:n-1]
	return x
}

// executeMergeSorted performs a k-way merge of pre-sorted input files.
// Each input file is already sorted by the sort keys (produced by a parallel sort stage).
// Instead of re-sorting O(n log n), this merges in O(n log k) where k = number of files.
func (e *Executor) executeMergeSorted(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read each input file's batches separately (preserve per-file sort order).
	type fileResult struct {
		batches []*batch.RecordBatch
		err     error
	}
	fileResults := make([]fileResult, len(task.InputFiles))

	const maxConcurrency = 8
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i, f := range task.InputFiles {
		wg.Add(1)
		go func(idx int, filePath string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			batches, err := e.readParquetFileBatches(ctx, task.ResultBucket, filePath, nil)
			fileResults[idx] = fileResult{batches: batches, err: err}
		}(i, f)
	}
	wg.Wait()

	// Build merge runs from non-empty file results.
	var runs []*mergeRun
	var schema []parquet.Column
	for i, fr := range fileResults {
		if fr.err != nil {
			return fmt.Errorf("reading file %s: %w", task.InputFiles[i], fr.err)
		}
		if len(fr.batches) == 0 {
			continue
		}
		if schema == nil {
			schema = fr.batches[0].Schema
		}
		runs = append(runs, &mergeRun{batches: fr.batches})
	}

	if len(runs) == 0 {
		return nil
	}

	// Resolve sort keys to column indices and typed comparison kernels.
	type resolvedKey struct {
		colIdx  int
		desc    bool
		compare kernel.SortCompareKernel
	}
	firstBatch := runs[0].batches[0]
	resolved := make([]resolvedKey, 0, len(task.SortKeys))
	for _, sk := range task.SortKeys {
		idx := firstBatch.ColumnIndex(sk.Column)
		if idx < 0 || idx >= len(firstBatch.Columns) {
			continue
		}
		colType := firstBatch.Columns[idx].Type
		// Use standard null-handling comparison (NULLS FIRST by default).
		cmp := kernel.ResolveSortCompare(colType)
		resolved = append(resolved, resolvedKey{colIdx: idx, desc: sk.Desc, compare: cmp})
	}

	// Build comparison function for the merge heap.
	lessFunc := func(a, b *mergeRun) bool {
		ab, bb := a.currentBatch(), b.currentBatch()
		ar, br := a.currentRow(), b.currentRow()
		for _, key := range resolved {
			if key.colIdx >= len(ab.Columns) || key.colIdx >= len(bb.Columns) {
				continue
			}
			cmp := key.compare(ab.Columns[key.colIdx], ar, bb.Columns[key.colIdx], br)
			if cmp == 0 {
				continue
			}
			if key.desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	}

	// Initialize the min-heap with one entry per non-empty run.
	h := &mergeHeap{
		runs: make([]*mergeRun, 0, len(runs)),
		less: lessFunc,
	}
	for _, r := range runs {
		if !r.exhausted() {
			h.runs = append(h.runs, r)
		}
	}
	heap.Init(h)

	// Merge: pop minimum row, copy to output batch, advance cursor, push back.
	var resultBatches []*batch.RecordBatch
	var totalRows int64
	outBatch := batch.NewRecordBatch(schema, batch.DefaultBatchSize)
	outPos := 0

	for h.Len() > 0 {
		// Check limit
		if task.Limit > 0 && totalRows >= int64(task.Limit) {
			break
		}

		minRun := h.runs[0]
		srcBatch := minRun.currentBatch()
		srcRow := minRun.currentRow()

		// Copy one row from source to output batch.
		for j := range schema {
			if j < len(srcBatch.Columns) && j < len(outBatch.Columns) {
				mergeCopyValue(outBatch.Columns[j], outPos, srcBatch.Columns[j], srcRow)
			}
		}
		outPos++
		totalRows++

		// Flush output batch when full.
		if outPos >= batch.DefaultBatchSize {
			outBatch.Len = outPos
			resultBatches = append(resultBatches, outBatch)
			outBatch = batch.NewRecordBatch(schema, batch.DefaultBatchSize)
			outPos = 0
		}

		// Advance the run cursor.
		if minRun.advance() {
			heap.Fix(h, 0) // re-heapify with new current row
		} else {
			heap.Pop(h) // run exhausted, remove from heap
		}
	}

	// Flush final partial batch.
	if outPos > 0 {
		outBatch.Len = outPos
		// Trim over-allocated vectors by setting a selection vector for exact length.
		if outPos < batch.DefaultBatchSize {
			sel := make([]uint32, outPos)
			for i := range sel {
				sel[i] = uint32(i)
			}
			outBatch.Sel = sel
		}
		resultBatches = append(resultBatches, outBatch)
	}

	// Trim last batch if limit causes overshoot.
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

// mergeCopyValue copies a single value from src[si] to dst[di] using typed access.
// For bytes-based types, values must be copied in sequential order for dst (di = 0, 1, 2, ...).
func mergeCopyValue(dst *batch.Vector, di int, src *batch.Vector, si int) {
	if src.Nulls.IsNull(si) {
		dst.Nulls.SetNull(di)
		switch dst.Type {
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			dst.BytesData.Set(di, nil)
		}
		return
	}
	dst.Nulls.SetValid(di)
	switch dst.Type {
	case batch.TypeBool:
		dst.BoolData[di] = src.BoolData[si]
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		dst.Int32Data[di] = src.Int32Data[si]
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		dst.Int64Data[di] = src.Int64Data[si]
	case batch.TypeFloat32:
		dst.Float32Data[di] = src.Float32Data[si]
	case batch.TypeFloat64:
		dst.Float64Data[di] = src.Float64Data[si]
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.Set(di, src.BytesData.Value(si))
	case batch.TypeDecimal:
		dst.DecimalData.Data[di] = src.DecimalData.Data[si]
	}
}

func (e *Executor) executeJoin(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read build (right) and probe (left) sides concurrently
	var buildBatches, probeBatches []*batch.RecordBatch
	var buildErr, probeErr error
	var rwg sync.WaitGroup
	rwg.Add(2)
	go func() {
		defer rwg.Done()
		buildBatches, buildErr = e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.BuildFiles, nil)
	}()
	go func() {
		defer rwg.Done()
		probeBatches, probeErr = e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, nil)
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
	hj.BuildTableAlias = task.BuildTableAlias

	// Set semi/anti join inequality filter (e.g., "l2.l_suppkey != l1.l_suppkey")
	if task.JoinFilter != "" && (joinType == exec.SemiJoin || joinType == exec.AntiJoin) {
		hj.SemiAntiFilter = physical.BuildSemiAntiFilter(task.JoinFilter)
	}

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

	// Apply post-join filter expressions (e.g., WHERE conditions that couldn't
	// be pushed to scan, like decorrelated subquery filters or nation-name filters).
	if len(task.FilterExprs) > 0 {
		resultBatches, totalRows = applyFilterExprs(resultBatches, task.FilterExprs)
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
// one output file per partition. Reads all input files concurrently via
// parallel S3 GETs, then partitions all batches and uploads partition
// files in parallel (up to 8 concurrent uploads).
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

	// Read all input files concurrently (parallel S3 GETs)
	allBatches, err := e.readParquetFilesConcurrentBatches(ctx, task.ResultBucket, task.InputFiles, task.Columns)
	if err != nil {
		return fmt.Errorf("reading shuffle inputs: %w", err)
	}

	for _, b := range allBatches {
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
			pqRows := batchToParquetRows(pb, b.Schema)
			if err := partWriters[pid].WriteParquetRows(pqRows); err != nil {
				return fmt.Errorf("writing to partition %d: %w", pid, err)
			}
			totalRows += int64(len(pqRows))
		}
	}

	if partWriters == nil {
		return nil // no data
	}

	// Close all writers (flushes in-memory buffers, no I/O)
	for pid := 0; pid < numPartitions; pid++ {
		if err := partWriters[pid].Close(); err != nil {
			return fmt.Errorf("closing partition %d writer: %w", pid, err)
		}
	}

	// Upload partition files concurrently (parallel S3 PUTs)
	type uploadResult struct {
		path string
		err  error
	}
	uploadResults := make([]uploadResult, numPartitions)

	const maxUploadConcurrency = 8
	sem := make(chan struct{}, maxUploadConcurrency)
	var wg sync.WaitGroup

	for pid := 0; pid < numPartitions; pid++ {
		data := partBufs[pid].Bytes()
		if len(data) == 0 {
			continue
		}

		partPath := fmt.Sprintf("%s%s-p%d.parquet", task.ResultPrefix, task.ID, pid)
		uploadResults[pid].path = partPath

		wg.Add(1)
		go func(id int, path string, buf []byte) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			_, putErr := e.store.Put(ctx, task.ResultBucket, path,
				bytes.NewReader(buf), int64(len(buf)), "application/octet-stream")
			uploadResults[id].err = putErr
		}(pid, partPath, data)
	}
	wg.Wait()

	// Collect results and check for errors
	resultFiles := make([]string, numPartitions)
	for pid := 0; pid < numPartitions; pid++ {
		if uploadResults[pid].err != nil {
			return fmt.Errorf("writing partition %d to store: %w", pid, uploadResults[pid].err)
		}
		resultFiles[pid] = uploadResults[pid].path
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
	case "semi":
		return exec.SemiJoin
	case "anti":
		return exec.AntiJoin
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
		pqRows := batchToParquetRows(b, schema)
		if err := pw.WriteParquetRows(pqRows); err != nil {
			return nil, fmt.Errorf("writing rows: %w", err)
		}
	}
	if err := pw.Close(); err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}
	return buf.Bytes(), nil
}

// batchToParquetRows converts a RecordBatch directly to parquet.Row values
// using typed column access, bypassing the map[string]any intermediate.
// This avoids per-row map allocation and interface boxing.
//
// Columns are emitted in alphabetical order to match parquet-go's internal
// schema ordering (Group.Fields() sorts by name).
func batchToParquetRows(b *batch.RecordBatch, schema []parquet.Column) []goparquet.Row {
	nRows := b.ActiveLen()
	nCols := len(schema)
	if nRows == 0 {
		return nil
	}

	// Build sorted column order: parquet-go sorts Group fields alphabetically.
	// Map from sorted position → original schema index.
	type colMapping struct {
		sortedIdx int
		origIdx   int
		col       parquet.Column
	}
	sorted := make([]colMapping, nCols)
	for i, col := range schema {
		sorted[i] = colMapping{origIdx: i, col: col}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].col.Name < sorted[j].col.Name
	})
	for i := range sorted {
		sorted[i].sortedIdx = i
	}

	rows := make([]goparquet.Row, nRows)
	for i := range rows {
		rows[i] = make(goparquet.Row, nCols)
	}

	for _, cm := range sorted {
		ci := cm.origIdx
		ri_out := cm.sortedIdx
		if ci >= len(b.Columns) {
			continue
		}
		vec := b.Columns[ci]
		col := cm.col
		nullable := col.Nullable
		if b.Sel != nil {
			for ri, idx := range b.Sel {
				rows[ri][ri_out] = vectorToParquetValue(vec, int(idx), col.Type, nullable, ri_out)
			}
		} else {
			for ri := 0; ri < nRows; ri++ {
				rows[ri][ri_out] = vectorToParquetValue(vec, ri, col.Type, nullable, ri_out)
			}
		}
	}
	return rows
}

// vectorToParquetValue converts a single vector element to a parquet.Value.
// Operates directly on typed column data — no interface boxing or map allocation.
// For nullable columns, sets definition level 1 for non-null values (parquet-go
// uses definition levels to distinguish null from present in OPTIONAL columns).
func vectorToParquetValue(vec *batch.Vector, idx int, typ parquet.TypeID, nullable bool, colIdx int) goparquet.Value {
	if vec.Nulls.IsNullFast(idx) {
		return goparquet.Value{}.Level(0, 0, colIdx)
	}

	var v goparquet.Value
	switch typ {
	case parquet.TypeBool:
		v = goparquet.BooleanValue(vec.BoolData[idx])
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		v = goparquet.Int32Value(vec.Int32Data[idx])
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		v = goparquet.Int64Value(vec.Int64Data[idx])
	case parquet.TypeFloat32:
		v = goparquet.FloatValue(vec.Float32Data[idx])
	case parquet.TypeFloat64:
		v = goparquet.DoubleValue(vec.Float64Data[idx])
	case parquet.TypeString, parquet.TypeCIDR:
		v = goparquet.ByteArrayValue(vec.BytesData.Value(idx))
	case parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeUUID:
		v = goparquet.ByteArrayValue(vec.BytesData.Value(idx))
	default:
		// Fallback for other types (Decimal, Array, Map, Row): use GetValue boxing
		raw := vec.GetValue(idx)
		if raw == nil {
			return goparquet.Value{}.Level(0, 0, colIdx)
		}
		switch tv := raw.(type) {
		case bool:
			v = goparquet.BooleanValue(tv)
		case int32:
			v = goparquet.Int32Value(tv)
		case int64:
			v = goparquet.Int64Value(tv)
		case float32:
			v = goparquet.FloatValue(tv)
		case float64:
			v = goparquet.DoubleValue(tv)
		case string:
			v = goparquet.ByteArrayValue([]byte(tv))
		case []byte:
			v = goparquet.ByteArrayValue(tv)
		default:
			return goparquet.Value{}.Level(0, 0, colIdx)
		}
	}

	defLevel := 0
	if nullable {
		defLevel = 1
	}
	return v.Level(0, defLevel, colIdx)
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
// aggregate task: group-by columns + aggregate input columns. Extracts raw
// column references from expression strings (e.g., "substr(l_shipdate, 1, 4)"
// → "l_shipdate"). Returns nil (read all) if no columns are specified.
func aggregateNeededCols(groupBy []string, aggs []distributed.AggSpec) []string {
	seen := make(map[string]struct{})
	var cols []string
	addRef := func(s string) {
		for _, ref := range extractColRefs(s) {
			if _, ok := seen[ref]; !ok {
				seen[ref] = struct{}{}
				cols = append(cols, ref)
			}
		}
	}
	for _, col := range groupBy {
		addRef(col)
	}
	for _, a := range aggs {
		if a.InputCol != "" {
			addRef(a.InputCol)
		}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

// extractColRefs extracts raw column references from a string that may be
// a simple column name ("l_shipdate"), a qualified name ("n1.n_name"), or
// an expression ("substr(l_shipdate, 1, 4)"). Returns the string itself for
// simple/qualified names; extracts identifier tokens from expressions.
// Skips string literals in single quotes (e.g., 'BRAZIL' is NOT a column ref).
func extractColRefs(s string) []string {
	// Simple or qualified column name
	isSimple := true
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			isSimple = false
			break
		}
	}
	if isSimple {
		return []string{s}
	}

	// Expression: tokenize and extract column-like identifiers.
	// Skip over single-quoted string literals to avoid treating values
	// like 'BRAZIL' as column references.
	var refs []string
	seen := make(map[string]bool)
	start := -1
	runes := []rune(s)
	inQuote := false
	for i, c := range runes {
		if c == '\'' {
			// Flush any pending token before entering/leaving quote
			if start >= 0 && !inQuote {
				tok := string(runes[start:i])
				start = -1
				if isColRef(tok) && !seen[tok] {
					refs = append(refs, tok)
					seen[tok] = true
				}
			}
			inQuote = !inQuote
			start = -1
			continue
		}
		if inQuote {
			continue
		}
		isIdent := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '.' || (c >= '0' && c <= '9')
		if isIdent {
			if start == -1 {
				start = i
			}
		} else {
			if start >= 0 {
				tok := string(runes[start:i])
				start = -1
				if isColRef(tok) && !seen[tok] {
					refs = append(refs, tok)
					seen[tok] = true
				}
			}
		}
	}
	if start >= 0 && !inQuote {
		tok := string(runes[start:])
		if isColRef(tok) && !seen[tok] {
			refs = append(refs, tok)
			seen[tok] = true
		}
	}
	return refs
}

func isColRef(tok string) bool {
	if len(tok) == 0 {
		return false
	}
	allDigit := true
	for _, c := range tok {
		if c < '0' || c > '9' {
			allDigit = false
			break
		}
	}
	if allDigit {
		return false
	}
	lower := strings.ToLower(tok)
	switch lower {
	case "case", "when", "then", "else", "end", "and", "or", "not", "in",
		"is", "null", "true", "false", "like", "between", "as", "asc", "desc",
		"substr", "sum", "count", "min", "max", "avg",
		"extract", "coalesce", "cast", "exists", "any", "all", "some",
		"upper", "lower", "trim", "length", "concat", "replace",
		"abs", "round", "floor", "ceil", "ceiling", "mod",
		"year", "month", "day", "hour", "minute", "second",
		"select", "from", "where", "having", "group", "by", "order",
		"inner", "outer", "left", "right", "full", "cross", "join", "on",
		"union", "intersect", "except", "distinct", "limit", "offset",
		"over", "partition", "rows", "range", "unbounded", "preceding",
		"following", "current", "row":
		return false
	}
	return true
}

// isExprString returns true if s contains operators, function calls, or other
// non-identifier characters indicating it's an expression rather than a column name.
func isExprString(s string) bool {
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
			return true
		}
	}
	return false
}

// aggExprCol represents a compiled expression column that needs to be added
// to each batch before the aggregate can process it.
type aggExprCol struct {
	name     string
	compiled expr.Expr
}

// collectAggExpressions finds expression strings in GROUP BY columns and
// aggregate inputs, parses and compiles them, and returns the compiled
// expressions. Returns nil if no expressions are found.
func collectAggExpressions(groupByCols []string, aggSpecs []distributed.AggSpec) []aggExprCol {
	var result []aggExprCol
	seen := make(map[string]bool)

	addExpr := func(s string) {
		if !isExprString(s) || seen[s] {
			return
		}
		seen[s] = true
		astNode, err := plansql.ParseExpression(s)
		if err != nil {
			return
		}
		compiled, err := expr.Compile(astNode)
		if err != nil {
			return
		}
		result = append(result, aggExprCol{name: s, compiled: compiled})
	}

	for _, col := range groupByCols {
		addExpr(col)
	}
	for _, a := range aggSpecs {
		if a.InputCol != "" {
			addExpr(a.InputCol)
		}
	}
	return result
}

// addComputedColumns evaluates expression columns and appends them to the batch.
// The input batch's columns are shared (not copied) for efficiency.
func addComputedColumns(in *batch.RecordBatch, exprCols []aggExprCol) *batch.RecordBatch {
	n := in.Len

	// Build extended schema
	outSchema := make([]parquet.Column, len(in.Schema), len(in.Schema)+len(exprCols))
	copy(outSchema, in.Schema)

	// Evaluate expressions to determine types and values
	newVecs := make([]*batch.Vector, len(exprCols))
	for ci, ec := range exprCols {
		// Determine type from first non-nil value
		typ := parquet.TypeFloat64 // default
		for ri := 0; ri < n && ri < 1; ri++ {
			row := ri
			if in.Sel != nil && len(in.Sel) > 0 {
				row = int(in.Sel[ri])
			}
			v := ec.compiled.Eval(in, row)
			if v != nil {
				switch v.(type) {
				case string, []byte:
					typ = parquet.TypeString
				case int, int32:
					typ = parquet.TypeInt32
				case int64:
					typ = parquet.TypeInt64
				case float32:
					typ = parquet.TypeFloat32
				case float64:
					typ = parquet.TypeFloat64
				case bool:
					typ = parquet.TypeString // bools stored as string for safety
				}
				break
			}
		}

		outSchema = append(outSchema, parquet.Column{Name: ec.name, Type: typ, Nullable: true})
		vec := batch.NewVector(typ, n)
		if in.Sel != nil {
			for outRow, idx := range in.Sel {
				v := ec.compiled.Eval(in, int(idx))
				if v != nil {
					vec.SetValue(outRow, v)
				} else {
					vec.Nulls.SetNull(outRow)
				}
			}
		} else {
			for ri := 0; ri < n; ri++ {
				v := ec.compiled.Eval(in, ri)
				if v != nil {
					vec.SetValue(ri, v)
				} else {
					vec.Nulls.SetNull(ri)
				}
			}
		}
		newVecs[ci] = vec
	}

	// Build output batch: share input columns + append new computed columns
	outCols := make([]*batch.Vector, len(in.Columns)+len(newVecs))
	copy(outCols, in.Columns)
	copy(outCols[len(in.Columns):], newVecs)

	out := &batch.RecordBatch{
		Schema:  outSchema,
		Columns: outCols,
		Len:     n,
	}
	// Clear selection vector since we materialized computed columns densely
	if in.Sel != nil {
		out.Len = len(in.Sel)
	}
	return out
}

// applyFilterExprs evaluates SQL filter expressions on result batches,
// keeping only rows where ALL filters evaluate to true.
func applyFilterExprs(batches []*batch.RecordBatch, filterExprs []string) ([]*batch.RecordBatch, int64) {
	// Compile filter expressions
	var filters []expr.Expr
	for _, fe := range filterExprs {
		astNode, err := plansql.ParseExpression(fe)
		if err != nil {
			continue
		}
		compiled, err := expr.Compile(astNode)
		if err != nil {
			continue
		}
		filters = append(filters, compiled)
	}
	if len(filters) == 0 {
		var total int64
		for _, b := range batches {
			total += int64(b.ActiveLen())
		}
		return batches, total
	}

	var result []*batch.RecordBatch
	var totalRows int64
	for _, b := range batches {
		n := b.ActiveLen()
		if n == 0 {
			continue
		}

		// Build selection vector of rows that pass all filters
		var sel []uint32
		for ri := 0; ri < n; ri++ {
			row := ri
			if b.Sel != nil {
				row = int(b.Sel[ri])
			}
			pass := true
			for _, f := range filters {
				v := f.Eval(b, row)
				if v == nil {
					pass = false
					break
				}
				switch bv := v.(type) {
				case bool:
					if !bv {
						pass = false
					}
				default:
					pass = false
				}
				if !pass {
					break
				}
			}
			if pass {
				sel = append(sel, uint32(row))
			}
		}

		if len(sel) > 0 {
			filtered := *b
			filtered.Sel = sel
			result = append(result, &filtered)
			totalRows += int64(len(sel))
		}
	}
	return result, totalRows
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
