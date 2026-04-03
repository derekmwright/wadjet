package worker

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
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

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
	"github.com/citc-tech/wadjet/internal/engine/expr"
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
	case distributed.TaskTypePipeline:
		err = e.executePipeline(ctx, task, &result)
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

	// Ensure TaskStats is always populated (fallback for tasks without spill)
	if result.TaskStats == nil {
		result.TaskStats = &distributed.TaskStats{RSS: distributed.ProcessRSS()}
	}

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

func (e *Executor) executeScan(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read all scan files concurrently (parallel S3 GETs)
	allBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, task.Files, task.Columns)
	if err != nil {
		return err
	}

	// Apply pushed-down filter predicates as selection vectors.
	// Uses vectorized kernel filters for simple predicates (col op literal,
	// IN, BETWEEN, LIKE) and falls back to per-row eval for complex ones.
	if len(task.FilterExprs) > 0 {
		filters := compileBatchFilters(task.FilterExprs)
		if len(filters) > 0 {
			var filtered []*batch.RecordBatch
			for _, b := range allBatches {
				b = applyBatchFilters(b, filters)
				if b != nil && b.ActiveLen() > 0 {
					filtered = append(filtered, b)
				}
			}
			allBatches = filtered
		}
	}

	// Fused scan-aggregate: perform partial aggregation at the scan level
	// to reduce data volume before writing results. Eliminates the separate
	// aggregate stage S3 round-trip (e.g., 60M rows → 4 aggregate groups).
	if len(task.ScanAggSpecs) > 0 {
		return e.executeFusedScanAggregate(ctx, task, allBatches, result)
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

// executeFusedScanAggregate performs partial aggregation on scanned batches,
// producing aggregate results instead of raw rows. This eliminates the
// scan→aggregate S3 round-trip by fusing both operations into one task.
func (e *Executor) executeFusedScanAggregate(ctx context.Context, task distributed.Task, allBatches []*batch.RecordBatch, result *distributed.ResultNotification) error {
	if len(allBatches) == 0 {
		return nil
	}

	// Pre-compute expression GROUP BY columns and aggregate inputs
	exprCols := collectAggExpressions(task.ScanAggGroupBy, task.ScanAggSpecs)
	if len(exprCols) > 0 {
		for i, b := range allBatches {
			allBatches[i] = addComputedColumns(b, exprCols)
		}
	}

	// Build aggregate operator
	aggs := make([]exec.AggColumn, len(task.ScanAggSpecs))
	for i, a := range task.ScanAggSpecs {
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

	agg := exec.NewHashAggregate(task.ScanAggGroupBy, aggs)

	// Wire spill manager if memory budget is set
	spill, tracker := e.newSpillManager(task.ID)
	if spill != nil {
		agg.Spill = spill
		defer spill.Cleanup()
		defer func() { result.TaskStats = e.collectTaskStats(spill, tracker) }()
	}

	if err := agg.Init(ctx); err != nil {
		return err
	}

	for _, b := range allBatches {
		if err := agg.Consume(ctx, b); err != nil {
			return err
		}
	}
	if err := agg.Finalize(ctx); err != nil {
		return err
	}

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

	e.logger.Info("fused scan-aggregate completed",
		"task_id", task.ID,
		"input_batches", len(allBatches),
		"output_rows", totalRows,
	)

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, resultBatches, result)
}

// batchFilter applies a filter to an entire batch at once using selection vectors.
type batchFilter func(b *batch.RecordBatch) *batch.RecordBatch

// compileBatchFilters creates batch-level filters from SQL expressions.
// Uses vectorized kernel filters for simple predicates (col op literal, IN,
// BETWEEN, LIKE) and falls back to per-row expression evaluation for complex ones.
func compileBatchFilters(filterExprs []string) []batchFilter {
	filters := make([]batchFilter, 0, len(filterExprs))
	for _, filterSQL := range filterExprs {
		f := buildKernelBatchFilter(filterSQL)
		if f == nil {
			f = buildExprBatchFilter(filterSQL)
			if f == nil {
				continue
			}
		}
		filters = append(filters, f)
	}
	return filters
}

// applyBatchFilters applies batch-level filters sequentially, threading
// the selection vector through each filter.
func applyBatchFilters(b *batch.RecordBatch, filters []batchFilter) *batch.RecordBatch {
	for _, filter := range filters {
		b = filter(b)
		if b == nil {
			return nil
		}
	}
	return b
}

// buildKernelBatchFilter tries to create a vectorized kernel filter from a SQL
// expression. Returns nil if the expression is too complex for kernel evaluation.
func buildKernelBatchFilter(filterSQL string) batchFilter {
	astNode, err := plansql.ParseExpression(filterSQL)
	if err != nil {
		return nil
	}
	return kernelFilterFromAST(astNode)
}

// kernelFilterFromAST recursively extracts kernel-eligible filter patterns from AST nodes.
func kernelFilterFromAST(node plansql.Node) batchFilter {
	switch n := node.(type) {
	case *plansql.CmpExpr:
		col, val, op, ok := extractColOpLit(n)
		if !ok {
			return nil
		}
		kf := exec.NewKernelFilter(col, op, val)
		return wrapUnaryOp(kf)

	case *plansql.InExpr:
		colRef, ok := n.Left.(*plansql.ColRef)
		if !ok {
			return nil
		}
		vals := make([]any, 0, len(n.Values))
		for _, v := range n.Values {
			lit, ok := v.(*plansql.Lit)
			if !ok {
				return nil
			}
			vals = append(vals, parseLitVal(lit))
		}
		inf := exec.NewInFilter(colRefName(colRef), vals, n.Not)
		return wrapUnaryOp(inf)

	case *plansql.BetweenExpr:
		if n.Not {
			return nil
		}
		colRef, ok := n.Left.(*plansql.ColRef)
		if !ok {
			return nil
		}
		lowLit, ok := n.Low.(*plansql.Lit)
		if !ok {
			return nil
		}
		highLit, ok := n.High.(*plansql.Lit)
		if !ok {
			return nil
		}
		col := colRefName(colRef)
		ge := exec.NewKernelFilter(col, exec.OpGe, parseLitVal(lowLit))
		le := exec.NewKernelFilter(col, exec.OpLe, parseLitVal(highLit))
		geBatch := wrapUnaryOp(ge)
		leBatch := wrapUnaryOp(le)
		return func(b *batch.RecordBatch) *batch.RecordBatch {
			b = geBatch(b)
			if b == nil {
				return nil
			}
			return leBatch(b)
		}

	case *plansql.LikeExpr:
		colRef, ok := n.Left.(*plansql.ColRef)
		if !ok {
			return nil
		}
		patLit, ok := n.Pattern.(*plansql.Lit)
		if !ok {
			return nil
		}
		lf := exec.NewLikeFilter(colRefName(colRef), patLit.Value, n.Not)
		return wrapUnaryOp(lf)

	case *plansql.AndNode:
		left := kernelFilterFromAST(n.Left)
		right := kernelFilterFromAST(n.Right)
		if left != nil && right != nil {
			return func(b *batch.RecordBatch) *batch.RecordBatch {
				b = left(b)
				if b == nil {
					return nil
				}
				return right(b)
			}
		}
		return nil

	case *plansql.ParenNode:
		return kernelFilterFromAST(n.Inner)

	default:
		return nil
	}
}

// wrapUnaryOp wraps a filter UnaryOperator (KernelFilter, InFilter, LikeFilter)
// into a batchFilter function.
func wrapUnaryOp(op exec.UnaryOperator) batchFilter {
	return func(b *batch.RecordBatch) *batch.RecordBatch {
		out, err := op.Execute(context.Background(), b)
		if err != nil || out == nil {
			return nil
		}
		return out
	}
}

// extractColOpLit extracts (column_name, literal_value, compare_op) from a CmpExpr.
// Handles both "col op lit" and "lit op col" (flipping the operator).
func extractColOpLit(n *plansql.CmpExpr) (string, any, exec.CompareOp, bool) {
	op := cmpStrToOp(n.Op)
	if op < 0 {
		return "", nil, 0, false
	}
	// col op lit
	if colRef, ok := n.Left.(*plansql.ColRef); ok {
		if lit, ok := n.Right.(*plansql.Lit); ok {
			return colRefName(colRef), parseLitVal(lit), exec.CompareOp(op), true
		}
	}
	// lit op col → flip operator
	if lit, ok := n.Left.(*plansql.Lit); ok {
		if colRef, ok := n.Right.(*plansql.ColRef); ok {
			return colRefName(colRef), parseLitVal(lit), flipOp(exec.CompareOp(op)), true
		}
	}
	return "", nil, 0, false
}

func colRefName(c *plansql.ColRef) string {
	if c.Table != "" {
		return c.Table + "." + c.Column
	}
	return c.Column
}

func cmpStrToOp(op string) exec.CompareOp {
	switch op {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return -1
	}
}

func flipOp(op exec.CompareOp) exec.CompareOp {
	switch op {
	case exec.OpLt:
		return exec.OpGt
	case exec.OpLe:
		return exec.OpGe
	case exec.OpGt:
		return exec.OpLt
	case exec.OpGe:
		return exec.OpLe
	default:
		return op
	}
}

func parseLitVal(lit *plansql.Lit) any {
	switch lit.Kind {
	case plansql.LitNumber:
		if i, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(lit.Value, 64); err == nil {
			return f
		}
		return lit.Value
	case plansql.LitString:
		return lit.Value
	case plansql.LitBool:
		return strings.EqualFold(lit.Value, "true")
	default:
		return lit.Value
	}
}

// buildExprBatchFilter wraps the generic per-row expression evaluator in a
// batchFilter. Used as fallback for complex expressions that can't use kernels.
func buildExprBatchFilter(filterSQL string) batchFilter {
	astNode, err := plansql.ParseExpression(filterSQL)
	if err != nil {
		return nil
	}
	compiled, err := expr.Compile(astNode)
	if err != nil {
		return nil
	}
	return func(b *batch.RecordBatch) *batch.RecordBatch {
		var sel []uint32
		if b.Sel != nil {
			sel = make([]uint32, 0, len(b.Sel))
			for _, idx := range b.Sel {
				v := compiled.Eval(b, int(idx))
				if bv, ok := v.(bool); ok && bv {
					sel = append(sel, idx)
				}
			}
		} else {
			sel = make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				v := compiled.Eval(b, i)
				if bv, ok := v.(bool); ok && bv {
					sel = append(sel, uint32(i))
				}
			}
		}
		if len(sel) == 0 {
			return nil
		}
		b.Sel = sel
		return b
	}
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
	allBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, task.InputFiles, neededCols)
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
		defer func() { result.TaskStats = e.collectTaskStats(spill, tracker) }()
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

	allBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, task.InputFiles, nil)
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
		defer func() { result.TaskStats = e.collectTaskStats(spill, tracker) }()
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

			data, err := e.getFileData(ctx, task.ResultBucket, filePath)
			if err != nil {
				fileResults[idx] = fileResult{err: err}
				return
			}
			data, err = DecompressShuffleData(data)
			if err != nil {
				fileResults[idx] = fileResult{err: fmt.Errorf("decompressing shuffle data: %w", err)}
				return
			}
			var batches []*batch.RecordBatch
			if isShuffleFormat(data) {
				batches, err = shuffleReadBatches(data)
			} else {
				reader, rErr := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
				if rErr != nil {
					fileResults[idx] = fileResult{err: rErr}
					return
				}
				schema := reader.Schema().Columns
				batches, err = scan.ReadFileBatches(reader, schema, nil)
			}
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
		dst.BytesData.SetFrom(di, &src.BytesData, si)
	case batch.TypeDecimal:
		dst.DecimalData.Data[di] = src.DecimalData.Data[si]
	}
}

func (e *Executor) executeJoin(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read build (right) side only — probe side is streamed file-by-file
	// to keep peak memory at O(build + one_probe_file) instead of
	// O(build + all_probe_files).
	buildBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, task.BuildFiles, nil)
	if err != nil {
		return fmt.Errorf("reading build files: %w", err)
	}

	if len(task.InputFiles) == 0 && len(buildBatches) == 0 {
		return nil
	}

	joinType := mapExecJoinType(task.JoinType)

	// For inner/semi/anti joins with no build data, there can be no matches.
	if len(buildBatches) == 0 && joinType != exec.LeftJoin && joinType != exec.FullOuterJoin {
		return nil
	}

	hj := exec.NewHashJoin(joinType, task.JoinLeftKeys, task.JoinRightKeys)
	hj.BuildTableAlias = task.BuildTableAlias

	// Pre-size hash table from build batch row counts to avoid repeated
	// grow+rehash cycles. Without this, the table starts at 64 entries
	// and doubles ~20 times for a 60M-row build side.
	var buildRowCount int64
	for _, b := range buildBatches {
		buildRowCount += int64(b.Len)
	}
	if buildRowCount > 0 {
		hj.BuildRowHint = buildRowCount
	}

	// Wire spill manager if memory budget is set. Scale budget up for
	// fused joins: each fused join builds a hash table that must coexist
	// in memory with the primary join's hash table during probing.
	// Without scaling, a task with 3 fused joins gets 1/4 the effective
	// budget per join, triggering premature spill on every table.
	spill, tracker := e.newSpillManagerScaled(task.ID, 1+len(task.FusedJoins))
	if spill != nil {
		hj.Spill = spill
		hj.MemTracker = tracker
		defer spill.Cleanup()
		defer func() { result.TaskStats = e.collectTaskStats(spill, tracker) }()
	}

	// Set semi/anti join inequality filter (e.g., "l2.l_suppkey != l1.l_suppkey")
	if task.JoinFilter != "" && (joinType == exec.SemiJoin || joinType == exec.AntiJoin) {
		hj.SemiAntiFilter = physical.BuildSemiAntiFilter(task.JoinFilter)
	}

	// Build the hash table from right side batches.
	if err := hj.Build(ctx, &batchSource{batches: buildBatches}); err != nil {
		return fmt.Errorf("building hash table: %w", err)
	}

	// Build fused join hash tables sequentially (broadcast joins absorbed
	// into this task). Sequential construction avoids concurrent peak memory
	// from multiple large hash tables building at once — at SF100, orders
	// (18 GB) + partsupp (10 GB) building simultaneously would OOM before
	// either can trigger a spill.
	fusedProbes := make([]*exec.HashJoinProbe, len(task.FusedJoins))
	fusedTypes := make([]exec.JoinType, len(task.FusedJoins))
	fusedFilters := make([][]string, len(task.FusedJoins))
	for i, fj := range task.FusedJoins {
		fjBuild, err := e.readInputFilesBatches(ctx, task.ResultBucket, fj.BuildFiles, nil)
		if err != nil {
			return fmt.Errorf("reading fused join %d build files: %w", i, err)
		}
		fjType := mapExecJoinType(fj.JoinType)
		fjHJ := exec.NewHashJoin(fjType, fj.JoinLeftKeys, fj.JoinRightKeys)
		fjHJ.BuildTableAlias = fj.BuildTableAlias
		if tracker != nil {
			fjHJ.MemTracker = tracker
			fjDir := e.spillDir
			if fjDir == "" {
				fjDir = os.TempDir()
			}
			if fjSpill, smErr := memory.NewSpillManager(fjDir, tracker); smErr == nil {
				fjHJ.Spill = fjSpill
				defer fjSpill.Cleanup()
			}
		}
		var fjRowCount int64
		for _, b := range fjBuild {
			fjRowCount += int64(b.Len)
		}
		if fjRowCount > 0 {
			fjHJ.BuildRowHint = fjRowCount
		}
		if fj.JoinFilter != "" && (fjType == exec.SemiJoin || fjType == exec.AntiJoin) {
			fjHJ.SemiAntiFilter = physical.BuildSemiAntiFilter(fj.JoinFilter)
		}
		if err := fjHJ.Build(ctx, &batchSource{batches: fjBuild}); err != nil {
			return fmt.Errorf("building fused join %d hash table: %w", i, err)
		}
		fjProbe := fjHJ.Probe()
		if err := fjProbe.Init(ctx); err != nil {
			return fmt.Errorf("init fused join %d probe: %w", i, err)
		}
		fusedProbes[i] = fjProbe
		fusedTypes[i] = fjType
		fusedFilters[i] = fj.FilterExprs
	}

	// For RIGHT and FULL OUTER joins, we may still have results even
	// with no probe rows (unmatched build-side rows).
	if len(task.InputFiles) == 0 && joinType != exec.RightJoin && joinType != exec.FullOuterJoin {
		return nil
	}

	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		return err
	}

	// Extract build-side key ranges for runtime filtering of probe batches.
	// Rows outside the build range can't match — skip them before probing.
	buildRanges := hj.BuildKeyRange()

	var resultBatches []*batch.RecordBatch
	var totalRows int64
	var probeSchema []parquet.Column

	// Stream probe files one at a time to avoid materializing all probe data.
	for _, probeFile := range task.InputFiles {
		fileBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, []string{probeFile}, nil)
		if err != nil {
			return fmt.Errorf("reading probe file %s: %w", probeFile, err)
		}
		for _, pb := range fileBatches {
			if probeSchema == nil {
				probeSchema = pb.Schema
			}
			rb := pb
			// Apply build-side runtime filter: skip probe rows outside build key range.
			if len(buildRanges) > 0 {
				rb = applyRuntimeFilter(rb, buildRanges)
				if rb == nil || rb.ActiveLen() == 0 {
					continue
				}
			}
			// Chain through fused joins FIRST: enrich probe stream with
			// columns from absorbed broadcast joins before the primary probe.
			// Original pipeline: probe → fused_join1 → fused_join2 → primary_join
			for fi, fp := range fusedProbes {
				rb, err = fp.Execute(ctx, rb)
				if err != nil {
					return fmt.Errorf("fused join %d probe: %w", fi, err)
				}
				if rb == nil || rb.ActiveLen() == 0 {
					break
				}
				// Apply per-step filters after the fused join
				if len(fusedFilters[fi]) > 0 {
					filtered, _ := applyFilterExprs([]*batch.RecordBatch{rb}, fusedFilters[fi])
					if len(filtered) == 0 {
						rb = nil
						break
					}
					rb = filtered[0]
				}
			}
			if rb == nil || rb.ActiveLen() == 0 {
				continue
			}
			// Now probe against the primary join's hash table
			rb, err = probe.Execute(ctx, rb)
			if err != nil {
				return err
			}
			if rb != nil && rb.ActiveLen() > 0 {
				totalRows += int64(rb.ActiveLen())
				resultBatches = append(resultBatches, rb)
			}
		}
		// fileBatches goes out of scope here — GC can reclaim before next file
	}

	// For RIGHT and FULL OUTER joins, flush unmatched build-side rows
	if joinType == exec.RightJoin || joinType == exec.FullOuterJoin {
		if probeSchema == nil && len(buildBatches) > 0 {
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
	allBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, task.InputFiles, nil)
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
		defer func() { result.TaskStats = e.collectTaskStats(spill, tracker) }()
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

	// Per-partition binary columnar writers — initialized from first batch's schema.
	var (
		partWriters []*shuffleWriter
		partBufs    []*bytes.Buffer
		keyIdxs     []int
		totalRows   int64
	)

	// Read all input files concurrently (parallel S3 GETs)
	allBatches, err := e.readInputFilesBatches(ctx, task.ResultBucket, task.InputFiles, task.Columns)
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
			partWriters = make([]*shuffleWriter, numPartitions)
			for pid := 0; pid < numPartitions; pid++ {
				partBufs[pid] = &bytes.Buffer{}
				sw := newShuffleWriter(partBufs[pid], schema)
				if err := sw.writeHeader(); err != nil {
					return fmt.Errorf("writing partition %d header: %w", pid, err)
				}
				partWriters[pid] = sw
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

		// Write partitioned rows as columnar chunks (no row materialization)
		for pid := 0; pid < numPartitions; pid++ {
			if len(partitionRows[pid]) == 0 {
				continue
			}
			if err := partWriters[pid].writeChunk(b.Columns, partitionRows[pid], len(partitionRows[pid])); err != nil {
				return fmt.Errorf("writing to partition %d: %w", pid, err)
			}
			totalRows += int64(len(partitionRows[pid]))
		}
	}

	if partWriters == nil {
		return nil // no data
	}

	// Patch chunk counts into each buffer's header
	for pid := 0; pid < numPartitions; pid++ {
		buf := partBufs[pid].Bytes()
		if len(buf) >= 8 {
			binary.LittleEndian.PutUint32(buf[4:8], partWriters[pid].numChunks)
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
		data := CompressShuffleData(partBufs[pid].Bytes())
		if len(data) <= 10 { // header only, no chunks
			continue
		}

		partPath := fmt.Sprintf("%s%s-p%d.wshf", task.ResultPrefix, task.ID, pid)
		uploadResults[pid].path = partPath

		wg.Add(1)
		go func(id int, path string, buf []byte) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Always write to S3 (durable). KV is a best-effort cache.
			_, putErr := e.store.Put(ctx, task.ResultBucket, path,
				bytes.NewReader(buf), int64(len(buf)), "application/octet-stream")
			uploadResults[id].err = putErr
			if putErr != nil {
				return
			}

			// Populate KV cache for fast cross-worker reads.
			if e.resultKV != nil && len(buf) <= natsKVResultThreshold {
				e.resultKV.Put(ctx, natsKVKey(path), buf) // best-effort
			}
			if e.resultStore != nil {
				e.resultStore.Put(task.QueryID, path, buf)
			}
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

// executePipeline runs an entire SQL query as a standalone pipeline on this
// worker. Used for deep join chains where the S3 materialization overhead of
// N distributed stages exceeds the benefit of parallelism. The worker creates
// a catalog from NATS KV, builds a standalone physical plan, and streams the
// entire query through in-memory pipelines — identical to standalone mode.
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

	// Scan-split pipeline mode: read pre-scanned data from distributed scan
	// tasks and inject as materialized inputs. The physical planner will use
	// BatchSource instead of scanning from the object store.
	if len(task.PreScannedInputs) > 0 {
		materializedInputs := make(map[string][]*batch.RecordBatch, len(task.PreScannedInputs))
		for tableName, files := range task.PreScannedInputs {
			batches, readErr := e.readInputFilesBatches(ctx, bucket, files, nil)
			if readErr != nil {
				return fmt.Errorf("reading pre-scanned input for %s: %w", tableName, readErr)
			}
			materializedInputs[tableName] = batches
			e.logger.Debug("loaded pre-scanned input",
				"table", tableName, "files", len(files), "batches", len(batches))
		}
		planner.MaterializedInputs = materializedInputs
	}

	// Probe-split pipeline mode: restrict scan files for the probe table.
	// Each worker reads its assigned partition of the probe table while
	// scanning build tables in full.
	if len(task.ScanFileFilter) > 0 {
		planner.ScanFileFilter = task.ScanFileFilter
		for alias, files := range task.ScanFileFilter {
			e.logger.Info("probe-split scan file filter",
				"task_id", task.ID, "alias", alias, "files", len(files))
		}
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
		"probe_split", len(task.ScanFileFilter) > 0,
		"partial_agg", task.PartialAggregate,
	)

	result.NumRows = totalRows
	if totalRows == 0 {
		return nil
	}

	return e.writeBatchResult(ctx, task, batches, result)
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

	// Use a detached context with generous timeout for S3 writes. The task
	// context can be cancelled (e.g., query abort) while the upload is
	// in-flight, and large results (SF100 joins can produce hundreds of MB)
	// need more time than the HTTP transport's ResponseHeaderTimeout.
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer writeCancel()
	_, err = e.store.Put(writeCtx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
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
	s.batches[s.idx] = nil // release reference so GC can reclaim after spill
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

// collectTaskStats gathers spill/memory stats for a completed task.
// Updates Prometheus counters and returns stats for the result notification.
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
		// Determine type from first non-nil value.
		// Default to float64 for numeric types — CASE expressions can return
		// different numeric types per row (e.g. float64 from THEN, int64 from ELSE 0),
		// so we must use a consistent type across all batches. Only override for
		// non-numeric types (string, bool).
		typ := parquet.TypeFloat64 // default for all numeric expressions
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

// applyRuntimeFilter uses build-side key ranges to pre-filter probe batches.
// Rows whose join key falls outside the build range cannot match and are excluded
// via a selection vector, avoiding unnecessary hash table probes.
func applyRuntimeFilter(b *batch.RecordBatch, ranges []exec.DynamicRange) *batch.RecordBatch {
	if b == nil || b.ActiveLen() == 0 {
		return b
	}
	n := b.ActiveLen()
	var sel []uint32
	for ri := 0; ri < n; ri++ {
		row := ri
		if b.Sel != nil {
			row = int(b.Sel[ri])
		}
		pass := true
		for _, dr := range ranges {
			ci := b.ColumnIndex(dr.Column)
			if ci < 0 {
				continue
			}
			v := b.Columns[ci]
			if v.Nulls.IsNullFast(row) {
				pass = false
				break
			}
			val := v.GetValue(row)
			if scan.CompareValues(val, dr.MinValue) < 0 || scan.CompareValues(val, dr.MaxValue) > 0 {
				pass = false
				break
			}
		}
		if pass {
			sel = append(sel, uint32(row))
		}
	}
	if len(sel) == n {
		return b // all rows pass — no filtering needed
	}
	if len(sel) == 0 {
		return nil
	}
	filtered := *b
	filtered.Sel = sel
	return &filtered
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
