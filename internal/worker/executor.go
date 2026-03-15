package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"sort"
	"strconv"
	"strings"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

const inlineResultThreshold = 64 * 1024 // 64 KB

// Executor dispatches task types to the appropriate execution logic.
type Executor struct {
	store objstore.Store
	cache *LRUCache
}

// NewExecutor creates a new task executor.
func NewExecutor(store objstore.Store, cache *LRUCache) *Executor {
	return &Executor{store: store, cache: cache}
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
	allRows := make([]map[string]any, 0, len(task.Files)*1024) // pre-allocate estimate

	for _, filePath := range task.Files {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, task.Columns)
		if err != nil {
			return fmt.Errorf("reading file %s: %w", filePath, err)
		}
		allRows = append(allRows, rows...)
	}

	// Apply pushed-down filter predicates
	if len(task.FilterExprs) > 0 && len(allRows) > 0 {
		allRows = e.applyRowFilters(allRows, task.FilterExprs)
	}

	result.NumRows = int64(len(allRows))

	if len(allRows) == 0 {
		return nil
	}

	// Write result to S3
	return e.writeResult(ctx, task, allRows, result)
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
	// Read input files from previous stage
	allRows := make([]map[string]any, 0, len(task.InputFiles)*1024)
	for _, filePath := range task.InputFiles {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, nil)
		if err != nil {
			return fmt.Errorf("reading input file %s: %w", filePath, err)
		}
		allRows = append(allRows, rows...)
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
	allRows := make([]map[string]any, 0, len(task.InputFiles)*1024)
	for _, filePath := range task.InputFiles {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, nil)
		if err != nil {
			return fmt.Errorf("reading input file %s: %w", filePath, err)
		}
		allRows = append(allRows, rows...)
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
	// Read build (right) side files
	buildRows := make([]map[string]any, 0, len(task.BuildFiles)*1024)
	for _, filePath := range task.BuildFiles {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, nil)
		if err != nil {
			return fmt.Errorf("reading build file %s: %w", filePath, err)
		}
		buildRows = append(buildRows, rows...)
	}

	// Read probe (left) side files
	probeRows := make([]map[string]any, 0, len(task.InputFiles)*1024)
	for _, filePath := range task.InputFiles {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, nil)
		if err != nil {
			return fmt.Errorf("reading probe file %s: %w", filePath, err)
		}
		probeRows = append(probeRows, rows...)
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
	allRows := make([]map[string]any, 0, len(task.InputFiles)*1024)
	for _, filePath := range task.InputFiles {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, nil)
		if err != nil {
			return fmt.Errorf("reading input file %s: %w", filePath, err)
		}
		allRows = append(allRows, rows...)
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

func (e *Executor) readParquetFile(ctx context.Context, bucket, path string, selectedCols []string) ([]map[string]any, error) {
	// Check cache first
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
	schema := schemaFromRows(rows)

	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		return fmt.Errorf("creating parquet writer: %w", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		return fmt.Errorf("writing rows: %w", err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}

	data := buf.Bytes()
	result.SizeBytes = int64(len(data))

	// Small result fast path: include inline
	if len(data) <= inlineResultThreshold {
		result.InlineData = data
		return nil
	}

	// Write to S3
	resultPath := task.ResultPrefix + task.ID + ".parquet"
	_, err = e.store.Put(ctx, task.ResultBucket, resultPath, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("writing result to S3: %w", err)
	}
	result.ResultPath = resultPath

	return nil
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
