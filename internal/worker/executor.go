package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

const inlineResultThreshold = 256 * 1024 // 256 KB

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
	var allRows []map[string]any

	for _, filePath := range task.Files {
		rows, err := e.readParquetFile(ctx, task.ResultBucket, filePath, task.Columns)
		if err != nil {
			return fmt.Errorf("reading file %s: %w", filePath, err)
		}
		allRows = append(allRows, rows...)
	}

	result.NumRows = int64(len(allRows))

	if len(allRows) == 0 {
		return nil
	}

	// Write result to S3
	return e.writeResult(ctx, task, allRows, result)
}

func (e *Executor) executeAggregate(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	// Read input files from previous stage
	var allRows []map[string]any
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

	var cols []parquet.Column
	// Use first row to determine schema
	for k, v := range rows[0] {
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
		cols = append(cols, col)
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
