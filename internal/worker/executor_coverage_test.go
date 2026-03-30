package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// --- Execute dispatch and error handling ---

func TestExecuteUnsupportedTaskType(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:      "bad-1",
		QueryID: "q1",
		StageID: "s1",
		Type:    "unknown_type",
	}

	result := executor.Execute(ctx, task, "w1")
	if result.Success {
		t.Fatal("expected failure for unsupported task type")
	}
	if result.Error == "" {
		t.Fatal("expected error message for unsupported task type")
	}
	if result.TaskID != "bad-1" {
		t.Errorf("TaskID: got %q, want %q", result.TaskID, "bad-1")
	}
	if result.WorkerID != "w1" {
		t.Errorf("WorkerID: got %q, want %q", result.WorkerID, "w1")
	}
	if result.Duration <= 0 {
		t.Error("expected non-zero duration")
	}
}

func TestExecuteScanEmptyFiles(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:           "scan-empty",
		QueryID:      "q1",
		StageID:      "s0",
		Type:         distributed.TaskTypeScan,
		Files:        nil,
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("scan with empty files should succeed: %s", result.Error)
	}
	if result.NumRows != 0 {
		t.Errorf("expected 0 rows, got %d", result.NumRows)
	}
}

func TestExecuteScanSingleFile(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}
	writeTestParquet(t, store, "results", "data/file1.parquet", rows)

	task := distributed.Task{
		ID:           "scan-1",
		QueryID:      "q1",
		StageID:      "s0",
		Type:         distributed.TaskTypeScan,
		Files:        []string{"data/file1.parquet"},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("scan failed: %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Errorf("expected 2 rows, got %d", result.NumRows)
	}
}

func TestExecuteScanMissingFile(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:           "scan-miss",
		QueryID:      "q1",
		StageID:      "s0",
		Type:         distributed.TaskTypeScan,
		Files:        []string{"nonexistent.parquet"},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if result.Success {
		t.Fatal("expected failure for missing file")
	}
	if result.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestExecuteScanWithColumnProjection(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "age": int64(30)},
		{"id": int64(2), "name": "bob", "age": int64(25)},
	}
	writeTestParquet(t, store, "results", "data/full.parquet", rows)

	task := distributed.Task{
		ID:           "scan-proj",
		QueryID:      "q1",
		StageID:      "s0",
		Type:         distributed.TaskTypeScan,
		Files:        []string{"data/full.parquet"},
		Columns:      []string{"name"},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("scan with projection failed: %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Errorf("expected 2 rows, got %d", result.NumRows)
	}
}

func TestExecuteAggregateEmptyInput(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:          "agg-empty",
		QueryID:     "q1",
		StageID:     "s1",
		Type:        distributed.TaskTypeAggregate,
		InputFiles:  []string{},
		GroupByCols: []string{"region"},
		Aggregates: []distributed.AggSpec{
			{Func: "sum", InputCol: "amount", OutputCol: "total"},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("aggregate with empty input should succeed: %s", result.Error)
	}
	if result.NumRows != 0 {
		t.Errorf("expected 0 rows, got %d", result.NumRows)
	}
}

func TestExecuteAggregateCount(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"region": "east", "amount": float64(10)},
		{"region": "east", "amount": float64(20)},
		{"region": "west", "amount": float64(30)},
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	task := distributed.Task{
		ID:          "agg-count",
		QueryID:     "q1",
		StageID:     "s1",
		Type:        distributed.TaskTypeAggregate,
		InputFiles:  []string{"input/data.parquet"},
		GroupByCols: []string{"region"},
		Aggregates: []distributed.AggSpec{
			{Func: "count", InputCol: "amount", OutputCol: "cnt"},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("aggregate count failed: %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Errorf("expected 2 groups, got %d", result.NumRows)
	}
}

func TestExecuteAggregateMinMax(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"grp": "a", "val": float64(10)},
		{"grp": "a", "val": float64(50)},
		{"grp": "a", "val": float64(30)},
	}
	writeTestParquet(t, store, "results", "input/minmax.parquet", rows)

	task := distributed.Task{
		ID:          "agg-minmax",
		QueryID:     "q1",
		StageID:     "s1",
		Type:        distributed.TaskTypeAggregate,
		InputFiles:  []string{"input/minmax.parquet"},
		GroupByCols: []string{"grp"},
		Aggregates: []distributed.AggSpec{
			{Func: "min", InputCol: "val", OutputCol: "min_val"},
			{Func: "max", InputCol: "val", OutputCol: "max_val"},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("aggregate min/max failed: %s", result.Error)
	}
	if result.NumRows != 1 {
		t.Errorf("expected 1 group, got %d", result.NumRows)
	}
}

func TestExecuteSortEmptyInput(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:           "sort-empty",
		QueryID:      "q1",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{},
		SortKeys:     []distributed.SortKeySpec{{Column: "val", Desc: false}},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort with empty input should succeed: %s", result.Error)
	}
	if result.NumRows != 0 {
		t.Errorf("expected 0 rows, got %d", result.NumRows)
	}
}

func TestExecuteSortAscending(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "charlie", "score": float64(70)},
		{"name": "alice", "score": float64(90)},
		{"name": "bob", "score": float64(80)},
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	task := distributed.Task{
		ID:           "sort-asc",
		QueryID:      "q1",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: false}},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort ascending failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.NumRows)
	}
}

func TestExecuteJoinEmptyBothSides(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:            "join-empty",
		QueryID:       "q1",
		StageID:       "s1",
		Type:          distributed.TaskTypeJoin,
		JoinType:      "inner",
		JoinLeftKeys:  []string{"id"},
		JoinRightKeys: []string{"id"},
		InputFiles:    []string{},
		BuildFiles:    []string{},
		ResultBucket:  "results",
		ResultPrefix:  "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("join with empty sides should succeed: %s", result.Error)
	}
	if result.NumRows != 0 {
		t.Errorf("expected 0 rows, got %d", result.NumRows)
	}
}

// --- Window execution ---

func TestExecuteWindowBasic(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"dept": "eng", "salary": float64(80000)},
		{"dept": "eng", "salary": float64(90000)},
		{"dept": "sales", "salary": float64(60000)},
	}
	writeTestParquet(t, store, "results", "input/employees.parquet", rows)

	task := distributed.Task{
		ID:         "win-1",
		QueryID:    "q1",
		StageID:    "s1",
		Type:       distributed.TaskTypeWindow,
		InputFiles: []string{"input/employees.parquet"},
		WindowCols: []distributed.WindowColSpec{
			{
				Func:        "row_number",
				OutputCol:   "rn",
				PartitionBy: []string{"dept"},
				OrderBy:     []distributed.SortKeySpec{{Column: "salary", Desc: true}},
			},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("window failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.NumRows)
	}
}

func TestExecuteWindowEmptyInput(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:         "win-empty",
		QueryID:    "q1",
		StageID:    "s1",
		Type:       distributed.TaskTypeWindow,
		InputFiles: []string{},
		WindowCols: []distributed.WindowColSpec{
			{Func: "row_number", OutputCol: "rn"},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("window with empty input should succeed: %s", result.Error)
	}
	if result.NumRows != 0 {
		t.Errorf("expected 0 rows, got %d", result.NumRows)
	}
}

func TestExecuteWindowWithRank(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"dept": "eng", "salary": float64(80000)},
		{"dept": "eng", "salary": float64(80000)},
		{"dept": "eng", "salary": float64(90000)},
	}
	writeTestParquet(t, store, "results", "input/emp.parquet", rows)

	task := distributed.Task{
		ID:         "win-rank",
		QueryID:    "q1",
		StageID:    "s1",
		Type:       distributed.TaskTypeWindow,
		InputFiles: []string{"input/emp.parquet"},
		WindowCols: []distributed.WindowColSpec{
			{
				Func:      "rank",
				OutputCol: "rnk",
				OrderBy:   []distributed.SortKeySpec{{Column: "salary", Desc: true}},
			},
			{
				Func:      "dense_rank",
				OutputCol: "drnk",
				OrderBy:   []distributed.SortKeySpec{{Column: "salary", Desc: true}},
			},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("window with rank failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.NumRows)
	}
}

func TestExecuteWindowAggFunctions(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"dept": "eng", "salary": float64(80000)},
		{"dept": "eng", "salary": float64(90000)},
		{"dept": "sales", "salary": float64(60000)},
	}
	writeTestParquet(t, store, "results", "input/emp2.parquet", rows)

	task := distributed.Task{
		ID:         "win-agg",
		QueryID:    "q1",
		StageID:    "s1",
		Type:       distributed.TaskTypeWindow,
		InputFiles: []string{"input/emp2.parquet"},
		WindowCols: []distributed.WindowColSpec{
			{Func: "sum", InputCol: "salary", OutputCol: "total_salary", PartitionBy: []string{"dept"}},
			{Func: "count", InputCol: "salary", OutputCol: "emp_count", PartitionBy: []string{"dept"}},
			{Func: "avg", InputCol: "salary", OutputCol: "avg_salary", PartitionBy: []string{"dept"}},
			{Func: "min", InputCol: "salary", OutputCol: "min_salary", PartitionBy: []string{"dept"}},
			{Func: "max", InputCol: "salary", OutputCol: "max_salary", PartitionBy: []string{"dept"}},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("window with agg functions failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.NumRows)
	}
}

// --- Helper functions ---

func TestParseWindowFunc(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"row_number"},
		{"rank"},
		{"dense_rank"},
		{"sum"},
		{"count"},
		{"avg"},
		{"min"},
		{"max"},
		{"unknown_func"}, // defaults to row_number
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// Just verify it doesn't panic
			_ = parseWindowFunc(tt.input)
		})
	}
}

func TestWindowOutputType(t *testing.T) {
	// Int64 types
	for _, fn := range []string{"row_number", "rank", "dense_rank", "count"} {
		typ := windowOutputType(fn)
		if typ == 0 {
			t.Errorf("windowOutputType(%q) returned 0", fn)
		}
	}
	// Float64 types
	for _, fn := range []string{"sum", "avg", "min", "max"} {
		typ := windowOutputType(fn)
		if typ == 0 {
			t.Errorf("windowOutputType(%q) returned 0", fn)
		}
	}
}

func TestMapExecJoinType(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"inner"},
		{"left"},
		{"right"},
		{"full"},
		{"cross"},
		{"unknown"}, // defaults to inner
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_ = mapExecJoinType(tt.input)
		})
	}
}

func TestParseAggFunc(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"sum"},
		{"count"},
		{"min"},
		{"max"},
		{"avg"},
		{"unknown"}, // defaults to count
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_ = parseAggFunc(tt.input)
		})
	}
}

func TestAggregateNeededCols(t *testing.T) {
	// With group-by and agg input cols
	cols := aggregateNeededCols(
		[]string{"region", "product"},
		[]distributed.AggSpec{
			{Func: "sum", InputCol: "amount", OutputCol: "total"},
			{Func: "count", InputCol: "id", OutputCol: "cnt"},
			{Func: "sum", InputCol: "amount", OutputCol: "total2"}, // duplicate col
		},
	)
	if len(cols) != 4 { // region, product, amount, id
		t.Errorf("expected 4 unique cols, got %d: %v", len(cols), cols)
	}

	// No group-by, no agg input (count(*))
	cols = aggregateNeededCols(nil, []distributed.AggSpec{
		{Func: "count", InputCol: "", OutputCol: "cnt"},
	})
	if cols != nil {
		t.Errorf("expected nil for no columns, got %v", cols)
	}

	// Only group-by
	cols = aggregateNeededCols([]string{"region"}, nil)
	if len(cols) != 1 {
		t.Errorf("expected 1 col, got %d", len(cols))
	}
}

func TestSchemaFromRows(t *testing.T) {
	// Empty rows
	schema := schemaFromRows(nil)
	if schema != nil {
		t.Errorf("expected nil schema for nil rows, got %v", schema)
	}

	schema = schemaFromRows([]map[string]any{})
	if schema != nil {
		t.Errorf("expected nil schema for empty rows, got %v", schema)
	}

	// Various types
	rows := []map[string]any{
		{
			"bool_col":    true,
			"int32_col":   int32(42),
			"int64_col":   int64(100),
			"int_col":     123,
			"float32_col": float32(1.5),
			"float64_col": float64(2.5),
			"string_col":  "hello",
			"bytes_col":   []byte{0x01, 0x02},
			"unknown_col": struct{}{}, // defaults to string
		},
	}
	schema = schemaFromRows(rows)
	if len(schema) != 9 {
		t.Fatalf("expected 9 columns, got %d", len(schema))
	}
	// Verify sorted
	for i := 1; i < len(schema); i++ {
		if schema[i].Name < schema[i-1].Name {
			t.Errorf("schema not sorted: %q < %q", schema[i].Name, schema[i-1].Name)
		}
	}
}

func TestCompileBatchFilters(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		isNil  bool
	}{
		{"simple_eq", "col = 42", false},
		{"simple_ne", "col != 42", false},
		{"simple_gt", "col > 42", false},
		{"simple_lt", "col < 42", false},
		{"simple_gte", "col >= 42", false},
		{"simple_lte", "col <= 42", false},
		{"string_eq", "name = 'alice'", false},
		{"float_gt", "price > 9.99", false},
		{"qualified_col", "t.col = 42", false},
		{"in_list", "col IN (1, 2, 3)", false},
		{"like_pattern", "name LIKE '%test%'", false},
		{"between", "col BETWEEN 1 AND 10", false},
		{"and_compound", "col > 1 AND col < 10", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filters := compileBatchFilters([]string{tt.filter})
			if tt.isNil && len(filters) > 0 {
				t.Error("expected no filters")
			}
			if !tt.isNil && len(filters) == 0 {
				t.Error("expected at least one filter")
			}
		})
	}
}

func TestKernelBatchFilterApply(t *testing.T) {
	// Verify that kernel batch filters work end-to-end on actual batches
	filters := compileBatchFilters([]string{"col > 5"})
	if len(filters) == 0 {
		t.Fatal("expected filter to compile")
	}

	b := batch.NewRecordBatch([]parquet.Column{{Name: "col", Type: parquet.TypeInt64}}, 10)
	for i := 0; i < 10; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
	}

	result := applyBatchFilters(b, filters)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Sel) != 4 { // 6,7,8,9
		t.Errorf("expected 4 rows, got %d", len(result.Sel))
	}
}

// --- Concurrent file reading ---

func TestReadParquetFilesConcurrentMultiple(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	// Write 3 files
	for i, name := range []string{"a", "b", "c"} {
		rows := []map[string]any{
			{"id": int64(i*10 + 1), "val": float64(i * 100)},
			{"id": int64(i*10 + 2), "val": float64(i*100 + 50)},
		}
		writeTestParquet(t, store, "results", "data/"+name+".parquet", rows)
	}

	rows, err := executor.readParquetFilesConcurrent(ctx, "results",
		[]string{"data/a.parquet", "data/b.parquet", "data/c.parquet"}, nil)
	if err != nil {
		t.Fatalf("readParquetFilesConcurrent: %v", err)
	}
	if len(rows) != 6 {
		t.Errorf("expected 6 rows, got %d", len(rows))
	}
}

func TestReadParquetFilesConcurrentSingleFile(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1)},
	}
	writeTestParquet(t, store, "results", "data/single.parquet", rows)

	result, err := executor.readParquetFilesConcurrent(ctx, "results",
		[]string{"data/single.parquet"}, nil)
	if err != nil {
		t.Fatalf("readParquetFilesConcurrent single: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
}

func TestReadParquetFilesConcurrentEmpty(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	result, err := executor.readParquetFilesConcurrent(ctx, "results", nil, nil)
	if err != nil {
		t.Fatalf("readParquetFilesConcurrent empty: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestReadParquetFilesConcurrentError(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	_, err := executor.readParquetFilesConcurrent(ctx, "results",
		[]string{"missing1.parquet", "missing2.parquet"}, nil)
	if err == nil {
		t.Fatal("expected error for missing files")
	}
}

// --- newSpillManager ---

func TestNewSpillManagerZeroBudget(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(0, "")

	spill, tracker := executor.newSpillManager("task-1")
	if spill != nil {
		t.Error("expected nil spill manager with zero budget")
	}
	if tracker != nil {
		t.Error("expected nil tracker with zero budget")
	}
}

func TestNewSpillManagerWithBudget(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(1024*1024, t.TempDir())

	spill, tracker := executor.newSpillManager("task-2")
	if spill == nil {
		t.Error("expected non-nil spill manager")
	}
	if tracker == nil {
		t.Error("expected non-nil tracker")
	}
	if spill != nil {
		spill.Cleanup()
	}
}

func TestNewSpillManagerDefaultDir(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMemoryBudget(1024*1024, "") // empty dir = use temp dir

	spill, tracker := executor.newSpillManager("task-3")
	if spill == nil {
		t.Error("expected non-nil spill manager with default dir")
	}
	if tracker == nil {
		t.Error("expected non-nil tracker")
	}
	if spill != nil {
		spill.Cleanup()
	}
}

// --- DefaultConfig ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxConcurrent != 4 {
		t.Errorf("MaxConcurrent: got %d, want 4", cfg.MaxConcurrent)
	}
	if cfg.CacheBytes != 256*1024*1024 {
		t.Errorf("CacheBytes: got %d, want %d", cfg.CacheBytes, 256*1024*1024)
	}
}

// --- Scan with filter expressions ---

func TestExecuteScanWithFilterExprs(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "alice", "score": float64(90)},
		{"name": "bob", "score": float64(40)},
		{"name": "carol", "score": float64(70)},
	}
	writeTestParquet(t, store, "results", "input/scores.parquet", rows)

	task := distributed.Task{
		ID:           "scan-filter",
		QueryID:      "q1",
		StageID:      "s0",
		Type:         distributed.TaskTypeScan,
		Files:        []string{"input/scores.parquet"},
		FilterExprs:  []string{"score >= 70"},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("scan with filter failed: %s", result.Error)
	}
	// alice (90) and carol (70) should pass; bob (40) should be filtered out
	if result.NumRows != 2 {
		t.Errorf("expected 2 rows after filter, got %d", result.NumRows)
	}
}

// --- SetMetrics and SetLogger ---

func TestSetMetricsNil(t *testing.T) {
	store := objstore.NewMemStore()
	cache := NewLRUCache(1024)
	executor := NewExecutor(store, cache, nil)
	executor.SetMetrics(nil) // should not panic
}

func TestSetLoggerCustom(t *testing.T) {
	store := objstore.NewMemStore()
	cache := NewLRUCache(1024)
	executor := NewExecutor(store, cache, nil)
	customLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	executor.SetLogger(customLogger) // should not panic
}

// --- Cache Size method ---

func TestLRUCacheSize(t *testing.T) {
	cache := NewLRUCache(100)

	if cache.Size() != 0 {
		t.Errorf("expected 0 size, got %d", cache.Size())
	}

	cache.Put("k1", []byte("hello")) // 5 bytes
	if cache.Size() != 5 {
		t.Errorf("expected 5 size, got %d", cache.Size())
	}

	cache.Put("k2", []byte("world!")) // 6 bytes
	if cache.Size() != 11 {
		t.Errorf("expected 11 size, got %d", cache.Size())
	}
}

func TestLRUCacheUpdateSizeAccounting(t *testing.T) {
	cache := NewLRUCache(100)

	cache.Put("k1", []byte("short"))     // 5 bytes
	cache.Put("k1", []byte("much longer value")) // replace with 17 bytes
	if cache.Size() != 17 {
		t.Errorf("expected 17 after update, got %d", cache.Size())
	}
	if cache.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", cache.Len())
	}
}

// TestExtractColRefsStringLiterals verifies that extractColRefs skips
// single-quoted string literals. Regression test for Q08 SF10 where 'BRAZIL'
// was incorrectly extracted as a column reference.
func TestExtractColRefsStringLiterals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple column",
			input:    "l_shipdate",
			expected: []string{"l_shipdate"},
		},
		{
			name:     "qualified column",
			input:    "n2.n_name",
			expected: []string{"n2.n_name"},
		},
		{
			name:     "expression with function",
			input:    "substr(l_shipdate, 1, 4)",
			expected: []string{"l_shipdate"},
		},
		{
			name:     "CASE WHEN with string literal",
			input:    "CASE WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0 END",
			expected: []string{"n2.n_name", "l_extendedprice", "l_discount"},
		},
		{
			name:     "multiple string literals",
			input:    "CASE WHEN n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY' THEN volume ELSE 0 END",
			expected: []string{"n1.n_name", "n2.n_name", "volume"},
		},
		{
			name:     "expression with no string literals",
			input:    "l_extendedprice * (1 - l_discount)",
			expected: []string{"l_extendedprice", "l_discount"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractColRefs(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("extractColRefs(%q): got %v, want %v", tt.input, got, tt.expected)
			}
			for i, g := range got {
				if g != tt.expected[i] {
					t.Errorf("extractColRefs(%q)[%d]: got %q, want %q", tt.input, i, g, tt.expected[i])
				}
			}
		})
	}
}

// mixedTypeExpr simulates a CASE expression that returns float64 for some rows
// and int64 for others (e.g. CASE WHEN p_type LIKE 'PROMO%' THEN price*discount ELSE 0 END).
type mixedTypeExpr struct {
	// rows where the condition is true return float64; others return int64(0)
	trueRows map[int]bool
}

func (e *mixedTypeExpr) Eval(b *batch.RecordBatch, row int) any {
	if e.trueRows[row] {
		return float64(42.5)
	}
	return int64(0) // ELSE 0 compiles as int64
}

// TestAddComputedColumnsMixedNumericTypes verifies that addComputedColumns
// always produces Float64 vectors for numeric expressions, even when the first
// row returns int64. This prevents type-mismatch panics in downstream aggregate
// kernels (regression test for Q14 distributed aggregate panic).
func TestAddComputedColumnsMixedNumericTypes(t *testing.T) {
	// Batch where row 0 hits the ELSE branch (int64), rows 1-3 hit THEN (float64)
	schema := []parquet.Column{
		{Name: "price", Type: parquet.TypeFloat64},
	}
	b := batch.NewRecordBatch(schema, 4)
	for i := 0; i < 4; i++ {
		b.Columns[0].Float64Data[i] = float64(i + 1)
	}

	exprCols := []aggExprCol{
		{
			name:     "case_result",
			compiled: &mixedTypeExpr{trueRows: map[int]bool{1: true, 2: true, 3: true}},
		},
	}

	out := addComputedColumns(b, exprCols)

	// The computed column must be Float64, not Int64
	compIdx := len(schema) // last column
	if compIdx >= len(out.Columns) {
		t.Fatalf("expected computed column at index %d, got %d columns", compIdx, len(out.Columns))
	}
	vec := out.Columns[compIdx]
	if vec.Type != batch.TypeFloat64 {
		t.Fatalf("computed column type = %v, want Float64 (TypeFloat64)", vec.Type)
	}

	// Row 0: int64(0) should be stored as float64(0)
	if vec.Float64Data[0] != 0.0 {
		t.Errorf("row 0: got %v, want 0.0", vec.Float64Data[0])
	}
	// Row 1: float64(42.5)
	if vec.Float64Data[1] != 42.5 {
		t.Errorf("row 1: got %v, want 42.5", vec.Float64Data[1])
	}

	// Run a second batch where row 0 hits THEN (float64) — type should still be Float64
	b2 := batch.NewRecordBatch(schema, 4)
	for i := 0; i < 4; i++ {
		b2.Columns[0].Float64Data[i] = float64(i + 10)
	}
	exprCols2 := []aggExprCol{
		{
			name:     "case_result",
			compiled: &mixedTypeExpr{trueRows: map[int]bool{0: true, 1: true}},
		},
	}
	out2 := addComputedColumns(b2, exprCols2)
	vec2 := out2.Columns[compIdx]
	if vec2.Type != batch.TypeFloat64 {
		t.Fatalf("batch 2: computed column type = %v, want Float64", vec2.Type)
	}
}

// --- Regression: writeBatchResult always writes to S3 (Q04 fix) ---

func TestWriteBatchResultAlwaysWritesToS3(t *testing.T) {
	// Q04 root cause: results were written to NATS KV only (no S3).
	// KV entries expired after 5-minute TTL, causing "object not found"
	// when downstream pipeline stages read them 11+ minutes later.
	// Fix: always write to S3 as durable store; KV is a cache.

	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	// No NATS KV set — simulates the S3-only path.

	// Create enough batches to exceed inlineResultThreshold (512 KB).
	// Use pseudo-random data to defeat compression.
	nCols := 16
	nRows := 2048
	schema := make([]parquet.Column, nCols)
	for i := range schema {
		schema[i] = parquet.Column{Name: fmt.Sprintf("c%d", i), Type: parquet.TypeInt64}
	}
	var batches []*batch.RecordBatch
	seed := int64(0x517cc1b727220a95) // FNV seed
	for bIdx := 0; bIdx < 10; bIdx++ {
		b := batch.NewRecordBatch(schema, nRows)
		for c := 0; c < nCols; c++ {
			for r := 0; r < nRows; r++ {
				seed = seed*6364136223846793005 + 1442695040888963407 // LCG
				b.Columns[c].Int64Data[r] = seed
			}
		}
		batches = append(batches, b)
	}

	task := distributed.Task{
		ID:           "t1",
		QueryID:      "q1",
		StageID:      "s1",
		ResultBucket: "results",
		ResultPrefix: "queries/q1/s1/",
	}
	var result distributed.ResultNotification

	err := executor.writeBatchResult(context.Background(), task, batches, &result)
	if err != nil {
		t.Fatalf("writeBatchResult: %v", err)
	}

	// Verify the result was written to S3
	if result.ResultPath == "" {
		t.Fatal("expected ResultPath to be set")
	}

	// Verify the file exists in the object store
	rc, _, err := store.Get(context.Background(), "results", result.ResultPath)
	if err != nil {
		t.Fatalf("S3 file missing after writeBatchResult: %v", err)
	}
	rc.Close()
}
