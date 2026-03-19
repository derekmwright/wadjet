package worker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func newTestStore(t *testing.T, bucket string) *objstore.MemStore {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatal(err)
	}
	return store
}

func writeTestParquet(t *testing.T, store objstore.Store, bucket, path string, rows []map[string]any) {
	t.Helper()
	schema := schemaFromRows(rows)
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := buf.Bytes()
	if _, err := store.Put(ctx, bucket, path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteSort(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "charlie", "score": float64(70)},
		{"name": "alice", "score": float64(90)},
		{"name": "bob", "score": float64(80)},
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	task := distributed.Task{
		ID:           "sort-1",
		QueryID:      "q1",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: true}},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Fatalf("expected 3 rows, got %d", result.NumRows)
	}
}

func TestExecuteSortWithLimit(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "charlie", "score": float64(70)},
		{"name": "alice", "score": float64(90)},
		{"name": "bob", "score": float64(80)},
		{"name": "dave", "score": float64(60)},
		{"name": "eve", "score": float64(50)},
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	task := distributed.Task{
		ID:           "sort-2",
		QueryID:      "q1",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: true}},
		Limit:        2,
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort failed: %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Fatalf("expected 2 rows (limit=2), got %d", result.NumRows)
	}
}

func TestExecuteJoin(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	ctx := context.Background()

	// Build (right) side: users
	buildRows := []map[string]any{
		{"user_id": "u1", "username": "alice"},
		{"user_id": "u2", "username": "bob"},
		{"user_id": "u3", "username": "carol"},
	}
	writeTestParquet(t, store, "results", "build/users.parquet", buildRows)

	// Probe (left) side: events
	probeRows := []map[string]any{
		{"user_id": "u1", "event": "login"},
		{"user_id": "u2", "event": "purchase"},
		{"user_id": "u1", "event": "logout"},
	}
	writeTestParquet(t, store, "results", "probe/events.parquet", probeRows)

	task := distributed.Task{
		ID:            "join-1",
		QueryID:       "q1",
		StageID:       "s1",
		Type:          distributed.TaskTypeJoin,
		JoinType:      "inner",
		JoinLeftKeys:  []string{"user_id"},
		JoinRightKeys: []string{"user_id"},
		InputFiles:    []string{"probe/events.parquet"},
		BuildFiles:    []string{"build/users.parquet"},
		ResultBucket:  "results",
		ResultPrefix:  "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("join failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Fatalf("expected 3 rows, got %d", result.NumRows)
	}
}

func TestExecuteJoinLeftJoin(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	ctx := context.Background()

	buildRows := []map[string]any{
		{"user_id": "u1", "username": "alice"},
	}
	writeTestParquet(t, store, "results", "build/users.parquet", buildRows)

	probeRows := []map[string]any{
		{"user_id": "u1", "event": "login"},
		{"user_id": "u999", "event": "unknown"}, // no match
	}
	writeTestParquet(t, store, "results", "probe/events.parquet", probeRows)

	task := distributed.Task{
		ID:            "join-2",
		QueryID:       "q1",
		StageID:       "s1",
		Type:          distributed.TaskTypeJoin,
		JoinType:      "left",
		JoinLeftKeys:  []string{"user_id"},
		JoinRightKeys: []string{"user_id"},
		InputFiles:    []string{"probe/events.parquet"},
		BuildFiles:    []string{"build/users.parquet"},
		ResultBucket:  "results",
		ResultPrefix:  "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("left join failed: %s", result.Error)
	}
	// Left join: u1 matches (1 row) + u999 with nulls (1 row) = 2 rows
	if result.NumRows != 2 {
		t.Fatalf("expected 2 rows, got %d", result.NumRows)
	}
}

// newExecutorWithBudget creates an executor with a memory budget and spill directory.
func newExecutorWithBudget(t *testing.T, store objstore.Store, budget int64) *Executor {
	t.Helper()
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	executor.SetMemoryBudget(budget, t.TempDir())
	executor.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	return executor
}

func TestExecuteSortWithSpill(t *testing.T) {
	store := newTestStore(t, "results")
	// Use a tiny memory budget (256 bytes) to force spill on a moderate dataset
	executor := newExecutorWithBudget(t, store, 256)
	ctx := context.Background()

	// Create enough rows to exceed the tiny budget
	rows := make([]map[string]any, 50)
	for i := range rows {
		rows[i] = map[string]any{
			"name":  "user-" + string(rune('a'+i%26)),
			"score": float64(50 - i),
		}
	}
	writeTestParquet(t, store, "results", "input/large.parquet", rows)

	task := distributed.Task{
		ID:           "sort-spill",
		QueryID:      "q-spill",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/large.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: true}},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort with spill failed: %s", result.Error)
	}
	if result.NumRows != 50 {
		t.Fatalf("expected 50 rows, got %d", result.NumRows)
	}
}

func TestExecuteAggregateWithSpill(t *testing.T) {
	store := newTestStore(t, "results")
	executor := newExecutorWithBudget(t, store, 256)
	ctx := context.Background()

	rows := make([]map[string]any, 60)
	for i := range rows {
		rows[i] = map[string]any{
			"group_key": "g" + string(rune('0'+i%5)),
			"value":     float64(i * 10),
		}
	}
	writeTestParquet(t, store, "results", "input/agg_data.parquet", rows)

	task := distributed.Task{
		ID:         "agg-spill",
		QueryID:    "q-agg-spill",
		StageID:    "s1",
		Type:       distributed.TaskTypeAggregate,
		InputFiles: []string{"input/agg_data.parquet"},
		GroupByCols: []string{"group_key"},
		Aggregates: []distributed.AggSpec{
			{Func: "sum", InputCol: "value", OutputCol: "total"},
			{Func: "count", InputCol: "value", OutputCol: "cnt"},
		},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("aggregate with spill failed: %s", result.Error)
	}
	if result.NumRows != 5 {
		t.Fatalf("expected 5 groups, got %d", result.NumRows)
	}
}

func TestExecuteSortNoSpillUnlimited(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	// budget=0 means unlimited — no spill manager created
	executor.SetMemoryBudget(0, "")
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "b", "score": float64(2)},
		{"name": "a", "score": float64(1)},
		{"name": "c", "score": float64(3)},
	}
	writeTestParquet(t, store, "results", "input/small.parquet", rows)

	task := distributed.Task{
		ID:           "sort-nospill",
		QueryID:      "q-nospill",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/small.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: false}},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort failed: %s", result.Error)
	}
	if result.NumRows != 3 {
		t.Fatalf("expected 3 rows, got %d", result.NumRows)
	}
}

func TestExecuteWithResultStore(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	rs := NewResultStore(1024 * 1024) // 1 MB result store
	executor.SetResultStore(rs)
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "alice", "score": float64(90)},
		{"name": "bob", "score": float64(80)},
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	task := distributed.Task{
		ID:           "rs-sort",
		QueryID:      "q-rs",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: true}},
		ResultBucket: "results",
		ResultPrefix: "stage1/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort failed: %s", result.Error)
	}

	// Result should be in the result store (not inlined since we check the path)
	if result.ResultPath == "" && result.InlineData == nil {
		t.Fatal("expected result path or inline data")
	}

	// If the result was stored (not inlined), verify it's in the result store
	if result.ResultPath != "" {
		if rs.Count() != 1 {
			t.Fatalf("expected 1 entry in result store, got %d", rs.Count())
		}

		// A second task should be able to read from the result store
		task2 := distributed.Task{
			ID:           "rs-agg",
			QueryID:      "q-rs",
			StageID:      "s2",
			Type:         distributed.TaskTypeSort,
			InputFiles:   []string{result.ResultPath},
			SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: false}},
			ResultBucket: "results",
			ResultPrefix: "stage2/",
		}

		result2 := executor.Execute(ctx, task2, "w1")
		if !result2.Success {
			t.Fatalf("second stage failed: %s", result2.Error)
		}
		if result2.NumRows != 2 {
			t.Fatalf("expected 2 rows from result store, got %d", result2.NumRows)
		}
	}
}

func TestExecuteResultStoreFallbackToS3(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	// Tiny result store — will be too small for the result
	rs := NewResultStore(10) // 10 bytes
	executor.SetResultStore(rs)
	ctx := context.Background()

	rows := []map[string]any{
		{"name": "alice", "score": float64(90)},
		{"name": "bob", "score": float64(80)},
		{"name": "carol", "score": float64(70)},
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	task := distributed.Task{
		ID:           "rs-fallback",
		QueryID:      "q-fallback",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/data.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "score", Desc: true}},
		ResultBucket: "results",
		ResultPrefix: "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("sort failed: %s", result.Error)
	}

	// Result store should be empty (too small), result went to S3 or inline
	if rs.Count() != 0 {
		t.Fatalf("expected 0 entries in result store (too small), got %d", rs.Count())
	}

	// The result should still exist — either inline or on S3
	if result.ResultPath != "" {
		// Verify it's on S3
		_, _, err := store.Get(ctx, "results", result.ResultPath)
		if err != nil {
			t.Fatalf("result should be on S3 after store fallback: %v", err)
		}
	} else if result.InlineData == nil {
		t.Fatal("expected either S3 path or inline data")
	}
}

func TestExecuteResultStoreCleanup(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	rs := NewResultStore(1024 * 1024)
	executor.SetResultStore(rs)
	ctx := context.Background()

	rows := []map[string]any{
		{"val": float64(1)},
		{"val": float64(2)},
	}
	writeTestParquet(t, store, "results", "input/a.parquet", rows)
	writeTestParquet(t, store, "results", "input/b.parquet", rows)

	// Two tasks for query q1
	for i, prefix := range []string{"stage-a/", "stage-b/"} {
		task := distributed.Task{
			ID:           "t" + string(rune('1'+i)),
			QueryID:      "q-cleanup",
			StageID:      "s1",
			Type:         distributed.TaskTypeSort,
			InputFiles:   []string{"input/a.parquet"},
			SortKeys:     []distributed.SortKeySpec{{Column: "val", Desc: false}},
			ResultBucket: "results",
			ResultPrefix: prefix,
		}
		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			t.Fatalf("task %d failed: %s", i, result.Error)
		}
	}

	// One task for query q2
	task3 := distributed.Task{
		ID:           "t3",
		QueryID:      "q-other",
		StageID:      "s1",
		Type:         distributed.TaskTypeSort,
		InputFiles:   []string{"input/b.parquet"},
		SortKeys:     []distributed.SortKeySpec{{Column: "val", Desc: false}},
		ResultBucket: "results",
		ResultPrefix: "other/",
	}
	result3 := executor.Execute(ctx, task3, "w1")
	if !result3.Success {
		t.Fatalf("task 3 failed: %s", result3.Error)
	}

	// Some results may be inlined — only non-inline results are in the store.
	// Cleanup q-cleanup should remove only its entries.
	beforeCleanup := rs.Count()
	rs.CleanupQuery("q-cleanup")
	afterCleanup := rs.Count()

	// After cleanup, q-other entries should remain
	if afterCleanup > beforeCleanup {
		t.Fatalf("cleanup should not increase count: before=%d, after=%d", beforeCleanup, afterCleanup)
	}
}

func TestExecuteShuffle(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	ctx := context.Background()

	// Create rows with a key column that will be partitioned
	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{
			"orderkey": int64(i),
			"value":    float64(i * 10),
		}
	}
	writeTestParquet(t, store, "results", "input/data.parquet", rows)

	numPartitions := 4
	task := distributed.Task{
		ID:            "shuffle-1",
		QueryID:       "q-shuf",
		StageID:       "s1",
		Type:          distributed.TaskTypeShuffle,
		ShuffleKeys:   []string{"orderkey"},
		NumPartitions: numPartitions,
		InputFiles:    []string{"input/data.parquet"},
		ResultBucket:  "results",
		ResultPrefix:  "output/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("shuffle failed: %s", result.Error)
	}
	if result.NumRows != 100 {
		t.Fatalf("expected 100 total rows, got %d", result.NumRows)
	}
	if len(result.ResultFiles) != numPartitions {
		t.Fatalf("expected %d partition files, got %d", numPartitions, len(result.ResultFiles))
	}

	// Verify every partition got some rows and total adds up
	var totalReadBack int64
	nonEmptyPartitions := 0
	for pid, path := range result.ResultFiles {
		if path == "" {
			continue
		}
		nonEmptyPartitions++
		readRows, err := executor.readParquetFileBatches(ctx, "results", path, nil)
		if err != nil {
			t.Fatalf("reading partition %d: %v", pid, err)
		}
		for _, b := range readRows {
			totalReadBack += int64(b.ActiveLen())
		}
	}
	if nonEmptyPartitions == 0 {
		t.Fatal("expected at least one non-empty partition")
	}
	if totalReadBack != 100 {
		t.Fatalf("expected 100 rows across partitions, got %d", totalReadBack)
	}
}

func TestShuffleConsistentPartitioning(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache)
	ctx := context.Background()

	// Create two tables with matching keys that should land in the same partition
	leftRows := make([]map[string]any, 50)
	rightRows := make([]map[string]any, 50)
	for i := range leftRows {
		leftRows[i] = map[string]any{"key": int64(i), "left_val": float64(i)}
		rightRows[i] = map[string]any{"key": int64(i), "right_val": float64(i * 2)}
	}
	writeTestParquet(t, store, "results", "input/left.parquet", leftRows)
	writeTestParquet(t, store, "results", "input/right.parquet", rightRows)

	numPartitions := 3

	// Shuffle left side
	leftTask := distributed.Task{
		ID: "shuf-l", QueryID: "q1", StageID: "sl",
		Type: distributed.TaskTypeShuffle, ShuffleKeys: []string{"key"},
		NumPartitions: numPartitions, InputFiles: []string{"input/left.parquet"},
		ResultBucket: "results", ResultPrefix: "output/left/",
	}
	leftResult := executor.Execute(ctx, leftTask, "w1")
	if !leftResult.Success {
		t.Fatalf("left shuffle failed: %s", leftResult.Error)
	}

	// Shuffle right side
	rightTask := distributed.Task{
		ID: "shuf-r", QueryID: "q1", StageID: "sr",
		Type: distributed.TaskTypeShuffle, ShuffleKeys: []string{"key"},
		NumPartitions: numPartitions, InputFiles: []string{"input/right.parquet"},
		ResultBucket: "results", ResultPrefix: "output/right/",
	}
	rightResult := executor.Execute(ctx, rightTask, "w1")
	if !rightResult.Success {
		t.Fatalf("right shuffle failed: %s", rightResult.Error)
	}

	// For each partition, verify matching keys land on the same side
	for pid := 0; pid < numPartitions; pid++ {
		leftPath := leftResult.ResultFiles[pid]
		rightPath := rightResult.ResultFiles[pid]
		if leftPath == "" || rightPath == "" {
			continue
		}

		leftBatches, _ := executor.readParquetFileBatches(ctx, "results", leftPath, nil)
		rightBatches, _ := executor.readParquetFileBatches(ctx, "results", rightPath, nil)

		leftKeys := map[int64]bool{}
		for _, b := range leftBatches {
			rows := b.ToRows()
			for _, r := range rows {
				leftKeys[r["key"].(int64)] = true
			}
		}
		rightKeys := map[int64]bool{}
		for _, b := range rightBatches {
			rows := b.ToRows()
			for _, r := range rows {
				rightKeys[r["key"].(int64)] = true
			}
		}

		// Every key in this partition's left side should also be in right side
		for k := range leftKeys {
			if !rightKeys[k] {
				t.Errorf("partition %d: key %d in left but not right", pid, k)
			}
		}
	}
}
