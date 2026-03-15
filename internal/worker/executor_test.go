package worker

import (
	"bytes"
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
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
