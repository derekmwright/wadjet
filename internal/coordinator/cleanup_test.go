package coordinator

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

func testStore(t *testing.T, bucket string) *objstore.MemStore {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatal(err)
	}
	return store
}

func putObject(t *testing.T, store objstore.Store, bucket, key string) {
	t.Helper()
	data := []byte("test-data")
	_, err := store.Put(context.Background(), bucket, key,
		bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewResultCleaner(t *testing.T) {
	store := testStore(t, "results")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	rc := NewResultCleaner(store, "results", 0, logger)
	if rc == nil {
		t.Fatal("expected non-nil ResultCleaner")
	}
	if rc.ttl != 1*time.Hour {
		t.Errorf("default TTL: got %v, want 1h", rc.ttl)
	}
}

func TestNewResultCleanerCustomTTL(t *testing.T) {
	store := testStore(t, "results")
	rc := NewResultCleaner(store, "results", 30*time.Minute, nil)
	if rc.ttl != 30*time.Minute {
		t.Errorf("TTL: got %v, want 30m", rc.ttl)
	}
}

func TestNewResultCleanerNilLogger(t *testing.T) {
	store := testStore(t, "results")
	rc := NewResultCleaner(store, "results", 0, nil)
	if rc == nil {
		t.Fatal("expected non-nil ResultCleaner with nil logger")
	}
}

func TestCleanQuery(t *testing.T) {
	store := testStore(t, "results")
	ctx := context.Background()

	putObject(t, store, "results", "queries/q1/scan-0/task-1.parquet")
	putObject(t, store, "results", "queries/q1/scan-0/task-2.parquet")
	putObject(t, store, "results", "queries/q2/scan-0/task-1.parquet")

	rc := NewResultCleaner(store, "results", 0, nil)
	deleted, err := rc.CleanQuery(ctx, "q1")
	if err != nil {
		t.Fatalf("CleanQuery: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}

	// q2 should still exist
	objects, err := store.List(ctx, "results", objstore.ListOptions{Prefix: "queries/q2/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Errorf("expected 1 remaining object, got %d", len(objects))
	}
}

func TestCleanQueryEmpty(t *testing.T) {
	store := testStore(t, "results")
	ctx := context.Background()

	rc := NewResultCleaner(store, "results", 0, nil)
	deleted, err := rc.CleanQuery(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("CleanQuery: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}
}

func TestCleanStale(t *testing.T) {
	store := testStore(t, "results")
	ctx := context.Background()

	// Put some objects
	putObject(t, store, "results", "queries/q-old/scan-0/task-1.parquet")
	putObject(t, store, "results", "queries/q-new/scan-0/task-1.parquet")

	// Use a very short TTL so all objects are "stale"
	rc := NewResultCleaner(store, "results", 1*time.Nanosecond, nil)
	time.Sleep(2 * time.Millisecond)

	deleted, err := rc.CleanStale(ctx)
	if err != nil {
		t.Fatalf("CleanStale: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}
}

func TestCleanStaleNoStaleObjects(t *testing.T) {
	store := testStore(t, "results")
	ctx := context.Background()

	putObject(t, store, "results", "queries/q-fresh/scan-0/task-1.parquet")

	// Long TTL — nothing should be stale
	rc := NewResultCleaner(store, "results", 24*time.Hour, nil)

	deleted, err := rc.CleanStale(ctx)
	if err != nil {
		t.Fatalf("CleanStale: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}
}

func TestCleanStaleSkipsActiveQueries(t *testing.T) {
	store := testStore(t, "results")
	ctx := context.Background()

	putObject(t, store, "results", "queries/q-active/scan-0/task-1.parquet")
	putObject(t, store, "results", "queries/q-active/scan-0/task-2.parquet")
	putObject(t, store, "results", "queries/q-done/scan-0/task-1.parquet")

	rc := NewResultCleaner(store, "results", 1*time.Nanosecond, nil)
	rc.SetActiveQueriesFunc(func() map[string]struct{} {
		return map[string]struct{}{"q-active": {}}
	})
	time.Sleep(2 * time.Millisecond)

	deleted, err := rc.CleanStale(ctx)
	if err != nil {
		t.Fatalf("CleanStale: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted (q-done only), got %d", deleted)
	}

	// q-active files should still exist
	objects, err := store.List(ctx, "results", objstore.ListOptions{Prefix: "queries/q-active/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Errorf("expected 2 active query files preserved, got %d", len(objects))
	}
}

// --- QueryIDFromPath ---

func TestQueryIDFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"queries/q-abc123/scan-0/task-1.parquet", "q-abc123"},
		{"queries/q123/stage-0/task.parquet", "q123"},
		{"other/path.parquet", ""},
		{"queries", ""},
		{"", ""},
		{"queries/q1/", "q1"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := QueryIDFromPath(tt.path)
			if got != tt.want {
				t.Errorf("QueryIDFromPath(%q): got %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestStartPeriodicCleanup(t *testing.T) {
	store := testStore(t, "results")
	ctx, cancel := context.WithCancel(context.Background())

	rc := NewResultCleaner(store, "results", 0, nil)
	rc.StartPeriodicCleanup(ctx, 0) // default interval

	// Just verify it starts and can be cancelled without panic
	time.Sleep(10 * time.Millisecond)
	cancel()
}
