package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// setupTestCatalog creates a catalog with a MemStore and seeds a table.
func setupTestCatalog(t *testing.T) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return cat, store
}

func TestCatalogSnapshotter_TakeSnapshot(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled: true,
		Prefix:  "_catalog/snapshots",
	}, nil)

	snap, err := s.TakeSnapshot(ctx)
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	if snap.ClusterID != "local" {
		t.Errorf("expected cluster_id=local, got %s", snap.ClusterID)
	}
	if snap.Version != 1 {
		t.Errorf("expected version=1, got %d", snap.Version)
	}
	if len(snap.Entries) == 0 {
		t.Fatal("snapshot has no entries")
	}

	// Should contain meta, table.users, manifest.users
	keys := make(map[string]bool)
	for _, e := range snap.Entries {
		keys[e.Key] = true
	}
	for _, want := range []string{"local.meta", "local.table.users", "local.manifest.users"} {
		if !keys[want] {
			t.Errorf("snapshot missing key %q", want)
		}
	}
}

func TestCatalogSnapshotter_ListSnapshots(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled: true,
		Prefix:  "_catalog/snapshots",
	}, nil)

	// Take three snapshots.
	for i := 0; i < 3; i++ {
		if _, err := s.TakeSnapshot(ctx); err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // ensure distinct timestamps
	}

	list, err := s.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(list))
	}

	// Should be newest first.
	if list[0].Key < list[1].Key || list[1].Key < list[2].Key {
		t.Errorf("snapshots not sorted newest-first: %v, %v, %v", list[0].Key, list[1].Key, list[2].Key)
	}
}

func TestCatalogSnapshotter_Prune(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled:   true,
		Prefix:    "_catalog/snapshots",
		Retention: 2,
	}, nil)

	for i := 0; i < 5; i++ {
		if _, err := s.TakeSnapshot(ctx); err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := s.Prune(ctx); err != nil {
		t.Fatalf("Prune: %v", err)
	}

	list, err := s.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots after prune: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 2 snapshots after prune, got %d", len(list))
	}
}

func TestCatalogSnapshotter_RestoreLatest(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled: true,
		Prefix:  "_catalog/snapshots",
	}, nil)

	// Snapshot current state (has "users" table).
	snap, err := s.TakeSnapshot(ctx)
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Add another table after the snapshot.
	schema2 := parquet.Schema{
		Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}},
	}
	if err := cat.CreateTable(ctx, "events", schema2, nil); err != nil {
		t.Fatalf("create events: %v", err)
	}

	// Verify events exists.
	tables, _ := cat.ListTables(ctx)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables before restore, got %d", len(tables))
	}

	// Restore to the snapshot (should lose "events").
	if err := s.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	tables, _ = cat.ListTables(ctx)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table after restore, got %d", len(tables))
	}
	if tables[0] != "users" {
		t.Errorf("expected table=users after restore, got %s", tables[0])
	}
}

func TestCatalogSnapshotter_RestoreLatestFromStorage(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled: true,
		Prefix:  "_catalog/snapshots",
	}, nil)

	if _, err := s.TakeSnapshot(ctx); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Wipe the catalog KV.
	kv := cat.KV()
	keys, _ := kv.List("local.")
	for _, k := range keys {
		_ = kv.Delete(k)
	}

	// Verify catalog is empty.
	tables, _ := cat.ListTables(ctx)
	if len(tables) != 0 {
		t.Fatalf("expected 0 tables after wipe, got %d", len(tables))
	}

	// RestoreLatest should recover.
	restored, err := s.RestoreLatest(ctx)
	if err != nil {
		t.Fatalf("RestoreLatest: %v", err)
	}
	if len(restored.Entries) == 0 {
		t.Fatal("restored snapshot has no entries")
	}

	tables, _ = cat.ListTables(ctx)
	if len(tables) != 1 || tables[0] != "users" {
		t.Errorf("expected [users] after restore, got %v", tables)
	}
}

func TestCatalogSnapshot_JSON_RoundTrip(t *testing.T) {
	snap := &CatalogSnapshot{
		Version:   1,
		ClusterID: "test",
		Timestamp: time.Now().UTC(),
		Entries: []CatalogSnapshotEntry{
			{Key: "test.meta", Value: json.RawMessage(`{"version":1,"tables":["t1"]}`)},
		},
	}

	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded CatalogSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Version != 1 || decoded.ClusterID != "test" || len(decoded.Entries) != 1 {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestCatalogSnapshotter_Disabled(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled: false,
	}, nil)

	// Start should be a no-op.
	s.Start(ctx)

	// No snapshots should exist.
	list, err := s.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected 0 snapshots when disabled, got %d", len(list))
	}
}

func TestCatalogSnapshotter_Debounce(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	s := NewCatalogSnapshotter(cat, store, "test-bucket", SnapshotConfig{
		Enabled:  true,
		Prefix:   "_catalog/snapshots",
		Debounce: 1 * time.Hour, // very long debounce
	}, nil)

	// First snapshot succeeds (sets lastSnap).
	s.trySnapshot(ctx, "test")

	// Debounced snapshot should be skipped because we just snapshotted.
	s.tryDebouncedSnapshot(ctx)

	list, _ := s.ListSnapshots(ctx)
	if len(list) != 1 {
		t.Errorf("expected 1 snapshot (debounce should skip), got %d", len(list))
	}
}

func TestCatalogSnapshotter_DefaultConfig(t *testing.T) {
	cfg := SnapshotConfig{}
	cat, store := setupTestCatalog(t)

	s := NewCatalogSnapshotter(cat, store, "test-bucket", cfg, nil)

	if s.config.Interval != DefaultSnapshotInterval {
		t.Errorf("expected default interval %v, got %v", DefaultSnapshotInterval, s.config.Interval)
	}
	if s.config.Retention != DefaultSnapshotRetention {
		t.Errorf("expected default retention %d, got %d", DefaultSnapshotRetention, s.config.Retention)
	}
	if s.config.Prefix != DefaultSnapshotPrefix {
		t.Errorf("expected default prefix %q, got %q", DefaultSnapshotPrefix, s.config.Prefix)
	}
	if s.config.Debounce != DefaultSnapshotDebounce {
		t.Errorf("expected default debounce %v, got %v", DefaultSnapshotDebounce, s.config.Debounce)
	}
}
