package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

const snapBucket = "test-bench"
const snapPrefix = "wadjet/catalog/"

func newSnapshotTestCoord(t *testing.T) *Coordinator {
	t.Helper()
	kv := catalog.NewMemKV()
	store := objstore.NewMemStore()
	cat := catalog.NewWithCluster(kv, store, snapBucket, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &Coordinator{catalog: cat}
}

func TestRestoreOnStartupIfKVEmpty(t *testing.T) {
	// Produce a snapshot from a seeded source catalog.
	srcKV := catalog.NewMemKV()
	store := objstore.NewMemStore()
	src := catalog.NewWithCluster(srcKV, store, snapBucket, "test")
	if err := src.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	_ = src.CreateTable(context.Background(), "orders", schema, nil)
	_, err := src.Snapshot(context.Background(), catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Fresh coord with empty KV pointing at same S3 prefix.
	dstKV := catalog.NewMemKV()
	dst := catalog.NewWithCluster(dstKV, store, snapBucket, "test")
	// Do NOT call dst.Init — we want the "KV empty" path.
	c := &Coordinator{catalog: dst}

	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	if err := c.MaybeRestoreCatalog(context.Background(), ""); err != nil {
		t.Fatal(err)
	}

	// Table should be queryable.
	if _, err := dst.GetTable(context.Background(), "orders"); err != nil {
		t.Errorf("expected orders table after restore: %v", err)
	}
}

// A real caller runs Catalog.Init before MaybeRestoreCatalog, and Init writes
// the <cluster>.meta key. The startup gate used to ask IsKVEmpty, which that
// key makes false forever, so `wadjet serve --catalog-snapshot-s3-prefix=...`
// against a bucket full of data restored nothing and served an empty table
// list. The gate asks whether the catalog holds tables instead.
func TestRestoreOnStartupAfterInit(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	src := catalog.NewWithCluster(catalog.NewMemKV(), store, snapBucket, "test")
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := src.CreateTable(ctx, "orders", schema, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Snapshot(ctx, catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	}); err != nil {
		t.Fatal(err)
	}

	// Destination is Init'ed, the way every production caller leaves it.
	dst := catalog.NewWithCluster(catalog.NewMemKV(), store, snapBucket, "test")
	if err := dst.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if empty, err := dst.IsKVEmpty(ctx); err != nil || empty {
		t.Fatalf("Init should leave the KV non-empty (empty=%v err=%v)", empty, err)
	}
	c := &Coordinator{catalog: dst}
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	if err := c.MaybeRestoreCatalog(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.GetTable(ctx, "orders"); err != nil {
		t.Errorf("expected orders after restore into an Init'ed catalog: %v", err)
	}
}

func TestRestoreSkippedWhenKVNonEmpty(t *testing.T) {
	// Seed destination catalog with a table (simulates previously-populated KV).
	c := newSnapshotTestCoord(t)
	_ = c.catalog.CreateTable(context.Background(), "preexisting", parquet.Schema{}, nil)

	// Write a snapshot with a different table under the same prefix.
	store := objstore.NewMemStore()
	alt := catalog.NewWithCluster(catalog.NewMemKV(), store, snapBucket, "test")
	_ = alt.Init(context.Background())
	_ = alt.CreateTable(context.Background(), "from_snapshot", parquet.Schema{}, nil)
	_, _ = alt.Snapshot(context.Background(), catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	// Point coord at that snapshot prefix.
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	// Without ForceTS, the non-empty check must skip restore.
	if err := c.MaybeRestoreCatalog(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := c.catalog.GetTable(context.Background(), "from_snapshot"); err == nil {
		t.Error("restore should have been skipped; from_snapshot should not exist")
	}
}

func TestStartStopCatalogSnapshotLoop(t *testing.T) {
	c := newSnapshotTestCoord(t)
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), snapBucket); err != nil {
		t.Fatal(err)
	}
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	c.SetCatalogSnapshotInterval(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.StartCatalogSnapshotLoop(ctx)
	if c.catalogSnapshotCancel == nil {
		t.Fatal("snapshot loop not started")
	}
	// Let at least one tick fire.
	time.Sleep(50 * time.Millisecond)
	c.StopCatalogSnapshotLoop()
	if c.catalogSnapshotCancel != nil {
		t.Error("cancel not cleared")
	}
	// Calling Stop again must be safe.
	c.StopCatalogSnapshotLoop()

	// latest pointer should exist (at least one snapshot was written).
	if _, _, err := store.Get(context.Background(), snapBucket, snapPrefix+"latest"); err != nil {
		t.Errorf("expected latest pointer after loop ran: %v", err)
	}
}

func TestSnapshotLoopNoOpWhenStoreNil(t *testing.T) {
	c := newSnapshotTestCoord(t)
	c.SetCatalogSnapshotInterval(10 * time.Millisecond)
	// Options not set — Store is nil.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.StartCatalogSnapshotLoop(ctx)
	if c.catalogSnapshotCancel != nil {
		t.Error("loop started despite nil Store")
	}
}

func TestHandleCreateSnapshotSQL(t *testing.T) {
	c := newSnapshotTestCoord(t)
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), snapBucket); err != nil {
		t.Fatal(err)
	}
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})

	res, err := c.handleCreateSnapshotSQL(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Columns) != 3 {
		t.Fatalf("want 3 cols (snapshot_id, prefix, key_count), got %d", len(res.Columns))
	}
	rows := mustRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["prefix"] != snapPrefix {
		t.Errorf("prefix col: want %q, got %v", snapPrefix, rows[0]["prefix"])
	}
	// key_count should be >= 1 (at minimum, the meta key).
	switch v := rows[0]["key_count"].(type) {
	case int64:
		if v < 1 {
			t.Errorf("key_count: want >=1, got %d", v)
		}
	case int:
		if v < 1 {
			t.Errorf("key_count: want >=1, got %d", v)
		}
	default:
		t.Errorf("key_count unexpected type %T: %v", v, v)
	}
}

func TestHandleCreateSnapshotWithoutOptions(t *testing.T) {
	c := newSnapshotTestCoord(t)
	_, err := c.handleCreateSnapshotSQL(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Errorf("want 'not configured' error, got %v", err)
	}
}

func TestMaybeRestoreCatalogForceTS(t *testing.T) {
	// Seed a snapshot.
	srcKV := catalog.NewMemKV()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), snapBucket); err != nil {
		t.Fatal(err)
	}
	src := catalog.NewWithCluster(srcKV, store, snapBucket, "test")
	if err := src.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = src.CreateTable(context.Background(), "orders", parquet.Schema{}, nil)
	ts, err := src.Snapshot(context.Background(), catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Valid specific TS restores correctly even when the "latest" pointer would
	// suggest otherwise (we delete latest to prove ForceTS bypasses it).
	_ = store.Delete(context.Background(), snapBucket, snapPrefix+"latest")

	dst := catalog.NewWithCluster(catalog.NewMemKV(), store, snapBucket, "test")
	c := &Coordinator{catalog: dst}
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	if err := c.MaybeRestoreCatalog(context.Background(), ts); err != nil {
		t.Fatalf("ForceTS restore failed: %v", err)
	}
	if _, err := dst.GetTable(context.Background(), "orders"); err != nil {
		t.Errorf("expected restored table: %v", err)
	}
}

func TestMaybeRestoreCatalogForceTSNonexistent(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), snapBucket); err != nil {
		t.Fatal(err)
	}
	dst := catalog.NewWithCluster(catalog.NewMemKV(), store, snapBucket, "test")
	c := &Coordinator{catalog: dst}
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	err := c.MaybeRestoreCatalog(context.Background(), "20260101T000000Z")
	if err == nil {
		t.Error("want error for nonexistent TS, got nil")
	}
}

func TestMaybeRestoreCatalogClusterIDMismatch(t *testing.T) {
	// Write a snapshot with cluster_id "source-cluster".
	srcKV := catalog.NewMemKV()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), snapBucket); err != nil {
		t.Fatal(err)
	}
	src := catalog.NewWithCluster(srcKV, store, snapBucket, "source-cluster")
	if err := src.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := src.Snapshot(context.Background(), catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Destination coordinator has cluster_id "different-cluster" — restore must reject.
	dst := catalog.NewWithCluster(catalog.NewMemKV(), store, snapBucket, "different-cluster")
	c := &Coordinator{catalog: dst}
	c.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
		Store: store, Bucket: snapBucket, Prefix: snapPrefix,
	})
	err = c.MaybeRestoreCatalog(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "cluster_id") {
		t.Errorf("want cluster_id mismatch error, got %v", err)
	}
}
