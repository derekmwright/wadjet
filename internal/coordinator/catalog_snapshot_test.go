package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
