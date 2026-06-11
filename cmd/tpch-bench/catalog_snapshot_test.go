package main

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

const (
	snapTestBucket = "bench-bucket"
	snapTestPrefix = "catalog/"
)

func newBenchCatalog(t *testing.T, store objstore.Store) *catalog.Catalog {
	t.Helper()
	cat := catalog.NewWithCluster(catalog.NewMemKV(), store, snapTestBucket, "local")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cat
}

// TestRestoreOrDiscoverSequence exercises the boot glue: first boot finds no
// snapshot (falls back to discovery, then snapshots); second boot against a
// fresh KV restores and reports true; a non-empty KV refuses to restore.
func TestRestoreOrDiscoverSequence(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore() // shared "S3" across boots

	// Boot 1: nothing to restore yet.
	cat1 := newBenchCatalog(t, store)
	if restoreCatalogSnapshot(ctx, cat1, store, snapTestBucket, snapTestPrefix) {
		t.Fatal("restore must report false when no snapshot exists")
	}
	// "Discovery" populates the catalog…
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := cat1.CreateTable(ctx, "orders", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat1.AddFiles(ctx, "orders", nil, "", []catalog.FileEntry{
		{Path: "orders/1.parquet", SizeBytes: 123, NumRows: 42},
	}); err != nil {
		t.Fatal(err)
	}
	// …and the boot path snapshots it.
	snapshotCatalog(ctx, cat1, store, snapTestBucket, snapTestPrefix)

	// Boot 2: fresh KV (new deploy), same bucket — restore must succeed and
	// reproduce the discovered state.
	cat2 := newBenchCatalog(t, store)
	if !restoreCatalogSnapshot(ctx, cat2, store, snapTestBucket, snapTestPrefix) {
		t.Fatal("restore must report true when a snapshot exists and KV is fresh")
	}
	tm, err := cat2.GetTable(ctx, "orders")
	if err != nil {
		t.Fatalf("restored catalog missing table: %v", err)
	}
	if len(tm.Schema.Columns) != 1 || tm.Schema.Columns[0].Name != "id" {
		t.Fatalf("restored schema mismatch: %+v", tm.Schema)
	}
	mf, err := cat2.GetManifest(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(mf.Partitions) != 1 || len(mf.Partitions[0].Files) != 1 || mf.Partitions[0].Files[0].NumRows != 42 {
		t.Fatalf("restored manifest mismatch: %+v", mf)
	}

	// Boot 3: catalog already populated (e.g. restore raced something) —
	// refuse to overwrite.
	if restoreCatalogSnapshot(ctx, cat2, store, snapTestBucket, snapTestPrefix) {
		t.Fatal("restore must report false when the KV is not empty")
	}

	// nil store (S3 client construction failed) degrades to disabled.
	if restoreCatalogSnapshot(ctx, cat2, nil, snapTestBucket, snapTestPrefix) {
		t.Fatal("restore with nil store must report false")
	}
	snapshotCatalog(ctx, cat2, nil, snapTestBucket, snapTestPrefix) // must not panic
}
