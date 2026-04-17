package coordinator

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestCatalogSnapshotEndToEnd simulates the full cycle:
//  1. coord A populates catalog + snapshots
//  2. coord B starts with empty KV and same S3 prefix; MaybeRestore populates it
//  3. coord B can read the table back
func TestCatalogSnapshotEndToEnd(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), snapBucket); err != nil {
		t.Fatal(err)
	}
	opts := catalog.SnapshotOptions{Store: store, Bucket: snapBucket, Prefix: snapPrefix}

	// A: seed + snapshot
	aKV := catalog.NewMemKV()
	a := catalog.NewWithCluster(aKV, store, snapBucket, "test")
	if err := a.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
	}}
	if err := a.CreateTable(context.Background(), "orders", schema, nil); err != nil {
		t.Fatal(err)
	}
	coordA := &Coordinator{catalog: a}
	coordA.SetCatalogSnapshotOptions(opts)
	resultA, err := coordA.handleCreateSnapshotSQL(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(resultA.Columns) != 3 {
		t.Fatalf("snapshot result columns: want 3, got %d", len(resultA.Columns))
	}

	// B: empty KV, same store, triggers startup restore
	bKV := catalog.NewMemKV()
	b := catalog.NewWithCluster(bKV, store, snapBucket, "test")
	coordB := &Coordinator{catalog: b}
	coordB.SetCatalogSnapshotOptions(opts)
	if err := coordB.MaybeRestoreCatalog(context.Background(), ""); err != nil {
		t.Fatal(err)
	}

	got, err := b.GetTable(context.Background(), "orders")
	if err != nil {
		t.Fatalf("restored table not found: %v", err)
	}
	if got.Name != "orders" || len(got.Schema.Columns) != 2 {
		t.Errorf("unexpected restored table: name=%s cols=%d", got.Name, len(got.Schema.Columns))
	}
}
