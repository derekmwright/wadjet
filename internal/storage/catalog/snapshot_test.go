package catalog

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

const testBucket = "test-bucket"
const testPrefix = "wadjet/catalog/"

func newSnapshotTestCatalog(t *testing.T) (*Catalog, objstore.Store) {
	t.Helper()
	kv := NewMemKV()
	store := objstore.NewMemStore()
	cat := NewWithCluster(kv, store, testBucket, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Seed a table so snapshot has non-trivial content.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(context.Background(), "orders", schema, nil); err != nil {
		t.Fatal(err)
	}
	return cat, store
}

func TestSnapshotWritesManifestAndKeys(t *testing.T) {
	cat, store := newSnapshotTestCatalog(t)
	ts, err := cat.Snapshot(context.Background(), SnapshotOptions{
		Store:  store,
		Bucket: testBucket,
		Prefix: testPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ts == "" {
		t.Fatal("empty snapshot timestamp")
	}

	// latest pointer
	r, _, err := store.Get(context.Background(), testBucket, testPrefix+"latest")
	if err != nil {
		t.Fatalf("latest missing: %v", err)
	}
	latestBody, _ := io.ReadAll(r)
	r.Close()
	if got := strings.TrimSpace(string(latestBody)); got != ts {
		t.Errorf("latest pointer: want %q, got %q", ts, got)
	}

	// manifest.json
	r2, _, err := store.Get(context.Background(), testBucket, testPrefix+"snapshots/"+ts+"/manifest.json")
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	mfBody, _ := io.ReadAll(r2)
	r2.Close()

	var mf SnapshotManifest
	if err := json.Unmarshal(mfBody, &mf); err != nil {
		t.Fatal(err)
	}
	if mf.Version != 1 {
		t.Errorf("manifest version: want 1, got %d", mf.Version)
	}
	if mf.Timestamp != ts {
		t.Errorf("manifest ts: want %q, got %q", ts, mf.Timestamp)
	}
	if mf.ClusterID != "test" {
		t.Errorf("manifest cluster_id: want test, got %q", mf.ClusterID)
	}
	if len(mf.Keys) == 0 {
		t.Fatal("manifest lists no keys")
	}

	// Each listed key must exist at its s3_path.
	for _, k := range mf.Keys {
		fullPath := testPrefix + "snapshots/" + ts + "/" + k.S3Path
		_, _, err := store.Get(context.Background(), testBucket, fullPath)
		if err != nil {
			t.Errorf("snapshotted key %q missing at %q: %v", k.KVKey, fullPath, err)
		}
	}
}
