package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

func TestSnapshotRoundTrip(t *testing.T) {
	src, store := newSnapshotTestCatalog(t)
	ts, err := src.Snapshot(context.Background(), SnapshotOptions{
		Store: store, Bucket: testBucket, Prefix: testPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Fresh KV, same store. Restore should populate it byte-for-byte.
	dstKV := NewMemKV()
	dst := NewWithCluster(dstKV, store, testBucket, "test")
	got, err := dst.Restore(context.Background(), RestoreOptions{
		SnapshotOptions: SnapshotOptions{Store: store, Bucket: testBucket, Prefix: testPrefix},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != ts {
		t.Errorf("restored ts: want %q, got %q", ts, got)
	}

	// Compare every <clusterID>.* key from src vs dst.
	prefix := "test."
	srcKeys, _ := src.kv.List(prefix)
	dstKeys, _ := dst.kv.List(prefix)
	if len(srcKeys) != len(dstKeys) {
		t.Fatalf("key count: src=%d dst=%d", len(srcKeys), len(dstKeys))
	}
	for _, k := range srcKeys {
		a, _, _ := src.kv.Get(k)
		b, _, _ := dst.kv.Get(k)
		if string(a) != string(b) {
			t.Errorf("key %q value mismatch", k)
		}
	}

	// Table is queryable from the restored catalog.
	tm, err := dst.GetTable(context.Background(), "orders")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if tm.Name != "orders" {
		t.Errorf("table name: want orders, got %q", tm.Name)
	}
}

func TestRestoreMissingLatest(t *testing.T) {
	kv := NewMemKV()
	store := objstore.NewMemStore()
	cat := NewWithCluster(kv, store, testBucket, "test")
	// No snapshot written; latest pointer is absent.
	ts, err := cat.Restore(context.Background(), RestoreOptions{
		SnapshotOptions: SnapshotOptions{Store: store, Bucket: testBucket, Prefix: testPrefix},
	})
	if err != nil {
		t.Fatalf("Restore with no latest should be a no-op, got %v", err)
	}
	if ts != "" {
		t.Errorf("no-op ts: want empty, got %q", ts)
	}
}

func TestGCSnapshotsKeepsNewest(t *testing.T) {
	kv := NewMemKV()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), testBucket); err != nil {
		t.Fatal(err)
	}
	cat := NewWithCluster(kv, store, testBucket, "test")

	// Seed 15 fake snapshot directories with decreasing timestamps.
	now := time.Now().UTC()
	for i := 0; i < 15; i++ {
		ts := now.Add(-time.Duration(i) * time.Hour).Format("20060102T150405Z")
		path := testPrefix + "snapshots/" + ts + "/manifest.json"
		_, err := store.Put(context.Background(), testBucket, path,
			bytes.NewReader([]byte("{}")), 2, "application/json")
		if err != nil {
			t.Fatal(err)
		}
	}
	// Also write latest pointing at the newest (0h).
	newestTS := now.Format("20060102T150405Z")
	_, _ = store.Put(context.Background(), testBucket, testPrefix+"latest",
		bytes.NewReader([]byte(newestTS+"\n")), int64(len(newestTS)+1), "text/plain")

	// GC keeping 10 newest + anything <24h. All 15 are <24h old, so nothing deleted.
	if err := cat.GCSnapshots(context.Background(), SnapshotOptions{
		Store: store, Bucket: testBucket, Prefix: testPrefix,
	}, 10, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	objs, _ := store.List(context.Background(), testBucket, objstore.ListOptions{
		Prefix: testPrefix + "snapshots/",
	})
	if len(objs) != 15 {
		t.Errorf("nothing should be deleted when all are <24h: got %d", len(objs))
	}

	// Now GC with minAge=0 — retention becomes strictly "keep 10 newest".
	if err := cat.GCSnapshots(context.Background(), SnapshotOptions{
		Store: store, Bucket: testBucket, Prefix: testPrefix,
	}, 10, 0); err != nil {
		t.Fatal(err)
	}
	objs, _ = store.List(context.Background(), testBucket, objstore.ListOptions{
		Prefix: testPrefix + "snapshots/",
	})
	if len(objs) != 10 {
		t.Errorf("expected 10 snapshots after GC, got %d", len(objs))
	}
}

func TestRestoreCorruptedManifest(t *testing.T) {
	src, store := newSnapshotTestCatalog(t)
	ts, err := src.Snapshot(context.Background(), SnapshotOptions{
		Store: store, Bucket: testBucket, Prefix: testPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Overwrite one of the snapshotted key files with garbage so its SHA no
	// longer matches the manifest entry.
	_, err = store.Put(context.Background(), testBucket,
		testPrefix+"snapshots/"+ts+"/table/orders.json",
		bytes.NewReader([]byte("CORRUPTED")), 9, "application/json")
	if err != nil {
		t.Fatal(err)
	}

	dst := NewWithCluster(NewMemKV(), store, testBucket, "test")
	_, err = dst.Restore(context.Background(), RestoreOptions{
		SnapshotOptions: SnapshotOptions{Store: store, Bucket: testBucket, Prefix: testPrefix},
	})
	if err == nil {
		t.Fatal("want SHA mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "sha") && !strings.Contains(err.Error(), "integrity") {
		t.Errorf("want integrity error, got %v", err)
	}
}
