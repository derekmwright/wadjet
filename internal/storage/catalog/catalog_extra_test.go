package catalog

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestNew(t *testing.T) {
	store := objstore.NewMemStore()
	kv := NewMemKV()
	cat := New(kv, store, "bucket")
	if cat == nil {
		t.Fatal("expected non-nil catalog")
	}
	if cat.ClusterID() != "local" {
		t.Fatalf("expected cluster ID 'local', got %q", cat.ClusterID())
	}
}

func TestNewWithCluster_EmptyClusterID(t *testing.T) {
	store := objstore.NewMemStore()
	kv := NewMemKV()
	cat := NewWithCluster(kv, store, "bucket", "")
	if cat.ClusterID() != "local" {
		t.Fatalf("expected cluster ID 'local' for empty input, got %q", cat.ClusterID())
	}
}

func TestStore(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "bucket")
	if cat.Store() != store {
		t.Fatal("Store() should return the underlying store")
	}
}

func TestBucket(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "my-bucket")
	if cat.Bucket() != "my-bucket" {
		t.Fatalf("Bucket() = %q, want %q", cat.Bucket(), "my-bucket")
	}
}

func TestReadFile(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "bucket")
	cat.Init(ctx)

	// Write a file to the store (bucket already created by Init)
	data := []byte("hello file")
	store.Put(ctx, "bucket", "test.txt", bytes.NewReader(data), int64(len(data)), "text/plain")

	rc, info, err := cat.ReadFile(ctx, "test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if info.Size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", info.Size, len(data))
	}
}

func TestSaveAndLoadUDFs(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "bucket")
	ctx := context.Background()
	cat.Init(ctx)

	defs := []UDFDef{
		{Name: "double", Params: []string{"x"}, Body: "x * 2", Owner: "admin", Locked: false},
		{Name: "greet", Params: []string{"name"}, Body: "'Hello ' || name", Owner: "user1", Locked: true},
	}

	if err := cat.SaveUDFs(defs); err != nil {
		t.Fatal(err)
	}

	loaded, err := cat.LoadUDFs()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 UDFs, got %d", len(loaded))
	}
	if loaded[0].Name != "double" {
		t.Fatalf("expected 'double', got %q", loaded[0].Name)
	}
	if loaded[1].Name != "greet" {
		t.Fatalf("expected 'greet', got %q", loaded[1].Name)
	}
	if !loaded[1].Locked {
		t.Fatal("expected greet to be locked")
	}
}

func TestLoadUDFs_Empty(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewWithStore(store, "bucket")
	ctx := context.Background()
	cat.Init(ctx)

	// No UDFs saved yet
	defs, err := cat.LoadUDFs()
	if err != nil {
		t.Fatal(err)
	}
	if defs != nil {
		t.Fatalf("expected nil for no saved UDFs, got %v", defs)
	}
}

func TestGetManifest_NotFound(t *testing.T) {
	cat, ctx := setupCatalog(t)

	_, err := cat.GetManifest(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent manifest")
	}
}

func TestGetRemoteTable_NotFound(t *testing.T) {
	store := objstore.NewMemStore()
	kv := NewMemKV()
	cat := NewWithCluster(kv, store, "bucket", "local")
	ctx := context.Background()
	cat.Init(ctx)

	_, err := cat.GetRemoteTable("remote-cluster", "nonexistent-table")
	if err == nil {
		t.Fatal("expected error for non-existent remote table")
	}
}

func TestGetRemoteManifest_NotFound(t *testing.T) {
	store := objstore.NewMemStore()
	kv := NewMemKV()
	cat := NewWithCluster(kv, store, "bucket", "local")
	ctx := context.Background()
	cat.Init(ctx)

	_, err := cat.GetRemoteManifest("remote-cluster", "nonexistent-table")
	if err == nil {
		t.Fatal("expected error for non-existent remote manifest")
	}
}

func TestListClusters_Empty(t *testing.T) {
	store := objstore.NewMemStore()
	kv := NewMemKV()
	cat := NewWithCluster(kv, store, "bucket", "solo")
	ctx := context.Background()
	cat.Init(ctx)

	clusters, err := cat.ListClusters()
	if err != nil {
		t.Fatal(err)
	}
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters))
	}
	if clusters[0].ClusterID != "solo" {
		t.Fatalf("expected 'solo', got %q", clusters[0].ClusterID)
	}
}

func TestCreateTable_NoPartitionKeys(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "simple", schema, nil); err != nil {
		t.Fatal(err)
	}

	meta, err := cat.GetTable(ctx, "simple")
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.PartitionKeys) != 0 {
		t.Fatalf("expected no partition keys, got %v", meta.PartitionKeys)
	}
}

func TestAddFiles_NewPartition(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	cat.CreateTable(ctx, "data", schema, []string{"region"})

	files1 := []FileEntry{
		{Path: "p1/f1.parquet", SizeBytes: 100, NumRows: 10},
	}
	if err := cat.AddFiles(ctx, "data", map[string]string{"region": "us"}, "p1", files1); err != nil {
		t.Fatal(err)
	}

	files2 := []FileEntry{
		{Path: "p2/f1.parquet", SizeBytes: 200, NumRows: 20},
	}
	if err := cat.AddFiles(ctx, "data", map[string]string{"region": "eu"}, "p2", files2); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(manifest.Partitions))
	}
}

func TestRemoveFiles(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	cat.CreateTable(ctx, "data", schema, nil)

	files := []FileEntry{
		{Path: "f1.parquet", SizeBytes: 100, NumRows: 10},
		{Path: "f2.parquet", SizeBytes: 200, NumRows: 20},
		{Path: "f3.parquet", SizeBytes: 300, NumRows: 30},
	}
	cat.AddFiles(ctx, "data", nil, "", files)

	if err := cat.RemoveFiles(ctx, "data", []string{"f1.parquet", "f3.parquet"}); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}

	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	if totalFiles != 1 {
		t.Fatalf("expected 1 file after removal, got %d", totalFiles)
	}
}

func TestMemKV_Get_NotFound(t *testing.T) {
	kv := NewMemKV()
	_, _, err := kv.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestMemKV_Update_NotFound(t *testing.T) {
	kv := NewMemKV()
	_, err := kv.Update("nonexistent", []byte("data"), 1)
	if err != ErrRevisionMismatch {
		t.Fatalf("expected ErrRevisionMismatch, got %v", err)
	}
}

func TestMemKV_Update_WrongRevision(t *testing.T) {
	kv := NewMemKV()
	kv.Put("key", []byte("v1"))
	_, err := kv.Update("key", []byte("v2"), 999)
	if err != ErrRevisionMismatch {
		t.Fatalf("expected ErrRevisionMismatch, got %v", err)
	}
}

func TestMemKV_Update_Success(t *testing.T) {
	kv := NewMemKV()
	rev1, _ := kv.Put("key", []byte("v1"))

	rev2, err := kv.Update("key", []byte("v2"), rev1)
	if err != nil {
		t.Fatal(err)
	}
	if rev2 <= rev1 {
		t.Fatalf("new revision %d should be > old revision %d", rev2, rev1)
	}

	data, _, err := kv.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "v2" {
		t.Fatalf("got %q, want %q", string(data), "v2")
	}
}

func TestMemKV_Delete(t *testing.T) {
	kv := NewMemKV()
	kv.Put("key", []byte("data"))

	if err := kv.Delete("key"); err != nil {
		t.Fatal(err)
	}

	_, _, err := kv.Get("key")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}

	// Delete non-existent key should not error
	if err := kv.Delete("nonexistent"); err != nil {
		t.Fatalf("delete nonexistent should not error: %v", err)
	}
}

func TestMemKV_List(t *testing.T) {
	kv := NewMemKV()
	kv.Put("a.1", []byte("v"))
	kv.Put("a.2", []byte("v"))
	kv.Put("b.1", []byte("v"))

	// Empty prefix returns all
	all, err := kv.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(all))
	}

	// With prefix
	aKeys, err := kv.List("a.")
	if err != nil {
		t.Fatal(err)
	}
	if len(aKeys) != 2 {
		t.Fatalf("expected 2 keys with prefix 'a.', got %d", len(aKeys))
	}

	// Non-matching prefix
	zKeys, err := kv.List("z.")
	if err != nil {
		t.Fatal(err)
	}
	if len(zKeys) != 0 {
		t.Fatalf("expected 0 keys with prefix 'z.', got %d", len(zKeys))
	}
}

func TestMemKV_CopyOnRead(t *testing.T) {
	kv := NewMemKV()
	kv.Put("key", []byte("original"))

	data, _, _ := kv.Get("key")
	// Mutate the returned slice
	data[0] = 'X'

	// Original should be unchanged
	data2, _, _ := kv.Get("key")
	if string(data2) != "original" {
		t.Fatalf("mutation leaked: got %q, want %q", string(data2), "original")
	}
}

func TestMemKV_Concurrent(t *testing.T) {
	kv := NewMemKV()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			kv.Put(key, []byte("data"))
			kv.Get(key)
			kv.List("")
		}(i)
	}
	wg.Wait()
}

func TestConcurrentAddFiles(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()
	cat.CreateTable(ctx, "events", schema, nil)

	// Simulate concurrent ingest flushes (CAS retry)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			files := []FileEntry{
				{Path: "part-" + string(rune('A'+i)) + ".parquet", SizeBytes: 100, NumRows: 10},
			}
			err := cat.AddFiles(ctx, "events", nil, "", files)
			if err != nil {
				t.Errorf("concurrent AddFiles(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}

	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	if totalFiles != 10 {
		t.Fatalf("expected 10 files after concurrent adds, got %d", totalFiles)
	}
}

func TestDropTable_VerifiesManifestRemoval(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	cat.CreateTable(ctx, "temp", schema, nil)
	cat.AddFiles(ctx, "temp", nil, "", []FileEntry{
		{Path: "file.parquet", SizeBytes: 100, NumRows: 10},
	})

	if err := cat.DropTable(ctx, "temp"); err != nil {
		t.Fatal(err)
	}

	// Both table and manifest should be gone
	_, err := cat.GetTable(ctx, "temp")
	if err == nil {
		t.Fatal("expected error for dropped table")
	}

	_, err = cat.GetManifest(ctx, "temp")
	if err == nil {
		t.Fatal("expected error for dropped table manifest")
	}
}

func TestCreateTable_MultiplePartitionKeys(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "year", Type: parquet.TypeString},
			{Name: "region", Type: parquet.TypeString},
		},
	}

	if err := cat.CreateTable(ctx, "multi", schema, []string{"year", "region"}); err != nil {
		t.Fatal(err)
	}

	meta, err := cat.GetTable(ctx, "multi")
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.PartitionKeys) != 2 {
		t.Fatalf("expected 2 partition keys, got %d", len(meta.PartitionKeys))
	}
}

