package objstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func setupFileStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestFileStore_MakeBucketAndExists(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()

	exists, err := fs.BucketExists(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("bucket should not exist yet")
	}

	if err := fs.MakeBucket(ctx, "data"); err != nil {
		t.Fatal(err)
	}

	exists, err = fs.BucketExists(ctx, "data")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("bucket should exist after MakeBucket")
	}

	// Idempotent
	if err := fs.MakeBucket(ctx, "data"); err != nil {
		t.Fatal(err)
	}
}

func TestFileStore_PutAndGet(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("hello world")
	etag, err := fs.Put(ctx, "b", "dir/file.txt", bytes.NewReader(content), int64(len(content)), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}

	rc, info, err := fs.Get(ctx, "b", "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, content) {
		t.Fatalf("got %q, want %q", got, content)
	}
	if info.Key != "dir/file.txt" {
		t.Fatalf("key = %q, want %q", info.Key, "dir/file.txt")
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", info.Size, len(content))
	}
	// Get derives its ETag from file metadata (mtime+size) like Head, not
	// content MD5 — hashing would require reading the whole object, which
	// defeats streaming. It must agree with Head for the same object.
	if info.ETag == "" {
		t.Fatal("expected non-empty etag from Get")
	}
	headInfo, err := fs.Head(ctx, "b", "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.ETag != headInfo.ETag {
		t.Fatalf("etag mismatch: get=%q head=%q", info.ETag, headInfo.ETag)
	}
}

// TestFileStore_GetStreams is the regression test for the whole-object Get:
// the reader handed back must be the file itself (an *os.File / io.ReaderAt),
// not a fully-materialized heap copy. The Put-side twin of this bug
// OOM-killed 512 MiB edge nodes on ~280 MB stage outputs (2026-06-11).
func TestFileStore_GetStreams(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("streaming content")
	if _, err := fs.Put(ctx, "b", "file.bin", bytes.NewReader(content), int64(len(content)), ""); err != nil {
		t.Fatal(err)
	}

	rc, info, err := fs.Get(ctx, "b", "file.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	if _, ok := rc.(*os.File); !ok {
		t.Fatalf("Get returned %T; want *os.File (streaming, not a heap copy)", rc)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("got %q, want %q", got, content)
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", info.Size, len(content))
	}
}

func TestFileStore_GetNotFound(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	_, _, err := fs.Get(ctx, "b", "nonexistent")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFileStore_Head(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("test data")
	_, _ = fs.Put(ctx, "b", "file.bin", bytes.NewReader(content), int64(len(content)), "")

	info, err := fs.Head(ctx, "b", "file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", info.Size, len(content))
	}
	// Head() uses stat-based ETag (mtime+size) for speed — just verify non-empty
	if info.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	_, err = fs.Head(ctx, "b", "missing")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFileStore_Delete(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("delete me")
	fs.Put(ctx, "b", "deep/nested/file.txt", bytes.NewReader(content), int64(len(content)), "")

	if err := fs.Delete(ctx, "b", "deep/nested/file.txt"); err != nil {
		t.Fatal(err)
	}

	_, _, err := fs.Get(ctx, "b", "deep/nested/file.txt")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	// Empty parent dirs should be cleaned up
	nestedDir := filepath.Join(fs.rootDir, "b", "deep", "nested")
	if _, err := os.Stat(nestedDir); !os.IsNotExist(err) {
		t.Fatal("expected nested dir to be cleaned up")
	}
	deepDir := filepath.Join(fs.rootDir, "b", "deep")
	if _, err := os.Stat(deepDir); !os.IsNotExist(err) {
		t.Fatal("expected deep dir to be cleaned up")
	}

	// Deleting non-existent key should not error
	if err := fs.Delete(ctx, "b", "nonexistent"); err != nil {
		t.Fatalf("delete nonexistent should not error: %v", err)
	}
}

func TestFileStore_List(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	files := map[string]string{
		"tables/events/year=2026/part-001.parquet": "a",
		"tables/events/year=2026/part-002.parquet": "b",
		"tables/events/year=2025/part-001.parquet": "c",
		"tables/users/part-001.parquet":            "d",
	}
	for key, val := range files {
		fs.Put(ctx, "b", key, bytes.NewReader([]byte(val)), int64(len(val)), "")
	}

	// List all
	result, err := fs.List(ctx, "b", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 4 {
		t.Fatalf("expected 4 objects, got %d", len(result))
	}

	// List with prefix
	result, err = fs.List(ctx, "b", ListOptions{Prefix: "tables/events/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 objects with prefix, got %d", len(result))
	}

	// List with prefix and delimiter
	result, err = fs.List(ctx, "b", ListOptions{Prefix: "tables/", Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	// Should return "tables/events/" and "tables/users/" as common prefixes
	if len(result) != 2 {
		t.Fatalf("expected 2 common prefixes, got %d: %v", len(result), result)
	}

	// List with MaxKeys
	result, err = fs.List(ctx, "b", ListOptions{MaxKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 objects with MaxKeys, got %d", len(result))
	}
}

func TestFileStore_PutIfMatch(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("original")

	// Create-only (empty ETag): should succeed
	etag, err := fs.PutIfMatch(ctx, "b", "file.txt", bytes.NewReader(content), int64(len(content)), "", "")
	if err != nil {
		t.Fatal(err)
	}

	// Create-only again: should fail (already exists)
	_, err = fs.PutIfMatch(ctx, "b", "file.txt", bytes.NewReader(content), int64(len(content)), "", "")
	if err != ErrPreconditionFailed {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	// Update with correct ETag: should succeed
	updated := []byte("updated")
	newEtag, err := fs.PutIfMatch(ctx, "b", "file.txt", bytes.NewReader(updated), int64(len(updated)), "", etag)
	if err != nil {
		t.Fatal(err)
	}
	if newEtag == etag {
		t.Fatal("new etag should differ from old")
	}

	// Update with wrong ETag: should fail
	_, err = fs.PutIfMatch(ctx, "b", "file.txt", bytes.NewReader(updated), int64(len(updated)), "", "wrong")
	if err != ErrPreconditionFailed {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
}

func TestFileStore_NestedKeys(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	// Simulate Parquet file paths with Hive partitioning
	key := "tables/sensor_data/base=afb-east/year=2026/month=03/chunk_0001.parquet"
	content := []byte("parquet-bytes")
	_, err := fs.Put(ctx, "b", key, bytes.NewReader(content), int64(len(content)), "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}

	rc, info, err := fs.Get(ctx, "b", key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch")
	}
	if info.Key != key {
		t.Fatalf("key = %q, want %q", info.Key, key)
	}
}

func TestFileStore_Overwrite(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	v1 := []byte("version 1")
	v2 := []byte("version 2 is longer")

	fs.Put(ctx, "b", "data.bin", bytes.NewReader(v1), int64(len(v1)), "")
	fs.Put(ctx, "b", "data.bin", bytes.NewReader(v2), int64(len(v2)), "")

	rc, info, _ := fs.Get(ctx, "b", "data.bin")
	defer rc.Close()
	got, _ := io.ReadAll(rc)

	if !bytes.Equal(got, v2) {
		t.Fatalf("expected v2, got %q", got)
	}
	if info.Size != int64(len(v2)) {
		t.Fatalf("size = %d, want %d", info.Size, len(v2))
	}
}
