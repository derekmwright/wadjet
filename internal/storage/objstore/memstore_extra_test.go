package objstore

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
)

func TestMemStore_BucketExists(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	exists, err := store.BucketExists(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("bucket should not exist")
	}

	store.MakeBucket(ctx, "yes")
	exists, err = store.BucketExists(ctx, "yes")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("bucket should exist")
	}
}

func TestMemStore_GetReaderAt(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	content := []byte("hello world from reader at")
	store.Put(ctx, "b", "file.txt", bytes.NewReader(content), int64(len(content)), "text/plain")

	rac, size, err := store.GetReaderAt(ctx, "b", "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rac.Close()

	if size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size, len(content))
	}

	// Read from offset
	buf := make([]byte, 5)
	n, err := rac.ReadAt(buf, 6)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("read %d bytes, want 5", n)
	}
	if string(buf) != "world" {
		t.Fatalf("got %q, want %q", string(buf), "world")
	}
}

func TestMemStore_GetReaderAt_NotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	_, _, err := store.GetReaderAt(ctx, "b", "missing")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemStore_GetReaderAt_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	_, _, err := store.GetReaderAt(ctx, "nope", "key")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_Put_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	_, err := store.Put(ctx, "nope", "key", bytes.NewReader([]byte("data")), 4, "")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_Get_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	_, _, err := store.Get(ctx, "nope", "key")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_Head_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	_, err := store.Head(ctx, "nope", "key")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_Delete_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	err := store.Delete(ctx, "nope", "key")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_List_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	_, err := store.List(ctx, "nope", ListOptions{})
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_PutIfMatch_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	_, err := store.PutIfMatch(ctx, "nope", "key", bytes.NewReader([]byte("data")), 4, "", "")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestMemStore_PutIfMatch_CreateOnly(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	// Create-only should succeed when key doesn't exist
	etag, err := store.PutIfMatch(ctx, "b", "new", bytes.NewReader([]byte("v1")), 2, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}

	// Verify the data
	rc, info, err := store.Get(ctx, "b", "new")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "v1" {
		t.Fatalf("got %q, want %q", string(data), "v1")
	}
	if info.ETag != etag {
		t.Fatalf("ETag mismatch: got %q, want %q", info.ETag, etag)
	}
}

func TestMemStore_PutIfMatch_NonexistentKeyWithETag(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	// Try to update with an etag when key doesn't exist
	_, err := store.PutIfMatch(ctx, "b", "missing", bytes.NewReader([]byte("v1")), 2, "", "some-etag")
	if err != ErrPreconditionFailed {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
}

func TestMemStore_List_MaxKeys(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	for i := 0; i < 10; i++ {
		key := string(rune('a'+i)) + ".txt"
		store.Put(ctx, "b", key, bytes.NewReader([]byte("x")), 1, "")
	}

	items, err := store.List(ctx, "b", ListOptions{MaxKeys: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 items with MaxKeys, got %d", len(items))
	}
}

func TestMemStore_List_Sorted(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	store.Put(ctx, "b", "c.txt", bytes.NewReader([]byte("x")), 1, "")
	store.Put(ctx, "b", "a.txt", bytes.NewReader([]byte("x")), 1, "")
	store.Put(ctx, "b", "b.txt", bytes.NewReader([]byte("x")), 1, "")

	items, err := store.List(ctx, "b", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if items[0].Key != "a.txt" || items[1].Key != "b.txt" || items[2].Key != "c.txt" {
		t.Fatalf("items not sorted: %v, %v, %v", items[0].Key, items[1].Key, items[2].Key)
	}
}

func TestMemStore_List_PrefixAndDelimiter(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	store.Put(ctx, "b", "data/a/1.txt", bytes.NewReader([]byte("x")), 1, "")
	store.Put(ctx, "b", "data/a/2.txt", bytes.NewReader([]byte("x")), 1, "")
	store.Put(ctx, "b", "data/b/1.txt", bytes.NewReader([]byte("x")), 1, "")
	store.Put(ctx, "b", "data/file.txt", bytes.NewReader([]byte("x")), 1, "")

	// With prefix and delimiter: should get common prefixes data/a/ and data/b/ plus data/file.txt
	items, err := store.List(ctx, "b", ListOptions{Prefix: "data/", Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 items (2 prefixes + 1 file), got %d: %v", len(items), items)
	}
}

func TestMemStore_List_EmptyBucket(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "empty")

	items, err := store.List(ctx, "empty", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(items))
	}
}

func TestMemStore_Delete_Nonexistent(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	// Delete non-existent key should not error
	err := store.Delete(ctx, "b", "nonexistent")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestMemStore_MakeBucket_Idempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	if err := store.MakeBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	// Put some data
	store.Put(ctx, "b", "key", bytes.NewReader([]byte("data")), 4, "")

	// MakeBucket again should not destroy existing data
	if err := store.MakeBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	rc, _, err := store.Get(ctx, "b", "key")
	if err != nil {
		t.Fatalf("data should still exist after MakeBucket: %v", err)
	}
	rc.Close()
}

func TestMemStore_Overwrite(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	store.Put(ctx, "b", "key", bytes.NewReader([]byte("v1")), 2, "text/plain")
	etag2, _ := store.Put(ctx, "b", "key", bytes.NewReader([]byte("v2_longer")), 9, "application/json")

	rc, info, err := store.Get(ctx, "b", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "v2_longer" {
		t.Fatalf("got %q, want %q", string(data), "v2_longer")
	}
	if info.ETag != etag2 {
		t.Fatalf("ETag should reflect the latest write")
	}
}

func TestMemStore_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	var wg sync.WaitGroup
	const n = 50

	// Concurrent writes
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			data := []byte("data")
			store.Put(ctx, "b", key, bytes.NewReader(data), int64(len(data)), "")
		}(i)
	}
	wg.Wait()

	// All writes should succeed without panic
	items, err := store.List(ctx, "b", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) == 0 {
		t.Fatal("expected some items after concurrent writes")
	}
}

func TestNopReaderAtCloser_Close(t *testing.T) {
	r := nopReaderAtCloser{bytes.NewReader([]byte("test"))}
	if err := r.Close(); err != nil {
		t.Fatalf("Close() should return nil, got %v", err)
	}
}

func TestMemStore_Head_ETagConsistency(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "b")

	data := []byte("consistent data")
	putEtag, _ := store.Put(ctx, "b", "key", bytes.NewReader(data), int64(len(data)), "text/plain")

	headInfo, err := store.Head(ctx, "b", "key")
	if err != nil {
		t.Fatal(err)
	}

	_, getInfo, err := store.Get(ctx, "b", "key")
	if err != nil {
		t.Fatal(err)
	}

	if putEtag != headInfo.ETag || putEtag != getInfo.ETag {
		t.Fatalf("ETag mismatch: put=%q head=%q get=%q", putEtag, headInfo.ETag, getInfo.ETag)
	}
}
