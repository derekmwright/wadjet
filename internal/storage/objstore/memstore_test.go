package objstore

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestMemStore_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	data := []byte("hello world")
	etag, err := store.Put(ctx, "test", "key1", bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}

	rc, info, err := store.Get(ctx, "test", "key1")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("got size %d, want %d", info.Size, len(data))
	}

	if err := store.Delete(ctx, "test", "key1"); err != nil {
		t.Fatal(err)
	}

	_, _, err = store.Get(ctx, "test", "key1")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemStore_PutIfMatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "test")

	data := []byte("v1")
	etag1, err := store.Put(ctx, "test", "key", bytes.NewReader(data), int64(len(data)), "")
	if err != nil {
		t.Fatal(err)
	}

	// Create-only should fail (object exists)
	_, err = store.PutIfMatch(ctx, "test", "key", bytes.NewReader([]byte("v2")), 2, "", "")
	if err != ErrPreconditionFailed {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	// Wrong etag should fail
	_, err = store.PutIfMatch(ctx, "test", "key", bytes.NewReader([]byte("v2")), 2, "", "wrong-etag")
	if err != ErrPreconditionFailed {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	// Correct etag should succeed
	_, err = store.PutIfMatch(ctx, "test", "key", bytes.NewReader([]byte("v2")), 2, "", etag1)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestMemStore_List(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "test")

	for _, key := range []string{"a/1", "a/2", "b/1", "b/2"} {
		store.Put(ctx, "test", key, bytes.NewReader([]byte("x")), 1, "")
	}

	// List with prefix
	items, err := store.List(ctx, "test", ListOptions{Prefix: "a/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	// List with delimiter
	items, err = store.List(ctx, "test", ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 { // a/ and b/
		t.Fatalf("expected 2 prefixes, got %d", len(items))
	}
}

func TestMemStore_Head(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	store.MakeBucket(ctx, "test")

	store.Put(ctx, "test", "key1", bytes.NewReader([]byte("data")), 4, "text/plain")

	info, err := store.Head(ctx, "test", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 4 {
		t.Fatalf("expected size 4, got %d", info.Size)
	}
	if info.ContentType != "text/plain" {
		t.Fatalf("expected text/plain, got %s", info.ContentType)
	}

	_, err = store.Head(ctx, "test", "nonexistent")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
