package objstore

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestFileStore_GetReaderAt(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("hello reader at world")
	fs.Put(ctx, "b", "file.txt", bytes.NewReader(content), int64(len(content)), "")

	rac, size, err := fs.GetReaderAt(ctx, "b", "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rac.Close()

	if size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size, len(content))
	}

	// Read from offset
	buf := make([]byte, 6)
	n, err := rac.ReadAt(buf, 6)
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("read %d bytes, want 6", n)
	}
	if string(buf) != "reader" {
		t.Fatalf("got %q, want %q", string(buf), "reader")
	}
}

func TestFileStore_GetReaderAt_NotFound(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	_, _, err := fs.GetReaderAt(ctx, "b", "nonexistent")
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFileStore_List_NonexistentBucket(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()

	_, err := fs.List(ctx, "no-such-bucket", ListOptions{})
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

func TestFileStore_BucketExists_NotDir(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()

	// After creation, bucket doesn't exist
	exists, err := fs.BucketExists(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("bucket should not exist")
	}
}

func TestFileStore_List_NoPrefix(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	fs.Put(ctx, "b", "a.txt", bytes.NewReader([]byte("a")), 1, "")
	fs.Put(ctx, "b", "b.txt", bytes.NewReader([]byte("b")), 1, "")

	items, err := fs.List(ctx, "b", ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	// Should be sorted
	if items[0].Key != "a.txt" {
		t.Fatalf("first key = %q, want a.txt", items[0].Key)
	}
}

func TestFileStore_Put_CreatesDirectories(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	// Deep nested path should auto-create directories
	content := []byte("deep data")
	_, err := fs.Put(ctx, "b", "a/b/c/d/e/file.txt", bytes.NewReader(content), int64(len(content)), "")
	if err != nil {
		t.Fatal(err)
	}

	rc, _, err := fs.Get(ctx, "b", "a/b/c/d/e/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(data, content) {
		t.Fatalf("content mismatch")
	}
}

func TestFileStore_PutIfMatch_PreconditionFailed_WrongETag(t *testing.T) {
	fs := setupFileStore(t)
	ctx := context.Background()
	_ = fs.MakeBucket(ctx, "b")

	content := []byte("original")
	fs.Put(ctx, "b", "file.txt", bytes.NewReader(content), int64(len(content)), "")

	// Update with wrong ETag
	_, err := fs.PutIfMatch(ctx, "b", "file.txt", bytes.NewReader([]byte("new")), 3, "", "wrong-etag")
	if err != ErrPreconditionFailed {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
}
