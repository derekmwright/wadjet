// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

func TestLRUCache_BasicOps(t *testing.T) {
	cache := NewLRUCache(100)

	cache.Put("k1", []byte("hello"))
	cache.Put("k2", []byte("world"))

	v, ok := cache.Get("k1")
	if !ok || string(v) != "hello" {
		t.Fatalf("expected 'hello', got %q ok=%v", v, ok)
	}

	_, ok = cache.Get("missing")
	if ok {
		t.Fatal("expected miss for 'missing'")
	}

	if cache.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", cache.Len())
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	cache := NewLRUCache(20) // small cache

	cache.Put("k1", make([]byte, 10))
	cache.Put("k2", make([]byte, 10))

	// Cache is full at 20 bytes. Adding another should evict k1 (LRU)
	cache.Put("k3", make([]byte, 10))

	_, ok := cache.Get("k1")
	if ok {
		t.Fatal("expected k1 to be evicted")
	}

	_, ok = cache.Get("k2")
	if !ok {
		t.Fatal("expected k2 to still be present")
	}
}

func TestLRUCache_CopyOnRead(t *testing.T) {
	cache := NewLRUCache(100)
	cache.Put("k1", []byte("original"))

	// Get returns a copy — mutating it must not affect the cached value
	v1, _ := cache.Get("k1")
	v1[0] = 'X'

	v2, _ := cache.Get("k1")
	if string(v2) != "original" {
		t.Fatalf("cache corrupted: expected 'original', got %q", v2)
	}
}

func TestLRUCache_Update(t *testing.T) {
	cache := NewLRUCache(100)

	cache.Put("k1", []byte("v1"))
	cache.Put("k1", []byte("v2-updated"))

	v, ok := cache.Get("k1")
	if !ok || string(v) != "v2-updated" {
		t.Fatalf("expected 'v2-updated', got %q", v)
	}

	if cache.Len() != 1 {
		t.Fatalf("expected 1 entry after update, got %d", cache.Len())
	}
}

func TestLRUCache_GetRef(t *testing.T) {
	cache := NewLRUCache(100)
	cache.Put("k1", []byte("hello"))

	// GetRef returns the original slice (no copy)
	v1, ok := cache.GetRef("k1")
	if !ok || string(v1) != "hello" {
		t.Fatalf("expected 'hello', got %q ok=%v", v1, ok)
	}

	// Verify it's the same underlying array as what's cached
	v2, _ := cache.GetRef("k1")
	if &v1[0] != &v2[0] {
		t.Fatal("GetRef should return the same slice, not a copy")
	}

	// Miss returns nil
	_, ok = cache.GetRef("missing")
	if ok {
		t.Fatal("expected miss for 'missing'")
	}
}

func TestCachedStore_Get(t *testing.T) {
	mem := objstore.NewMemStore()
	ctx := context.Background()
	mem.MakeBucket(ctx, "b")
	putString(ctx, t, mem, "b", "file1.parquet", "parquet-data-1")

	cache := NewLRUCache(1024 * 1024)
	cs := NewCachedStore(mem, cache, nil)

	// First read: cache miss, reads from inner store
	rc, info, err := cs.Get(ctx, "b", "file1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "parquet-data-1" {
		t.Fatalf("expected 'parquet-data-1', got %q", data)
	}
	if info.Size != 14 {
		t.Fatalf("expected size 14, got %d", info.Size)
	}

	// Second read: cache hit
	rc2, _, err := cs.Get(ctx, "b", "file1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	data2, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(data2) != "parquet-data-1" {
		t.Fatalf("cache hit returned wrong data: %q", data2)
	}

	// Verify it's in the LRU cache
	if cache.Len() != 1 {
		t.Fatalf("expected 1 cache entry, got %d", cache.Len())
	}
}

func TestCachedStore_GetReaderAt(t *testing.T) {
	mem := objstore.NewMemStore()
	ctx := context.Background()
	mem.MakeBucket(ctx, "b")
	putString(ctx, t, mem, "b", "file1.parquet", "0123456789ABCDEF")

	cache := NewLRUCache(1024 * 1024)
	cs := NewCachedStore(mem, cache, nil)

	// GetReaderAt downloads full file and caches it
	ra, size, err := cs.GetReaderAt(ctx, "b", "file1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if size != 16 {
		t.Fatalf("expected size 16, got %d", size)
	}

	// Read a range from the middle
	buf := make([]byte, 4)
	n, err := ra.ReadAt(buf, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || string(buf) != "ABCD" {
		t.Fatalf("expected 'ABCD', got %q (n=%d)", buf, n)
	}
	ra.Close()

	// Second call should hit cache
	ra2, _, err := cs.GetReaderAt(ctx, "b", "file1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	n, err = ra2.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != "0123" {
		t.Fatalf("expected '0123', got %q", buf)
	}
	ra2.Close()
}

func TestCachedStore_CrossQueryReuse(t *testing.T) {
	mem := objstore.NewMemStore()
	ctx := context.Background()
	mem.MakeBucket(ctx, "b")
	putString(ctx, t, mem, "b", "table/chunk1.parquet", "chunk-data")

	cache := NewLRUCache(1024 * 1024)

	// Simulate query 1: creates CachedStore, reads file
	cs1 := NewCachedStore(mem, cache, nil)
	rc, _, _ := cs1.Get(ctx, "b", "table/chunk1.parquet")
	io.ReadAll(rc)
	rc.Close()

	// Delete from backing store to prove cache is serving data
	mem.Delete(ctx, "b", "table/chunk1.parquet")

	// Simulate query 2: new CachedStore, same cache — should hit
	cs2 := NewCachedStore(mem, cache, nil)
	rc2, _, err := cs2.Get(ctx, "b", "table/chunk1.parquet")
	if err != nil {
		t.Fatalf("expected cache hit after backing store delete: %v", err)
	}
	data, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(data) != "chunk-data" {
		t.Fatalf("expected 'chunk-data', got %q", data)
	}
}

func putString(ctx context.Context, t *testing.T, s objstore.Store, bucket, key, val string) {
	t.Helper()
	_, err := s.Put(ctx, bucket, key, io.NopCloser(strings.NewReader(val)), int64(len(val)), "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
}

// TestCachedStore_DownloadTransientCharged verifies the download buffer is
// charged to the tracker for the window before cache insertion and fully
// released once the LRU cache owns the bytes.
func TestCachedStore_DownloadTransientCharged(t *testing.T) {
	mem := objstore.NewMemStore()
	ctx := context.Background()
	mem.MakeBucket(ctx, "b")
	putString(ctx, t, mem, "b", "file1.parquet", "parquet-data-1")

	cache := NewLRUCache(1024 * 1024)
	tracker := memory.NewTracker("worker-test", 1<<30)
	cs := NewCachedStore(mem, cache, tracker)

	rc, _, err := cs.Get(ctx, "b", "file1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()

	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after Get, want 0 (cache owns the bytes)", used)
	}
	if tracker.Peak() <= 0 {
		t.Fatal("peak = 0 — download transient was never charged")
	}

	// Cache hit must not touch the tracker.
	peak := tracker.Peak()
	rc, _, err = cs.Get(ctx, "b", "file1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if tracker.Used() != 0 || tracker.Peak() != peak {
		t.Fatal("cache hit must not charge the tracker")
	}
}

// sizeStore serves one body with a chosen reported size: 0 takes readFully's
// unknown-size path, a size past the body's end with a failing reader takes
// its read-error path.
type sizeStore struct {
	objstore.Store
	body io.Reader
	size int64
}

func (s *sizeStore) Get(_ context.Context, _, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	return io.NopCloser(s.body), objstore.ObjectInfo{Key: key, Size: s.size}, nil
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("injected: read failed") }

// TestCachedStore_DownloadForcedCensusBalanced (#1685): a download buffer is a
// forced charge on the worker-lifetime tracker, and every way readFully gives
// it back (the sized and unknown-size hand-offs to the cache, the sized read
// error) gives it back AS a forced charge. A plain release left the forced
// census growing by every download the worker ever made, so a refusal on
// that tracker named bytes that were long gone.
func TestCachedStore_DownloadForcedCensusBalanced(t *testing.T) {
	const payload = "parquet-data-1"
	for _, tc := range []struct {
		name    string
		store   objstore.Store
		wantErr bool
	}{
		{"sized", &sizeStore{body: strings.NewReader(payload), size: int64(len(payload))}, false},
		{"unknown-size", &sizeStore{body: strings.NewReader(payload), size: 0}, false},
		{"read-error", &sizeStore{body: failingReader{}, size: 1 << 20}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := memory.NewTracker("worker", 1<<30)
			cs := NewCachedStore(tc.store, NewLRUCache(1<<20), tracker)
			rc, _, err := cs.Get(context.Background(), "b", "k")
			if (err != nil) != tc.wantErr {
				t.Fatalf("Get error = %v, want error %v", err, tc.wantErr)
			}
			if rc != nil {
				rc.Close()
			}
			if tracker.Peak() <= 0 {
				t.Fatal("the download was never charged")
			}
			if used := tracker.Used(); used != 0 {
				t.Errorf("used = %d, want 0", used)
			}
			if total, top, n := tracker.ForcedBytes(); total != 0 {
				t.Errorf("forced census = %d (largest %d by %q), want 0", total, n, top.String())
			}
		})
	}
}
