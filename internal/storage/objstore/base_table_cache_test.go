package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// countingStore wraps a Store and counts Get / GetReaderAt calls so tests
// can assert whether the cache consulted the inner store.
type countingStore struct {
	Store
	gets      atomic.Int64
	readerAts atomic.Int64
	// sizeInflate, when non-zero, is added to the ObjectInfo.Size that Get
	// advertises — simulating a store whose metadata disagrees with the
	// body it streams.
	sizeInflate int64
}

func (s *countingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	s.gets.Add(1)
	rc, info, err := s.Store.Get(ctx, bucket, key)
	info.Size += s.sizeInflate
	return rc, info, err
}

func (s *countingStore) GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	s.readerAts.Add(1)
	return s.Store.(ReaderAtStore).GetReaderAt(ctx, bucket, key)
}

func newTestCache(tb testing.TB, budget int64) (*BaseTableCache, *countingStore, string) {
	tb.Helper()
	dir := tb.TempDir()
	inner := &countingStore{Store: NewMemStore()}
	c, err := NewBaseTableCache(inner, dir, budget, nil)
	if err != nil {
		tb.Fatalf("NewBaseTableCache: %v", err)
	}
	return c, inner, dir
}

func putObject(tb testing.TB, s Store, bucket, key string, data []byte) {
	tb.Helper()
	ctx := context.Background()
	if err := s.MakeBucket(ctx, bucket); err != nil {
		tb.Fatalf("MakeBucket: %v", err)
	}
	if _, err := s.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		tb.Fatalf("Put: %v", err)
	}
}

// getAll reads an object through the cache to EOF and closes it, the way
// every scan-path consumer does.
func getAll(tb testing.TB, c *BaseTableCache, bucket, key string) []byte {
	tb.Helper()
	rc, info, err := c.Get(context.Background(), bucket, key)
	if err != nil {
		tb.Fatalf("Get(%s/%s): %v", bucket, key, err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		tb.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	if info.Size > 0 && int64(len(data)) != info.Size {
		tb.Fatalf("body length %d != advertised size %d", len(data), info.Size)
	}
	return data
}

func TestBaseTableCacheRoundTrip(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := bytes.Repeat([]byte("wadjet"), 1000)
	putObject(t, inner.Store, "data", "tables/lineitem/chunk_abc.parquet", data)

	if got := getAll(t, c, "data", "tables/lineitem/chunk_abc.parquet"); !bytes.Equal(got, data) {
		t.Fatal("miss body mismatch")
	}
	if got := getAll(t, c, "data", "tables/lineitem/chunk_abc.parquet"); !bytes.Equal(got, data) {
		t.Fatal("hit body mismatch")
	}
	if inner.gets.Load() != 1 {
		t.Fatalf("inner gets = %d, want 1 (second read must be a cache hit)", inner.gets.Load())
	}
	s := c.Stats()
	if s.Hits != 1 || s.Misses != 1 || s.Entries != 1 {
		t.Fatalf("stats = %+v, want 1 hit / 1 miss / 1 entry", s)
	}
	if s.HitBytes != int64(len(data)) || s.MissBytes != int64(len(data)) {
		t.Fatalf("byte stats = %+v, want %d each", s, len(data))
	}
}

func TestBaseTableCacheEligibility(t *testing.T) {
	c, inner, dir := newTestCache(t, 1<<20)
	cases := []struct {
		name   string
		bucket string
		key    string
	}{
		{"non-parquet", "data", "tables/t/chunk.wshf"},
		{"query scratch", "data", "queries/q1/stage-2/part.parquet"},
		{"dot-dot segment", "data", "tables/../escape.parquet"},
		{"empty segment", "data", "tables//x.parquet"},
		{"bucket with slash", "da/ta", "tables/t/x.parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			putObject(t, inner.Store, tc.bucket, tc.key, []byte("payload"))
			before := inner.gets.Load()
			if got := getAll(t, c, tc.bucket, tc.key); string(got) != "payload" {
				t.Fatal("body mismatch")
			}
			if got := getAll(t, c, tc.bucket, tc.key); string(got) != "payload" {
				t.Fatal("body mismatch on second read")
			}
			if inner.gets.Load() != before+2 {
				t.Fatalf("ineligible key was cached: inner gets went %d -> %d", before, inner.gets.Load())
			}
		})
	}
	if s := c.Stats(); s.Entries != 0 {
		t.Fatalf("ineligible keys created %d entries", s.Entries)
	}
	// Nothing may have escaped or landed under the cache dir.
	var files []string
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if len(files) != 0 {
		t.Fatalf("ineligible keys left files on disk: %v", files)
	}
}

func TestBaseTableCacheEarlyCloseDiscards(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := bytes.Repeat([]byte("x"), 64<<10)
	putObject(t, inner.Store, "data", "t/a.parquet", data)

	rc, _, err := c.Get(context.Background(), "data", "t/a.parquet")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	buf := make([]byte, 1024)
	if _, err := rc.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}
	rc.Close() // abandon before EOF

	if got := getAll(t, c, "data", "t/a.parquet"); !bytes.Equal(got, data) {
		t.Fatal("body mismatch after abandoned read")
	}
	if inner.gets.Load() != 2 {
		t.Fatalf("inner gets = %d, want 2 (abandoned read must not populate)", inner.gets.Load())
	}
}

func TestBaseTableCacheSizeMismatchDiscards(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	inner.sizeInflate = 17
	data := []byte("short body")
	putObject(t, inner.Store, "data", "t/a.parquet", data)

	if got := getAll2(t, c, "data", "t/a.parquet"); !bytes.Equal(got, data) {
		t.Fatal("body mismatch")
	}
	if s := c.Stats(); s.Entries != 0 {
		t.Fatalf("size-mismatched body was admitted: %+v", s)
	}
	if got := getAll2(t, c, "data", "t/a.parquet"); !bytes.Equal(got, data) {
		t.Fatal("body mismatch on re-read")
	}
	if inner.gets.Load() != 2 {
		t.Fatalf("inner gets = %d, want 2", inner.gets.Load())
	}
}

// getAll2 is getAll without the size cross-check (for tests that
// deliberately advertise a wrong size).
func getAll2(tb testing.TB, c *BaseTableCache, bucket, key string) []byte {
	tb.Helper()
	rc, _, err := c.Get(context.Background(), bucket, key)
	if err != nil {
		tb.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		tb.Fatalf("ReadAll: %v", err)
	}
	return data
}

func TestBaseTableCacheEvictionLRU(t *testing.T) {
	obj := func(i int) (string, []byte) {
		return fmt.Sprintf("t/chunk_%d.parquet", i), bytes.Repeat([]byte{byte('a' + i)}, 400)
	}
	c, inner, _ := newTestCache(t, 1000) // fits two 400-byte entries, not three
	for i := 0; i < 2; i++ {
		key, data := obj(i)
		putObject(t, inner.Store, "data", key, data)
		getAll(t, c, "data", key)
	}
	// Touch entry 0 so entry 1 is the LRU tail.
	key0, _ := obj(0)
	getAll(t, c, "data", key0)

	key2, data2 := obj(2)
	putObject(t, inner.Store, "data", key2, data2)
	getAll(t, c, "data", key2)

	s := c.Stats()
	if s.Entries != 2 || s.Evictions != 1 {
		t.Fatalf("stats = %+v, want 2 entries / 1 eviction", s)
	}
	// Entry 1 (the LRU tail at admit time) must be the one evicted; 0 and 2
	// stay resident. Probe membership via CachedLocalPath — a Get would
	// re-populate and evict again.
	key1, _ := obj(1)
	if _, ok := c.CachedLocalPath("data", key1); ok {
		t.Fatal("LRU-tail entry survived eviction")
	}
	if _, ok := c.CachedLocalPath("data", key0); !ok {
		t.Fatal("recently-touched entry was evicted")
	}
	if _, ok := c.CachedLocalPath("data", key2); !ok {
		t.Fatal("newest entry was evicted")
	}
}

func TestBaseTableCacheOverBudgetNeverAdmitted(t *testing.T) {
	c, inner, _ := newTestCache(t, 100)
	data := bytes.Repeat([]byte("y"), 500)
	putObject(t, inner.Store, "data", "t/big.parquet", data)
	getAll(t, c, "data", "t/big.parquet")
	if s := c.Stats(); s.Entries != 0 {
		t.Fatalf("over-budget object admitted: %+v", s)
	}
}

func TestBaseTableCacheStartupRebuild(t *testing.T) {
	dir := t.TempDir()
	inner := &countingStore{Store: NewMemStore()}
	c1, err := NewBaseTableCache(inner, dir, 1<<20, nil)
	if err != nil {
		t.Fatalf("NewBaseTableCache: %v", err)
	}
	data := bytes.Repeat([]byte("z"), 2048)
	putObject(t, inner.Store, "data", "t/a.parquet", data)
	getAll(t, c1, "data", "t/a.parquet")

	// Leave a stray partial population behind; the next instance must sweep it.
	stray := filepath.Join(dir, "tmp", "populate-stray.tmp")
	if err := os.WriteFile(stray, []byte("partial"), 0o644); err != nil {
		t.Fatalf("writing stray tmp: %v", err)
	}

	c2, err := NewBaseTableCache(inner, dir, 1<<20, nil)
	if err != nil {
		t.Fatalf("NewBaseTableCache (rebuild): %v", err)
	}
	before := inner.gets.Load()
	if got := getAll(t, c2, "data", "t/a.parquet"); !bytes.Equal(got, data) {
		t.Fatal("body mismatch after rebuild")
	}
	if inner.gets.Load() != before {
		t.Fatal("rebuilt cache did not serve the adopted entry")
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatal("stray tmp file survived the startup sweep")
	}
	if s := c2.Stats(); s.Entries != 1 || s.Bytes != int64(len(data)) {
		t.Fatalf("rebuilt stats = %+v", s)
	}
}

func TestBaseTableCacheRebuildEvictsToShrunkBudget(t *testing.T) {
	dir := t.TempDir()
	inner := &countingStore{Store: NewMemStore()}
	c1, err := NewBaseTableCache(inner, dir, 1<<20, nil)
	if err != nil {
		t.Fatalf("NewBaseTableCache: %v", err)
	}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("t/c%d.parquet", i)
		putObject(t, inner.Store, "data", key, bytes.Repeat([]byte{byte(i)}, 400))
		getAll(t, c1, "data", key)
	}

	c2, err := NewBaseTableCache(inner, dir, 900, nil)
	if err != nil {
		t.Fatalf("NewBaseTableCache (shrunk): %v", err)
	}
	if s := c2.Stats(); s.Entries != 2 || s.Bytes > 900 {
		t.Fatalf("shrunk-budget rebuild stats = %+v, want 2 entries <= 900 bytes", s)
	}
}

func TestBaseTableCacheConcurrentSameKey(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := bytes.Repeat([]byte("c"), 128<<10)
	putObject(t, inner.Store, "data", "t/hot.parquet", data)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := getAll(t, c, "data", "t/hot.parquet")
			if !bytes.Equal(got, data) {
				errs <- fmt.Errorf("body mismatch (%d bytes)", len(got))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if s := c.Stats(); s.Entries != 1 {
		t.Fatalf("stats = %+v, want exactly 1 entry", s)
	}
	// Steady state: everything is a hit.
	before := inner.gets.Load()
	getAll(t, c, "data", "t/hot.parquet")
	if inner.gets.Load() != before {
		t.Fatal("steady-state read consulted the inner store")
	}
}

func TestBaseTableCacheGetReaderAt(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := []byte("0123456789abcdef")
	putObject(t, inner.Store, "data", "t/a.parquet", data)

	// Miss: passes through to the inner ReaderAtStore, does NOT populate.
	ra, size, err := c.GetReaderAt(context.Background(), "data", "t/a.parquet")
	if err != nil {
		t.Fatalf("GetReaderAt (miss): %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", size, len(data))
	}
	buf := make([]byte, 4)
	if _, err := ra.ReadAt(buf, 10); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf) != "abcd" {
		t.Fatalf("ReadAt = %q, want abcd", buf)
	}
	ra.Close()
	if inner.readerAts.Load() != 1 {
		t.Fatalf("inner readerAts = %d, want 1", inner.readerAts.Load())
	}
	if s := c.Stats(); s.Entries != 0 {
		t.Fatalf("ranged miss populated the cache: %+v", s)
	}

	// Populate via whole-file Get, then the ReaderAt must be a local hit.
	getAll(t, c, "data", "t/a.parquet")
	ra, size, err = c.GetReaderAt(context.Background(), "data", "t/a.parquet")
	if err != nil {
		t.Fatalf("GetReaderAt (hit): %v", err)
	}
	defer ra.Close()
	if size != int64(len(data)) {
		t.Fatalf("hit size = %d, want %d", size, len(data))
	}
	if _, err := ra.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt (hit): %v", err)
	}
	if string(buf) != "0123" {
		t.Fatalf("ReadAt (hit) = %q, want 0123", buf)
	}
	if inner.readerAts.Load() != 1 {
		t.Fatalf("inner readerAts = %d, want 1 (hit must not pass through)", inner.readerAts.Load())
	}
}

func TestBaseTableCacheDeleteAndPutInvalidate(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	ctx := context.Background()
	data := []byte("version one!")
	putObject(t, inner.Store, "data", "t/a.parquet", data)
	getAll(t, c, "data", "t/a.parquet")
	if s := c.Stats(); s.Entries != 1 {
		t.Fatalf("setup failed: %+v", s)
	}

	if err := c.Delete(ctx, "data", "t/a.parquet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s := c.Stats(); s.Entries != 0 {
		t.Fatalf("Delete did not invalidate: %+v", s)
	}

	// Out-of-contract overwrite through the cache must invalidate too.
	putObject(t, inner.Store, "data", "t/b.parquet", data)
	getAll(t, c, "data", "t/b.parquet")
	newData := []byte("version two.")
	if _, err := c.Put(ctx, "data", "t/b.parquet", bytes.NewReader(newData), int64(len(newData)), ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := getAll(t, c, "data", "t/b.parquet"); !bytes.Equal(got, newData) {
		t.Fatalf("stale bytes after overwrite: %q", got)
	}
}

func TestBaseTableCacheCachedLocalPath(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	if _, ok := c.CachedLocalPath("data", "t/a.parquet"); ok {
		t.Fatal("CachedLocalPath reported a hit on an empty cache")
	}
	data := []byte("mmap me in place")
	putObject(t, inner.Store, "data", "t/a.parquet", data)
	getAll(t, c, "data", "t/a.parquet")

	p, ok := c.CachedLocalPath("data", "t/a.parquet")
	if !ok {
		t.Fatal("CachedLocalPath missed a resident entry")
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading cache file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("cache file content mismatch")
	}
	if !strings.HasSuffix(filepath.ToSlash(p), "data/t/a.parquet") {
		t.Fatalf("unexpected cache layout: %s", p)
	}
	if _, ok := c.CachedLocalPath("data", "queries/q/x.parquet"); ok {
		t.Fatal("CachedLocalPath answered for an ineligible key")
	}
}
