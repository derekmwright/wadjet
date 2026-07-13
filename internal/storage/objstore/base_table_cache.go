package objstore

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/diskio"
)

// LocalPathStore is an optional interface for stores that hold whole
// objects on local disk. Callers that would otherwise stream a copy to
// their own scratch file (e.g. the worker's parquet mmap path) can open
// the store's file in place. The path is best-effort: eviction may unlink
// it between the call and the open — POSIX keeps the inode alive for
// already-open descriptors, so callers open first, then treat an open
// failure as a miss and fall back to Get.
type LocalPathStore interface {
	// CachedLocalPath returns the local file for a resident object. It
	// counts as a cache hit — call it only to serve.
	CachedLocalPath(bucket, key string) (string, bool)
	// HasCachedPath is the counting-free membership probe, for callers
	// deciding whether to skip work (e.g. a prefetcher electing not to
	// download) without inflating hit statistics.
	HasCachedPath(bucket, key string) bool
}

// BaseTableCacheStats is a point-in-time snapshot of cache counters.
type BaseTableCacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	HitBytes  int64
	MissBytes int64
	Entries   int
	Bytes     int64
}

// BaseTableCache is a read-through, disk-backed, whole-file cache for
// immutable base-table parquet objects, decorating an inner Store
// (docs/design/base-table-nvme-cache.md). Eligible keys (*.parquet
// outside the queries/ scratch prefix) are served from the cache
// directory on hit — never consulting the inner store or, when stacked
// above CircuitStore, the breaker — and teed into the cache on miss.
// Non-eligible keys pass through untouched.
//
// A (bucket, key) pair is content-stable for its lifetime: ingest writes
// UUID-named chunks and compaction/GC swap in new keys via the manifest
// rather than overwriting (see the design memo §2), so entries need no
// ETag validation. Population is strictly best-effort — any tee failure,
// short read, or size mismatch discards the temp file and the caller's
// stream is unaffected.
type BaseTableCache struct {
	inner  Store
	dir    string
	tmpDir string
	budget int64
	logger *slog.Logger

	mu       sync.Mutex
	entries  map[string]*list.Element // cache key -> element holding *btcEntry
	lru      *list.List               // front = most recently used
	curBytes int64
	inflight map[string]struct{} // keys with a tee in progress (dedupe, non-blocking)

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	hitBytes  atomic.Int64
	missBytes atomic.Int64
}

var (
	_ Store          = (*BaseTableCache)(nil)
	_ ReaderAtStore  = (*BaseTableCache)(nil)
	_ LocalPathStore = (*BaseTableCache)(nil)
)

type btcEntry struct {
	key  string // bucket + "/" + object key
	path string
	size int64
}

// NewBaseTableCache creates the cache rooted at dir with an LRU byte
// budget. The directory layout mirrors <dir>/<bucket>/<key>; existing
// entries are adopted at startup (recency seeded by mtime) so the cache
// survives process restarts. budget must be > 0 — a zero budget means
// the feature is off and the decorator should not be constructed.
func NewBaseTableCache(inner Store, dir string, budget int64, logger *slog.Logger) (*BaseTableCache, error) {
	if budget <= 0 {
		return nil, fmt.Errorf("base-table cache: budget must be > 0, got %d", budget)
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &BaseTableCache{
		inner:    inner,
		dir:      dir,
		tmpDir:   filepath.Join(dir, "tmp"),
		budget:   budget,
		logger:   logger,
		entries:  make(map[string]*list.Element),
		lru:      list.New(),
		inflight: make(map[string]struct{}),
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating base-table cache dir: %w", err)
	}
	// Partial populations from a previous run are never valid entries.
	if err := os.RemoveAll(c.tmpDir); err != nil {
		return nil, fmt.Errorf("sweeping base-table cache tmp dir: %w", err)
	}
	if err := os.MkdirAll(c.tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating base-table cache tmp dir: %w", err)
	}
	if err := c.rebuild(); err != nil {
		return nil, fmt.Errorf("rebuilding base-table cache index: %w", err)
	}
	return c, nil
}

// rebuild adopts entries already on disk, oldest-mtime first so the most
// recently written files end up at the LRU front, then evicts down to
// budget (the budget may have shrunk across restarts).
func (c *BaseTableCache) rebuild() error {
	type found struct {
		key   string
		path  string
		size  int64
		mtime int64
	}
	var files []found
	err := filepath.WalkDir(c.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == c.tmpDir {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(c.dir, p)
		if err != nil {
			return err
		}
		// rel is <bucket>/<key...>; anything shallower is not ours — leave it.
		if !strings.Contains(filepath.ToSlash(rel), "/") {
			return nil
		}
		files = append(files, found{
			key:   filepath.ToSlash(rel),
			path:  p,
			size:  info.Size(),
			mtime: info.ModTime().UnixNano(),
		})
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime < files[j].mtime })
	var evicted int
	for _, f := range files {
		c.entries[f.key] = c.lru.PushFront(&btcEntry{key: f.key, path: f.path, size: f.size})
		c.curBytes += f.size
	}
	for c.curBytes > c.budget && c.lru.Len() > 0 {
		e := c.removeTailLocked()
		_ = os.Remove(e.path)
		evicted++
	}
	if len(files) > 0 {
		c.logger.Info("base-table cache: adopted entries from previous run",
			"entries", c.lru.Len(), "bytes", c.curBytes, "evicted", evicted)
	}
	return nil
}

// eligibleKey reports whether the object is a cacheable base-table file.
// Query-scratch objects (stage outputs, aggregate/build caches under
// queries/<id>/) have their own tiers and race async uploads — never
// cache them. The path-shape checks keep the mirrored on-disk layout
// inside c.dir.
func (c *BaseTableCache) eligibleKey(bucket, key string) bool {
	if !strings.HasSuffix(key, ".parquet") || strings.HasPrefix(key, "queries/") {
		return false
	}
	if bucket == "" || strings.ContainsAny(bucket, "/\\") || bucket == ".." || bucket == "." {
		return false
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func cacheKey(bucket, key string) string { return bucket + "/" + key }

func (c *BaseTableCache) localPath(bucket, key string) string {
	return filepath.Join(c.dir, bucket, filepath.FromSlash(key))
}

// Get implements Store. Hits stream from the cache file; misses stream
// from the inner store while teeing into the cache.
func (c *BaseTableCache) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	if !c.eligibleKey(bucket, key) {
		return c.inner.Get(ctx, bucket, key)
	}
	ck := cacheKey(bucket, key)
	if f, info, ok := c.openHit(ck, key); ok {
		return f, info, nil
	}
	rc, info, err := c.inner.Get(ctx, bucket, key)
	if err != nil {
		return rc, info, err
	}
	c.misses.Add(1)
	if info.Size > 0 {
		c.missBytes.Add(info.Size)
	}
	tee := c.startTee(ck, bucket, key, info.Size)
	if tee == nil {
		return rc, info, nil
	}
	return &teeCacheReader{rc: rc, tee: tee}, info, nil
}

// openHit opens the cached file for ck, bumping recency. A failed open
// (evicted concurrently, corrupt) drops the entry and reports a miss.
func (c *BaseTableCache) openHit(ck, objKey string) (*os.File, ObjectInfo, bool) {
	c.mu.Lock()
	el, ok := c.entries[ck]
	if ok {
		c.lru.MoveToFront(el)
	}
	c.mu.Unlock()
	if !ok {
		return nil, ObjectInfo{}, false
	}
	e := el.Value.(*btcEntry)
	f, err := os.Open(e.path)
	if err != nil {
		c.dropEntry(ck)
		return nil, ObjectInfo{}, false
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		c.dropEntry(ck)
		return nil, ObjectInfo{}, false
	}
	c.hits.Add(1)
	c.hitBytes.Add(st.Size())
	return f, ObjectInfo{Key: objKey, Size: st.Size(), LastModified: st.ModTime()}, true
}

// GetReaderAt implements ReaderAtStore. Hits serve column-chunk range
// reads as local preads; misses pass through WITHOUT populating (ranged
// misses are footer-sized — the whole-file Get on every scan path is the
// populator).
func (c *BaseTableCache) GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	if c.eligibleKey(bucket, key) {
		if f, info, ok := c.openHit(cacheKey(bucket, key), key); ok {
			return f, info.Size, nil
		}
	}
	ras, ok := c.inner.(ReaderAtStore)
	if !ok {
		return nil, 0, fmt.Errorf("underlying store does not support ReaderAt")
	}
	return ras.GetReaderAt(ctx, bucket, key)
}

// CachedLocalPath implements LocalPathStore.
func (c *BaseTableCache) CachedLocalPath(bucket, key string) (string, bool) {
	e, ok := c.lookup(bucket, key)
	if !ok {
		return "", false
	}
	c.hits.Add(1)
	c.hitBytes.Add(e.size)
	return e.path, true
}

// HasCachedPath implements LocalPathStore.
func (c *BaseTableCache) HasCachedPath(bucket, key string) bool {
	_, ok := c.lookup(bucket, key)
	return ok
}

// lookup finds a resident entry and bumps its recency.
func (c *BaseTableCache) lookup(bucket, key string) (*btcEntry, bool) {
	if !c.eligibleKey(bucket, key) {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[cacheKey(bucket, key)]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(el)
	return el.Value.(*btcEntry), true
}

// startTee begins a population for ck unless one is already in flight,
// the entry already exists, or the advertised size is unknown or over
// budget. Returns nil when the caller should stream without teeing.
func (c *BaseTableCache) startTee(ck, bucket, key string, size int64) *cacheTee {
	if size <= 0 || size > c.budget {
		return nil
	}
	c.mu.Lock()
	_, busy := c.inflight[ck]
	_, exists := c.entries[ck]
	if busy || exists {
		c.mu.Unlock()
		return nil
	}
	c.inflight[ck] = struct{}{}
	c.mu.Unlock()
	f, err := os.CreateTemp(c.tmpDir, "populate-*.tmp")
	if err != nil {
		c.clearInflight(ck)
		c.logger.Warn("base-table cache: creating populate temp failed", "error", err)
		return nil
	}
	w, fl := diskio.NewWriter(f, diskio.Spill)
	return &cacheTee{c: c, ck: ck, bucket: bucket, key: key, f: f, w: w, fl: fl, want: size}
}

func (c *BaseTableCache) clearInflight(ck string) {
	c.mu.Lock()
	delete(c.inflight, ck)
	c.mu.Unlock()
}

// admit registers a renamed-into-place file. Evicts from the LRU tail
// until the entry fits. If a concurrent populate raced us in, the rename
// replaced the file with identical bytes — keep the existing accounting.
func (c *BaseTableCache) admit(ck, path string, size int64) {
	var evicted []*btcEntry
	c.mu.Lock()
	if el, ok := c.entries[ck]; ok {
		c.lru.MoveToFront(el)
		c.mu.Unlock()
		return
	}
	for c.curBytes+size > c.budget && c.lru.Len() > 0 {
		evicted = append(evicted, c.removeTailLocked())
	}
	c.entries[ck] = c.lru.PushFront(&btcEntry{key: ck, path: path, size: size})
	c.curBytes += size
	c.mu.Unlock()
	for _, e := range evicted {
		_ = os.Remove(e.path)
		c.evictions.Add(1)
	}
}

// removeTailLocked unlinks the least-recently-used entry from the index
// (not the filesystem). Caller holds c.mu and removes the file.
func (c *BaseTableCache) removeTailLocked() *btcEntry {
	tail := c.lru.Back()
	e := tail.Value.(*btcEntry)
	c.lru.Remove(tail)
	delete(c.entries, e.key)
	c.curBytes -= e.size
	return e
}

func (c *BaseTableCache) dropEntry(ck string) {
	c.mu.Lock()
	el, ok := c.entries[ck]
	var e *btcEntry
	if ok {
		e = el.Value.(*btcEntry)
		c.lru.Remove(el)
		delete(c.entries, ck)
		c.curBytes -= e.size
	}
	c.mu.Unlock()
	if ok {
		_ = os.Remove(e.path)
	}
}

// Stats returns a snapshot of the cache counters.
func (c *BaseTableCache) Stats() BaseTableCacheStats {
	c.mu.Lock()
	entries := c.lru.Len()
	bytes := c.curBytes
	c.mu.Unlock()
	return BaseTableCacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
		HitBytes:  c.hitBytes.Load(),
		MissBytes: c.missBytes.Load(),
		Entries:   entries,
		Bytes:     bytes,
	}
}

// LogStats emits the greppable stats marker (design memo §8).
func (c *BaseTableCache) LogStats() {
	s := c.Stats()
	c.logger.Info("base-table cache stats",
		"hits", s.Hits, "misses", s.Misses,
		"hit_bytes", s.HitBytes, "miss_bytes", s.MissBytes,
		"evictions", s.Evictions, "entries", s.Entries, "bytes", s.Bytes)
}

// Put implements Store. Eligible keys invalidate defensively — ingest
// never rewrites a live key (memo §2), but a stale entry after any
// out-of-contract overwrite would silently serve old bytes.
func (c *BaseTableCache) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	etag, err := c.inner.Put(ctx, bucket, key, r, size, contentType)
	if err == nil && c.eligibleKey(bucket, key) {
		c.dropEntry(cacheKey(bucket, key))
	}
	return etag, err
}

// PutIfMatch implements Store.
func (c *BaseTableCache) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	etag, err := c.inner.PutIfMatch(ctx, bucket, key, r, size, contentType, expectedETag)
	if err == nil && c.eligibleKey(bucket, key) {
		c.dropEntry(cacheKey(bucket, key))
	}
	return etag, err
}

// Delete implements Store. Dropping the entry eagerly reclaims disk that
// LRU pressure would otherwise take time to find (compaction/GC swaps).
func (c *BaseTableCache) Delete(ctx context.Context, bucket, key string) error {
	err := c.inner.Delete(ctx, bucket, key)
	if err == nil && c.eligibleKey(bucket, key) {
		c.dropEntry(cacheKey(bucket, key))
	}
	return err
}

// Head implements Store.
func (c *BaseTableCache) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	return c.inner.Head(ctx, bucket, key)
}

// List implements Store.
func (c *BaseTableCache) List(ctx context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	return c.inner.List(ctx, bucket, opts)
}

// BucketExists implements Store.
func (c *BaseTableCache) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return c.inner.BucketExists(ctx, bucket)
}

// MakeBucket implements Store.
func (c *BaseTableCache) MakeBucket(ctx context.Context, bucket string) error {
	return c.inner.MakeBucket(ctx, bucket)
}

// Unwrap returns the underlying store.
func (c *BaseTableCache) Unwrap() Store { return c.inner }

// cacheTee owns one in-flight population: the temp file the miss body is
// copied into, finalized (fsync + rename + admit) only on clean EOF with
// the byte count matching the advertised size.
type cacheTee struct {
	c         *BaseTableCache
	ck        string
	bucket    string
	key       string
	f         *os.File
	w         io.Writer
	fl        *diskio.Flusher
	want, got int64
	failed    bool
}

// write copies one chunk into the temp. Failures poison the tee but never
// surface to the consumer's stream.
func (t *cacheTee) write(p []byte) {
	if t.failed {
		return
	}
	n, err := t.w.Write(p)
	t.got += int64(n)
	if err != nil || t.got > t.want {
		t.failed = true
	}
}

func (t *cacheTee) finish(sawEOF bool) {
	defer t.c.clearInflight(t.ck)
	discard := func() {
		t.f.Close()
		_ = os.Remove(t.f.Name())
	}
	if t.failed || !sawEOF || t.got != t.want {
		discard()
		return
	}
	t.fl.Finish()
	if err := t.f.Sync(); err != nil {
		discard()
		return
	}
	tmpPath := t.f.Name()
	if err := t.f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return
	}
	final := t.c.localPath(t.bucket, t.key)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		_ = os.Remove(tmpPath)
		return
	}
	if err := os.Rename(tmpPath, final); err != nil {
		_ = os.Remove(tmpPath)
		return
	}
	t.c.admit(t.ck, final, t.want)
}

// teeCacheReader passes the inner body through while feeding the tee.
type teeCacheReader struct {
	rc     io.ReadCloser
	tee    *cacheTee
	sawEOF bool
}

func (r *teeCacheReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if n > 0 {
		r.tee.write(p[:n])
	}
	if err == io.EOF {
		r.sawEOF = true
	}
	return n, err
}

func (r *teeCacheReader) Close() error {
	err := r.rc.Close()
	if r.tee != nil {
		r.tee.finish(r.sawEOF)
		r.tee = nil
	}
	return err
}
