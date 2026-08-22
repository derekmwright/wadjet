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
	"time"

	"github.com/derekmwright/wadjet/internal/engine/diskio"
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

	// Peer tier (docs/design/scan-affinity.md §peer tier). Consumer side:
	// misses served from the file's rendezvous owner instead of S3.
	// Server side: fetches this cache served to non-owner peers.
	// Misses/MissBytes above stay S3-only so the first-touch ledger keeps
	// measuring exactly the reads that left the cluster.
	PeerHits         int64
	PeerBytes        int64
	PeerFallthroughs int64
	PeerServes       int64
	PeerServeBytes   int64
	// PeerFetchNanos is the wall spent inside successful peer fetches
	// (owner stream + local spool + admit), so the per-minute ledger
	// yields an effective peer-tier MB/s — the number that tells a peer
	// transfer apart from an S3 miss when both show up as src_ms.
	PeerFetchNanos int64

	// Owner read-through (docs/design/scan-affinity.md §first-touch
	// single-flight): populates performed on behalf of a peer's fetch for
	// a not-yet-resident owned file. ReadThroughBytes land in the cache
	// and are S3 reads, but are counted separately from Misses/MissBytes —
	// those keep measuring local demand that left the cluster; these
	// measure demand a peer redirected here.
	ReadThroughs     int64
	ReadThroughBytes int64
	ReadThroughFails int64
}

// BaseTablePeerFetcher is the seam between the cache's miss path and the
// worker's peer tier. FetchBaseTable returns a whole-object stream from
// the file's rendezvous owner, or ok=false when the tier cannot help
// (this worker owns the file, no live owner is known, or the tier is
// disabled) — the miss then goes to the inner store exactly as before.
// A returned stream may still fail mid-read; the cache treats any error
// as a fallthrough to the inner store.
type BaseTablePeerFetcher interface {
	FetchBaseTable(ctx context.Context, bucket, key string) (io.ReadCloser, bool)
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

	mu         sync.Mutex
	entries    map[string]*list.Element // cache key -> element holding *btcEntry
	lru        *list.List               // front = most recently used
	curBytes   int64
	inflight   map[string]struct{}    // keys with a tee in progress (dedupe, non-blocking)
	populating map[string]*populateOp // keys with a ReadThrough populate in flight

	peer BaseTablePeerFetcher // nil = no peer tier (default)

	hits             atomic.Int64
	misses           atomic.Int64
	evictions        atomic.Int64
	hitBytes         atomic.Int64
	missBytes        atomic.Int64
	peerHits         atomic.Int64
	peerBytes        atomic.Int64
	peerFetchNanos   atomic.Int64
	peerFallthroughs atomic.Int64
	peerServes       atomic.Int64
	peerServeBytes   atomic.Int64
	readThroughs     atomic.Int64
	readThroughBytes atomic.Int64
	readThroughFails atomic.Int64
}

// populateOp is one in-flight ReadThrough population. The runner closes
// done after setting err; waiters select on it against their own ctx.
type populateOp struct {
	done chan struct{}
	err  error
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
		inner:      inner,
		dir:        dir,
		tmpDir:     filepath.Join(dir, "tmp"),
		budget:     budget,
		logger:     logger,
		entries:    make(map[string]*list.Element),
		lru:        list.New(),
		inflight:   make(map[string]struct{}),
		populating: make(map[string]*populateOp),
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

// Get implements Store. Hits stream from the cache file; misses try the
// peer tier (the file's rendezvous owner's cache, when wired), then
// stream from the inner store while teeing into the cache.
func (c *BaseTableCache) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	if !c.eligibleKey(bucket, key) {
		return c.inner.Get(ctx, bucket, key)
	}
	ck := cacheKey(bucket, key)
	if f, info, ok := c.openHit(ck, key); ok {
		return f, info, nil
	}
	if c.peer != nil {
		if f, info, ok := c.getFromPeer(ctx, ck, bucket, key); ok {
			return f, info, nil
		}
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

// StoreID implements IdentifiedStore by delegating: this cache is a local
// materialization of inner's objects under the SAME (bucket, key) names, so
// it must share inner's namespace rather than mint its own.
func (c *BaseTableCache) StoreID() string { return StoreID(c.inner) }

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

// SetPeerFetcher wires the peer tier into the miss path. Must be called
// before the cache serves traffic (worker wiring, pre-Start) — the field
// is read without synchronization on every Get.
func (c *BaseTableCache) SetPeerFetcher(f BaseTablePeerFetcher) { c.peer = f }

// getFromPeer spools the whole object from its rendezvous owner into the
// cache, then serves the admitted local file. ok=false on any failure —
// the caller falls through to the inner store, which re-populates via the
// ordinary tee. The inflight map dedupes against concurrent populations
// of the same key (racing readers go straight to the inner store, same as
// the tee path).
func (c *BaseTableCache) getFromPeer(ctx context.Context, ck, bucket, key string) (*os.File, ObjectInfo, bool) {
	c.mu.Lock()
	if _, busy := c.inflight[ck]; busy {
		c.mu.Unlock()
		return nil, ObjectInfo{}, false
	}
	c.inflight[ck] = struct{}{}
	c.mu.Unlock()
	defer c.clearInflight(ck)

	start := time.Now()
	rc, ok := c.peer.FetchBaseTable(ctx, bucket, key)
	if !ok {
		return nil, ObjectInfo{}, false
	}
	defer rc.Close()
	size, path, err := c.spool(bucket, key, rc, -1)
	if err != nil {
		c.peerFallthroughs.Add(1)
		c.logger.Debug("base-table cache: peer fetch fell through to S3",
			"bucket", bucket, "key", key, "error", err)
		return nil, ObjectInfo{}, false
	}
	c.admit(ck, path, size)
	f, err := os.Open(path)
	if err != nil {
		// Admitted then instantly evicted under extreme pressure — the
		// bytes are gone; let the inner store serve.
		c.peerFallthroughs.Add(1)
		return nil, ObjectInfo{}, false
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		c.peerFallthroughs.Add(1)
		return nil, ObjectInfo{}, false
	}
	took := time.Since(start)
	c.peerHits.Add(1)
	c.peerBytes.Add(size)
	c.peerFetchNanos.Add(took.Nanoseconds())
	// One line per transfer (tens per worker per suite run): the
	// per-file cost a straggler task pays is otherwise only inferable
	// from src_ms, which cannot tell this tier from an S3 miss.
	c.logger.Info("base-table cache: peer fetch",
		"key", key, "bytes", size, "ms", took.Milliseconds())
	return f, ObjectInfo{Key: key, Size: st.Size(), LastModified: st.ModTime()}, true
}

// parquetMagic frames every valid parquet file ("PAR1" head and tail).
// The peer stream has no advertised size to validate against — clean EOF
// plus both magics is the truncation/corruption guard before a peer copy
// is admitted into the safety-critical parquet read path.
var parquetMagic = [4]byte{'P', 'A', 'R', '1'}

// spool copies a population stream (peer fetch or read-through) into a
// temp file and renames it into place, returning the final size and path.
// want is the advertised size to enforce, or -1 when the stream carries
// none (peer fetches). Any error leaves no state behind.
func (c *BaseTableCache) spool(bucket, key string, rc io.Reader, want int64) (int64, string, error) {
	f, err := os.CreateTemp(c.tmpDir, "peer-*.tmp")
	if err != nil {
		return 0, "", fmt.Errorf("creating peer spool temp: %w", err)
	}
	tmpPath := f.Name()
	discard := func(err error) (int64, string, error) {
		f.Close()
		_ = os.Remove(tmpPath)
		return 0, "", err
	}
	w, fl := diskio.NewWriter(f, diskio.Spill)
	size, err := io.Copy(w, io.LimitReader(rc, c.budget+1))
	if err != nil {
		return discard(fmt.Errorf("copying peer stream: %w", err))
	}
	if size > c.budget {
		return discard(fmt.Errorf("peer object exceeds cache budget (%d)", c.budget))
	}
	fl.Finish()
	if want >= 0 && size != want {
		return discard(fmt.Errorf("spooled %d bytes, want %d", size, want))
	}
	if size < int64(2*len(parquetMagic)) {
		return discard(fmt.Errorf("peer object too short (%d bytes)", size))
	}
	var head, tail [4]byte
	if _, err := f.ReadAt(head[:], 0); err != nil {
		return discard(fmt.Errorf("reading spooled head: %w", err))
	}
	if _, err := f.ReadAt(tail[:], size-4); err != nil {
		return discard(fmt.Errorf("reading spooled tail: %w", err))
	}
	if head != parquetMagic || tail != parquetMagic {
		return discard(fmt.Errorf("peer object is not framed parquet (head %q tail %q)", head[:], tail[:]))
	}
	if err := f.Sync(); err != nil {
		return discard(fmt.Errorf("syncing peer spool: %w", err))
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, "", fmt.Errorf("closing peer spool: %w", err)
	}
	final := c.localPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		_ = os.Remove(tmpPath)
		return 0, "", fmt.Errorf("creating cache dir: %w", err)
	}
	if err := os.Rename(tmpPath, final); err != nil {
		_ = os.Remove(tmpPath)
		return 0, "", fmt.Errorf("renaming peer spool into place: %w", err)
	}
	return size, final, nil
}

// readThroughTimeout bounds one detached read-through fetch (largest
// ingest chunks are a few hundred MB; S3 streams them in seconds — a
// minute means the store is in real trouble and the peer should go
// durable itself). readThroughPoll is the cadence for waiting out a
// concurrent tee. Both are vars so tests can shrink them.
var (
	readThroughTimeout = 60 * time.Second
	readThroughPoll    = 25 * time.Millisecond
)

// ReadThrough ensures bucket/key is resident, fetching it from the inner
// store if needed (docs/design/scan-affinity.md §first-touch
// single-flight). The worker's peer resolver is the only caller: a peer
// asked this worker — the file's rendezvous owner — for a copy it doesn't
// hold yet, and fetching it HERE (once) instead of bouncing the peer to
// S3 is what keeps cluster-wide first-touch at one S3 read per file.
//
// Concurrent callers for one key coalesce onto a single fetch. The fetch
// runs detached with its own timeout — a canceled waiter neither strands
// other waiters nor aborts a populate whose bytes this node will want
// anyway. An error return means the caller should fall back to
// NotFound-style behavior (the peer goes durable); the cache is
// unchanged.
func (c *BaseTableCache) ReadThrough(ctx context.Context, bucket, key string) error {
	if !c.eligibleKey(bucket, key) {
		return fmt.Errorf("read-through: ineligible key %s/%s", bucket, key)
	}
	ck := cacheKey(bucket, key)
	c.mu.Lock()
	if _, ok := c.entries[ck]; ok {
		c.mu.Unlock()
		return nil
	}
	op, joined := c.populating[ck]
	if !joined {
		op = &populateOp{done: make(chan struct{})}
		c.populating[ck] = op
	}
	c.mu.Unlock()
	if !joined {
		go c.runReadThrough(ck, bucket, key, op)
	}
	select {
	case <-op.done:
		return op.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runReadThrough is the detached single-flight runner for one key. It
// waits out any concurrent tee population (the bytes are already flowing
// to this node — starting a second S3 stream would recreate exactly the
// duplication this path exists to kill), rechecks residency, then fetches
// from the inner store and admits via the standard spool.
func (c *BaseTableCache) runReadThrough(ck, bucket, key string, op *populateOp) {
	defer func() {
		c.mu.Lock()
		delete(c.populating, ck)
		c.mu.Unlock()
		if op.err != nil {
			c.readThroughFails.Add(1)
		}
		close(op.done)
	}()
	deadline := time.Now().Add(readThroughTimeout)
	for {
		c.mu.Lock()
		if _, resident := c.entries[ck]; resident {
			c.mu.Unlock()
			return
		}
		if _, busy := c.inflight[ck]; !busy {
			c.inflight[ck] = struct{}{} // claim: local tees now skip, same as any populate
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			op.err = fmt.Errorf("read-through %s: tee population still in flight after %s", ck, readThroughTimeout)
			return
		}
		time.Sleep(readThroughPoll)
	}
	defer c.clearInflight(ck)
	ctx, cancel := context.WithTimeout(context.Background(), readThroughTimeout)
	defer cancel()
	rc, info, err := c.inner.Get(ctx, bucket, key)
	if err != nil {
		op.err = fmt.Errorf("read-through %s: %w", ck, err)
		return
	}
	defer rc.Close()
	want := info.Size
	if want <= 0 {
		want = -1 // stores that don't advertise a size fall back to framing checks
	}
	size, path, err := c.spool(bucket, key, rc, want)
	if err != nil {
		op.err = fmt.Errorf("read-through %s: %w", ck, err)
		return
	}
	c.admit(ck, path, size)
	c.readThroughs.Add(1)
	c.readThroughBytes.Add(size)
}

// PeerLocalPath resolves a resident entry for a peer's fetch, counting it
// as a peer serve (never a local hit — served bytes leave on the wire and
// must not inflate the hit ledger). The worker's ShuffleFileResolver
// base-table branch is the only caller.
func (c *BaseTableCache) PeerLocalPath(bucket, key string) (string, bool) {
	e, ok := c.lookup(bucket, key)
	if !ok {
		return "", false
	}
	c.peerServes.Add(1)
	c.peerServeBytes.Add(e.size)
	return e.path, true
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
		Hits:             c.hits.Load(),
		Misses:           c.misses.Load(),
		Evictions:        c.evictions.Load(),
		HitBytes:         c.hitBytes.Load(),
		MissBytes:        c.missBytes.Load(),
		Entries:          entries,
		Bytes:            bytes,
		PeerHits:         c.peerHits.Load(),
		PeerBytes:        c.peerBytes.Load(),
		PeerFetchNanos:   c.peerFetchNanos.Load(),
		PeerFallthroughs: c.peerFallthroughs.Load(),
		PeerServes:       c.peerServes.Load(),
		PeerServeBytes:   c.peerServeBytes.Load(),
		ReadThroughs:     c.readThroughs.Load(),
		ReadThroughBytes: c.readThroughBytes.Load(),
		ReadThroughFails: c.readThroughFails.Load(),
	}
}

// LogStats emits the greppable stats marker (design memo §8).
func (c *BaseTableCache) LogStats() {
	s := c.Stats()
	c.logger.Info("base-table cache stats",
		"hits", s.Hits, "misses", s.Misses,
		"hit_bytes", s.HitBytes, "miss_bytes", s.MissBytes,
		"evictions", s.Evictions, "entries", s.Entries, "bytes", s.Bytes,
		"peer_hits", s.PeerHits, "peer_bytes", s.PeerBytes,
		"peer_fetch_ms", s.PeerFetchNanos/1e6,
		"peer_fallthroughs", s.PeerFallthroughs,
		"peer_serves", s.PeerServes, "peer_serve_bytes", s.PeerServeBytes,
		"readthroughs", s.ReadThroughs, "readthrough_bytes", s.ReadThroughBytes,
		"readthrough_fails", s.ReadThroughFails)
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

// FindBaseTableCache walks a decorator chain (via Unwrap) to the
// BaseTableCache layer, or nil when the store has none. Lets the worker
// wire the peer tier without threading the concrete cache through every
// construction path.
func FindBaseTableCache(s Store) *BaseTableCache {
	for s != nil {
		if btc, ok := s.(*BaseTableCache); ok {
			return btc
		}
		u, ok := s.(interface{ Unwrap() Store })
		if !ok {
			return nil
		}
		s = u.Unwrap()
	}
	return nil
}

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
