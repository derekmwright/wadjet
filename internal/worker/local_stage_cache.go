package worker

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// LocalStageCache maps (queryID, key) → localPath for stage outputs the
// worker wrote locally and can serve back without an S3 round-trip when a
// downstream task lands on the same worker.
//
// In single-node / standalone mode every stage transition is same-worker, so
// hits are common and each one saves an upload + download of a partition
// shuffle file (~1–3s per partition at SF10). In distributed mode cache hits
// happen only when the JetStream consumer load-balances a downstream task
// onto the same worker — when it doesn't, the consumer simply misses and
// falls through to the existing KV/S3 path.
//
// Producers register entries via Adopt(): the producer's local file is
// renamed into the cache's own per-query directory (so the producer's
// task-scoped spill dir can still be RemoveAll'd by deferred cleanup).
// Consumers consult the cache before falling through to KV/S3. CleanupQuery
// drops entries and unlinks files when the coordinator reports a query as
// complete or cancelled.
type LocalStageCache struct {
	rootDir string
	logger  *slog.Logger
	mu      sync.RWMutex
	entries map[localStageKey]string
	// cleaned tombstones queries whose CleanupQuery already ran. A task
	// that was mid-upload when its query terminated can reach Adopt AFTER
	// the cleanup signal was processed; without the tombstone the adopted
	// file is registered into a query directory nothing will ever clean
	// again (the cleanup message fires once per query). Entries are pruned
	// on insert after tombstoneTTL.
	cleaned map[string]time.Time

	// asyncPurge defers file deletion to the janitor goroutine
	// (docs/design/async-scratch-purge.md): CleanupQuery renames the
	// query's directory into .trash (instant) and the janitor unlinks its
	// contents with pacing. Without it (kill switch), CleanupQuery
	// unlinks inline on the caller — the query-complete broadcast
	// handler — which at SF100 is a multi-GB unlink storm precisely when
	// the NEXT query's first tasks are starting (the Q22/Q14/Q11
	// straggler-tail diagnosis, 2026-07-24).
	asyncPurge  bool
	janitorOnce sync.Once
	trashCh     chan trashedQuery
	trashSeq    atomic.Int64
}

// trashedQuery is one detached per-query directory awaiting background
// deletion, plus the ledger identity for the completion log line.
type trashedQuery struct {
	dir     string
	queryID string
	entries int
}

// Janitor pacing: unlink in bursts of purgeBatchFiles with purgeBatchPause
// between bursts, bounding the unlink storm's IO/journal contention with
// the running query's tasks. ~1-2k files per SF100 query-worker → the
// pause adds well under a second of janitor wall per query, all off the
// broadcast handler.
const (
	purgeBatchFiles = 64
	purgeBatchPause = 5 * time.Millisecond
)

// tombstoneTTL bounds how long a cleaned query refuses late Adopts. It only
// needs to outlive the slowest straggler task of a terminated query; 30
// minutes matches the worker's cancelled-map retention.
const tombstoneTTL = 30 * time.Minute

type localStageKey struct {
	queryID string
	key     string
}

// cacheScope normalizes the grouping ID for a cache entry: query-scratch
// keys ("queries/<id>/...") group under the ROOT query ID regardless of the
// caller's task QueryID, because task QueryIDs are stage-scoped
// ("st-<stage>-<id>") — the producer that Adopts and the downstream stage
// that Gets never share one. Root-scoping makes cross-stage same-worker
// hits land, lets streaming-exchange peers resolve fetches, and makes
// CleanupQuery (which the coordinator broadcasts with the root ID) actually
// match. Non-scratch keys keep the caller's ID.
func cacheScope(queryID, key string) string {
	if root := distributed.ScratchQueryID(key); root != "" {
		return root
	}
	return queryID
}

// NewLocalStageCache returns an empty cache that stores adopted files under
// rootDir/<queryID>/. Each producer's spill file is moved into this tree so
// it survives the producing task's spill cleanup.
//
// Any rootDir contents from a prior worker process are wiped at construction
// time — they're necessarily orphaned (this process has no entries pointing
// at them), and leaving them would slowly fill the spill volume on workers
// that crash before publishing query-complete signals.
func NewLocalStageCache(rootDir string) *LocalStageCache {
	if rootDir != "" {
		_ = os.RemoveAll(rootDir)
	}
	return &LocalStageCache{
		rootDir: rootDir,
		entries: make(map[localStageKey]string),
		cleaned: make(map[string]time.Time),
		trashCh: make(chan trashedQuery, 64),
	}
}

// SetAsyncPurge enables deferred scratch deletion (janitor goroutine,
// started lazily on first use). Call before the worker serves traffic
// (same contract as the other Set<X> setters); false = inline unlinks on
// the cleanup caller, the pre-2026-07-24 behavior and the A/B kill switch.
func (c *LocalStageCache) SetAsyncPurge(on bool) {
	if c != nil {
		c.asyncPurge = on
	}
}

// SetLogger attaches the worker's structured logger for the purge ledger
// lines. nil-safe; without it slog.Default() is used.
func (c *LocalStageCache) SetLogger(l *slog.Logger) {
	if c != nil && l != nil {
		c.logger = l
	}
}

func (c *LocalStageCache) log() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

// Adopt moves srcPath into the cache's per-query directory and registers it
// under (queryID, key). The producer no longer owns the file after a
// successful Adopt — the cache will unlink it on CleanupQuery.
//
// Returns the new path on success, or an empty string on failure (the caller
// should leave srcPath where it is and proceed without registering — the
// downstream consumer will simply fall through to S3/KV).
func (c *LocalStageCache) Adopt(queryID, key, srcPath string) string {
	if c == nil || c.rootDir == "" {
		return ""
	}
	queryID = cacheScope(queryID, key)
	if _, err := os.Stat(srcPath); err != nil {
		return ""
	}
	c.mu.RLock()
	_, tombstoned := c.cleaned[queryID]
	c.mu.RUnlock()
	if tombstoned {
		// The query already terminated and its cleanup ran; adopting now
		// would leak the file forever. Decline — the caller keeps srcPath
		// ownership and deletes it via its normal no-adopt path.
		return ""
	}
	queryDir := filepath.Join(c.rootDir, queryID)
	if err := os.MkdirAll(queryDir, 0o755); err != nil {
		return ""
	}
	dstPath := filepath.Join(queryDir, fileNameFor(key))
	if err := os.Rename(srcPath, dstPath); err != nil {
		// Cross-device rename or other failure — bail. The caller still
		// owns srcPath and will delete it as before; cache simply doesn't
		// register an entry.
		return ""
	}
	c.mu.Lock()
	c.entries[localStageKey{queryID, key}] = dstPath
	c.mu.Unlock()
	return dstPath
}

// Get returns the local path for (queryID, key), or "" if absent.
func (c *LocalStageCache) Get(queryID, key string) string {
	if c == nil {
		return ""
	}
	queryID = cacheScope(queryID, key)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.entries[localStageKey{queryID, key}]
}

// CleanupQuery drops all entries for queryID and disposes of the files.
// With asyncPurge the per-query directory is renamed into .trash (instant
// — open fds and mmaps stay valid across rename and unlink) and the
// janitor deletes it with pacing; otherwise files are unlinked inline,
// blocking the caller for the whole storm. Safe to call multiple times
// and against unknown query IDs.
func (c *LocalStageCache) CleanupQuery(queryID string) int {
	if c == nil {
		return 0
	}
	start := time.Now()
	c.mu.Lock()
	paths := make([]string, 0)
	for k, p := range c.entries {
		if k.queryID == queryID {
			paths = append(paths, p)
			delete(c.entries, k)
		}
	}
	if c.cleaned != nil {
		c.cleaned[queryID] = time.Now()
		cutoff := time.Now().Add(-tombstoneTTL)
		for q, at := range c.cleaned {
			if at.Before(cutoff) {
				delete(c.cleaned, q)
			}
		}
	}
	c.mu.Unlock()

	if len(paths) == 0 && c.rootDir == "" {
		return 0
	}
	if c.asyncPurge && c.rootDir != "" && c.deferPurge(queryID, len(paths)) {
		c.log().Info("scratch purge deferred",
			"query_id", queryID, "entries", len(paths),
			"handler_us", time.Since(start).Microseconds())
		return len(paths)
	}
	// Inline (kill switch, empty dir, rename failure, or full janitor
	// queue): the pre-async behavior, instrumented — handler_ms is the
	// storm the broadcast handler absorbs.
	var bytes int64
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil {
			bytes += fi.Size()
		}
		_ = os.Remove(p)
	}
	if c.rootDir != "" {
		_ = os.RemoveAll(filepath.Join(c.rootDir, queryID))
	}
	if len(paths) > 0 {
		c.log().Info("scratch purge inline",
			"query_id", queryID, "entries", len(paths), "bytes", bytes,
			"handler_ms", time.Since(start).Milliseconds())
	}
	return len(paths)
}

// deferPurge detaches the query's directory into .trash and hands it to
// the janitor. Returns false when the caller must delete inline (rename
// failed, or the janitor queue is full — blocking here would put the
// storm right back on the broadcast handler).
func (c *LocalStageCache) deferPurge(queryID string, entries int) bool {
	qdir := filepath.Join(c.rootDir, queryID)
	if _, err := os.Stat(qdir); err != nil {
		return false // nothing on disk (no adopts); inline path is a no-op
	}
	trashDir := filepath.Join(c.rootDir, ".trash")
	if err := os.MkdirAll(trashDir, 0o755); err != nil {
		return false
	}
	dst := filepath.Join(trashDir, fmt.Sprintf("%s-%d", queryID, c.trashSeq.Add(1)))
	if err := os.Rename(qdir, dst); err != nil {
		return false
	}
	c.janitorOnce.Do(func() { go c.janitor() })
	select {
	case c.trashCh <- trashedQuery{dir: dst, queryID: queryID, entries: entries}:
		return true
	default:
		// Queue full — delete the detached dir inline rather than blocking.
		_ = os.RemoveAll(dst)
		return true // still off the entries' unlink path; dir handled here
	}
}

// janitor deletes trashed query directories with pacing: bursts of
// purgeBatchFiles unlinks separated by purgeBatchPause, bounding
// journal/IO contention with the running query. Daemon goroutine, one per
// cache, lives for the process (boot-time RemoveAll of rootDir reclaims
// anything a crash strands in .trash).
func (c *LocalStageCache) janitor() {
	for t := range c.trashCh {
		start := time.Now()
		var files int
		var bytes int64
		_ = filepath.WalkDir(t.dir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if fi, ferr := d.Info(); ferr == nil {
				bytes += fi.Size()
			}
			_ = os.Remove(path)
			files++
			if files%purgeBatchFiles == 0 {
				time.Sleep(purgeBatchPause)
			}
			return nil
		})
		_ = os.RemoveAll(t.dir)
		c.log().Info("scratch purge done",
			"query_id", t.queryID, "entries", t.entries, "files", files,
			"bytes", bytes, "purge_ms", time.Since(start).Milliseconds())
	}
}

// Count returns the total number of cached entries across all queries.
// Observability/test helper.
func (c *LocalStageCache) Count() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// fileNameFor maps a stage-output key (which may contain '/') to a flat,
// filesystem-safe filename. We use a hex SHA-1 prefix plus a short basename
// suffix for human debuggability; collisions on the prefix don't matter
// because we only ever look entries up via the in-memory map.
func fileNameFor(key string) string {
	h := sha1.Sum([]byte(key))
	hexed := hex.EncodeToString(h[:8])
	base := filepath.Base(key)
	if base == "" || base == "." || base == "/" {
		base = "stage"
	}
	return fmt.Sprintf("%s_%s", hexed, base)
}
