package worker

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	mu      sync.RWMutex
	entries map[localStageKey]string
}

type localStageKey struct {
	queryID string
	key     string
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
	}
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
	if _, err := os.Stat(srcPath); err != nil {
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.entries[localStageKey{queryID, key}]
}

// CleanupQuery drops all entries for queryID, unlinks the files, and removes
// the per-query directory. Safe to call multiple times and against unknown
// query IDs.
func (c *LocalStageCache) CleanupQuery(queryID string) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	paths := make([]string, 0)
	for k, p := range c.entries {
		if k.queryID == queryID {
			paths = append(paths, p)
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
	for _, p := range paths {
		_ = os.Remove(p)
	}
	if c.rootDir != "" {
		_ = os.RemoveAll(filepath.Join(c.rootDir, queryID))
	}
	return len(paths)
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
