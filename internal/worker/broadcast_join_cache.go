package worker

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// broadcastJoinCache is a worker-local, query-scoped cache that lets multiple
// concurrent probe tasks for the same broadcast share a single built
// *exec.HashJoin, instead of each task independently scanning + decoding the
// build-side files and constructing its own hashtable.
//
// SF100 Q21 22Q profile showed the supplier broadcast (5 files, ~154 MB cache)
// decoded 3× per worker — once per concurrent probe task within max_concurrent=3.
// HashJoin's probe path is already designed for concurrent use (per-instance
// HashJoinProbe scratch via h.Probe(), atomic.Bool-guarded lazy probeKeyIdx
// resolution), so the shared-build flow is safe.
//
// First arrival builds; subsequent arrivals with the same key block on the
// builder's `ready` channel, then attach refcount++ to the existing entry.
// Each acquire returns a release func; refcount=0 closes the HashJoin
// (releasing tracker reservation) and evicts the entry.
//
// CleanupQuery drops entries belonging to a query whose probe tasks all
// finished — mirrors LocalStageCache.CleanupQuery, called by the worker
// when the coordinator reports query completion.
type broadcastJoinCache struct {
	mu      sync.Mutex
	entries map[string]*broadcastJoinEntry
}

type broadcastJoinEntry struct {
	hj       *exec.HashJoin
	refcount int
	ready    chan struct{} // closed once the builder completes
	err      error         // set before ready closes; sticky
	queryID  string        // for CleanupQuery scoping
}

func newBroadcastJoinCache() *broadcastJoinCache {
	return &broadcastJoinCache{entries: make(map[string]*broadcastJoinEntry)}
}

// computeBroadcastJoinKey hashes the build-side identity of a broadcast probe
// OpSpec. Probe tasks for the same broadcast (same query, same build files +
// alias + join shape) produce the same key; different broadcasts (different
// aliases or file lists) produce different keys.
func computeBroadcastJoinKey(queryID string, spec distributed.OpSpec) string {
	h := sha1.New()
	fmt.Fprintf(h, "q:%s\x00a:%s\x00j:%s\x00s:%t\x00f:%s",
		queryID, spec.BuildAlias, spec.JoinType, spec.SemiAntiKeyOnly, spec.JoinFilter)
	for _, k := range spec.RightKeys {
		fmt.Fprintf(h, "\x00R:%s", k)
	}
	// Build-input filters change the built hash table: two joins over the
	// same files with different BuildFilterExprs must not share a build.
	for _, f := range spec.BuildFilterExprs {
		fmt.Fprintf(h, "\x00BF:%s", f)
	}
	files := append([]string(nil), spec.BuildFiles...)
	sort.Strings(files)
	for _, f := range files {
		fmt.Fprintf(h, "\x00F:%s", f)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Acquire returns the shared HashJoin for a broadcast build, calling build()
// on first arrival. Concurrent callers with the same key block on the same
// ready signal — only one Build runs.
//
// The returned release fn MUST be invoked when the caller is done probing.
// When the last release drops refcount to zero, the cache calls hj.Close()
// and evicts the entry.
//
// On builder error the entry is evicted before this call returns; the caller
// receives (nil, nil, err) and must NOT call release.
//
// ctx cancellation aborts a waiter's block on the builder (it releases its
// refcount and returns ctx.Err()), so a cancelled or deadlined probe task can
// never strand a goroutine — or its concurrency slot — on a build that hangs.
func (c *broadcastJoinCache) Acquire(ctx context.Context, key, queryID string, build func() (*exec.HashJoin, error)) (*exec.HashJoin, func(), error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		e.refcount++
		c.mu.Unlock()
		select {
		case <-e.ready:
		case <-ctx.Done():
			// Abandon the wait without leaking the refcount we just claimed.
			c.release(key)
			return nil, nil, ctx.Err()
		}
		if e.err != nil {
			// The builder failed; this waiter inherits the error and decrements
			// the refcount it just claimed. When refcount hits zero the entry
			// is evicted (hj is nil so Close is skipped).
			c.release(key)
			return nil, nil, e.err
		}
		return e.hj, c.releaseFnFor(key), nil
	}
	// First arrival: we are the builder.
	e := &broadcastJoinEntry{
		ready:    make(chan struct{}),
		refcount: 1,
		queryID:  queryID,
	}
	c.entries[key] = e
	c.mu.Unlock()

	// buildGuarded converts a panic in build() into an error so the path below
	// ALWAYS reaches close(e.ready). A panic that unwound past close() would
	// strand every concurrent and future waiter on this key forever (each
	// holding a worker concurrency slot) — the root of the Q07 final_aggregate
	// stall (#101).
	hj, err := buildGuarded(build)

	c.mu.Lock()
	e.hj = hj
	e.err = err
	close(e.ready)
	c.mu.Unlock()

	if err != nil {
		// Builder failed: release our own refcount and report. Other waiters
		// (if any) will see e.err and release themselves.
		c.release(key)
		return nil, nil, err
	}
	return hj, c.releaseFnFor(key), nil
}

// buildGuarded runs build, recovering a panic into an error (with stack) so the
// caller can always signal completion to waiters. The panicking builder task
// then fails cleanly via the normal error path instead of stranding waiters.
func buildGuarded(build func() (*exec.HashJoin, error)) (hj *exec.HashJoin, err error) {
	defer func() {
		if r := recover(); r != nil {
			hj = nil
			err = fmt.Errorf("broadcast build panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return build()
}

func (c *broadcastJoinCache) releaseFnFor(key string) func() {
	return func() { c.release(key) }
}

func (c *broadcastJoinCache) release(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return
	}
	e.refcount--
	if e.refcount <= 0 {
		if e.hj != nil {
			_ = e.hj.Close()
		}
		delete(c.entries, key)
	}
}

// CleanupQuery drops any entries belonging to queryID, closing their
// HashJoins. Intended for the coordinator's "query complete" signal after
// all probe tasks have released; safe to call on a query with no entries.
//
// If a probe task is still mid-probe when CleanupQuery fires, calling Close
// on its hj would corrupt its read. Callers MUST gate this on all-tasks-done.
func (c *broadcastJoinCache) CleanupQuery(queryID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var dropped []*broadcastJoinEntry
	for k, e := range c.entries {
		if e.queryID == queryID {
			dropped = append(dropped, e)
			delete(c.entries, k)
		}
	}
	for _, e := range dropped {
		if e.hj != nil {
			_ = e.hj.Close()
		}
	}
	return len(dropped)
}

// Count returns the total number of cached entries across all queries.
// Observability / test helper.
func (c *broadcastJoinCache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
