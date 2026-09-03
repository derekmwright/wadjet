package worker

import (
	"sync"
	"time"
)

// resultStoreTTL bounds how long one query's results may sit in the store
// without the coordinator's CompleteSubject / CancelSubject arriving to
// remove them. It mirrors the worker's `cancelled` map (worker.go): prune
// on insert, with a window that comfortably outlives any straggler.
//
// The store had NO eviction of any kind: Put refused new entries once the
// capacity was reached and CleanupQuery — driven only by a broadcast that
// a cancelled query never sent (#817) — was the sole removal path. Enough
// missed broadcasts and the store was permanently full, so every later
// stage output silently fell back to S3 for the rest of the process's
// life: a memory leak AND a silent, permanent performance cliff on the
// feature this type's own comment calls the primary optimization for
// standalone mode (#818).
const resultStoreTTL = 30 * time.Minute

// ResultStore holds intermediate stage results in memory to avoid S3 round-trips
// when stages execute on the same worker. Results are keyed by their S3 path
// (the path that would have been used) so the executor can transparently check
// the store before falling back to S3.
//
// This is the primary performance optimization for standalone mode and co-located
// workers where all stages run on the same node. For a 3-stage query, this
// eliminates 4-6 S3 round-trips (serialize→upload→download→deserialize per stage).
//
// Memory is bounded by maxBytes. When exceeded, new results spill to S3 as usual.
// Results are cleaned up per-query when the query completes.
type ResultStore struct {
	mu        sync.RWMutex
	results   map[string][]byte    // path → parquet bytes
	byQuery   map[string][]string  // queryID → paths (for cleanup)
	firstPut  map[string]time.Time // queryID → when its first result landed
	usedBytes int64
	maxBytes  int64
	ttl       time.Duration
	evicted   int64 // queries dropped by the TTL, not by a cleanup broadcast
}

// NewResultStore creates an in-memory result store with the given capacity.
// A maxBytes of 0 disables in-memory result passing.
func NewResultStore(maxBytes int64) *ResultStore {
	return &ResultStore{
		results:  make(map[string][]byte),
		byQuery:  make(map[string][]string),
		firstPut: make(map[string]time.Time),
		maxBytes: maxBytes,
		ttl:      resultStoreTTL,
	}
}

// Put stores result data in memory if there's capacity.
// Returns true if stored, false if the caller should write to S3 instead.
func (rs *ResultStore) Put(queryID, path string, data []byte) bool {
	if rs.maxBytes <= 0 {
		return false
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Prune on insert. The store's only other removal path is a broadcast
	// from the coordinator, and a worker that misses one (restarted
	// coordinator, dropped message, a cancel that never published
	// CompleteSubject — #817) held those bytes for its process lifetime.
	rs.pruneExpiredLocked(time.Now())

	newSize := rs.usedBytes + int64(len(data))
	if newSize > rs.maxBytes {
		return false // no room — caller falls back to S3
	}

	// Copy data to avoid holding references to the caller's buffer
	stored := make([]byte, len(data))
	copy(stored, data)

	rs.results[path] = stored
	rs.byQuery[queryID] = append(rs.byQuery[queryID], path)
	if _, seen := rs.firstPut[queryID]; !seen {
		rs.firstPut[queryID] = time.Now()
	}
	rs.usedBytes = newSize
	return true
}

// pruneExpiredLocked drops every query whose first result is older than the
// TTL. Caller holds rs.mu.
func (rs *ResultStore) pruneExpiredLocked(now time.Time) {
	if rs.ttl <= 0 {
		return
	}
	cutoff := now.Add(-rs.ttl)
	for q, at := range rs.firstPut {
		if at.Before(cutoff) {
			rs.dropQueryLocked(q)
			rs.evicted++
		}
	}
}

// dropQueryLocked removes one query's entries. Caller holds rs.mu.
func (rs *ResultStore) dropQueryLocked(queryID string) {
	for _, path := range rs.byQuery[queryID] {
		if data, exists := rs.results[path]; exists {
			rs.usedBytes -= int64(len(data))
			delete(rs.results, path)
		}
	}
	delete(rs.byQuery, queryID)
	delete(rs.firstPut, queryID)
}

// Evicted returns how many queries the TTL dropped — i.e. how many times
// the coordinator's cleanup broadcast never arrived. A non-zero and rising
// value is the signal that something is not publishing a terminal message.
func (rs *ResultStore) Evicted() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.evicted
}

// Get retrieves result data from memory.
// Returns nil, false if not found (caller should read from S3).
func (rs *ResultStore) Get(path string) ([]byte, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	data, ok := rs.results[path]
	return data, ok
}

// CleanupQuery removes all results for a terminal query — completed OR
// cancelled. Both handlers call it: a cancelled query will never read its
// own stage outputs either.
func (rs *ResultStore) CleanupQuery(queryID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if _, ok := rs.byQuery[queryID]; !ok {
		return
	}
	rs.dropQueryLocked(queryID)
}

// UsedBytes returns the current memory usage.
func (rs *ResultStore) UsedBytes() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.usedBytes
}

// MaxBytes returns the configured capacity (0 disables the store). Used as the
// resultstore reservoir's cap.
func (rs *ResultStore) MaxBytes() int64 { return rs.maxBytes }

// Count returns the number of stored results.
func (rs *ResultStore) Count() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.results)
}
