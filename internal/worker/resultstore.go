package worker

import (
	"sync"
)

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
	results   map[string][]byte   // path → parquet bytes
	byQuery   map[string][]string // queryID → paths (for cleanup)
	usedBytes int64
	maxBytes  int64
}

// NewResultStore creates an in-memory result store with the given capacity.
// A maxBytes of 0 disables in-memory result passing.
func NewResultStore(maxBytes int64) *ResultStore {
	return &ResultStore{
		results:  make(map[string][]byte),
		byQuery:  make(map[string][]string),
		maxBytes: maxBytes,
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

	newSize := rs.usedBytes + int64(len(data))
	if newSize > rs.maxBytes {
		return false // no room — caller falls back to S3
	}

	// Copy data to avoid holding references to the caller's buffer
	stored := make([]byte, len(data))
	copy(stored, data)

	rs.results[path] = stored
	rs.byQuery[queryID] = append(rs.byQuery[queryID], path)
	rs.usedBytes = newSize
	return true
}

// Get retrieves result data from memory.
// Returns nil, false if not found (caller should read from S3).
func (rs *ResultStore) Get(path string) ([]byte, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	data, ok := rs.results[path]
	return data, ok
}

// CleanupQuery removes all results for a completed query.
func (rs *ResultStore) CleanupQuery(queryID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	paths, ok := rs.byQuery[queryID]
	if !ok {
		return
	}

	for _, path := range paths {
		if data, exists := rs.results[path]; exists {
			rs.usedBytes -= int64(len(data))
			delete(rs.results, path)
		}
	}
	delete(rs.byQuery, queryID)
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
