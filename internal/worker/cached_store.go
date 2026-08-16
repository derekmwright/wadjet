package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// CachedStore wraps an objstore.Store with the worker's LRU cache so that
// scanners (which use cat.Store() directly) benefit from cross-query file
// caching. Without this, each query re-reads all Parquet files from S3.
//
// CachedStore implements both objstore.Store and objstore.ReaderAtStore.
// For ReaderAt requests, it downloads the full file into the cache and
// serves random-access reads from memory — this is faster than S3 range
// requests for files that will be accessed across multiple queries.
type CachedStore struct {
	inner   objstore.Store
	cache   *LRUCache
	tracker *memory.Tracker // charges download transients; nil = no accounting
}

// NewCachedStore creates a store that checks the LRU cache before delegating
// to the underlying store. Files read from the inner store are automatically
// cached for subsequent queries. tracker (nil-safe) accounts each download
// buffer for the window between allocation and cache insertion — before the
// Put the bytes are invisible to admission and spill pressure; after it the
// LRU cache's reservoir owns them.
func NewCachedStore(inner objstore.Store, cache *LRUCache, tracker *memory.Tracker) *CachedStore {
	return &CachedStore{inner: inner, cache: cache, tracker: tracker}
}

// readFully drains rc into a buffer sized by size when known, charging the
// tracker for the buffer's lifetime. The returned release must be called
// exactly once, after the bytes are handed to the cache (or on error paths
// it has already run). rc is always closed.
func (s *CachedStore) readFully(rc io.ReadCloser, size int64, bucket, key string) (data []byte, release func(), err error) {
	release = func() {}
	if size > 0 {
		// Charge BEFORE the allocation so pressure machinery sees it coming.
		if s.tracker != nil {
			s.tracker.ForceReserve(size)
			release = func() { s.tracker.Release(size) }
		}
		// Pre-allocate buffer using known file size to avoid io.ReadAll's
		// O(log n) doubling strategy (512 → 1K → ... → 40MB = ~17 copies).
		buf := make([]byte, size)
		n, readErr := io.ReadFull(rc, buf)
		rc.Close()
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			release()
			return nil, func() {}, fmt.Errorf("reading %s/%s: %w", bucket, key, readErr)
		}
		return buf[:n], release, nil
	}
	data, err = io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, release, fmt.Errorf("reading %s/%s: %w", bucket, key, err)
	}
	if s.tracker != nil && len(data) > 0 {
		n := int64(len(data))
		s.tracker.ForceReserve(n)
		release = func() { s.tracker.Release(n) }
	}
	return data, release, nil
}

func (s *CachedStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	cacheKey := bucket + "/" + key
	if data, ok := s.cache.GetRef(cacheKey); ok {
		info := objstore.ObjectInfo{Key: key, Size: int64(len(data))}
		return io.NopCloser(bytes.NewReader(data)), info, nil
	}

	rc, info, err := s.inner.Get(ctx, bucket, key)
	if err != nil {
		return nil, info, err
	}
	data, release, err := s.readFully(rc, info.Size, bucket, key)
	if err != nil {
		return nil, info, err
	}

	s.cache.Put(cacheKey, data)
	release() // the LRU cache's reservoir owns the bytes from here
	return io.NopCloser(bytes.NewReader(data)), info, nil
}

// GetReaderAt serves random-access reads from the cache. On cache hit, it
// returns a zero-latency in-memory reader. On cache miss, it delegates to
// the inner store's ReaderAt (S3 range reads) to avoid downloading full
// files when only a few columns are needed. The cache is populated by
// Get() calls so subsequent pipeline queries on the same worker benefit
// from cached full files.
func (s *CachedStore) GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error) {
	cacheKey := bucket + "/" + key
	if data, ok := s.cache.GetRef(cacheKey); ok {
		return &nopCloseReaderAt{bytes.NewReader(data)}, int64(len(data)), nil
	}

	// Cache miss: delegate to inner store's range-read API if available.
	// Downloading full files on miss would penalize column-pruned queries
	// (e.g., Q07 reading 3 of 16 lineitem columns: 1 GB vs 3.5 GB).
	if ras, ok := s.inner.(objstore.ReaderAtStore); ok {
		return ras.GetReaderAt(ctx, bucket, key)
	}

	// Inner store doesn't support ReaderAt: download full file and cache.
	rc, info, err := s.inner.Get(ctx, bucket, key)
	if err != nil {
		return nil, 0, err
	}
	data, release, err := s.readFully(rc, info.Size, bucket, key)
	if err != nil {
		return nil, 0, err
	}

	s.cache.Put(cacheKey, data)
	release() // the LRU cache's reservoir owns the bytes from here
	return &nopCloseReaderAt{bytes.NewReader(data)}, int64(len(data)), nil
}

// Pass-through methods — these don't need caching.

func (s *CachedStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	return s.inner.Put(ctx, bucket, key, r, size, contentType)
}

func (s *CachedStore) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	return s.inner.PutIfMatch(ctx, bucket, key, r, size, contentType, expectedETag)
}

func (s *CachedStore) Head(ctx context.Context, bucket, key string) (objstore.ObjectInfo, error) {
	return s.inner.Head(ctx, bucket, key)
}

func (s *CachedStore) List(ctx context.Context, bucket string, opts objstore.ListOptions) ([]objstore.ObjectInfo, error) {
	return s.inner.List(ctx, bucket, opts)
}

func (s *CachedStore) Delete(ctx context.Context, bucket, key string) error {
	return s.inner.Delete(ctx, bucket, key)
}

func (s *CachedStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return s.inner.BucketExists(ctx, bucket)
}

func (s *CachedStore) MakeBucket(ctx context.Context, bucket string) error {
	return s.inner.MakeBucket(ctx, bucket)
}

// nopCloseReaderAt wraps bytes.Reader to implement ReaderAtCloser.
type nopCloseReaderAt struct {
	*bytes.Reader
}

func (r *nopCloseReaderAt) Close() error { return nil }
