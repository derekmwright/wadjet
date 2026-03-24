package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
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
	inner objstore.Store
	cache *LRUCache
}

// NewCachedStore creates a store that checks the LRU cache before delegating
// to the underlying store. Files read from the inner store are automatically
// cached for subsequent queries.
func NewCachedStore(inner objstore.Store, cache *LRUCache) *CachedStore {
	return &CachedStore{inner: inner, cache: cache}
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
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, info, fmt.Errorf("reading %s/%s: %w", bucket, key, err)
	}

	s.cache.Put(cacheKey, data)
	return io.NopCloser(bytes.NewReader(data)), info, nil
}

// GetReaderAt serves random-access reads from the cache. If the file is not
// cached, it downloads the full file first. This is intentional: at SF10,
// a single S3 GET (~50ms) is cheaper than multiple range requests, and the
// cached file benefits all subsequent queries on this worker.
func (s *CachedStore) GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error) {
	cacheKey := bucket + "/" + key
	if data, ok := s.cache.GetRef(cacheKey); ok {
		return &nopCloseReaderAt{bytes.NewReader(data)}, int64(len(data)), nil
	}

	// Download full file and cache it
	rc, _, err := s.inner.Get(ctx, bucket, key)
	if err != nil {
		return nil, 0, err
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, 0, fmt.Errorf("reading %s/%s: %w", bucket, key, err)
	}

	s.cache.Put(cacheKey, data)
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
