package objstore

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type memObject struct {
	data        []byte
	etag        string
	contentType string
	modTime     time.Time
}

// MemStore is an in-memory implementation of Store for testing.
type MemStore struct {
	mu      sync.RWMutex
	buckets map[string]map[string]*memObject
}

// NewMemStore creates a new in-memory object store.
func NewMemStore() *MemStore {
	return &MemStore{
		buckets: make(map[string]map[string]*memObject),
	}
}

func computeETag(data []byte) string {
	h := md5.Sum(data)
	return fmt.Sprintf("%x", h)
}

func (m *MemStore) MakeBucket(_ context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[bucket]; !ok {
		m.buckets[bucket] = make(map[string]*memObject)
	}
	return nil
}

func (m *MemStore) BucketExists(_ context.Context, bucket string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.buckets[bucket]
	return ok, nil
}

func (m *MemStore) getBucket(bucket string) (map[string]*memObject, error) {
	b, ok := m.buckets[bucket]
	if !ok {
		return nil, ErrBucketNotFound
	}
	return b, nil
}

func (m *MemStore) Put(_ context.Context, bucket, key string, r io.Reader, _ int64, contentType string) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("reading data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return "", err
	}

	etag := computeETag(data)
	b[key] = &memObject{
		data:        data,
		etag:        etag,
		contentType: contentType,
		modTime:     time.Now(),
	}
	return etag, nil
}

func (m *MemStore) PutIfMatch(_ context.Context, bucket, key string, r io.Reader, _ int64, contentType string, expectedETag string) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("reading data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return "", err
	}

	existing, exists := b[key]
	if expectedETag == "" {
		// Create-only: object must not exist
		if exists {
			return "", ErrPreconditionFailed
		}
	} else {
		if !exists || existing.etag != expectedETag {
			return "", ErrPreconditionFailed
		}
	}

	etag := computeETag(data)
	b[key] = &memObject{
		data:        data,
		etag:        etag,
		contentType: contentType,
		modTime:     time.Now(),
	}
	return etag, nil
}

func (m *MemStore) Get(_ context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	obj, ok := b[key]
	if !ok {
		return nil, ObjectInfo{}, ErrNotFound
	}

	info := ObjectInfo{
		Key:          key,
		Size:         int64(len(obj.data)),
		ETag:         obj.etag,
		LastModified: obj.modTime,
		ContentType:  obj.contentType,
	}
	return io.NopCloser(bytes.NewReader(obj.data)), info, nil
}

// nopReaderAtCloser wraps an io.ReaderAt with a no-op Close.
type nopReaderAtCloser struct {
	io.ReaderAt
}

func (nopReaderAtCloser) Close() error { return nil }

func (m *MemStore) GetReaderAt(_ context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return nil, 0, err
	}
	obj, ok := b[key]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return nopReaderAtCloser{bytes.NewReader(obj.data)}, int64(len(obj.data)), nil
}

func (m *MemStore) Head(_ context.Context, bucket, key string) (ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return ObjectInfo{}, err
	}

	obj, ok := b[key]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}

	return ObjectInfo{
		Key:          key,
		Size:         int64(len(obj.data)),
		ETag:         obj.etag,
		LastModified: obj.modTime,
		ContentType:  obj.contentType,
	}, nil
}

func (m *MemStore) List(_ context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return nil, err
	}

	var result []ObjectInfo
	seen := make(map[string]bool)

	for key, obj := range b {
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}

		if opts.Delimiter != "" {
			// Simulate prefix listing with delimiter
			rest := strings.TrimPrefix(key, opts.Prefix)
			idx := strings.Index(rest, opts.Delimiter)
			if idx >= 0 {
				// This is a "common prefix" — return it as a directory marker
				prefix := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if !seen[prefix] {
					seen[prefix] = true
					result = append(result, ObjectInfo{Key: prefix})
				}
				continue
			}
		}

		result = append(result, ObjectInfo{
			Key:          key,
			Size:         int64(len(obj.data)),
			ETag:         obj.etag,
			LastModified: obj.modTime,
			ContentType:  obj.contentType,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	if opts.MaxKeys > 0 && len(result) > opts.MaxKeys {
		result = result[:opts.MaxKeys]
	}

	return result, nil
}

func (m *MemStore) Delete(_ context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, err := m.getBucket(bucket)
	if err != nil {
		return err
	}

	delete(b, key)
	return nil
}
