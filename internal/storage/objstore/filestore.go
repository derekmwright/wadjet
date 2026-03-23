package objstore

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileStore is a local filesystem implementation of Store.
// Designed for edge deployments where S3 is unavailable.
//
// Layout:
//
//	<rootDir>/<bucket>/<key>
//
// Keys may contain "/" which maps to subdirectories.
// ETags are computed as MD5 hex of file contents.
type FileStore struct {
	rootDir string
	mu      sync.RWMutex // protects concurrent writes to the same path
}

// NewFileStore creates a FileStore rooted at the given directory.
// The directory is created if it does not exist.
func NewFileStore(rootDir string) (*FileStore, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating filestore root %q: %w", rootDir, err)
	}
	return &FileStore{rootDir: rootDir}, nil
}

func (f *FileStore) bucketDir(bucket string) string {
	return filepath.Join(f.rootDir, bucket)
}

func (f *FileStore) objectPath(bucket, key string) string {
	return filepath.Join(f.rootDir, bucket, filepath.FromSlash(key))
}

func (f *FileStore) MakeBucket(_ context.Context, bucket string) error {
	return os.MkdirAll(f.bucketDir(bucket), 0o755)
}

func (f *FileStore) BucketExists(_ context.Context, bucket string) (bool, error) {
	info, err := os.Stat(f.bucketDir(bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func (f *FileStore) Put(_ context.Context, bucket, key string, r io.Reader, _ int64, contentType string) (string, error) {
	_ = contentType // filesystem doesn't store content type metadata

	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("reading data: %w", err)
	}

	path := f.objectPath(bucket, key)

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("creating directories: %w", err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("writing file: %w", err)
	}

	etag := fmt.Sprintf("%x", md5.Sum(data))
	return etag, nil
}

func (f *FileStore) PutIfMatch(_ context.Context, bucket, key string, r io.Reader, _ int64, contentType string, expectedETag string) (string, error) {
	_ = contentType

	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("reading data: %w", err)
	}

	path := f.objectPath(bucket, key)

	f.mu.Lock()
	defer f.mu.Unlock()

	if expectedETag == "" {
		// Create-only: file must not exist
		if _, err := os.Stat(path); err == nil {
			return "", ErrPreconditionFailed
		}
	} else {
		existing, err := os.ReadFile(path)
		if err != nil {
			return "", ErrPreconditionFailed
		}
		currentETag := fmt.Sprintf("%x", md5.Sum(existing))
		if currentETag != expectedETag {
			return "", ErrPreconditionFailed
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("creating directories: %w", err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("writing file: %w", err)
	}

	etag := fmt.Sprintf("%x", md5.Sum(data))
	return etag, nil
}

func (f *FileStore) Get(_ context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	path := f.objectPath(bucket, key)

	f.mu.RLock()
	defer f.mu.RUnlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectInfo{}, ErrNotFound
		}
		return nil, ObjectInfo{}, fmt.Errorf("reading file: %w", err)
	}

	info, _ := os.Stat(path)
	etag := fmt.Sprintf("%x", md5.Sum(data))

	return io.NopCloser(bytes.NewReader(data)), ObjectInfo{
		Key:          key,
		Size:         int64(len(data)),
		ETag:         etag,
		LastModified: info.ModTime(),
	}, nil
}

func (f *FileStore) GetReaderAt(_ context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	path := f.objectPath(bucket, key)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("opening file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("stat file: %w", err)
	}

	return file, info.Size(), nil
}

func (f *FileStore) Head(_ context.Context, bucket, key string) (ObjectInfo, error) {
	path := f.objectPath(bucket, key)

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, ErrNotFound
		}
		return ObjectInfo{}, fmt.Errorf("stat file: %w", err)
	}

	// Derive ETag from file metadata (mtime + size) instead of reading the
	// entire file for MD5. Head() is meant to be lightweight — callers that
	// need content-based ETags should use Get().
	etag := fmt.Sprintf("%x-%x", info.ModTime().UnixNano(), info.Size())

	return ObjectInfo{
		Key:          key,
		Size:         info.Size(),
		ETag:         etag,
		LastModified: info.ModTime(),
	}, nil
}

func (f *FileStore) List(_ context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	bucketPath := f.bucketDir(bucket)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		return nil, ErrBucketNotFound
	}

	var result []ObjectInfo
	seen := make(map[string]bool)

	err := filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if info.IsDir() {
			return nil
		}

		// Convert filesystem path back to key (forward slashes)
		rel, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)

		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			return nil
		}

		if opts.Delimiter != "" {
			rest := strings.TrimPrefix(key, opts.Prefix)
			idx := strings.Index(rest, opts.Delimiter)
			if idx >= 0 {
				prefix := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if !seen[prefix] {
					seen[prefix] = true
					result = append(result, ObjectInfo{Key: prefix})
				}
				return nil
			}
		}

		result = append(result, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking directory: %w", err)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	if opts.MaxKeys > 0 && len(result) > opts.MaxKeys {
		result = result[:opts.MaxKeys]
	}

	return result, nil
}

func (f *FileStore) Delete(_ context.Context, bucket, key string) error {
	path := f.objectPath(bucket, key)

	f.mu.Lock()
	defer f.mu.Unlock()

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting file: %w", err)
	}

	// Clean up empty parent directories up to the bucket root
	dir := filepath.Dir(path)
	bucketPath := f.bucketDir(bucket)
	for dir != bucketPath {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			break
		}
		os.Remove(dir)
		dir = filepath.Dir(dir)
	}

	return nil
}
