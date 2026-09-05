package objstore

import (
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
	id      string
	mu      sync.RWMutex // protects concurrent writes to the same path
}

// NewFileStore creates a FileStore rooted at the given directory.
// The directory is created if it does not exist.
func NewFileStore(rootDir string) (*FileStore, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating filestore root %q: %w", rootDir, err)
	}
	// Absolute root: two FileStores over one directory ARE one store and must
	// share a namespace; two over different directories must not. A relative
	// root would alias them under a changed working directory.
	abs, err := filepath.Abs(rootDir)
	if err != nil {
		abs = rootDir
	}
	return &FileStore{rootDir: rootDir, id: "file:" + abs}, nil
}

// StoreID implements IdentifiedStore.
func (f *FileStore) StoreID() string { return f.id }

func (f *FileStore) bucketDir(bucket string) string {
	return filepath.Join(f.rootDir, bucket)
}

// objectPath maps a bucket and an object key to a path under the store root,
// and REFUSES a key that would leave it.
//
// filepath.Join CLEANS its result, so `filepath.Join(root, bucket, "../../x")`
// is a path outside the root with no error and no ".." left to see. Every
// caller below therefore has to be able to fail: `CREATE TABLE "../../../tmp/x"`
// put an arbitrary path here — the table prefix is built from the identifier
// and a double-quoted identifier is taken verbatim — and on the supported
// `storage.type: file` deployment that was Get's open, Put's temp file and
// Put's rename writing wherever the process could reach (CodeQL #23, #24, #25).
//
// The rule is ValidateObjectKey's, so it is the same rule the other stores
// apply; the containment check below is belt to that braces and costs one
// string comparison on a path that is about to be opened anyway.
func (f *FileStore) objectPath(bucket, key string) (string, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return "", err
	}
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	root := f.bucketDir(bucket)
	path := filepath.Join(root, filepath.FromSlash(key))
	if path != root && !strings.HasPrefix(path, root+string(filepath.Separator)) {
		return "", fmt.Errorf("object key %q resolves outside its bucket", key)
	}
	return path, nil
}

func (f *FileStore) MakeBucket(_ context.Context, bucket string) error {
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	return os.MkdirAll(f.bucketDir(bucket), 0o755)
}

func (f *FileStore) BucketExists(_ context.Context, bucket string) (bool, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return false, err
	}
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

	path, err := f.objectPath(bucket, key)
	if err != nil {
		return "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("creating directories: %w", err)
	}

	// Stream to a temp file in the target dir (hashing as we go), then
	// rename into place — same atomicity as the old WriteFile, no
	// buffering. The previous io.ReadAll held the WHOLE object live in
	// heap: a scan task's multi-hundred-MB stage-output upload arrived as
	// one allocation, which OOM-killed every node of a 512 MiB edge-box
	// cluster the moment scan outputs uploaded (2026-06-11 edge
	// validation, ~280 MB live in <200 ms). FileStore is the local/edge
	// store; it must stream like the S3 stores do.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".put-*")
	if err != nil {
		return "", fmt.Errorf("creating temp file: %w", err)
	}
	h := md5.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), r); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("writing data: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("renaming into place: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (f *FileStore) PutIfMatch(_ context.Context, bucket, key string, r io.Reader, _ int64, contentType string, expectedETag string) (string, error) {
	_ = contentType

	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("reading data: %w", err)
	}

	path, perr := f.objectPath(bucket, key)
	if perr != nil {
		return "", perr
	}

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
	path, err := f.objectPath(bucket, key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Return the open file directly instead of os.ReadFile — the previous
	// whole-object read meant every "streaming" consumer (stream_source
	// staging, coordinator result readers, scan) actually held the full
	// object in heap, uncharged, multiplied by read concurrency. Same
	// envelope bug as the Put side fixed 2026-06-11. The ETag is derived
	// from mtime+size like Head(); no production caller consumes a
	// content-MD5 ETag from Get. Reads after the rename-based Put see a
	// consistent file (Put never writes in place).
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectInfo{}, ErrNotFound
		}
		return nil, ObjectInfo{}, fmt.Errorf("opening file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, ObjectInfo{}, fmt.Errorf("stat file: %w", err)
	}

	return file, ObjectInfo{
		Key:          key,
		Size:         info.Size(),
		ETag:         fmt.Sprintf("%x-%x", info.ModTime().UnixNano(), info.Size()),
		LastModified: info.ModTime(),
	}, nil
}

func (f *FileStore) GetReaderAt(_ context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	path, err := f.objectPath(bucket, key)
	if err != nil {
		return nil, 0, err
	}

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
	path, err := f.objectPath(bucket, key)
	if err != nil {
		return ObjectInfo{}, err
	}

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
	// The bucket is a directory name directly under the root, so it takes the
	// same rule objectPath applies to a key. List and BucketExists reached
	// bucketDir without it, so `List(ctx, "../..")` WALKED outside the store
	// root and returned what it found there (round-1 review P4).
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}
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
	path, err := f.objectPath(bucket, key)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	err = os.Remove(path)
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
