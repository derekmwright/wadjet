// Package objstore provides an abstraction over S3-compatible object storage.
package objstore

import (
	"context"
	"io"
	"time"
)

// ObjectInfo contains metadata about a stored object.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// ListOptions configures a list operation.
type ListOptions struct {
	Prefix    string
	Delimiter string
	MaxKeys   int
}

// ReaderAtCloser combines io.ReaderAt with io.Closer for random-access reads.
type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

// ReaderAtStore is an optional interface that Store implementations can provide
// to support random-access reads. This enables Parquet column pruning by reading
// only needed column chunks instead of downloading entire files.
type ReaderAtStore interface {
	GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error)
}

// Store defines the interface for S3-compatible object storage operations.
type Store interface {
	// Put writes an object to storage. Returns the ETag of the written object.
	Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (etag string, err error)

	// PutIfMatch writes an object only if the current ETag matches expectedETag.
	// If expectedETag is empty, the object must not exist (create-only).
	// Returns the new ETag or ErrPreconditionFailed.
	PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (etag string, err error)

	// Get retrieves an object from storage.
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error)

	// Head retrieves metadata without downloading the object body.
	Head(ctx context.Context, bucket, key string) (ObjectInfo, error)

	// List returns objects matching the given options.
	List(ctx context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error)

	// Delete removes an object from storage.
	Delete(ctx context.Context, bucket, key string) error

	// BucketExists checks whether a bucket exists.
	BucketExists(ctx context.Context, bucket string) (bool, error)

	// MakeBucket creates a bucket if it does not exist.
	MakeBucket(ctx context.Context, bucket string) error
}
