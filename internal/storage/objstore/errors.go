package objstore

import "errors"

var (
	// ErrNotFound is returned when a requested object does not exist.
	ErrNotFound = errors.New("object not found")

	// ErrPreconditionFailed is returned when a conditional write fails due to ETag mismatch.
	ErrPreconditionFailed = errors.New("precondition failed: etag mismatch")

	// ErrBucketNotFound is returned when a referenced bucket does not exist.
	ErrBucketNotFound = errors.New("bucket not found")
)
