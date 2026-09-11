package objstore

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateObjectKey is the shared store rule: one or more slash-separated
// components, none empty, '.' or '..'; no NUL/backslash, absolute path or
// platform-recognized volume path. Joining must stay lexically under root/bucket.
// Enforce here regardless of caller, even though catalog CREATE validates names
// as well (#23, #24, #25). filepath.Join cleaning must not hide traversal.
// This string check does not itself establish filesystem symlink confinement.
// See docs/internals/objstore-object-key-component-rule.md for the design.
func ValidateObjectKey(key string) error {
	if key == "" {
		return fmt.Errorf("object key is empty")
	}
	if strings.ContainsRune(key, 0) {
		return fmt.Errorf("object key %q contains a NUL byte", key)
	}
	if strings.ContainsRune(key, '\\') {
		return fmt.Errorf("object key %q contains a backslash", key)
	}
	if strings.HasPrefix(key, "/") || filepath.VolumeName(key) != "" {
		return fmt.Errorf("object key %q is absolute", key)
	}
	for _, seg := range strings.Split(key, "/") {
		switch seg {
		case "":
			return fmt.Errorf("object key %q has an empty path component", key)
		case ".", "..":
			return fmt.Errorf("object key %q has a %q path component", key, seg)
		}
	}
	return nil
}

// ValidateBucketName holds a bucket to a single safe path component, for the
// same reason and by the same rule: FileStore turns it into a directory
// directly under the root.
func ValidateBucketName(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket name is empty")
	}
	if strings.ContainsAny(bucket, "/\\") || strings.ContainsRune(bucket, 0) {
		return fmt.Errorf("bucket name %q is not a single path component", bucket)
	}
	if bucket == "." || bucket == ".." {
		return fmt.Errorf("bucket name %q is not a usable directory name", bucket)
	}
	return nil
}

// CheckObjectAccess is the pair of checks every store makes before it touches a
// named object, on EVERY operation and not only on the write.
//
// FileStore has always had to, because its key becomes a path; MemStore and the
// S3 store validated on Put and PutIfMatch only, so `Get("../escape")` was a
// key error on one store and "object not found" on the others. ADR-0012's
// entry says the rule is applied by all three "alike", and a rule that answers
// differently per store is how a table that works in a test fails in
// production — this arc's own argument (round-1 review P3).
func CheckObjectAccess(bucket, key string) error {
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	return ValidateObjectKey(key)
}
