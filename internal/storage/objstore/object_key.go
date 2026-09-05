package objstore

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateObjectKey is the ONE rule for what an object key may be, and every
// store asks it, so a key one store accepts is a key they all accept.
//
// The rule: a key is one or more "/"-separated components; no component is
// empty, "." or ".."; no byte is NUL; the key is neither absolute nor a
// Windows volume path and carries no backslash. That is exactly the set for
// which `filepath.Join(root, bucket, filepath.FromSlash(key))` is guaranteed
// to stay under `root/bucket`.
//
// It exists because FileStore had no such rule and filepath.Join CLEANS its
// result: `filepath.Join(root, bucket, "../../../tmp/x")` is a path outside
// the root, with no error and no ".." left to see. The keys this store is
// handed are built from user-controlled names — a table's data lives under
// `tables/<name>/` (partition.TablePrefix) and a table name is a SQL
// identifier, which a DOUBLE-QUOTED spelling takes verbatim — so
// `CREATE TABLE "../../../tmp/x"` was an arbitrary file write anywhere the
// process could reach, on the supported `storage.type: file` deployment
// (CodeQL go/path-injection alerts #23, #24 and #25: Get's open, Put's temp
// file, and Put's rename into place).
//
// The catalog refuses such a NAME as well, at CREATE, which is where a person
// can be told what is wrong. This is the layer that has to be right anyway:
// the catalog is one of several callers, and a store cannot know what its keys
// were made of.
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
