package catalog

import "errors"

// ErrKeyNotFound is returned when a key does not exist in the KV store.
var ErrKeyNotFound = errors.New("key not found")

// MetaKV abstracts key-value storage for catalog metadata.
// Production uses NATSKVAdapter; tests/embedded use MemKV.
type MetaKV interface {
	// Get returns the value and revision for a key.
	// Returns ErrKeyNotFound if the key does not exist.
	Get(key string) (value []byte, revision uint64, err error)

	// Put creates or updates a key, returning the new revision.
	Put(key string, value []byte) (revision uint64, err error)

	// Delete removes a key. No error if the key does not exist.
	Delete(key string) error

	// List returns all keys matching the given prefix.
	// An empty prefix returns all keys.
	List(prefix string) ([]string, error)
}
