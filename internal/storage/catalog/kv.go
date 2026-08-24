package catalog

import "errors"

// ErrKeyNotFound is returned when a key does not exist in the KV store.
var ErrKeyNotFound = errors.New("key not found")

// ErrRevisionMismatch is returned when a CAS update fails due to a
// concurrent modification (the key's current revision != expected).
var ErrRevisionMismatch = errors.New("revision mismatch")

// MetaKV abstracts key-value storage for catalog metadata.
// Production uses NATSKVAdapter; tests/embedded use MemKV.
type MetaKV interface {
	// Get returns the value and revision for a key.
	// Returns ErrKeyNotFound if the key does not exist.
	Get(key string) (value []byte, revision uint64, err error)

	// Put creates or updates a key, returning the new revision.
	Put(key string, value []byte) (revision uint64, err error)

	// Update performs a compare-and-swap: writes value only if the key's
	// current revision matches expectedRev. Returns ErrRevisionMismatch
	// if a concurrent write changed the key since it was read.
	Update(key string, value []byte, expectedRev uint64) (revision uint64, err error)

	// Delete removes a key. No error if the key does not exist.
	Delete(key string) error

	// List returns all keys matching the given prefix.
	// An empty prefix returns all keys.
	List(prefix string) ([]string, error)
}

// RevisionReader is an OPTIONAL MetaKV capability: report a key's current
// revision without transferring its value.
//
// Catalog validates every cached manifest against the KV revision on every
// read (a wall-clock TTL is not a correctness mechanism — see
// Catalog.GetManifest), so the validation happens on the hottest planner
// path there is. A store that can answer "what revision is this key at"
// without shipping back a manifest that is megabytes of JSON at SF100
// turns that validation into an O(1) probe. A store that cannot (NATS KV
// has no value-free get) simply omits the method and pays the full read,
// which is what the pre-cache code did anyway.
type RevisionReader interface {
	// Revision returns the key's current revision, or ErrKeyNotFound.
	Revision(key string) (uint64, error)
}
