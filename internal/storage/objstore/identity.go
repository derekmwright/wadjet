package objstore

import (
	"fmt"
	"sync/atomic"
)

// IdentifiedStore names a backing instance uniquely for process-lifetime caches.
// StoreID must be stable and never reused by another backing store; wrappers
// or instances addressing the SAME backing should share an ID.
// Within an ID, (bucket,key) must identify IMMUTABLE content: rewrites get new
// keys. Ingest, compaction and GC use fresh UUID identities and manifest swaps.
// Stores unable to honor this, including arbitrary mutable remote sources, must
// not implement it; caches treat absent/empty identity as uncacheable.
// See docs/internals/objstore-process-cache-identity.md for the design.
type IdentifiedStore interface {
	StoreID() string
}

// StoreID returns s's process-unique instance identity, or "" when s does
// not provide one. Callers keying a cache on object identity MUST treat ""
// as "not cacheable" — failing closed is the only safe default, because a
// store that declines to identify itself is exactly the store whose
// (bucket, key) namespace we cannot reason about.
func StoreID(s Store) string {
	if s == nil {
		return ""
	}
	if is, ok := s.(IdentifiedStore); ok {
		return is.StoreID()
	}
	return ""
}

// storeSeq numbers MemStore instances. In-memory stores have no external
// name to key on, and two tests in one binary routinely use identical
// bucket and object names for different content, so each instance gets its
// own namespace.
var storeSeq atomic.Uint64

func nextStoreID(prefix string) string {
	return fmt.Sprintf("%s:%d", prefix, storeSeq.Add(1))
}
