# Objstore process cache identity

Source: internal/storage/objstore/identity.go — type IdentifiedStore interface {, moved 2026-09-11 (#1026)
Superseded: Compaction and delete-marker GC now use UUIDv7 output names, not nanosecond-stamped keys.

IdentifiedStore is the optional interface a Store implements when it can
name its backing INSTANCE uniquely within this process. It exists for
process-lifetime caches keyed by object identity (the parquet footer
cache): a cache key must not collide across two unrelated stores that
happen to use the same bucket and key names.

The contract a store accepts by implementing this:

  - StoreID is stable for the store's lifetime and never reused by a
    different backing store within the process.
  - Two Store values that address the SAME backing store (e.g. two
    FileStores rooted at one directory, or a wrapper and its inner store)
    may — and should — return the same ID.
  - Under a given StoreID, a (bucket, key) pair names one immutable
    object: writers give new content a new key rather than rewriting a key
    in place. That is Wadjet's manifest discipline — ingest names chunks
    with a fresh UUID (ingest/ingest.go:300); compaction and delete-marker
    GC write new nanosecond-stamped keys and swap the manifest
    (compaction/compactor.go:50, catalog.SwapFileForGC) — and it is the
    same premise the base-table NVMe cache already relies on.

A store that cannot honour those (arbitrary remote URLs, mutable
third-party buckets) simply does not implement the interface, and
identity-keyed caches fail closed on it.
