# String hash chunked key arena

Source: internal/engine/exec/str_hash.go — strHashTable, moved 2026-09-11 (#1026)

strHashTable is an open-addressing hash table mapping byte-slice keys to int32 values.
Used for string-keyed GROUP BY to avoid GC overhead of Go's built-in map[string].

Key storage is a CHUNKED ARENA: keys are copied into fixed-size blocks that
are allocated on demand and, once allocated, are never reallocated, copied,
or mutated. A key's bytes therefore keep one address for as long as anything
references them, and the GC keeps a chunk alive through any interior pointer
into it. Two properties ride on that invariant:

  - Insert costs exactly one memmove of the key. The single `arena []byte`
    this replaced grew by plain append, so Go's ~1.25x large-slice growth
    recopied every live key byte on every grow — 240 MB allocated and
    memmoved for 24 MB of live keys on ClickBench Q34 (GROUP BY URL, ~18M
    distinct keys at a mean 88.6 bytes), 24.8% of that query's CPU in
    memmove+memclr.
  - Callers may alias the stored bytes instead of copying the key a second
    time. GetOrInsertRef returns the arena-resident copy; the single-string
    GROUP BY path in aggregate.go builds its serializedKeys headers over it
    (see arenaString there), which removed a full duplicate of every group
    key from the heap.

Addressing: strEntry.ref packs the chunk index in the high bits and the byte
offset within that chunk in the low bits. Keys never span chunks — a key
that does not fit the current chunk's remaining space starts a new one (the
wasted tail is smaller than one key), and a key larger than a full chunk
gets a dedicated, exact-size chunk of its own. Empty slots are marked by
keyLen < 0.
