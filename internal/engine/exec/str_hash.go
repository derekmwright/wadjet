package exec

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// strHashTable is an open-addressing hash table mapping byte-slice keys to int32 values.
// Used for string-keyed GROUP BY to avoid GC overhead of Go's built-in map[string].
//
// Key storage is a CHUNKED ARENA: keys are copied into fixed-size blocks that
// are allocated on demand and, once allocated, are never reallocated, copied,
// or mutated. A key's bytes therefore keep one address for as long as anything
// references them, and the GC keeps a chunk alive through any interior pointer
// into it. Two properties ride on that invariant:
//
//   - Insert costs exactly one memmove of the key. The single `arena []byte`
//     this replaced grew by plain append, so Go's ~1.25x large-slice growth
//     recopied every live key byte on every grow — 240 MB allocated and
//     memmoved for 24 MB of live keys on ClickBench Q34 (GROUP BY URL, ~18M
//     distinct keys at a mean 88.6 bytes), 24.8% of that query's CPU in
//     memmove+memclr.
//   - Callers may alias the stored bytes instead of copying the key a second
//     time. GetOrInsertRef returns the arena-resident copy; the single-string
//     GROUP BY path in aggregate.go builds its serializedKeys headers over it
//     (see arenaString there), which removed a full duplicate of every group
//     key from the heap.
//
// Addressing: strEntry.ref packs the chunk index in the high bits and the byte
// offset within that chunk in the low bits. Keys never span chunks — a key
// that does not fit the current chunk's remaining space starts a new one (the
// wasted tail is smaller than one key), and a key larger than a full chunk
// gets a dedicated, exact-size chunk of its own. Empty slots are marked by
// keyLen < 0.
type strHashTable struct {
	entries []strEntry
	mask    uint64
	size    int

	// Key arena. chunks[i] has len == bytes used and cap == the chunk's
	// fixed size; only the last chunk is ever appended to, and appends are
	// capacity-checked so append() can never reallocate a chunk out from
	// under a key slice handed to a caller.
	chunks   [][]byte
	arenaCap int64 // sum of cap(chunks[i]), maintained incrementally
}

// Key-arena geometry: 20 offset bits and 12 chunk-index bits, so chunks hold
// up to 1 MiB and a table addresses at most 4096 of them (4 GiB of key bytes).
// The single-slice arena this replaced held its offset in an int32 and so
// corrupted silently past 2 GiB; the limit here is checked, not silent.
const (
	strArenaOffsetBits = 20
	strArenaMaxChunk   = 1 << strArenaOffsetBits
	strArenaOffsetMask = strArenaMaxChunk - 1
	strArenaMaxChunks  = 1 << (32 - strArenaOffsetBits)
	// Chunk sizes ramp 8 KiB → 1 MiB so a 4-group aggregate does not pay a
	// megabyte up front and a 20M-group one does not pay thousands of
	// allocations.
	strArenaFirstChunk = 8 << 10
	strArenaRampSteps  = 7 // strArenaFirstChunk << 7 == strArenaMaxChunk
)

type strEntry struct {
	ref     uint32 // packed key location: chunk index << strArenaOffsetBits | offset
	keyLen  int32  // key length in bytes (-1 = empty slot)
	val     int32  // user value (group index)
	hashTag uint32 // lower 32 bits of key hash — fast-reject before byte comparison
}

// newStrHashTable creates a hash table pre-sized for at least n entries at 70% load.
// The key arena is not pre-sized: chunks are allocated on demand as keys arrive,
// so an over-estimated n costs only the entries array.
func newStrHashTable(n int) *strHashTable {
	cap := 16
	target := n + n/3
	for cap < target {
		cap <<= 1
	}
	entries := make([]strEntry, cap)
	fillEmptyStrEntries(entries)
	return &strHashTable{
		entries: entries,
		mask:    uint64(cap - 1),
	}
}

// fillEmptyStrEntries sets all entries to the empty sentinel using copy-doubling.
// This is O(n) but uses hardware-optimized memmove via copy() instead of
// per-element assignment, which is ~8x faster for large tables.
func fillEmptyStrEntries(entries []strEntry) {
	if len(entries) == 0 {
		return
	}
	entries[0] = strEntry{keyLen: -1}
	for i := 1; i < len(entries); i *= 2 {
		copy(entries[i:], entries[:i])
	}
}

// strHash hashes a byte slice using FNV-1a-style mixing.
// Processes 8 bytes at a time for keys >= 8 bytes to reduce loop iterations.
func strHash(key []byte) uint64 {
	h := uint64(0x517cc1b727220a95) // seed
	// Process 8 bytes at a time
	for len(key) >= 8 {
		k := binary.LittleEndian.Uint64(key)
		h = (h ^ k) * 0x9e3779b97f4a7c15
		key = key[8:]
	}
	// Process remaining bytes
	for _, b := range key {
		h = (h ^ uint64(b)) * 0x9e3779b97f4a7c15
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}

// keyAt returns the arena bytes of an occupied entry. The result aliases the
// chunk and stays valid — and unchanged — for as long as the chunk is
// referenced.
func (h *strHashTable) keyAt(e strEntry) []byte {
	off := int(e.ref & strArenaOffsetMask)
	return h.chunks[e.ref>>strArenaOffsetBits][off : off+int(e.keyLen)]
}

// storeKey copies key into the arena, returning its packed ref and the
// arena-resident bytes.
func (h *strHashTable) storeKey(key []byte) (uint32, []byte) {
	ci := len(h.chunks) - 1
	if ci < 0 || len(key) > cap(h.chunks[ci])-len(h.chunks[ci]) {
		h.addChunk(len(key))
		ci = len(h.chunks) - 1
	}
	off := len(h.chunks[ci])
	// The fit check above guarantees spare capacity, so this append never
	// reallocates — that is what keeps previously handed-out key slices (and
	// the string headers built over them) valid.
	h.chunks[ci] = append(h.chunks[ci], key...)
	return uint32(ci)<<strArenaOffsetBits | uint32(off), h.chunks[ci][off : off+len(key) : off+len(key)]
}

// addChunk appends a fresh arena chunk with room for a key of `need` bytes.
// Sizes ramp toward strArenaMaxChunk; a key too large for a full chunk gets a
// dedicated chunk sized exactly to it, which the fit check in storeKey then
// treats as full (offset 0 is the only address it ever hands out, so oversize
// chunks cannot overflow the offset field).
func (h *strHashTable) addChunk(need int) {
	if len(h.chunks) >= strArenaMaxChunks {
		panic(fmt.Sprintf("exec: string key arena exhausted after %d chunks (%d bytes); "+
			"a single hash table cannot hold more than %d bytes of keys",
			len(h.chunks), h.arenaCap, strArenaMaxChunks*strArenaMaxChunk))
	}
	size := strArenaMaxChunk
	if n := len(h.chunks); n < strArenaRampSteps {
		size = strArenaFirstChunk << n
	}
	if need > size {
		size = need
	}
	h.chunks = append(h.chunks, make([]byte, 0, size))
	h.arenaCap += int64(size)
}

// Get looks up a key. Returns the value and whether the key was found.
func (h *strHashTable) Get(key []byte) (int32, bool) {
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.keyLen < 0 {
			return 0, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.keyAt(*e), key) {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// Put inserts a key-value pair. Returns the previous value and whether the key existed.
func (h *strHashTable) Put(key []byte, val int32) (int32, bool) {
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.keyLen < 0 {
			// New entry — copy the key into the arena
			ref, _ := h.storeKey(key)
			e.ref = ref
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return 0, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.keyAt(*e), key) {
			old := e.val
			e.val = val
			return old, true
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsert looks up a key. If found, returns the value and true.
// If not found, inserts with the given value and returns the value and false.
// This is the hot path for GROUP BY — combines lookup and insert in one probe.
func (h *strHashTable) GetOrInsert(key []byte, val int32) (int32, bool) {
	v, found, _ := h.GetOrInsertRef(key, val)
	return v, found
}

// GetOrInsertRef is GetOrInsert plus the arena-resident copy of a newly
// inserted key (nil when the key was already present — that caller already
// holds the bytes it probed with). Those bytes are owned by the table and are
// immutable for as long as they are referenced, so a caller that needs to keep
// the key can build a string header over them instead of copying it again.
func (h *strHashTable) GetOrInsertRef(key []byte, val int32) (int32, bool, []byte) {
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.keyLen < 0 {
			ref, stored := h.storeKey(key)
			e.ref = ref
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return val, false, stored
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.keyAt(*e), key) {
			return e.val, true, nil
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsertRefAt is GetOrInsertRef with the key hash supplied by the caller:
// the partition router already ran strHash over these bytes to pick this
// table's owner, and on an ~88-byte URL that pass is the expensive half of the
// probe (hash once — partitioned_agg.go). hash MUST be strHash(key) — the
// stored hashTag is its low 32 bits and grow() re-indexes off that tag.
func (h *strHashTable) GetOrInsertRefAt(key []byte, hash uint64, val int32) (int32, bool, []byte) {
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.keyLen < 0 {
			ref, stored := h.storeKey(key)
			e.ref = ref
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return val, false, stored
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.keyAt(*e), key) {
			return e.val, true, nil
		}
		idx = (idx + 1) & h.mask
	}
}

// PutNoGrow inserts a key-value pair without checking the load factor.
// The caller must call CheckGrow() after a batch of inserts. This variant
// defers growth to reduce overhead in hot loops like hash join build.
func (h *strHashTable) PutNoGrow(key []byte, val int32) (int32, bool) {
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.keyLen < 0 {
			ref, _ := h.storeKey(key)
			e.ref = ref
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			return 0, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.keyAt(*e), key) {
			old := e.val
			e.val = val
			return old, true
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsertNoGrow is like GetOrInsert but defers the growth check.
// The caller must call CheckGrow() after a batch of inserts.
func (h *strHashTable) GetOrInsertNoGrow(key []byte, val int32) (int32, bool) {
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.keyLen < 0 {
			ref, _ := h.storeKey(key)
			e.ref = ref
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			return val, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.keyAt(*e), key) {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// CheckGrow grows the table if load factor exceeds 70%.
// Must be called after a batch of PutNoGrow/GetOrInsertNoGrow calls.
//
//go:noinline
func (h *strHashTable) CheckGrow() {
	if h.size*10 > len(h.entries)*7 {
		h.grow()
	}
}

// EnsureCapacity grows the table so that `additional` more entries can be
// inserted via PutNoGrow without exceeding the 70% load factor.
// Call once per batch before a PutNoGrow loop.
//
//go:noinline
func (h *strHashTable) EnsureCapacity(additional int) {
	for (h.size+additional)*10 > len(h.entries)*7 {
		h.grow()
	}
}

// Len returns the number of entries.
func (h *strHashTable) Len() int { return h.size }

// MemoryUsage returns the total heap bytes consumed by the hash table
// (entries array + key arena chunks).
func (h *strHashTable) MemoryUsage() int64 {
	return int64(cap(h.entries))*16 + h.arenaCap // sizeof(strEntry) = 16
}

// grow doubles the table capacity and rehashes entries.
// The arena stays the same — only the index is rebuilt.
// Uses stored hashTag for index computation instead of re-reading keys
// from the arena and re-computing the hash, avoiding cache misses and
// hash computation for every entry.
func (h *strHashTable) grow() {
	newCap := len(h.entries) * 2
	newEntries := make([]strEntry, newCap)
	fillEmptyStrEntries(newEntries)
	newMask := uint64(newCap - 1)

	for _, e := range h.entries {
		if e.keyLen < 0 {
			continue
		}
		// hashTag stores lower 32 bits of the original hash. Since newMask
		// is always < 2^32 for practical table sizes, this gives the same
		// index as recomputing the full 64-bit hash.
		idx := uint64(e.hashTag) & newMask
		for newEntries[idx].keyLen >= 0 {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = e
	}

	h.entries = newEntries
	h.mask = newMask
}

// ForEach calls fn for each key in the table.
func (h *strHashTable) ForEach(fn func(key []byte)) {
	for _, e := range h.entries {
		if e.keyLen >= 0 {
			fn(h.keyAt(e))
		}
	}
}

// ForEachWithValue calls fn for each key-value pair. Saves a separate
// Get lookup when the caller needs both key and value (e.g., merge phase).
func (h *strHashTable) ForEachWithValue(fn func(key []byte, val int32)) {
	for _, e := range h.entries {
		if e.keyLen >= 0 {
			fn(h.keyAt(e), e.val)
		}
	}
}

// strKeyEqual compares two byte slices for equality.
// Uses bytes.Equal which has assembly-optimized comparison.
func strKeyEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}
