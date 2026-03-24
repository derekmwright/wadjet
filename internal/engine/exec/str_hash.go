package exec

import (
	"bytes"
	"encoding/binary"
)

// strHashTable is an open-addressing hash table mapping byte-slice keys to int32 values.
// Used for string-keyed GROUP BY to avoid GC overhead of Go's built-in map[string].
//
// Keys are stored in a contiguous byte arena to minimize pointer chasing and GC pressure.
// Empty slots are indicated by offset == -1 in the entries slice.
type strHashTable struct {
	entries []strEntry
	mask    uint64
	size    int

	// Key arena: all keys are packed contiguously here.
	// Each key is stored as [len:4 bytes][data:len bytes].
	arena []byte
}

type strEntry struct {
	offset  int32  // byte offset into arena (-1 = empty)
	keyLen  int32  // key length in bytes
	val     int32  // user value (group index)
	hashTag uint32 // lower 32 bits of key hash — fast-reject before byte comparison
}

// newStrHashTable creates a hash table pre-sized for at least n entries at 70% load.
func newStrHashTable(n int) *strHashTable {
	cap := 16
	target := n + n/3
	for cap < target {
		cap <<= 1
	}
	entries := make([]strEntry, cap)
	fillEmptyStrEntries(entries)
	// Pre-allocate arena assuming ~32 bytes per key on average
	arenaSize := n * 32
	if arenaSize < 4096 {
		arenaSize = 4096
	}
	return &strHashTable{
		entries: entries,
		mask:    uint64(cap - 1),
		arena:   make([]byte, 0, arenaSize),
	}
}

// fillEmptyStrEntries sets all entries to the empty sentinel using copy-doubling.
// This is O(n) but uses hardware-optimized memmove via copy() instead of
// per-element assignment, which is ~8x faster for large tables.
func fillEmptyStrEntries(entries []strEntry) {
	if len(entries) == 0 {
		return
	}
	entries[0] = strEntry{offset: -1}
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

// getKey returns the key bytes at the given entry.
func (h *strHashTable) getKey(e strEntry) []byte {
	return h.arena[e.offset : e.offset+e.keyLen]
}

// Get looks up a key. Returns the value and whether the key was found.
func (h *strHashTable) Get(key []byte) (int32, bool) {
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.offset == -1 {
			return 0, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
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
		if e.offset == -1 {
			// New entry — append key to arena
			offset := int32(len(h.arena))
			h.arena = append(h.arena, key...)
			e.offset = offset
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return 0, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
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
	hash := strHash(key)
	tag := uint32(hash)
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.offset == -1 {
			offset := int32(len(h.arena))
			h.arena = append(h.arena, key...)
			e.offset = offset
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return val, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
			return e.val, true
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
		if e.offset == -1 {
			offset := int32(len(h.arena))
			h.arena = append(h.arena, key...)
			e.offset = offset
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			return 0, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
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
		if e.offset == -1 {
			offset := int32(len(h.arena))
			h.arena = append(h.arena, key...)
			e.offset = offset
			e.keyLen = int32(len(key))
			e.val = val
			e.hashTag = tag
			h.size++
			return val, false
		}
		if e.hashTag == tag && e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
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
		if e.offset == -1 {
			continue
		}
		// hashTag stores lower 32 bits of the original hash. Since newMask
		// is always < 2^32 for practical table sizes, this gives the same
		// index as recomputing the full 64-bit hash.
		idx := uint64(e.hashTag) & newMask
		for newEntries[idx].offset != -1 {
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
		if e.offset >= 0 {
			fn(h.arena[e.offset : e.offset+e.keyLen])
		}
	}
}

// ForEachWithValue calls fn for each key-value pair. Saves a separate
// Get lookup when the caller needs both key and value (e.g., merge phase).
func (h *strHashTable) ForEachWithValue(fn func(key []byte, val int32)) {
	for _, e := range h.entries {
		if e.offset >= 0 {
			fn(h.arena[e.offset:e.offset+e.keyLen], e.val)
		}
	}
}

// strKeyEqual compares two byte slices for equality.
// Uses bytes.Equal which has assembly-optimized comparison.
func strKeyEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}
