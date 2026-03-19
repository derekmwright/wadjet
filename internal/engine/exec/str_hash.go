package exec

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
	offset int32 // byte offset into arena (-1 = empty)
	keyLen int32 // key length in bytes
	val    int32 // user value (group index)
}

// newStrHashTable creates a hash table pre-sized for at least n entries at 70% load.
func newStrHashTable(n int) *strHashTable {
	cap := 16
	target := n + n/3
	for cap < target {
		cap <<= 1
	}
	entries := make([]strEntry, cap)
	for i := range entries {
		entries[i].offset = -1
	}
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

// wyhash-inspired fast hash for byte slices.
// Uses the same mixing constants as wyhash for excellent distribution.
func strHash(key []byte) uint64 {
	var h uint64 = 0x517cc1b727220a95 // seed
	for _, b := range key {
		h = (h ^ uint64(b)) * 0x9e3779b97f4a7c15
	}
	// Final mix
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
	idx := strHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.offset == -1 {
			return 0, false
		}
		if e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// Put inserts a key-value pair. Returns the previous value and whether the key existed.
func (h *strHashTable) Put(key []byte, val int32) (int32, bool) {
	idx := strHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.offset == -1 {
			// New entry — append key to arena
			offset := int32(len(h.arena))
			h.arena = append(h.arena, key...)
			e.offset = offset
			e.keyLen = int32(len(key))
			e.val = val
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return 0, false
		}
		if e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
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
	idx := strHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.offset == -1 {
			offset := int32(len(h.arena))
			h.arena = append(h.arena, key...)
			e.offset = offset
			e.keyLen = int32(len(key))
			e.val = val
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return val, false
		}
		if e.keyLen == int32(len(key)) && strKeyEqual(h.arena[e.offset:e.offset+e.keyLen], key) {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// Len returns the number of entries.
func (h *strHashTable) Len() int { return h.size }

// grow doubles the table capacity and rehashes entries.
// The arena stays the same — only the index is rebuilt.
func (h *strHashTable) grow() {
	newCap := len(h.entries) * 2
	newEntries := make([]strEntry, newCap)
	for i := range newEntries {
		newEntries[i].offset = -1
	}
	newMask := uint64(newCap - 1)

	for _, e := range h.entries {
		if e.offset == -1 {
			continue
		}
		key := h.arena[e.offset : e.offset+e.keyLen]
		idx := strHash(key) & newMask
		for newEntries[idx].offset != -1 {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = e
	}

	h.entries = newEntries
	h.mask = newMask
}

// strKeyEqual compares two byte slices for equality.
// Inlined by the compiler for short keys.
func strKeyEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
