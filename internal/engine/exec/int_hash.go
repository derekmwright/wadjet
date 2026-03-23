package exec

// intHashEntry stores a key-value pair co-located in memory.
// Size: 16 bytes (8 key + 4 val + 4 padding). Four entries fit in one 64-byte
// cache line, so a probe hit reads both key and value in a single cache access
// instead of two separate array lookups (SoA layout).
type intHashEntry struct {
	key int64
	val int32
}

// intHashTable is an open-addressing hash table mapping int64 keys to int32 values.
// Used for the integer join key fast path. Linear probing with Fibonacci hashing
// gives excellent cache locality and avoids the GC overhead of Go's built-in map.
//
// Array-of-Structs layout: key and value are co-located so that a probe hit
// loads both from the same cache line, halving cache misses vs separate arrays.
//
// Empty slots are indicated by the sentinel key value (intHashEmpty).
// Deleted slots are not needed since we never remove entries during build/probe.
type intHashTable struct {
	entries []intHashEntry
	mask    uint64 // len(entries) - 1, for power-of-two modular indexing
	size    int    // number of occupied slots
}

const intHashEmpty int64 = -0x7FFFFFFFFFFFFFFF // sentinel for empty slot

// newIntHashTable creates a hash table pre-sized for at least n entries at 70% load.
func newIntHashTable(n int) *intHashTable {
	// Round up to next power of 2, targeting ~70% max load factor
	cap := 16
	target := n + n/3 // ~143% of n → 70% load at n entries
	for cap < target {
		cap <<= 1
	}
	entries := make([]intHashEntry, cap)
	fillEmptyEntries(entries)
	return &intHashTable{
		entries: entries,
		mask:    uint64(cap - 1),
	}
}

// fillEmptyEntries sets all entries to the empty sentinel using copy-doubling.
// This is O(n) but uses hardware-optimized memmove via copy() instead of
// per-element assignment, which is ~8x faster for large tables.
func fillEmptyEntries(entries []intHashEntry) {
	if len(entries) == 0 {
		return
	}
	entries[0].key = intHashEmpty
	for i := 1; i < len(entries); i *= 2 {
		copy(entries[i:], entries[:i])
	}
}

// fibHash mixes an int64 key using Fibonacci hashing for good distribution.
func fibHash(key int64) uint64 {
	// Fibonacci hashing constant for 64-bit: golden ratio * 2^64
	const phi = 11400714819323198485
	return uint64(key) * phi
}

// Put inserts or updates a key-value pair. Returns the previous value and whether
// the key already existed.
func (h *intHashTable) Put(key int64, val int32) (int32, bool) {
	idx := fibHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			e.key = key
			e.val = val
			h.size++
			// Grow if load exceeds 70%
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return 0, false
		}
		if e.key == key {
			old := e.val
			e.val = val
			return old, true
		}
		idx = (idx + 1) & h.mask
	}
}

// Get looks up a key. Returns the value and whether the key was found.
func (h *intHashTable) Get(key int64) (int32, bool) {
	idx := fibHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			return 0, false
		}
		if e.key == key {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsert looks up a key. If found, returns the value and true.
// If not found, inserts with the given value and returns the value and false.
// Combines lookup and insert in a single probe chain (one hash, one walk).
func (h *intHashTable) GetOrInsert(key int64, val int32) (int32, bool) {
	idx := fibHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			e.key = key
			e.val = val
			h.size++
			if h.size*10 > len(h.entries)*7 {
				h.grow()
			}
			return val, false
		}
		if e.key == key {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// Len returns the number of entries in the table.
func (h *intHashTable) Len() int { return h.size }

// ForEach iterates over all entries in the table.
func (h *intHashTable) ForEach(fn func(key int64, val int32)) {
	for i := range h.entries {
		if h.entries[i].key != intHashEmpty {
			fn(h.entries[i].key, h.entries[i].val)
		}
	}
}

// grow doubles the table capacity and rehashes all entries.
func (h *intHashTable) grow() {
	newCap := len(h.entries) * 2
	newEntries := make([]intHashEntry, newCap)
	fillEmptyEntries(newEntries)
	newMask := uint64(newCap - 1)

	for i := range h.entries {
		e := &h.entries[i]
		if e.key == intHashEmpty {
			continue
		}
		idx := fibHash(e.key) & newMask
		for newEntries[idx].key != intHashEmpty {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = *e
	}

	h.entries = newEntries
	h.mask = newMask
}
