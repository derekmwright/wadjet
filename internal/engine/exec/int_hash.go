package exec

// intHashTable is an open-addressing hash table mapping int64 keys to int32 values.
// Used for the integer join key fast path. Linear probing with Fibonacci hashing
// gives excellent cache locality and avoids the GC overhead of Go's built-in map.
//
// Empty slots are indicated by the sentinel key value (intHashEmpty).
// Deleted slots are not needed since we never remove entries during build/probe.
type intHashTable struct {
	keys []int64
	vals []int32
	mask uint64 // len(keys) - 1, for power-of-two modular indexing
	size int    // number of occupied slots
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
	keys := make([]int64, cap)
	for i := range keys {
		keys[i] = intHashEmpty
	}
	return &intHashTable{
		keys: keys,
		vals: make([]int32, cap),
		mask: uint64(cap - 1),
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
		k := h.keys[idx]
		if k == intHashEmpty {
			h.keys[idx] = key
			h.vals[idx] = val
			h.size++
			// Grow if load exceeds 70%
			if h.size*10 > len(h.keys)*7 {
				h.grow()
			}
			return 0, false
		}
		if k == key {
			old := h.vals[idx]
			h.vals[idx] = val
			return old, true
		}
		idx = (idx + 1) & h.mask
	}
}

// Get looks up a key. Returns the value and whether the key was found.
func (h *intHashTable) Get(key int64) (int32, bool) {
	idx := fibHash(key) & h.mask
	for {
		k := h.keys[idx]
		if k == intHashEmpty {
			return 0, false
		}
		if k == key {
			return h.vals[idx], true
		}
		idx = (idx + 1) & h.mask
	}
}

// Len returns the number of entries in the table.
func (h *intHashTable) Len() int { return h.size }

// ForEach iterates over all entries in the table.
func (h *intHashTable) ForEach(fn func(key int64, val int32)) {
	for i, k := range h.keys {
		if k != intHashEmpty {
			fn(k, h.vals[i])
		}
	}
}

// grow doubles the table capacity and rehashes all entries.
func (h *intHashTable) grow() {
	newCap := len(h.keys) * 2
	newKeys := make([]int64, newCap)
	for i := range newKeys {
		newKeys[i] = intHashEmpty
	}
	newVals := make([]int32, newCap)
	newMask := uint64(newCap - 1)

	for i, k := range h.keys {
		if k == intHashEmpty {
			continue
		}
		idx := fibHash(k) & newMask
		for newKeys[idx] != intHashEmpty {
			idx = (idx + 1) & newMask
		}
		newKeys[idx] = k
		newVals[idx] = h.vals[i]
	}

	h.keys = newKeys
	h.vals = newVals
	h.mask = newMask
}
