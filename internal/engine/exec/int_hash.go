package exec

import (
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

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

	// reg, when non-nil, backs entries with off-heap reservations (ADR-0006
	// amendment): the entry array is pointer-free bulk state, and at the
	// 100M-group scale each doubling's old+new-live rehash transient is
	// exactly the GC-pressure class the group-state arena eliminated.
	// grow() releases the old reservation immediately after the rehash.
	// nil (join builds, generic keys) keeps plain heap entries.
	reg            *memory.OffheapRegistry
	entriesOffheap bool
}

const intHashEmpty int64 = -0x7FFFFFFFFFFFFFFF // sentinel for empty slot

// newIntHashTable creates a hash table pre-sized for at least n entries at 70% load.
func newIntHashTable(n int) *intHashTable {
	return newIntHashTableReg(n, nil)
}

// newIntHashTableReg is newIntHashTable with off-heap entry backing when
// reg is non-nil (aggregate group indexes own an OffheapRegistry).
func newIntHashTableReg(n int, reg *memory.OffheapRegistry) *intHashTable {
	// Round up to next power of 2, targeting ~70% max load factor
	cap := 16
	target := n + n/3 // ~143% of n → 70% load at n entries
	for cap < target {
		cap <<= 1
	}
	h := &intHashTable{reg: reg}
	entries := h.allocEntries(cap)
	fillEmptyEntries(entries)
	h.entries = entries
	h.mask = uint64(cap - 1)
	return h
}

// allocEntries returns a len=n entry array, off-heap when the table has a
// registry and the reservation fits, heap otherwise.
func (h *intHashTable) allocEntries(n int) []intHashEntry {
	if h.reg != nil {
		if s, ok := memory.OffheapSized[intHashEntry](h.reg, n); ok {
			h.entriesOffheap = true
			return s
		}
	}
	h.entriesOffheap = false
	return make([]intHashEntry, n)
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

// fibHash mixes an int64 key into the 64-bit hash every int-keyed table in
// this engine indexes by.
//
// It used to be the Fibonacci multiply alone — `key * phi` — and a
// multiply-only hash has one bit-level property that matters here: the low k
// bits of the product depend ONLY on the low k bits of the key. So a key set
// whose members are all multiples of 2^s shares the same low s bits, and in
// any table with at most 2^s slots every one of them lands on the SAME slot:
// one probe chain, O(n) per lookup, a GROUP BY that degrades from linear to
// quadratic. That is not a contrived input — an id column allocated in
// strided blocks, a timestamp truncated to a minute or hour boundary, and a
// byte-aligned offset all produce it (#306). The property was DOCUMENTED
// in-tree rather than fixed, because the obvious fix gives up the other half
// of the multiply-only behaviour: for DENSE keys 0..n-1, `key * phi mod 2^k`
// is a bijection, so sequential ids collided exactly zero times.
//
// The mixing step is a PREFIX-XOR fold of the key's high bits into its low
// ones, applied BEFORE the multiply. Four xorshift-rights, each of them a
// bijection on 64 bits, so the composition with the multiply is a bijection
// too (TestFibHashIsInjective): two distinct keys never share a hash, and the
// only collisions left are the birthday collisions of the truncation to
// slots.
//
// # Why a fold and not a real avalanche
//
// murmur3's fmix64 (what DuckDB uses) was the first attempt. It avalanches,
// so it fixes every stride — but it also gives up the dense-key bijection,
// and that bijection is worth keeping: at the tables' 70% load factor, linear
// probing's UNSUCCESSFUL-search cost, which is what an INSERT pays and
// near-unique aggregation is all inserts, goes from 1.00 probes to about 6.
// The fold keeps both, and the evidence is the probe count itself — a
// deterministic number, unlike this package's aggregate wall-clock
// benchmarks, whose run-to-run variance on a loaded host reaches ±200% even
// on arms this function cannot touch (the string and packed key shapes hash
// elsewhere).
//
// Simulated over 2^20 keys in 2^21 slots, average probes per insert:
//
//	family        dense  +1e9  ×3    2^4    2^8   2^12  2^16   2^24    random
//	bare multiply  1.00  1.00  1.00  4.50   64.5  1024  16384  524288  1.50
//	this fold      1.00  1.00  1.19  1.00   1.73  2.05  1.00   1.00    1.50
//	fmix64         1.50  1.50  1.50  1.50   1.50  1.50  1.50   1.50    1.50
//
// with truncated timestamps (×60000, ×3600000) going 8.50→1.54 and 32.5→1.50.
// A dense key below 2^7 is untouched by any of the four shifts, and over a
// dense RANGE the fold stays injective on the low log2(slots) bits, which is
// what preserves the multiply's bijection there.
//
// TPC-H SF1 confirms no wall-clock cost, interleaved in one window, three
// runs each: bare 59.41 / 58.91 / 57.61 s, this 58.06 / 56.95 / 58.83 s.
//
// The partition bits — which partitionFor takes off the TOP of the same word,
// where the multiply already spreads well — are unaffected, so hash-once can
// still route and index from one value (TestHashSpreadFamilies). The fold is
// LINEAR, not an avalanche, so for a strided family the top and low windows
// stay mildly correlated; TestTwoLevelThreeWindowSpread's ownerBucketSkew
// records the 1.8× bucket skew that leaves.
//
// Not gated by an optswitch toggle: a hash cannot change a query's row SET,
// only the ORDER an unordered GROUP BY emits its groups in, and a toggle over
// that would hand the optimization-invariance oracle a false divergence to
// chase. Bisecting it means deleting the four shifts.
func fibHash(key int64) uint64 {
	// Fibonacci hashing constant for 64-bit: golden ratio * 2^64
	const phi = 11400714819323198485
	k := uint64(key)
	k ^= k >> 43
	k ^= k >> 29
	k ^= k >> 17
	k ^= k >> 7
	return k * phi
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

// GetOrInsertNoGrow is like GetOrInsert but defers the growth check.
// The caller must call CheckGrow() after a batch of inserts to maintain
// the load factor invariant. This variant is inlineable (cost < 80) unlike
// GetOrInsert (cost 132), eliminating function call overhead in hot loops
// like consumeBatchIntGroup which calls this millions of times.
func (h *intHashTable) GetOrInsertNoGrow(key int64, val int32) (int32, bool) {
	idx := fibHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			e.key = key
			e.val = val
			h.size++
			return val, false
		}
		if e.key == key {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsertNoGrowAt is GetOrInsertNoGrow with the slot hash supplied by the
// caller — the partition router already computed fibHash(key) to pick this
// table's owner, so the sink does not compute it a second time (hash once,
// partitioned_agg.go). hash MUST be fibHash(key): the table's probe order,
// its grow() rehash and every other entry in it assume exactly that function.
//
// Deliberately a copy of GetOrInsertNoGrow's body rather than a shared helper:
// both must stay under the inliner's budget on the hottest loop in the engine.
func (h *intHashTable) GetOrInsertNoGrowAt(key int64, hash uint64, val int32) (int32, bool) {
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			e.key = key
			e.val = val
			h.size++
			return val, false
		}
		if e.key == key {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// PutNoGrow inserts or updates a key-value pair without checking the load factor.
// The caller must call CheckGrow() after a batch of inserts. This variant is
// inlineable (cost < 80), eliminating function call overhead in hot loops like
// hash join build which calls Put millions of times.
func (h *intHashTable) PutNoGrow(key int64, val int32) (int32, bool) {
	idx := fibHash(key) & h.mask
	for {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			e.key = key
			e.val = val
			h.size++
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

// CheckGrow grows the table if load factor exceeds 70%.
// Must be called after a batch of GetOrInsertNoGrow/PutNoGrow calls.
//
//go:noinline
func (h *intHashTable) CheckGrow() {
	if h.size*10 > len(h.entries)*7 {
		h.grow()
	}
}

// EnsureCapacity grows the table so that `additional` more entries can be
// inserted via PutNoGrow without exceeding the 70% load factor.
// Call once per batch before a PutNoGrow loop.
//
//go:noinline
func (h *intHashTable) EnsureCapacity(additional int) {
	for (h.size+additional)*10 > len(h.entries)*7 {
		h.grow()
	}
}

// Len returns the number of entries in the table.
func (h *intHashTable) Len() int { return h.size }

// Slots returns the table's slot count — the denominator of the 70% load
// factor. The two-level conversion reads it to decide whether a whole-table
// rehash is already due (two_level_hash.go).
func (h *intHashTable) Slots() int { return len(h.entries) }

// MemoryUsage returns the total heap bytes consumed by the hash table.
func (h *intHashTable) MemoryUsage() int64 {
	return int64(cap(h.entries)) * 16 // sizeof(intHashEntry) = 16
}

// ForEach iterates over all entries in the table.
func (h *intHashTable) ForEach(fn func(key int64, val int32)) {
	for i := range h.entries {
		if h.entries[i].key != intHashEmpty {
			fn(h.entries[i].key, h.entries[i].val)
		}
	}
}

// Delete removes a key from the table, preserving probe-chain correctness via
// back-shift deletion. Returns (oldVal, true) if the key was present, or
// (0, false) otherwise. O(probe chain length) per call — O(1) amortized at
// the 70% load factor invariant.
//
// Back-shift rather than tombstones because tombstones bloat the hot lookup
// path (Get / GetOrInsertNoGrow) with an extra "is this a tombstone?" branch
// and cause the table to never naturally shrink. Used by HashAggregate's
// partial-drain spill path, where we Delete drained group keys before
// recycling their slots through the free-list.
func (h *intHashTable) Delete(key int64) (int32, bool) {
	n := len(h.entries)
	if n == 0 {
		return 0, false
	}
	idx := fibHash(key) & h.mask
	found := false
	for k := 0; k < n; k++ {
		e := &h.entries[idx]
		if e.key == intHashEmpty {
			return 0, false
		}
		if e.key == key {
			found = true
			break
		}
		idx = (idx + 1) & h.mask
	}
	if !found {
		return 0, false
	}
	deleted := h.entries[idx].val

	// Back-shift loop (Knuth Algorithm R). Walk forward from the hole; for
	// each filled slot j, an entry whose ideal position id lies cyclically
	// in (i, j] is still reachable with the hole at i and STAYS — but the
	// walk KEEPS GOING, because an entry further along can still have a home
	// at or before the hole and would be orphaned by it. The walk ends only
	// at an empty slot, which is where every probe chain ends anyway.
	//
	// Stopping at the first entry that stays was the bug: with a home of
	// exactly j that entry does not move, but the entry at j+1 can have
	// probed from a home before the hole, and leaving the hole where it was
	// cut its chain — Get returned "missing" for a key that was still in the
	// table. It was invisible while fibHash was multiply-only, because that
	// hash is a bijection on the low bits, so the dense integer keys this
	// path sees never formed a chain long enough to reach the case (#306).
	i := idx
	j := idx
	for k := 0; k < n; k++ {
		j = (j + 1) & h.mask
		e := &h.entries[j]
		if e.key == intHashEmpty {
			break
		}
		id := fibHash(e.key) & h.mask
		// Move iff e probed at least as far as the hole is from j — i.e.
		// the hole lies on e's chain.
		if ((j - id) & h.mask) < ((j - i) & h.mask) {
			continue
		}
		h.entries[i] = *e
		i = j
	}
	h.entries[i].key = intHashEmpty
	h.size--
	return deleted, true
}

// freeEntries drops the entry array, returning its off-heap reservation
// immediately when it had one. Used by the two-level conversion, which has
// already copied every live entry out (two_level_hash.go); the table is
// unusable afterwards.
func (h *intHashTable) freeEntries() {
	if h.entriesOffheap && len(h.entries) > 0 {
		h.reg.Release(unsafe.Pointer(unsafe.SliceData(h.entries)))
	}
	h.entries = nil
	h.entriesOffheap = false
	h.mask = 0
	h.size = 0
}

// grow doubles the table capacity and rehashes all entries.
func (h *intHashTable) grow() {
	newCap := len(h.entries) * 2
	oldOffheap := h.entriesOffheap
	old := h.entries
	newEntries := h.allocEntries(newCap)
	fillEmptyEntries(newEntries)
	newMask := uint64(newCap - 1)

	for i := range old {
		e := &old[i]
		if e.key == intHashEmpty {
			continue
		}
		idx := fibHash(e.key) & newMask
		for newEntries[idx].key != intHashEmpty {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = *e
	}

	if oldOffheap {
		// Return the old table's pages now — dead RSS otherwise held
		// until the owning aggregate's registry Close.
		h.reg.Release(unsafe.Pointer(unsafe.SliceData(old)))
	}
	h.entries = newEntries
	h.mask = newMask
}
