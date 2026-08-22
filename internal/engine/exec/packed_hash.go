package exec

import (
	"math/bits"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Packed composite group keys (G3 in
// docs/benchmarks/high-card-aggregation-gap-2026-08-17.md).
//
// The dual-int GROUP BY path stored the key across THREE per-group SoA
// arrays (dualIntKeysA, dualIntKeysB, dualIntNextGroup) while the hash table
// held only hash→chain-head. Verifying one candidate key therefore took
// three dependent loads into three multi-GB arrays, a chain walk multiplied
// that, and every insert probed twice (Get, then Put down the same chain).
// ClickBench Q33 (GROUP BY WatchID, ClientIP at ~1:1 group:row) spends 62.8%
// of its CPU in that loop.
//
// When every group column is fixed-width int-class and the widths sum to
// <= 16 bytes, the whole key fits in one 128-bit cell that lives INSIDE the
// hash entry (ClickHouse's keys128 method): one probe, one compare against
// data already in the loaded cache line, no chain, no side arrays. The
// per-group key SoA survives as a single []packedKey (16 B/group in ONE
// off-heap array, down from 20 B across three) because emission, merge, the
// drain cursor and the spill run format all index state by group id.
//
// Eligibility is decided once at Init (buildPackedLayout) and covers every
// shape the dual-int path covered — two int columns are at most 8+8 bytes —
// plus wider ones: (Int64, Int32), three or four narrow ints, etc.

// packedKeysToggle gates the WIDENED routing only: shapes with three or four
// int-class columns that previously landed on the compact/generic paths.
// Turning it off restores their old path exactly. Two-column shapes always
// take the packed table — for them this is a pure data-structure swap of the
// dual-int machinery (identical groups, identical group ids, identical
// emission order), which no kill switch can meaningfully bisect.
var packedKeysToggle = optswitch.Register("packed-keys", "WADJET_PACKED_KEYS",
	"packed 128-bit composite group keys for 3+ fixed-width int GROUP BY columns")

// packedKey is one group's composite key: the group-column values packed
// little-endian into two words per the aggregate's packedField layout.
// Pointer-free, so the per-group array can live off-heap.
type packedKey struct {
	lo, hi uint64
}

// packedHashEntry co-locates the full 128-bit key with its group id so a
// probe hit resolves from one cache line.
//
// Size: 24 bytes (8+8+4, padded to the 8-byte alignment the uint64 fields
// force). A 20-byte entry is not expressible in Go without unaligned unsafe
// access, so the real choice is 24 (this) vs 32 — where two entries fill a
// cache line exactly, no entry straddles two lines, and the slot index
// becomes a shift. BenchmarkPackedHashLayout measures both on this machine
// (5900X): lookup 224.8 vs 237.0 ms at 8M keys and 0.47 vs 0.49 ms at 100K,
// insert 4.65 vs 5.10 ms at 100K — 24 B wins every arm, because the
// straddle it risks is cheaper than the 33% extra memory traffic it avoids.
// That extra is also 2.1 GB of RSS at Q33's 256M-slot table, which decides
// whether the query fits under GOMEMLIMIT. So: 24.
type packedHashEntry struct {
	lo, hi uint64
	val    int32
	_      int32 // explicit tail pad: the layout is load-bearing, not incidental
}

// packedHashEmpty marks a free slot.
//
// This is the correctness crux of the table. A magic KEY value cannot work:
// every one of the 2^128 key bit patterns is a legal composite key (two
// int64 columns span the full space), so any sentinel key would alias a real
// group. The VALUE side has slack instead — group ids are indices into the
// per-group SoA arrays and are never negative on any path that reaches this
// table (they come from numIntGroups++ or from freeGroupIDs, both >= 0) — so
// -1 in the value field is the presence bit, and fillEmptyPackedEntries
// stamps it across the table at construction and after every grow.
const packedHashEmpty int32 = -1

// packedHashTable is an open-addressing hash table mapping 128-bit packed
// keys to int32 group ids. Same conventions as intHashTable: linear probing,
// 70% max load, inlineable NoGrow insert paired with CheckGrow, optional
// off-heap entry backing that releases the old reservation on grow.
//
// No Delete: the partial-drain spill path that needs back-shift deletion is
// gated to the single-int key mode (spillPartialPartitions); packed-key
// aggregates whole-drain like the dual-int path they replace.
type packedHashTable struct {
	entries []packedHashEntry
	mask    uint64
	size    int

	reg            *memory.OffheapRegistry
	entriesOffheap bool
}

// newPackedHashTable creates a heap-backed table pre-sized for n entries.
func newPackedHashTable(n int) *packedHashTable {
	return newPackedHashTableReg(n, nil)
}

// newPackedHashTableReg is newPackedHashTable with off-heap entry backing
// when reg is non-nil (aggregate group indexes own an OffheapRegistry).
func newPackedHashTableReg(n int, reg *memory.OffheapRegistry) *packedHashTable {
	capacity := 16
	target := n + n/3 // ~143% of n → 70% load at n entries
	for capacity < target {
		capacity <<= 1
	}
	h := &packedHashTable{reg: reg}
	entries := h.allocEntries(capacity)
	fillEmptyPackedEntries(entries)
	h.entries = entries
	h.mask = uint64(capacity - 1)
	return h
}

// allocEntries returns a len=n entry array, off-heap when the table has a
// registry and the reservation fits, heap otherwise.
func (h *packedHashTable) allocEntries(n int) []packedHashEntry {
	if h.reg != nil {
		if s, ok := memory.OffheapSized[packedHashEntry](h.reg, n); ok {
			h.entriesOffheap = true
			return s
		}
	}
	h.entriesOffheap = false
	return make([]packedHashEntry, n)
}

// fillEmptyPackedEntries stamps the empty marker across every slot using
// copy-doubling (hardware memmove instead of per-element stores).
func fillEmptyPackedEntries(entries []packedHashEntry) {
	if len(entries) == 0 {
		return
	}
	entries[0].val = packedHashEmpty
	for i := 1; i < len(entries); i *= 2 {
		copy(entries[i:], entries[:i])
	}
}

// packedHash mixes a 128-bit key into a slot hash.
//
// The table masks the LOW bits, and the packing layout puts whole columns in
// the HIGH half of a word — so intHashTable's multiply-only Fibonacci hash
// would be wrong here: the low k bits of key*phi depend only on the low k
// bits of key, i.e. every row sharing the low-half column would land in the
// same slot. bits.Mul64 folds a full 128-bit product's halves together (one
// MULQ on amd64) so every input bit reaches every output bit. The trailing
// `^ (lo + hi)` breaks the two degenerate families of the bare fold: a word
// equal to its seed zeroes the product for EVERY value of the other word.
func packedHash(lo, hi uint64) uint64 {
	hp, lp := bits.Mul64(lo^0x9E3779B97F4A7C15, hi^0x517CC1B727220A95)
	return hp ^ lp ^ (lo + hi)
}

// Get looks up a packed key, returning its group id.
func (h *packedHashTable) Get(lo, hi uint64) (int32, bool) {
	idx := packedHash(lo, hi) & h.mask
	for {
		e := &h.entries[idx]
		if e.val == packedHashEmpty {
			return 0, false
		}
		if e.lo == lo && e.hi == hi {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsertNoGrow returns the group id for a key, inserting val when the
// key is new. Defers the load-factor check to CheckGrow: the caller must call
// it after a batch of inserts. Callers detect an insert by comparing the
// result against the id they offered (they always offer a fresh, never-issued
// one) — hence no "found" flag.
//
// This does NOT inline (cost 88 against the inliner's budget of 80; the
// 128-bit compare is what intHashTable's 64-bit one has room for). Getting
// under the budget needs the size counter hoisted into the caller, which
// makes the load factor the caller's business — and an A/B of exactly that
// variant measured within run-to-run noise on both bench shapes (two-int64
// 2.57 vs 2.56 ms, four-int32 2.71 vs 2.75 ms), because this loop is bound
// by the entry's cache miss, not by the call. The safe API wins.
//
// The empty test must precede the key compare: a free slot's key words are
// zero, and zero is a perfectly legal composite key (GROUP BY two columns
// that are both 0).
func (h *packedHashTable) GetOrInsertNoGrow(lo, hi uint64, val int32) int32 {
	idx := packedHash(lo, hi) & h.mask
	for {
		e := &h.entries[idx]
		if e.val == packedHashEmpty {
			e.lo = lo
			e.hi = hi
			e.val = val
			h.size++
			return val
		}
		if e.lo == lo && e.hi == hi {
			return e.val
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsertNoGrowAt is GetOrInsertNoGrow with the slot hash supplied by the
// caller: the partition router already computed packedHash(lo, hi) to pick
// this table's owner (hash once — partitioned_agg.go). hash MUST be
// packedHash(lo, hi), which is what grow() and every resident entry assume.
func (h *packedHashTable) GetOrInsertNoGrowAt(lo, hi, hash uint64, val int32) int32 {
	idx := hash & h.mask
	for {
		e := &h.entries[idx]
		if e.val == packedHashEmpty {
			e.lo = lo
			e.hi = hi
			e.val = val
			h.size++
			return val
		}
		if e.lo == lo && e.hi == hi {
			return e.val
		}
		idx = (idx + 1) & h.mask
	}
}

// GetOrInsert is GetOrInsertNoGrow with the load-factor check folded in, for
// the cold callers (merge) that insert one key at a time. It reports whether
// the key already existed, which requires val to be an id that has never
// been issued — the same precondition GetOrInsertNoGrow's callers already
// meet (they offer the next unused group slot).
func (h *packedHashTable) GetOrInsert(lo, hi uint64, val int32) (int32, bool) {
	got := h.GetOrInsertNoGrow(lo, hi, val)
	if got == val {
		h.CheckGrow()
		return got, false
	}
	return got, true
}

// CheckGrow grows the table if the load factor exceeds 70%. Must follow a
// batch of GetOrInsertNoGrow calls.
//
//go:noinline
func (h *packedHashTable) CheckGrow() {
	if h.size*10 > len(h.entries)*7 {
		h.grow()
	}
}

// EnsureCapacity grows so `additional` more inserts fit under the load
// factor. Call once per batch before a NoGrow loop.
//
//go:noinline
func (h *packedHashTable) EnsureCapacity(additional int) {
	for (h.size+additional)*10 > len(h.entries)*7 {
		h.grow()
	}
}

// Len returns the number of occupied slots.
func (h *packedHashTable) Len() int { return h.size }

// Slots returns the table's slot count — see intHashTable.Slots.
func (h *packedHashTable) Slots() int { return len(h.entries) }

// MemoryUsage returns the bytes consumed by the entry array.
func (h *packedHashTable) MemoryUsage() int64 {
	return int64(cap(h.entries)) * int64(unsafe.Sizeof(packedHashEntry{}))
}

// ForEach iterates the occupied slots.
func (h *packedHashTable) ForEach(fn func(lo, hi uint64, val int32)) {
	for i := range h.entries {
		if h.entries[i].val != packedHashEmpty {
			fn(h.entries[i].lo, h.entries[i].hi, h.entries[i].val)
		}
	}
}

// freeEntries drops the entry array, returning its off-heap reservation
// immediately when it had one. Used by the two-level conversion, which has
// already copied every live entry out (two_level_hash.go); the table is
// unusable afterwards.
func (h *packedHashTable) freeEntries() {
	if h.entriesOffheap && len(h.entries) > 0 {
		h.reg.Release(unsafe.Pointer(unsafe.SliceData(h.entries)))
	}
	h.entries = nil
	h.entriesOffheap = false
	h.mask = 0
	h.size = 0
}

// grow doubles the capacity and rehashes. Off-heap tables release the old
// reservation immediately — a 100M-group rehash holding old+new live is
// exactly the transient ADR-0006's amendment removed.
func (h *packedHashTable) grow() {
	newCap := len(h.entries) * 2
	oldOffheap := h.entriesOffheap
	old := h.entries
	newEntries := h.allocEntries(newCap)
	fillEmptyPackedEntries(newEntries)
	newMask := uint64(newCap - 1)

	for i := range old {
		e := &old[i]
		if e.val == packedHashEmpty {
			continue
		}
		idx := packedHash(e.lo, e.hi) & newMask
		for newEntries[idx].val != packedHashEmpty {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = *e
	}

	if oldOffheap {
		h.reg.Release(unsafe.Pointer(unsafe.SliceData(old)))
	}
	h.entries = newEntries
	h.mask = newMask
}

// --- key layout ---------------------------------------------------------

// packedField places one group-key column inside the composite key.
type packedField struct {
	word  uint8 // 0 = lo, 1 = hi
	shift uint8 // bit offset within the word (0 or 32)
	i32   bool  // column is stored in Int32Data and occupies 32 bits
}

// get extracts this column's value from a packed key, widened to int64 the
// same way the consume path read it (int32-class columns sign-extend, so
// negative Int32/Date values round-trip exactly).
func (f packedField) get(k packedKey) int64 {
	w := k.lo
	if f.word != 0 {
		w = k.hi
	}
	if f.i32 {
		return int64(int32(w >> f.shift))
	}
	return int64(w)
}

// packedKeyWidth returns the byte width a group column occupies in the
// composite key, or 0 when the type cannot be packed. The width follows the
// column's STORAGE, not its logical size: anything read from Int64Data takes
// the full 8 bytes, so IPv4 (unsigned 32-bit values living in an int64) and
// MAC (48-bit) round-trip bit-exactly instead of relying on a sign-extension
// that would corrupt them.
//
// The accepted set is exactly isAggIntType's (asserted by
// TestPackedKeyWidthMatchesAggIntTypes).
func packedKeyWidth(t batch.TypeID) int {
	switch t {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return 4
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC,
		batch.TypeDuration:
		return 8
	}
	return 0
}

// packedKeyMaxBytes is the composite key's capacity: two 64-bit words.
const packedKeyMaxBytes = 16

// buildPackedLayout assigns each group column a slot in the 128-bit key, or
// returns nil when the shape is ineligible (a non-int-class column, fewer
// than two columns, or widths summing past 16 bytes).
//
// Placement runs widest-first: 8-byte columns claim whole words, then
// 4-byte columns fill the remaining half-words. A single in-order pass would
// decline (Int32, Int64, Int32) — 16 bytes exactly, but the Int64 cannot
// straddle the word boundary — whereas widest-first succeeds whenever the
// widths sum to <= 16. Column order inside the key is arbitrary: the layout
// is fixed for the aggregate's lifetime and pack/unpack share it.
func buildPackedLayout(types []batch.TypeID) []packedField {
	if len(types) < 2 {
		return nil
	}
	total := 0
	for _, t := range types {
		w := packedKeyWidth(t)
		if w == 0 {
			return nil
		}
		total += w
	}
	if total > packedKeyMaxBytes {
		return nil
	}

	fields := make([]packedField, len(types))
	nextWide := uint8(0)
	for i, t := range types {
		if packedKeyWidth(t) == 8 {
			fields[i] = packedField{word: nextWide}
			nextWide++
		}
	}
	// Half-word slots not claimed by a wide column, in (word, shift) order.
	nextNarrow := 0
	narrowSlots := make([][2]uint8, 0, 4)
	for w := nextWide; w < 2; w++ {
		narrowSlots = append(narrowSlots, [2]uint8{w, 0}, [2]uint8{w, 32})
	}
	for i, t := range types {
		if packedKeyWidth(t) != 4 {
			continue
		}
		slot := narrowSlots[nextNarrow]
		nextNarrow++
		fields[i] = packedField{word: slot[0], shift: slot[1], i32: true}
	}
	return fields
}

// samePackedLayout reports whether two key layouts place every column in the
// same word and bit offset — the precondition for a hash computed against one
// of them to index a table built with the other (hash once, partitioned_agg.go).
func samePackedLayout(a, b []packedField) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// packedKeyCol is one group column's per-batch resolved accessor: the typed
// slice plus its fixed position in the composite key. Rebuilt per batch
// (scratch on the sink), read per row.
type packedKeyCol struct {
	i32   []int32
	i64   []int64
	nulls *batch.Bitmap
	word  uint8
	shift uint8
}

// packedKeyAt builds the composite key for one row from the resolved
// columns. Kept as a plain loop over a small slice (2-4 entries, identical
// branch outcomes every row) rather than a per-shape specialization.
func packedKeyAt(cols []packedKeyCol, row int) (lo, hi uint64) {
	for i := range cols {
		c := &cols[i]
		var v uint64
		if c.i32 != nil {
			v = uint64(uint32(c.i32[row])) << c.shift
		} else {
			v = uint64(c.i64[row])
		}
		if c.word == 0 {
			lo |= v
		} else {
			hi |= v
		}
	}
	return lo, hi
}
