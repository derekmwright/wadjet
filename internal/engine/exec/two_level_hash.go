package exec

import (
	"os"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// The int/packed group indexes split one hash into disjoint partition, bucket
// and slot windows: owner uses the top ≤12 bits, bucket the low 8, slot the
// bits above those. growSub caps sub-table bits; low buckets preserve flat
// collision behavior, including fibHash's dense-key bijection and fold (#306).
// Construction-time epoch/row bounds can pin flat; otherwise conversion runs
// at batch end where flat would rehash, into the flat slot count split 256 ways.
// Large NDV hints may build bucketed directly; below threshold stay flat.
// Group ids remain dense global indices into unchanged accumulators/key SoAs;
// emission, spill cursors, partial-state format and merge see the same values.
// String mode stays flat: splitting its index must preserve arenaString aliases.
// See docs/internals/two-level-group-index.md for the design.
var twoLevelToggle = optswitch.Register("two-level-ht", "WADJET_TWO_LEVEL_HT",
	"256-bucket two-level group index past twoLevelConvertAt keys (per-bucket rehash instead of whole-table)")

// bornFlatToggle is the kill switch for the construction-time layout
// decision (twoLevelBoundedMinGroups / HashAggregate.indexLayoutStaysFlat).
// Off = the pre-2026-08-22 behavior: every sink starts flat and converts at
// runtime whenever convertsToTwoLevel says so, epoch cap or not.
var bornFlatToggle = optswitch.Register("two-level-born-flat", "WADJET_TWO_LEVEL_BORN_FLAT",
	"epoch-capped (bounded) aggregates build a flat group index and never convert")

// rowBoundToggle is the kill switch for the second construction-time bound:
// an aggregate whose owner knows EXACTLY how many rows it will read
// (twoLevelMinAmortizeRows / HashAggregate.SetInputRowBound). Off = such an
// aggregate takes the unchanged adaptive path, as it did before 2026-08-22.
var rowBoundToggle = optswitch.Register("two-level-row-bound", "WADJET_TWO_LEVEL_ROW_BOUND",
	"aggregates whose known input-row bound cannot amortize a flat→bucketed conversion are born flat")

// twoLevelBoundedMinGroups is the minimum reachable epoch group count G*
// required before a byte-capped sink may use a bucketed index.
// A sink rebuilt every C bytes reaches at most C/perGroupStateBytes groups;
// its index cannot outlive the epoch, so conversion needs its own bound.
// G* is 4 M, independent of twoLevelConvertAt's unbounded live-count threshold.
// The production 128 MB cap keeps bounded sinks flat; larger caps may qualify.
// See docs/internals/two-level-epoch-cap-bound.md for the design.
const twoLevelBoundedMinGroups = 4 << 20

// twoLevelAmortizeMultiple sets the minimum total input R* to 8 times
// twoLevelConvertAt, including under WADJET_TWO_LEVEL_AT overrides.
// Only exact owner-supplied row bounds qualify (StageOutput.PartitionRows);
// InputRowHint/GroupNDVHint estimates must not pin a large aggregate flat.
// This monotone gate can only remove conversions. It bounds rows remaining
// after the earliest conversion, independently of live-count/load-factor tests.
// It deliberately does not cover the measured loss at 16.78 M rows / 4.19 M
// groups: that shape passes the row gate despite losing to flat.
// See docs/internals/two-level-input-amortization-bound.md for the design.
const twoLevelAmortizeMultiple = 8

// twoLevelMinAmortizeRows is R* in rows. A var-derived function rather than a
// constant because twoLevelConvertAt is itself overridable (WADJET_TWO_LEVEL_AT).
func twoLevelMinAmortizeRows() int64 {
	return int64(twoLevelConvertAt) * twoLevelAmortizeMultiple
}

// TwoLevelConversions counts flat→bucketed conversions, TwoLevelDirectBuilds
// counts indexes built bucketed from an NDV hint, and TwoLevelBornFlat counts
// sinks whose layout was decided FLAT at construction because their epoch cap
// bounds them below twoLevelBoundedMinGroups (observability + test
// assertions; exported into the worker's task logs).
var (
	TwoLevelConversions  atomic.Int64
	TwoLevelDirectBuilds atomic.Int64
	TwoLevelBornFlat     atomic.Int64
)

const (
	// twoLevelBuckets is the sub-table count. 256 matches ClickHouse's
	// TwoLevelHashTable; the bucket header array is 12 KB per index, which
	// stays cache-resident while the sub-tables are the multi-GB part.
	twoLevelBuckets = 256
	// twoLevelBucketMask extracts the bucket from the LOW bits of the hash.
	twoLevelBucketMask = twoLevelBuckets - 1
	// twoLevelSlotShift is how far the slot window sits above the bucket
	// window. log2(twoLevelBuckets).
	twoLevelSlotShift = 8
	// twoLevelMaxSubBits caps one sub-table at 2^36 slots so the slot window
	// (bits 8..43) can never reach the partition window (bit 52 and up).
	// 2^36 entries is 1 TiB of int entries in a SINGLE bucket — unreachable,
	// but the invariant is what the bit budget above rests on.
	twoLevelMaxSubBits = 36
	// twoLevelMinSubCap is the floor for a sub-table's slot count. Buckets a
	// skewed key family leaves empty cost 16 entries (256 B) each.
	twoLevelMinSubCap = 16
)

// twoLevelConvertAt is the live-entry conversion threshold, default 1 M;
// production never writes it. Normal conversion also requires load-factor
// lookahead so it replaces a doubling (TestTwoLevelConvertsAtTheDoubling).
// WADJET_TWO_LEVEL_AT is a positive test/oracle override, not an operator knob:
// it also enables eager size-only conversion to reach small-corpus coverage.
// It scales R* too; DAG coverage must set WADJET_TWO_LEVEL_ROW_BOUND=0 to
// bypass exact row bounds that can still pin flat. Eager conversion changes
// when the index converts, never its values. Flat backing: ADR-0006 amendment.
// See docs/internals/two-level-conversion-threshold.md for the design.
var twoLevelConvertAt, twoLevelConvertEager = twoLevelConvertPolicy()

// twoLevelConvertPolicy reads the conversion threshold and reports whether
// it came from an accepted WADJET_TWO_LEVEL_AT override.
func twoLevelConvertPolicy() (int, bool) {
	if v, err := strconv.Atoi(os.Getenv("WADJET_TWO_LEVEL_AT")); err == nil && v > 0 {
		return v, true
	}
	return 1_000_000, false
}

// offheapSubMinBytes is the 2 MiB huge-page threshold; tests may override it.
// Apply it to total table backing: carve 256 independently addressed buckets
// from one reservation with MADV_HUGEPAGE, not many sub-page mappings.
// A bucket growing out allocates independently using the same per-bucket gate
// as fallback; release the arena when its last bucket leaves.
// The shared arena preserves off-heap backing across conversion (ADR-0006).
// See docs/internals/two-level-shared-offheap-arena.md for the design.
var offheapSubMinBytes = 2 << 20

// bucketOf selects a sub-table from a key hash. See the bit budget above:
// the LOW 8 bits, deliberately.
func bucketOf(hash uint64) uint64 { return hash & twoLevelBucketMask }

// --- int key mode --------------------------------------------------------

// intSubTable is one bucket of an intTwoLevelTable: a complete
// open-addressing table with its own capacity, mask, and load factor. 48 B,
// so the 256 headers are 12 KB — the extra dependent load a bucketed probe
// pays lands in L1/L2, not in DRAM.
type intSubTable struct {
	entries []intHashEntry
	mask    uint64
	size    int
	offheap bool
	// arena marks entries as a slice of the table's shared reservation
	// rather than an allocation of its own: it is neither heap-freed nor
	// individually Released, and growing out of it decrements arenaLive.
	arena bool
}

// intTwoLevelTable is the bucketed form of intHashTable: same keys, same
// values, same fibHash, same 70% load factor and linear probing — the index
// is split across 256 independently-grown sub-tables.
type intTwoLevelTable struct {
	subs [twoLevelBuckets]intSubTable
	reg  *memory.OffheapRegistry
	// arena is the one off-heap reservation the 256 buckets were carved
	// from (offheapSubMinBytes) and arenaLive counts how many still slice
	// it. A bucket that grows out hands its pages back to the kernel;
	// arenaFreed is how many bytes of the mapping that returned, so
	// MemoryUsage charges the arena by what is still resident. The mapping
	// itself is released when the last bucket leaves.
	arena      []intHashEntry
	arenaLive  int
	arenaFreed int64
}

// newIntTwoLevelTable builds an empty bucketed index pre-sized for n TOTAL
// entries (each bucket gets n/256 at 70% load).
func newIntTwoLevelTable(n int, reg *memory.OffheapRegistry) *intTwoLevelTable {
	return newIntTwoLevelTableSub(subCapFor(n), reg)
}

// newIntTwoLevelTableSub builds an empty bucketed index with an explicit
// per-bucket slot count. The conversion path uses it to reproduce EXACTLY the
// capacity the flat table's own doubling policy would have reached (see
// convertIntHashTableToTwoLevel), which is what makes the conversion a
// replacement for that doubling rather than an addition to it.
func newIntTwoLevelTableSub(capPerSub int, reg *memory.OffheapRegistry) *intTwoLevelTable {
	t := &intTwoLevelTable{reg: reg}
	mask := uint64(capPerSub - 1)
	// One reservation for all 256 buckets when the TABLE is worth a huge
	// page — see offheapSubMinBytes. fillEmptyEntries runs once over the
	// whole arena rather than 256 times.
	if arena, ok := allocOffheapArena[intHashEntry](reg, twoLevelBuckets*capPerSub); ok {
		fillEmptyEntries(arena)
		t.arena, t.arenaLive = arena, twoLevelBuckets
		for i := range t.subs {
			s := &t.subs[i]
			lo, hi := i*capPerSub, (i+1)*capPerSub
			s.entries = arena[lo:hi:hi]
			s.mask, s.offheap, s.arena = mask, true, true
		}
		return t
	}
	for i := range t.subs {
		s := &t.subs[i]
		s.entries, s.offheap = allocIntSubEntries(reg, capPerSub)
		fillEmptyEntries(s.entries)
		s.mask = mask
	}
	return t
}

// allocOffheapArena returns one reservation big enough for all 256 buckets,
// or ok=false when off-heap is unavailable or the table is too small to be
// worth a mapping. Shared by both key modes.
func allocOffheapArena[T any](reg *memory.OffheapRegistry, n int) ([]T, bool) {
	var zero T
	if reg == nil || n*int(unsafe.Sizeof(zero)) < offheapSubMinBytes {
		return nil, false
	}
	return memory.OffheapExact[T](reg, n)
}

// releaseArenaSlot records that one bucket has left the shared reservation
// and unmaps it once none are left. Called from growSub, the only path that
// replaces a bucket's entry array; old is the departing bucket's slice.
//
// The departing bucket's PAGES go back to the kernel immediately, and the
// bytes that actually went back are subtracted from the arena's charge.
// Without that the mapping keeps every vacated bucket resident until the
// last one leaves, so a table mid-growth really does hold the old arena AND
// the new arrays — up to twice its live slot count — and MemoryUsage charges
// it, correctly, against the owner's budget. That is what made a conversion
// (which grows buckets inside its own loop) cost a capped aggregate's epoch
// budget twice over: returning the pages is the fix, and the accounting only
// follows it.
//
// madvise works in whole pages, so a bucket that does not own one returns
// nothing and stays charged. The arena gate (offheapSubMinBytes: 2 MiB
// across 256 buckets) puts the production floor at 8 KiB per bucket, so
// every bucket that can reach this path owns whole pages.
func (t *intTwoLevelTable) releaseArenaSlot(old []intHashEntry) {
	t.arenaFreed += memory.DiscardSlice(old)
	t.arenaLive--
	if t.arenaLive > 0 || t.arena == nil {
		return
	}
	t.reg.Release(unsafe.Pointer(unsafe.SliceData(t.arena)))
	t.arena, t.arenaFreed = nil, 0
}

// subCapFor returns the power-of-two slot count one bucket needs to hold
// n/256 entries at the 70% load factor.
func subCapFor(n int) int {
	per := n / twoLevelBuckets
	target := per + per/3 // ~143% of per → 70% load
	c := twoLevelMinSubCap
	for c < target {
		c <<= 1
	}
	return c
}

// subCapForFlatSlots splits a FLAT slot count across the 256 buckets. Both
// are powers of two, so the split is exact and the bucketed table indexes
// with the same low 8+log2(subcap) hash bits the flat table of that size
// would have used (see the bit budget above).
func subCapForFlatSlots(slots int) int {
	per := slots / twoLevelBuckets
	if per < twoLevelMinSubCap {
		return twoLevelMinSubCap
	}
	return per
}

// allocIntSubEntries returns a zeroed entry array for one bucket, off-heap
// once the bucket is large enough to be worth a mapping.
func allocIntSubEntries(reg *memory.OffheapRegistry, n int) ([]intHashEntry, bool) {
	if reg != nil && n*int(unsafe.Sizeof(intHashEntry{})) >= offheapSubMinBytes {
		if s, ok := memory.OffheapExact[intHashEntry](reg, n); ok {
			return s, true
		}
	}
	return make([]intHashEntry, n), false
}

// convertIntHashTableToTwoLevel rebuilds the index and releases flat's entries;
// the emptied flat table must not be used afterwards.
// Split the flat slot count 256 ways, not doubled capacity, preserving the
// (bucket, slot) hash index. At the load-factor boundary, growth then occurs
// as cache-resident per-bucket rehashes instead of one whole-table scatter.
// Source keys are unique, so the insertion loop needs no duplicate test.
// See docs/internals/two-level-int-index-conversion.md for the design.
func convertIntHashTableToTwoLevel(flat *intHashTable, reg *memory.OffheapRegistry) *intTwoLevelTable {
	t := newIntTwoLevelTableSub(subCapForFlatSlots(len(flat.entries)), reg)
	for i := range flat.entries {
		e := &flat.entries[i]
		if e.key == intHashEmpty {
			continue
		}
		hash := fibHash(e.key)
		b := bucketOf(hash)
		s := &t.subs[b]
		idx := (hash >> twoLevelSlotShift) & s.mask
		for s.entries[idx].key != intHashEmpty {
			idx = (idx + 1) & s.mask
		}
		s.entries[idx] = *e
		s.size++
		if s.size*10 > len(s.entries)*7 {
			t.growSub(b)
		}
	}
	flat.freeEntries()
	return t
}

// GetOrInsertAt is intHashTable.GetOrInsertNoGrowAt with the bucket step in
// front: pick the sub-table from the low hash bits, probe with the bits
// above them. hash MUST be fibHash(key).
//
// Unlike the flat table there is NO caller-paired CheckGrow. The flat table
// splits the load-factor test out because folding it in would push the probe
// past the inliner's budget; here the probe is a call either way, and the
// sub-table header the test needs is already in a register at the point of
// insert. Making the caller re-derive the bucket and re-load that header on
// every new key measured ~10% of the whole consume path in the near-unique
// regime, where EVERY row inserts.
func (t *intTwoLevelTable) GetOrInsertAt(key int64, hash uint64, val int32) (int32, bool) {
	b := bucketOf(hash)
	s := &t.subs[b]
	idx := (hash >> twoLevelSlotShift) & s.mask
	for {
		e := &s.entries[idx]
		if e.key == intHashEmpty {
			e.key = key
			e.val = val
			s.size++
			// Grow ONLY this bucket. That is the whole point of the
			// structure: the rehash touches 1/256 of the entries and stays
			// cache-resident.
			if s.size*10 > len(s.entries)*7 {
				t.growSub(b)
			}
			return val, false
		}
		if e.key == key {
			return e.val, true
		}
		idx = (idx + 1) & s.mask
	}
}

// Get looks up a key, hashing it itself.
func (t *intTwoLevelTable) Get(key int64) (int32, bool) {
	hash := fibHash(key)
	s := &t.subs[bucketOf(hash)]
	idx := (hash >> twoLevelSlotShift) & s.mask
	for {
		e := &s.entries[idx]
		if e.key == intHashEmpty {
			return 0, false
		}
		if e.key == key {
			return e.val, true
		}
		idx = (idx + 1) & s.mask
	}
}

// GetOrInsert is the cold-path insert (merge): hashes the key itself.
func (t *intTwoLevelTable) GetOrInsert(key int64, val int32) (int32, bool) {
	return t.GetOrInsertAt(key, fibHash(key), val)
}

// Len returns the total live entry count across buckets. Summed on demand
// rather than mirrored in a counter so the hot insert path stores once.
func (t *intTwoLevelTable) Len() int {
	n := 0
	for i := range t.subs {
		n += t.subs[i].size
	}
	return n
}

// MemoryUsage returns the bytes held by every bucket's entry array plus the
// bucket header array itself. The shared arena is charged as the mapping
// minus the pages releaseArenaSlot has already handed back, so a
// partially-vacated arena is charged for exactly what is still resident —
// never the whole mapping ON TOP OF the departed buckets' new arrays.
func (t *intTwoLevelTable) MemoryUsage() int64 {
	const entryBytes = int64(unsafe.Sizeof(intHashEntry{}))
	n := int64(len(t.arena))*entryBytes - t.arenaFreed
	for i := range t.subs {
		if !t.subs[i].arena {
			n += int64(cap(t.subs[i].entries)) * entryBytes
		}
	}
	return n + int64(unsafe.Sizeof(t.subs))
}

// ForEach iterates every live entry, bucket by bucket. Order differs from
// the flat table's; no caller depends on it (the drain cursor sorts, the
// merge probes by key).
func (t *intTwoLevelTable) ForEach(fn func(key int64, val int32)) {
	for i := range t.subs {
		es := t.subs[i].entries
		for j := range es {
			if es[j].key != intHashEmpty {
				fn(es[j].key, es[j].val)
			}
		}
	}
}

// Delete removes a key with back-shift deletion, exactly as intHashTable
// does — the probe chain never leaves its bucket, so the back-shift walk is
// local to one sub-table.
func (t *intTwoLevelTable) Delete(key int64) (int32, bool) {
	hash := fibHash(key)
	s := &t.subs[bucketOf(hash)]
	n := len(s.entries)
	if n == 0 {
		return 0, false
	}
	idx := (hash >> twoLevelSlotShift) & s.mask
	found := false
	for k := 0; k < n; k++ {
		e := &s.entries[idx]
		if e.key == intHashEmpty {
			return 0, false
		}
		if e.key == key {
			found = true
			break
		}
		idx = (idx + 1) & s.mask
	}
	if !found {
		return 0, false
	}
	deleted := s.entries[idx].val
	// Knuth Algorithm R, the same walk intHashTable.Delete runs and with the
	// same correction: an entry that must STAY does not end the walk, because
	// one further along can still have a home at or before the hole. See the
	// comment there (#306).
	i := idx
	j := idx
	for k := 0; k < n; k++ {
		j = (j + 1) & s.mask
		e := &s.entries[j]
		if e.key == intHashEmpty {
			break
		}
		id := (fibHash(e.key) >> twoLevelSlotShift) & s.mask
		if ((j - id) & s.mask) < ((j - i) & s.mask) {
			continue
		}
		s.entries[i] = *e
		i = j
	}
	s.entries[i].key = intHashEmpty
	s.size--
	return deleted, true
}

// growSub doubles ONE bucket and rehashes only its entries.
func (t *intTwoLevelTable) growSub(b uint64) {
	s := &t.subs[b]
	newCap := len(s.entries) * 2
	if newCap>>twoLevelMaxSubBits > 0 {
		return // bit-budget invariant: never let the slot window reach bit 52
	}
	old := s.entries
	oldOffheap, oldArena := s.offheap, s.arena
	newEntries, offheap := allocIntSubEntries(t.reg, newCap)
	fillEmptyEntries(newEntries)
	newMask := uint64(newCap - 1)
	for i := range old {
		e := &old[i]
		if e.key == intHashEmpty {
			continue
		}
		idx := (fibHash(e.key) >> twoLevelSlotShift) & newMask
		for newEntries[idx].key != intHashEmpty {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = *e
	}
	s.entries = newEntries
	s.offheap = offheap
	s.arena = false
	s.mask = newMask
	switch {
	case oldArena:
		// Not this bucket's mapping to unmap — its pages go back now, the
		// mapping itself when the last bucket has left it.
		t.releaseArenaSlot(old)
	case oldOffheap:
		t.reg.Release(unsafe.Pointer(unsafe.SliceData(old)))
	}
}

// --- packed composite key mode -------------------------------------------

// packedSubTable is one bucket of a packedTwoLevelTable (see intSubTable).
type packedSubTable struct {
	entries []packedHashEntry
	mask    uint64
	size    int
	offheap bool
	arena   bool     // see intSubTable.arena
	_       [22]byte // pad to 64 B
}

// packedTwoLevelTable is the bucketed form of packedHashTable. Same
// conventions: 128-bit key inline in the entry, val == packedHashEmpty
// marks a free slot, 70% load, caller-paired CheckGrowAt.
type packedTwoLevelTable struct {
	subs       [twoLevelBuckets]packedSubTable
	reg        *memory.OffheapRegistry
	arena      []packedHashEntry // see intTwoLevelTable.arena
	arenaLive  int
	arenaFreed int64
}

func newPackedTwoLevelTable(n int, reg *memory.OffheapRegistry) *packedTwoLevelTable {
	return newPackedTwoLevelTableSub(subCapFor(n), reg)
}

// newPackedTwoLevelTableSub is newIntTwoLevelTableSub for the packed mode.
func newPackedTwoLevelTableSub(capPerSub int, reg *memory.OffheapRegistry) *packedTwoLevelTable {
	t := &packedTwoLevelTable{reg: reg}
	mask := uint64(capPerSub - 1)
	if arena, ok := allocOffheapArena[packedHashEntry](reg, twoLevelBuckets*capPerSub); ok {
		fillEmptyPackedEntries(arena)
		t.arena, t.arenaLive = arena, twoLevelBuckets
		for i := range t.subs {
			s := &t.subs[i]
			lo, hi := i*capPerSub, (i+1)*capPerSub
			s.entries = arena[lo:hi:hi]
			s.mask, s.offheap, s.arena = mask, true, true
		}
		return t
	}
	for i := range t.subs {
		s := &t.subs[i]
		s.entries, s.offheap = allocPackedSubEntries(reg, capPerSub)
		fillEmptyPackedEntries(s.entries)
		s.mask = mask
	}
	return t
}

// releaseArenaSlot is intTwoLevelTable.releaseArenaSlot for the packed mode.
func (t *packedTwoLevelTable) releaseArenaSlot(old []packedHashEntry) {
	t.arenaFreed += memory.DiscardSlice(old)
	t.arenaLive--
	if t.arenaLive > 0 || t.arena == nil {
		return
	}
	t.reg.Release(unsafe.Pointer(unsafe.SliceData(t.arena)))
	t.arena, t.arenaFreed = nil, 0
}

func allocPackedSubEntries(reg *memory.OffheapRegistry, n int) ([]packedHashEntry, bool) {
	if reg != nil && n*int(unsafe.Sizeof(packedHashEntry{})) >= offheapSubMinBytes {
		if s, ok := memory.OffheapExact[packedHashEntry](reg, n); ok {
			return s, true
		}
	}
	return make([]packedHashEntry, n), false
}

// convertPackedHashTableToTwoLevel rebuilds a flat packed index as a
// bucketed one and releases the flat entry array — see
// convertIntHashTableToTwoLevel for the sizing rule and why it is that one.
func convertPackedHashTableToTwoLevel(flat *packedHashTable, reg *memory.OffheapRegistry) *packedTwoLevelTable {
	t := newPackedTwoLevelTableSub(subCapForFlatSlots(len(flat.entries)), reg)
	for i := range flat.entries {
		e := &flat.entries[i]
		if e.val == packedHashEmpty {
			continue
		}
		hash := packedHash(e.lo, e.hi)
		b := bucketOf(hash)
		s := &t.subs[b]
		idx := (hash >> twoLevelSlotShift) & s.mask
		for s.entries[idx].val != packedHashEmpty {
			idx = (idx + 1) & s.mask
		}
		s.entries[idx] = *e
		s.size++
		if s.size*10 > len(s.entries)*7 {
			t.growSub(b)
		}
	}
	flat.freeEntries()
	return t
}

// GetOrInsertAt returns the group id for a key, inserting val when the key
// is new and growing that bucket alone if the insert crosses its load
// factor. hash MUST be packedHash(lo, hi). See intTwoLevelTable.GetOrInsertAt
// for why the grow check is folded in rather than caller-paired.
func (t *packedTwoLevelTable) GetOrInsertAt(lo, hi, hash uint64, val int32) int32 {
	b := bucketOf(hash)
	s := &t.subs[b]
	idx := (hash >> twoLevelSlotShift) & s.mask
	for {
		e := &s.entries[idx]
		if e.val == packedHashEmpty {
			e.lo = lo
			e.hi = hi
			e.val = val
			s.size++
			if s.size*10 > len(s.entries)*7 {
				t.growSub(b)
			}
			return val
		}
		if e.lo == lo && e.hi == hi {
			return e.val
		}
		idx = (idx + 1) & s.mask
	}
}

// Get looks up a packed key, hashing it itself.
func (t *packedTwoLevelTable) Get(lo, hi uint64) (int32, bool) {
	hash := packedHash(lo, hi)
	s := &t.subs[bucketOf(hash)]
	idx := (hash >> twoLevelSlotShift) & s.mask
	for {
		e := &s.entries[idx]
		if e.val == packedHashEmpty {
			return 0, false
		}
		if e.lo == lo && e.hi == hi {
			return e.val, true
		}
		idx = (idx + 1) & s.mask
	}
}

// GetOrInsert is the cold-path insert (merge). Reports whether the key
// already existed, which requires val to be an id never issued before —
// the same precondition packedHashTable.GetOrInsert carries.
func (t *packedTwoLevelTable) GetOrInsert(lo, hi uint64, val int32) (int32, bool) {
	got := t.GetOrInsertAt(lo, hi, packedHash(lo, hi), val)
	return got, got != val
}

func (t *packedTwoLevelTable) Len() int {
	n := 0
	for i := range t.subs {
		n += t.subs[i].size
	}
	return n
}

// MemoryUsage is intTwoLevelTable.MemoryUsage for the packed mode.
func (t *packedTwoLevelTable) MemoryUsage() int64 {
	const entryBytes = int64(unsafe.Sizeof(packedHashEntry{}))
	n := int64(len(t.arena))*entryBytes - t.arenaFreed
	for i := range t.subs {
		if !t.subs[i].arena {
			n += int64(cap(t.subs[i].entries)) * entryBytes
		}
	}
	return n + int64(unsafe.Sizeof(t.subs))
}

func (t *packedTwoLevelTable) ForEach(fn func(lo, hi uint64, val int32)) {
	for i := range t.subs {
		es := t.subs[i].entries
		for j := range es {
			if es[j].val != packedHashEmpty {
				fn(es[j].lo, es[j].hi, es[j].val)
			}
		}
	}
}

func (t *packedTwoLevelTable) growSub(b uint64) {
	s := &t.subs[b]
	newCap := len(s.entries) * 2
	if newCap>>twoLevelMaxSubBits > 0 {
		return
	}
	old := s.entries
	oldOffheap, oldArena := s.offheap, s.arena
	newEntries, offheap := allocPackedSubEntries(t.reg, newCap)
	fillEmptyPackedEntries(newEntries)
	newMask := uint64(newCap - 1)
	for i := range old {
		e := &old[i]
		if e.val == packedHashEmpty {
			continue
		}
		idx := (packedHash(e.lo, e.hi) >> twoLevelSlotShift) & newMask
		for newEntries[idx].val != packedHashEmpty {
			idx = (idx + 1) & newMask
		}
		newEntries[idx] = *e
	}
	s.entries = newEntries
	s.offheap = offheap
	s.arena = false
	s.mask = newMask
	switch {
	case oldArena:
		t.releaseArenaSlot(old)
	case oldOffheap:
		t.reg.Release(unsafe.Pointer(unsafe.SliceData(old)))
	}
}
