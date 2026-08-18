package exec

import (
	"os"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Two-level (bucketed) group index — G6 in
// docs/benchmarks/high-card-aggregation-gap-2026-08-17.md.
//
// A flat open-addressing table grows by WHOLE-TABLE rehash: allocate 2x,
// memset it to the empty marker, then scatter every live entry into random
// slots of an array far larger than any cache. A 100M-group aggregate pays
// ~8 of those, the last few touching tens of millions of entries with old
// and new tables both live. ClickHouse converts to a 256-bucket two-level
// table past ~100K keys and DuckDB radix-partitions for the same reason:
// afterwards a rehash touches 1/256 of the data, stays cache-resident, and
// the buckets are independent — merge and emit can walk them in parallel,
// and sizing no longer needs a cardinality estimate.
//
// This file holds the int and packed key modes' bucketed indexes. The
// string mode keeps its flat table for now (see "String mode" below).
//
// # Bit budget (the one place it is written down)
//
// Every group key yields ONE 64-bit hash — fibHash for single-int keys,
// packedHash for composite keys (G5's hash-once, partitioned_agg.go). Three
// independent consumers read DISJOINT windows of it:
//
//	 63                        52 51                  8 7            0
//	+----------------------------+----------------------+-------------+
//	| partition owner            | sub-table slot       | bucket      |
//	| top ceil(log2 parts) bits  | log2(subcap) bits    | 8 bits      |
//	+----------------------------+----------------------+-------------+
//	  partitionFor(h, parts)       (h >> 8) & (cap-1)     h & 255
//
// PARTITION OWNER — unchanged from G5: the high half of h*parts (Lemire
// multiply-shift), a function of the top ceil(log2(parts)) bits. parts is
// one per worker, <= 4096 in any plausible deployment, so the window is at
// most 12 bits.
//
// BUCKET — the LOW 8 bits, and the sub-table's slot is what USED to be the
// low bits, shifted up by 8. So (bucket, slot) together are exactly the low
// 8+log2(subcap) bits of the same hash: the identical index a FLAT table of
// 256*subcap slots would compute. Two keys collide in the two-level table
// iff they would have collided in that flat table
// (TestTwoLevelMatchesFlatCollisions). Everything the flat tables' spread
// rests on carries over unchanged, including fibHash's collision-free
// bijection on dense integer ids.
//
// SLOT — bits (7+log2(subcap))..8. Disjointness with the partition window
// holds while 8+log2(subcap) <= 52, i.e. up to 2^44 slots in a SINGLE
// sub-table (16 PiB of entries). Enforced by construction: growSub refuses
// past twoLevelMaxSubBits.
//
// WHY THE BUCKET IS THE LOW WINDOW (measured, not assumed): the obvious
// choice — a middle window like bits 39..32, which is what ClickHouse's
// `hash >> (32 - 8)` amounts to — is correct only for an avalanching hash.
// fibHash is multiply-only (`key * phi`), so picking keys by ANY high-ish
// window selects a near-arithmetic subsequence of a dense key range, and
// fibHash collapses strided keys onto few slots (the pre-existing property
// recorded in TestFibHashStrideCollapse). Simulated on 33M dense int keys:
// bucket=bits 39..32 gave 6.46 average probes per insert at 8M keys and
// degrades with scale, while bucket=low 8 bits gives exactly 1.00 at every
// size — the flat table's own number. Random and packed-composite families
// measure 1.50 either way. The low window is the only one that inherits the
// flat table's guarantees instead of replacing them with new ones.
//
// # Adaptive conversion, not construction
//
// A sink starts FLAT, exactly as before, and converts when it is both big
// enough and still filling — see convertsToTwoLevel (aggregate.go) for the
// two tests and twoLevelConvertAt for the measured curve behind the size
// one. The decision runs once per BATCH, never per row: the consume loop
// hoists its table pointer for the whole batch and the conversion lands at
// the batch's end. Aggregates whose NDV hint already exceeds the threshold
// construct bucketed directly (resolveIndices) and never pay a conversion at
// all. Below the threshold nothing changes: no bucket indirection, no extra
// shift, byte-identical behavior to G5.
//
// What the conversion does NOT touch: group ids stay dense global indices
// into the same flat accumulator arrays and the same key SoAs, so emission,
// the spill drain cursor, the partial-state run format and the merge all see
// exactly what they saw before. Only the INDEX is two-level.
//
// # String mode
//
// strHashTable is not converted here. Its keys live in a chunked arena
// shared by the whole table and its entries carry a 32-bit hashTag; a
// two-level split needs either a per-bucket arena (256 chunk lists, and
// every arenaString alias must stay valid across the split — they do, the
// chunks are append-only and never move, so the conversion can hand each
// bucket the SAME chunk list and only re-index) or an arena that stays
// global while only the entry array splits (simpler: the arena is already
// chunked, so it is not the thing that rehashes). The tag is a stored
// value, not an index window, so the low-8-bit bucket does not disturb it.
// Deferred to keep this change to the two modes that dominate Q33.
var twoLevelToggle = optswitch.Register("two-level-ht", "WADJET_TWO_LEVEL_HT",
	"256-bucket two-level group index past twoLevelConvertAt keys (per-bucket rehash instead of whole-table)")

// TwoLevelConversions counts flat→bucketed conversions and
// TwoLevelDirectBuilds counts indexes built bucketed from an NDV hint
// (observability + test assertions).
var (
	TwoLevelConversions  atomic.Int64
	TwoLevelDirectBuilds atomic.Int64
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

// twoLevelConvertAt is the live-entry count at which a flat group index
// converts to the bucketed form. A var so benchmarks and tests can move it;
// production never writes it.
//
// ClickHouse converts at 100K. On THIS stack that is too early, because our
// flat table does not have the costs 100K is meant to dodge: its entries live
// in a MAP_NORESERVE reservation (ADR-0006 amendment), so a doubling is one
// mmap the kernel backs with huge pages, never a Go-heap allocation, and up to
// a few million entries the whole table is still L3-resident.
//
// Measured through the real consume path on a 5900X
// (BenchmarkHashAggregateHighCardTwoLevel, near-unique keys, COUNT+SUM+AVG,
// one interleaved window, min of 5):
//
//	shape              groups   flat     bucketed   delta
//	single int64          8M    783 ms    758 ms    -3.2%
//	packed two-int64      8M   1144 ms   1170 ms    +2.2%
//	single int64          1M     76 ms     88 ms    +16%   (see below)
//	packed two-int64      1M     99 ms    131 ms    +33%   (see below)
//
// And on the index alone (BenchmarkIntIndexConvertThreshold, fill from empty,
// off-heap backing, conversion forced at 100K so the sweep shows the
// STRUCTURAL crossover rather than this threshold):
//
//	entries    256K   512K    1M     2M     4M     8M    16M    32M
//	flat       2.81   6.93   21.6   60.6  140.6  296.7  628.6 1289.8  ms
//	bucketed   5.16   9.66   25.3   61.2  141.2  290.6  619.0 1268.6  ms
//
// Two things follow. First, the crossover is a few million entries — that is
// where the flat rehash stops being a cache-resident scatter — so converting
// at 100K would tax every mid-cardinality GROUP BY for nothing. Second, the
// 1M rows above are NOT the steady state: at exactly the threshold the table
// converts on its last batch and pays the whole conversion rehash (~30 ns per
// live entry) with nothing left to amortize it. That is the irreducible worst
// case of any size threshold; convertsToTwoLevel's growth test removes the
// common version of it (a table that has saturated), and the default is set
// at 1M so that everything below a million groups per sink — which is every
// TPC-H shape and most ClickBench ones — keeps the flat index unchanged.
// ClickBench Q33 is ~6M groups per partitioned sink and converts.
//
// The second, harder-to-benchmark half of the case is the growth transient:
// a flat doubling holds old+new live (1.5x the final table) at exactly the
// moment memory is tightest, while a bucketed doubling holds one bucket extra.
// Measured peak RSS filling 32M int keys: flat 1809 MB, bucketed 1685 MB.
// That margin is the GOMEMLIMIT class from the Q33 postmortem, not a
// throughput number.
//
// WADJET_TWO_LEVEL_AT overrides it. That is not a tuning knob for operators:
// it exists so the invariance oracle and the differential harness can drive
// the bucketed path on corpora whose group counts are nowhere near a
// million, which is otherwise the only way this code stays dark in CI.
var twoLevelConvertAt = envPositiveInt("WADJET_TWO_LEVEL_AT", 1_000_000)

// envPositiveInt reads a positive integer from the environment, falling back
// to def on anything unparseable.
func envPositiveInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// offheapSubMinBytes is the size at which a bucket's entry array moves
// off-heap. 2 MiB, and the number is load-bearing: it is the huge-page size.
//
// The flat table goes off-heap unconditionally, which is right for ONE array
// of tens of MB. Applied per bucket it is a trap: a mapping smaller than a
// huge page is faulted in 4 KiB at a time and stays 4 KiB-paged, so a 256 MB
// index spread over 256 sub-mappings takes ~65k faults and blows the dTLB,
// where the same bytes in one big mapping take ~128 huge-page faults.
// Measured (BenchmarkIntIndexOffheapSubGate, fill 8M int keys, min of 5,
// one interleaved window):
//
//	flat                       306 ms
//	buckets off-heap >= 2 MiB  281 ms   (-8%)
//	buckets off-heap >= 64 KiB 427 ms   (+39%)
//	buckets off-heap >= 4 KiB  407 ms   (+33%)
//
// So a bucket goes off-heap once it can be huge-page backed, and below that
// it stays on the Go heap, where the allocator hands back already-faulted
// spans out of THP-backed arenas. The heap exposure that leaves is bounded
// by 256 x 2 MiB per index, and it is pointer-free (allocated from a
// noscan span, never traced).
//
// A var so tests can force either side of the gate.
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
}

// intTwoLevelTable is the bucketed form of intHashTable: same keys, same
// values, same fibHash, same 70% load factor and linear probing — the index
// is split across 256 independently-grown sub-tables.
type intTwoLevelTable struct {
	subs [twoLevelBuckets]intSubTable
	reg  *memory.OffheapRegistry
}

// newIntTwoLevelTable builds an empty bucketed index pre-sized for n TOTAL
// entries (each bucket gets n/256 at 70% load).
func newIntTwoLevelTable(n int, reg *memory.OffheapRegistry) *intTwoLevelTable {
	t := &intTwoLevelTable{reg: reg}
	capPerSub := subCapFor(n)
	for i := range t.subs {
		s := &t.subs[i]
		s.entries, s.offheap = allocIntSubEntries(reg, capPerSub)
		fillEmptyEntries(s.entries)
		s.mask = uint64(capPerSub - 1)
	}
	return t
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

// convertIntHashTableToTwoLevel rebuilds a flat int index as a bucketed one
// and releases the flat table's entry array. The flat table is left empty
// and must not be used afterwards.
func convertIntHashTableToTwoLevel(flat *intHashTable, reg *memory.OffheapRegistry) *intTwoLevelTable {
	t := newIntTwoLevelTable(flat.size, reg)
	for i := range flat.entries {
		e := &flat.entries[i]
		if e.key == intHashEmpty {
			continue
		}
		t.GetOrInsertAt(e.key, fibHash(e.key), e.val)
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
// bucket header array itself.
func (t *intTwoLevelTable) MemoryUsage() int64 {
	var n int64
	for i := range t.subs {
		n += int64(cap(t.subs[i].entries))
	}
	return n*int64(unsafe.Sizeof(intHashEntry{})) + int64(unsafe.Sizeof(t.subs))
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
	i := idx
	for k := 0; k < n; k++ {
		j := (i + 1) & s.mask
		e := &s.entries[j]
		if e.key == intHashEmpty {
			s.entries[i].key = intHashEmpty
			s.size--
			return deleted, true
		}
		id := (fibHash(e.key) >> twoLevelSlotShift) & s.mask
		if ((i - id) & s.mask) < ((j - id) & s.mask) {
			s.entries[i] = *e
			i = j
			continue
		}
		s.entries[i].key = intHashEmpty
		s.size--
		return deleted, true
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
	oldOffheap := s.offheap
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
	if oldOffheap {
		t.reg.Release(unsafe.Pointer(unsafe.SliceData(old)))
	}
	s.entries = newEntries
	s.offheap = offheap
	s.mask = newMask
}

// --- packed composite key mode -------------------------------------------

// packedSubTable is one bucket of a packedTwoLevelTable (see intSubTable).
type packedSubTable struct {
	entries []packedHashEntry
	mask    uint64
	size    int
	offheap bool
	_       [23]byte // pad to 64 B
}

// packedTwoLevelTable is the bucketed form of packedHashTable. Same
// conventions: 128-bit key inline in the entry, val == packedHashEmpty
// marks a free slot, 70% load, caller-paired CheckGrowAt.
type packedTwoLevelTable struct {
	subs [twoLevelBuckets]packedSubTable
	reg  *memory.OffheapRegistry
}

func newPackedTwoLevelTable(n int, reg *memory.OffheapRegistry) *packedTwoLevelTable {
	t := &packedTwoLevelTable{reg: reg}
	capPerSub := subCapFor(n)
	for i := range t.subs {
		s := &t.subs[i]
		s.entries, s.offheap = allocPackedSubEntries(reg, capPerSub)
		fillEmptyPackedEntries(s.entries)
		s.mask = uint64(capPerSub - 1)
	}
	return t
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
// bucketed one and releases the flat entry array.
func convertPackedHashTableToTwoLevel(flat *packedHashTable, reg *memory.OffheapRegistry) *packedTwoLevelTable {
	t := newPackedTwoLevelTable(flat.size, reg)
	for i := range flat.entries {
		e := &flat.entries[i]
		if e.val == packedHashEmpty {
			continue
		}
		t.GetOrInsertAt(e.lo, e.hi, packedHash(e.lo, e.hi), e.val)
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

func (t *packedTwoLevelTable) MemoryUsage() int64 {
	var n int64
	for i := range t.subs {
		n += int64(cap(t.subs[i].entries))
	}
	return n*int64(unsafe.Sizeof(packedHashEntry{})) + int64(unsafe.Sizeof(t.subs))
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
	oldOffheap := s.offheap
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
	if oldOffheap {
		t.reg.Release(unsafe.Pointer(unsafe.SliceData(old)))
	}
	s.entries = newEntries
	s.offheap = offheap
	s.mask = newMask
}
