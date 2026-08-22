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
// # Layout is decided at construction where the sink's bounds allow it
//
// Before any of the adaptive machinery below runs, a sink whose OWNER
// finalizes and rebuilds it on a byte cap — the shuffle sender's exchange
// partial aggregation, worker.cappedPartialAgg — is born FLAT and never
// converts. Such a sink's index cannot outlive one epoch, so a conversion
// has nothing after it to amortize against; see twoLevelBoundedMinGroups for
// the rule, its derivation and its measurements. Everything below applies to
// UNBOUNDED sinks (final/standalone aggregates), which is where the
// structure's measured wins are.
//
// # Adaptive conversion, not construction
//
// An unbounded sink starts FLAT, and converts AT THE POINT WHERE THE
// FLAT TABLE WOULD HAVE REHASHED ITSELF ANYWAY — see convertsToTwoLevel
// (aggregate.go) for the two tests and twoLevelConvertAt for the measured
// curve behind the size one. The decision runs once per BATCH, never per row:
// the consume loop hoists its table pointer for the whole batch and the
// conversion lands at the batch's end. Aggregates whose NDV hint already
// exceeds the threshold construct bucketed directly (resolveIndices) and never
// pay a conversion at all. Below the threshold nothing changes: no bucket
// indirection, no extra shift, byte-identical behavior to G5.
//
// The "would have rehashed anyway" half is load-bearing and was NOT true in
// the first version of this file, which converted on the first batch-end past
// a live-entry threshold. That point falls, on average, halfway between two
// doublings, so the conversion's scatter REPLACED NOTHING: the flat table
// still owed its next doubling, the bucketed table paid it as per-bucket
// growth, and the conversion was pure additional work. Measured on SF100
// TPC-H (release v0.16.0-correctness, merged 3-worker CPU profile) that came
// to ~79.6 CPU-s per suite run of conversion rehash against ~7.7 CPU-s of
// two-level probe benefit — ≈10:1 — concentrated in the shape that can never
// veto a growth-rate test: a NEAR-UNIQUE key, where every row mints a group
// (Q18's GROUP BY l_orderkey over 150M lineitem rows, +87% in that release).
//
// Converting at the load-factor crossing instead pays the conversion INSTEAD
// OF that doubling rather than on top of it, and the destination is the flat
// table's own slot count split 256 ways, so the doubling it displaced then
// happens as 256 per-bucket, cache-resident rehashes rather than one more
// whole-table scatter. A table that is not about to rehash is a table with
// nothing to save, so it stays flat — which also retires the old growth-rate
// heuristic: a saturated table cannot cross its load factor, so it can no
// longer convert at all.
//
// Sizing the destination at the DOUBLED capacity was tried and rejected on
// measurement. It looks like the tidier "replace grow() exactly" — the
// bucketed table is then born at 35% load and no bucket regrows — but it
// only moves the per-bucket doublings into the conversion's own scatter,
// which then works over twice the bytes and is DRAM-bound instead of
// cache-resident. Same-window A/B on the Q18 capped-epoch shape
// (BenchmarkAggIntCappedEpochs, near-unique 16M, n=5 medians): flat
// 2037 ms, doubled-capacity 2204 ms (+8.2%), flat-capacity 2058 ms (+1.0%).
// The scatter is the expensive part of a rehash, and the bucketed form's
// whole value is that its scatters are small.
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

// bornFlatToggle is the kill switch for the construction-time layout
// decision (twoLevelBoundedMinGroups / HashAggregate.boundedIndexStaysFlat).
// Off = the pre-2026-08-22 behavior: every sink starts flat and converts at
// runtime whenever convertsToTwoLevel says so, epoch cap or not.
var bornFlatToggle = optswitch.Register("two-level-born-flat", "WADJET_TWO_LEVEL_BORN_FLAT",
	"epoch-capped (bounded) aggregates build a flat group index and never convert")

// twoLevelBoundedMinGroups is G* — the group count a BOUNDED sink's epoch
// must be able to reach before the bucketed layout is allowed at all.
//
// A bounded sink is one whose owner finalizes it and builds a fresh one
// every C bytes of state (worker.cappedPartialAgg, C = 128 MB). Its index
// never outlives one epoch, so a conversion has nothing after it to
// amortize against: the flat→bucketed rehash is paid once per epoch, in
// full, near the epoch's end, on a table that is about to be thrown away.
// That is not a threshold to tune — it is a property of the operator's
// configuration, and it is known before the first row arrives.
//
// DERIVATION (SF100 TPC-H Q18, three same-window arms 2026-08-22,
// scratchpad/window-analysis-2026-08-22.md §1; ClickBench 3-arm run):
//
//		Gmax = C / s, s = per-group state (perGroupStateBytes)
//
//	  - Q18's exchange partial aggregate: C = 128 MB, s ≈ 46 B
//	    ⇒ Gmax ≈ 2.9 M. Measured groups per flush on the arm with the
//	    bucketed layout DISABLED: 12 497 812 out_rows / 5 flushes = 2.50 M.
//	    At that Gmax the bucketed layout costs the stage 3-4×: mean task
//	    2.25 s (flat) → 6.96 s (old gate) → 10.13 s (load-factor gate), and
//	    the conversion itself measures ~675 ns per live entry in production
//	    — 22-27× the 25-30 ns the structure was calibrated on, because 8-10
//	    tasks run the scatter concurrently against one shared L3.
//	  - The bucketed layout's measured wins are all UNBOUNDED sinks that keep
//	    one index for the whole input: ClickBench's high-cardinality GROUP BYs
//	    (~6 M groups per partitioned sink on Q33) and the 16 M near-unique
//	    arm of BenchmarkAggIntCardinalitySweep (−4.1 % vs flat). At 4 M groups
//	    the same sweep measures the bucketed arm +31 % — a LOSS — so 4 M is a
//	    floor on where bucketing could pay even with a full unbounded tail to
//	    amortize against, and a bounded sink has no tail at all.
//
// So G* = 4 M: at or below it every measurement of the bucketed layout is a
// loss or a wash, and only above it is there a measured win. With today's
// 128 MB cap no bounded sink reaches it (Gmax tops out around 3.5 M for the
// cheapest possible per-group state), which makes the rule equivalent to
// "bounded ⇒ flat" in production while keeping the door open for a future
// larger C. It is deliberately NOT compared against twoLevelConvertAt (1 M):
// that is a live-count crossover for a table that will keep growing, and a
// bounded sink's table by construction will not.
const twoLevelBoundedMinGroups = 4 << 20

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
// converted on its last batch and paid a whole conversion rehash (~30 ns per
// live entry) with nothing left to amortize it. That was the irreducible
// worst case of a bare size threshold, and it is what
// convertsToTwoLevel's load-factor test removes: a table that settles just
// past the threshold never crosses its load factor, so it never converts.
// The default stays at 1M so that everything below a million
// groups per sink — which is every TPC-H shape and most ClickBench ones —
// keeps the flat index unchanged. ClickBench Q33 is ~6M groups per
// partitioned sink and converts.
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
//
// Overriding it also switches conversion to EAGER — the size test alone
// decides, without convertsToTwoLevel's load-factor lookahead. Both halves
// of the shipped gate have to relax together for the override to do its job:
// a corpus whose tables never reach a million groups is also a corpus whose
// tables never reach a doubling, so a low threshold on its own would leave
// the conversion, and everything downstream of it, dark. What eager mode
// changes is WHEN the index converts, never WHAT it holds — the conversion
// is value-preserving, so the oracle's row sets are the same either way and
// its coverage of the bucketed path is strictly larger. Production, with no
// override, always takes the load-factor rule (and
// TestTwoLevelConvertsAtTheDoubling pins it there).
var twoLevelConvertAt, twoLevelConvertEager = twoLevelConvertPolicy()

// twoLevelConvertPolicy reads the conversion threshold and reports whether
// it came from an accepted WADJET_TWO_LEVEL_AT override.
func twoLevelConvertPolicy() (int, bool) {
	if v, err := strconv.Atoi(os.Getenv("WADJET_TWO_LEVEL_AT")); err == nil && v > 0 {
		return v, true
	}
	return 1_000_000, false
}

// offheapSubMinBytes is the size at which an entry array moves off-heap.
// 2 MiB, and the number is load-bearing: it is the huge-page size.
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
// Which is right, and was applied to the wrong UNIT. A bucket reaches 2 MiB
// only when the whole index is ~33M slots — past 23M groups in one sink,
// which nothing in TPC-H or ClickBench reaches. So in practice the gate was
// unreachable, and converting to the bucketed form silently moved the entire
// group index off its MAP_NORESERVE huge-page reservation onto the Go heap:
// 470 MB of heap churn per 16M-group fill where the flat table allocates
// 11 KB (ADR-0006's amendment, undone by the structure meant to complement
// it). With WADJET_OFFHEAP_AGG=0 — which puts BOTH forms on the heap — the
// same 16M near-unique fill inverts from +14.8% to -4.6% against flat: the
// backing, not the structure, was the loss.
//
// The unit that wants a huge page is the TABLE, not the bucket. So the 256
// buckets are carved out of ONE reservation whenever their total clears this
// gate (allocIntArena / newIntTwoLevelTableSub) — one mapping, one
// MADV_HUGEPAGE, zero Go heap, and the buckets still index and probe
// independently because the arena is only their backing store, never their
// addressing. A bucket that outgrows its slice allocates on its own (per
// the per-bucket gate below, which is now the fallback rather than the
// rule), and the arena is released as soon as the last bucket has left it.
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

// convertIntHashTableToTwoLevel rebuilds a flat int index as a bucketed one
// and releases the flat table's entry array. The flat table is left empty
// and must not be used afterwards.
//
// The destination has EXACTLY the flat table's slot count, split 256 ways.
// Two things follow. The conversion is then a pure re-permutation into the
// same number of slots — by the bit budget above, (bucket, slot) is
// bit-for-bit the index the flat table of that size computes — and, called
// where convertsToTwoLevel says (the flat table one batch from its load
// factor), the doubling the flat table was about to perform as ONE
// whole-table rehash instead happens as 256 per-bucket rehashes, each of
// them cache-resident. That is the structure's whole claim, applied to the
// one rehash that was already due.
//
// Sizing the destination at the DOUBLED capacity instead was measured and
// rejected: it removes the per-bucket doublings, but only by moving them
// into the conversion's own scatter, which then works over twice the bytes
// and is DRAM-bound. On the Q18 capped-epoch shape that cost +7 points of
// wall against this sizing (BenchmarkAggIntCappedEpochs, near-unique 16M).
//
// The insert loop is written out rather than calling GetOrInsertAt: the
// source keys are unique by construction, so there is no duplicate to test
// for.
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
