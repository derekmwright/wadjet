package exec

import (
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// The hash index is PER GRACE PARTITION, and that is what makes it reclaimable.
//
// A grace build evicts one partition at a time: it writes that partition's
// batches to disk and nils their `buildBatches` slots, which frees the COLUMN
// data. Before this file the index was one global structure — one hash table,
// one arena of build refs, one chain — so the entries belonging to an evicted
// partition stayed resident and stayed charged. Measured on a 2,000-row build
// at a 1 MiB budget with all 64 partitions evicted and no build column data
// left in memory at all: `used = 106,320`, of which 98,304 (92%) was index —
// 65,536 of hash table, 28,672 of arena and chain, 4,096 of bloom (#823). At
// 20,000 rows the same shape held 696,320 gross index. The join's floor was
// therefore proportional to TOTAL build rows however much of the build it had
// spilled, which is the opposite of what a grace join promises.
//
// # Why per-partition TABLES and not just per-partition arenas
//
// Per-partition arenas alone are the contained change — a key's partition is a
// function of the KEY, so a table can index `arena[spillPartition(key)]` with
// the same int32 it already stores — and they reclaim the arena and chain,
// which is 27%. They leave the hash TABLE, which is 62%. That is a fix bounded
// by a model the same commit knows is incomplete, and it leaves #823's own
// headline shape where it was, so it was refused (rule 11) and the whole fix
// deferred to this arc.
//
// With one table PER PARTITION the arenas follow for free, because a
// partition's table only ever addresses its own arena: partition p's table
// holds only keys k with spillPartition(k) == p, and p's arena holds only the
// rows those keys chain through. Eviction frees the table, the arena, the
// chain and the matched bitmap together with the columns. What must survive is
// the BLOOM FILTER, and it is 4%. It is built at the END of the build over the
// keys the index then holds, which is exactly the key set the IN-MEMORY probe
// asks about — the partition router has already sent the rest to disk. It is
// NOT a filter over the whole build side, and that is why a build that spilled
// does not publish it upstream of the router (BloomPushdownOp, join.go).
//
// # The floor is therefore DERIVED, not measured
//
// After evicting every partition a join holds: the bloom filter, and the
// `joinIndexPart` headers themselves (one struct per partition, whose slices
// and table pointers are all nil). Both are computed from the structure sizes
// rather than observed, which is what `TestEvictingEveryPartitionFreesTheIndex`
// asserts. Nothing about that number depends on how many rows the build saw.
//
// # The cost, and where it is NOT paid
//
// The probe's inner loop gains a partition selection per row —
// `spillPartition(key) & partMask`, one multiply, one shift, two ands — and 64
// independently sized tables replace one. A build that cannot evict does not
// pay for a reclaim it can never use: `partMask` is 0 for every non-partitioned
// build (the flat path, the spilled-partition replay, the key-only builds), so
// those keep ONE table, ONE arena and ONE chain, and the selection folds to
// part 0. The two paths share one body rather than being written twice, because
// a probe that indexed a different table from the one the build wrote is a
// silently wrong answer and the way to make that impossible is to have one
// routing function — `spillPartition`, the same one the build's scatter and the
// probe's spilled-row routing already use.

// spillPartMul is the multiplier spillPartition mixes an int64 key with. It is
// deliberately not fibHash's: the partition and the slot must spread
// independently (join_spill.go).
const spillPartMul = 0x517CC1B727220A95

// joinIndexPart is one partition's share of the hash index. Exactly one of
// ints / strs is used, decided by the build's key encoding; the other stays
// nil for the life of the join.
//
// A nil `ints` (or `strs`) means "no key has landed in this partition yet",
// which the probe reads as "no match" — a partition that never received a build
// row cannot match anything. The tables are allocated on first insert rather
// than up front: 64 eagerly minted tables would put a fixed 16 KiB floor on
// every partitioned build, including the small ones a tight budget is made of.
type joinIndexPart struct {
	ints *intHashTable
	strs *strHashTable

	// arena holds this partition's build refs; next chains the refs that
	// share a key (-1 ends a chain). The table's int32 value is an index
	// into THIS partition's arena and is meaningless in any other.
	arena []buildRef
	next  []int32

	// matched marks arena entries a probe row matched. Allocated only for the
	// join types that emit unmatched (or matched) build rows.
	matched []bool
}

// sizeofJoinIndexPart is what one partition header costs when everything it
// points at has been freed. It is part of the derived floor.
const sizeofJoinIndexPart = int64(unsafe.Sizeof(joinIndexPart{}))

// initIndexParts gives the join n index partitions. n is either 1 (every build
// that cannot evict) or numSpillPartitions (the grace build). Idempotent for
// the same n, so a build path may call it without knowing whether Build
// already did.
func (h *HashJoin) initIndexParts(n int) {
	if len(h.parts) == n {
		return
	}
	h.parts = make([]joinIndexPart, n)
	if n > 1 {
		h.partMask = spillPartMask
	} else {
		h.partMask = 0
	}
}

// ensureIndexParts gives a join built as a struct literal its single index
// part. Every constructor and every build entry point calls it, so no path can
// reach idxPart with an empty slice.
func (h *HashJoin) ensureIndexParts() {
	if len(h.parts) == 0 {
		h.initIndexParts(1)
	}
}

// idxPart is the part a build row or probe row with this int key belongs to.
// It uses spillPartition — the SAME function the build's scatter and the
// probe's spilled-partition routing use — so a key can never be indexed in one
// partition and looked up in another.
func (h *HashJoin) idxPart(key int64) *joinIndexPart {
	return &h.parts[uint64(spillPartition(key))&h.partMask]
}

// idxPartBytes is idxPart for a serialized key.
func (h *HashJoin) idxPartBytes(key []byte) *joinIndexPart {
	return &h.parts[uint64(spillPartitionBytes(key))&h.partMask]
}

// arenaRows is the total number of build refs across every partition. It is
// what `len(arena)` used to answer, and the only callers left are the ones
// deciding whether a matched bitmap is needed at all.
func (h *HashJoin) arenaRows() int {
	n := 0
	for i := range h.parts {
		n += len(h.parts[i].arena)
	}
	return n
}

// allocMatched sizes every partition's matched bitmap to its arena. Called
// once, after the build has finished appending.
func (h *HashJoin) allocMatched() {
	for i := range h.parts {
		if n := len(h.parts[i].arena); n > 0 {
			h.parts[i].matched = make([]bool, n)
		}
	}
}

// hasMatchedBitmaps reports whether this join tracks matched build entries.
// It replaces the `h.arenaMatched != nil` test the probe's fast-path selection
// and the emit paths use.
func (h *HashJoin) hasMatchedBitmaps() bool { return h.matchedAlloc }

// freeIndexPart releases everything partition p's index holds. The bloom filter
// is deliberately untouched — it is one structure over the whole join, not a
// per-partition one, and the in-memory probe still consults it.
func (h *HashJoin) freeIndexPart(p int) {
	if p < 0 || p >= len(h.parts) {
		return
	}
	pt := &h.parts[p]
	pt.ints = nil
	pt.strs = nil
	pt.arena = nil
	pt.next = nil
	pt.matched = nil
}

// indexBytes is the heap the index structures occupy right now: every
// partition's table, arena, chain and matched bitmap, the partition headers
// themselves, and the bloom filter. It is recomputed rather than accumulated,
// so a freed partition shows up as a smaller number on the next reconcile —
// which is how the reclaim reaches the tracker.
func (h *HashJoin) indexBytes() int64 {
	size := int64(len(h.parts)) * sizeofJoinIndexPart
	for i := range h.parts {
		pt := &h.parts[i]
		if pt.ints != nil {
			size += pt.ints.MemoryUsage()
		}
		if pt.strs != nil {
			size += pt.strs.MemoryUsage()
		}
		size += int64(cap(pt.arena)) * 8 // buildRef = 8 bytes
		size += int64(cap(pt.next)) * 4  // int32 = 4 bytes
		size += int64(cap(pt.matched))   // bool = 1 byte
	}
	size += int64(len(h.bloom)) * 8 // uint64 = 8 bytes
	return size
}

// evictedIndexFloor is what indexBytes CANNOT fall below: the partition
// headers and the bloom filter, the two structures an eviction does not free.
// The reclaim gate asserts the measured floor equals this derived one.
func (h *HashJoin) evictedIndexFloor() int64 {
	return int64(len(h.parts))*sizeofJoinIndexPart + int64(len(h.bloom))*8
}

// intPartTable returns partition p's int table, minting it if this is the
// partition's first key. hint sizes a fresh table.
func (pt *joinIndexPart) intTable(hint int) *intHashTable {
	if pt.ints == nil {
		if hint < 1 {
			hint = 1
		}
		pt.ints = newIntHashTable(hint)
	}
	return pt.ints
}

// strTable returns partition p's string table, minting it if this is the
// partition's first key.
//
// The key arena's first chunk is scaled DOWN by the partition count. A string
// table's arena ramps 8 KiB -> 1 MiB, and 64 of them would put half a megabyte
// of key arena on a build whose keys are a few hundred bytes — larger than the
// budgets the spill sweep runs at. Dividing the first chunk by the partition
// count keeps the SUM of the first chunks at what one table used to take, and
// the ramp reaches the same 1 MiB ceiling for a partition that really does hold
// a megabyte of keys.
func (h *HashJoin) strTable(pt *joinIndexPart, hint int) *strHashTable {
	if pt.strs == nil {
		if hint < 1 {
			hint = 1
		}
		pt.strs = newStrHashTableChunked(hint, strArenaFirstChunk/len(h.parts))
	}
	return pt.strs
}

// releaseIndexCharge gives back everything this join has charged the tracker
// for its index, under the purpose it was charged with.
func (h *HashJoin) releaseIndexCharge() {
	if h.MemTracker == nil || h.trackedHashOverhead <= 0 {
		return
	}
	h.MemTracker.ReleaseForced(h.trackedHashOverhead, memory.ForceJoinIndex)
	h.trackedMem -= h.trackedHashOverhead
	h.trackedHashOverhead = 0
}

// growPartFor pre-grows one partition's table, arena and chain so a run of
// n NoGrow inserts cannot overflow. Both the flat build (one part, the whole
// batch) and the grace build (one part, that partition's slice of the batch)
// go through it, so the load-factor invariant is stated once.
//
// A table minted here is sized to n: seeding at the 16-slot default and then
// inserting more than 16 keys with PutNoGrow spins forever on a full table,
// which is a hang the string path has hit twice.
func (h *HashJoin) growPartFor(pt *joinIndexPart, n int) {
	if n <= 0 {
		return
	}
	if h.useIntKey || h.useDualIntKey {
		pt.intTable(n).EnsureCapacity(n)
	} else {
		h.strTable(pt, n).EnsureCapacity(n)
	}
	// Pre-grow the ref arena the same way: every inserted row appends one
	// buildRef + one chain link, and the per-append doubling inside
	// arenaAppend* re-memmoved both slices log2(buildRows) times per build
	// (14% of worker growslice CPU, 2026-08-12 treatment profile).
	if need := len(pt.arena) + n; cap(pt.arena) < need {
		grown := make([]buildRef, len(pt.arena), need+need/2)
		copy(grown, pt.arena)
		pt.arena = grown
	}
	if need := len(pt.next) + n; cap(pt.next) < need {
		grown := make([]int32, len(pt.next), need+need/2)
		copy(grown, pt.next)
		pt.next = grown
	}
}

// checkGrowPart is the deferred load-factor check a batch of NoGrow inserts
// owes its partition.
func (h *HashJoin) checkGrowPart(pt *joinIndexPart) {
	if pt.ints != nil {
		pt.ints.CheckGrow()
	}
	if pt.strs != nil {
		pt.strs.CheckGrow()
	}
}

// forEachArenaEntry walks every partition's arena in partition order, handing
// the callback the part, the index within THAT part's arena (which is what a
// matched bitmap is indexed by) and the ref.
//
// The order it visits refs in is not the order one global arena had. Nothing
// downstream depends on it: every caller collects refs, deduplicates them on
// (batchIdx, rowIdx) and emits build rows, and a RIGHT/FULL join's unmatched
// rows have no defined order (ADR-0013's legal-nondeterminism list).
func (h *HashJoin) forEachArenaEntry(fn func(pt *joinIndexPart, i int, ref buildRef)) {
	for pi := range h.parts {
		pt := &h.parts[pi]
		for i, ref := range pt.arena {
			fn(pt, i, ref)
		}
	}
}
