// SPDX-License-Identifier: MIT

package exec

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

// keyOnlyBuildWorkers is how many workers a key-only build uses; one selects
// the serial route. It is runtime.NumCPU(), which follows the process's CPU
// affinity but neither GOMAXPROCS nor the planner's WADJET_SCAN_WORKERS. A
// variable so a test can select the serial route on a many-core host.
var keyOnlyBuildWorkers = runtime.NumCPU

// CompactBloomsOmitted counts compact builds that finished without a bloom
// filter because its reservation was refused.
var CompactBloomsOmitted atomic.Int64

// CompactNEDeclined counts NE-eligible joins that took the generic filtered,
// spillable build because the distinct-pair state was estimated not to fit.
var CompactNEDeclined atomic.Int64

// neShouldDecline reports whether a NE-eligible join should take the generic
// filtered build instead of the distinct-pair one, decided at Build entry
// before any row is consumed — the only point at which the choice is free:
// the distinct-pair state keeps at most two values per key and no NULL values,
// so once rows are folded into it the generic build cannot be reconstructed.
//
// It declines only when all of these hold:
//   - the join has a budget, a spill manager and a routed probe, so the
//     generic build is the grace-partitioned one that spills under pressure;
//   - the plan gave a row estimate (none: keep the distinct-pair build, and a
//     build that overruns refuses honestly);
//   - that estimate, read as an upper bound on distinct keys, needs more than
//     the budget's room: the key table at its final size plus the half-size
//     table it grows from (both live during the last rehash), one pair per
//     key, and the bloom.
//
// The generic filtered build answers the same SQL: it is the route every `<>`
// residual takes when its value types do not pair exactly (neExactPair), and
// the distinct-pair build is held to its answers (join_semianti_ne_test.go).
func (h *HashJoin) neShouldDecline() bool {
	if !h.semiAntiNEEligible() || h.MemTracker == nil || h.Spill == nil || !h.probeRoutesByPartition() {
		return false
	}
	budget := h.MemTracker.Budget()
	if budget <= 0 || h.BuildRowHint <= 0 {
		return false
	}
	return neStateEstimate(h.BuildRowHint) > budget-h.MemTracker.Used()
}

// neStateEstimate is the distinct-pair state for up to n distinct keys.
func neStateEstimate(n int64) int64 {
	slots := int64(tableSlotsFor(int(n)))
	return slots*intEntryBytes*3/2 + n*nePairBytes + int64(bloomSlotsFor(int(n)))*8
}

// Accounting for the compact builds (#1706, line 1706p; ADR-0006): a key-only
// semi/anti build, serial or parallel, and the distinct-pair NE build
// (join_semianti_ne.go). They keep no batches, so they are never grace-
// partitioned and cannot spill; before this they reserved nothing as they
// grew. The parallel route charged nothing at all, and the serial key-only
// and NE routes force-charged their index only once the build had finished,
// with the NE pairs never charged.
//
// Every allocation those builds make is now reserved BEFORE it is made, as an
// ordinary, refusable reservation on the join's tracker:
//
//   - every hash table (the initial hinted table and every replacement entry
//     array: the full replacement is reserved while the old one is still
//     live, and the old one is released after the rehash);
//   - every string key-arena chunk, and the chunk-header slice when it grows;
//   - the parallel route's per-worker tables and key buffers, and the merge:
//     each donor stays charged until its table is dropped, and the adopted
//     table's charge simply stays where it is;
//   - the index part headers, the NE pair slice (its hinted preallocation and
//     every growth) and the bloom filter.
//
// A refused reservation stops the build before the allocation it would have
// paid for, and the build fails with the budget error naming the join and
// what it holds: an honest refusal in place of a silent overrun. The bloom is
// the one optional structure: when it is refused the build completes without
// one (every probe path treats a nil bloom as "may contain").
//
// The local key-encoding buffer is the one allocation charged AFTER it grows
// (at the end of the batch whose longest key grew it): append decides its size,
// and it is bounded by the longest single key.
//
// These reservations are kept apart from the forced index reconciliation the
// storing builds use (reconcileHashMemory): a compact build never calls it, so
// no byte is charged twice and Close gives these back as ordinary bytes, not as
// forced ones. The shared hash tables' own growth policy is unchanged: the
// builds call growTo, nextChunkSize and getOrInsertNoGrowNoChunk, which the
// aggregates do not.
//
// None of these builds registers for relief. They cannot spill, so there is
// nothing another operator's RequestRelief could take from them; advertising
// it would make that operator wait for room that never comes.

// compactReserve reserves n bytes for a compact build. It is safe from the
// parallel workers: the tracker is atomic and compactCharged is an atomic, so
// no lock is taken and none can be held across a source read or a relief call.
func (h *HashJoin) compactReserve(n int64, what string) error {
	if h.MemTracker == nil || n <= 0 {
		return nil
	}
	if err := h.MemTracker.Reserve(n); err != nil {
		kind := "semi"
		if h.JoinType == AntiJoin {
			kind = "anti"
		}
		return fmt.Errorf("hash join %s (%s join, build keys %v): %s needs %d more bytes "+
			"while the build holds %d; this build cannot spill: %w",
			h.compactRoute(), kind, h.RightKeys, what, n, h.compactCharged.Load(), err)
	}
	h.compactCharged.Add(n)
	return nil
}

// CompactCharged is what a compact build of this join holds reserved now.
func (h *HashJoin) CompactCharged() int64 { return h.compactCharged.Load() }

// compactRelease gives back n bytes a compact build reserved, after the
// structure they paid for is no longer referenced.
func (h *HashJoin) compactRelease(n int64) {
	if h.MemTracker == nil || n <= 0 {
		return
	}
	h.compactCharged.Add(-n)
	h.MemTracker.Release(n)
}

// releaseCompactCharge gives back everything the compact build still holds a
// reservation for. Called with the structures already dropped: by Close, and
// by a compact build's failure exit.
func (h *HashJoin) releaseCompactCharge() {
	if n := h.compactCharged.Swap(0); n > 0 && h.MemTracker != nil {
		h.MemTracker.Release(n)
	}
}

// dropCompactState drops everything a compact build allocated and releases its
// reservation. A failed compact build calls it on every exit (error, cancel,
// refusal, panic), so no state and no charge survive the failure whether or not
// the caller ever closes the join.
func (h *HashJoin) dropCompactState() {
	for i := range h.parts {
		h.parts[i].ints = nil
		h.parts[i].strs = nil
	}
	h.bloom = nil
	h.bloomMask = 0
	h.nePairs = nil
	h.keyBuf = nil
	h.compactKeyBufCharged = 0
	h.releaseCompactCharge()
}

// compactReserveStart reserves what exists before a compact build inserts its
// first key: the index part headers and the join's key buffer.
func (h *HashJoin) compactReserveStart() error {
	if err := h.compactReserve(int64(cap(h.parts))*sizeofJoinIndexPart, "the index part headers"); err != nil {
		return err
	}
	return h.chargeKeyBufGrowth(h.keyBuf, &h.compactKeyBufCharged)
}

func (h *HashJoin) compactRoute() string {
	if h.neActive {
		return "distinct-pair NE build"
	}
	return "key-only build"
}

// tableSlotsFor is newIntHashTable's and newStrHashTableChunked's sizing: the
// power of two (at least 16) that holds n entries under the 70% load factor.
func tableSlotsFor(n int) int {
	c := 16
	target := n + n/3
	for c < target {
		c <<= 1
	}
	return c
}

// slotsAfter is the slot count EnsureCapacity would reach for `additional`
// more entries: the current count doubled until the load factor holds.
func slotsAfter(slots, size, additional int) int {
	for (size+additional)*10 > slots*7 {
		slots <<= 1
	}
	return slots
}

const (
	intEntryBytes   = 16 // intHashEntry
	strEntryBytes   = 16 // strEntry
	chunkHeaderSize = 24 // one []byte header in strHashTable.chunks
	nePairBytes     = 24 // nePair
)

// newIntTableCharged reserves and makes an int table for n entries.
func (h *HashJoin) newIntTableCharged(n int, what string) (*intHashTable, error) {
	if n < 1 {
		n = 1
	}
	if err := h.compactReserve(int64(tableSlotsFor(n))*intEntryBytes, what); err != nil {
		return nil, err
	}
	return newIntHashTable(n), nil
}

// newStrTableCharged reserves and makes a string table for n entries. Its
// arena starts empty; chunks are reserved as they are added.
func (h *HashJoin) newStrTableCharged(n, firstChunk int, what string) (*strHashTable, error) {
	if n < 1 {
		n = 1
	}
	if err := h.compactReserve(int64(tableSlotsFor(n))*strEntryBytes, what); err != nil {
		return nil, err
	}
	return newStrHashTableChunked(n, firstChunk), nil
}

// ensureIntCapacityCharged is EnsureCapacity with the replacement reserved
// first: the full new entry array while the old one is live, the old one
// released after the single rehash.
func (h *HashJoin) ensureIntCapacityCharged(t *intHashTable, additional int, what string) error {
	cur := len(t.entries)
	next := slotsAfter(cur, t.size, additional)
	if next == cur {
		return nil
	}
	if err := h.compactReserve(int64(next)*intEntryBytes, what); err != nil {
		return err
	}
	t.growTo(next)
	h.compactRelease(int64(cur) * intEntryBytes)
	return nil
}

// ensureStrCapacityCharged is ensureIntCapacityCharged for a string table's
// entry array (its arena does not move on a rehash).
func (h *HashJoin) ensureStrCapacityCharged(t *strHashTable, additional int, what string) error {
	cur := len(t.entries)
	next := slotsAfter(cur, t.size, additional)
	if next == cur {
		return nil
	}
	if err := h.compactReserve(int64(next)*strEntryBytes, what); err != nil {
		return err
	}
	t.growTo(next)
	h.compactRelease(int64(cur) * strEntryBytes)
	return nil
}

// addStrChunkCharged reserves and adds the arena chunk a key of need bytes
// requires, and the chunk-header slice's replacement when it is full.
func (h *HashJoin) addStrChunkCharged(t *strHashTable, need int) error {
	size := int64(t.nextChunkSize(need))
	var hdrCap int
	if len(t.chunks) == cap(t.chunks) {
		hdrCap = max(4, 2*cap(t.chunks))
	}
	if err := h.compactReserve(size+int64(hdrCap)*chunkHeaderSize, "a string key-arena chunk"); err != nil {
		return err
	}
	if hdrCap > 0 {
		old := cap(t.chunks)
		grown := make([][]byte, len(t.chunks), hdrCap)
		copy(grown, t.chunks)
		t.chunks = grown
		h.compactRelease(int64(old) * chunkHeaderSize)
	}
	t.addChunk(need)
	return nil
}

// ensureLocalKeyTable makes a parallel key-only worker's table (and, for a
// string key, its key buffer) on the worker's first batch. The hinted size is
// an optimization: when its reservation is refused the worker starts at the
// unhinted size and reserves its growth as keys arrive, so a pessimistic row
// hint alone cannot refuse a build whose keys fit. The caller then reserves
// the batch's own growth (ensure*CapacityCharged), which is what keeps a
// first batch from filling a small table mid-loop.
func (h *HashJoin) ensureLocalKeyTable(lk *localKeyBuild) error {
	if lk.intIndex != nil || lk.strIndex != nil {
		return nil
	}
	var err error
	for _, n := range []int{lk.hint, 64} {
		if h.useIntKey || h.useDualIntKey {
			lk.intIndex, err = h.newIntTableCharged(n, "a worker's key table")
		} else {
			lk.strIndex, err = h.newStrTableCharged(n, strArenaFirstChunk, "a worker's key table")
		}
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	if !h.useIntKey && !h.useDualIntKey && lk.keyBuf == nil {
		if err := h.compactReserve(localKeyBufCap, "a worker's key buffer"); err != nil {
			return err
		}
		lk.keyBuf, lk.keyBufCharged = make([]byte, 0, localKeyBufCap), localKeyBufCap
	}
	return nil
}

// strInsertCharged inserts key into t, adding (reserved) arena chunks as the
// key needs them. The table must already have room for one more entry.
func (h *HashJoin) strInsertCharged(t *strHashTable, key []byte) error {
	if _, _, need := t.getOrInsertNoGrowNoChunk(key, 0); !need {
		return nil
	}
	if err := h.addStrChunkCharged(t, len(key)); err != nil {
		return err
	}
	if _, _, need := t.getOrInsertNoGrowNoChunk(key, 0); need {
		return fmt.Errorf("hash join %s: a fresh %d-byte arena chunk did not fit a %d-byte key",
			h.compactRoute(), cap(t.chunks[len(t.chunks)-1]), len(key))
	}
	return nil
}

// chargeKeyBufGrowth charges a key buffer's growth since it was last charged.
// *charged is the capacity already paid for.
func (h *HashJoin) chargeKeyBufGrowth(buf []byte, charged *int) error {
	if c := cap(buf); c > *charged {
		if err := h.compactReserve(int64(c-*charged), "a key encoding buffer"); err != nil {
			return err
		}
		*charged = c
	}
	return nil
}

// buildBloomCharged builds the bloom filter only when its reservation is
// granted: the bloom is an optimization, so a refused one is omitted rather
// than allocated uncharged.
func (h *HashJoin) buildBloomCharged() {
	slots := bloomSlotsFor(h.indexKeyCount())
	if slots == 0 {
		return
	}
	if err := h.compactReserve(int64(slots)*8, "the bloom filter"); err != nil {
		CompactBloomsOmitted.Add(1)
		return
	}
	h.buildBloom() // sizes itself with bloomSlotsFor over the same keys
}

// indexKeyCount is the number of keys in the join's index tables.
func (h *HashJoin) indexKeyCount() int {
	var n int
	for i := range h.parts {
		if t := h.parts[i].ints; t != nil {
			n += t.Len()
		}
		if t := h.parts[i].strs; t != nil {
			n += t.Len()
		}
	}
	return n
}

// bloomSlotsFor is buildBloom's sizing: ~10 bits per key in a power-of-two
// count of 64-bit slots, at least 8; 0 for no keys.
func bloomSlotsFor(nKeys int) int {
	if nKeys == 0 {
		return 0
	}
	nBits := nKeys * 10
	nSlots := 1
	for nSlots*64 < nBits {
		nSlots *= 2
	}
	if nSlots < 8 {
		nSlots = 8
	}
	return nSlots
}

// growNEPairsCharged makes room in nePairs for `additional` more pairs before
// a batch is folded in, reserving the full replacement while the old slice is
// live and releasing the old one after the copy.
func (h *HashJoin) growNEPairsCharged(additional int) error {
	need := len(h.nePairs) + additional
	cur := cap(h.nePairs)
	if need <= cur {
		return nil
	}
	next := max(need, 2*cur)
	if err := h.compactReserve(int64(next)*nePairBytes, "the distinct-pair values"); err != nil {
		return err
	}
	grown := make([]nePair, len(h.nePairs), next)
	copy(grown, h.nePairs)
	h.nePairs = grown
	h.compactRelease(int64(cur) * nePairBytes)
	return nil
}
