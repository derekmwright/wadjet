// This file holds group-index layout decisions and accessors.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- two-level group index plumbing (two_level_hash.go) ------------------
//
// The int and packed key modes hold EITHER a flat table or a bucketed one.
// These helpers are the seam: hot paths hoist the choice out of the row loop
// (see consumeBatchIntGroup), cold paths — merge, spill, accounting — call
// through here so the mode is decided in exactly one place per operation.

// convertsToTwoLevel decides, at the END of a batch, whether a flat index
// should become bucketed. Two conditions:
//
//   - size: live >= twoLevelConvertAt — the measured structural crossover,
//     below which a flat rehash is still a cache-resident scatter and
//     bucketing is overhead (see two_level_hash.go for the curve).
//   - imminent rehash: live + incoming crosses the flat table's 70% load
//     factor. That is the only moment the conversion is free: it rehashes
//     the entries grow() was about to rehash, into the capacity grow() was
//     about to allocate, and the flat doubling then never happens. Anywhere
//     else in the fill the conversion REPLACES NOTHING and the table still
//     owes its doubling — the ≈10:1 overhead-to-benefit ratio the SF100
//     profile found, and the near-unique-key regression it produced (Q18,
//     +87%).
//
// incoming is what the caller is about to insert: the consume path passes
// the new-group count of the batch just finished, as the estimate of the
// next batch's; the merge path passes the incoming aggregate's group count,
// which is exact.
//
// The growth-rate test this replaces (newGroups*4 >= rows, "still filling")
// could not veto the losing bet it was written for: a near-unique key mints
// a group on every row, so it passed unconditionally, on every batch, for
// exactly the shape that pays the most. The load-factor test subsumes its
// real intent — a saturated table adds no groups, so it can never cross,
// so it can never convert.
//
// Both are per-BATCH tests on numbers the consume loop already has; nothing
// here runs per row.
func convertsToTwoLevel(live, slots, incoming int) bool {
	if !twoLevelToggle.On() || live < twoLevelConvertAt {
		return false
	}
	// WADJET_TWO_LEVEL_AT (and the test helper that stands in for it) drops
	// the lookahead so CI corpora, whose tables never reach a doubling
	// either, still exercise the bucketed path — see two_level_hash.go.
	return twoLevelConvertEager || (live+incoming)*10 > slots*7
}

// indexConverts is convertsToTwoLevel with this sink's construction-time
// layout decision in front of it: a bounded sink was born flat and stays
// flat for its whole (single-epoch) life. Every conversion site — consume,
// both SoA merges — goes through here so the rule lives in one place.
func (h *HashAggregate) indexConverts(live, slots, incoming int) bool {
	return !h.indexBornFlat && convertsToTwoLevel(live, slots, incoming)
}

// indexLayoutStaysFlat decides the group-index layout from what the sink
// knows before its first row. Two independent bounds, either of which pins
// the index flat for life; see two_level_hash.go for both rules, their
// derivations and their measurements:
//
//   - the epoch byte cap and the per-group state size give a group ceiling
//     Gmax = C/s, compared against twoLevelBoundedMinGroups (ADR-0014);
//   - the declared input-row bound is compared against
//     twoLevelMinAmortizeRows() — an index that will see fewer rows than
//     that in total has nothing left to repay a conversion with.
//
// Returns whether the index must be flat, the epoch-cap group ceiling
// (0 = no cap — kept for the worker's log line), and which bound decided it.
func (h *HashAggregate) indexLayoutStaysFlat(b *batch.RecordBatch) (flat bool, ceiling int64, why indexFlatReason) {
	if h.epochByteCap > 0 && bornFlatToggle.On() {
		s := h.perGroupStateBytes(b)
		if s <= 0 {
			// No usable estimate. A bounded sink is flat unconditionally
			// then — an index rebuilt every epoch can never amortize a
			// conversion.
			return true, 0, flatReasonEpochCap
		}
		ceiling = h.epochByteCap / int64(s)
		if ceiling < twoLevelBoundedMinGroups {
			return true, ceiling, flatReasonEpochCap
		}
	}
	if h.inputRowBound > 0 && rowBoundToggle.On() &&
		h.inputRowBound < twoLevelMinAmortizeRows() {
		return true, ceiling, flatReasonRowBound
	}
	return false, ceiling, flatNotPinned
}

// indexFlatReason is which of the two construction-time bounds pinned a
// group index flat, as it surfaces through the exported IndexFlatReason
// accessor and, from there, the worker's task logs: the fragment aggregate's
// "fragment task phases" line carries it as agg_layout/agg_layout_reason
// (executor_fragment.go's aggregateLayoutReporter); the shuffle partial
// agg's "shuffle partial agg" line carries only the born_flat bool, since
// that path never sets an input-row bound and so is always epoch-cap when
// pinned (executor.go).
type indexFlatReason uint8

const (
	flatNotPinned indexFlatReason = iota
	flatReasonEpochCap
	flatReasonRowBound
)

func (r indexFlatReason) String() string {
	switch r {
	case flatReasonEpochCap:
		return "epoch-cap"
	case flatReasonRowBound:
		return "row-bound"
	}
	return ""
}

// perGroupStateBytes is a LOWER bound on the bytes one group adds to
// groupMemoryUsage on the int/packed SoA fast paths — the only paths that
// can hold a two-level index. A lower bound gives an UPPER bound on the
// group ceiling, which is the safe direction: the layout is pinned flat only
// when even the most optimistic group count stays under G*.
//
// The terms mirror what resolveIndices and initFlatAccums actually allocate:
// one hash-table entry per group at the load-factor ceiling, the key SoA,
// and one flat accumulator array element per aggregate. Anything charged
// beyond these (probe slack below 70 % load, the group-state pool, generic
// key mirrors) only makes the real per-group cost larger, i.e. the real
// ceiling smaller.
func (h *HashAggregate) perGroupStateBytes(b *batch.RecordBatch) int {
	// Index: 16-byte entries (intHashEntry; packedHashEntry is 32, charged
	// as 16 to keep this a lower bound for both) at the 70 % load factor.
	// The bucketed form splits the same slot count 256 ways, so this term is
	// identical for either layout.
	n := 16 * 10 / 7
	// Key SoA: one int64 for the single-int path, one 128-bit packed key for
	// the composite path.
	if len(h.GroupByCols) >= 2 {
		n += 16
	} else {
		n += 8
	}
	needsCount := false
	for i, agg := range h.Aggs {
		if aggNeedsCount(agg.Func) {
			needsCount = true
		}
		ci := -1
		if i < len(h.aggColIdx) {
			ci = h.aggColIdx[i]
		}
		if ci < 0 || ci >= len(b.Columns) {
			continue // COUNT(*): the shared count array below is all it takes
		}
		switch agg.Func {
		case AggSum, AggAvg, AggMin, AggMax:
			if b.Columns[ci].Type == batch.TypeDecimal {
				n += 16 // Int128 sum/min/max
			} else {
				n += 8 // int64 / float64
			}
			if agg.Func == AggMin || agg.Func == AggMax {
				n++ // hasMin / hasMax
			}
		}
	}
	if needsCount {
		n += 8 // one shared count[] array (planCountArrays folds the rest)
	}
	return n
}

// maybeConvertIntIndex converts the flat single-int index to the bucketed
// form. Called at the end of a batch, so the consume loop's hoisted table
// pointer stays valid for the whole batch and the conversion applies from
// the next one.
func (h *HashAggregate) maybeConvertIntIndex(newGroups int) {
	idx := h.intGroupIndex
	if idx == nil || !h.indexConverts(idx.Len(), idx.Slots(), newGroups) {
		return
	}
	h.intTwoLevel = convertIntHashTableToTwoLevel(idx, h.offheapReg())
	h.intGroupIndex = nil
	h.indexConversions++
	TwoLevelConversions.Add(1)
}

// maybeConvertPackedIndex is maybeConvertIntIndex for the packed composite
// key mode.
func (h *HashAggregate) maybeConvertPackedIndex(newGroups int) {
	idx := h.packedIdx
	if idx == nil || !h.indexConverts(idx.Len(), idx.Slots(), newGroups) {
		return
	}
	h.packedTwoLevel = convertPackedHashTableToTwoLevel(idx, h.offheapReg())
	h.packedIdx = nil
	h.indexConversions++
	TwoLevelConversions.Add(1)
}

// intIndexLen reports the live entry count of whichever int index is active.
func (h *HashAggregate) intIndexLen() int {
	if h.intTwoLevel != nil {
		return h.intTwoLevel.Len()
	}
	if h.intGroupIndex != nil {
		return h.intGroupIndex.Len()
	}
	return 0
}

// intIndexPresent reports whether this aggregate holds a single-int index.
func (h *HashAggregate) intIndexPresent() bool {
	return h.intGroupIndex != nil || h.intTwoLevel != nil
}

// intIndexForEach iterates the active int index's live entries.
func (h *HashAggregate) intIndexForEach(fn func(key int64, val int32)) {
	if h.intTwoLevel != nil {
		h.intTwoLevel.ForEach(fn)
		return
	}
	if h.intGroupIndex != nil {
		h.intGroupIndex.ForEach(fn)
	}
}

// intIndexDelete removes a key from the active int index (partial-drain slot
// reclaim).
func (h *HashAggregate) intIndexDelete(key int64) {
	if h.intTwoLevel != nil {
		h.intTwoLevel.Delete(key)
		return
	}
	if h.intGroupIndex != nil {
		h.intGroupIndex.Delete(key)
	}
}

// intIndexGetOrInsert is the cold-path (merge) insert into the active int
// index.
func (h *HashAggregate) intIndexGetOrInsert(key int64, val int32) (int32, bool) {
	if h.intTwoLevel != nil {
		return h.intTwoLevel.GetOrInsert(key, val)
	}
	return h.intGroupIndex.GetOrInsert(key, val)
}

// packedIndexGetOrInsert is the cold-path (merge) insert into the active
// packed index.
func (h *HashAggregate) packedIndexGetOrInsert(lo, hi uint64, val int32) (int32, bool) {
	if h.packedTwoLevel != nil {
		return h.packedTwoLevel.GetOrInsert(lo, hi, val)
	}
	return h.packedIdx.GetOrInsert(lo, hi, val)
}
