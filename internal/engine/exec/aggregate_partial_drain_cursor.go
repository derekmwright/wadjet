package exec

import (
	"sort"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// partialKeyMode selects how a cursor reifies group-key values from SoA state.
// One branch per HashAggregate group-key path: int, dual-int, compact, and
// (string-or-generic).
type partialKeyMode int

const (
	partialKeyModeInt partialKeyMode = iota
	partialKeyModeDualInt
	partialKeyModeCompact
	partialKeyModeStrOrGeneric
)

// partialGroupCursor is a streaming partialRunSource backed by a HashAggregate's
// SoA hash state. It builds a sort-key arena + sorted index up-front (memory
// proportional to keys, not to full partial-group records) and emits one
// *partialGroup per Peek/Advance by reading from the flat accumulator arrays
// at the next sorted index.
//
// Memory characteristics for N groups, ngc group cols, na aggs, k bytes/key:
//   - Pre-existing SoA arrays — already live; we don't re-allocate them
//   - Key arena       = N*k bytes  (typ. 16 B/group → 320 MB at N=20M)
//   - Key offsets     = (N+1)*4    (~80 MB at N=20M)
//   - Sort index      = N*4        (~80 MB at N=20M)
//   - Reusable head   = O(ngc + na), single struct overwritten per Advance
//
// Compare with the prior []*partialGroup materialization: each group allocated
// a *partialGroup (24 B) + a fresh []byte SortKey + a fresh []any KeyVals + a
// fresh []kernel.Accumulator Accs, totaling ~150–200 B per group, or ~3–4 GB
// at N=20M. The cursor cuts that to ~480 MB at SF100 Q17 scale.
//
// Lifetime: the cursor borrows references to the HashAggregate's SoA arrays
// and group-state slices. The caller must not mutate those arrays for the
// cursor's lifetime. finalizeViaPartialMerge transfers ownership by clearing
// the aggregate's references after construction so the cursor is the sole
// owner; spillPartialState consumes the cursor synchronously before its
// resetGroupStateAfterSpill call frees the references.
type partialGroupCursor struct {
	// Source state borrowed from HashAggregate.
	flatAccs       []flatAccumArrays
	intGroupStates []*groupState
	strGroupStates []*groupState
	dualIntKeysA   []int64
	dualIntKeysB   []int64
	serializedKeys []string // binary group keys, 1:1 with strGroupStates (deferred-boxing source of truth)
	groupColTypes  []batch.TypeID
	nAggs          int
	nGroupCols     int
	keyMode        partialKeyMode
	// useAoSAccs flips Acc loading from SoA flat arrays to per-group
	// gs.extras.accs. True iff the aggregate's intFlatAccs slice has been
	// cleared by a prior materializeFlatAccums (e.g. MergeSink → generic
	// migration), in which case the canonical accumulator state lives in
	// extras.accs and reading flat arrays would panic on empty slices.
	useAoSAccs bool

	// liveGroups, when non-nil, restricts the cursor to a subset of the
	// aggregate's slots. Position p in [0, numGroups) maps to SoA slot
	// liveGroups[p]. Used by the partial-drain spill path (subset = the
	// drained partitions) and by finalize after partial drains (subset =
	// surviving slots derived from intGroupIndex). nil means "all slots in
	// 0..len-1 order" — the pre-partial-drain behavior.
	liveGroups []int32

	// Sort state (built once during construction; immutable thereafter).
	keyArena   []byte   // contiguous serialized sort keys
	keyOffsets []int32  // len == numGroups+1; key for position p = arena[off[p]:off[p+1]]
	sortedIdx  []uint32 // permutation; cursor at pos emits position sortedIdx[pos]
	pos        int
	numGroups  int

	// Reusable head storage (overwritten in place per loadHeadAt).
	head        partialGroup
	headKeyVals []partialKeyValue
	headAccs    []kernel.Accumulator
}

// newPartialGroupCursor walks the aggregate's hash state, builds a sort-key
// arena + sorted index, and pre-positions on the first sorted group so the
// merger's Peek-then-Advance pattern works with no extra plumbing.
//
// Caller's contract:
//   - h.intFlatAccs is populated (one of the SoA paths is in use).
//   - h.canUseExternalMerge() is true (simpleAggs + SoA path).
//   - h's mode flags reflect the path that built the SoA state.
//
// The cursor does NOT call materializeFlatAccums — accs are read SoA-direct
// per Advance. This is the second-pass optimization equivalent to the one
// shipped for Next() in PR #90 (commit 91cd65f), now applied to the spill
// and finalize-via-merge drain paths.
func newPartialGroupCursor(h *HashAggregate, liveGroups []int32) *partialGroupCursor {
	var (
		keyMode   partialKeyMode
		srcGroups int
	)
	switch {
	case h.useIntGroupKey:
		keyMode = partialKeyModeInt
		srcGroups = len(h.intGroupStates)
	case h.useDualIntGroupKey:
		keyMode = partialKeyModeDualInt
		srcGroups = len(h.intGroupStates)
	case h.useCompactGroupKey:
		keyMode = partialKeyModeCompact
		srcGroups = len(h.intGroupStates)
	default: // useStrGroupKey || useGenericSoA || post-MergeSink generic
		keyMode = partialKeyModeStrOrGeneric
		srcGroups = len(h.strGroupStates)
	}
	numGroups := srcGroups
	if liveGroups != nil {
		numGroups = len(liveGroups)
	}

	nAggs := len(h.Aggs)
	nGroupCols := len(h.GroupByCols)

	c := &partialGroupCursor{
		flatAccs:       h.intFlatAccs,
		intGroupStates: h.intGroupStates,
		strGroupStates: h.strGroupStates,
		dualIntKeysA:   h.dualIntKeysA,
		dualIntKeysB:   h.dualIntKeysB,
		serializedKeys: h.serializedKeys,
		groupColTypes:  h.groupColTypes,
		nAggs:          nAggs,
		nGroupCols:     nGroupCols,
		keyMode:        keyMode,
		useAoSAccs:     len(h.intFlatAccs) == 0,
		liveGroups:     liveGroups,
		// Initial 16 B/group arena estimate fits typical numeric-key shapes;
		// strings will grow it via append, which is fine — arena writes are
		// strictly append-only and slices into it remain valid after grow.
		keyArena:    make([]byte, 0, numGroups*16),
		keyOffsets:  make([]int32, numGroups+1),
		sortedIdx:   make([]uint32, numGroups),
		numGroups:   numGroups,
		headKeyVals: make([]partialKeyValue, nGroupCols),
		headAccs:    make([]kernel.Accumulator, nAggs),
	}
	c.head.KeyVals = c.headKeyVals
	c.head.Accs = c.headAccs

	// Arena build pass. Position p in [0, numGroups) maps to SoA slot
	// c.slotAt(p) — either p (when liveGroups is nil) or liveGroups[p]
	// (when restricted to a subset). int / dual-int paths read SoA int
	// values straight into the arena via typed strconv writes (skips `any`
	// boxing). Compact and str/generic modes alias the existing
	// gs.extras.keyValues `[]any` — those boxes were allocated at consume
	// time and we just hand them to appendSerializedKey for serialization.
	// No new allocations on either branch.
	switch keyMode {
	case partialKeyModeInt, partialKeyModeDualInt:
		for p := 0; p < numGroups; p++ {
			slot := c.slotAt(p)
			c.keyOffsets[p] = int32(len(c.keyArena))
			c.keyArena = c.appendIntModeSortKey(c.keyArena, slot)
			c.sortedIdx[p] = uint32(p)
		}
	case partialKeyModeCompact:
		for p := 0; p < numGroups; p++ {
			slot := c.slotAt(p)
			c.keyOffsets[p] = int32(len(c.keyArena))
			c.keyArena = appendSerializedKey(c.keyArena, c.intGroupStates[slot].extras.keyValues)
			c.sortedIdx[p] = uint32(p)
		}
	default: // partialKeyModeStrOrGeneric
		for p := 0; p < numGroups; p++ {
			slot := c.slotAt(p)
			c.keyOffsets[p] = int32(len(c.keyArena))
			c.keyArena = appendSerializedKey(c.keyArena, c.strKeyValsAt(slot))
			c.sortedIdx[p] = uint32(p)
		}
	}
	c.keyOffsets[numGroups] = int32(len(c.keyArena))

	sort.Slice(c.sortedIdx, func(i, j int) bool {
		ai, bi := c.sortedIdx[i], c.sortedIdx[j]
		ak := c.keyArena[c.keyOffsets[ai]:c.keyOffsets[ai+1]]
		bk := c.keyArena[c.keyOffsets[bi]:c.keyOffsets[bi+1]]
		return bytesCompare(ak, bk) < 0
	})

	if numGroups > 0 {
		c.loadHeadAt(int(c.sortedIdx[0]))
	}
	return c
}

// slotAt translates a cursor position to a SoA slot index. With liveGroups
// nil, positions are slot indices directly (the pre-partial-drain default).
// With liveGroups non-nil, positions index into the supplied subset.
func (c *partialGroupCursor) slotAt(position int) int {
	if c.liveGroups == nil {
		return position
	}
	return int(c.liveGroups[position])
}

// populateHeadKeyVals fills c.headKeyVals (len == nGroupCols) with typed
// values for the group at SoA index gi.
//
// int / dual-int branches set typed slots without any `any` boxing:
// the int64 SoA value goes straight into headKeyVals[k].I64. Compact and
// str/generic branches read from gs.extras.keyValues (which is `[]any`
// from consume time) and unbox into the typed slot via setPartialKeyFromAny
// — no new boxes, just a one-time per-group dispatch on the existing box.
func (c *partialGroupCursor) populateHeadKeyVals(gi int) {
	switch c.keyMode {
	case partialKeyModeInt:
		gs := c.intGroupStates[gi]
		setPartialKeyFromInt(&c.headKeyVals[0], gs.intKey, c.groupColTypes[0])
	case partialKeyModeDualInt:
		setPartialKeyFromInt(&c.headKeyVals[0], c.dualIntKeysA[gi], c.groupColTypes[0])
		setPartialKeyFromInt(&c.headKeyVals[1], c.dualIntKeysB[gi], c.groupColTypes[1])
	case partialKeyModeCompact:
		gs := c.intGroupStates[gi]
		for i, v := range gs.extras.keyValues {
			if i >= len(c.headKeyVals) {
				break
			}
			setPartialKeyFromAny(&c.headKeyVals[i], v)
		}
	default: // partialKeyModeStrOrGeneric
		for i, v := range c.strKeyValsAt(gi) {
			if i >= len(c.headKeyVals) {
				break
			}
			setPartialKeyFromAny(&c.headKeyVals[i], v)
		}
	}
}

// strKeyValsAt returns the boxed key values for a str/generic-mode group,
// decoding from the binary serialized key when consume-time boxing was
// deferred (generic SoA path). The decode allocates, but this runs only
// on the spill/drain cold path — the whole point of deferral is that the
// hot consume loop never boxes.
func (c *partialGroupCursor) strKeyValsAt(gi int) []any {
	if ext := c.strGroupStates[gi].extras; ext != nil && ext.keyValues != nil {
		return ext.keyValues
	}
	return decodeSerializedKey(c.serializedKeys[gi], c.groupColTypes)
}

// setPartialKeyFromInt populates dst from an int64 SoA key value typed by t.
// Counterpart to reifyIntKey but writes a typed slot (no `any` box).
func setPartialKeyFromInt(dst *partialKeyValue, v int64, t batch.TypeID) {
	dst.Bytes = nil
	dst.F64 = 0
	dst.Dec = batch.Int128{}
	dst.I64 = v
	switch t {
	case batch.TypeBool:
		if v != 0 {
			dst.Tag = partialTagBoolTrue
		} else {
			dst.Tag = partialTagBoolFalse
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		dst.Tag = partialTagInt32
	default: // int64 / timestamp / ipv4 / mac / duration
		dst.Tag = partialTagInt64
	}
}

// loadHeadAt populates the reusable head storage from the cursor's source
// SoA arrays for the group at iteration position `position`. SortKey is read
// from the arena (indexed by position); KeyVals/Accs are read by translating
// position → slot via c.slotAt.
//
// SortKey aliases the arena slice (no copy) — the kWayMerger always copies
// SortKey into its merged group on Pop, and the heap entry's key is captured
// at heap.Push time. The arena is built once during construction and never
// modified after, so aliasing is safe across Advance calls.
//
// KeyVals/Accs reuse the underlying arrays. The merger copies their contents
// during merge (typed copy of partialKeyValue slots, plus
// `append([]kernel.Accumulator(nil), current.Accs...)`), so overwriting them
// in place between Advance calls cannot corrupt the merger's state.
func (c *partialGroupCursor) loadHeadAt(position int) {
	c.head.SortKey = c.keyArena[c.keyOffsets[position]:c.keyOffsets[position+1]]
	slot := c.slotAt(position)
	c.populateHeadKeyVals(slot)

	if c.useAoSAccs {
		c.loadHeadAccsAoS(slot)
		return
	}
	c.loadHeadAccsSoA(slot)
}

// loadHeadAccsSoA fills c.headAccs from the flat accumulator arrays at index
// gi. Used when the aggregate is still in a SoA mode and intFlatAccs carries
// the live accumulator state.
func (c *partialGroupCursor) loadHeadAccsSoA(gi int) {
	for ai := 0; ai < c.nAggs; ai++ {
		fa := &c.flatAccs[ai]
		acc := &c.headAccs[ai]
		// Reset acc so a previously-loaded group's HasMin/HasMax/etc. don't
		// leak into a fresh load that doesn't touch those fields.
		*acc = kernel.Accumulator{
			Count:     fa.count[gi],
			IsFloat:   fa.isFloat,
			IsDecimal: fa.isDecimal,
			DecScale:  fa.decScale,
		}
		if fa.sumI64 != nil {
			acc.SumI64 = fa.sumI64[gi]
		}
		if fa.sumF64 != nil {
			acc.SumF64 = fa.sumF64[gi]
		}
		if fa.sumDec != nil {
			acc.SumDec = fa.sumDec[gi]
		}
		if fa.minI64 != nil {
			acc.MinI64 = fa.minI64[gi]
			acc.HasMin = fa.hasMin[gi]
		}
		if fa.maxI64 != nil {
			acc.MaxI64 = fa.maxI64[gi]
			acc.HasMax = fa.hasMax[gi]
		}
		if fa.minF64 != nil {
			acc.MinF64 = fa.minF64[gi]
			acc.HasMin = fa.hasMin[gi]
		}
		if fa.maxF64 != nil {
			acc.MaxF64 = fa.maxF64[gi]
			acc.HasMax = fa.hasMax[gi]
		}
		if fa.minDec != nil {
			acc.MinDec = fa.minDec[gi]
			acc.HasMin = fa.hasMin[gi]
		}
		if fa.maxDec != nil {
			acc.MaxDec = fa.maxDec[gi]
			acc.HasMax = fa.hasMax[gi]
		}
	}
}

// loadHeadAccsAoS fills c.headAccs from per-group gs.extras.accs at index gi.
// Used when migrateToGenericMap (MergeSink path) cleared intFlatAccs and the
// canonical state lives in extras.accs.
func (c *partialGroupCursor) loadHeadAccsAoS(gi int) {
	var gs *groupState
	switch c.keyMode {
	case partialKeyModeInt, partialKeyModeDualInt, partialKeyModeCompact:
		gs = c.intGroupStates[gi]
	default:
		gs = c.strGroupStates[gi]
	}
	if gs.extras == nil || gs.extras.accs == nil {
		// Defensive: a group with no extras at this point would be a bug
		// upstream (migrate populates extras for all groups). Zero out the
		// head so any partial spill written from this cursor produces
		// identity values instead of stale fields from a prior load.
		for ai := range c.headAccs {
			c.headAccs[ai] = kernel.Accumulator{}
		}
		return
	}
	for ai := 0; ai < c.nAggs && ai < len(gs.extras.accs); ai++ {
		c.headAccs[ai] = gs.extras.accs[ai]
	}
	// Zero any tail slots so a short extras.accs doesn't leave stale data.
	for ai := len(gs.extras.accs); ai < c.nAggs; ai++ {
		c.headAccs[ai] = kernel.Accumulator{}
	}
}

// appendSerializedKey writes the same byte sequence as serializeKey directly
// into buf, returning the extended buf. The two share one definition of the
// on-the-wire format; callers track the pre-call length to recover offset
// boundaries.
func appendSerializedKey(buf []byte, vals []any) []byte {
	for i, v := range vals {
		if i > 0 {
			buf = append(buf, 0)
		}
		buf = appendKeyValue(buf, v)
	}
	return buf
}

// appendIntModeSortKey writes the sort key for an int- or dual-int-keyed
// group at SoA index gi directly from typed int64s, bypassing the []any
// box that appendKeyValue's type switch would otherwise require. Output is
// byte-identical to serializeKey([]any{reifyIntKey(...)}, ...) so spill
// files written via the cursor stay compatible with file-source readers.
func (c *partialGroupCursor) appendIntModeSortKey(buf []byte, gi int) []byte {
	if c.keyMode == partialKeyModeInt {
		gs := c.intGroupStates[gi]
		return appendTypedIntKey(buf, gs.intKey, c.groupColTypes[0])
	}
	// partialKeyModeDualInt
	buf = appendTypedIntKey(buf, c.dualIntKeysA[gi], c.groupColTypes[0])
	buf = append(buf, 0)
	return appendTypedIntKey(buf, c.dualIntKeysB[gi], c.groupColTypes[1])
}

// appendTypedIntKey writes one int-mode column value to buf using the same
// text encoding appendKeyValue produces for the corresponding boxed `any`.
// The shared encoding makes this byte-identical to the prior boxed path.
func appendTypedIntKey(buf []byte, v int64, t batch.TypeID) []byte {
	switch t {
	case batch.TypeBool:
		return strconv.AppendBool(buf, v != 0)
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return strconv.AppendInt(buf, int64(int32(v)), 10)
	default:
		// int64 + every wider-or-equal type stored as int64 (timestamp,
		// ipv4, mac, duration, ...). reifyIntKey returns int64(v) for these,
		// and appendKeyValue's int64 branch is strconv.AppendInt(buf, tv, 10).
		return strconv.AppendInt(buf, v, 10)
	}
}

// Peek implements partialRunSource.Peek. Returns nil when exhausted.
func (c *partialGroupCursor) Peek() *partialGroup {
	if c.pos >= c.numGroups {
		return nil
	}
	return &c.head
}

// Advance implements partialRunSource.Advance. Idempotent at exhaustion.
func (c *partialGroupCursor) Advance() error {
	if c.pos >= c.numGroups {
		return nil
	}
	c.pos++
	if c.pos < c.numGroups {
		c.loadHeadAt(int(c.sortedIdx[c.pos]))
	}
	return nil
}

// Close implements partialRunSource.Close. Drops the cursor's references to
// the source SoA arrays so they (and the group-state chunks they alias into)
// can be reclaimed by the next GC. Idempotent.
func (c *partialGroupCursor) Close() error {
	c.flatAccs = nil
	c.intGroupStates = nil
	c.strGroupStates = nil
	c.dualIntKeysA = nil
	c.dualIntKeysB = nil
	c.groupColTypes = nil
	c.liveGroups = nil
	c.keyArena = nil
	c.keyOffsets = nil
	c.sortedIdx = nil
	c.head = partialGroup{}
	c.headKeyVals = nil
	c.headAccs = nil
	c.pos = 0
	c.numGroups = 0
	return nil
}
