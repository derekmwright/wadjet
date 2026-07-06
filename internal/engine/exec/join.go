package exec

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// JoinType identifies the kind of join.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullOuterJoin
	CrossJoin
	SemiJoin      // returns left row if match found, no duplicates
	AntiJoin      // returns left row only if NO match found
	RightSemiJoin // builds LEFT (small), probes RIGHT (large), returns matched build rows
	RightAntiJoin // builds LEFT (small), probes RIGHT (large), returns unmatched build rows
)

// buildRef is a pointer to a single row in the columnar build-side storage.
type buildRef struct {
	batchIdx int32
	rowIdx   int32
}

// HashJoin implements a hash join with build and probe phases.
// Build side is stored in columnar RecordBatches, indexed by a hash map
// of join keys to batch/row references. This avoids the ~10x memory overhead
// of storing build-side rows as map[string]any.
type HashJoin struct {
	JoinType  JoinType
	LeftKeys  []string // join key columns from left (probe) side
	RightKeys []string // join key columns from right (build) side

	mu            sync.Mutex
	buildBatches  []*batch.RecordBatch // columnar storage of build side
	strIndex      *strHashTable        // arena-based hash table for string keys (general path)
	intIndex      *intHashTable        // fast path: single-column integer join key
	arena         []buildRef           // flat storage for all build refs
	arenaNext     []int32              // chain: arenaNext[i] = next arena index for same key (-1 = end)
	useIntKey     bool                 // true when single int32/int64 join key detected
	useDualIntKey bool                 // true when exactly two int32/int64 join keys
	buildDone     bool
	buildSchema   []parquet.Column
	buildRows     int64 // total rows in build side

	// Memory tracking (optional). When set, Reserve() is called for each
	// build-side batch. If the budget is exceeded, Build returns ErrMemoryExceeded.
	MemTracker *memory.Tracker

	// Spill-to-disk (optional). When set, build-side batches are spilled to disk
	// when memory pressure exceeds 80% of budget using Grace Hash Join partitioning.
	Spill *memory.SpillManager

	// arenaMatched tracks which build-side arena entries have been matched during
	// probing. Only allocated for RightJoin and FullOuterJoin.
	arenaMatched []bool

	// SemiAntiFilter is an optional predicate applied during semi/anti join probe.
	// When set, each candidate build row is checked in addition to hash key equality.
	// This enables non-equality join conditions (e.g., "!=") from decorrelated EXISTS.
	SemiAntiFilter func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool

	// BuildTableAlias is the table alias of the build side. When set, duplicate
	// column names in the output schema are qualified as "alias.column" to avoid
	// ambiguity (e.g., self-joins like nation n1 JOIN nation n2).
	BuildTableAlias string

	// QualifyAllBuildCols forces every build-side column into the output
	// schema under its qualified name ("BuildTableAlias.col"), not just the
	// columns that would collide with the probe schema. The planner sets
	// this on co-pathing self-join scans so the FIRST self-join's column
	// is reachable downstream by its qualified name (avoiding the NULL-via-
	// columnIndexFallback path that breaks Q07).
	QualifyAllBuildCols bool

	// Pre-resolved column indices and reusable key buffer for typed serialization.
	// Avoids fmt.Sprint + GetValue boxing on every row.
	keyBuf        []byte
	buildKeyIdx   []int // column indices for RightKeys in build batches
	probeKeyIdx   []int // column indices for LeftKeys in probe batches
	probeResolved atomic.Bool

	// SemiAntiKeyOnly enables a lightweight build for semi/anti joins that have
	// no SemiAntiFilter. Only the key index and bloom filter are built — batch
	// storage and arena refs are skipped. Reduces memory and build time by ~2-4x
	// for large build sides (e.g., 6M-row lineitem scan for EXISTS subqueries).
	SemiAntiKeyOnly bool

	// BuildStoreCols, when non-empty, narrows every stored build batch to the
	// named columns (join keys + SemiAntiFilter-referenced columns) at arrival
	// time. Filtered semi/anti joins never emit build rows, but the probe must
	// evaluate SemiAntiFilter against the filter's build-side columns — storing
	// only keys + those columns keeps partitioned builds, their per-partition
	// accumulators, and their spill files narrow from the first batch. The
	// post-build PruneBuildColumns cannot achieve this: it skips partition-on-
	// arrival builds entirely (evicted entries are nil'd and spilled files
	// carry the storage schema), which is every spill-eligible build. Names
	// are resolved once against the first arrival batch; if any name fails to
	// resolve, projection is disabled and full batches are stored.
	BuildStoreCols []string

	// buildStore* hold the once-resolved projection state for BuildStoreCols.
	buildStoreChecked  bool
	buildStoreDisabled bool
	buildStoreIdx      []int
	buildStoreSchema   []parquet.Column

	// BuildRowHint is an optional hint for the expected number of build-side rows.
	// When set, the arena and hash table are pre-allocated to avoid repeated growth.
	BuildRowHint int64

	// Bloom filter for fast negative lookups during probe phase.
	// When the build side is small relative to expected probe volume,
	// this rejects non-matching probe rows without touching the hash table.
	bloom     []uint64
	bloomMask uint64

	// Dynamic filter: min/max of build-side join key column(s).
	// Collected during Build() for range-based row-group pruning on the probe side.
	buildKeyMin []any
	buildKeyMax []any

	// trackedMem tracks how much memory THIS join has reserved from the shared
	// MemTracker. Used during spill to release only this join's contribution
	// rather than resetting the entire shared tracker (which would wipe other
	// concurrent builds' accounting).
	trackedMem int64

	// AccountedOperator (Phase 2) state — see HashAggregate for the contract.
	// Registered during Build only (the spill-eligible phase); accState is read
	// by Inspect off the pipeline goroutine.
	accInstanceID uint64
	accState      atomic.Int32

	// trackedHashOverhead tracks how much hash table overhead has been charged
	// to the memory tracker via EstimateBatchBytes (40 bytes/row). When the
	// actual hash table grows beyond this (e.g., string arenas, grow() doubling),
	// the delta is reserved so spill triggers at the right threshold.
	trackedHashOverhead int64

	// Grace Hash Join spill state. Non-nil when build-side data has been
	// partitioned and spilled to disk due to memory pressure.
	spillState *spillState

	// spillOutputFilter and spillLeftSchema are captured during the first
	// probe Execute() so spilled partition processing can reproduce the
	// output schema. Only set when spillState is non-nil.
	spillOutputFilter map[string]bool
	spillLeftSchema   []parquet.Column
}

// BloomPushdownOp returns a UnaryOperator that pre-filters probe batches using
// the build-side bloom filter. Must be called after Build() completes.
// Returns nil if bloom filter pushdown is not applicable (empty build, wrong
// join type). Safe for InnerJoin, SemiJoin, and RightJoin only.
func (h *HashJoin) BloomPushdownOp() *BloomFilterOp {
	if h.bloom == nil {
		return nil
	}
	// Only safe for join types where non-matching probe rows produce no output.
	// LEFT/FULL OUTER: must preserve all probe rows (with NULLs for no match).
	// ANTI: returns rows that don't match — bloom rejection would be inverted.
	switch h.JoinType {
	case InnerJoin, SemiJoin, RightJoin:
		// safe
	default:
		return nil
	}
	return &BloomFilterOp{
		bloom:         h.bloom,
		bloomMask:     h.bloomMask,
		leftKeys:      h.LeftKeys,
		useIntKey:     h.useIntKey,
		useDualIntKey: h.useDualIntKey,
	}
}

// DynamicRange holds min/max values for a probe-side join key column,
// collected during build. Used for row-group-level scan pruning.
type DynamicRange struct {
	Column   string // probe-side join key column name
	MinValue any
	MaxValue any
}

// BuildKeyRange returns the min/max range of each build-side join key column.
// Column names are mapped to probe-side names (LeftKeys) since the scan knows
// its own columns. Must be called after Build() completes. Returns nil if no
// rows were built or if the join type doesn't support range pushdown.
func (h *HashJoin) BuildKeyRange() []DynamicRange {
	switch h.JoinType {
	case InnerJoin, SemiJoin, RightJoin:
		// safe — non-matching probe rows produce no output
	default:
		return nil
	}
	if h.buildRows == 0 || len(h.buildKeyMin) == 0 {
		return nil
	}
	ranges := make([]DynamicRange, 0, len(h.LeftKeys))
	for i, col := range h.LeftKeys {
		if i < len(h.buildKeyMin) && h.buildKeyMin[i] != nil {
			ranges = append(ranges, DynamicRange{
				Column:   col,
				MinValue: h.buildKeyMin[i],
				MaxValue: h.buildKeyMax[i],
			})
		}
	}
	if len(ranges) == 0 {
		return nil
	}
	return ranges
}

// updateKeyMinMax updates the running min/max for each build-side key column
// from the given batch. Called under h.mu.Lock during Build().
func (h *HashJoin) updateKeyMinMax(b *batch.RecordBatch) {
	if len(h.buildKeyIdx) == 0 {
		return
	}
	if h.buildKeyMin == nil {
		h.buildKeyMin = make([]any, len(h.buildKeyIdx))
		h.buildKeyMax = make([]any, len(h.buildKeyIdx))
	}
	for ki, colIdx := range h.buildKeyIdx {
		if colIdx < 0 || colIdx >= len(b.Columns) {
			continue
		}
		col := b.Columns[colIdx]
		iterRows := func(fn func(row int)) {
			if b.Sel != nil {
				for _, si := range b.Sel {
					fn(int(si))
				}
			} else {
				for i := 0; i < b.Len; i++ {
					fn(i)
				}
			}
		}
		switch col.Type {
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			var lo, hi int64
			first := true
			iterRows(func(row int) {
				if col.Nulls.IsNull(row) {
					return
				}
				v := int64(col.Int32Data[row])
				if first {
					lo, hi = v, v
					first = false
				} else {
					if v < lo {
						lo = v
					}
					if v > hi {
						hi = v
					}
				}
			})
			if !first {
				if h.buildKeyMin[ki] == nil {
					h.buildKeyMin[ki] = lo
					h.buildKeyMax[ki] = hi
				} else {
					prev := h.buildKeyMin[ki].(int64)
					if lo < prev {
						h.buildKeyMin[ki] = lo
					}
					prev = h.buildKeyMax[ki].(int64)
					if hi > prev {
						h.buildKeyMax[ki] = hi
					}
				}
			}
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			var lo, hi int64
			first := true
			iterRows(func(row int) {
				if col.Nulls.IsNull(row) {
					return
				}
				v := col.Int64Data[row]
				if first {
					lo, hi = v, v
					first = false
				} else {
					if v < lo {
						lo = v
					}
					if v > hi {
						hi = v
					}
				}
			})
			if !first {
				if h.buildKeyMin[ki] == nil {
					h.buildKeyMin[ki] = lo
					h.buildKeyMax[ki] = hi
				} else {
					prev := h.buildKeyMin[ki].(int64)
					if lo < prev {
						h.buildKeyMin[ki] = lo
					}
					prev = h.buildKeyMax[ki].(int64)
					if hi > prev {
						h.buildKeyMax[ki] = hi
					}
				}
			}
		case batch.TypeFloat64:
			var lo, hi float64
			first := true
			iterRows(func(row int) {
				if col.Nulls.IsNull(row) {
					return
				}
				v := col.Float64Data[row]
				if first {
					lo, hi = v, v
					first = false
				} else {
					if v < lo {
						lo = v
					}
					if v > hi {
						hi = v
					}
				}
			})
			if !first {
				if h.buildKeyMin[ki] == nil {
					h.buildKeyMin[ki] = lo
					h.buildKeyMax[ki] = hi
				} else {
					prev := h.buildKeyMin[ki].(float64)
					if lo < prev {
						h.buildKeyMin[ki] = lo
					}
					prev = h.buildKeyMax[ki].(float64)
					if hi > prev {
						h.buildKeyMax[ki] = hi
					}
				}
			}
		case batch.TypeString:
			var lo, hi string
			first := true
			iterRows(func(row int) {
				if col.Nulls.IsNull(row) {
					return
				}
				v := col.GetValue(row)
				s, ok := v.(string)
				if !ok {
					return
				}
				if first {
					lo, hi = s, s
					first = false
				} else {
					if s < lo {
						lo = s
					}
					if s > hi {
						hi = s
					}
				}
			})
			if !first {
				if h.buildKeyMin[ki] == nil {
					h.buildKeyMin[ki] = lo
					h.buildKeyMax[ki] = hi
				} else {
					prev := h.buildKeyMin[ki].(string)
					if lo < prev {
						h.buildKeyMin[ki] = lo
					}
					prev = h.buildKeyMax[ki].(string)
					if hi > prev {
						h.buildKeyMax[ki] = hi
					}
				}
			}
		}
	}
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(joinType JoinType, leftKeys, rightKeys []string) *HashJoin {
	hj := &HashJoin{
		JoinType:  joinType,
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
		keyBuf:    make([]byte, 0, 128),
	}
	return hj
}

// isIntKeyColumn returns true if the column type supports the int64 hash fast path.
func isIntKeyColumn(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol,
		batch.TypeDate, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC,
		batch.TypeDuration:
		return true
	}
	return false
}

// intKeyFromVector extracts the int64 value from an integer-typed vector at row.
func intKeyFromVector(v *batch.Vector, row int) (int64, bool) {
	if v.Nulls.IsNullFast(row) {
		return 0, false
	}
	switch v.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row]), true
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return v.Int64Data[row], true
	}
	return 0, false
}

// tryEnableIntKey checks if the join uses a single integer column and enables
// the int64 hash fast path, avoiding string allocation per build/probe row.
// sizeHint is used to pre-size the hash table (0 = default 64).
func (h *HashJoin) tryEnableIntKey(b *batch.RecordBatch) {
	hint := 64
	if h.BuildRowHint > 0 {
		hint = int(h.BuildRowHint)
	}
	if len(h.buildKeyIdx) == 1 && h.buildKeyIdx[0] >= 0 {
		col := b.Columns[h.buildKeyIdx[0]]
		if isIntKeyColumn(col.Type) {
			h.useIntKey = true
			h.intIndex = newIntHashTable(hint)
			h.strIndex = nil
			return
		}
	}
	// Two-int-key fast path: exactly 2 integer key columns.
	// Uses composite hash in intHashTable with equality verification
	// during chain traversal. Avoids string serialization + map[string] overhead.
	if len(h.buildKeyIdx) == 2 && h.buildKeyIdx[0] >= 0 && h.buildKeyIdx[1] >= 0 {
		col0 := b.Columns[h.buildKeyIdx[0]]
		col1 := b.Columns[h.buildKeyIdx[1]]
		if isIntKeyColumn(col0.Type) && isIntKeyColumn(col1.Type) {
			h.useDualIntKey = true
			h.intIndex = newIntHashTable(hint)
			h.strIndex = nil
		}
	}
}

// arenaAppendInt adds a buildRef to the arena and chains it under an int64 key.
// Uses PutNoGrow to defer growth checks — caller must call intIndex.CheckGrow()
// after each batch to maintain the load factor invariant.
func (h *HashJoin) arenaAppendInt(key int64, ref buildRef) {
	idx := int32(len(h.arena))
	h.arena = append(h.arena, ref)
	old, existed := h.intIndex.PutNoGrow(key, idx)
	if existed {
		h.arenaNext = append(h.arenaNext, old)
	} else {
		h.arenaNext = append(h.arenaNext, -1)
	}
}

func (h *HashJoin) arenaAppendStr(ref buildRef) {
	idx := int32(len(h.arena))
	h.arena = append(h.arena, ref)
	head, existed := h.strIndex.PutNoGrow(h.keyBuf, idx)
	if existed {
		h.arenaNext = append(h.arenaNext, head)
	} else {
		h.arenaNext = append(h.arenaNext, -1)
	}
}

// existsInBuild checks if a probe row has any match in the build-side hash table.
// Unlike lookupBuild, it returns immediately on finding the first match — no list
// construction. Used by semi/anti joins when no SemiAntiFilter is set.
func (p *HashJoinProbe) existsInBuild(in *batch.RecordBatch, row int) bool {
	h := p.join
	if h.useIntKey {
		key, ok := h.intProbeKey(in, row)
		if !ok {
			return false
		}
		if h.bloom != nil && !h.bloomMayContain(bloomHashInt(key)) {
			return false
		}
		_, ok = h.intIndex.Get(key)
		return ok
	}
	if h.useDualIntKey {
		h.resolveProbeKeyIdx(in)
		col0, col1 := in.Columns[h.probeKeyIdx[0]], in.Columns[h.probeKeyIdx[1]]
		a, b, ok := dualIntKeyFromVectors(col0, col1, row)
		if !ok {
			return false
		}
		compositeKey := dualIntHash(a, b)
		if h.bloom != nil && !h.bloomMayContain(bloomHashInt(compositeKey)) {
			return false
		}
		_, ok = h.intIndex.Get(compositeKey)
		return ok
	}
	if h.strIndex == nil {
		return false
	}
	p.buildProbeKey(in, row)
	if h.bloom != nil && !h.bloomMayContain(bloomHashBytes(p.keyBuf)) {
		return false
	}
	_, ok := h.strIndex.Get(p.keyBuf)
	return ok
}

// lookupBuild collects build refs for a probe row into the probe's reusable buffer.
// Uses bloom filter to skip hash table lookups for definite non-matches.
func (p *HashJoinProbe) lookupBuild(in *batch.RecordBatch, row int) []buildRef {
	h := p.join
	p.lookupBuf = p.lookupBuf[:0]
	if h.useIntKey {
		key, ok := h.intProbeKey(in, row)
		if !ok {
			return p.lookupBuf
		}
		if h.bloom != nil && !h.bloomMayContain(bloomHashInt(key)) {
			return p.lookupBuf
		}
		head, ok := h.intIndex.Get(key)
		if !ok {
			return p.lookupBuf
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			p.lookupBuf = append(p.lookupBuf, h.arena[idx])
		}
		return p.lookupBuf
	}
	if h.useDualIntKey {
		h.resolveProbeKeyIdx(in)
		col0, col1 := in.Columns[h.probeKeyIdx[0]], in.Columns[h.probeKeyIdx[1]]
		a, b, ok := dualIntKeyFromVectors(col0, col1, row)
		if !ok {
			return p.lookupBuf
		}
		compositeKey := dualIntHash(a, b)
		if h.bloom != nil && !h.bloomMayContain(bloomHashInt(compositeKey)) {
			return p.lookupBuf
		}
		head, ok := h.intIndex.Get(compositeKey)
		if !ok {
			return p.lookupBuf
		}
		// Traverse chain, verifying both keys match (composite hash may collide)
		bcol0, bcol1 := h.buildBatches[0].Columns[h.buildKeyIdx[0]], h.buildBatches[0].Columns[h.buildKeyIdx[1]]
		prevBatch := int32(0)
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			ref := h.arena[idx]
			if ref.batchIdx != prevBatch {
				bcol0 = h.buildBatches[ref.batchIdx].Columns[h.buildKeyIdx[0]]
				bcol1 = h.buildBatches[ref.batchIdx].Columns[h.buildKeyIdx[1]]
				prevBatch = ref.batchIdx
			}
			ba, bb, _ := dualIntKeyFromVectors(bcol0, bcol1, int(ref.rowIdx))
			if ba == a && bb == b {
				p.lookupBuf = append(p.lookupBuf, ref)
			}
		}
		return p.lookupBuf
	}
	if h.strIndex == nil {
		return p.lookupBuf
	}
	p.buildProbeKey(in, row)
	if h.bloom != nil && !h.bloomMayContain(bloomHashBytes(p.keyBuf)) {
		return p.lookupBuf
	}
	head, ok := h.strIndex.Get(p.keyBuf)
	if !ok {
		return p.lookupBuf
	}
	for idx := head; idx >= 0; idx = h.arenaNext[idx] {
		p.lookupBuf = append(p.lookupBuf, h.arena[idx])
	}
	return p.lookupBuf
}

// TrackedMem returns how much memory this join has reserved from the shared tracker.
func (h *HashJoin) TrackedMem() int64 { return h.trackedMem }

// SpillState returns the spill state (nil if no spill has occurred). Test-only.
func (h *HashJoin) SpillState() *spillState { return h.spillState }

// intProbeKey extracts the int64 probe key for the int fast path.
func (h *HashJoin) intProbeKey(in *batch.RecordBatch, row int) (int64, bool) {
	h.resolveProbeKeyIdx(in)
	if h.probeKeyIdx[0] < 0 {
		return 0, false
	}
	return intKeyFromVector(in.Columns[h.probeKeyIdx[0]], row)
}

// hashTableOverhead returns the actual heap bytes consumed by the hash table
// index structures (entries, arenas, chains, bloom). This excludes build-side
// batch data, which is tracked separately.
func (h *HashJoin) hashTableOverhead() int64 {
	var size int64
	if h.intIndex != nil {
		size += h.intIndex.MemoryUsage()
	}
	if h.strIndex != nil {
		size += h.strIndex.MemoryUsage()
	}
	size += int64(cap(h.arena)) * 8     // buildRef = 8 bytes
	size += int64(cap(h.arenaNext)) * 4 // int32 = 4 bytes
	size += int64(cap(h.arenaMatched))  // bool = 1 byte
	size += int64(len(h.bloom)) * 8     // uint64 = 8 bytes
	return size
}

// reconcileHashMemory checks if the hash table has grown beyond what
// EstimateBatchBytes charged (40 bytes/row) and reserves the delta.
// Called periodically during Build to keep the tracker accurate.
func (h *HashJoin) reconcileHashMemory() {
	if h.MemTracker == nil {
		return
	}
	actual := h.hashTableOverhead()
	if actual > h.trackedHashOverhead {
		delta := actual - h.trackedHashOverhead
		h.MemTracker.ForceReserve(delta) // always track; triggers ShouldSpill sooner
		h.trackedHashOverhead = actual
		h.trackedMem += delta
		// Publish the true owned footprint for the drift-backstop. keyBuf is
		// scratch we also own; fold it in so OwnedTotal is honest.
		if h.accInstanceID != 0 {
			h.MemTracker.PublishOwned(h.accInstanceID, h.trackedMem+int64(cap(h.keyBuf)))
		}
	}
}

// hashBuildBytes is the HashJoin build-side reservation: the batch's honest
// column footprint (RecordBatch.MemBytes) plus the ~40 B/row hash-index charge
// (key string + buildRef + map bucket). Only HashJoin build paths add the hash
// charge — every other operator reserves b.MemBytes() directly. This replaces
// the old EstimateBatchBytes, whose per-type estimate (notably b.Len*48 for
// string columns) is now subsumed by the byte-true MemBytes accounting.
func hashBuildBytes(b *batch.RecordBatch) int64 {
	return b.MemBytes() + int64(b.Len)*40
}

// Build consumes all rows from the build (right) side into the columnar hash table.
// Uses parallel workers when the build side is large enough to benefit from
// concurrent hash table construction with per-worker local tables.
func (h *HashJoin) Build(ctx context.Context, source Source) error {
	if err := source.Init(ctx); err != nil {
		return fmt.Errorf("build source init: %w", err)
	}
	defer source.Close()

	workers := runtime.NumCPU()

	// Key-only builds (semi/anti joins without filter) store no batches —
	// only the key index and bloom filter. Parallel is safe since there is
	// no merge overhead (each worker inserts directly into the shared table).
	if workers > 1 && h.SemiAntiKeyOnly {
		return h.buildParallelKeyOnly(ctx, source, workers)
	}

	// Cooperative-spill registration: when the join has both a Spill manager
	// and a MemTracker (so it dispatches to buildPartitioned below), register as
	// a Spillable so the worker's SpillManager.RequestRelief can target this
	// operator's in-memory partitions when another task hits Reserve failure.
	if h.Spill != nil && h.MemTracker != nil && !h.SemiAntiKeyOnly {
		// Register with the relief registry for the duration of Build (the
		// spill-eligible phase).
		h.accInstanceID = memory.NextInstanceID()
		h.accState.Store(int32(memory.OpActive))
		unregisterAccounted := h.Spill.RegisterAccounted(h)
		defer func() {
			h.accState.Store(int32(memory.OpClosed))
			unregisterAccounted()
		}()
	}

	// Spill-eligible builds (MemTracker + Spill both configured — the
	// production worker config) always partition on arrival. Spill is then
	// O(partition) instead of O(total): each pressure event evicts one
	// partition rather than freezing, repartitioning, and rebuilding the whole
	// flat state. There is no at-entry pressure heuristic — partitioning is
	// unconditional for spill-eligible builds, so a build that mispredicts its
	// pressure can never get stuck on a flat path with no cheap spill (the
	// Q17/Q18 mc=4 failure mode). The flat path below runs only for callers
	// without a tracker/spill (embedded queries, tests, spill-replay rebuilds),
	// where it is a pure in-memory build that never spills.
	if h.Spill != nil && h.MemTracker != nil && !h.SemiAntiKeyOnly {
		return h.buildPartitioned(ctx, source)
	}

	// No-spill flat build: serial insertion. A per-worker parallel build was
	// removed in the flat-path retirement — its local-table merge re-inserted
	// every key and cost as much as the parallel insertion saved (~9s/60M rows),
	// while serial is a tight ~0.6s/60M-row loop. (Spill-eligible builds use
	// buildPartitioned; semi/anti key-only builds use buildParallelKeyOnly.)

	// Report build-side row consumption to the per-task progress reporter.
	// Without this, a long broadcast_join build phase (minutes for SF10
	// lineitem) emits no rows-processed signal — the per-task heartbeat
	// goroutine (worker.go) doesn't publish TaskProgress messages, AckWait
	// extensions stop after the InProgress cap, and the coord's
	// multi-signal liveness check (PR #78) has nothing to fall back on
	// when the worker's global heartbeat goroutine is GC-starved. Result:
	// the build task hot-potatoes across workers (observed 2026-04-29 PM
	// SF10 deploy of 3b57e93 — task 3455f719 reaped 3× in 3 minutes).
	progress := ProgressReporterFromContext(ctx)

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("join build cancelled: %w", err)
		}

		b, err := source.Next(ctx)
		if err != nil {
			return fmt.Errorf("build source next: %w", err)
		}
		if b == nil {
			break
		}
		if progress != nil {
			progress.AddRows(int64(b.ActiveLen()))
		}

		h.mu.Lock()
		arrival := b
		b = h.projectForStore(b)
		if h.buildSchema == nil {
			h.buildSchema = b.Schema
			// Pre-resolve build key column indices. Use fallback to handle
			// schemas with qualified columns (self-join chain output).
			h.buildKeyIdx = make([]int, len(h.RightKeys))
			for i, col := range h.RightKeys {
				h.buildKeyIdx[i] = columnIndexFallback(b, col)
			}
			// Try to enable int64 fast path for single-column integer keys
			h.tryEnableIntKey(b)

			// Pre-allocate arena and index to avoid repeated slice growth.
			if h.BuildRowHint > 0 && !h.SemiAntiKeyOnly {
				hint := int(h.BuildRowHint)
				h.arena = make([]buildRef, 0, hint)
				h.arenaNext = make([]int32, 0, hint)
				// intIndex already pre-sized by tryEnableIntKey; only pre-size string table
				if !h.useIntKey && !h.useDualIntKey {
					h.strIndex = newStrHashTable(hint)
				}
			}
		}

		if h.SemiAntiKeyOnly {
			// Collect min/max even for key-only builds.
			h.updateKeyMinMax(b)
			// Key-only build: populate index without storing batches or arena refs.
			// Semi/anti joins only need key existence, not row data.
			// Uses PutNoGrow/GetOrInsertNoGrow for deferred growth — one CheckGrow
			// per batch instead of per row.
			batchRows := b.ActiveLen()
			if h.useIntKey || h.useDualIntKey {
				h.intIndex.EnsureCapacity(batchRows)
			} else if h.strIndex != nil {
				h.strIndex.EnsureCapacity(batchRows)
			}
			if h.useIntKey {
				col := b.Columns[h.buildKeyIdx[0]]
				if b.Sel != nil {
					for _, si := range b.Sel {
						key, ok := intKeyFromVector(col, int(si))
						if !ok {
							continue
						}
						h.intIndex.PutNoGrow(key, 0)
					}
				} else if !col.Nulls.HasNulls() {
					switch col.Type {
					case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
						data := col.Int32Data
						for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
							h.intIndex.PutNoGrow(int64(data[rowIdx]), 0)
						}
					default:
						data := col.Int64Data
						for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
							h.intIndex.PutNoGrow(data[rowIdx], 0)
						}
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						key, ok := intKeyFromVector(col, rowIdx)
						if !ok {
							continue
						}
						h.intIndex.PutNoGrow(key, 0)
					}
				}
				h.intIndex.CheckGrow()
			} else if h.useDualIntKey {
				col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
				if b.Sel != nil {
					for _, si := range b.Sel {
						a, bb, ok := dualIntKeyFromVectors(col0, col1, int(si))
						if !ok {
							continue
						}
						h.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
						if !ok {
							continue
						}
						h.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
					}
				}
				h.intIndex.CheckGrow()
			} else {
				if h.strIndex == nil {
					h.strIndex = newStrHashTable(64)
				}
				if b.Sel != nil {
					for _, si := range b.Sel {
						h.buildKeyFromBatch(b, int(si))
						h.strIndex.GetOrInsertNoGrow(h.keyBuf, 0)
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						h.buildKeyFromBatch(b, rowIdx)
						h.strIndex.GetOrInsertNoGrow(h.keyBuf, 0)
					}
				}
				h.strIndex.CheckGrow()
			}
			h.buildRows += int64(b.ActiveLen())
			h.mu.Unlock()
			continue
		}

		// Track memory if a budget is set. This flat path runs only for callers
		// without a configured Spill manager (embedded queries, tests, the
		// spill-replay tmpJoin rebuild) — see Build's dispatch above — so a
		// Reserve failure has no spill recourse and fails loudly rather than
		// silently over-committing. Spill-eligible builds use buildPartitioned.
		if h.MemTracker != nil {
			cost := hashBuildBytes(b)
			if err := h.MemTracker.Reserve(cost); err != nil {
				h.mu.Unlock()
				return fmt.Errorf("hash join build (no spill configured): %w (build_rows=%d, batches=%d)",
					err, h.buildRows, len(h.buildBatches))
			}
			h.trackedMem += cost
		}

		// Collect min/max for dynamic filter pushdown before key indexing.
		h.updateKeyMinMax(b)

		// Skip Compact() — iterate through Sel (if any) directly.
		// Avoids copying entire batch just to remove selection vector gaps.
		// Arena refs store original row indices, which are valid for direct access.
		// Detach the ARRIVAL batch: b may be a projectForStore view sharing its
		// vectors, and a pooled arrival recycled under the view would corrupt
		// the stored build data. When projection is off, arrival == b.
		arrival.Detach() // prevent pooled batches from being recycled — build stores references
		batchIdx := int32(len(h.buildBatches))
		h.buildBatches = append(h.buildBatches, b)

		// Pre-grow hash table for this batch so PutNoGrow won't overflow.
		batchRows := b.ActiveLen()
		if h.useIntKey || h.useDualIntKey {
			h.intIndex.EnsureCapacity(batchRows)
		} else if h.strIndex != nil {
			h.strIndex.EnsureCapacity(batchRows)
		}

		if h.useIntKey {
			col := b.Columns[h.buildKeyIdx[0]]
			if b.Sel != nil {
				for _, si := range b.Sel {
					key, ok := intKeyFromVector(col, int(si))
					if !ok {
						continue
					}
					h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					h.buildRows++
				}
			} else if !col.Nulls.HasNulls() {
				// Null-free: inline typed data access, skip null checks
				switch col.Type {
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					data := col.Int32Data
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						h.arenaAppendInt(int64(data[rowIdx]), buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					}
				default:
					data := col.Int64Data
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						h.arenaAppendInt(data[rowIdx], buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					}
				}
				h.buildRows += int64(b.Len)
			} else {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					key, ok := intKeyFromVector(col, rowIdx)
					if !ok {
						continue
					}
					h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					h.buildRows++
				}
			}
		} else if h.useDualIntKey {
			col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
			if b.Sel != nil {
				for _, si := range b.Sel {
					a, bb, ok := dualIntKeyFromVectors(col0, col1, int(si))
					if !ok {
						continue
					}
					h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					h.buildRows++
				}
			} else {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
					if !ok {
						continue
					}
					h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					h.buildRows++
				}
			}
		} else {
			if h.strIndex == nil {
				// Pre-size to this batch's row count so PutNoGrow has room
				// for the inserts that follow. The earlier EnsureCapacity
				// branch only fires when strIndex is non-nil; if we entered
				// with strIndex==nil and seeded it at 64 buckets, PutNoGrow
				// would loop forever once load exceeds 100%. Surfaced by
				// TestPartitionOnArrival_StringKeySpill once pressure-aware
				// routing started sending string-key builds through the
				// legacy flat path under low pool pressure.
				h.strIndex = newStrHashTable(b.ActiveLen())
			}
			if b.Sel != nil {
				for _, si := range b.Sel {
					h.buildKeyFromBatch(b, int(si))
					h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					h.buildRows++
				}
			} else {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.buildKeyFromBatch(b, rowIdx)
					h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					h.buildRows++
				}
			}
		}
		// Deferred growth check: PutNoGrow skips per-row growth checks for
		// inlineability. One check per batch (2048 rows) instead of per row.
		if h.useIntKey || h.useDualIntKey {
			h.intIndex.CheckGrow()
		} else if h.strIndex != nil {
			h.strIndex.CheckGrow()
		}

		// Reconcile hash table memory: grow() may have doubled the entries
		// array, consuming much more than EstimateBatchBytes predicted.
		h.reconcileHashMemory()

		h.mu.Unlock()
	}

	// This flat path is no-spill (spillState is always nil here): spill-eligible
	// builds run buildPartitioned, which indexes per-partition on arrival.

	// Allocate matched bitmap for right/full outer join and right semi/anti tracking
	if (h.JoinType == RightJoin || h.JoinType == FullOuterJoin ||
		h.JoinType == RightSemiJoin || h.JoinType == RightAntiJoin) && len(h.arena) > 0 {
		h.arenaMatched = make([]bool, len(h.arena))
	}

	// Consolidate build batches into a single contiguous batch to eliminate
	// O(n log n) pair sorting during probe and improve gather cache locality.
	h.consolidateBuild()

	// Build bloom filter for fast negative lookups during probe.
	h.buildBloom()

	// Final reconciliation: bloom + arenaMatched allocations happened outside
	// the per-batch tracking loop. Charge them to the memory tracker.
	h.reconcileHashMemory()

	h.buildDone = true
	return nil
}

// buildParallelKeyOnly is a parallel build path for semi/anti joins that only
// need key existence (no batch storage, no arena). Each worker builds a local
// hash table, then tables are merged by inserting all keys into the main table.
// For Q21-style queries this parallelizes two 6M-row lineitem builds.
func (h *HashJoin) buildParallelKeyOnly(ctx context.Context, source Source, workers int) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("join build cancelled: %w", err)
	}

	first, err := source.Next(ctx)
	if err != nil {
		return fmt.Errorf("build source next: %w", err)
	}
	if first == nil {
		h.buildDone = true
		return nil
	}

	h.buildSchema = first.Schema
	h.buildKeyIdx = make([]int, len(h.RightKeys))
	for i, col := range h.RightKeys {
		h.buildKeyIdx[i] = first.ColumnIndex(col)
	}
	h.tryEnableIntKey(first)

	// Per-worker local hash tables (key-only, no arena/batch storage).
	hint := 64
	if h.BuildRowHint > 0 {
		hint = int(h.BuildRowHint) / workers
		if hint < 64 {
			hint = 64
		}
	}
	locals := make([]*localKeyBuild, workers)
	for i := range locals {
		lb := &localKeyBuild{keyBuf: make([]byte, 0, 128)}
		if h.useIntKey || h.useDualIntKey {
			lb.intIndex = newIntHashTable(hint)
		} else {
			lb.strIndex = newStrHashTable(hint)
		}
		locals[i] = lb
	}

	// Insert first batch into worker 0.
	h.insertKeyOnlyBatch(locals[0], first)
	progress := ProgressReporterFromContext(ctx)
	if progress != nil {
		progress.AddRows(int64(first.ActiveLen()))
	}

	// Launch workers.
	var sourceMu sync.Mutex
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(lb *localKeyBuild) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				sourceMu.Lock()
				b, err := source.Next(ctx)
				if b != nil && b.Sel != nil {
					sel := make([]uint32, len(b.Sel))
					copy(sel, b.Sel)
					b.Sel = sel
				}
				sourceMu.Unlock()
				if err != nil {
					firstErr.CompareAndSwap(nil, fmt.Errorf("build source next: %w", err))
					return
				}
				if b == nil {
					return
				}
				if progress != nil {
					progress.AddRows(int64(b.ActiveLen()))
				}
				h.insertKeyOnlyBatch(lb, b)
			}
		}(locals[i])
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return v.(error)
	}

	// Merge: count total rows, pick largest local table as base, insert rest.
	var totalRows int64
	bestIdx := 0
	var bestSize int
	for i, lb := range locals {
		totalRows += lb.rows
		var sz int
		if h.useIntKey || h.useDualIntKey {
			sz = lb.intIndex.Len()
		} else if lb.strIndex != nil {
			sz = lb.strIndex.Len()
		}
		if sz > bestSize {
			bestSize = sz
			bestIdx = i
		}
	}
	h.buildRows = totalRows

	// Adopt the largest table directly, merge others into it.
	if h.useIntKey || h.useDualIntKey {
		h.intIndex = locals[bestIdx].intIndex
		for i, lb := range locals {
			if i == bestIdx {
				continue
			}
			lb.intIndex.ForEach(func(key int64, _ int32) {
				h.intIndex.Put(key, 0)
			})
		}
	} else {
		h.strIndex = locals[bestIdx].strIndex
		for i, lb := range locals {
			if i == bestIdx {
				continue
			}
			if lb.strIndex != nil {
				lb.strIndex.ForEach(func(key []byte) {
					h.strIndex.GetOrInsert(key, 0)
				})
			}
		}
	}

	h.buildBloom()
	h.buildDone = true
	return nil
}

// localKeyBuild is a per-worker accumulator for parallel key-only hash join build.
type localKeyBuild struct {
	intIndex *intHashTable
	strIndex *strHashTable
	rows     int64
	keyBuf   []byte
}

// insertKeyOnlyBatch inserts keys from a batch into a local key-only hash table.
// Uses PutNoGrow/GetOrInsertNoGrow for deferred growth — one CheckGrow per batch.
func (h *HashJoin) insertKeyOnlyBatch(lk *localKeyBuild, b *batch.RecordBatch) {
	batchRows := b.ActiveLen()
	if h.useIntKey || h.useDualIntKey {
		lk.intIndex.EnsureCapacity(batchRows)
	} else if lk.strIndex != nil {
		lk.strIndex.EnsureCapacity(batchRows)
	}
	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				key, ok := intKeyFromVector(col, int(si))
				if !ok {
					continue
				}
				lk.intIndex.PutNoGrow(key, 0)
			}
		} else if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					lk.intIndex.PutNoGrow(int64(data[rowIdx]), 0)
				}
			default:
				data := col.Int64Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					lk.intIndex.PutNoGrow(data[rowIdx], 0)
				}
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					continue
				}
				lk.intIndex.PutNoGrow(key, 0)
			}
		}
		lk.intIndex.CheckGrow()
	} else if h.useDualIntKey {
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, int(si))
				if !ok {
					continue
				}
				lk.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
					continue
				}
				lk.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
			}
		}
		lk.intIndex.CheckGrow()
	} else {
		if lk.strIndex == nil {
			lk.strIndex = newStrHashTable(64)
		}
		if b.Sel != nil {
			for _, si := range b.Sel {
				lk.keyBuf = lk.keyBuf[:0]
				for _, idx := range h.buildKeyIdx {
					if idx < 0 {
						lk.keyBuf = append(lk.keyBuf, 1)
						continue
					}
					v := b.Columns[idx]
					if v.Nulls.IsNullFast(int(si)) {
						lk.keyBuf = append(lk.keyBuf, 1)
					} else {
						lk.keyBuf = append(lk.keyBuf, 0)
						lk.keyBuf = appendColumnValue(lk.keyBuf, v, int(si), v.Type)
					}
				}
				lk.strIndex.GetOrInsertNoGrow(lk.keyBuf, 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				lk.keyBuf = lk.keyBuf[:0]
				for _, idx := range h.buildKeyIdx {
					if idx < 0 {
						lk.keyBuf = append(lk.keyBuf, 1)
						continue
					}
					v := b.Columns[idx]
					if v.Nulls.IsNullFast(rowIdx) {
						lk.keyBuf = append(lk.keyBuf, 1)
					} else {
						lk.keyBuf = append(lk.keyBuf, 0)
						lk.keyBuf = appendColumnValue(lk.keyBuf, v, rowIdx, v.Type)
					}
				}
				lk.strIndex.GetOrInsertNoGrow(lk.keyBuf, 0)
			}
		}
		lk.strIndex.CheckGrow()
	}
	lk.rows += int64(b.ActiveLen())
}

// buildBloom populates the bloom filter from the build-side hash table keys.
// Uses a 64-bit-per-slot bloom with 2 hash functions. The filter size is
// chosen to give ~1% false positive rate for the number of distinct keys.
func (h *HashJoin) buildBloom() {
	var nKeys int
	if h.useIntKey || h.useDualIntKey {
		nKeys = h.intIndex.Len()
	} else if h.strIndex != nil {
		nKeys = h.strIndex.Len()
	}
	if nKeys == 0 {
		return
	}
	// Size: ~10 bits per key for ~1% FPR, rounded to power-of-2 uint64 slots.
	nBits := nKeys * 10
	nSlots := 1
	for nSlots*64 < nBits {
		nSlots *= 2
	}
	if nSlots < 8 {
		nSlots = 8
	}
	h.bloom = make([]uint64, nSlots)
	h.bloomMask = uint64(nSlots - 1)

	if h.useIntKey || h.useDualIntKey {
		h.intIndex.ForEach(func(key int64, _ int32) {
			h.bloomSet(bloomHashInt(key))
		})
	} else if h.strIndex != nil {
		h.strIndex.ForEach(func(key []byte) {
			h.bloomSet(bloomHashBytes(key))
		})
	}
}

// bloomSet marks the bloom filter for a given hash.
func (h *HashJoin) bloomSet(hash uint64) {
	// Two hash functions derived from the same hash (split high/low)
	h1 := hash & h.bloomMask
	h2 := (hash >> 17) & h.bloomMask
	b1 := hash & 63
	b2 := (hash >> 6) & 63
	h.bloom[h1] |= 1 << b1
	h.bloom[h2] |= 1 << b2
}

// bloomMayContain returns false if the key is definitely not in the build side.
func (h *HashJoin) bloomMayContain(hash uint64) bool {
	return bloomContains(h.bloom, h.bloomMask, hash)
}

// consolidateBuild merges all build batches into a single contiguous batch.
// This eliminates the O(n log n) pair sort in the probe phase (sort is skipped
// when len(buildBatches) == 1) and improves gatherBuildVector cache locality
// by removing per-pair batch switching. Cost: one-time O(n) copy during build —
// at exactly the moment the build is finishing and probe is about to begin,
// peak heap is doubled by the consolidation.
//
// Skip the consolidation under any of:
//   - Single batch already → no-op (legacy)
//   - Spill state attached → arena entries reference per-partition batches
//   - Semi/anti key-only → no batches to merge
//   - Shared pool > 30% used → the 2× spike is unsafe; pay the per-pair sort
//     cost in probe instead. Removes a class of OOM observed when concurrent
//     builds finish in close succession and each tries to consolidate.
func (h *HashJoin) consolidateBuild() {
	if len(h.buildBatches) <= 1 || h.spillState != nil || h.SemiAntiKeyOnly {
		return
	}
	if h.MemTracker != nil && h.MemTracker.Budget() > 0 {
		used := h.MemTracker.Used()
		if used*100 > h.MemTracker.Budget()*30 {
			return
		}
	}

	// Compute total rows and cumulative offsets per batch.
	// Copy ALL rows (including unselected) so arena rowIdx values remain valid
	// at their new offset positions.
	totalRows := 0
	for _, b := range h.buildBatches {
		totalRows += b.Len
	}

	// For large build sides, the O(n) copy cost exceeds the benefit of
	// eliminating the probe-phase pair sort. The break-even is ~2M rows
	// based on SF10 profiling (consolidateBuild was 7.8% of CPU time).
	if totalRows > 2_000_000 {
		return
	}

	totalRows = 0
	offsets := make([]int, len(h.buildBatches))
	for i, b := range h.buildBatches {
		offsets[i] = totalRows
		totalRows += b.Len
	}

	// Allocate the consolidated batch.
	consolidated := batch.NewRecordBatch(h.buildSchema, totalRows)
	consolidated.Len = totalRows

	// Copy data from each batch at its cumulative offset.
	for batchIdx, b := range h.buildBatches {
		off := offsets[batchIdx]
		for colIdx, src := range b.Columns {
			dst := consolidated.Columns[colIdx]
			// Null bitmap
			if src.Nulls.HasNulls() {
				for i := 0; i < b.Len; i++ {
					if src.Nulls.IsNullFast(i) {
						dst.Nulls.SetNull(off + i)
					}
				}
			}
			// Typed data
			switch dst.Type {
			case batch.TypeBool:
				copy(dst.BoolData[off:off+b.Len], src.BoolData[:b.Len])
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				copy(dst.Int32Data[off:off+b.Len], src.Int32Data[:b.Len])
			case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
				copy(dst.Int64Data[off:off+b.Len], src.Int64Data[:b.Len])
			case batch.TypeFloat32:
				copy(dst.Float32Data[off:off+b.Len], src.Float32Data[:b.Len])
			case batch.TypeFloat64:
				copy(dst.Float64Data[off:off+b.Len], src.Float64Data[:b.Len])
			case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
				dst.BytesData.BulkCopy(off, &src.BytesData, 0, b.Len)
			case batch.TypeDecimal:
				copy(dst.DecimalData.Data[off:off+b.Len], src.DecimalData.Data[:b.Len])
			}
		}
	}

	// Remap arena entries: all point to batch 0 with offset-adjusted row indices.
	for i := range h.arena {
		h.arena[i].rowIdx += int32(offsets[h.arena[i].batchIdx])
		h.arena[i].batchIdx = 0
	}

	h.buildBatches = []*batch.RecordBatch{consolidated}
}

// countingSortPairs sorts matchPairs by batchIdx using counting sort.
// O(n+k) where k = numBatches, much faster than O(n log n) comparison sort
// for the typical case of many pairs with few distinct batch indices.
// Uses buf as scratch space (caller manages reuse).
func countingSortPairs(pairs []matchPair, numBatches int, buf *[]matchPair) {
	n := len(pairs)
	if n <= 1 || numBatches <= 1 {
		return
	}

	// Count occurrences of each batchIdx.
	counts := make([]int, numBatches)
	for i := 0; i < n; i++ {
		counts[pairs[i].ref.batchIdx]++
	}

	// Prefix sums → scatter offsets.
	offset := 0
	for i := 0; i < numBatches; i++ {
		c := counts[i]
		counts[i] = offset
		offset += c
	}

	// Scatter into output buffer.
	if cap(*buf) < n {
		*buf = make([]matchPair, n)
	}
	out := (*buf)[:n]
	for i := 0; i < n; i++ {
		bi := pairs[i].ref.batchIdx
		out[counts[bi]] = pairs[i]
		counts[bi]++
	}

	copy(pairs, out)
}

// dualIntHash combines two int64 keys into a single int64 composite key
// for the intHashTable. Uses different golden-ratio multipliers to minimize
// collisions. Hash collisions are handled by chain traversal with exact
// key verification in the probe phase.
func dualIntHash(a, b int64) int64 {
	return int64(uint64(a)*0x9E3779B97F4A7C15 ^ uint64(b)*0x517CC1B727220A95)
}

// dualIntKeyFromVectors extracts two int64 values from two vectors at a given row.
func dualIntKeyFromVectors(v0, v1 *batch.Vector, row int) (int64, int64, bool) {
	if v0.Nulls.IsNullFast(row) || v1.Nulls.IsNullFast(row) {
		return 0, 0, false
	}
	var a, b int64
	switch v0.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		a = int64(v0.Int32Data[row])
	default:
		a = v0.Int64Data[row]
	}
	switch v1.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		b = int64(v1.Int32Data[row])
	default:
		b = v1.Int64Data[row]
	}
	return a, b, true
}

func bloomHashInt(key int64) uint64 {
	// Mix bits using a multiply-shift hash
	x := uint64(key) * 0x9E3779B97F4A7C15
	return x ^ (x >> 32)
}

func bloomHashBytes(key []byte) uint64 {
	// FNV-1a style hash
	h := uint64(14695981039346656037)
	for _, b := range key {
		h ^= uint64(b)
		h *= 16777619
	}
	return h ^ (h >> 32)
}

// projectForStore narrows an arrival build batch to BuildStoreCols before it
// is stored, partitioned, or spilled. The returned batch is a view sharing
// b's vectors (Sel and Len preserved), so buildRef row indices stay valid;
// callers that retain the view must Detach the original arrival batch (a
// pooled original could otherwise be recycled under the view). Resolution
// happens once, against the first batch, via the same columnIndexFallback
// the key index uses; an unresolvable name disables projection so behavior
// degrades to full-width storage. Caller must hold h.mu.
func (h *HashJoin) projectForStore(b *batch.RecordBatch) *batch.RecordBatch {
	if len(h.BuildStoreCols) == 0 || h.buildStoreDisabled {
		return b
	}
	if !h.buildStoreChecked {
		h.buildStoreChecked = true
		keep := make(map[int]bool, len(h.BuildStoreCols))
		for _, name := range h.BuildStoreCols {
			idx := columnIndexFallback(b, name)
			if idx < 0 {
				h.buildStoreDisabled = true
				return b
			}
			keep[idx] = true
		}
		if len(keep) == len(b.Columns) {
			h.buildStoreDisabled = true // nothing to drop
			return b
		}
		for i := range b.Schema {
			if keep[i] {
				h.buildStoreIdx = append(h.buildStoreIdx, i)
				h.buildStoreSchema = append(h.buildStoreSchema, b.Schema[i])
			}
		}
	}
	cols := make([]*batch.Vector, len(h.buildStoreIdx))
	for j, i := range h.buildStoreIdx {
		cols[j] = b.Columns[i]
	}
	return &batch.RecordBatch{Columns: cols, Schema: h.buildStoreSchema, Len: b.Len, Sel: b.Sel}
}

// PruneBuildColumns removes non-essential columns from the build-side batches.
// For SEMI/ANTI joins, the build side never appears in the output, so after
// the hash index is built we only need columns referenced by SemiAntiFilter.
// If keepCols is empty and no SemiAntiFilter is set, buildBatches are cleared.
func (h *HashJoin) PruneBuildColumns(keepCols []string) {
	// Partition-on-arrival builds are incompatible with pruning: evicted
	// partitions nil their buildBatches entries (dereferencing them here
	// was the SF10 standalone Q21 SIGSEGV, 2026-07-05), and a partition
	// restored from disk carries the UNPRUNED schema — pruning
	// buildSchema would desync it from the spilled data. The prune is
	// purely a memory optimization and the spill machinery already bounds
	// build memory, so skip it. Mirrors consolidateBuild's guard.
	if h.spillState != nil {
		return
	}
	if len(h.buildBatches) == 0 {
		return
	}
	if len(keepCols) == 0 {
		h.buildBatches = nil
		h.buildSchema = nil
		return
	}

	keep := make(map[string]bool, len(keepCols))
	for _, c := range keepCols {
		keep[c] = true
	}

	// Build new pruned schema
	var newSchema []parquet.Column
	var colIdx []int
	for i, col := range h.buildSchema {
		if keep[col.Name] {
			newSchema = append(newSchema, col)
			colIdx = append(colIdx, i)
		}
	}
	if len(newSchema) == len(h.buildSchema) {
		return // nothing to prune
	}

	h.buildSchema = newSchema
	for bi, b := range h.buildBatches {
		newCols := make([]*batch.Vector, len(colIdx))
		for j, idx := range colIdx {
			newCols[j] = b.Columns[idx]
		}
		h.buildBatches[bi] = &batch.RecordBatch{
			Columns: newCols,
			Schema:  newSchema,
			Len:     b.Len,
		}
	}
}

// BuildFromRows loads the build side directly from rows (used by tests and worker).
func (h *HashJoin) BuildFromRows(schema []parquet.Column, rows []map[string]any) {
	h.buildSchema = schema
	if len(rows) == 0 {
		h.buildDone = true
		return
	}
	b := batch.FromRows(schema, rows)
	// Resolve build key indices if not yet done
	if h.buildKeyIdx == nil {
		h.buildKeyIdx = make([]int, len(h.RightKeys))
		for i, col := range h.RightKeys {
			h.buildKeyIdx[i] = b.ColumnIndex(col)
		}
		// Pre-size hash table for all rows to avoid growth during PutNoGrow.
		if h.BuildRowHint == 0 {
			h.BuildRowHint = int64(len(rows))
		}
		h.tryEnableIntKey(b)
	}
	b.Detach() // prevent pooled batches from being recycled — build stores references
	batchIdx := int32(len(h.buildBatches))
	h.buildBatches = append(h.buildBatches, b)
	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		for i := 0; i < b.Len; i++ {
			key, ok := intKeyFromVector(col, i)
			if !ok {
				continue
			}
			h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
			h.buildRows++
		}
		h.intIndex.CheckGrow()
	} else {
		if h.strIndex == nil {
			h.strIndex = newStrHashTable(64)
		}
		for i := 0; i < b.Len; i++ {
			h.buildKeyFromBatch(b, i)
			h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
			h.buildRows++
		}
		h.strIndex.CheckGrow()
	}
	if (h.JoinType == RightJoin || h.JoinType == FullOuterJoin) && len(h.arena) > 0 {
		h.arenaMatched = make([]bool, len(h.arena))
	}
	h.buildBloom()
	h.buildDone = true
}

// BuildRows returns the number of rows in the build side.
func (h *HashJoin) BuildRows() int64 {
	return h.buildRows
}

// FixKeyAssignment corrects misassigned join keys after the build phase.
// SQL may place the build-side column on the left of "=" (e.g., JOIN t ON t.id = src.id),
// causing parseJoinKeys to assign it as a left/probe key. This detects and swaps
// misassigned pairs by checking which keys exist in the build schema.
func (h *HashJoin) FixKeyAssignment() {
	if h.buildSchema == nil || len(h.LeftKeys) == 0 {
		return
	}
	buildCols := make(map[string]bool, len(h.buildSchema))
	for _, col := range h.buildSchema {
		buildCols[col.Name] = true
	}

	needsRebuild := false
	for i := range h.LeftKeys {
		leftInBuild := buildCols[h.LeftKeys[i]]
		rightInBuild := buildCols[h.RightKeys[i]]
		// If left key is in build but right key is not, swap them
		if leftInBuild && !rightInBuild {
			h.LeftKeys[i], h.RightKeys[i] = h.RightKeys[i], h.LeftKeys[i]
			needsRebuild = true
			h.probeResolved.Store(false) // force re-resolution of probe key indices
		}
	}

	// Rebuild hash index if keys were swapped
	if needsRebuild {
		// A build that EVICTED partitions cannot rebuild from buildBatches:
		// eviction nils their entries (dereferencing them here is the same
		// crash class as PruneBuildColumns, the SF10 standalone Q21 SIGSEGV
		// of 2026-07-05) and the evicted rows aren't in memory at all, so a
		// rebuild would silently drop them from the index. The key SWAP
		// above stands — probe-side resolution needs the corrected names —
		// and the arrival-time index stays authoritative: buildPartitioned
		// resolved buildKeyIdx through columnIndexFallback, which maps the
		// misassigned name to the same physical build column (had it
		// resolved to nothing, the build itself would have failed).
		// NOTE: the guard keys on nil'd entries, not on spillState —
		// buildPartitioned creates spillState for every spill-eligible
		// build, and a partitioned build with nothing evicted has complete
		// buildBatches and still relies on this rebuild (Q02's scalar-
		// subquery join at SF0.01 breaks if it's skipped).
		if h.spillState != nil {
			for _, b := range h.buildBatches {
				if b == nil {
					return
				}
			}
		}
		// Re-resolve build key indices after swap
		if len(h.buildBatches) > 0 {
			b := h.buildBatches[0]
			h.buildKeyIdx = make([]int, len(h.RightKeys))
			for i, col := range h.RightKeys {
				h.buildKeyIdx[i] = b.ColumnIndex(col)
			}
			// Re-check int key eligibility with new key assignment
			h.useIntKey = false
			h.useDualIntKey = false
			h.tryEnableIntKey(b)
		}
		h.buildRows = 0
		// Count total rows across build batches for pre-sizing
		totalBuildRows := 0
		for _, b := range h.buildBatches {
			totalBuildRows += b.Len
		}
		// Pre-allocate arena and arenaNext to avoid slice growth during build
		if cap(h.arena) < totalBuildRows {
			h.arena = make([]buildRef, 0, totalBuildRows)
		} else {
			h.arena = h.arena[:0]
		}
		if cap(h.arenaNext) < totalBuildRows {
			h.arenaNext = make([]int32, 0, totalBuildRows)
		} else {
			h.arenaNext = h.arenaNext[:0]
		}
		if h.useIntKey {
			h.intIndex = newIntHashTable(totalBuildRows)
			for batchIdx, b := range h.buildBatches {
				col := b.Columns[h.buildKeyIdx[0]]
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					key, ok := intKeyFromVector(col, rowIdx)
					if !ok {
						continue
					}
					h.arenaAppendInt(key, buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
					h.buildRows++
				}
				h.intIndex.CheckGrow()
			}
		} else if h.useDualIntKey {
			h.intIndex = newIntHashTable(totalBuildRows)
			for batchIdx, b := range h.buildBatches {
				col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
					if !ok {
						continue
					}
					h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
					h.buildRows++
				}
				h.intIndex.CheckGrow()
			}
		} else {
			h.strIndex = newStrHashTable(totalBuildRows)
			for batchIdx, b := range h.buildBatches {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.buildKeyFromBatch(b, rowIdx)
					h.arenaAppendStr(buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
					h.buildRows++
				}
				h.strIndex.CheckGrow()
			}
		}

		// Rebuild bloom filter with corrected keys.
		h.bloom = nil
		h.buildBloom()
	}
}

// Probe is a UnaryOperator that probes the hash table for each input batch.
func (h *HashJoin) Probe() *HashJoinProbe {
	// Pre-allocate scratch buffers to avoid repeated slice growth during
	// parallel pipeline execution. Each clone gets its own buffers.
	// pairsBuf sized at 16x batch size to handle 1:N join fan-out without
	// growslice (avg ~4:1 for TPC-H lineitem→orders, with skew up to 8-10x).
	return &HashJoinProbe{
		join:     h,
		pairsBuf: make([]matchPair, 0, 16*batch.DefaultBatchSize),
		indexBuf: make([]int, 0, 16*batch.DefaultBatchSize),
		keyBuf:   make([]byte, 0, 128),
	}
}

// buildKeyFromBatch fills h.keyBuf with the serialized build-side key for a row.
// Uses pre-resolved column indices and a reusable buffer to avoid allocations.
func (h *HashJoin) buildKeyFromBatch(b *batch.RecordBatch, rowIdx int) {
	h.keyBuf = h.keyBuf[:0]
	for _, idx := range h.buildKeyIdx {
		if idx < 0 {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(rowIdx) {
			h.keyBuf = append(h.keyBuf, 1) // null flag
		} else {
			h.keyBuf = append(h.keyBuf, 0) // not-null flag
			h.keyBuf = appendColumnValue(h.keyBuf, v, rowIdx, v.Type)
		}
	}
}

// resolveProbeKeyIdx lazily resolves probe-side column indices.
// Safe for concurrent calls: probeResolved (atomic.Bool) provides the
// happens-before edge for the writes to probeKeyIdx, so workers in the
// parallel pipeline can hit this lazy path concurrently without racing.
func (h *HashJoin) resolveProbeKeyIdx(b *batch.RecordBatch) {
	if h.probeResolved.Load() {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.probeResolved.Load() {
		return // another goroutine resolved while we waited
	}
	h.probeKeyIdx = make([]int, len(h.LeftKeys))
	for i, col := range h.LeftKeys {
		h.probeKeyIdx[i] = columnIndexFallback(b, col)
	}
	h.probeResolved.Store(true)
}

// buildProbeKey fills p.keyBuf with the serialized probe key for a row.
// Uses the per-probe keyBuf to avoid races when multiple cloned probes
// execute in parallel.
func (p *HashJoinProbe) buildProbeKey(b *batch.RecordBatch, row int) {
	h := p.join
	h.resolveProbeKeyIdx(b)
	p.keyBuf = p.keyBuf[:0]
	for _, idx := range h.probeKeyIdx {
		if idx < 0 {
			p.keyBuf = append(p.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			p.keyBuf = append(p.keyBuf, 1) // null flag
		} else {
			p.keyBuf = append(p.keyBuf, 0) // not-null flag
			p.keyBuf = appendColumnValue(p.keyBuf, v, row, v.Type)
		}
	}
}

// joinOutputPoolSize is the capacity for join output batch pooling.
// 16x DefaultBatchSize handles most 1:N join outputs without fresh allocation,
// including skewed batches with high fan-out.
const joinOutputPoolSize = 16 * batch.DefaultBatchSize

// matchPair tracks a probe-build row match for output construction.
// probeRow is int32 (batch sizes ≤8192) to compact the struct from 24→16 bytes,
// reducing sort swap cost and improving cache utilization during gather.
type matchPair struct {
	probeRow int32
	ref      buildRef
	matched  bool
}

// HashJoinProbe is a UnaryOperator that probes the build-side hash table.
type HashJoinProbe struct {
	join          *HashJoin
	pairsBuf      []matchPair // reusable buffer to avoid per-batch allocation
	sortBuf       []matchPair // reusable buffer for counting sort scratch space
	semiSelBuf    []uint32    // reusable selection vector for semi/anti join output
	lookupBuf     []buildRef  // reusable buffer for lookupBuild results
	indexBuf      []int       // reusable buffer for probe-side gather indices
	buildIndexBuf []int       // reusable buffer for build-side gather indices
	keyBuf        []byte      // per-probe key serialization buffer (avoids race on shared h.keyBuf)

	// Cached output schema and column mapping (computed once on first batch)
	cachedSchema  []parquet.Column
	cachedMapping []outColSource

	// OutputFilter restricts which columns the probe materializes.
	// When set, only columns in this map appear in the output batch.
	// This avoids allocating and gathering unneeded intermediate columns
	// in multi-way join pipelines.
	OutputFilter map[string]bool

	// outPool caches output batches for reuse. Created on first Execute
	// when the output schema is known. Eliminates per-batch allocation
	// in multi-way join pipelines where intermediate batches are released
	// back to the pool after the next operator consumes them.
	outPool      *batch.BatchPool
	largeOutPool *batch.BatchPool // for outputs > DefaultBatchSize

	// Grace Hash Join flush state. Spilled partitions are processed fully
	// streaming: one partition at a time, one probe batch at a time, one
	// joined output batch at a time. Previous implementations accumulated
	// every output batch from every spilled partition into a single slice
	// before yielding any — for SF100 Q05 lineitem⋈orders that was tens of
	// GB of joined batches alive simultaneously.
	spillFlushInit     bool
	spillFlushPartIDs  []int               // ordered partition IDs to process
	spillFlushPartIdx  int                 // index of next partition to process
	spillFlushPrefetch chan preloadedBuild // pre-fetch channel for next partition's build data
	// Per-current-partition state.
	spillFlushTmpJoin     *HashJoin         // hash join built for the current partition
	spillFlushTmpProbe    *HashJoinProbe    // probe operator for the current partition
	spillFlushProbeFiles  []string          // probe spill files for the current partition
	spillFlushProbeFileIx int               // next file to open in spillFlushProbeFiles
	spillFlushReader      *spillBatchReader // open reader for the current probe file
	spillFlushDone        bool              // current partition's probe batches all consumed; emit unmatched/move on
}

func (p *HashJoinProbe) Init(_ context.Context) error {
	if !p.join.buildDone {
		return fmt.Errorf("hash join build phase not complete")
	}
	return nil
}

// markKeyMatched marks all arena entries for a probe row's key as matched.
// Must be called with h.mu held.
func (p *HashJoinProbe) markKeyMatched(in *batch.RecordBatch, row int) {
	h := p.join
	if h.useIntKey {
		key, ok := h.intProbeKey(in, row)
		if !ok {
			return
		}
		head, ok := h.intIndex.Get(key)
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	} else if h.strIndex != nil {
		p.buildProbeKey(in, row)
		head, ok := h.strIndex.Get(p.keyBuf)
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	}
}

func (p *HashJoinProbe) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if p.join.JoinType == CrossJoin {
		outSchema := p.outputSchema(in.Schema)
		return p.executeCrossJoin(in, outSchema)
	}

	// When Grace Hash Join is active, partition probe rows and only probe
	// in-memory partitions. Spilled-partition rows are buffered to disk.
	if p.join.spillState != nil && len(p.join.spillState.spilledParts) > 0 {
		// Capture probe schema for spilled partition processing
		if p.join.spillLeftSchema == nil {
			p.join.spillLeftSchema = in.Schema
			p.join.spillOutputFilter = p.OutputFilter
		}

		inMemSel, err := p.partitionProbeBatch(in)
		if err != nil {
			return nil, fmt.Errorf("partitioning probe batch: %w", err)
		}
		if len(inMemSel) == 0 {
			return nil, nil // all rows went to spilled partitions
		}
		// Set selection vector to only include in-memory partition rows
		in.Sel = inMemSel
	}

	if p.join.JoinType == SemiJoin || p.join.JoinType == AntiJoin {
		return p.executeSemiAntiJoin(in)
	}

	// RightSemiJoin/RightAntiJoin: probe marks matched build entries
	// but doesn't output probe rows. Matched/unmatched build rows are
	// emitted later via Next() after all probing completes.
	if p.join.JoinType == RightSemiJoin || p.join.JoinType == RightAntiJoin {
		p.markMatchedBuildEntries(in)
		return nil, nil // no output during probe phase
	}

	// Cache output schema and column mapping on first batch (avoids per-batch allocation)
	if p.cachedSchema == nil {
		p.cachedSchema, p.cachedMapping = p.outputSchemaWithMapping(in.Schema)
	}
	outSchema, mapping := p.cachedSchema, p.cachedMapping

	// Collect match pairs using reusable buffer
	pairs := p.pairsBuf[:0]

	h := p.join
	// Fast path: single int key inner join without right/full outer tracking.
	// Inlines hash table lookup + typed data access, eliminating 4 levels of
	// per-row function calls (probeRow → lookupBuild → intProbeKey → intKeyFromVector).
	if h.useIntKey && h.JoinType == InnerJoin && h.arenaMatched == nil {
		h.resolveProbeKeyIdx(in)
		if keyIdx := h.probeKeyIdx[0]; keyIdx >= 0 {
			pairs = p.inlineIntProbe(in.Columns[keyIdx], in, pairs)
		}
	} else if h.useDualIntKey && h.JoinType == InnerJoin && h.arenaMatched == nil {
		h.resolveProbeKeyIdx(in)
		if h.probeKeyIdx[0] >= 0 && h.probeKeyIdx[1] >= 0 {
			pairs = p.inlineDualIntProbe(in.Columns[h.probeKeyIdx[0]], in.Columns[h.probeKeyIdx[1]], in, pairs)
		}
	} else {
		probeRow := func(row int) {
			buildMatches := p.lookupBuild(in, row)

			if len(buildMatches) == 0 {
				if h.JoinType == LeftJoin || h.JoinType == FullOuterJoin {
					pairs = append(pairs, matchPair{probeRow: int32(row)})
				}
				return
			}

			for _, ref := range buildMatches {
				pairs = append(pairs, matchPair{probeRow: int32(row), ref: ref, matched: true})
			}

			if h.arenaMatched != nil {
				h.mu.Lock()
				p.markKeyMatched(in, row)
				h.mu.Unlock()
			}
		}

		if in.Sel != nil {
			for _, idx := range in.Sel {
				probeRow(int(idx))
			}
		} else {
			for i := 0; i < in.Len; i++ {
				probeRow(i)
			}
		}
	}
	p.pairsBuf = pairs // save grown slice for reuse

	if len(pairs) == 0 {
		return nil, nil
	}

	// Sort pairs by build batch index so gatherBuildVector accesses each
	// build batch's column vectors contiguously. The per-type gather loops
	// cache the current src vector and skip reload while batchIdx is unchanged;
	// grouping pairs by batch maximizes that cache hit rate and keeps the
	// underlying column data in L1/L2 across the entire run.
	// After consolidateBuild, len(buildBatches)==1 so this is typically skipped.
	if len(p.join.buildBatches) > 1 {
		countingSortPairs(pairs, len(p.join.buildBatches), &p.sortBuf)
	}

	// Build output batch using precomputed column source mapping.
	// Two-pool strategy: standard pool for ≤DefaultBatchSize (common case,
	// cache-friendly), large pool for oversized 1:N outputs (avoids fresh alloc).
	var out *batch.RecordBatch
	if len(pairs) <= batch.DefaultBatchSize {
		if p.outPool == nil {
			p.outPool = batch.NewBatchPool(outSchema, batch.DefaultBatchSize)
			p.outPool.PreWarm(2)
		}
		out = p.outPool.Get()
		// Pool.Get() calls Reset(batchSize) which already clears nulls for
		// all rows 0..batchSize-1. Only need to set the actual row count.
		out.Len = len(pairs)
		for _, col := range out.Columns {
			col.Len = len(pairs)
		}
	} else if len(pairs) <= joinOutputPoolSize {
		if p.largeOutPool == nil {
			p.largeOutPool = batch.NewBatchPool(outSchema, joinOutputPoolSize)
			p.largeOutPool.PreWarm(2)
		}
		out = p.largeOutPool.GetForSize(len(pairs))
		// GetForSize calls Reset(numRows) which already handles nulls.
		out.Len = len(pairs)
		for _, col := range out.Columns {
			col.Len = len(pairs)
		}
	} else {
		out = batch.NewRecordBatch(outSchema, len(pairs))
	}

	// Pre-extract probe row indices for bulk gather (packed int array
	// has better cache locality than reading from 16-byte matchPair stride).
	if cap(p.indexBuf) < len(pairs) {
		p.indexBuf = make([]int, len(pairs))
	}
	probeIndices := p.indexBuf[:len(pairs)]
	for i, pair := range pairs {
		probeIndices[i] = int(pair.probeRow)
	}

	allMatched := p.join.JoinType != LeftJoin && p.join.JoinType != FullOuterJoin

	// When build side is consolidated to a single batch and all rows are matched
	// (inner/right/semi/cross joins), extract build row indices into a flat array
	// and reuse gatherVector. This avoids iterating 16-byte matchPair structs
	// when only the 4-byte rowIdx is needed, improving cache utilization by 4x.
	useFastBuildGather := allMatched && len(p.join.buildBatches) == 1
	var buildIndices []int
	if useFastBuildGather {
		if cap(p.buildIndexBuf) < len(pairs) {
			p.buildIndexBuf = make([]int, len(pairs))
		}
		buildIndices = p.buildIndexBuf[:len(pairs)]
		for i, pair := range pairs {
			buildIndices[i] = int(pair.ref.rowIdx)
		}
	}

	for outColIdx, m := range mapping {
		dst := out.Columns[outColIdx]
		if m.fromProbe {
			gatherVector(dst, in.Columns[m.srcIdx], probeIndices)
		} else if useFastBuildGather {
			gatherVector(dst, p.join.buildBatches[0].Columns[m.srcIdx], buildIndices)
		} else {
			gatherBuildVector(dst, m.srcIdx, pairs, p.join.buildBatches, allMatched)
		}
	}

	return out, nil
}

// inlineIntProbe is the fast probe path for single int key inner joins.
// It inlines the hash table lookup with typed data access, eliminating
// per-row function call overhead from lookupBuild/intProbeKey/intKeyFromVector.
// The probe logic is fully inlined (no closure) to avoid heap allocation of
// the closure + captured pairs slice, which saves ~2.5GB of allocations at SF1.
func (p *HashJoinProbe) inlineIntProbe(keyCol *batch.Vector, in *batch.RecordBatch, pairs []matchPair) []matchPair {
	h := p.join
	arena := h.arena
	arenaNext := h.arenaNext
	idx := h.intIndex

	// Bloom filter is NOT checked here — it's already applied as a separate
	// BloomFilterOp in the pipeline (InnerJoin always gets one). Checking
	// it again inline would be redundant work.

	if in.Sel != nil {
		if !keyCol.Nulls.HasNulls() {
			switch keyCol.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := keyCol.Int32Data
				for _, si := range in.Sel {
					head, ok := idx.Get(int64(data[si]))
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						pairs = append(pairs, matchPair{probeRow: int32(si), ref: arena[ref], matched: true})
					}
				}
			default:
				data := keyCol.Int64Data
				for _, si := range in.Sel {
					head, ok := idx.Get(data[si])
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						pairs = append(pairs, matchPair{probeRow: int32(si), ref: arena[ref], matched: true})
					}
				}
			}
		} else {
			for _, si := range in.Sel {
				if keyCol.Nulls.IsNullFast(int(si)) {
					continue
				}
				key, ok := intKeyFromVector(keyCol, int(si))
				if !ok {
					continue
				}
				head, ok := idx.Get(key)
				if !ok {
					continue
				}
				for ref := head; ref >= 0; ref = arenaNext[ref] {
					pairs = append(pairs, matchPair{probeRow: int32(si), ref: arena[ref], matched: true})
				}
			}
		}
	} else {
		if !keyCol.Nulls.HasNulls() {
			switch keyCol.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := keyCol.Int32Data
				for i := 0; i < in.Len; i++ {
					head, ok := idx.Get(int64(data[i]))
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						pairs = append(pairs, matchPair{probeRow: int32(i), ref: arena[ref], matched: true})
					}
				}
			default:
				data := keyCol.Int64Data
				for i := 0; i < in.Len; i++ {
					head, ok := idx.Get(data[i])
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						pairs = append(pairs, matchPair{probeRow: int32(i), ref: arena[ref], matched: true})
					}
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if keyCol.Nulls.IsNullFast(i) {
					continue
				}
				key, ok := intKeyFromVector(keyCol, i)
				if !ok {
					continue
				}
				head, ok := idx.Get(key)
				if !ok {
					continue
				}
				for ref := head; ref >= 0; ref = arenaNext[ref] {
					pairs = append(pairs, matchPair{probeRow: int32(i), ref: arena[ref], matched: true})
				}
			}
		}
	}
	return pairs
}

// inlineDualIntProbe is the fast probe path for dual int key inner joins.
// Inlines composite hash computation + chain traversal with typed key verification,
// eliminating per-row lookupBuild/dualIntKeyFromVectors function call overhead.
func (p *HashJoinProbe) inlineDualIntProbe(col0, col1 *batch.Vector, in *batch.RecordBatch, pairs []matchPair) []matchPair {
	h := p.join
	arena := h.arena
	arenaNext := h.arenaNext
	idx := h.intIndex
	buildBatches := h.buildBatches
	bkIdx0, bkIdx1 := h.buildKeyIdx[0], h.buildKeyIdx[1]

	// Pre-extract typed probe data arrays (branch predictor handles per-loop dispatch).
	var pd0i32 []int32
	var pd0i64 []int64
	var pd1i32 []int32
	var pd1i64 []int64
	switch col0.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		pd0i32 = col0.Int32Data
	default:
		pd0i64 = col0.Int64Data
	}
	switch col1.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		pd1i32 = col1.Int32Data
	default:
		pd1i64 = col1.Int64Data
	}

	// Cache build-side typed arrays for chain verification (switch on batch boundary).
	var bd0i32 []int32
	var bd0i64 []int64
	var bd1i32 []int32
	var bd1i64 []int64
	prevBatch := int32(-1)

	switchBuild := func(batchIdx int32) {
		bc0 := buildBatches[batchIdx].Columns[bkIdx0]
		bc1 := buildBatches[batchIdx].Columns[bkIdx1]
		switch bc0.Type {
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			bd0i32 = bc0.Int32Data
			bd0i64 = nil
		default:
			bd0i64 = bc0.Int64Data
			bd0i32 = nil
		}
		switch bc1.Type {
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			bd1i32 = bc1.Int32Data
			bd1i64 = nil
		default:
			bd1i64 = bc1.Int64Data
			bd1i32 = nil
		}
		prevBatch = batchIdx
	}

	noNulls := !col0.Nulls.HasNulls() && !col1.Nulls.HasNulls()

	if in.Sel != nil {
		if noNulls {
			for _, si := range in.Sel {
				var a, b int64
				if pd0i32 != nil {
					a = int64(pd0i32[si])
				} else {
					a = pd0i64[si]
				}
				if pd1i32 != nil {
					b = int64(pd1i32[si])
				} else {
					b = pd1i64[si]
				}
				ck := dualIntHash(a, b)
				head, ok := idx.Get(ck)
				if !ok {
					continue
				}
				for ri := head; ri >= 0; ri = arenaNext[ri] {
					r := arena[ri]
					if r.batchIdx != prevBatch {
						switchBuild(r.batchIdx)
					}
					var ba, bb int64
					if bd0i32 != nil {
						ba = int64(bd0i32[r.rowIdx])
					} else {
						ba = bd0i64[r.rowIdx]
					}
					if bd1i32 != nil {
						bb = int64(bd1i32[r.rowIdx])
					} else {
						bb = bd1i64[r.rowIdx]
					}
					if ba == a && bb == b {
						pairs = append(pairs, matchPair{probeRow: int32(si), ref: r, matched: true})
					}
				}
			}
		} else {
			for _, si := range in.Sel {
				if col0.Nulls.IsNullFast(int(si)) || col1.Nulls.IsNullFast(int(si)) {
					continue
				}
				var a, b int64
				if pd0i32 != nil {
					a = int64(pd0i32[si])
				} else {
					a = pd0i64[si]
				}
				if pd1i32 != nil {
					b = int64(pd1i32[si])
				} else {
					b = pd1i64[si]
				}
				ck := dualIntHash(a, b)
				head, ok := idx.Get(ck)
				if !ok {
					continue
				}
				for ri := head; ri >= 0; ri = arenaNext[ri] {
					r := arena[ri]
					if r.batchIdx != prevBatch {
						switchBuild(r.batchIdx)
					}
					var ba, bb int64
					if bd0i32 != nil {
						ba = int64(bd0i32[r.rowIdx])
					} else {
						ba = bd0i64[r.rowIdx]
					}
					if bd1i32 != nil {
						bb = int64(bd1i32[r.rowIdx])
					} else {
						bb = bd1i64[r.rowIdx]
					}
					if ba == a && bb == b {
						pairs = append(pairs, matchPair{probeRow: int32(si), ref: r, matched: true})
					}
				}
			}
		}
	} else {
		if noNulls {
			for i := 0; i < in.Len; i++ {
				var a, b int64
				if pd0i32 != nil {
					a = int64(pd0i32[i])
				} else {
					a = pd0i64[i]
				}
				if pd1i32 != nil {
					b = int64(pd1i32[i])
				} else {
					b = pd1i64[i]
				}
				ck := dualIntHash(a, b)
				head, ok := idx.Get(ck)
				if !ok {
					continue
				}
				for ri := head; ri >= 0; ri = arenaNext[ri] {
					r := arena[ri]
					if r.batchIdx != prevBatch {
						switchBuild(r.batchIdx)
					}
					var ba, bb int64
					if bd0i32 != nil {
						ba = int64(bd0i32[r.rowIdx])
					} else {
						ba = bd0i64[r.rowIdx]
					}
					if bd1i32 != nil {
						bb = int64(bd1i32[r.rowIdx])
					} else {
						bb = bd1i64[r.rowIdx]
					}
					if ba == a && bb == b {
						pairs = append(pairs, matchPair{probeRow: int32(i), ref: r, matched: true})
					}
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if col0.Nulls.IsNullFast(i) || col1.Nulls.IsNullFast(i) {
					continue
				}
				var a, b int64
				if pd0i32 != nil {
					a = int64(pd0i32[i])
				} else {
					a = pd0i64[i]
				}
				if pd1i32 != nil {
					b = int64(pd1i32[i])
				} else {
					b = pd1i64[i]
				}
				ck := dualIntHash(a, b)
				head, ok := idx.Get(ck)
				if !ok {
					continue
				}
				for ri := head; ri >= 0; ri = arenaNext[ri] {
					r := arena[ri]
					if r.batchIdx != prevBatch {
						switchBuild(r.batchIdx)
					}
					var ba, bb int64
					if bd0i32 != nil {
						ba = int64(bd0i32[r.rowIdx])
					} else {
						ba = bd0i64[r.rowIdx]
					}
					if bd1i32 != nil {
						bb = int64(bd1i32[r.rowIdx])
					} else {
						bb = bd1i64[r.rowIdx]
					}
					if ba == a && bb == b {
						pairs = append(pairs, matchPair{probeRow: int32(i), ref: r, matched: true})
					}
				}
			}
		}
	}
	return pairs
}

// executeSemiAntiJoin handles SemiJoin and AntiJoin semantics.
// Uses a selection vector on the input batch to avoid copying rows.
func (p *HashJoinProbe) executeSemiAntiJoin(in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if cap(p.semiSelBuf) < in.Len {
		p.semiSelBuf = make([]uint32, 0, in.Len)
	}
	sel := p.semiSelBuf[:0]

	h := p.join
	isSemi := h.JoinType == SemiJoin
	hasFilter := h.SemiAntiFilter != nil

	// Fast path: int-key semi/anti without filter — fully inlined typed loops.
	// Eliminates closure overhead by splitting into typed branches outside the loop.
	// Each branch has a single comparison + hash lookup with no per-row branching
	// on type/null/bloom/semi-vs-anti. For Q21's 3 semi/anti joins at SF10,
	// this eliminates ~600K closure calls per batch.
	if h.useIntKey && !hasFilter {
		h.resolveProbeKeyIdx(in)
		keyIdx := h.probeKeyIdx[0]
		if keyIdx >= 0 {
			keyCol := in.Columns[keyIdx]
			hasNulls := keyCol.Nulls.HasNulls()
			isInt32 := keyCol.Type == batch.TypeInt32 || keyCol.Type == batch.TypePort ||
				keyCol.Type == batch.TypeProtocol || keyCol.Type == batch.TypeDate
			hasBloom := h.bloom != nil
			intIdx := h.intIndex

			// Dispatch to the tightest possible loop based on type/null/bloom/semi.
			// Common case first: int64, no nulls, no bloom.
			if !hasNulls && !hasBloom {
				if isSemi {
					if isInt32 {
						sel = semiProbeInt32(intIdx, keyCol.Int32Data, in.Sel, in.Len, sel)
					} else {
						sel = semiProbeInt64(intIdx, keyCol.Int64Data, in.Sel, in.Len, sel)
					}
				} else {
					if isInt32 {
						sel = antiProbeInt32(intIdx, keyCol.Int32Data, in.Sel, in.Len, sel)
					} else {
						sel = antiProbeInt64(intIdx, keyCol.Int64Data, in.Sel, in.Len, sel)
					}
				}
			} else {
				// Fallback for nulls or bloom: per-row checks needed
				checkIntRow := func(row int) {
					if hasNulls && keyCol.Nulls.IsNullFast(row) {
						if !isSemi {
							sel = append(sel, uint32(row))
						}
						return
					}
					var key int64
					if isInt32 {
						key = int64(keyCol.Int32Data[row])
					} else {
						key = keyCol.Int64Data[row]
					}
					if hasBloom && !h.bloomMayContain(bloomHashInt(key)) {
						if !isSemi {
							sel = append(sel, uint32(row))
						}
						return
					}
					_, exists := intIdx.Get(key)
					if (isSemi && exists) || (!isSemi && !exists) {
						sel = append(sel, uint32(row))
					}
				}
				if in.Sel != nil {
					for _, idx := range in.Sel {
						checkIntRow(int(idx))
					}
				} else {
					for i := 0; i < in.Len; i++ {
						checkIntRow(i)
					}
				}
			}
			goto done
		}
	}

	// Fast path: int-key semi/anti WITH filter — inline hash lookup + chain walk.
	// Avoids lookupBuild overhead and intermediate slice; breaks early on first
	// filter match instead of collecting all candidates.
	if h.useIntKey && hasFilter {
		h.resolveProbeKeyIdx(in)
		keyIdx := h.probeKeyIdx[0]
		if keyIdx >= 0 {
			keyCol := in.Columns[keyIdx]
			hasNulls := keyCol.Nulls.HasNulls()
			isInt32 := keyCol.Type == batch.TypeInt32 || keyCol.Type == batch.TypePort ||
				keyCol.Type == batch.TypeProtocol || keyCol.Type == batch.TypeDate
			hasBloom := h.bloom != nil

			// Pre-cache hash table internals for inline lookup
			htEntries := h.intIndex.entries
			htMask := h.intIndex.mask
			arena := h.arena
			arenaNext := h.arenaNext
			buildBatches := h.buildBatches
			filter := h.SemiAntiFilter

			checkRow := func(row int) {
				if hasNulls && keyCol.Nulls.IsNullFast(row) {
					if !isSemi {
						sel = append(sel, uint32(row))
					}
					return
				}
				var key int64
				if isInt32 {
					key = int64(keyCol.Int32Data[row])
				} else {
					key = keyCol.Int64Data[row]
				}
				if hasBloom && !h.bloomMayContain(bloomHashInt(key)) {
					if !isSemi {
						sel = append(sel, uint32(row))
					}
					return
				}
				// Inline intIndex.Get: fibHash + linear probe (AoS layout)
				htIdx := fibHash(key) & htMask
				for {
					e := &htEntries[htIdx]
					if e.key == intHashEmpty {
						// Key not in table — no match possible
						if !isSemi {
							sel = append(sel, uint32(row))
						}
						return
					}
					if e.key == key {
						// Key found — walk chain and evaluate filter, break on first match
						var hasMatch bool
						for ai := e.val; ai >= 0; ai = arenaNext[ai] {
							ref := arena[ai]
							if filter(in, row, buildBatches[ref.batchIdx], int(ref.rowIdx)) {
								hasMatch = true
								break
							}
						}
						emit := (isSemi && hasMatch) || (!isSemi && !hasMatch)
						if emit {
							sel = append(sel, uint32(row))
						}
						return
					}
					htIdx = (htIdx + 1) & htMask
				}
			}

			if in.Sel != nil {
				for _, idx := range in.Sel {
					checkRow(int(idx))
				}
			} else {
				for i := 0; i < in.Len; i++ {
					checkRow(i)
				}
			}
			goto done
		}
	}

	// General path: uses existence-only check when no filter is set
	{
		checkRow := func(row int) {
			var hasMatch bool
			if hasFilter {
				candidates := p.lookupBuild(in, row)
				if len(candidates) > 0 {
					for _, ref := range candidates {
						buildBatch := h.buildBatches[ref.batchIdx]
						if h.SemiAntiFilter(in, row, buildBatch, int(ref.rowIdx)) {
							hasMatch = true
							break
						}
					}
				}
			} else {
				hasMatch = p.existsInBuild(in, row)
			}
			emit := (isSemi && hasMatch) || (!isSemi && !hasMatch)
			if emit {
				sel = append(sel, uint32(row))
			}
		}

		if in.Sel != nil {
			for _, idx := range in.Sel {
				checkRow(int(idx))
			}
		} else {
			for i := 0; i < in.Len; i++ {
				checkRow(i)
			}
		}
	}

done:
	if len(sel) == 0 {
		return nil, nil
	}

	// Copy the selection vector so that reuse of semiSelBuf on the next
	// call doesn't corrupt this batch's Sel (they share the backing array).
	out := make([]uint32, len(sel))
	copy(out, sel)
	in.Sel = out
	return in, nil
}

// executeCrossJoin produces the Cartesian product of probe rows with all build-side rows.
// crossPair tracks a probe row matched to a build-side row in cross joins.
type crossPair struct {
	probeRow int
	batchIdx int32
	buildRow int
}

// markMatchedBuildEntries probes the input batch against the hash table and
// marks matching build-side entries in arenaMatched. Used by RightSemiJoin and
// RightAntiJoin where we don't output probe rows during probing — instead,
// matched/unmatched build rows are emitted after all probing completes.
func (p *HashJoinProbe) markMatchedBuildEntries(in *batch.RecordBatch) {
	h := p.join
	// Resolve probe key indices first (acquires h.mu internally)
	h.resolveProbeKeyIdx(in)
	// Then mark matches without locking — RightSemiJoin probe is single-threaded
	if in.Sel != nil {
		for _, idx := range in.Sel {
			p.markKeyMatchedLocked(in, int(idx))
		}
	} else {
		for i := 0; i < in.Len; i++ {
			p.markKeyMatchedLocked(in, i)
		}
	}
}

// markKeyMatchedLocked is markKeyMatched without locking — caller must hold h.mu.
func (p *HashJoinProbe) markKeyMatchedLocked(in *batch.RecordBatch, row int) {
	h := p.join
	if h.useIntKey {
		key, ok := h.intProbeKey(in, row)
		if !ok {
			return
		}
		head, ok := h.intIndex.Get(key)
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	} else if h.strIndex != nil {
		p.buildProbeKey(in, row)
		head, ok := h.strIndex.Get(p.keyBuf)
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	}
}

func (p *HashJoinProbe) executeCrossJoin(in *batch.RecordBatch, outSchema []parquet.Column) (*batch.RecordBatch, error) {
	_, mapping := p.outputSchemaWithMapping(in.Schema)

	var pairs []crossPair

	addProbeRow := func(row int) {
		for bi, buildBatch := range p.join.buildBatches {
			if buildBatch.Sel != nil {
				for _, bidx := range buildBatch.Sel {
					pairs = append(pairs, crossPair{probeRow: row, batchIdx: int32(bi), buildRow: int(bidx)})
				}
			} else {
				for br := 0; br < buildBatch.Len; br++ {
					pairs = append(pairs, crossPair{probeRow: row, batchIdx: int32(bi), buildRow: br})
				}
			}
		}
	}

	if in.Sel != nil {
		for _, idx := range in.Sel {
			addProbeRow(int(idx))
		}
	} else {
		for i := 0; i < in.Len; i++ {
			addProbeRow(i)
		}
	}

	if len(pairs) == 0 {
		return nil, nil
	}

	out := batch.NewRecordBatch(outSchema, len(pairs))

	// Pre-extract probe row indices for bulk gather
	if cap(p.indexBuf) < len(pairs) {
		p.indexBuf = make([]int, len(pairs))
	}
	crossProbeIdx := p.indexBuf[:len(pairs)]
	for i, cp := range pairs {
		crossProbeIdx[i] = int(cp.probeRow)
	}

	for outColIdx, m := range mapping {
		dst := out.Columns[outColIdx]
		if m.fromProbe {
			gatherVector(dst, in.Columns[m.srcIdx], crossProbeIdx)
		} else {
			gatherCrossBuildVector(dst, m.srcIdx, pairs, p.join.buildBatches)
		}
	}

	return out, nil
}

// FlushMatched returns a RecordBatch containing build-side rows that WERE
// matched during probing. For RightSemiJoin only. Returns build-side columns only.
func (p *HashJoinProbe) FlushMatched() *batch.RecordBatch {
	if p.join.JoinType != RightSemiJoin {
		return nil
	}

	var refs []buildRef
	for i, ref := range p.join.arena {
		if p.join.arenaMatched != nil && p.join.arenaMatched[i] {
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return nil
	}

	// Deduplicate: multiple arena entries can reference the same build row
	// (when multiple probe rows match the same build key). Use a seen set
	// keyed on (batchIdx, rowIdx) to avoid emitting duplicate build rows.
	seen := make(map[buildRef]bool, len(refs))
	var unique []buildRef
	for _, ref := range refs {
		if !seen[ref] {
			seen[ref] = true
			unique = append(unique, ref)
		}
	}
	refs = unique

	// Output only build-side columns
	out := batch.NewRecordBatch(p.join.buildSchema, len(refs))
	for colIdx := range p.join.buildSchema {
		dst := out.Columns[colIdx]
		for outRow, ref := range refs {
			if int(ref.batchIdx) >= len(p.join.buildBatches) {
				continue
			}
			buildBatch := p.join.buildBatches[ref.batchIdx]
			if int(ref.rowIdx) >= buildBatch.Len {
				continue
			}
			copyVectorValue(dst, outRow, buildBatch.Columns[colIdx], int(ref.rowIdx))
		}
	}
	return out
}

// FlushAntiMatched returns build-side rows that were NOT matched. For RightAntiJoin.
func (p *HashJoinProbe) FlushAntiMatched() *batch.RecordBatch {
	if p.join.JoinType != RightAntiJoin {
		return nil
	}

	var refs []buildRef
	for i, ref := range p.join.arena {
		if p.join.arenaMatched == nil || !p.join.arenaMatched[i] {
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return nil
	}

	// Deduplicate: multiple arena entries can reference the same build row.
	// Key on full (batchIdx, rowIdx) pair to avoid dropping rows from
	// different batches that happen to share the same rowIdx.
	seen := make(map[buildRef]bool, len(refs))
	var unique []buildRef
	for _, ref := range refs {
		if !seen[ref] {
			seen[ref] = true
			unique = append(unique, ref)
		}
	}
	refs = unique

	out := batch.NewRecordBatch(p.join.buildSchema, len(refs))
	for colIdx := range p.join.buildSchema {
		dst := out.Columns[colIdx]
		for outRow, ref := range refs {
			if int(ref.batchIdx) >= len(p.join.buildBatches) {
				continue
			}
			buildBatch := p.join.buildBatches[ref.batchIdx]
			if int(ref.rowIdx) >= buildBatch.Len {
				continue
			}
			copyVectorValue(dst, outRow, buildBatch.Columns[colIdx], int(ref.rowIdx))
		}
	}
	return out
}

// FlushUnmatched returns a RecordBatch containing build-side rows that were never
// matched during probing. For RightJoin and FullOuterJoin only.
func (p *HashJoinProbe) FlushUnmatched(leftSchema []parquet.Column) *batch.RecordBatch {
	if p.join.JoinType != RightJoin && p.join.JoinType != FullOuterJoin {
		return nil
	}

	// Collect unmatched build refs from arena
	var refs []buildRef
	for i, ref := range p.join.arena {
		if p.join.arenaMatched != nil && p.join.arenaMatched[i] {
			continue
		}
		refs = append(refs, ref)
	}

	if len(refs) == 0 {
		return nil
	}

	// Deduplicate: multiple arena entries can reference the same build row
	// (hash chain entries for duplicate keys). Key on full (batchIdx, rowIdx)
	// to avoid emitting duplicate unmatched rows.
	seen := make(map[buildRef]bool, len(refs))
	var unique []buildRef
	for _, ref := range refs {
		if !seen[ref] {
			seen[ref] = true
			unique = append(unique, ref)
		}
	}
	refs = unique

	outSchema, mapping := p.outputSchemaWithMapping(leftSchema)
	out := batch.NewRecordBatch(outSchema, len(refs))

	for outColIdx, m := range mapping {
		dst := out.Columns[outColIdx]
		if m.fromProbe {
			// Left side is all NULLs for unmatched build rows
			for outRow := range refs {
				setVectorNull(dst, outRow)
			}
		} else {
			for outRow, ref := range refs {
				buildBatch := p.join.buildBatches[ref.batchIdx]
				copyVectorValue(dst, outRow, buildBatch.Columns[m.srcIdx], int(ref.rowIdx))
			}
		}
	}

	return out
}

func (p *HashJoinProbe) Close() error { return nil }

// Close releases any memory still reserved with the shared MemTracker and
// drops references to the build-side state so Go's GC can reclaim it
// promptly. Must be called by the owner of the HashJoin (the worker
// executor) after the probe pipeline has fully drained — including any
// spilled-partition flushes. Calling Close more than once is safe; the
// release amount goes to zero on the first call.
//
// Without this, an operator that builds-probes-completes WITHOUT spilling
// never returns its reservation to the shared tracker. The hash table is
// GC-eligible but the tracker thinks it's still in use, so the worker
// reports inflated PoolPressure to the coordinator and worker-side spill
// thresholds fire prematurely. With many concurrent broadcast joins (e.g.,
// TPC-H Q02), phantom reservations accumulate query-over-query.
func (h *HashJoin) Close() error {
	if h.MemTracker != nil && h.trackedMem > 0 {
		h.MemTracker.Release(h.trackedMem)
		h.trackedMem = 0
	}
	h.trackedHashOverhead = 0
	h.buildBatches = nil
	h.arena = nil
	h.arenaNext = nil
	h.intIndex = nil
	h.strIndex = nil
	h.bloom = nil
	h.bloomMask = 0
	return nil
}

// Clone returns a new HashJoinProbe that shares the same build-side hash table
// but has its own scratch buffers (pairsBuf, semiSelBuf, lookupBuf, indexBuf).
func (p *HashJoinProbe) Clone() UnaryOperator {
	c := p.join.Probe()
	c.OutputFilter = p.OutputFilter
	return c
}

// outColSource tracks the source of each output column in the join result.
type outColSource struct {
	fromProbe bool // true = probe side, false = build side
	srcIdx    int  // column index in the source batch
}

func (p *HashJoinProbe) outputSchema(leftSchema []parquet.Column) []parquet.Column {
	schema, _ := p.outputSchemaWithMapping(leftSchema)
	return schema
}

func (p *HashJoinProbe) outputSchemaWithMapping(leftSchema []parquet.Column) ([]parquet.Column, []outColSource) {
	var out []parquet.Column
	var mapping []outColSource

	if p.join.JoinType == RightJoin || p.join.JoinType == FullOuterJoin {
		for i, col := range leftSchema {
			col.Nullable = true
			out = append(out, col)
			mapping = append(mapping, outColSource{fromProbe: true, srcIdx: i})
		}
	} else {
		for i, col := range leftSchema {
			out = append(out, col)
			mapping = append(mapping, outColSource{fromProbe: true, srcIdx: i})
		}
	}

	seen := make(map[string]bool, len(leftSchema))
	for _, col := range leftSchema {
		seen[col.Name] = true
	}

	for i, col := range p.join.buildSchema {
		isDup := seen[col.Name]
		// Force qualification for self-join scenarios — the planner sets
		// QualifyAllBuildCols when this build is one of two co-pathing
		// scans of the same source table. Without it, the FIRST scan's
		// column ships unqualified ("n_name") and downstream lookups for
		// the qualified name ("n1.n_name") miss.
		shouldQualify := (isDup || p.join.QualifyAllBuildCols) && p.join.BuildTableAlias != ""
		switch {
		case shouldQualify:
			qualCol := col
			qualCol.Name = p.join.BuildTableAlias + "." + col.Name
			if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
				qualCol.Nullable = true
			}
			out = append(out, qualCol)
			mapping = append(mapping, outColSource{fromProbe: false, srcIdx: i})
		case isDup:
			// Duplicate with no alias to disambiguate by — skip (backward compatible).
		default:
			if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
				col.Nullable = true
			}
			out = append(out, col)
			mapping = append(mapping, outColSource{fromProbe: false, srcIdx: i})
			seen[col.Name] = true
		}
	}

	// Apply output filter: skip columns not needed by downstream operators.
	// This avoids allocating and gathering unneeded intermediate columns
	// in multi-way join pipelines, reducing both CPU and memory pressure.
	if len(p.OutputFilter) > 0 {
		var filteredSchema []parquet.Column
		var filteredMapping []outColSource
		for i, col := range out {
			keep := p.OutputFilter[col.Name]
			// For qualified columns (e.g., "n2.n_name" from self-joins), also
			// check if the unqualified base name is needed. Without this, the
			// output filter would drop disambiguated self-join columns.
			if !keep {
				if dot := strings.IndexByte(col.Name, '.'); dot >= 0 {
					keep = p.OutputFilter[col.Name[dot+1:]]
				}
			}
			if keep {
				filteredSchema = append(filteredSchema, col)
				filteredMapping = append(filteredMapping, mapping[i])
			}
		}
		if len(filteredSchema) < len(out) {
			return filteredSchema, filteredMapping
		}
	}

	return out, mapping
}

func (p *HashJoinProbe) isRightJoinKey(name string) bool {
	for _, k := range p.join.RightKeys {
		if k == name {
			return true
		}
	}
	return false
}

func (p *HashJoinProbe) leftHasColumn(name string, leftSchema []parquet.Column) bool {
	for _, col := range leftSchema {
		if col.Name == name {
			return true
		}
	}
	return false
}

// gatherBuildVector copies build-side column values into the output vector for
// all match pairs. Hoists the type switch outside the loop, eliminating per-row
// function call and type dispatch overhead vs per-row copyVectorValue.
// Batch pointer caching and null-free fast paths are inlined (closures don't
// inline in Go when they capture mutable variables).
//
// When allMatched is true (inner/right joins), the per-row !pair.matched branch
// is skipped entirely, generating tighter loops with no null-for-unmatched logic.
func gatherBuildVector(dst *batch.Vector, srcIdx int, pairs []matchPair, buildBatches []*batch.RecordBatch, allMatched bool) {
	switch dst.Type {
	case batch.TypeBool:
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.BoolData[di] = src.BoolData[si]
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.BoolData[di] = src.BoolData[si]
				}
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int32Data[di] = src.Int32Data[si]
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int32Data[di] = src.Int32Data[si]
				}
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int64Data[di] = src.Int64Data[si]
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int64Data[di] = src.Int64Data[si]
				}
			}
		}
	case batch.TypeFloat32:
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float32Data[di] = src.Float32Data[si]
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float32Data[di] = src.Float32Data[si]
				}
			}
		}
	case batch.TypeFloat64:
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float64Data[di] = src.Float64Data[si]
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float64Data[di] = src.Float64Data[si]
				}
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		// Pre-calculate total byte size to avoid growslice in Set's append.
		{
			var src *batch.Vector
			prevBatch := int32(-1)
			totalBytes := 0
			for _, pair := range pairs {
				if !pair.matched {
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
				}
				si := int(pair.ref.rowIdx)
				totalBytes += int(src.BytesData.Offsets[si+1] - src.BytesData.Offsets[si])
			}
			dst.BytesData.PreAllocBytes(totalBytes)
		}
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		// Offsets carry-forward for skipped rows: when a row's value
		// isn't written (unmatched pair or null source), the
		// destination BytesColumn still needs Offsets[di+1] to be
		// monotonically non-decreasing so Value(i) returns a valid
		// (empty) slice rather than a descending pair that panics in
		// writeBytesData when the null bitmap isn't consulted.
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
					dst.BytesData.Offsets[di+1] = dst.BytesData.Offsets[di]
				} else {
					dst.BytesData.SetFrom(di, &src.BytesData, si)
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					dst.BytesData.Offsets[di+1] = dst.BytesData.Offsets[di]
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
					dst.BytesData.Offsets[di+1] = dst.BytesData.Offsets[di]
				} else {
					dst.BytesData.SetFrom(di, &src.BytesData, si)
				}
			}
		}
	case batch.TypeDecimal:
		var src *batch.Vector
		prevBatch := int32(-1)
		srcHasNulls := true
		if allMatched {
			for di, pair := range pairs {
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.DecimalData.Data[di] = src.DecimalData.Data[si]
				}
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					dst.Nulls.SetNull(di)
					continue
				}
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.DecimalData.Data[di] = src.DecimalData.Data[si]
				}
			}
		}
	default:
		if allMatched {
			for di, pair := range pairs {
				buildBatch := buildBatches[pair.ref.batchIdx]
				copyVectorValue(dst, di, buildBatch.Columns[srcIdx], int(pair.ref.rowIdx))
			}
		} else {
			for di, pair := range pairs {
				if !pair.matched {
					setVectorNull(dst, di)
				} else {
					buildBatch := buildBatches[pair.ref.batchIdx]
					copyVectorValue(dst, di, buildBatch.Columns[srcIdx], int(pair.ref.rowIdx))
				}
			}
		}
	}
}

// gatherCrossBuildVector is like gatherBuildVector but for cross join pairs
// where all rows are matched (no null handling for unmatched).
func gatherCrossBuildVector(dst *batch.Vector, srcIdx int, pairs []crossPair, buildBatches []*batch.RecordBatch) {
	switch dst.Type {
	case batch.TypeBool:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.BoolData[di] = src.BoolData[si]
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Int32Data[di] = src.Int32Data[si]
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Int64Data[di] = src.Int64Data[si]
			}
		}
	case batch.TypeFloat32:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Float32Data[di] = src.Float32Data[si]
			}
		}
	case batch.TypeFloat64:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Float64Data[di] = src.Float64Data[si]
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
				dst.BytesData.Set(di, nil)
			} else {
				dst.Nulls.SetValid(di)
				dst.BytesData.SetFrom(di, &src.BytesData, si)
			}
		}
	case batch.TypeDecimal:
		for di, cp := range pairs {
			src := buildBatches[cp.batchIdx].Columns[srcIdx]
			si := cp.buildRow
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.DecimalData.Data[di] = src.DecimalData.Data[si]
			}
		}
	default:
		for di, cp := range pairs {
			buildBatch := buildBatches[cp.batchIdx]
			copyVectorValue(dst, di, buildBatch.Columns[srcIdx], cp.buildRow)
		}
	}
}

// setVectorNull marks a position as null, handling BytesColumn offset alignment.
func setVectorNull(dst *batch.Vector, row int) {
	// WriteNullAt advances variable-length bookkeeping for every column
	// shape. The old inline switch covered only the flat bytes types, so an
	// unmatched outer-join row over a nested build column skipped its
	// offset/child slot and every later row read back shifted/concatenated.
	dst.WriteNullAt(row)
}

// semiProbeInt64 is the inlined semi-join probe for int64 keys without nulls or bloom.
// Emits rows whose key EXISTS in the hash table.
// Hash lookup is fully inlined (no idx.Get call) to eliminate per-row function overhead.
func semiProbeInt64(idx *intHashTable, data []int64, inSel []uint32, inLen int, sel []uint32) []uint32 {
	entries := idx.entries
	mask := idx.mask
	if inSel != nil {
		for _, si := range inSel {
			key := data[si]
			htIdx := fibHash(key) & mask
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					sel = append(sel, si)
					break
				}
				htIdx = (htIdx + 1) & mask
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			key := data[i]
			htIdx := fibHash(key) & mask
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					sel = append(sel, uint32(i))
					break
				}
				htIdx = (htIdx + 1) & mask
			}
		}
	}
	return sel
}

// semiProbeInt32 is the inlined semi-join probe for int32 keys without nulls or bloom.
func semiProbeInt32(idx *intHashTable, data []int32, inSel []uint32, inLen int, sel []uint32) []uint32 {
	entries := idx.entries
	mask := idx.mask
	if inSel != nil {
		for _, si := range inSel {
			key := int64(data[si])
			htIdx := fibHash(key) & mask
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					sel = append(sel, si)
					break
				}
				htIdx = (htIdx + 1) & mask
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			key := int64(data[i])
			htIdx := fibHash(key) & mask
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					sel = append(sel, uint32(i))
					break
				}
				htIdx = (htIdx + 1) & mask
			}
		}
	}
	return sel
}

// antiProbeInt64 is the inlined anti-join probe for int64 keys without nulls or bloom.
// Emits rows whose key does NOT exist in the hash table.
func antiProbeInt64(idx *intHashTable, data []int64, inSel []uint32, inLen int, sel []uint32) []uint32 {
	entries := idx.entries
	mask := idx.mask
	if inSel != nil {
		for _, si := range inSel {
			key := data[si]
			htIdx := fibHash(key) & mask
			found := false
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					found = true
					break
				}
				htIdx = (htIdx + 1) & mask
			}
			if !found {
				sel = append(sel, si)
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			key := data[i]
			htIdx := fibHash(key) & mask
			found := false
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					found = true
					break
				}
				htIdx = (htIdx + 1) & mask
			}
			if !found {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel
}

// antiProbeInt32 is the inlined anti-join probe for int32 keys without nulls or bloom.
func antiProbeInt32(idx *intHashTable, data []int32, inSel []uint32, inLen int, sel []uint32) []uint32 {
	entries := idx.entries
	mask := idx.mask
	if inSel != nil {
		for _, si := range inSel {
			key := int64(data[si])
			htIdx := fibHash(key) & mask
			found := false
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					found = true
					break
				}
				htIdx = (htIdx + 1) & mask
			}
			if !found {
				sel = append(sel, si)
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			key := int64(data[i])
			htIdx := fibHash(key) & mask
			found := false
			for {
				e := &entries[htIdx]
				if e.key == intHashEmpty {
					break
				}
				if e.key == key {
					found = true
					break
				}
				htIdx = (htIdx + 1) & mask
			}
			if !found {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel
}
