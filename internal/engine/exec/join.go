package exec

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

	// Residual is the ON-clause residual predicate of a LEFT, RIGHT or FULL
	// OUTER join (#358): every ON conjunct that is not an equi-join key,
	// evaluated on the COMBINED row (probe row + candidate build row) before a
	// key match is accepted. An outer join's ON runs BEFORE the NULL-padding,
	// so this cannot be a filter above the join: a probe row whose candidates
	// all fail the residual is UNMATCHED — a LEFT/FULL join still emits it
	// NULL-padded rather than dropping it — and a build row counts as matched
	// only when some probe row passed BOTH key and residual, which is what
	// FlushUnmatched consults for RIGHT/FULL. A residual returning false OR
	// NULL rejects the candidate (the compiled evaluator folds UNKNOWN to
	// false, which is the SQL ON semantics).
	//
	// With no join keys at all (`LEFT JOIN r ON n.x = r.y + 3` — no conjunct
	// is a bare-column equality) the build degenerates to a single empty-key
	// chain holding every build row, so each probe row's candidate set is the
	// whole build side and the residual does all of the work.
	Residual func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool

	// rowMatched tracks matched build rows per (batchIdx, rowIdx) when
	// Residual is active on a RIGHT/FULL join. arenaMatched cannot carry
	// this: markKeyMatched marks a whole key CHAIN, while a residual accepts
	// or rejects individual candidates of that chain. Allocated lazily on
	// first mark; guarded by mu like arenaMatched.
	rowMatched [][]bool

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

	// BuildColOrigins maps each bare build-column name (lowercased) to the
	// scan alias that owns it. Set by the planner only when the build side
	// spans multiple tables (bushy join subtrees); nil for single-scan
	// builds. Duplicate qualification then uses the owning alias instead of
	// the single BuildTableAlias, which is ambiguous for multi-table builds.
	BuildColOrigins map[string]string

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

	// NullAwareAnti marks an AntiJoin that must answer `NOT IN (subquery)`
	// rather than the two-valued question an anti join asks on its own.
	//
	// An anti join emits a probe row when nothing in the build matched it.
	// `k NOT IN (SELECT v …)` is three-valued: TRUE only when k differs from
	// EVERY v, FALSE when it equals one, and UNKNOWN — so WHERE drops the
	// row — the moment k itself is NULL, or the subquery yielded a NULL that
	// k did not match on some other value. A plain anti join cannot tell "no
	// match because the row genuinely differs" from "no match because NULL
	// equals nothing", and emits both (#507).
	//
	// Two rules restore the difference, and both are decided by the BUILD:
	// a probe row whose own key is NULL never survives, and a NULL anywhere
	// in the build poisons every non-matching comparison, so the join's whole
	// output is empty. See buildHasNullKey.
	NullAwareAnti bool

	// buildHasNullKey records that some build row's key was NULL. Set by
	// nullBuildKey on every build path — flat, key-only, parallel key-only,
	// partition-on-arrival and spill — and read only by a NullAwareAnti
	// join. It is a property of the build side this operator saw: under a
	// hash-partitioned exchange each worker sees one partition, so a
	// distributed null-aware anti join needs its build complete per worker
	// (broadcast/replicated), which is what the planner arranges.
	buildHasNullKey bool

	// SemiAntiNEProbeCol/SemiAntiNEBuildCol carry the planner-recognized
	// single-condition `probe.col <> build.col` join filter (the
	// decorrelated-EXISTS self-inequality class). When both are set on a
	// semi/anti join, Build collapses to a distinct-pair table — see
	// join_semianti_ne.go. SemiAntiFilter stays wired as the fallback for
	// shapes the runtime can't activate (non-int value column).
	SemiAntiNEProbeCol string
	SemiAntiNEBuildCol string
	neActive           bool
	neValIdx           int
	neValInt32         bool
	nePairs            []nePair
	neProbeValIdx      int

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

	// BuildSchemaHint / ProbeSchemaHint declare each side's output columns at
	// PLAN time, for the case where that side delivers no batch at all and the
	// runtime therefore never learns its schema.
	//
	// An outer join still owes rows when one side is empty — a LEFT JOIN emits
	// every probe row with the build columns NULL, a RIGHT/FULL JOIN emits
	// every build row with the probe columns NULL — and it cannot name those
	// columns without a schema. buildSchema left nil produced a join output
	// carrying ONLY the preserved side: the values still read as NULL through
	// the projection's missing-column fallback, but the column was ABSENT
	// rather than NULL, so `COUNT(o.o_orderstatus)` counted 1500 of them and
	// `WHERE r.r_regionkey IS NULL` matched none (#348).
	//
	// The hints are only consulted when the side produced nothing; a real
	// batch's schema always wins, so an imprecise hint cannot corrupt a
	// non-empty join.
	BuildSchemaHint []parquet.Column
	ProbeSchemaHint []parquet.Column

	// probeSchema is the probe input schema as the probe actually saw it,
	// recorded on the first Execute across all clones. FlushUnmatched needs
	// the PROBE-side schema to name the NULL half of an unmatched build row,
	// and its caller (the pipeline driver) only has the join's OUTPUT schema.
	probeSchema []parquet.Column

	// unmatchedFlushed guards the RIGHT/FULL unmatched-build-row emission so
	// it happens exactly once per join, whichever probe clone gets there
	// first. Every clone shares this HashJoin, and every clone's driver
	// drains FlushableOperator at the end.
	unmatchedFlushed bool

	// Bloom filter for fast negative lookups during probe phase.
	// When the build side is small relative to expected probe volume,
	// this rejects non-matching probe rows without touching the hash table.
	bloom     []uint64
	bloomMask uint64

	// Dynamic filter: min/max of build-side join key column(s).
	// Collected during Build() for range-based row-group pruning on the probe side.
	buildKeyMin []any
	buildKeyMax []any
	// buildKeyNaN[i] records that build key column i held a NaN, which
	// SUPPRESSES its dynamic range entirely. A NaN join key matches another
	// NaN (PostgreSQL's float order, ADR-0012 item 8) and parquet statistics
	// deliberately exclude NaN from min/max, so no [min,max] bound can tell
	// whether a probe row group holds a matching one: any range would prune
	// row groups the join would have matched.
	buildKeyNaN []bool

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
		if i < len(h.buildKeyNaN) && h.buildKeyNaN[i] {
			continue // see buildKeyNaN
		}
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
		h.buildKeyNaN = make([]bool, len(h.buildKeyIdx))
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
				if v != v {
					// A NaN build key. It is a real key that a probe-side NaN
					// matches, and it has no place in a [min,max] bound the
					// probe's parquet statistics could be compared against —
					// those exclude NaN by specification. Record it and leave
					// the bound to the finite values; BuildKeyRange then
					// withholds this column's range altogether.
					h.buildKeyNaN[ki] = true
					return
				}
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

// nullBuildKey records a build row whose join key is NULL — a row the hash
// index must NOT hold, because NULL equals nothing and no probe may match it.
//
// Skipping the index insert is the whole of "must not match". Skipping the
// row is a different claim, and two consumers need it not to be made:
//
//   - A RIGHT / FULL OUTER / RIGHT ANTI join owes every unmatched build row a
//     NULL-padded output row, and FlushUnmatched / FlushAntiMatched enumerate
//     the ARENA. The integer key paths used to `continue` past the arena
//     append as well, so those rows were invisible to the flush and vanished
//     — while the serialized-key path appended them and answered correctly,
//     which is why the same query was right with a TEXT key and wrong with a
//     BIGINT one (#496). storeRows=true appends the row with arenaNext = -1:
//     a chain of one that no hash bucket points at. arenaMatched is sized
//     from len(arena) after every append, so the extra entries are safe.
//
//   - A null-aware anti join needs to know the build contained a NULL AT ALL,
//     because that alone makes `NOT IN`'s answer UNKNOWN for every probe row
//     it did not otherwise match (#507). That is recorded on every path,
//     including the key-only builds that store no rows.
//
// buildRows counts it: it is a real build row. (nullBuildKeyOnly is the
// key-only variant, for the builds that store no rows and already count every
// arriving row in bulk.)
//
// Caller must hold h.mu on the paths that take it.
func (h *HashJoin) nullBuildKey(ref buildRef) {
	h.buildHasNullKey = true
	h.buildRows++
	if !h.emitsUnmatchedBuildRows() {
		return
	}
	h.arena = append(h.arena, ref)
	h.arenaNext = append(h.arenaNext, -1)
}

// nullBuildKeyOnly records a NULL build key on a build that stores no rows —
// SemiAntiKeyOnly and its parallel variant. There is no arena to append to and
// nothing to count (those paths add ActiveLen() per batch), so only the
// null-aware anti join's poison flag is left to set.
func (h *HashJoin) nullBuildKeyOnly() { h.buildHasNullKey = true }

// emitsUnmatchedBuildRows reports whether this join owes output for a build
// row nothing matched — the rows FlushUnmatched and FlushAntiMatched read out
// of the arena. RightSemiJoin emits MATCHED build rows, and a NULL-keyed row
// is never matched, so it is deliberately not in the list.
func (h *HashJoin) emitsUnmatchedBuildRows() bool {
	switch h.JoinType {
	case RightJoin, FullOuterJoin, RightAntiJoin:
		return true
	}
	return false
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
	if !p.buildProbeKey(in, row) {
		return false // NULL key: matches nothing, itself included
	}
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
	if !p.buildProbeKey(in, row) {
		return p.lookupBuf // NULL key: matches nothing, itself included
	}
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

// warmBuildNullBitmaps forces Bitmap.HasNulls memoization on every stored
// build column at build completion — the last single-threaded point. View
// output batches share these vectors as bases across concurrently-consumed
// batches (morsel-parallel sinks, per-partition append bursts), and
// HasNulls' lazy first-call write would otherwise race between consumers.
func (h *HashJoin) warmBuildNullBitmaps() {
	for _, b := range h.buildBatches {
		if b == nil {
			continue // partition-on-arrival: spilled partitions leave nil slots
		}
		for _, col := range b.Columns {
			if col == nil {
				continue
			}
			_ = col.Nulls.HasNulls()
		}
	}
}

// Build consumes all rows from the build (right) side into the columnar hash table.
// Uses parallel workers when the build side is large enough to benefit from
// concurrent hash table construction with per-worker local tables.
func (h *HashJoin) Build(ctx context.Context, source Source) error {
	// Build pulls directly from a Source (no pipeline loop in between) and
	// stores or key-reads what it pulls — arriving view batches must be
	// materialized at this boundary.
	source = &flattenSource{inner: source}
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
	if h.Spill != nil && h.MemTracker != nil && !h.SemiAntiKeyOnly && !h.semiAntiNEEligible() {
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
	// Distinct-pair NE builds (join_semianti_ne.go) take the flat path
	// below: their state is keyOnly-class compact (24 B/key vs stored
	// batches), so the partition-on-arrival spill machinery is skipped the
	// same way SemiAntiKeyOnly skips it. The defensive fallback (value
	// column fails to resolve on the first batch) builds flat+full without
	// spill — planner catalog type gating makes that branch theoretical.
	if h.Spill != nil && h.MemTracker != nil && !h.SemiAntiKeyOnly && !h.semiAntiNEEligible() {
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
			// Distinct-pair NE activation (join_semianti_ne.go): needs the
			// int-key path plus an integer value vector on this batch.
			h.neTryEnable(b)

			// Pre-allocate arena and index to avoid repeated slice growth.
			if h.BuildRowHint > 0 && !h.SemiAntiKeyOnly && !h.neActive {
				hint := int(h.BuildRowHint)
				h.arena = make([]buildRef, 0, hint)
				h.arenaNext = make([]int32, 0, hint)
				// intIndex already pre-sized by tryEnableIntKey; only pre-size string table
				if !h.useIntKey && !h.useDualIntKey {
					h.strIndex = newStrHashTable(hint)
				}
			}
		}

		if h.neActive {
			// Distinct-pair NE build: key index + per-key value pairs, no
			// batch storage (join_semianti_ne.go).
			h.updateKeyMinMax(b)
			h.insertNEBatch(b)
			h.buildRows += int64(b.ActiveLen())
			h.mu.Unlock()
			continue
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
							h.nullBuildKeyOnly()
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
							h.nullBuildKeyOnly()
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
							h.nullBuildKeyOnly()
							continue
						}
						h.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
						if !ok {
							h.nullBuildKeyOnly()
							continue
						}
						h.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
					}
				}
				h.intIndex.CheckGrow()
			} else {
				if h.strIndex == nil {
					// Seed to this batch's row count: the EnsureCapacity
					// above skipped a nil strIndex, and a 64-bucket seed
					// would let the first batch fill the table mid-loop
					// (GetOrInsertNoGrow spins forever on a full table).
					h.strIndex = newStrHashTable(batchRows)
				}
				if b.Sel != nil {
					for _, si := range b.Sel {
						if !h.buildKeyFromBatch(b, int(si)) {
							h.nullBuildKeyOnly()
						}
						h.strIndex.GetOrInsertNoGrow(h.keyBuf, 0)
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						if !h.buildKeyFromBatch(b, rowIdx) {
							h.nullBuildKeyOnly()
						}
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
		// Pre-grow the ref arena the same way: every inserted row appends
		// one buildRef + one chain link, and the per-append doubling inside
		// arenaAppend* re-memmoved both slices log2(buildRows) times per
		// build (14% of worker growslice CPU, 2026-08-12 treatment profile).
		if need := len(h.arena) + batchRows; cap(h.arena) < need {
			grown := make([]buildRef, len(h.arena), need+need/2)
			copy(grown, h.arena)
			h.arena = grown
		}
		if need := len(h.arenaNext) + batchRows; cap(h.arenaNext) < need {
			grown := make([]int32, len(h.arenaNext), need+need/2)
			copy(grown, h.arenaNext)
			h.arenaNext = grown
		}

		if h.useIntKey {
			col := b.Columns[h.buildKeyIdx[0]]
			if b.Sel != nil {
				for _, si := range b.Sel {
					key, ok := intKeyFromVector(col, int(si))
					if !ok {
						h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
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
						h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
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
						h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
						continue
					}
					h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					h.buildRows++
				}
			} else {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
					if !ok {
						h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
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
					if !h.buildKeyFromBatch(b, int(si)) {
						h.buildHasNullKey = true
					}
					h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					h.buildRows++
				}
			} else {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					if !h.buildKeyFromBatch(b, rowIdx) {
						h.buildHasNullKey = true
					}
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

	h.warmBuildNullBitmaps()
	h.applyBuildSchemaHint()
	h.buildDone = true
	return nil
}

// applyBuildSchemaHint adopts the plan-declared build schema when the build
// side delivered no batch at all. Everything downstream — the output schema,
// the probe's column mapping, the NULL padding an outer join emits — reads
// buildSchema, so a nil one silently drops the build side's columns from the
// join's output rather than emitting them as NULL (#348).
func (h *HashJoin) applyBuildSchemaHint() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buildSchema == nil && len(h.BuildSchemaHint) > 0 {
		h.buildSchema = append([]parquet.Column(nil), h.BuildSchemaHint...)
	}
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
		// Same fallback the serial build and buildPartitioned use: the plan
		// may keep the relation qualifier on a key the build batch emits bare.
		h.buildKeyIdx[i] = columnIndexFallback(first, col)
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

	// Launch workers. The workers share a cancellable child context so the
	// first failure — an error or a recovered panic — stops the siblings
	// instead of leaving them to pull the rest of the source.
	buildCtx, cancelBuild := context.WithCancel(ctx)
	defer cancelBuild()
	var sourceMu sync.Mutex
	var wg sync.WaitGroup
	var firstErr FirstError

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(lb *localKeyBuild) {
			defer wg.Done()
			// Morsel-parallel key insertion runs on goroutines the caller
			// never joins for errors, so an unrecovered panic here — a
			// value the key encoder cannot hold, a bad column index — ends
			// the process instead of the query (#511).
			//
			// source.Next runs UNDER sourceMu, and a panic raised inside it
			// unwinds past the Unlock below. Recovering without releasing
			// the mutex trades the crash for something worse: every sibling
			// blocks on sourceMu.Lock forever and the wg.Wait below never
			// returns, so the query hangs holding its memory budget and its
			// connection. FlattenViews on a batch the source hands back is
			// a live panic surface there (the #361/#392 class), so this is
			// reachable, not theoretical.
			//
			// A boundary owes every obligation the dying goroutine held,
			// not just the one in flight (ADR-0019). Here that is the
			// mutex, tracked in a variable rather than a per-batch defer so
			// the pull loop keeps its cost.
			holdsSource := false
			defer CatchQueryPanic(buildCtx, "hash join key build worker", func(err error) {
				if holdsSource {
					holdsSource = false
					sourceMu.Unlock()
				}
				firstErr.Set(err)
				cancelBuild()
			})
			for {
				if buildCtx.Err() != nil {
					return
				}
				sourceMu.Lock()
				holdsSource = true
				b, err := source.Next(buildCtx)
				if b != nil && b.Sel != nil {
					sel := make([]uint32, len(b.Sel))
					copy(sel, b.Sel)
					b.Sel = sel
				}
				holdsSource = false
				sourceMu.Unlock()
				if err != nil {
					firstErr.Set(fmt.Errorf("build source next: %w", err))
					cancelBuild()
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

	if err := firstErr.Err(); err != nil {
		return err
	}

	// Merge: count total rows, pick largest local table as base, insert rest.
	var totalRows int64
	bestIdx := 0
	var bestSize int
	for i, lb := range locals {
		totalRows += lb.rows
		if lb.hasNullKey {
			h.buildHasNullKey = true
		}
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
	// hasNullKey is this worker's half of HashJoin.buildHasNullKey; the
	// merge below ORs them, because a NULL anywhere in the build poisons a
	// null-aware anti join's whole answer (#507).
	hasNullKey bool
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
					lk.hasNullKey = true
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
					lk.hasNullKey = true
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
					lk.hasNullKey = true
					continue
				}
				lk.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
					lk.hasNullKey = true
					continue
				}
				lk.intIndex.PutNoGrow(dualIntHash(a, bb), 0)
			}
		}
		lk.intIndex.CheckGrow()
	} else {
		if lk.strIndex == nil {
			// Seed to this batch's row count — the EnsureCapacity above
			// skipped a nil strIndex, and a 64-bucket seed would let the
			// first batch fill the table mid-loop (GetOrInsertNoGrow spins
			// forever on a full table).
			lk.strIndex = newStrHashTable(batchRows)
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
						lk.hasNullKey = true
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
						lk.hasNullKey = true
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
			default:
				// ARRAY, ROW, MAP, VECTOR. There was no arm and no default,
				// so the null bitmap above was copied and the VALUES were
				// not — and :1583 then discards the originals, leaving every
				// row of such a payload column empty. It fired only when the
				// build had >1 batch, ≤2M rows and <30% tracker use, so the
				// same join answered correctly or emptily depending on
				// memory.
				//
				// Element storage is append-built and array offsets must
				// advance in row order, so this is per row rather than a bulk
				// slice copy — CopyValueFrom is the primitive every other
				// gather in the engine uses, and it advances offsets and
				// children for NULL rows too. The loop below is in row order
				// within a batch and the batches are visited in offset order,
				// which is the sequential-destination contract it requires.
				for i := 0; i < b.Len; i++ {
					copyVectorValue(dst, off+i, src, i)
				}
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

	// Both spellings of every kept column. A join emits a build column
	// qualified when its bare name collides and bare when it does not, and
	// the filter that named it may have used either — pruning by exact name
	// alone dropped the column the filter then could not resolve (#527).
	// Keeping one column too many is free; the prune is a memory
	// optimization, not a semantic one.
	keep := make(map[string]bool, len(keepCols)*2)
	for _, c := range keepCols {
		keep[c] = true
		if dot := strings.LastIndex(c, "."); dot >= 0 {
			keep[c[dot+1:]] = true
		}
	}

	// Build new pruned schema
	var newSchema []parquet.Column
	var colIdx []int
	for i, col := range h.buildSchema {
		kept := keep[col.Name]
		if !kept {
			if dot := strings.LastIndex(col.Name, "."); dot >= 0 {
				kept = keep[col.Name[dot+1:]]
			}
		}
		if kept {
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
			h.buildKeyIdx[i] = columnIndexFallback(b, col)
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
	// Pre-grow the index for this call's rows so PutNoGrow can't fill the
	// table mid-loop — a full table turns its probe loop into an infinite
	// spin. The int index is pre-sized by tryEnableIntKey only on the first
	// call, and the string index was seeded at 64 buckets, so any call
	// inserting more distinct keys than the remaining headroom hung here.
	// tryEnableIntKey has THREE outcomes, not two: a single integer key sets
	// useIntKey, a two-integer key sets useDualIntKey (and nils strIndex),
	// and everything else leaves both false. Testing only useIntKey sent a
	// two-integer-key build down the string branch and populated strIndex,
	// while lookupBuild and existsInBuild test useDualIntKey FIRST and probe
	// the intIndex nothing had filled — every probe missed, so the join
	// returned no rows at all (an anti join: every row). BuildFromRows is the
	// worker's entry point, so that was reachable from the distributed path
	// (#498).
	switch {
	case h.useIntKey:
		h.intIndex.EnsureCapacity(b.Len)
		col := b.Columns[h.buildKeyIdx[0]]
		for i := 0; i < b.Len; i++ {
			key, ok := intKeyFromVector(col, i)
			if !ok {
				h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
				continue
			}
			h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
			h.buildRows++
		}
		h.intIndex.CheckGrow()
	case h.useDualIntKey:
		h.intIndex.EnsureCapacity(b.Len)
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		for i := 0; i < b.Len; i++ {
			a, bb, ok := dualIntKeyFromVectors(col0, col1, i)
			if !ok {
				h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
				continue
			}
			h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
			h.buildRows++
		}
		h.intIndex.CheckGrow()
	default:
		if h.strIndex == nil {
			h.strIndex = newStrHashTable(b.Len)
		}
		h.strIndex.EnsureCapacity(b.Len)
		for i := 0; i < b.Len; i++ {
			if !h.buildKeyFromBatch(b, i) {
				h.buildHasNullKey = true
			}
			h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
			h.buildRows++
		}
		h.strIndex.CheckGrow()
	}
	if (h.JoinType == RightJoin || h.JoinType == FullOuterJoin) && len(h.arena) > 0 {
		h.arenaMatched = make([]bool, len(h.arena))
	}
	h.buildBloom()
	h.warmBuildNullBitmaps()
	h.buildDone = true
}

// BuildRows returns the number of rows in the build side.
func (h *HashJoin) BuildRows() int64 {
	return h.buildRows
}

// KeyAssignmentRepairs counts runtime probe/build key swaps performed by
// FixKeyAssignment after a build completed — cases where PLAN-TIME side
// assignment (assignJoinKeySides) got a pair wrong.
//
// "Rescued" is only sometimes the word. The repair's premise is that a left
// key present in the build schema must be misassigned, and that premise is
// FALSE whenever the bare name is on BOTH sides — every self-join. There the
// swap leaves the probe resolving a name only the build has, the join matches
// nothing, and the query answers zero rows with no error: it was the second
// half of #516 and it is the mechanism of #526. So a firing on a
// planner-produced plan is a defect signal, not a save.
//
// Asserted to stay 0 by benchmarks/tpch.TestTPCHQueries over the whole
// 22-query corpus and by physical.TestBushyBuild_* over the bushy-build
// shapes. A nonzero count means a plan shape leaked through the planner's
// ownership resolution (rebuild cost, a hazard for partitioned builds, and
// possibly a wrong answer).
var KeyAssignmentRepairs atomic.Int64

// FixKeyAssignment corrects misassigned join keys after the build phase.
// SQL may place the build-side column on the left of "=" (e.g., JOIN t ON t.id = src.id),
// causing parseJoinKeys to assign it as a left/probe key. This detects and swaps
// misassigned pairs by checking which keys exist in the build schema.
// It returns true when any pair was swapped, so callers can surface the
// repair (it should never fire on planner-produced plans).
func (h *HashJoin) FixKeyAssignment() bool {
	if h.buildSchema == nil || len(h.LeftKeys) == 0 {
		return false
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
	if needsRebuild {
		KeyAssignmentRepairs.Add(1)
	}

	// Rebuild hash index if keys were swapped
	if needsRebuild {
		// A build that stores NO ROWS has nothing to rebuild from, and
		// rebuilding anyway destroys what it does hold. SemiAntiKeyOnly —
		// every unfiltered semi/anti join (physical/plan.go, "enable key-only
		// build") — populates the key index and the bloom and leaves
		// h.buildBatches empty by design; the distinct-pair NE build
		// (join_semianti_ne.go) is the same shape. The rebuild below resets
		// buildRows to 0 and buildHasNullKey to false and then recomputes
		// them by walking h.buildBatches, which for these builds is zero
		// iterations: both facts stay at their zero values, and the fresh
		// empty index replaces the populated one.
		//
		// buildHasNullKey is not bookkeeping. It is the whole of NOT IN's
		// three-valued rule (#507): a NULL anywhere in the build makes the
		// answer UNKNOWN for every probe row that did not otherwise match,
		// so losing it turns `x NOT IN (…)` from "no rows" into "every row"
		// — silently (#572).
		//
		// Nothing here needs rebuilding. The key SWAP above stands, because
		// probe-side resolution needs the corrected names, and the
		// arrival-time index stays authoritative: the key-only builds
		// resolve their build key through columnIndexFallback, which maps the
		// misassigned name to the same physical build column (had it resolved
		// to nothing, the build itself would have failed). That is the same
		// argument the evicted-partition guard below makes.
		if h.SemiAntiKeyOnly || h.neActive {
			return true
		}
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
					return true
				}
			}
		}
		// Re-resolve build key indices after swap
		if len(h.buildBatches) > 0 {
			b := h.buildBatches[0]
			h.buildKeyIdx = make([]int, len(h.RightKeys))
			for i, col := range h.RightKeys {
				h.buildKeyIdx[i] = columnIndexFallback(b, col)
			}
			// Re-check int key eligibility with new key assignment
			h.useIntKey = false
			h.useDualIntKey = false
			h.tryEnableIntKey(b)
		}
		h.buildRows = 0
		// The key columns just changed sides, so which rows have a NULL key is
		// a different question than it was. Recompute it with the rest (#507).
		h.buildHasNullKey = false
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
						h.nullBuildKey(buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
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
						h.nullBuildKey(buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
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
					if !h.buildKeyFromBatch(b, rowIdx) {
						h.buildHasNullKey = true
					}
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
	return needsRebuild
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
// It reports matchability the way buildProbeKey does: false when a key column
// of this row is NULL, which is what a null-aware anti join reads to learn the
// build held a NULL at all (#507). The row is still keyed and still stored —
// nothing probes it, because the probe side refuses to match a NULL key
// (#459), and a RIGHT/FULL join still owes it a NULL-padded output row.
// Callers that do not care discard the result.
func (h *HashJoin) buildKeyFromBatch(b *batch.RecordBatch, rowIdx int) bool {
	h.keyBuf = h.keyBuf[:0]
	matchable := true
	for _, idx := range h.buildKeyIdx {
		if idx < 0 {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(rowIdx) {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			matchable = false
		} else {
			h.keyBuf = append(h.keyBuf, 0) // not-null flag
			h.keyBuf = appendColumnValue(h.keyBuf, v, rowIdx, v.Type)
		}
	}
	return matchable
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

// buildProbeKey fills p.keyBuf with the serialized probe key for a row and
// reports whether that key may MATCH. Uses the per-probe keyBuf to avoid races
// when multiple cloned probes execute in parallel.
//
// A row holding a NULL in any key column reports false: SQL's `=` is UNKNOWN
// against a NULL, so an equi-join must not pair it with anything — not even
// with another NULL. The key bytes are still filled in, because the partition
// router (probePartition, join_spill.go) needs a deterministic partition for
// every row including that one, exactly as the integer paths return partition
// 0 for a key they refuse to match.
//
// Without the flag, a NULL serialized to a lone 0x01 flag byte with no
// payload, so two NULL rows produced IDENTICAL key bytes and the string hash
// table — which matches keys by byte equality — joined them. The integer fast
// paths (intProbeKey, dualIntKeyFromVectors) have always refused a NULL key,
// so which answer a query got depended on whether its key columns happened to
// be integers (#459).
//
// An UNRESOLVABLE key column (idx < 0) is deliberately NOT a NULL here: it
// keeps its flag byte and its matchability, because folding it in would turn
// a join whose key column is missing from the probe schema from "matches
// everything" into "matches nothing" — a different bug, in a different place.
func (p *HashJoinProbe) buildProbeKey(b *batch.RecordBatch, row int) bool {
	h := p.join
	h.resolveProbeKeyIdx(b)
	p.keyBuf = p.keyBuf[:0]
	matchable := true
	for _, idx := range h.probeKeyIdx {
		if idx < 0 {
			p.keyBuf = append(p.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			p.keyBuf = append(p.keyBuf, 1) // null flag
			matchable = false
		} else {
			p.keyBuf = append(p.keyBuf, 0) // not-null flag
			p.keyBuf = appendColumnValue(p.keyBuf, v, row, v.Type)
		}
	}
	return matchable
}

// joinOutputPoolSize is the capacity for join output batch pooling.
// 16x DefaultBatchSize handles most 1:N join outputs without fresh allocation,
// including skewed batches with high fan-out.
const joinOutputPoolSize = 16 * batch.DefaultBatchSize

// MaxProbeOutputRows bounds how many match pairs one probe call materialises.
// Past it the fan-out is suspended at (probe row, hash-chain position) and
// resumed on the next call, making probe output O(batch) instead of
// O(batch x fan-out) — see the resume protocol on HashJoinProbe.
//
// The value is joinOutputPoolSize, which is also pairsBuf's pre-allocated
// capacity and the large output pool's size. Bounding at exactly that point
// means every join shape that fits today keeps its current batch shape, pool,
// and allocation profile; only the shapes that used to grow pairsBuf without
// limit change behaviour.
//
// Exported because the drivers that hold the probe to it live outside this
// package (internal/worker, internal/planner/physical), as do the tests that
// assert they do.
const MaxProbeOutputRows = joinOutputPoolSize

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

	// LateMaterialize emits inner/left join output as view (dictionary)
	// columns over the probe input and build batches instead of gathering
	// copies — the deferred gather happens at the first consumer that needs
	// owned storage (see flattenForConsumer). The probe input is Detach()ed
	// when views reference it so pool recycling can't mutate the shared
	// vectors; GC reclaims it once the views die. Off by default.
	LateMaterialize bool

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

	// Bounded fan-out state (#317). boundOutput is set by a driver that
	// implements the BoundedOutputOperator protocol; res is the cursor into
	// the input batch currently being drained. All of it is per-operator, and
	// Clone() hands every parallel worker its own HashJoinProbe, so cloned
	// probes never share a cursor.
	boundOutput bool
	res         probeResume

	// emitBuf is the reusable late-materialization output storage — index
	// arrays, view composition buffers and gathered build columns written
	// over on each emit while no consumer has claimed them. Per-operator, and
	// Clone() mints a fresh probe, so parallel workers never share one. See
	// join_emit_reuse.go for the ownership rule.
	emitBuf probeEmitBuf
}

// probeResume is the suspended position of a probe's fan-out: which probe row
// is being expanded, and how far into that row's match chain the previous
// call got. A probe call stops once it has materialised MaxProbeOutputRows
// match pairs and stores the cursor here; the next call picks up exactly
// where it left off, so no match is emitted twice and none is skipped.
type probeResume struct {
	in     *batch.RecordBatch // input batch being drained; nil when idle
	active bool               // more output is pending for in
	pos    int                // next probe position (index into in.Sel, else row index)
	ref    int32              // cursor within the current probe row's matches
	mid    bool               // ref is live: resume inside pos's match chain
	// accepted carries, across a mid-chain suspension, whether the suspended
	// probe row has had any candidate pass the Residual so far — the bit that
	// decides whether a LEFT/FULL join owes the row a NULL-padded emission
	// once its chain is exhausted.
	accepted bool

	// Cross-join cursor: which build batch, and which row within it.
	crossSchema []parquet.Column
	crossBatch  int
	crossRow    int
}

func (p *HashJoinProbe) Init(_ context.Context) error {
	// A probe reused across queries must not carry last query's emit storage
	// (different schema, and its consumers are long gone).
	p.emitBuf.reset()
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
	} else if h.useDualIntKey {
		// The two-integer key had no arm here at all, and its index is never
		// the string one (tryEnableIntKey nils strIndex when it takes this
		// path) — so nothing marked a build row matched, and a RIGHT or FULL
		// OUTER join on two integer columns emitted EVERY build row as
		// unmatched on top of the pairs it had already joined.
		//
		// The chain is keyed by dualIntHash, which collides by construction,
		// so each entry's actual pair is verified before it is marked —
		// exactly what lookupBuild does on this path.
		h.resolveProbeKeyIdx(in)
		if h.probeKeyIdx[0] < 0 || h.probeKeyIdx[1] < 0 {
			return
		}
		a, b, ok := dualIntKeyFromVectors(in.Columns[h.probeKeyIdx[0]], in.Columns[h.probeKeyIdx[1]], row)
		if !ok {
			return // NULL key: matches nothing, so it marks nothing
		}
		head, ok := h.intIndex.Get(dualIntHash(a, b))
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			ref := h.arena[idx]
			bb := h.buildBatches[ref.batchIdx]
			ba, bbv, bok := dualIntKeyFromVectors(bb.Columns[h.buildKeyIdx[0]], bb.Columns[h.buildKeyIdx[1]], int(ref.rowIdx))
			if bok && ba == a && bbv == b {
				h.arenaMatched[idx] = true
			}
		}
	} else if h.strIndex != nil {
		if !p.buildProbeKey(in, row) {
			return // NULL key: matches nothing, so it marks nothing
		}
		head, ok := h.strIndex.Get(p.keyBuf)
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	}
}

// markRowMatched records that ONE build row was matched under key AND
// residual. Must be called with h.mu held. Complements arenaMatched for the
// Residual path — see the field docs.
func (h *HashJoin) markRowMatched(ref buildRef) {
	if h.rowMatched == nil {
		h.rowMatched = make([][]bool, len(h.buildBatches))
	}
	rm := h.rowMatched[ref.batchIdx]
	if rm == nil {
		rm = make([]bool, h.buildBatches[ref.batchIdx].Len)
		h.rowMatched[ref.batchIdx] = rm
	}
	rm[ref.rowIdx] = true
}

// refMatched reports whether a build row was residual-matched. Callers hold
// h.mu or run after all probing completed (the flush paths).
func (h *HashJoin) refMatched(ref buildRef) bool {
	if int(ref.batchIdx) >= len(h.rowMatched) {
		return false
	}
	rm := h.rowMatched[ref.batchIdx]
	return rm != nil && rm[ref.rowIdx]
}

// AcceptsViews marks the probe view-aware: Execute self-manages view input
// via prepareViewInput — key columns (the only probe-side columns the probe
// reads positionally) are flattened individually, pass-through columns stay
// lazy and compose in emitViewOutput, and the paths that read or persist
// arbitrary columns flatten everything. This is what makes a fused join
// chain one-copy-per-column: join N's output views compose over join N-1's
// bases instead of materializing between probes.
func (p *HashJoinProbe) AcceptsViews() bool { return true }

// prepareViewInput materializes exactly as much of a view-carrying input
// batch as this probe's execution path will read.
func (p *HashJoinProbe) prepareViewInput(in *batch.RecordBatch) {
	h := p.join
	// Paths that touch arbitrary probe columns must see owned storage:
	// cross join gathers every column; SemiAntiFilter evaluates a compiled
	// expression over unresolved column sets; grace partitioning writes
	// whole probe rows to spill files; right/full outer take the eager
	// gather path (they never emit views); inner/left without the flag
	// likewise gather eagerly.
	lazyKeys := h.SemiAntiFilter == nil && h.Residual == nil &&
		!(h.spillState != nil && len(h.spillState.spilledParts) > 0) &&
		((p.LateMaterialize && (h.JoinType == InnerJoin || h.JoinType == LeftJoin)) ||
			h.JoinType == SemiJoin || h.JoinType == AntiJoin ||
			h.JoinType == RightSemiJoin || h.JoinType == RightAntiJoin)
	if !lazyKeys {
		FlattenForConsumer(in, nil)
		return
	}
	h.resolveProbeKeyIdx(in)
	for _, idx := range h.probeKeyIdx {
		if idx >= 0 && in.Columns[idx].IsView() {
			in.FlattenColumn(idx)
			LateMatFlattens.Add(1)
		}
	}
}

// EnableBoundedOutput opts this probe into the BoundedOutputOperator
// protocol: Execute emits at most MaxProbeOutputRows joined rows and suspends
// the rest of the input batch's fan-out for NextOutput to resume. Only a
// driver that drains NextOutput after every Execute may call it.
func (p *HashJoinProbe) EnableBoundedOutput() { p.boundOutput = true }

// HasPendingOutput reports whether the last input batch still has fan-out
// left to emit.
func (p *HashJoinProbe) HasPendingOutput() bool { return p.res.active }

// NextOutput resumes a suspended fan-out and returns the next bounded slice
// of the current input batch's join output.
func (p *HashJoinProbe) NextOutput(ctx context.Context) (*batch.RecordBatch, error) {
	if !p.res.active {
		return nil, nil
	}
	if p.join.JoinType == CrossJoin {
		return p.nextCrossChunk()
	}
	return p.nextProbeChunk(ctx)
}

// pairLimit is the number of match pairs one call may materialise. Without a
// driver that resumes, the probe has nowhere to put the remainder, so it
// keeps its historical single-shot behaviour.
func (p *HashJoinProbe) pairLimit() int {
	if p.boundOutput {
		return MaxProbeOutputRows
	}
	return math.MaxInt32
}

// beginResume arms the fan-out cursor for a freshly arrived input batch.
func (p *HashJoinProbe) beginResume(in *batch.RecordBatch) {
	p.res = probeResume{in: in, active: true}
}

// finishResume clears the cursor once an input batch is fully expanded.
func (p *HashJoinProbe) finishResume() {
	p.res = probeResume{}
}

func (p *HashJoinProbe) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in.HasViews() {
		p.prepareViewInput(in)
	}
	if p.join.JoinType == CrossJoin {
		p.beginResume(in)
		p.res.crossSchema = p.outputSchema(in.Schema)
		return p.nextCrossChunk()
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
		p.recordProbeSchema(in.Schema)
	}

	p.beginResume(in)
	return p.nextProbeChunk(ctx)
}

// nextProbeChunk materialises up to MaxProbeOutputRows match pairs for the
// input batch under the fan-out cursor and turns them into one output batch.
// It is the shared body of Execute and NextOutput: the only difference
// between the first call for an input batch and a resumption is where the
// cursor starts.
func (p *HashJoinProbe) nextProbeChunk(_ context.Context) (*batch.RecordBatch, error) {
	in := p.res.in
	outSchema, mapping := p.cachedSchema, p.cachedMapping

	// Collect match pairs using reusable buffer. The typed loops fill it
	// through a pre-sized window, so it must always be at least `limit` long.
	if cap(p.pairsBuf) < MaxProbeOutputRows {
		p.pairsBuf = make([]matchPair, 0, MaxProbeOutputRows)
	}
	pairs := p.pairsBuf[:0]
	limit := MaxProbeOutputRows
	if !p.boundOutput {
		limit = cap(pairs)
	}
	done := true

	h := p.join
	// Fast path: single int key inner join without right/full outer tracking.
	// Inlines hash table lookup + typed data access, eliminating 4 levels of
	// per-row function calls (probeRow → lookupBuild → intProbeKey → intKeyFromVector).
	inlineInt := h.useIntKey && h.JoinType == InnerJoin && h.arenaMatched == nil && h.Residual == nil
	inlineDual := !inlineInt && h.useDualIntKey && h.JoinType == InnerJoin && h.arenaMatched == nil && h.Residual == nil
	if inlineInt || inlineDual {
		h.resolveProbeKeyIdx(in)
	}
	for {
		switch {
		case inlineInt:
			if keyIdx := h.probeKeyIdx[0]; keyIdx >= 0 {
				pairs, done = p.inlineIntProbe(in.Columns[keyIdx], in, pairs, limit)
			}
		case inlineDual:
			if h.probeKeyIdx[0] >= 0 && h.probeKeyIdx[1] >= 0 {
				pairs, done = p.inlineDualIntProbe(in.Columns[h.probeKeyIdx[0]], in.Columns[h.probeKeyIdx[1]], in, pairs, limit)
			}
		default:
			pairs, done = p.genericProbe(in, pairs, limit)
		}
		if done || p.boundOutput {
			break
		}
		// The driver did not opt into the resume protocol, so nobody will
		// call NextOutput: grow the buffer and pick the fan-out back up where
		// it suspended. This reproduces exactly what pairsBuf used to do on
		// its own — the whole input batch's fan-out in one output batch. Every
		// driver that can be handed a probe now opts in (exec.Pipeline, the
		// worker's fragment executors, physical.pipelineSource), so this is
		// the fallback for a probe built and driven ad hoc — a test harness,
		// or a future driver — not a live query path.
		grown := make([]matchPair, len(pairs), 2*cap(pairs))
		copy(grown, pairs)
		pairs = grown
		limit = cap(pairs)
	}
	p.pairsBuf = pairs // save grown slice for reuse
	if done {
		p.finishResume()
	}

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

	// Late materialization: emit view columns instead of gathered copies.
	// Inner and left joins only in v1 — right/full-outer probe output could
	// also ride views, but their FlushUnmatched/FlushMatched emission paths
	// stay eager, so they are excluded until measured separately.
	if p.LateMaterialize && (h.JoinType == InnerJoin || h.JoinType == LeftJoin) {
		return p.emitViewOutput(in, outSchema, mapping, pairs), nil
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

// emitViewOutput builds the join output as view (dictionary) columns: probe
// columns reference the input batch through the pair probe rows, build
// columns reference the (single) build batch through the pair build rows.
// The only per-row work is filling two shared uint32 index slices — the
// value gather is deferred to the first consumer that needs owned storage.
// Multi-batch builds (consolidation skipped: spilling, >2M rows, >30%
// budget) keep the eager gather for build columns, since a view has one
// base; the output is then mixed view/owned, which is fine — views are
// per-column.
func (p *HashJoinProbe) emitViewOutput(in *batch.RecordBatch, outSchema []parquet.Column, mapping []outColSource, pairs []matchPair) *batch.RecordBatch {
	n := len(pairs)
	h := p.join
	allMatched := h.JoinType != LeftJoin && h.JoinType != FullOuterJoin
	singleBuild := len(h.buildBatches) == 1

	// Storage reuse: last call's output is written over when no consumer
	// claimed it, which is what removes ~208 GB per SF100 suite run of gather
	// backing plus ~62 GB of index arrays from the Go heap lock's queue. See
	// probeEmitBuf's ownership rule (join_emit_reuse.go) — the invariant is
	// that a consumer keeping a batch past its call calls Detach, and Detach
	// records the claim on the column vectors, so a derived batch or a
	// downstream view over one of our columns carries it back to us.
	e := &p.emitBuf
	reuse := e.reusable(mapping)
	if !reuse {
		e.reset()
	}

	var probeIdx, buildIdx []uint32
	hasProbeCols, hasBuildCols := false, false
	for _, m := range mapping {
		if m.fromProbe {
			hasProbeCols = true
		} else {
			hasBuildCols = true
		}
	}
	if hasProbeCols {
		e.probeIdx = ensureU32(e.probeIdx, n)
		probeIdx = e.probeIdx
		for i, pair := range pairs {
			probeIdx[i] = uint32(pair.probeRow)
		}
	}
	if hasBuildCols && singleBuild {
		e.buildIdx = ensureU32(e.buildIdx, n)
		buildIdx = e.buildIdx
		for i, pair := range pairs {
			buildIdx[i] = uint32(pair.ref.rowIdx)
		}
	}

	if cap(e.cols) >= len(mapping) {
		e.cols = e.cols[:len(mapping)]
	} else {
		e.cols = make([]*batch.Vector, len(mapping))
		e.composed = make([][]uint32, len(mapping))
	}
	if len(e.composed) != len(mapping) {
		e.composed = make([][]uint32, len(mapping))
	}
	cols := e.cols
	viewCols := 0
	for outColIdx, m := range mapping {
		switch {
		case m.fromProbe:
			// The view Vector itself is minted fresh every batch (a header
			// plus a null bitmap, both small-object); only the composition
			// buffer a view-over-view adopts — one large object per column
			// per batch on a fused join chain — is carried across.
			v, adopted := batch.NewViewVectorReuse(in.Columns[m.srcIdx], probeIdx, e.composed[outColIdx])
			e.composed[outColIdx] = adopted
			cols[outColIdx] = v
			viewCols++
		case singleBuild:
			v, adopted := batch.NewViewVectorReuse(h.buildBatches[0].Columns[m.srcIdx], buildIdx, e.composed[outColIdx])
			e.composed[outColIdx] = adopted
			if !allMatched {
				// Left-join null-fill: unmatched pairs carry a zero buildRef
				// whose index value is meaningless — the view's own null bit
				// masks it (Flatten and all view-aware readers honor it).
				for i, pair := range pairs {
					if !pair.matched {
						v.Nulls.SetNull(i)
					}
				}
			}
			cols[outColIdx] = v
			viewCols++
		default:
			// Multi-batch build: this column is gathered eagerly, so its
			// storage is ours and is the one thing here worth reusing —
			// ResetForWrite resizes and clears it in place, retaining the
			// typed slice and (for BYTES) the arena PreAllocBytes would
			// otherwise re-request at full size every batch.
			dst := cols[outColIdx]
			if reuse && reusableGather(dst, outSchema[outColIdx]) {
				dst.ResetForWrite(n)
			} else {
				dst = batch.NewColumnVector(outSchema[outColIdx], n)
			}
			gatherBuildVector(dst, m.srcIdx, pairs, h.buildBatches, allMatched)
			cols[outColIdx] = dst
		}
	}

	if hasProbeCols {
		// Views reference the input's vectors: sever it from its pool so a
		// recycle can't truncate the shared arenas mid-flight. DetachPool,
		// not Detach: the reference is transitive — it dies with THIS output
		// batch — so claiming the input outright would stop an upstream probe
		// reusing its own storage forever. A consumer that really keeps our
		// output propagates the claim through Vector.Base for us.
		in.DetachPool()
	}

	out := e.out
	if out == nil {
		out = &batch.RecordBatch{}
		e.out = out
	}
	out.Columns = cols
	out.Schema = outSchema
	out.Len = n
	out.Sel = nil

	LateMatBatchesEmitted.Add(1)
	LateMatViewColumns.Add(int64(viewCols))
	return out
}

// inlineIntProbe is the fast probe path for single int key inner joins.
// It inlines the hash table lookup with typed data access, eliminating
// per-row function call overhead from lookupBuild/intProbeKey/intKeyFromVector.
// The probe logic is fully inlined (no closure) to avoid heap allocation of
// the closure + captured pairs slice, which saves ~2.5GB of allocations at SF1.
//
// It fills at most limit-len(pairs) pairs and returns done=false when it
// stopped short; p.res then names the probe row and the chain position to
// resume from.
//
// The loops write through a pre-sized window (buf[:limit], n) rather than
// appending, so `n >= limit` — the test that suspends the fan-out — is the
// same compare append already made against cap. Nothing is added per probe
// row either: a resumed chain is drained by resumeIntChain before the typed
// loops start, so they still begin at a row boundary. What the chain walk
// actually compiles to on amd64 is the arena/arenaNext load pair, the
// 16-byte store, and three compares — `ref >= 0`, the suspend test, and a
// bounds check on buf[n] that the prover does not fold into the suspend test
// (it knows n != limit, not n < len(buf)). Measured against the unbounded
// version: 1:1 fan-out is unchanged, 1:4 costs ~1.5% best-case, and 1:64 is
// ~45% faster because pairsBuf stops growing. Callers guarantee
// cap(pairs) >= limit.
func (p *HashJoinProbe) inlineIntProbe(keyCol *batch.Vector, in *batch.RecordBatch, pairs []matchPair, limit int) ([]matchPair, bool) {
	h := p.join
	arena := h.arena
	arenaNext := h.arenaNext
	idx := h.intIndex

	// Bloom filter is NOT checked here — it's already applied as a separate
	// BloomFilterOp in the pipeline (InnerJoin always gets one). Checking
	// it again inline would be redundant work.

	if p.res.mid {
		var ok bool
		if pairs, ok = p.resumeIntChain(in, pairs, limit); !ok {
			return pairs, false
		}
	}

	buf := pairs[:limit]
	n := len(pairs)

	if in.Sel != nil {
		sel := in.Sel
		if !keyCol.Nulls.HasNulls() {
			switch keyCol.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := keyCol.Int32Data
				for pos := p.res.pos; pos < len(sel); pos++ {
					si := sel[pos]
					head, ok := idx.Get(int64(data[si]))
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = pos, ref, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(si), ref: arena[ref], matched: true}
						n++
					}
				}
			default:
				data := keyCol.Int64Data
				for pos := p.res.pos; pos < len(sel); pos++ {
					si := sel[pos]
					head, ok := idx.Get(data[si])
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = pos, ref, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(si), ref: arena[ref], matched: true}
						n++
					}
				}
			}
		} else {
			for pos := p.res.pos; pos < len(sel); pos++ {
				si := sel[pos]
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
					if n >= limit {
						p.res.pos, p.res.ref, p.res.mid = pos, ref, true
						return buf[:n], false
					}
					buf[n] = matchPair{probeRow: int32(si), ref: arena[ref], matched: true}
					n++
				}
			}
		}
	} else {
		if !keyCol.Nulls.HasNulls() {
			switch keyCol.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := keyCol.Int32Data
				for i := p.res.pos; i < in.Len; i++ {
					head, ok := idx.Get(int64(data[i]))
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = i, ref, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(i), ref: arena[ref], matched: true}
						n++
					}
				}
			default:
				data := keyCol.Int64Data
				for i := p.res.pos; i < in.Len; i++ {
					head, ok := idx.Get(data[i])
					if !ok {
						continue
					}
					for ref := head; ref >= 0; ref = arenaNext[ref] {
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = i, ref, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(i), ref: arena[ref], matched: true}
						n++
					}
				}
			}
		} else {
			for i := p.res.pos; i < in.Len; i++ {
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
					if n >= limit {
						p.res.pos, p.res.ref, p.res.mid = i, ref, true
						return buf[:n], false
					}
					buf[n] = matchPair{probeRow: int32(i), ref: arena[ref], matched: true}
					n++
				}
			}
		}
	}
	return buf[:n], true
}

// resumeIntChain drains what is left of the hash chain a previous
// inlineIntProbe call suspended, and advances the cursor to the next probe
// row. done=false means the chain still did not fit in this call.
func (p *HashJoinProbe) resumeIntChain(in *batch.RecordBatch, pairs []matchPair, limit int) ([]matchPair, bool) {
	arena := p.join.arena
	arenaNext := p.join.arenaNext
	row := p.res.pos
	if in.Sel != nil {
		row = int(in.Sel[p.res.pos])
	}
	for ref := p.res.ref; ref >= 0; ref = arenaNext[ref] {
		if len(pairs) >= limit {
			p.res.ref = ref
			return pairs, false
		}
		pairs = append(pairs, matchPair{probeRow: int32(row), ref: arena[ref], matched: true})
	}
	p.res.mid = false
	p.res.pos++
	return pairs, true
}

// inlineDualIntProbe is the fast probe path for dual int key inner joins.
// Inlines composite hash computation + chain traversal with typed key verification,
// eliminating per-row lookupBuild/dualIntKeyFromVectors function call overhead.
//
// Bounded like inlineIntProbe (see there), and with the same shape: the
// suspended chain is drained by resumeDualIntChain before the typed loops
// start, so the loops themselves gain only the pre-sized-window bound test.
// The chain walk here verifies both key components against the build row, so
// the resume helper re-derives the probe row's key instead of threading it
// through the cursor.
func (p *HashJoinProbe) inlineDualIntProbe(col0, col1 *batch.Vector, in *batch.RecordBatch, pairs []matchPair, limit int) ([]matchPair, bool) {
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

	sel := in.Sel

	if p.res.mid {
		var ok bool
		if pairs, ok = p.resumeDualIntChain(col0, col1, in, pairs, limit); !ok {
			return pairs, false
		}
	}

	buf := pairs[:limit]
	n := len(pairs)

	if in.Sel != nil {
		if noNulls {
			for pos := p.res.pos; pos < len(sel); pos++ {
				si := sel[pos]
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
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = pos, ri, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(si), ref: r, matched: true}
						n++
					}
				}
			}
		} else {
			for pos := p.res.pos; pos < len(sel); pos++ {
				si := sel[pos]
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
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = pos, ri, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(si), ref: r, matched: true}
						n++
					}
				}
			}
		}
	} else {
		if noNulls {
			for i := p.res.pos; i < in.Len; i++ {
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
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = i, ri, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(i), ref: r, matched: true}
						n++
					}
				}
			}
		} else {
			for i := p.res.pos; i < in.Len; i++ {
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
						if n >= limit {
							p.res.pos, p.res.ref, p.res.mid = i, ri, true
							return buf[:n], false
						}
						buf[n] = matchPair{probeRow: int32(i), ref: r, matched: true}
						n++
					}
				}
			}
		}
	}
	return buf[:n], true
}

// resumeDualIntChain drains what is left of the chain a previous
// inlineDualIntProbe call suspended. It re-derives the probe row's key from
// the vectors rather than threading it through the cursor: this runs at most
// once per call, so the cost stays entirely off the typed loops.
func (p *HashJoinProbe) resumeDualIntChain(col0, col1 *batch.Vector, in *batch.RecordBatch, pairs []matchPair, limit int) ([]matchPair, bool) {
	h := p.join
	row := p.res.pos
	if in.Sel != nil {
		row = int(in.Sel[p.res.pos])
	}
	a, b, ok := dualIntKeyFromVectors(col0, col1, row)
	if !ok {
		p.res.mid = false
		p.res.pos++
		return pairs, true
	}
	for ri := p.res.ref; ri >= 0; ri = h.arenaNext[ri] {
		r := h.arena[ri]
		bb := h.buildBatches[r.batchIdx]
		ba0, ba1, bok := dualIntKeyFromVectors(bb.Columns[h.buildKeyIdx[0]], bb.Columns[h.buildKeyIdx[1]], int(r.rowIdx))
		if !bok || ba0 != a || ba1 != b {
			continue
		}
		if len(pairs) >= limit {
			p.res.ref = ri
			return pairs, false
		}
		pairs = append(pairs, matchPair{probeRow: int32(row), ref: r, matched: true})
	}
	p.res.mid = false
	p.res.pos++
	return pairs, true
}

// genericProbe is the resumable form of the general probe loop: serialized
// (string/composite) keys, outer joins, and any shape the inline int paths
// decline. Unmatched probe rows for LEFT/FULL OUTER are emitted here, exactly
// once — the cursor only ever moves forward past a row after that row's
// entire contribution has been appended, so a resumption can neither repeat
// nor skip the null-filled row.
//
// Right/full-outer build-side match marking likewise happens only when a row
// is finished, so a suspended row is marked once, on the call that completes
// it, before FlushUnmatched can run.
func (p *HashJoinProbe) genericProbe(in *batch.RecordBatch, pairs []matchPair, limit int) ([]matchPair, bool) {
	h := p.join
	sel := in.Sel
	n := in.Len
	if sel != nil {
		n = len(sel)
	}
	// start is the index into this row's match list to resume at; it is reset
	// to 0 as soon as the suspended row is finished. accepted rides with it
	// on the Residual path — see probeResume.accepted.
	start := 0
	accepted := false
	if p.res.mid {
		start = int(p.res.ref)
		accepted = p.res.accepted
		p.res.mid = false
	}

	residual := h.Residual
	for pos := p.res.pos; pos < n; pos++ {
		row := pos
		if sel != nil {
			row = int(sel[pos])
		}
		buildMatches := p.lookupBuild(in, row)

		if len(buildMatches) == 0 {
			if h.JoinType == LeftJoin || h.JoinType == FullOuterJoin {
				if len(pairs) >= limit {
					p.res.pos, p.res.ref, p.res.mid, p.res.accepted = pos, 0, true, false
					return pairs, false
				}
				pairs = append(pairs, matchPair{probeRow: int32(row)})
			}
			start = 0
			continue
		}

		if residual != nil {
			// ON-residual path (#358): each key candidate must also pass the
			// residual on the combined row. A rejected candidate emits
			// nothing; a probe row whose whole chain was rejected is
			// UNMATCHED, which for LEFT/FULL means one NULL-padded emission.
			for i := start; i < len(buildMatches); i++ {
				ref := buildMatches[i]
				if !residual(in, row, h.buildBatches[ref.batchIdx], int(ref.rowIdx)) {
					continue
				}
				if len(pairs) >= limit {
					p.res.pos, p.res.ref, p.res.mid, p.res.accepted = pos, int32(i), true, accepted
					return pairs, false
				}
				pairs = append(pairs, matchPair{probeRow: int32(row), ref: ref, matched: true})
				accepted = true
				if h.arenaMatched != nil {
					// RIGHT/FULL unmatched tracking is per accepted build ROW,
					// not per key chain — see rowMatched.
					h.mu.Lock()
					h.markRowMatched(ref)
					h.mu.Unlock()
				}
			}
			if !accepted && (h.JoinType == LeftJoin || h.JoinType == FullOuterJoin) {
				if len(pairs) >= limit {
					p.res.pos, p.res.ref, p.res.mid, p.res.accepted = pos, int32(len(buildMatches)), true, false
					return pairs, false
				}
				pairs = append(pairs, matchPair{probeRow: int32(row)})
			}
			start, accepted = 0, false
			continue
		}

		for i := start; i < len(buildMatches); i++ {
			if len(pairs) >= limit {
				p.res.pos, p.res.ref, p.res.mid = pos, int32(i), true
				return pairs, false
			}
			pairs = append(pairs, matchPair{probeRow: int32(row), ref: buildMatches[i], matched: true})
		}
		start = 0

		if h.arenaMatched != nil {
			h.mu.Lock()
			p.markKeyMatched(in, row)
			h.mu.Unlock()
		}
	}
	p.res.pos = n
	return pairs, true
}

// nonNullProbeKeys returns a selection over in that drops every row with a
// NULL in any join key column, and ok=false when there is nothing to drop (no
// null bitmap on any key column, or a key column that does not resolve — the
// latter is a naming problem, not a NULL, and belongs to the paths below).
//
// It exists so the null-aware anti join can apply `k IS NULL → UNKNOWN` once,
// on the way in, instead of in each of the four typed probe loops — every one
// of which treats a NULL probe key as "no match" and therefore emits it.
func (p *HashJoinProbe) nonNullProbeKeys(in *batch.RecordBatch) ([]uint32, bool) {
	h := p.join
	h.resolveProbeKeyIdx(in)
	var nullCols []*batch.Vector
	for _, idx := range h.probeKeyIdx {
		if idx < 0 {
			return nil, false
		}
		if col := in.Columns[idx]; col.Nulls.HasNulls() {
			nullCols = append(nullCols, col)
		}
	}
	if len(nullCols) == 0 {
		return nil, false
	}
	keep := func(row int) bool {
		for _, col := range nullCols {
			if col.Nulls.IsNullFast(row) {
				return false
			}
		}
		return true
	}
	sel := make([]uint32, 0, in.ActiveLen())
	if in.Sel != nil {
		for _, idx := range in.Sel {
			if keep(int(idx)) {
				sel = append(sel, idx)
			}
		}
	} else {
		for i := 0; i < in.Len; i++ {
			if keep(i) {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel, true
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

	// NOT IN is three-valued, and an anti join on its own is not (#507). Two
	// rules restore the difference, and both are applied here so every probe
	// path below sees only the rows an ordinary anti join may answer for.
	//
	// Both rules are about a COMPARISON, so both need something to compare
	// against: over an EMPTY subquery `k NOT IN ()` is TRUE for every row
	// INCLUDING a NULL-keyed one, because there is no value for the
	// comparison to be UNKNOWN about. Dropping the NULL-keyed rows there
	// answered 2 where PostgreSQL answers 3. buildRows counts NULL-keyed
	// build rows too (#496), so a subquery that yields only NULLs is not
	// empty and still poisons.
	if h.NullAwareAnti && !isSemi && h.buildRows > 0 {
		// A NULL anywhere in the subquery's result makes `k <> that value`
		// UNKNOWN for every k it did not match on some other value, so the
		// predicate is TRUE for nothing at all.
		if h.buildHasNullKey {
			return nil, nil
		}
		// And a probe row whose own key is NULL compares UNKNOWN against
		// every value, matched or not — WHERE drops it either way.
		if sel, ok := p.nonNullProbeKeys(in); ok {
			if len(sel) == 0 {
				return nil, nil
			}
			in.Sel = sel
		}
	}

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

	// Distinct-pair NE path (join_semianti_ne.go): lookup + ≤2 compares per
	// row, no chain walk, no filter closure. Takes precedence over the
	// generic filtered path when the build activated.
	if h.useIntKey && h.neActive {
		h.resolveProbeKeyIdx(in)
		keyIdx := h.probeKeyIdx[0]
		valIdx := columnIndexFallback(in, h.SemiAntiNEProbeCol)
		if keyIdx >= 0 && valIdx >= 0 {
			sel = h.probeNESemiAnti(in, keyIdx, valIdx, isSemi, sel)
			goto done
		}
		// Probe column unresolvable — fall through to the closure path,
		// which reports the same rows via GetValue-based comparison...
		// except the NE build stored no batches, so the closure path
		// cannot run. Fail safe: emit nothing for semi, everything for
		// anti ONLY via the closure would be wrong; instead treat as a
		// planner/runtime contract violation loudly.
		return nil, fmt.Errorf("semi/anti NE probe column %q unresolvable in probe schema", h.SemiAntiNEProbeCol)
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
		if !p.buildProbeKey(in, row) {
			return // NULL key: matches nothing, so it marks nothing
		}
		head, ok := h.strIndex.Get(p.keyBuf)
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	}
}

// nextCrossChunk emits the next bounded slice of a cross join's output. A
// cross join's fan-out is the entire build side per probe row, so it needs the
// same suspension as the hash paths; the cursor here is (probe position,
// build batch, row within that build batch).
func (p *HashJoinProbe) nextCrossChunk() (*batch.RecordBatch, error) {
	in := p.res.in
	outSchema := p.res.crossSchema
	_, mapping := p.outputSchemaWithMapping(in.Schema)
	limit := p.pairLimit()

	sel := in.Sel
	n := in.Len
	if sel != nil {
		n = len(sel)
	}

	var pairs []crossPair
	done := true

expand:
	for pos := p.res.pos; pos < n; pos++ {
		row := pos
		if sel != nil {
			row = int(sel[pos])
		}
		for bi := p.res.crossBatch; bi < len(p.join.buildBatches); bi++ {
			buildBatch := p.join.buildBatches[bi]
			bn := buildBatch.Len
			if buildBatch.Sel != nil {
				bn = len(buildBatch.Sel)
			}
			for br := p.res.crossRow; br < bn; br++ {
				if len(pairs) >= limit {
					p.res.pos, p.res.crossBatch, p.res.crossRow = pos, bi, br
					done = false
					break expand
				}
				buildRow := br
				if buildBatch.Sel != nil {
					buildRow = int(buildBatch.Sel[br])
				}
				pairs = append(pairs, crossPair{probeRow: row, batchIdx: int32(bi), buildRow: buildRow})
			}
			p.res.crossRow = 0
		}
		p.res.crossBatch = 0
	}

	if done {
		p.finishResume()
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

// residentBuildBatch returns the in-memory build batch an arena entry points
// at, or nil when that entry's rows are no longer resident.
//
// Two ways an entry stops being resident, and both are answers rather than
// errors here:
//
//   - Its PARTITION WAS EVICTED. spillOneInMemoryPartition writes the
//     partition's batches to disk and nils their h.buildBatches slots, leaving
//     the arena entries that point at them in place — its correctness argument
//     covers the in-memory PROBE path (partition routing diverts a probe row
//     for a spilled partition to disk before any hash lookup), and the
//     build-side flushes are not that path. They walk the arena directly, so
//     they used to dereference the nil slot and take the whole query down with
//     a nil pointer panic on any spilling RIGHT/FULL/RIGHT-ANTI join (#550).
//     Those rows are NOT lost by skipping them: NextFlush replays every
//     spilled partition from disk through a temp join whose own flush emits
//     them, and that replay reads the partition's COMPLETE contents — the
//     batches evicted here plus every row that arrived for the partition
//     afterwards, which was never indexed and has no arena entry at all.
//     Emitting them here as well would double them.
//   - The index outruns the slice, which nothing is expected to do; it was
//     already tolerated by two of the three callers and is kept.
func (h *HashJoin) residentBuildBatch(ref buildRef) *batch.RecordBatch {
	if int(ref.batchIdx) >= len(h.buildBatches) {
		return nil
	}
	return h.buildBatches[ref.batchIdx]
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
			if p.join.residentBuildBatch(ref) == nil {
				continue // evicted partition — the spilled replay emits it
			}
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
			buildBatch := p.join.residentBuildBatch(ref)
			if buildBatch == nil || int(ref.rowIdx) >= buildBatch.Len {
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
			if p.join.residentBuildBatch(ref) == nil {
				continue // evicted partition — the spilled replay emits it
			}
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
			buildBatch := p.join.residentBuildBatch(ref)
			if buildBatch == nil || int(ref.rowIdx) >= buildBatch.Len {
				continue
			}
			copyVectorValue(dst, outRow, buildBatch.Columns[colIdx], int(ref.rowIdx))
		}
	}
	return out
}

// recordProbeSchema remembers the probe input schema for FlushUnmatched. Every
// clone shares one HashJoin, so the first to run wins and the rest are no-ops.
func (p *HashJoinProbe) recordProbeSchema(schema []parquet.Column) {
	h := p.join
	h.mu.Lock()
	if h.probeSchema == nil && len(schema) > 0 {
		h.probeSchema = schema
	}
	h.mu.Unlock()
}

// FlushUnmatchedRows emits a RIGHT/FULL join's unmatched build rows exactly
// once per join, whichever probe clone reaches it first, and names the probe
// half of each row from the schema the probe itself observed.
//
// It is the single entry point for the two drivers that own the end of a probe
// pipeline: physical.joinFlushSource (single process) and the worker's
// FlushableOperator drain (stage DAG). Before it existed only the first of
// those flushed at all, so every unmatched row of a distributed RIGHT or FULL
// join was dropped (#352) — and the caller passed the join's OUTPUT schema
// where the PROBE schema was wanted, so on the shapes where it did run the
// preserved side came back NULL.
func (p *HashJoinProbe) FlushUnmatchedRows() *batch.RecordBatch {
	h := p.join
	if h.JoinType != RightJoin && h.JoinType != FullOuterJoin {
		return nil
	}
	h.mu.Lock()
	if h.unmatchedFlushed {
		h.mu.Unlock()
		return nil
	}
	h.unmatchedFlushed = true
	h.mu.Unlock()
	return p.FlushUnmatched(nil)
}

// FlushUnmatched returns a RecordBatch containing build-side rows that were never
// matched during probing. For RightJoin and FullOuterJoin only.
//
// leftSchema is a fallback for the probe-side schema: the probe's own cached
// mapping is preferred (it is what its output batches were built from), then
// the schema recorded on the first Execute, then the plan-declared
// ProbeSchemaHint. A caller with nothing better may pass nil.
func (p *HashJoinProbe) FlushUnmatched(leftSchema []parquet.Column) *batch.RecordBatch {
	if p.join.JoinType != RightJoin && p.join.JoinType != FullOuterJoin {
		return nil
	}

	// Collect unmatched build refs from arena. With a Residual active the
	// matched bit lives per build ROW (rowMatched), because a residual accepts
	// individual chain candidates; without one it lives per arena entry.
	var refs []buildRef
	if p.join.Residual != nil {
		for _, ref := range p.join.arena {
			if p.join.refMatched(ref) {
				continue
			}
			if p.join.residentBuildBatch(ref) == nil {
				continue // evicted partition — the spilled replay emits it
			}
			refs = append(refs, ref)
		}
	} else {
		for i, ref := range p.join.arena {
			if p.join.arenaMatched != nil && p.join.arenaMatched[i] {
				continue
			}
			if p.join.residentBuildBatch(ref) == nil {
				continue // evicted partition — the spilled replay emits it
			}
			refs = append(refs, ref)
		}
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

	outSchema, mapping := p.cachedSchema, p.cachedMapping
	if outSchema == nil {
		probeSchema := leftSchema
		if probeSchema == nil {
			p.join.mu.Lock()
			probeSchema = p.join.probeSchema
			if probeSchema == nil {
				probeSchema = p.join.ProbeSchemaHint
			}
			p.join.mu.Unlock()
		}
		outSchema, mapping = p.outputSchemaWithMapping(probeSchema)
	}
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

// Close drops the fan-out cursor's reference to the last input batch. A probe
// closed mid-suspension (a cancelled query) would otherwise keep that batch
// reachable for as long as the operator is.
func (p *HashJoinProbe) Close() error {
	p.res = probeResume{}
	return nil
}

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
	h.rowMatched = nil
	h.intIndex = nil
	h.strIndex = nil
	h.bloom = nil
	h.bloomMask = 0
	return nil
}

// Clone returns a new HashJoinProbe that shares the same build-side hash table
// but has its own scratch buffers (pairsBuf, semiSelBuf, lookupBuf, indexBuf)
// and its own fan-out cursor — parallel pipeline workers each suspend and
// resume independently.
func (p *HashJoinProbe) Clone() UnaryOperator {
	c := p.join.Probe()
	c.OutputFilter = p.OutputFilter
	c.LateMaterialize = p.LateMaterialize
	c.boundOutput = p.boundOutput
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
	return joinOutputSchemaWithMapping(p.join.JoinType, leftSchema, p.join.buildSchema,
		p.join.BuildTableAlias, p.join.BuildColOrigins, p.join.QualifyAllBuildCols, p.OutputFilter)
}

// joinOutputSchemaWithMapping computes a join's output schema — probe columns
// first, then build columns with duplicate-name qualification — and the
// per-output-column source mapping. Shared by HashJoinProbe and SortMergeJoin
// so both emit identical schemas for the same join shape.
func joinOutputSchemaWithMapping(joinType JoinType, leftSchema, buildSchema []parquet.Column, buildAlias string, buildColOrigins map[string]string, qualifyAllBuildCols bool, outputFilter map[string]bool) ([]parquet.Column, []outColSource) {
	var out []parquet.Column
	var mapping []outColSource

	if joinType == RightJoin || joinType == FullOuterJoin {
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

	for i, col := range buildSchema {
		isDup := seen[col.Name]
		// A build column that already carries a qualifier was named by a
		// nested join INSIDE the build subtree (bushy shapes). The name is
		// already unique and stable — re-qualifying would double-qualify
		// ("s.n2.n_name") and dropping it would lose the column. Emit verbatim.
		if strings.IndexByte(col.Name, '.') >= 0 {
			if joinType == LeftJoin || joinType == FullOuterJoin {
				col.Nullable = true
			}
			out = append(out, col)
			mapping = append(mapping, outColSource{fromProbe: false, srcIdx: i})
			seen[col.Name] = true
			continue
		}
		// Qualification alias: the column's OWNING scan when the planner
		// provided per-column origins (multi-table build subtrees), else the
		// build side's single table alias.
		alias := buildAlias
		if origin := buildColOrigins[strings.ToLower(col.Name)]; origin != "" {
			alias = origin
		}
		// Force qualification for self-join scenarios — the planner sets
		// QualifyAllBuildCols when this build is one of two co-pathing
		// scans of the same source table. Without it, the FIRST scan's
		// column ships unqualified ("n_name") and downstream lookups for
		// the qualified name ("n1.n_name") miss.
		shouldQualify := (isDup || qualifyAllBuildCols) && alias != ""
		switch {
		case shouldQualify:
			qualCol := col
			qualCol.Name = alias + "." + col.Name
			if joinType == LeftJoin || joinType == FullOuterJoin {
				qualCol.Nullable = true
			}
			out = append(out, qualCol)
			mapping = append(mapping, outColSource{fromProbe: false, srcIdx: i})
		case isDup:
			// Duplicate with no alias to disambiguate by — skip (backward compatible).
		default:
			if joinType == LeftJoin || joinType == FullOuterJoin {
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
	if len(outputFilter) > 0 {
		var filteredSchema []parquet.Column
		var filteredMapping []outColSource
		for i, col := range out {
			keep := outputFilter[col.Name]
			// For qualified columns (e.g., "n2.n_name" from self-joins), also
			// check if the unqualified base name is needed. Without this, the
			// output filter would drop disambiguated self-join columns.
			if !keep {
				if dot := strings.IndexByte(col.Name, '.'); dot >= 0 {
					keep = outputFilter[col.Name[dot+1:]]
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
		//
		// Runs of ascending-contiguous build rows collapse into ONE
		// BulkCopy instead of per-row SetFrom appends (the 2026-07-17
		// bytes-run shape from appendBatchRowsBulk, transplanted): a
		// fact-side build stores rows in insertion order, so a probe hit
		// against a clustered key (lineitem by l_orderkey/l_partkey —
		// the Q17/Q18/Q21 class) matches consecutive build rows, and the
		// per-row ~25 B memmove fixed cost dominated the copy. The
		// 2026-08-12 treatment profile put SetFrom-under-gather at
		// 380 CPU-s/suite, 81% of it memmove. Runs break at batch
		// switches, unmatched pairs, and source nulls (a null row's
		// offsets may be malformed — see BytesColumn.Value). Singletons
		// keep the SetFrom fast path: the run probe costs one compare on
		// the next pair, and hash-scattered layouts stay at parity.
		n := len(pairs)
		if allMatched {
			for di := 0; di < n; {
				pair := pairs[di]
				if bi := pair.ref.batchIdx; bi != prevBatch {
					src = buildBatches[bi].Columns[srcIdx]
					prevBatch = bi
					srcHasNulls = src.Nulls.HasNulls()
				}
				si := int(pair.ref.rowIdx)
				if srcHasNulls && src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
					dst.BytesData.Offsets[di+1] = dst.BytesData.Offsets[di]
					di++
					continue
				}
				// Probe order: rowIdx first — on hash-scattered layouts it
				// fails immediately and the row takes the SetFrom fast path
				// at parity with the pre-run code.
				run := 1
				for di+run < n {
					np := pairs[di+run]
					if int(np.ref.rowIdx) != si+run ||
						np.ref.batchIdx != pair.ref.batchIdx ||
						(srcHasNulls && src.Nulls.IsNullFast(si+run)) {
						break
					}
					run++
				}
				if run == 1 {
					dst.BytesData.SetFrom(di, &src.BytesData, si)
					di++
					continue
				}
				dst.BytesData.BulkCopy(di, &src.BytesData, si, run)
				di += run
			}
		} else {
			for di := 0; di < n; {
				pair := pairs[di]
				if !pair.matched {
					dst.Nulls.SetNull(di)
					dst.BytesData.Offsets[di+1] = dst.BytesData.Offsets[di]
					di++
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
					di++
					continue
				}
				run := 1
				for di+run < n {
					np := pairs[di+run]
					if int(np.ref.rowIdx) != si+run ||
						np.ref.batchIdx != pair.ref.batchIdx ||
						!np.matched ||
						(srcHasNulls && src.Nulls.IsNullFast(si+run)) {
						break
					}
					run++
				}
				if run == 1 {
					dst.BytesData.SetFrom(di, &src.BytesData, si)
					di++
					continue
				}
				dst.BytesData.BulkCopy(di, &src.BytesData, si, run)
				di += run
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
