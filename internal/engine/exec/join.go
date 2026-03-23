package exec

import (
	"context"
	"fmt"
	"runtime"
	"slices"
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
	SemiJoin // returns left row if match found, no duplicates
	AntiJoin // returns left row only if NO match found
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

	mu           sync.Mutex
	buildBatches []*batch.RecordBatch // columnar storage of build side
	strIndex     *strHashTable     // arena-based hash table for string keys (general path)
	intIndex     *intHashTable    // fast path: single-column integer join key
	arena        []buildRef           // flat storage for all build refs
	arenaNext    []int32              // chain: arenaNext[i] = next arena index for same key (-1 = end)
	useIntKey    bool                 // true when single int32/int64 join key detected
	useDualIntKey bool               // true when exactly two int32/int64 join keys
	buildDone    bool
	buildSchema  []parquet.Column
	buildRows    int64 // total rows in build side

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

	// Pre-resolved column indices and reusable key buffer for typed serialization.
	// Avoids fmt.Sprint + GetValue boxing on every row.
	keyBuf       []byte
	buildKeyIdx  []int // column indices for RightKeys in build batches
	probeKeyIdx  []int // column indices for LeftKeys in probe batches
	probeResolved bool

	// SemiAntiKeyOnly enables a lightweight build for semi/anti joins that have
	// no SemiAntiFilter. Only the key index and bloom filter are built — batch
	// storage and arena refs are skipped. Reduces memory and build time by ~2-4x
	// for large build sides (e.g., 6M-row lineitem scan for EXISTS subqueries).
	SemiAntiKeyOnly bool

	// BuildRowHint is an optional hint for the expected number of build-side rows.
	// When set, the arena and hash table are pre-allocated to avoid repeated growth.
	BuildRowHint int64

	// Bloom filter for fast negative lookups during probe phase.
	// When the build side is small relative to expected probe volume,
	// this rejects non-matching probe rows without touching the hash table.
	bloom     []uint64
	bloomMask uint64

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
// Uses Put's return value to get the old chain head in a single hash probe,
// avoiding a redundant Get + Put double-probe.
func (h *HashJoin) arenaAppendInt(key int64, ref buildRef) {
	idx := int32(len(h.arena))
	h.arena = append(h.arena, ref)
	old, existed := h.intIndex.Put(key, idx)
	if existed {
		h.arenaNext = append(h.arenaNext, old)
	} else {
		h.arenaNext = append(h.arenaNext, -1)
	}
}

func (h *HashJoin) arenaAppendStr(ref buildRef) {
	idx := int32(len(h.arena))
	h.arena = append(h.arena, ref)
	head, existed := h.strIndex.Put(h.keyBuf, idx)
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
		if !h.probeResolved {
			h.probeKeyIdx = make([]int, len(h.LeftKeys))
			for i, col := range h.LeftKeys {
				h.probeKeyIdx[i] = in.ColumnIndex(col)
			}
			h.probeResolved = true
		}
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
		if !h.probeResolved {
			h.probeKeyIdx = make([]int, len(h.LeftKeys))
			for i, col := range h.LeftKeys {
				h.probeKeyIdx[i] = in.ColumnIndex(col)
			}
			h.probeResolved = true
		}
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

// intProbeKey extracts the int64 probe key for the int fast path.
func (h *HashJoin) intProbeKey(in *batch.RecordBatch, row int) (int64, bool) {
	h.resolveProbeKeyIdx(in)
	if h.probeKeyIdx[0] < 0 {
		return 0, false
	}
	return intKeyFromVector(in.Columns[h.probeKeyIdx[0]], row)
}

// EstimateBatchBytes estimates the memory footprint of a RecordBatch.
func EstimateBatchBytes(b *batch.RecordBatch) int64 {
	var size int64
	for _, v := range b.Columns {
		switch v.Type {
		case batch.TypeBool:
			size += int64(len(v.BoolData))
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			size += int64(len(v.Int32Data)) * 4
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			size += int64(len(v.Int64Data)) * 8
		case batch.TypeFloat32:
			size += int64(len(v.Float32Data)) * 4
		case batch.TypeFloat64:
			size += int64(len(v.Float64Data)) * 8
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			size += int64(b.Len) * 48 // rough estimate: offset array + string data
		case batch.TypeDecimal:
			size += int64(len(v.DecimalData.Data)) * 16
		}
	}
	// Hash index overhead: ~40 bytes per row (key string + buildRef + map bucket)
	size += int64(b.Len) * 40
	return size
}

// Build consumes all rows from the build (right) side into the columnar hash table.
// Uses parallel workers when the build side is large enough to benefit from
// concurrent hash table construction with per-worker local tables.
func (h *HashJoin) Build(ctx context.Context, source Source) error {
	if err := source.Init(ctx); err != nil {
		return fmt.Errorf("build source init: %w", err)
	}
	defer source.Close()

	// Use parallel build when: enough CPUs and no spill/memory tracking
	// (complex interactions with partitioning).
	workers := runtime.NumCPU()
	if workers > 1 && h.Spill == nil && h.MemTracker == nil {
		if h.SemiAntiKeyOnly {
			return h.buildParallelKeyOnly(ctx, source, workers)
		}
		return h.buildParallel(ctx, source, workers)
	}

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

		h.mu.Lock()
		if h.buildSchema == nil {
			h.buildSchema = b.Schema
			// Pre-resolve build key column indices
			h.buildKeyIdx = make([]int, len(h.RightKeys))
			for i, col := range h.RightKeys {
				h.buildKeyIdx[i] = b.ColumnIndex(col)
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
			// Key-only build: populate index without storing batches or arena refs.
			// Semi/anti joins only need key existence, not row data.
			if h.useIntKey {
				col := b.Columns[h.buildKeyIdx[0]]
				if b.Sel != nil {
					for _, si := range b.Sel {
						key, ok := intKeyFromVector(col, int(si))
						if !ok {
							continue
						}
						h.intIndex.Put(key, 0)
					}
				} else if !col.Nulls.HasNulls() {
					switch col.Type {
					case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
						data := col.Int32Data
						for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
							h.intIndex.Put(int64(data[rowIdx]), 0)
						}
					default:
						data := col.Int64Data
						for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
							h.intIndex.Put(data[rowIdx], 0)
						}
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						key, ok := intKeyFromVector(col, rowIdx)
						if !ok {
							continue
						}
						h.intIndex.Put(key, 0)
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
						h.intIndex.Put(dualIntHash(a, bb), 0)
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
						if !ok {
							continue
						}
						h.intIndex.Put(dualIntHash(a, bb), 0)
					}
				}
			} else {
				if h.strIndex == nil {
					h.strIndex = newStrHashTable(64)
				}
				if b.Sel != nil {
					for _, si := range b.Sel {
						h.buildKeyFromBatch(b, int(si))
						h.strIndex.GetOrInsert(h.keyBuf, 0)
					}
				} else {
					for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
						h.buildKeyFromBatch(b, rowIdx)
						h.strIndex.GetOrInsert(h.keyBuf, 0)
					}
				}
			}
			h.buildRows += int64(b.ActiveLen())
			h.mu.Unlock()
			continue
		}

		// Spill to disk if memory pressure is high
		if h.Spill != nil && h.Spill.ShouldSpill() && (len(h.buildBatches) > 0 || h.spillState != nil) {
			if err := h.spillBuildBatches(0); err != nil {
				h.mu.Unlock()
				return fmt.Errorf("spilling build side: %w", err)
			}
		}

		// Track memory if budget is set
		if h.MemTracker != nil {
			cost := EstimateBatchBytes(b)
			if err := h.MemTracker.Reserve(cost); err != nil {
				// Try spilling before giving up
				if h.Spill != nil {
					if spillErr := h.spillBuildBatches(cost); spillErr == nil {
						// After spill, tracker is reset to reflect only in-memory partitions.
						// Try Reserve again — should succeed if enough was spilled.
						if err2 := h.MemTracker.Reserve(cost); err2 != nil {
							h.mu.Unlock()
							return fmt.Errorf("hash join build: %w (build_rows=%d, batches=%d)",
								err2, h.buildRows, len(h.buildBatches))
						}
					}
				} else {
					h.mu.Unlock()
					return fmt.Errorf("hash join build: %w (build_rows=%d, batches=%d)",
						err, h.buildRows, len(h.buildBatches))
				}
			}
		}

		// If spill state is active, route new batches through partitioning
		if h.spillState != nil {
			b.Detach()
			h.partitionBuildBatch(b)
			h.mu.Unlock()
			continue
		}

		// Skip Compact() — iterate through Sel (if any) directly.
		// Avoids copying entire batch just to remove selection vector gaps.
		// Arena refs store original row indices, which are valid for direct access.
		b.Detach() // prevent pooled batches from being recycled — build stores references
		batchIdx := int32(len(h.buildBatches))
		h.buildBatches = append(h.buildBatches, b)

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
				h.strIndex = newStrHashTable(64)
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
		h.mu.Unlock()
	}

	// When Grace Hash Join is active, rebuild hash table from in-memory partitions only.
	// Spilled partitions will be processed one at a time during probe flush.
	if h.spillState != nil {
		if err := h.reloadInMemoryPartitions(); err != nil {
			return fmt.Errorf("rebuilding in-memory partitions: %w", err)
		}
	}

	// Allocate matched bitmap for right/full outer join tracking
	if (h.JoinType == RightJoin || h.JoinType == FullOuterJoin) && len(h.arena) > 0 {
		h.arenaMatched = make([]bool, len(h.arena))
	}

	// Build bloom filter for fast negative lookups during probe.
	h.buildBloom()

	h.buildDone = true
	return nil
}

// localBuild accumulates hash table state for one parallel build worker.
// Each worker builds into its own local hash table and arena to avoid
// contention. After all workers finish, locals are merged into the main
// HashJoin state.
type localBuild struct {
	batches   []*batch.RecordBatch
	arena     []buildRef
	arenaNext []int32
	intIndex  *intHashTable
	strIndex  *strHashTable
	keyBuf    []byte
	buildRows int64
}

func (lb *localBuild) appendInt(key int64, ref buildRef) {
	idx := int32(len(lb.arena))
	lb.arena = append(lb.arena, ref)
	if prev, ok := lb.intIndex.Put(key, idx); ok {
		lb.arenaNext = append(lb.arenaNext, prev)
	} else {
		lb.arenaNext = append(lb.arenaNext, -1)
	}
}

func (lb *localBuild) appendStr(ref buildRef, key []byte) {
	idx := int32(len(lb.arena))
	lb.arena = append(lb.arena, ref)
	if prev, ok := lb.strIndex.GetOrInsert(key, idx); ok {
		lb.arenaNext = append(lb.arenaNext, prev)
	} else {
		lb.arenaNext = append(lb.arenaNext, -1)
	}
}

// buildParallel uses per-worker local hash tables for concurrent build.
// Each worker reads batches (serialized by mutex), inserts into its local
// table (no contention, fits in L2 cache), then all locals are merged.
func (h *HashJoin) buildParallel(ctx context.Context, source Source, workers int) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("join build cancelled: %w", err)
	}

	// Read first batch to initialize schema, key indices, and hash type.
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

	// Create per-worker local accumulators.
	hint := 64
	if h.BuildRowHint > 0 {
		hint = int(h.BuildRowHint) / workers
		if hint < 64 {
			hint = 64
		}
	}
	locals := make([]*localBuild, workers)
	for i := range locals {
		lb := &localBuild{
			keyBuf: make([]byte, 0, 128),
		}
		if h.useIntKey || h.useDualIntKey {
			lb.intIndex = newIntHashTable(hint)
		} else {
			lb.strIndex = newStrHashTable(hint)
		}
		if h.BuildRowHint > 0 {
			lb.arena = make([]buildRef, 0, hint)
			lb.arenaNext = make([]int32, 0, hint)
		}
		locals[i] = lb
	}

	// Process first batch in worker 0's local.
	h.processLocalBatch(locals[0], first)

	// Launch workers.
	var sourceMu sync.Mutex
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(lb *localBuild) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				sourceMu.Lock()
				b, err := source.Next(ctx)
				sourceMu.Unlock()
				if err != nil {
					firstErr.CompareAndSwap(nil, fmt.Errorf("build source next: %w", err))
					return
				}
				if b == nil {
					return
				}
				h.processLocalBatch(lb, b)
			}
		}(locals[i])
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return v.(error)
	}

	// Merge all local builds into the main hash join state.
	h.mergeLocalBuilds(locals)

	// Allocate matched bitmap for right/full outer join tracking.
	if (h.JoinType == RightJoin || h.JoinType == FullOuterJoin) && len(h.arena) > 0 {
		h.arenaMatched = make([]bool, len(h.arena))
	}

	h.buildBloom()
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
				sourceMu.Unlock()
				if err != nil {
					firstErr.CompareAndSwap(nil, fmt.Errorf("build source next: %w", err))
					return
				}
				if b == nil {
					return
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
func (h *HashJoin) insertKeyOnlyBatch(lk *localKeyBuild, b *batch.RecordBatch) {
	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				key, ok := intKeyFromVector(col, int(si))
				if !ok {
					continue
				}
				lk.intIndex.Put(key, 0)
			}
		} else if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					lk.intIndex.Put(int64(data[rowIdx]), 0)
				}
			default:
				data := col.Int64Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					lk.intIndex.Put(data[rowIdx], 0)
				}
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					continue
				}
				lk.intIndex.Put(key, 0)
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
				lk.intIndex.Put(dualIntHash(a, bb), 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
					continue
				}
				lk.intIndex.Put(dualIntHash(a, bb), 0)
			}
		}
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
				lk.strIndex.GetOrInsert(lk.keyBuf, 0)
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
				lk.strIndex.GetOrInsert(lk.keyBuf, 0)
			}
		}
	}
	lk.rows += int64(b.ActiveLen())
}

// processLocalBatch inserts one batch into a worker-local build accumulator.
// Caller must not hold any locks — this function is lock-free per worker.
func (h *HashJoin) processLocalBatch(lb *localBuild, b *batch.RecordBatch) {
	b.Detach()
	batchIdx := int32(len(lb.batches))
	lb.batches = append(lb.batches, b)

	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				key, ok := intKeyFromVector(col, int(si))
				if !ok {
					continue
				}
				lb.appendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				lb.buildRows++
			}
		} else if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					lb.appendInt(int64(data[rowIdx]), buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				}
			default:
				data := col.Int64Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					lb.appendInt(data[rowIdx], buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				}
			}
			lb.buildRows += int64(b.Len)
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					continue
				}
				lb.appendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				lb.buildRows++
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
				lb.appendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				lb.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
					continue
				}
				lb.appendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				lb.buildRows++
			}
		}
	} else {
		if lb.strIndex == nil {
			lb.strIndex = newStrHashTable(64)
		}
		if b.Sel != nil {
			for _, si := range b.Sel {
				lb.keyBuf = lb.keyBuf[:0]
				for _, idx := range h.buildKeyIdx {
					if idx < 0 {
						lb.keyBuf = append(lb.keyBuf, 1)
						continue
					}
					v := b.Columns[idx]
					if v.Nulls.IsNullFast(int(si)) {
						lb.keyBuf = append(lb.keyBuf, 1)
					} else {
						lb.keyBuf = append(lb.keyBuf, 0)
						lb.keyBuf = appendColumnValue(lb.keyBuf, v, int(si), v.Type)
					}
				}
				lb.appendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(si)}, lb.keyBuf)
				lb.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				lb.keyBuf = lb.keyBuf[:0]
				for _, idx := range h.buildKeyIdx {
					if idx < 0 {
						lb.keyBuf = append(lb.keyBuf, 1)
						continue
					}
					v := b.Columns[idx]
					if v.Nulls.IsNullFast(rowIdx) {
						lb.keyBuf = append(lb.keyBuf, 1)
					} else {
						lb.keyBuf = append(lb.keyBuf, 0)
						lb.keyBuf = appendColumnValue(lb.keyBuf, v, rowIdx, v.Type)
					}
				}
				lb.appendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)}, lb.keyBuf)
				lb.buildRows++
			}
		}
	}
}

// mergeLocalBuilds combines per-worker local accumulators into the main
// HashJoin state. Concatenates batches and arenas, then re-inserts hash
// table entries with adjusted indices. O(distinct_keys) hash work, not
// O(total_rows), since each key appears once per local table.
func (h *HashJoin) mergeLocalBuilds(locals []*localBuild) {
	// Count totals for pre-allocation.
	var totalArena, totalBatches int
	for _, lb := range locals {
		totalArena += len(lb.arena)
		totalBatches += len(lb.batches)
		h.buildRows += lb.buildRows
	}

	h.buildBatches = make([]*batch.RecordBatch, 0, totalBatches)
	h.arena = make([]buildRef, 0, totalArena)
	h.arenaNext = make([]int32, 0, totalArena)

	// Pre-size the merged hash table for the total row count.
	if h.useIntKey || h.useDualIntKey {
		h.intIndex = newIntHashTable(int(h.buildRows))
	} else {
		h.strIndex = newStrHashTable(int(h.buildRows))
	}

	for _, lb := range locals {
		batchOffset := int32(len(h.buildBatches))
		arenaOffset := int32(len(h.arena))

		// Append batches.
		h.buildBatches = append(h.buildBatches, lb.batches...)

		// Append arena with adjusted batchIdx.
		for _, ref := range lb.arena {
			h.arena = append(h.arena, buildRef{
				batchIdx: ref.batchIdx + batchOffset,
				rowIdx:   ref.rowIdx,
			})
		}

		// Append arenaNext with adjusted chain links.
		for _, next := range lb.arenaNext {
			if next >= 0 {
				h.arenaNext = append(h.arenaNext, next+arenaOffset)
			} else {
				h.arenaNext = append(h.arenaNext, -1)
			}
		}

		// Re-insert hash entries with adjusted arena indices.
		// When a key exists in the merged table, link the local chain's
		// tail to the existing merged chain head.
		if h.useIntKey || h.useDualIntKey {
			lb.intIndex.ForEach(func(key int64, localHead int32) {
				adjustedHead := localHead + arenaOffset
				if existingHead, found := h.intIndex.Get(key); found {
					// Find tail of the local chain (now in merged arenaNext).
					tail := adjustedHead
					for h.arenaNext[tail] >= 0 {
						tail = h.arenaNext[tail]
					}
					h.arenaNext[tail] = existingHead
				}
				h.intIndex.Put(key, adjustedHead)
			})
		} else if lb.strIndex != nil {
			lb.strIndex.ForEach(func(key []byte) {
				localHead, _ := lb.strIndex.Get(key)
				adjustedHead := localHead + arenaOffset
				if existingHead, found := h.strIndex.Get(key); found {
					tail := adjustedHead
					for h.arenaNext[tail] >= 0 {
						tail = h.arenaNext[tail]
					}
					h.arenaNext[tail] = existingHead
				}
				h.strIndex.Put(key, adjustedHead)
			})
		}
	}
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



// spillBuildBatches partitions current build batches by hash and spills the
// largest partition(s) to disk. Must be called with h.mu held.
// neededBytes is the amount of additional memory needed (0 = just reduce to
// under 80% threshold). When non-zero, partitions are spilled until
// in-memory usage + neededBytes fits within budget.
func (h *HashJoin) spillBuildBatches(neededBytes int64) error {
	ss := h.spillState
	if ss == nil {
		// First spill: initialize partition state and redistribute existing batches
		dir := h.Spill.SpillDir()
		ss = newSpillState(dir, h.buildSchema)
		h.spillState = ss

		// Redistribute existing build batches into partitions
		for _, b := range h.buildBatches {
			h.partitionBuildBatch(b)
		}

		// Clear the flat build state — partitions now own the data
		h.buildBatches = nil
		h.arena = h.arena[:0]
		h.arenaNext = h.arenaNext[:0]
		if h.useIntKey || h.useDualIntKey {
			h.intIndex = newIntHashTable(64)
		} else {
			h.strIndex = newStrHashTable(64)
		}
		h.buildRows = 0
	}

	// Spill largest partition(s) until memory pressure is resolved
	for {
		// Re-sync tracker with actual in-memory usage
		var inMem int64
		for p, mem := range ss.partMemory {
			if !ss.spilledParts[p] {
				inMem += mem
			}
		}
		if h.MemTracker != nil {
			h.MemTracker.Reset()
			h.MemTracker.ForceReserve(inMem)
		}

		// Check if we've freed enough
		if neededBytes > 0 {
			// Spill until there's room for the incoming batch
			if h.MemTracker == nil || inMem+neededBytes <= h.MemTracker.Budget() {
				break
			}
		} else {
			// Proactive spill: stop when under 80% threshold
			if h.MemTracker == nil || !h.Spill.ShouldSpill() {
				break
			}
		}

		partID := ss.largestInMemoryPartition()
		if partID < 0 {
			break // nothing left to spill
		}
		if _, err := ss.spillBuildPartition(partID); err != nil {
			return err
		}
	}

	return nil
}

// reloadInMemoryPartitions rebuilds the hash table from in-memory partitions only.
// Spilled partitions are NOT loaded — they'll be processed one at a time during probe.
func (h *HashJoin) reloadInMemoryPartitions() error {
	ss := h.spillState
	if ss == nil {
		return nil
	}

	// Count in-memory rows for pre-allocation
	var totalRows int
	for partID, batches := range ss.partBuildBatches {
		if ss.spilledParts[partID] {
			continue
		}
		for _, b := range batches {
			totalRows += b.Len
		}
	}

	// Reset build state
	h.buildBatches = nil
	h.buildRows = 0
	h.arena = make([]buildRef, 0, totalRows)
	h.arenaNext = make([]int32, 0, totalRows)
	if h.useIntKey || h.useDualIntKey {
		h.intIndex = newIntHashTable(totalRows)
	} else {
		h.strIndex = newStrHashTable(totalRows)
	}

	// Rebuild hash table from in-memory partitions
	for partID, batches := range ss.partBuildBatches {
		if ss.spilledParts[partID] {
			continue
		}
		for _, b := range batches {
			batchIdx := int32(len(h.buildBatches))
			h.buildBatches = append(h.buildBatches, b)
			if h.SemiAntiKeyOnly {
				h.indexBuildBatchKeyOnly(b)
			} else {
				h.indexBuildBatch(b, batchIdx)
			}
		}
	}

	return nil
}

// PruneBuildColumns removes non-essential columns from the build-side batches.
// For SEMI/ANTI joins, the build side never appears in the output, so after
// the hash index is built we only need columns referenced by SemiAntiFilter.
// If keepCols is empty and no SemiAntiFilter is set, buildBatches are cleared.
func (h *HashJoin) PruneBuildColumns(keepCols []string) {
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
	} else {
		if h.strIndex == nil {
			h.strIndex = newStrHashTable(64)
		}
		for i := 0; i < b.Len; i++ {
			h.buildKeyFromBatch(b, i)
			h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
			h.buildRows++
		}
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
			h.probeResolved = false // force re-resolution of probe key indices
		}
	}

	// Rebuild hash index if keys were swapped
	if needsRebuild {
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
			}
		} else {
			h.strIndex = newStrHashTable(totalBuildRows)
			for batchIdx, b := range h.buildBatches {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.buildKeyFromBatch(b, rowIdx)
					h.arenaAppendStr(buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
					h.buildRows++
				}
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
	return &HashJoinProbe{
		join:     h,
		pairsBuf: make([]matchPair, 0, 2*batch.DefaultBatchSize),
		indexBuf: make([]int, 0, 2*batch.DefaultBatchSize),
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
// Safe for concurrent calls: probeResolved is set after probeKeyIdx is fully
// populated, and repeated resolution produces identical results.
func (h *HashJoin) resolveProbeKeyIdx(b *batch.RecordBatch) {
	if h.probeResolved {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.probeResolved {
		return // another goroutine resolved while we waited
	}
	h.probeKeyIdx = make([]int, len(h.LeftKeys))
	for i, col := range h.LeftKeys {
		h.probeKeyIdx[i] = b.ColumnIndex(col)
	}
	h.probeResolved = true
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
// 4x DefaultBatchSize handles most 1:N join outputs without fresh allocation.
const joinOutputPoolSize = 4 * batch.DefaultBatchSize

// matchPair tracks a probe-build row match for output construction.
type matchPair struct {
	probeRow int
	ref      buildRef
	matched  bool
}

// HashJoinProbe is a UnaryOperator that probes the build-side hash table.
type HashJoinProbe struct {
	join       *HashJoin
	pairsBuf   []matchPair // reusable buffer to avoid per-batch allocation
	semiSelBuf []uint32    // reusable selection vector for semi/anti join output
	lookupBuf  []buildRef  // reusable buffer for lookupBuild results
	indexBuf   []int       // reusable buffer for probe-side gather indices
	keyBuf     []byte      // per-probe key serialization buffer (avoids race on shared h.keyBuf)

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

	// Grace Hash Join flush state — populated when spilled partitions are processed
	spillFlushResults []*batch.RecordBatch
	spillFlushIdx     int
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
					pairs = append(pairs, matchPair{probeRow: row})
				}
				return
			}

			for _, ref := range buildMatches {
				pairs = append(pairs, matchPair{probeRow: row, ref: ref, matched: true})
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
	if len(p.join.buildBatches) > 1 {
		slices.SortFunc(pairs, func(a, b matchPair) int {
			return int(a.ref.batchIdx) - int(b.ref.batchIdx)
		})
	}

	// Build output batch using precomputed column source mapping.
	// Two-pool strategy: standard pool for ≤DefaultBatchSize (common case,
	// cache-friendly), large pool for oversized 1:N outputs (avoids fresh alloc).
	var out *batch.RecordBatch
	if len(pairs) <= batch.DefaultBatchSize {
		if p.outPool == nil {
			p.outPool = batch.NewBatchPool(outSchema, batch.DefaultBatchSize)
		}
		out = p.outPool.Get()
		out.Len = len(pairs)
		for _, col := range out.Columns {
			col.Len = len(pairs)
			col.Nulls.ResetNonNull(len(pairs))
		}
	} else if len(pairs) <= joinOutputPoolSize {
		if p.largeOutPool == nil {
			p.largeOutPool = batch.NewBatchPool(outSchema, joinOutputPoolSize)
		}
		out = p.largeOutPool.GetForSize(len(pairs))
		out.Len = len(pairs)
		for _, col := range out.Columns {
			col.Len = len(pairs)
			col.Nulls.ResetNonNull(len(pairs))
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
		probeIndices[i] = pair.probeRow
	}

	for outColIdx, m := range mapping {
		dst := out.Columns[outColIdx]
		if m.fromProbe {
			gatherVector(dst, in.Columns[m.srcIdx], probeIndices)
		} else {
			allMatched := p.join.JoinType != LeftJoin && p.join.JoinType != FullOuterJoin
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
						pairs = append(pairs, matchPair{probeRow: int(si), ref: arena[ref], matched: true})
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
						pairs = append(pairs, matchPair{probeRow: int(si), ref: arena[ref], matched: true})
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
					pairs = append(pairs, matchPair{probeRow: int(si), ref: arena[ref], matched: true})
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
						pairs = append(pairs, matchPair{probeRow: i, ref: arena[ref], matched: true})
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
						pairs = append(pairs, matchPair{probeRow: i, ref: arena[ref], matched: true})
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
					pairs = append(pairs, matchPair{probeRow: i, ref: arena[ref], matched: true})
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
						pairs = append(pairs, matchPair{probeRow: int(si), ref: r, matched: true})
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
						pairs = append(pairs, matchPair{probeRow: int(si), ref: r, matched: true})
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
						pairs = append(pairs, matchPair{probeRow: i, ref: r, matched: true})
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
						pairs = append(pairs, matchPair{probeRow: i, ref: r, matched: true})
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
		if !h.probeResolved {
			h.probeKeyIdx = make([]int, len(h.LeftKeys))
			for i, col := range h.LeftKeys {
				h.probeKeyIdx[i] = in.ColumnIndex(col)
			}
			h.probeResolved = true
		}
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
		crossProbeIdx[i] = cp.probeRow
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
		if seen[col.Name] {
			if p.join.BuildTableAlias != "" {
				// Duplicate column: include with qualified name for disambiguation
				qualCol := col
				qualCol.Name = p.join.BuildTableAlias + "." + col.Name
				if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
					qualCol.Nullable = true
				}
				out = append(out, qualCol)
				mapping = append(mapping, outColSource{fromProbe: false, srcIdx: i})
			}
			// When no alias: skip duplicate (backward compatible)
		} else {
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
					dst.BytesData.Set(di, src.BytesData.Value(si))
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
					dst.BytesData.Set(di, src.BytesData.Value(si))
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
				dst.BytesData.Set(di, src.BytesData.Value(si))
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
	dst.Nulls.SetNull(row)
	switch dst.Type {
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.Set(row, nil)
	}
}

// semiProbeInt64 is the inlined semi-join probe for int64 keys without nulls or bloom.
// Emits rows whose key EXISTS in the hash table.
func semiProbeInt64(idx *intHashTable, data []int64, inSel []uint32, inLen int, sel []uint32) []uint32 {
	if inSel != nil {
		for _, si := range inSel {
			if _, ok := idx.Get(data[si]); ok {
				sel = append(sel, si)
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			if _, ok := idx.Get(data[i]); ok {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel
}

// semiProbeInt32 is the inlined semi-join probe for int32 keys without nulls or bloom.
func semiProbeInt32(idx *intHashTable, data []int32, inSel []uint32, inLen int, sel []uint32) []uint32 {
	if inSel != nil {
		for _, si := range inSel {
			if _, ok := idx.Get(int64(data[si])); ok {
				sel = append(sel, si)
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			if _, ok := idx.Get(int64(data[i])); ok {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel
}

// antiProbeInt64 is the inlined anti-join probe for int64 keys without nulls or bloom.
// Emits rows whose key does NOT exist in the hash table.
func antiProbeInt64(idx *intHashTable, data []int64, inSel []uint32, inLen int, sel []uint32) []uint32 {
	if inSel != nil {
		for _, si := range inSel {
			if _, ok := idx.Get(data[si]); !ok {
				sel = append(sel, si)
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			if _, ok := idx.Get(data[i]); !ok {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel
}

// antiProbeInt32 is the inlined anti-join probe for int32 keys without nulls or bloom.
func antiProbeInt32(idx *intHashTable, data []int32, inSel []uint32, inLen int, sel []uint32) []uint32 {
	if inSel != nil {
		for _, si := range inSel {
			if _, ok := idx.Get(int64(data[si])); !ok {
				sel = append(sel, si)
			}
		}
	} else {
		for i := 0; i < inLen; i++ {
			if _, ok := idx.Get(int64(data[i])); !ok {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel
}
