package exec

import (
	"context"
	"fmt"
	"sync"

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
	hashIndex    map[string]int32  // hash key -> head index in arena (general path)
	intIndex     *intHashTable    // fast path: single-column integer join key
	arena        []buildRef           // flat storage for all build refs
	arenaNext    []int32              // chain: arenaNext[i] = next arena index for same key (-1 = end)
	useIntKey    bool                 // true when single int32/int64 join key detected
	buildDone    bool
	buildSchema  []parquet.Column
	buildRows    int64 // total rows in build side

	// Memory tracking (optional). When set, Reserve() is called for each
	// build-side batch. If the budget is exceeded, Build returns ErrMemoryExceeded.
	MemTracker *memory.Tracker

	// Spill-to-disk (optional). When set, build-side batches are spilled to disk
	// when memory pressure exceeds 80% of budget, then re-loaded before probing.
	Spill      *memory.SpillManager
	spillFiles []string

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

	// Bloom filter for fast negative lookups during probe phase.
	// When the build side is small relative to expected probe volume,
	// this rejects non-matching probe rows without touching the hash table.
	bloom     []uint64
	bloomMask uint64
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(joinType JoinType, leftKeys, rightKeys []string) *HashJoin {
	hj := &HashJoin{
		JoinType:  joinType,
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
		hashIndex: make(map[string]int32),
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
func (h *HashJoin) tryEnableIntKey(b *batch.RecordBatch) {
	if len(h.buildKeyIdx) != 1 || h.buildKeyIdx[0] < 0 {
		return
	}
	col := b.Columns[h.buildKeyIdx[0]]
	if isIntKeyColumn(col.Type) {
		h.useIntKey = true
		h.intIndex = newIntHashTable(64)
		h.hashIndex = nil // free the string map
	}
}

// arenaAppendInt adds a buildRef to the arena and chains it under an int64 key.
func (h *HashJoin) arenaAppendInt(key int64, ref buildRef) {
	idx := int32(len(h.arena))
	h.arena = append(h.arena, ref)
	head, ok := h.intIndex.Get(key)
	if ok {
		h.arenaNext = append(h.arenaNext, head)
	} else {
		h.arenaNext = append(h.arenaNext, -1)
	}
	h.intIndex.Put(key, idx)
}

func (h *HashJoin) arenaAppendStr(key string, ref buildRef) {
	idx := int32(len(h.arena))
	h.arena = append(h.arena, ref)
	head, ok := h.hashIndex[key]
	if ok {
		h.arenaNext = append(h.arenaNext, head)
	} else {
		h.arenaNext = append(h.arenaNext, -1)
	}
	h.hashIndex[key] = idx
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
	h.buildProbeKeyBuf(in, row)
	if h.bloom != nil && !h.bloomMayContain(bloomHashStr(string(h.keyBuf))) {
		return false
	}
	_, ok := h.hashIndex[string(h.keyBuf)]
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
		// Bloom filter pre-check: reject definite non-matches
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
	h.buildProbeKeyBuf(in, row)
	// Bloom filter pre-check for string keys
	if h.bloom != nil && !h.bloomMayContain(bloomHashStr(string(h.keyBuf))) {
		return p.lookupBuf
	}
	head, ok := h.hashIndex[string(h.keyBuf)]
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
	if !h.probeResolved {
		h.probeKeyIdx = make([]int, len(h.LeftKeys))
		for i, col := range h.LeftKeys {
			h.probeKeyIdx[i] = in.ColumnIndex(col)
		}
		h.probeResolved = true
	}
	if h.probeKeyIdx[0] < 0 {
		return 0, false
	}
	return intKeyFromVector(in.Columns[h.probeKeyIdx[0]], row)
}

// estimateBatchBytes estimates the memory footprint of a RecordBatch.
func estimateBatchBytes(b *batch.RecordBatch) int64 {
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
func (h *HashJoin) Build(ctx context.Context, source Source) error {
	if err := source.Init(ctx); err != nil {
		return fmt.Errorf("build source init: %w", err)
	}
	defer source.Close()

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

		// Compact filtered batches so we only store active rows
		b = b.Compact()

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
		}

		// Spill to disk if memory pressure is high
		if h.Spill != nil && h.Spill.ShouldSpill() && len(h.buildBatches) > 0 {
			if err := h.spillBuildBatches(); err != nil {
				h.mu.Unlock()
				return fmt.Errorf("spilling build side: %w", err)
			}
		}

		// Track memory if budget is set
		if h.MemTracker != nil {
			cost := estimateBatchBytes(b)
			if err := h.MemTracker.Reserve(cost); err != nil {
				// Try spilling before giving up
				if h.Spill != nil && len(h.buildBatches) > 0 {
					if spillErr := h.spillBuildBatches(); spillErr == nil {
						h.MemTracker.Reset()
						// Re-reserve for just this batch
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

		batchIdx := int32(len(h.buildBatches))
		h.buildBatches = append(h.buildBatches, b)

		// After Compact(), Sel is nil so we iterate all rows (which are only the active ones)
		if h.useIntKey {
			col := b.Columns[h.buildKeyIdx[0]]
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					continue // skip null keys
				}
				h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key := h.buildKeyFromBatch(b, rowIdx)
				h.arenaAppendStr(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		}
		h.mu.Unlock()
	}

	// Re-load spilled data before probing
	if len(h.spillFiles) > 0 {
		if err := h.reloadSpilledBuild(); err != nil {
			return fmt.Errorf("reloading spilled build data: %w", err)
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

// buildBloom populates the bloom filter from the build-side hash table keys.
// Uses a 64-bit-per-slot bloom with 2 hash functions. The filter size is
// chosen to give ~1% false positive rate for the number of distinct keys.
func (h *HashJoin) buildBloom() {
	var nKeys int
	if h.useIntKey {
		nKeys = h.intIndex.Len()
	} else {
		nKeys = len(h.hashIndex)
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

	if h.useIntKey {
		h.intIndex.ForEach(func(key int64, _ int32) {
			h.bloomSet(bloomHashInt(key))
		})
	} else {
		for key := range h.hashIndex {
			h.bloomSet(bloomHashStr(key))
		}
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
	h1 := hash & h.bloomMask
	h2 := (hash >> 17) & h.bloomMask
	b1 := hash & 63
	b2 := (hash >> 6) & 63
	return (h.bloom[h1]>>b1)&1 != 0 && (h.bloom[h2]>>b2)&1 != 0
}

func bloomHashInt(key int64) uint64 {
	// Mix bits using a multiply-shift hash
	x := uint64(key) * 0x9E3779B97F4A7C15
	return x ^ (x >> 32)
}

func bloomHashStr(key string) uint64 {
	// FNV-1a style hash
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 16777619
	}
	return h ^ (h >> 32)
}

// spillBuildBatches writes current build batches to disk and clears in-memory state.
// Must be called with h.mu held.
func (h *HashJoin) spillBuildBatches() error {
	var rows []map[string]any
	for _, b := range h.buildBatches {
		rows = append(rows, b.ToRows()...)
	}
	path, err := h.Spill.SpillRows(rows)
	if err != nil {
		return err
	}
	h.spillFiles = append(h.spillFiles, path)
	// Clear in-memory state
	h.buildBatches = h.buildBatches[:0]
	h.arena = h.arena[:0]
	h.arenaNext = h.arenaNext[:0]
	if h.useIntKey {
		h.intIndex = newIntHashTable(64)
	} else {
		h.hashIndex = make(map[string]int32)
	}
	h.buildRows = 0
	if h.MemTracker != nil {
		h.MemTracker.Reset()
	}
	return nil
}

// reloadSpilledBuild reads all spilled build data and rebuilds the hash index.
func (h *HashJoin) reloadSpilledBuild() error {
	// Collect existing in-memory rows
	var allRows []map[string]any
	for _, b := range h.buildBatches {
		allRows = append(allRows, b.ToRows()...)
	}

	// Read spilled data
	for _, path := range h.spillFiles {
		rows, err := memory.ReadSpilledRows(path)
		if err != nil {
			return err
		}
		allRows = append(allRows, rows...)
	}
	h.spillFiles = nil

	// Rebuild from all rows
	h.buildBatches = nil
	h.arena = h.arena[:0]
	h.arenaNext = h.arenaNext[:0]
	if h.useIntKey {
		h.intIndex = newIntHashTable(64)
	} else {
		h.hashIndex = make(map[string]int32)
	}
	h.buildRows = 0
	if h.MemTracker != nil {
		h.MemTracker.Reset()
	}

	// Convert to batches in chunks
	for pos := 0; pos < len(allRows); {
		end := pos + batch.DefaultBatchSize
		if end > len(allRows) {
			end = len(allRows)
		}
		b := batch.FromRows(h.buildSchema, allRows[pos:end])
		batchIdx := int32(len(h.buildBatches))
		h.buildBatches = append(h.buildBatches, b)
		if h.useIntKey {
			col := b.Columns[h.buildKeyIdx[0]]
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					continue
				}
				h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key := h.buildKeyFromBatch(b, rowIdx)
				h.arenaAppendStr(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		}
		pos = end
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
		for i := 0; i < b.Len; i++ {
			key := h.buildKeyFromBatch(b, i)
			h.arenaAppendStr(key, buildRef{batchIdx: batchIdx, rowIdx: int32(i)})
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
			h.tryEnableIntKey(b)
		}
		h.buildRows = 0
		h.arena = h.arena[:0]
		h.arenaNext = h.arenaNext[:0]
		if h.useIntKey {
			h.intIndex = newIntHashTable(64)
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
		} else {
			h.hashIndex = make(map[string]int32)
			for batchIdx, b := range h.buildBatches {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					key := h.buildKeyFromBatch(b, rowIdx)
					h.arenaAppendStr(key, buildRef{batchIdx: int32(batchIdx), rowIdx: int32(rowIdx)})
					h.buildRows++
				}
			}
		}
	}
}

// Probe is a UnaryOperator that probes the hash table for each input batch.
func (h *HashJoin) Probe() *HashJoinProbe {
	return &HashJoinProbe{join: h}
}

// buildKeyFromBatch computes the hash key for a build-side row using typed serialization.
// Uses pre-resolved column indices and a reusable buffer to avoid allocations.
func (h *HashJoin) buildKeyFromBatch(b *batch.RecordBatch, rowIdx int) string {
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
	return string(h.keyBuf)
}

// buildProbeKeyBuf fills h.keyBuf with the serialized probe key for a row.
// The caller should use string(h.keyBuf) directly in a map index expression
// to benefit from Go's compiler optimization that avoids string allocation.
func (h *HashJoin) buildProbeKeyBuf(b *batch.RecordBatch, row int) {
	if !h.probeResolved {
		h.probeKeyIdx = make([]int, len(h.LeftKeys))
		for i, col := range h.LeftKeys {
			h.probeKeyIdx[i] = b.ColumnIndex(col)
		}
		h.probeResolved = true
	}
	h.keyBuf = h.keyBuf[:0]
	for _, idx := range h.probeKeyIdx {
		if idx < 0 {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			h.keyBuf = append(h.keyBuf, 1) // null flag
		} else {
			h.keyBuf = append(h.keyBuf, 0) // not-null flag
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, v.Type)
		}
	}
}

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
	semiSelBuf []uint16    // reusable selection vector for semi/anti join output
	lookupBuf  []buildRef  // reusable buffer for lookupBuild results
	indexBuf   []int       // reusable buffer for probe-side gather indices

	// Cached output schema and column mapping (computed once on first batch)
	cachedSchema  []parquet.Column
	cachedMapping []outColSource
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
	} else {
		h.buildProbeKeyBuf(in, row)
		head, ok := h.hashIndex[string(h.keyBuf)]
		if !ok {
			return
		}
		for idx := head; idx >= 0; idx = h.arenaNext[idx] {
			h.arenaMatched[idx] = true
		}
	}
}

func (p *HashJoinProbe) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if p.join.JoinType == CrossJoin {
		outSchema := p.outputSchema(in.Schema)
		return p.executeCrossJoin(in, outSchema)
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

	probeRow := func(row int) {
		buildMatches := p.lookupBuild(in, row)

		if len(buildMatches) == 0 {
			if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
				pairs = append(pairs, matchPair{probeRow: row})
			}
			return
		}

		for _, ref := range buildMatches {
			pairs = append(pairs, matchPair{probeRow: row, ref: ref, matched: true})
		}

		// Mark all build-side refs for this key as matched (for right/full outer)
		if p.join.arenaMatched != nil {
			p.join.mu.Lock()
			p.markKeyMatched(in, row)
			p.join.mu.Unlock()
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
	p.pairsBuf = pairs // save grown slice for reuse

	if len(pairs) == 0 {
		return nil, nil
	}

	// Build output batch using precomputed column source mapping
	out := batch.NewRecordBatch(outSchema, len(pairs))

	// Pre-extract probe row indices for bulk gather
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
			gatherBuildVector(dst, m.srcIdx, pairs, p.join.buildBatches)
		}
	}

	return out, nil
}

// executeSemiAntiJoin handles SemiJoin and AntiJoin semantics.
// Uses a selection vector on the input batch to avoid copying rows.
func (p *HashJoinProbe) executeSemiAntiJoin(in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if cap(p.semiSelBuf) < in.Len {
		p.semiSelBuf = make([]uint16, 0, in.Len)
	}
	sel := p.semiSelBuf[:0]

	h := p.join
	isSemi := h.JoinType == SemiJoin
	hasFilter := h.SemiAntiFilter != nil

	// Fast path: int-key semi/anti without filter — inline everything
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

			checkIntRow := func(row int) {
				if hasNulls && keyCol.Nulls.IsNullFast(row) {
					if !isSemi {
						sel = append(sel, uint16(row))
					}
					return
				}
				var key int64
				if isInt32 {
					key = int64(keyCol.Int32Data[row])
				} else {
					key = keyCol.Int64Data[row]
				}
				// Bloom filter pre-check
				if hasBloom && !h.bloomMayContain(bloomHashInt(key)) {
					if !isSemi {
						sel = append(sel, uint16(row))
					}
					return
				}
				_, exists := h.intIndex.Get(key)
				emit := (isSemi && exists) || (!isSemi && !exists)
				if emit {
					sel = append(sel, uint16(row))
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
				sel = append(sel, uint16(row))
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
	p.semiSelBuf = sel // save grown slice for reuse

	if len(sel) == 0 {
		return nil, nil
	}

	// Use selection vector instead of copying — zero allocation output
	in.Sel = sel
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
	return p.join.Probe()
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
func gatherBuildVector(dst *batch.Vector, srcIdx int, pairs []matchPair, buildBatches []*batch.RecordBatch) {
	switch dst.Type {
	case batch.TypeBool:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.BoolData[di] = src.BoolData[si]
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Int32Data[di] = src.Int32Data[si]
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Int64Data[di] = src.Int64Data[si]
			}
		}
	case batch.TypeFloat32:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Float32Data[di] = src.Float32Data[si]
			}
		}
	case batch.TypeFloat64:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.Float64Data[di] = src.Float64Data[si]
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				dst.BytesData.Set(di, nil)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
				dst.BytesData.Set(di, nil)
			} else {
				dst.Nulls.SetValid(di)
				dst.BytesData.Set(di, src.BytesData.Value(si))
			}
		}
	case batch.TypeDecimal:
		for di, pair := range pairs {
			if !pair.matched {
				dst.Nulls.SetNull(di)
				continue
			}
			src := buildBatches[pair.ref.batchIdx].Columns[srcIdx]
			si := int(pair.ref.rowIdx)
			if src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Nulls.SetValid(di)
				dst.DecimalData.Data[di] = src.DecimalData.Data[si]
			}
		}
	default:
		// Fallback for any unhandled types
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
