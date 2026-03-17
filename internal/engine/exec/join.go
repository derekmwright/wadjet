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
	buildBatches []*batch.RecordBatch   // columnar storage of build side
	hashIndex    map[string][]buildRef  // hash key -> list of (batch, row) refs (general path)
	intIndex     map[int64][]buildRef   // fast path: single-column integer join key
	useIntKey    bool                   // true when single int32/int64 join key detected
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

	// matched tracks which build-side rows have been matched during probing.
	// Only populated for RightJoin and FullOuterJoin.
	matched    map[string]map[int]bool
	intMatched map[int64]map[int]bool // matched tracking for int key path

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
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(joinType JoinType, leftKeys, rightKeys []string) *HashJoin {
	hj := &HashJoin{
		JoinType:  joinType,
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
		hashIndex: make(map[string][]buildRef),
		keyBuf:    make([]byte, 0, 128),
	}
	if joinType == RightJoin || joinType == FullOuterJoin {
		hj.matched = make(map[string]map[int]bool)
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
		h.intIndex = make(map[int64][]buildRef)
		h.hashIndex = nil // free the string map
		if h.matched != nil {
			h.intMatched = make(map[int64]map[int]bool)
			h.matched = nil
		}
	}
}

// lookupBuild returns build refs for a probe row, using the appropriate index.
func (h *HashJoin) lookupBuild(in *batch.RecordBatch, row int) []buildRef {
	if h.useIntKey {
		key, ok := h.intProbeKey(in, row)
		if !ok {
			return nil
		}
		return h.intIndex[key]
	}
	key := h.probeKey(in, row)
	return h.hashIndex[key]
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
				h.intIndex[key] = append(h.intIndex[key], buildRef{
					batchIdx: batchIdx,
					rowIdx:   int32(rowIdx),
				})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key := h.buildKeyFromBatch(b, rowIdx)
				h.hashIndex[key] = append(h.hashIndex[key], buildRef{
					batchIdx: batchIdx,
					rowIdx:   int32(rowIdx),
				})
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

	h.buildDone = true
	return nil
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
	if h.useIntKey {
		h.intIndex = make(map[int64][]buildRef)
	} else {
		h.hashIndex = make(map[string][]buildRef)
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
	if h.useIntKey {
		h.intIndex = make(map[int64][]buildRef)
	} else {
		h.hashIndex = make(map[string][]buildRef)
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
				h.intIndex[key] = append(h.intIndex[key], buildRef{
					batchIdx: batchIdx,
					rowIdx:   int32(rowIdx),
				})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key := h.buildKeyFromBatch(b, rowIdx)
				h.hashIndex[key] = append(h.hashIndex[key], buildRef{
					batchIdx: batchIdx,
					rowIdx:   int32(rowIdx),
				})
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
			h.intIndex[key] = append(h.intIndex[key], buildRef{
				batchIdx: batchIdx,
				rowIdx:   int32(i),
			})
			h.buildRows++
		}
	} else {
		for i := 0; i < b.Len; i++ {
			key := h.buildKeyFromBatch(b, i)
			h.hashIndex[key] = append(h.hashIndex[key], buildRef{
				batchIdx: batchIdx,
				rowIdx:   int32(i),
			})
			h.buildRows++
		}
	}
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
		if h.useIntKey {
			h.intIndex = make(map[int64][]buildRef)
			for batchIdx, b := range h.buildBatches {
				col := b.Columns[h.buildKeyIdx[0]]
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					key, ok := intKeyFromVector(col, rowIdx)
					if !ok {
						continue
					}
					h.intIndex[key] = append(h.intIndex[key], buildRef{
						batchIdx: int32(batchIdx),
						rowIdx:   int32(rowIdx),
					})
					h.buildRows++
				}
			}
		} else {
			h.hashIndex = make(map[string][]buildRef)
			for batchIdx, b := range h.buildBatches {
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					key := h.buildKeyFromBatch(b, rowIdx)
					h.hashIndex[key] = append(h.hashIndex[key], buildRef{
						batchIdx: int32(batchIdx),
						rowIdx:   int32(rowIdx),
					})
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
	for i, idx := range h.buildKeyIdx {
		if i > 0 {
			h.keyBuf = append(h.keyBuf, 0)
		}
		if idx < 0 {
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(rowIdx) {
			h.keyBuf = append(h.keyBuf, "<null>"...)
		} else {
			h.keyBuf = appendColumnValue(h.keyBuf, v, rowIdx, v.Type)
		}
	}
	return string(h.keyBuf)
}

// probeKey computes the hash key for a probe-side row using typed serialization.
func (h *HashJoin) probeKey(b *batch.RecordBatch, row int) string {
	if !h.probeResolved {
		h.probeKeyIdx = make([]int, len(h.LeftKeys))
		for i, col := range h.LeftKeys {
			h.probeKeyIdx[i] = b.ColumnIndex(col)
		}
		h.probeResolved = true
	}
	h.keyBuf = h.keyBuf[:0]
	for i, idx := range h.probeKeyIdx {
		if i > 0 {
			h.keyBuf = append(h.keyBuf, 0)
		}
		if idx < 0 {
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			h.keyBuf = append(h.keyBuf, "<null>"...)
		} else {
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, v.Type)
		}
	}
	return string(h.keyBuf)
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
}

func (p *HashJoinProbe) Init(_ context.Context) error {
	if !p.join.buildDone {
		return fmt.Errorf("hash join build phase not complete")
	}
	return nil
}

func (p *HashJoinProbe) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if p.join.JoinType == CrossJoin {
		outSchema := p.outputSchema(in.Schema)
		return p.executeCrossJoin(in, outSchema)
	}

	if p.join.JoinType == SemiJoin || p.join.JoinType == AntiJoin {
		return p.executeSemiAntiJoin(in)
	}

	outSchema, mapping := p.outputSchemaWithMapping(in.Schema)

	// Collect match pairs using reusable buffer
	pairs := p.pairsBuf[:0]

	probeRow := func(row int) {
		buildMatches := p.join.lookupBuild(in, row)

		if len(buildMatches) == 0 {
			if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
				pairs = append(pairs, matchPair{probeRow: row})
			}
			return
		}

		for i, ref := range buildMatches {
			pairs = append(pairs, matchPair{probeRow: row, ref: ref, matched: true})

			if p.join.useIntKey && p.join.intMatched != nil {
				p.join.mu.Lock()
				key, _ := p.join.intProbeKey(in, row)
				if p.join.intMatched[key] == nil {
					p.join.intMatched[key] = make(map[int]bool)
				}
				p.join.intMatched[key][i] = true
				p.join.mu.Unlock()
			} else if p.join.matched != nil {
				p.join.mu.Lock()
				key := p.join.probeKey(in, row)
				if p.join.matched[key] == nil {
					p.join.matched[key] = make(map[int]bool)
				}
				p.join.matched[key][i] = true
				p.join.mu.Unlock()
			}
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

	for outColIdx, m := range mapping {
		dst := out.Columns[outColIdx]
		if m.fromProbe {
			for outRow, pair := range pairs {
				copyVectorValue(dst, outRow, in.Columns[m.srcIdx], pair.probeRow)
			}
		} else {
			for outRow, pair := range pairs {
				if pair.matched {
					buildBatch := p.join.buildBatches[pair.ref.batchIdx]
					copyVectorValue(dst, outRow, buildBatch.Columns[m.srcIdx], int(pair.ref.rowIdx))
				} else {
					setVectorNull(dst, outRow)
				}
			}
		}
	}

	return out, nil
}

// executeSemiAntiJoin handles SemiJoin and AntiJoin semantics.
// Uses a selection vector on the input batch to avoid copying rows.
func (p *HashJoinProbe) executeSemiAntiJoin(in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// Collect qualifying probe row indices into a selection vector
	if cap(p.semiSelBuf) < in.Len {
		p.semiSelBuf = make([]uint16, 0, in.Len)
	}
	sel := p.semiSelBuf[:0]

	checkRow := func(row int) {
		candidates := p.join.lookupBuild(in, row)
		hasMatch := false

		if len(candidates) > 0 {
			if p.join.SemiAntiFilter != nil {
				for _, ref := range candidates {
					buildBatch := p.join.buildBatches[ref.batchIdx]
					if p.join.SemiAntiFilter(in, row, buildBatch, int(ref.rowIdx)) {
						hasMatch = true
						break
					}
				}
			} else {
				hasMatch = true
			}
		}

		emit := (p.join.JoinType == SemiJoin && hasMatch) ||
			(p.join.JoinType == AntiJoin && !hasMatch)
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

	p.semiSelBuf = sel // save grown slice for reuse

	if len(sel) == 0 {
		return nil, nil
	}

	// Use selection vector instead of copying — zero allocation output
	in.Sel = sel
	return in, nil
}

// executeCrossJoin produces the Cartesian product of probe rows with all build-side rows.
func (p *HashJoinProbe) executeCrossJoin(in *batch.RecordBatch, outSchema []parquet.Column) (*batch.RecordBatch, error) {
	_, mapping := p.outputSchemaWithMapping(in.Schema)

	type crossPair struct {
		probeRow int
		batchIdx int32
		buildRow int
	}
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

	for outColIdx, m := range mapping {
		dst := out.Columns[outColIdx]
		if m.fromProbe {
			for outRow, cp := range pairs {
				copyVectorValue(dst, outRow, in.Columns[m.srcIdx], cp.probeRow)
			}
		} else {
			for outRow, cp := range pairs {
				buildBatch := p.join.buildBatches[cp.batchIdx]
				copyVectorValue(dst, outRow, buildBatch.Columns[m.srcIdx], cp.buildRow)
			}
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

	// Collect unmatched build refs
	var refs []buildRef
	if p.join.useIntKey {
		for key, keyRefs := range p.join.intIndex {
			matchedSet := p.join.intMatched[key]
			for i, ref := range keyRefs {
				if matchedSet != nil && matchedSet[i] {
					continue
				}
				refs = append(refs, ref)
			}
		}
	} else {
		for key, keyRefs := range p.join.hashIndex {
			matchedSet := p.join.matched[key]
			for i, ref := range keyRefs {
				if matchedSet != nil && matchedSet[i] {
					continue
				}
				refs = append(refs, ref)
			}
		}
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

// setVectorNull marks a position as null, handling BytesColumn offset alignment.
func setVectorNull(dst *batch.Vector, row int) {
	dst.Nulls.SetNull(row)
	switch dst.Type {
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.Set(row, nil)
	}
}
