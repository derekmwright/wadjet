package exec

import (
	"context"
	"fmt"
	"sync"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/memory"
	"github.com/derekmwright/caelum/internal/storage/parquet"
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
	hashIndex    map[string][]buildRef  // hash key -> list of (batch, row) refs
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
	matched map[string]map[int]bool

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
		for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
			key := h.buildKeyFromBatch(b, rowIdx)
			h.hashIndex[key] = append(h.hashIndex[key], buildRef{
				batchIdx: batchIdx,
				rowIdx:   int32(rowIdx),
			})
			h.buildRows++
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
	h.hashIndex = make(map[string][]buildRef)
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
	h.hashIndex = make(map[string][]buildRef)
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
		for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
			key := h.buildKeyFromBatch(b, rowIdx)
			h.hashIndex[key] = append(h.hashIndex[key], buildRef{
				batchIdx: batchIdx,
				rowIdx:   int32(rowIdx),
			})
			h.buildRows++
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
	}
	batchIdx := int32(len(h.buildBatches))
	h.buildBatches = append(h.buildBatches, b)
	for i := 0; i < b.Len; i++ {
		key := h.buildKeyFromBatch(b, i)
		h.hashIndex[key] = append(h.hashIndex[key], buildRef{
			batchIdx: batchIdx,
			rowIdx:   int32(i),
		})
		h.buildRows++
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
		}
		h.hashIndex = make(map[string][]buildRef)
		h.buildRows = 0
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

// readBuildValue reads a single value from the columnar build-side storage.
func (h *HashJoin) readBuildValue(ref buildRef, colName string) any {
	b := h.buildBatches[ref.batchIdx]
	v := b.ColumnByName(colName)
	if v == nil {
		return nil
	}
	return v.GetValue(int(ref.rowIdx))
}

// HashJoinProbe is a UnaryOperator that probes the build-side hash table.
type HashJoinProbe struct {
	join *HashJoin
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

	// Collect match pairs: (probe row, build ref, matched flag)
	type matchPair struct {
		probeRow int
		ref      buildRef
		matched  bool
	}
	var pairs []matchPair

	iter := batchIterator(in)
	for _, row := range iter {
		key := p.join.probeKey(in, row)
		buildMatches := p.join.hashIndex[key]

		if len(buildMatches) == 0 {
			if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
				pairs = append(pairs, matchPair{probeRow: row})
			}
			continue
		}

		for i, ref := range buildMatches {
			pairs = append(pairs, matchPair{probeRow: row, ref: ref, matched: true})

			if p.join.matched != nil {
				p.join.mu.Lock()
				if p.join.matched[key] == nil {
					p.join.matched[key] = make(map[int]bool)
				}
				p.join.matched[key][i] = true
				p.join.mu.Unlock()
			}
		}
	}

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
func (p *HashJoinProbe) executeSemiAntiJoin(in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// Collect qualifying probe row indices
	var rows []int

	iter := batchIterator(in)
	for _, row := range iter {
		key := p.join.probeKey(in, row)
		candidates := p.join.hashIndex[key]
		hasMatch := false

		if len(candidates) > 0 {
			if p.join.SemiAntiFilter != nil {
				// Check each candidate against the extra filter
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
			rows = append(rows, row)
		}
	}

	if len(rows) == 0 {
		return nil, nil
	}

	out := batch.NewRecordBatch(in.Schema, len(rows))
	for colIdx := range in.Schema {
		dst := out.Columns[colIdx]
		src := in.Columns[colIdx]
		for outRow, srcRow := range rows {
			copyVectorValue(dst, outRow, src, srcRow)
		}
	}
	return out, nil
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

	iter := batchIterator(in)
	for _, row := range iter {
		for bi, buildBatch := range p.join.buildBatches {
			buildIter := batchIterator(buildBatch)
			for _, buildRow := range buildIter {
				pairs = append(pairs, crossPair{probeRow: row, batchIdx: int32(bi), buildRow: buildRow})
			}
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
	for key, keyRefs := range p.join.hashIndex {
		matchedSet := p.join.matched[key]
		for i, ref := range keyRefs {
			if matchedSet != nil && matchedSet[i] {
				continue
			}
			refs = append(refs, ref)
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
