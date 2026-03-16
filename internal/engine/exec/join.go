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
}

// NewHashJoin creates a new hash join operator.
func NewHashJoin(joinType JoinType, leftKeys, rightKeys []string) *HashJoin {
	hj := &HashJoin{
		JoinType:  joinType,
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
		hashIndex: make(map[string][]buildRef),
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

// BuildFromRows loads the build side directly from rows (used by tests and worker).
func (h *HashJoin) BuildFromRows(schema []parquet.Column, rows []map[string]any) {
	h.buildSchema = schema
	if len(rows) == 0 {
		h.buildDone = true
		return
	}
	b := batch.FromRows(schema, rows)
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
		}
	}

	// Rebuild hash index if keys were swapped
	if needsRebuild {
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

// buildKeyFromBatch computes the hash key for a build-side row from columnar storage.
func (h *HashJoin) buildKeyFromBatch(b *batch.RecordBatch, rowIdx int) string {
	key := ""
	for i, col := range h.RightKeys {
		if i > 0 {
			key += "\x00"
		}
		v := b.ColumnByName(col)
		if v != nil {
			key += fmt.Sprint(v.GetValue(rowIdx))
		}
	}
	return key
}

func (h *HashJoin) probeKey(b *batch.RecordBatch, row int) string {
	key := ""
	for i, col := range h.LeftKeys {
		if i > 0 {
			key += "\x00"
		}
		v := b.ColumnByName(col)
		if v != nil {
			key += fmt.Sprint(v.GetValue(row))
		}
	}
	return key
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
	outSchema := p.outputSchema(in.Schema)

	if p.join.JoinType == CrossJoin {
		return p.executeCrossJoin(in, outSchema)
	}

	if p.join.JoinType == SemiJoin || p.join.JoinType == AntiJoin {
		return p.executeSemiAntiJoin(in)
	}

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

	// Build output batch directly in columnar form
	out := batch.NewRecordBatch(outSchema, len(pairs))

	// Pre-compute column source mappings
	probeSeen := make(map[string]bool, len(in.Schema))
	for _, col := range in.Schema {
		probeSeen[col.Name] = true
	}

	for outColIdx, col := range outSchema {
		dst := out.Columns[outColIdx]
		probeColIdx := in.ColumnIndex(col.Name)
		buildColIdx := -1
		if probeColIdx < 0 {
			for bi, bc := range p.join.buildSchema {
				if bc.Name == col.Name {
					buildColIdx = bi
					break
				}
			}
		}

		for outRow, m := range pairs {
			if probeColIdx >= 0 {
				copyVectorValue(dst, outRow, in.Columns[probeColIdx], m.probeRow)
			} else if buildColIdx >= 0 && m.matched {
				buildBatch := p.join.buildBatches[m.ref.batchIdx]
				copyVectorValue(dst, outRow, buildBatch.Columns[buildColIdx], int(m.ref.rowIdx))
			} else {
				setVectorNull(dst, outRow)
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
		hasMatch := len(p.join.hashIndex[key]) > 0

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
	// Collect cross product pairs
	type crossPair struct {
		probeRow  int
		batchIdx  int32
		buildRow  int
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
	probeSeen := make(map[string]bool, len(in.Schema))
	for _, col := range in.Schema {
		probeSeen[col.Name] = true
	}

	for outColIdx, col := range outSchema {
		dst := out.Columns[outColIdx]
		probeColIdx := in.ColumnIndex(col.Name)
		buildColIdx := -1
		if probeColIdx < 0 {
			for bi, bc := range p.join.buildSchema {
				if bc.Name == col.Name {
					buildColIdx = bi
					break
				}
			}
		}

		for outRow, cp := range pairs {
			if probeColIdx >= 0 {
				copyVectorValue(dst, outRow, in.Columns[probeColIdx], cp.probeRow)
			} else if buildColIdx >= 0 {
				buildBatch := p.join.buildBatches[cp.batchIdx]
				copyVectorValue(dst, outRow, buildBatch.Columns[buildColIdx], cp.buildRow)
			} else {
				setVectorNull(dst, outRow)
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

	outSchema := p.outputSchema(leftSchema)
	out := batch.NewRecordBatch(outSchema, len(refs))

	leftSeen := make(map[string]bool, len(leftSchema))
	for _, col := range leftSchema {
		leftSeen[col.Name] = true
	}

	for outColIdx, col := range outSchema {
		dst := out.Columns[outColIdx]
		if leftSeen[col.Name] {
			// Left side is all NULLs for unmatched build rows
			for outRow := range refs {
				setVectorNull(dst, outRow)
			}
		} else {
			buildColIdx := -1
			for bi, bc := range p.join.buildSchema {
				if bc.Name == col.Name {
					buildColIdx = bi
					break
				}
			}
			for outRow, ref := range refs {
				if buildColIdx >= 0 {
					buildBatch := p.join.buildBatches[ref.batchIdx]
					copyVectorValue(dst, outRow, buildBatch.Columns[buildColIdx], int(ref.rowIdx))
				} else {
					setVectorNull(dst, outRow)
				}
			}
		}
	}

	return out
}

func (p *HashJoinProbe) Close() error { return nil }

func (p *HashJoinProbe) outputSchema(leftSchema []parquet.Column) []parquet.Column {
	var out []parquet.Column

	if p.join.JoinType == RightJoin || p.join.JoinType == FullOuterJoin {
		for _, col := range leftSchema {
			col.Nullable = true
			out = append(out, col)
		}
	} else {
		out = append(out, leftSchema...)
	}

	seen := make(map[string]bool, len(leftSchema))
	for _, col := range leftSchema {
		seen[col.Name] = true
	}

	for _, col := range p.join.buildSchema {
		if !seen[col.Name] {
			if p.join.JoinType == LeftJoin || p.join.JoinType == FullOuterJoin {
				col.Nullable = true
			}
			out = append(out, col)
		}
	}
	return out
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
