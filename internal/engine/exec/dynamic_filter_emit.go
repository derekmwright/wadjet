package exec

import (
	"context"
	"math"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// DynamicFilterEmitOp is a pass-through UnaryOperator that accumulates a
// streaming partial bloom + min/max over a single integer key column.
// Inserted by the fragment runner at the head of a build-scan task's op
// chain when the OpScan spec carries a DynamicFilterEmit. Partial stats are
// drained on close via Snapshot and uploaded by the fragment runner.
//
// All tasks for one emit use the same bloomBits so the coordinator unions
// partials with a trivial bitwise OR (no rehash).
type DynamicFilterEmitOp struct {
	filterID  string
	keyColumn string
	keyType   string // "int32" | "int64" | "date"

	bloomBits int
	bloom     []uint64
	bloomMask uint64

	keyIdx   int
	resolved bool
	sawRows  bool

	hasRange bool
	min, max int64
	rowCount int64
}

// NewDynamicFilterEmitOp constructs an emit op. bloomBits must be a power
// of two; values <64 are clamped to 64. The bitset is allocated lazily on
// the first batch to keep the cost out of constructor paths in the planner.
func NewDynamicFilterEmitOp(filterID, keyColumn, keyType string, bloomBits int) *DynamicFilterEmitOp {
	if bloomBits < 64 {
		bloomBits = 64
	}
	// Round up to next power of two.
	for bloomBits&(bloomBits-1) != 0 {
		bloomBits++
	}
	return &DynamicFilterEmitOp{
		filterID:  filterID,
		keyColumn: keyColumn,
		keyType:   keyType,
		bloomBits: bloomBits,
		keyIdx:    -1,
		min:       math.MaxInt64,
		max:       math.MinInt64,
	}
}

func (op *DynamicFilterEmitOp) Init(_ context.Context) error { return nil }

func (op *DynamicFilterEmitOp) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || in.ActiveLen() == 0 {
		return in, nil
	}
	op.sawRows = true

	if !op.resolved {
		op.keyIdx = in.ColumnIndex(op.keyColumn)
		if op.keyIdx < 0 {
			// Join outputs may carry the key alias-qualified
			// ("l1.l_orderkey"). Accept a UNIQUE suffix match; ambiguity
			// (two qualified columns with the same base name) stays
			// unresolved — a wrong column would poison the bloom.
			op.keyIdx = uniqueSuffixColumnIndex(in, op.keyColumn)
		}
		op.resolved = true
		if op.bloom == nil {
			nSlots := op.bloomBits / 64
			op.bloom = make([]uint64, nSlots)
			op.bloomMask = uint64(nSlots - 1)
		}
	}
	// Column not found in this batch: pass through. The emit will produce a
	// row_count=0 partial, which the coordinator treats as a no-op union.
	if op.keyIdx < 0 {
		return in, nil
	}

	col := in.Columns[op.keyIdx]
	bloom := op.bloom
	mask := op.bloomMask

	set := func(key int64) {
		h := bloomHashInt(key)
		h1 := h & mask
		h2 := (h >> 17) & mask
		b1 := h & 63
		b2 := (h >> 6) & 63
		bloom[h1] |= 1 << b1
		bloom[h2] |= 1 << b2
		if key < op.min {
			op.min = key
		}
		if key > op.max {
			op.max = key
		}
		op.rowCount++
	}

	if in.Sel != nil {
		if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for _, idx := range in.Sel {
					set(int64(data[idx]))
				}
			default:
				data := col.Int64Data
				for _, idx := range in.Sel {
					set(data[idx])
				}
			}
		} else {
			for _, idx := range in.Sel {
				if col.Nulls.IsNullFast(int(idx)) {
					continue
				}
				key, ok := intKeyFromVector(col, int(idx))
				if ok {
					set(key)
				}
			}
		}
	} else {
		if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for i := 0; i < in.Len; i++ {
					set(int64(data[i]))
				}
			default:
				data := col.Int64Data
				for i := 0; i < in.Len; i++ {
					set(data[i])
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if col.Nulls.IsNullFast(i) {
					continue
				}
				key, ok := intKeyFromVector(col, i)
				if ok {
					set(key)
				}
			}
		}
	}
	op.hasRange = op.hasRange || op.rowCount > 0
	return in, nil
}

func (op *DynamicFilterEmitOp) Close() error { return nil }

// DynamicFilterPartial is the in-memory representation of one task's
// emitted partial stats, ready for serialization by the fragment runner.
type DynamicFilterPartial struct {
	FilterID  string
	KeyType   string
	KeyColumn string
	BloomBits int
	Bloom     []uint64
	BloomMask uint64
	HasRange  bool
	Min, Max  int64
	RowCount  int64
	// Unresolved is set when the op observed input rows but could not
	// resolve KeyColumn in the batch schema — the partial is missing keys
	// that exist in the stream and MUST NOT be uploaded (a bloom missing
	// live keys falsely rejects rows at the consume side). A task that saw
	// zero rows is NOT unresolved: its key set is legitimately empty.
	Unresolved bool
}

// Snapshot returns the accumulated partial. Safe to call once after the
// pipeline has finished — caller takes ownership of the bloom slice.
func (op *DynamicFilterEmitOp) Snapshot() DynamicFilterPartial {
	return DynamicFilterPartial{
		FilterID:   op.filterID,
		KeyType:    op.keyType,
		KeyColumn:  op.keyColumn,
		BloomBits:  op.bloomBits,
		Bloom:      op.bloom,
		BloomMask:  op.bloomMask,
		HasRange:   op.hasRange,
		Min:        op.min,
		Max:        op.max,
		RowCount:   op.rowCount,
		Unresolved: op.sawRows && op.keyIdx < 0,
	}
}

// FilterID lets the runner correlate the op back to its OpSpec entry.
func (op *DynamicFilterEmitOp) FilterID() string { return op.filterID }

// uniqueSuffixColumnIndex returns the index of the single column whose
// base name (segment after the last '.') equals name, or -1 when zero or
// multiple columns match.
func uniqueSuffixColumnIndex(in *batch.RecordBatch, name string) int {
	found := -1
	for i, col := range in.Schema {
		cn := col.Name
		for j := len(cn) - 1; j >= 0; j-- {
			if cn[j] == '.' {
				cn = cn[j+1:]
				break
			}
		}
		if cn == name {
			if found >= 0 {
				return -1
			}
			found = i
		}
	}
	return found
}
