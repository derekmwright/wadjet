package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// BloomFilterOp is a UnaryOperator that pre-filters probe batches using the
// build-side bloom filter. Rows whose join key hash is not in the bloom are
// eliminated via selection vector before reaching the probe operator.
//
// This is read-only on the bloom data, so multiple clones safely share it.
type BloomFilterOp struct {
	bloom     []uint64 // shared, read-only after Build()
	bloomMask uint64

	leftKeys      []string // probe-side key column names
	useIntKey     bool     // single-column integer fast path
	useDualIntKey bool     // two-column integer fast path
	keyIdx        []int    // resolved column indices (lazy)
	resolved      bool
	selBuf        []uint32 // scratch for selection vector
	keyBuf        []byte   // scratch for multi-column key serialization

	// Adaptive disabling: if the bloom filter isn't filtering enough rows,
	// the hash computation + random memory accesses cost more than they save.
	// Track rejection rate over the first 8 batches; disable if <5% rejected.
	totalChecked int
	totalPassed  int
	batchesSeen  int
	disabled     bool
}

func (op *BloomFilterOp) Init(_ context.Context) error { return nil }

func (op *BloomFilterOp) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || in.ActiveLen() == 0 {
		return in, nil
	}

	// Adaptive bypass: if the bloom filter isn't rejecting enough rows,
	// skip it entirely. The hash + two random reads per row cost more
	// than the probe savings when rejection rate is below 5%.
	if op.disabled {
		return in, nil
	}

	// Lazy column index resolution on first batch
	if !op.resolved {
		op.keyIdx = make([]int, len(op.leftKeys))
		for i, col := range op.leftKeys {
			op.keyIdx[i] = in.ColumnIndex(col)
		}
		op.resolved = true
	}

	activeLen := in.ActiveLen()
	if cap(op.selBuf) < activeLen {
		op.selBuf = make([]uint32, 0, activeLen)
	}
	sel := op.selBuf[:0]

	if op.useIntKey && len(op.keyIdx) == 1 && op.keyIdx[0] >= 0 {
		col := in.Columns[op.keyIdx[0]]
		if in.Sel != nil {
			if !col.Nulls.HasNulls() {
				// Null-free + selection: skip null checks, inline typed access
				switch col.Type {
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					data := col.Int32Data
					for _, idx := range in.Sel {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(int64(data[idx]))) {
							sel = append(sel, idx)
						}
					}
				default:
					data := col.Int64Data
					for _, idx := range in.Sel {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(data[idx])) {
							sel = append(sel, idx)
						}
					}
				}
			} else {
				for _, idx := range in.Sel {
					if col.Nulls.IsNullFast(int(idx)) {
						continue
					}
					key, ok := intKeyFromVector(col, int(idx))
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(key)) {
						sel = append(sel, idx)
					}
				}
			}
		} else {
			if !col.Nulls.HasNulls() {
				// Null-free: inline typed data access, no null checks
				switch col.Type {
				case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
					data := col.Int32Data
					for i := 0; i < in.Len; i++ {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(int64(data[i]))) {
							sel = append(sel, uint32(i))
						}
					}
				default:
					data := col.Int64Data
					for i := 0; i < in.Len; i++ {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(data[i])) {
							sel = append(sel, uint32(i))
						}
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					if col.Nulls.IsNullFast(i) {
						continue
					}
					key, ok := intKeyFromVector(col, i)
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(key)) {
						sel = append(sel, uint32(i))
					}
				}
			}
		}
	} else if op.useDualIntKey && len(op.keyIdx) == 2 && op.keyIdx[0] >= 0 && op.keyIdx[1] >= 0 {
		col0, col1 := in.Columns[op.keyIdx[0]], in.Columns[op.keyIdx[1]]
		noNulls := !col0.Nulls.HasNulls() && !col1.Nulls.HasNulls()
		if in.Sel != nil {
			if noNulls {
				for _, idx := range in.Sel {
					a := intValFromCol(col0, int(idx))
					b := intValFromCol(col1, int(idx))
					if bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, idx)
					}
				}
			} else {
				for _, idx := range in.Sel {
					a, b, ok := dualIntKeyFromVectors(col0, col1, int(idx))
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, idx)
					}
				}
			}
		} else {
			if noNulls {
				for i := 0; i < in.Len; i++ {
					a := intValFromCol(col0, i)
					b := intValFromCol(col1, i)
					if bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, uint32(i))
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					a, b, ok := dualIntKeyFromVectors(col0, col1, i)
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, uint32(i))
					}
				}
			}
		}
	} else {
		// General string key path
		if in.Sel != nil {
			for _, idx := range in.Sel {
				if op.probeKeyHash(in, int(idx)) {
					sel = append(sel, idx)
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if op.probeKeyHash(in, i) {
					sel = append(sel, uint32(i))
				}
			}
		}
	}

	if len(sel) == 0 {
		return nil, nil
	}

	// Track rejection rate for adaptive disabling.
	// After 8 batches, if <5% of rows are rejected, the bloom filter
	// costs more than it saves and is disabled for remaining batches.
	if !op.disabled {
		op.totalChecked += activeLen
		op.totalPassed += len(sel)
		op.batchesSeen++
		if op.batchesSeen >= 8 && op.totalChecked > 0 {
			rejected := op.totalChecked - op.totalPassed
			if rejected*20 < op.totalChecked { // <5% rejection
				op.disabled = true
			}
		}
	}

	op.selBuf = sel
	in.Sel = sel
	return in, nil
}

// probeKeyHash builds the key buffer for a row and checks the bloom filter.
func (op *BloomFilterOp) probeKeyHash(in *batch.RecordBatch, row int) bool {
	op.keyBuf = op.keyBuf[:0]
	for _, idx := range op.keyIdx {
		if idx < 0 {
			op.keyBuf = append(op.keyBuf, 1) // null flag
			continue
		}
		v := in.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			return false // null key → no match
		}
		op.keyBuf = append(op.keyBuf, 0) // not-null flag
		op.keyBuf = appendColumnValue(op.keyBuf, v, row, v.Type)
	}
	return bloomContains(op.bloom, op.bloomMask, bloomHashBytes(op.keyBuf))
}

func (op *BloomFilterOp) Close() error { return nil }

// KeyColumns returns the probe-side key column names this bloom filter checks.
func (op *BloomFilterOp) KeyColumns() []string { return op.leftKeys }

// Clone returns a new BloomFilterOp sharing the same bloom data.
func (op *BloomFilterOp) Clone() UnaryOperator {
	return &BloomFilterOp{
		bloom:         op.bloom,
		bloomMask:     op.bloomMask,
		leftKeys:      op.leftKeys,
		useIntKey:     op.useIntKey,
		useDualIntKey: op.useDualIntKey,
	}
}

// intValFromCol reads an int64 value from a column without null checks.
// Caller must ensure the row is not null.
func intValFromCol(col *batch.Vector, row int) int64 {
	switch col.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(col.Int32Data[row])
	default:
		return col.Int64Data[row]
	}
}

// bloomContains checks if a hash may be in the bloom filter.
// Standalone function so BloomFilterOp can call it without a HashJoin receiver.
func bloomContains(bloom []uint64, mask, hash uint64) bool {
	h1 := hash & mask
	h2 := (hash >> 17) & mask
	b1 := hash & 63
	b2 := (hash >> 6) & 63
	return (bloom[h1]>>b1)&1 != 0 && (bloom[h2]>>b2)&1 != 0
}

// BloomScanFilter holds bloom filter data for row-group-level scan pushdown.
type BloomScanFilter struct {
	Bloom     []uint64 // shared, read-only
	BloomMask uint64
	Column    string // probe-side join key column name
	UseIntKey bool   // true for single integer column join key
}

// BloomScanFilter returns a BloomScanFilter for scan-level pushdown.
// Only applicable for single-column integer join keys.
// Returns nil if not applicable.
func (op *BloomFilterOp) BloomScanFilter() *BloomScanFilter {
	if !op.useIntKey || len(op.leftKeys) != 1 {
		return nil
	}
	return &BloomScanFilter{
		Bloom:     op.bloom,
		BloomMask: op.bloomMask,
		Column:    op.leftKeys[0],
		UseIntKey: true,
	}
}

// BloomHashInt computes the bloom filter hash for an integer key.
// Exported for use by the scan layer for row-group-level pruning.
func BloomHashInt(key int64) uint64 {
	return bloomHashInt(key)
}

// BloomContains checks if a hash may be in the bloom filter.
// Exported for use by the scan layer for row-group-level pruning.
func BloomContains(bloom []uint64, mask, hash uint64) bool {
	return bloomContains(bloom, mask, hash)
}

// NewBloomFilterOp creates a BloomFilterOp from pre-built bloom data.
// Used for reverse bloom pushdown where the probe side's key set filters
// the build side's scan.
func NewBloomFilterOp(bloom []uint64, bloomMask uint64, keys []string, useIntKey bool) *BloomFilterOp {
	return &BloomFilterOp{
		bloom:     bloom,
		bloomMask: bloomMask,
		leftKeys:  keys, // "left" from the op's perspective = the column to check
		useIntKey: useIntKey,
	}
}

// BuildBloomFromBatches constructs a bloom filter from a column across
// multiple batches. Returns nil bloom if no rows. Used for reverse bloom
// pushdown: the probe side's join key values filter the build side's scan.
func BuildBloomFromBatches(batches []*batch.RecordBatch, keyCol string) (bloom []uint64, bloomMask uint64) {
	totalRows := 0
	for _, b := range batches {
		totalRows += b.ActiveLen()
	}
	if totalRows == 0 {
		return nil, 0
	}

	// Allocate bloom: ~10 bits per key for ~1% FPR
	nSlots := 1
	for nSlots*64 < totalRows*10 {
		nSlots *= 2
	}
	if nSlots < 8 {
		nSlots = 8
	}
	bloom = make([]uint64, nSlots)
	bloomMask = uint64(nSlots - 1)

	set := func(hash uint64) {
		h1 := hash & bloomMask
		h2 := (hash >> 17) & bloomMask
		b1 := hash & 63
		b2 := (hash >> 6) & 63
		bloom[h1] |= 1 << b1
		bloom[h2] |= 1 << b2
	}

	for _, b := range batches {
		colIdx := b.ColumnIndex(keyCol)
		if colIdx < 0 {
			continue
		}
		col := b.Columns[colIdx]
		isInt := col.Type == batch.TypeInt32 || col.Type == batch.TypeInt64 ||
			col.Type == batch.TypePort || col.Type == batch.TypeProtocol ||
			col.Type == batch.TypeDate

		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				if col.Nulls.IsNull(row) {
					continue
				}
				if isInt {
					set(bloomHashInt(intValFromCol(col, row)))
				} else {
					set(bloomHashBytes(col.BytesData.Value(row)))
				}
			}
		} else {
			for row := 0; row < b.Len; row++ {
				if col.Nulls.IsNull(row) {
					continue
				}
				if isInt {
					set(bloomHashInt(intValFromCol(col, row)))
				} else {
					set(bloomHashBytes(col.BytesData.Value(row)))
				}
			}
		}
	}
	return bloom, bloomMask
}
