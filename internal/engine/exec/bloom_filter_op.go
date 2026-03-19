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
	selBuf        []uint16 // scratch for selection vector
	keyBuf        []byte   // scratch for multi-column key serialization
}

func (op *BloomFilterOp) Init(_ context.Context) error { return nil }

func (op *BloomFilterOp) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || in.ActiveLen() == 0 {
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
		op.selBuf = make([]uint16, 0, activeLen)
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
							sel = append(sel, uint16(i))
						}
					}
				default:
					data := col.Int64Data
					for i := 0; i < in.Len; i++ {
						if bloomContains(op.bloom, op.bloomMask, bloomHashInt(data[i])) {
							sel = append(sel, uint16(i))
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
						sel = append(sel, uint16(i))
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
						sel = append(sel, uint16(i))
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					a, b, ok := dualIntKeyFromVectors(col0, col1, i)
					if ok && bloomContains(op.bloom, op.bloomMask, bloomHashInt(dualIntHash(a, b))) {
						sel = append(sel, uint16(i))
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
					sel = append(sel, uint16(i))
				}
			}
		}
	}

	if len(sel) == 0 {
		return nil, nil
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
