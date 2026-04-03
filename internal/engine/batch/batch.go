package batch

import "github.com/citc-tech/wadjet/internal/storage/parquet"

// DefaultBatchSize is the number of rows per batch (2048 for cache-friendly vectorized processing).
const DefaultBatchSize = 2048

// RecordBatch is the unit of data flowing between operators.
type RecordBatch struct {
	Columns []*Vector
	Schema  []parquet.Column
	Len     int
	Sel     []uint32 // selection vector: indices of active rows (nil = all rows active)
	pool    *BatchPool
}

// NewRecordBatch creates a new record batch with the given schema and row count.
func NewRecordBatch(schema []parquet.Column, numRows int) *RecordBatch {
	cols := make([]*Vector, len(schema))
	for i, col := range schema {
		cols[i] = newVectorFromColumn(col, numRows)
	}
	return &RecordBatch{
		Columns: cols,
		Schema:  schema,
		Len:     numRows,
	}
}

// newVectorFromColumn creates a Vector from a Column definition, recursively
// initializing nested type children.
func newVectorFromColumn(col parquet.Column, numRows int) *Vector {
	v := NewVectorWithScale(col.Type, numRows, col.Scale)
	switch col.Type {
	case TypeVector:
		if col.Dimension > 0 {
			v.VectorDim = col.Dimension
			v.Float32Data = make([]float32, numRows*col.Dimension)
		}
	case TypeArray, TypeMap:
		if col.ElementType != nil {
			v.Child = newVectorFromColumn(*col.ElementType, 0)
		}
	case TypeRow:
		if len(col.Fields) > 0 {
			v.FieldNames = make([]string, len(col.Fields))
			v.Children = make([]*Vector, len(col.Fields))
			for i, f := range col.Fields {
				v.FieldNames[i] = f.Name
				v.Children[i] = newVectorFromColumn(f, numRows)
			}
		}
	}
	return v
}

// ActiveLen returns the number of active rows (respecting selection vector).
func (b *RecordBatch) ActiveLen() int {
	if b.Sel == nil {
		return b.Len
	}
	return len(b.Sel)
}

// ColumnByName returns the vector for the named column, or nil if not found.
func (b *RecordBatch) ColumnByName(name string) *Vector {
	for i, col := range b.Schema {
		if col.Name == name {
			return b.Columns[i]
		}
	}
	return nil
}

// ColumnIndex returns the index of the named column, or -1.
func (b *RecordBatch) ColumnIndex(name string) int {
	for i, col := range b.Schema {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// Release returns the batch to its pool if applicable.
func (b *RecordBatch) Release() {
	if b.pool != nil {
		b.pool.Put(b)
	}
}

// Detach removes the batch from its pool so Release() becomes a no-op.
// Use this when a batch will be stored long-term (e.g., hash join build side).
func (b *RecordBatch) Detach() {
	b.pool = nil
}

// Reset clears the batch for reuse, keeping allocated memory.
func (b *RecordBatch) Reset(numRows int) {
	b.Len = numRows
	b.Sel = nil
	for _, col := range b.Columns {
		col.Len = numRows
		col.Nulls.ResetNonNull(numRows)
		switch col.Type {
		case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
			col.BytesData.Reset()
		}
	}
}

// FromRows creates a RecordBatch from row-oriented data.
func FromRows(schema []parquet.Column, rows []map[string]any) *RecordBatch {
	b := NewRecordBatch(schema, len(rows))
	for i, row := range rows {
		for j, col := range schema {
			val := row[col.Name]
			b.Columns[j].SetValue(i, val)
		}
	}
	return b
}

// Compact creates a new RecordBatch containing only the active rows.
// If there is no selection vector, it returns the batch as-is.
func (b *RecordBatch) Compact() *RecordBatch {
	if b.Sel == nil {
		return b
	}
	n := len(b.Sel)
	out := NewRecordBatch(b.Schema, n)
	for di, si := range b.Sel {
		for j := range b.Schema {
			src := b.Columns[j]
			dst := out.Columns[j]
			if src.Nulls.IsNullFast(int(si)) {
				dst.Nulls.SetNull(di)
				switch dst.Type {
				case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
					dst.BytesData.Set(di, nil)
				}
				continue
			}
			dst.Nulls.SetValid(di)
			switch dst.Type {
			case TypeBool:
				dst.BoolData[di] = src.BoolData[si]
			case TypeInt32, TypePort, TypeProtocol, TypeDate:
				dst.Int32Data[di] = src.Int32Data[si]
			case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
				dst.Int64Data[di] = src.Int64Data[si]
			case TypeFloat32:
				dst.Float32Data[di] = src.Float32Data[si]
			case TypeFloat64:
				dst.Float64Data[di] = src.Float64Data[si]
			case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
				dst.BytesData.SetFrom(di, &src.BytesData, int(si))
			case TypeDecimal:
				dst.DecimalData.Data[di] = src.DecimalData.Data[si]
			case TypeVector:
				dim := src.VectorDim
				if dim > 0 {
					srcOff := int(si) * dim
					dstOff := di * dim
					copy(dst.Float32Data[dstOff:dstOff+dim], src.Float32Data[srcOff:srcOff+dim])
				}
			case TypeArray, TypeMap:
				if src.Child != nil && dst.Child != nil {
					start := int(src.Offsets[si])
					end := int(src.Offsets[si+1])
					dstStart := int32(dst.Child.Len)
					for k := start; k < end; k++ {
						appendToVector(dst.Child, src.Child.GetValue(k))
					}
					dst.Offsets[di] = dstStart
					dst.Offsets[di+1] = int32(dst.Child.Len)
				}
			case TypeRow:
				for ci, child := range src.Children {
					if ci < len(dst.Children) {
						dstChild := dst.Children[ci]
						if child.Nulls.IsNullFast(int(si)) {
							dstChild.Nulls.SetNull(di)
						} else {
							dstChild.SetValue(di, child.GetValue(int(si)))
						}
					}
				}
			}
		}
	}
	return out
}

// ToRows converts a RecordBatch to row-oriented data.
func (b *RecordBatch) ToRows() []map[string]any {
	rows := make([]map[string]any, 0, b.ActiveLen())
	if b.Sel != nil {
		for _, idx := range b.Sel {
			row := make(map[string]any, len(b.Schema))
			for j, col := range b.Schema {
				row[col.Name] = b.Columns[j].GetValue(int(idx))
			}
			rows = append(rows, row)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			row := make(map[string]any, len(b.Schema))
			for j, col := range b.Schema {
				row[col.Name] = b.Columns[j].GetValue(i)
			}
			rows = append(rows, row)
		}
	}
	return rows
}
