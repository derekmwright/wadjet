package batch

import "github.com/derekmwright/caelum/internal/storage/parquet"

// DefaultBatchSize is the number of rows per batch (2048 for cache-friendly vectorized processing).
const DefaultBatchSize = 2048

// RecordBatch is the unit of data flowing between operators.
type RecordBatch struct {
	Columns []*Vector
	Schema  []parquet.Column
	Len     int
	Sel     []uint16 // selection vector: indices of active rows (nil = all rows active)
	pool    *BatchPool
}

// NewRecordBatch creates a new record batch with the given schema and row count.
func NewRecordBatch(schema []parquet.Column, numRows int) *RecordBatch {
	cols := make([]*Vector, len(schema))
	for i, col := range schema {
		cols[i] = NewVectorWithScale(col.Type, numRows, col.Scale)
	}
	return &RecordBatch{
		Columns: cols,
		Schema:  schema,
		Len:     numRows,
	}
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

// Reset clears the batch for reuse, keeping allocated memory.
func (b *RecordBatch) Reset(numRows int) {
	b.Len = numRows
	b.Sel = nil
	for _, col := range b.Columns {
		col.Len = numRows
		col.Nulls = NewBitmap(numRows)
		if col.Type == TypeString || col.Type == TypeBytes {
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
