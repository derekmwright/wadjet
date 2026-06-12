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
	// ownerID identifies which memory-tier owner currently accounts this batch's
	// bytes. 0 means unstamped (not pool-owned, e.g. the over-size escape hatch
	// or a Detach'd long-lived batch); ReservoirOwner means a BatchPool minted it
	// and its bytes are charged to the pool reservoir. Survives Reset so a reused
	// batch keeps its owner. The non-zero sentinel keeps the zero value
	// unambiguous, matching the Sel==nil / pool==nil "absent" conventions.
	ownerID uint64
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

// MemBytes returns the in-memory byte footprint of the batch's column data,
// summing each Vector's MemBytes(). It deliberately omits operator-specific
// overhead (e.g. the HashJoin hash-index charge) — that stays at the call site
// (see hashBuildBytes in package exec). Replaces the per-type estimate that
// lived in exec.EstimateBatchBytes.
func (b *RecordBatch) MemBytes() int64 {
	var size int64
	for _, v := range b.Columns {
		size += v.MemBytes()
	}
	return size
}

// EnsureCapacity grows every column so positions [0, n) are addressable for
// in-place writes, preserving existing data, and sets Len to n. Used by
// append-style builders (e.g. the hash-join per-partition accumulator) that
// grow a batch across many source batches rather than pre-sizing to a
// worst-case capacity. See Vector.EnsureLen for per-type behavior and the
// nested-type caveat.
func (b *RecordBatch) EnsureCapacity(n int) {
	for _, col := range b.Columns {
		col.EnsureLen(n)
	}
	if n > b.Len {
		b.Len = n
	}
}

// Reset clears the batch for reuse, keeping allocated memory.
func (b *RecordBatch) Reset(numRows int) {
	b.Len = numRows
	b.Sel = nil
	for _, col := range b.Columns {
		resetVectorForReuse(col, numRows)
	}
}

// resetVectorForReuse clears a vector's per-row state for pooled reuse,
// recursing into nested children. Without the recursion, a pooled ROW/ARRAY
// column kept its previous cycle's child arenas, offsets and null bits —
// the first reused row read back the prior batch's data concatenated with
// the new value, and child arenas grew monotonically per reuse cycle.
func resetVectorForReuse(col *Vector, numRows int) {
	col.Len = numRows
	col.Nulls.ResetNonNull(numRows)
	switch col.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		col.BytesData.Reset()
	case TypeArray, TypeMap:
		for i := 0; i < numRows+1 && i < len(col.Offsets); i++ {
			col.Offsets[i] = 0
		}
		if col.Child != nil {
			resetVectorForReuse(col.Child, 0)
			// Child element storage is append-built; truncate the arenas so
			// CopyValueFrom/AppendFrom start from a zero-length child.
			truncateVectorStorage(col.Child)
		}
	case TypeRow:
		for _, ch := range col.Children {
			resetVectorForReuse(ch, numRows)
		}
	}
}

// truncateVectorStorage re-slices a vector's element storage to zero length
// (capacity retained for reuse). Recurses into nested children.
func truncateVectorStorage(v *Vector) {
	v.Len = 0
	v.BoolData = v.BoolData[:0]
	v.Int32Data = v.Int32Data[:0]
	v.Int64Data = v.Int64Data[:0]
	v.Float32Data = v.Float32Data[:0]
	v.Float64Data = v.Float64Data[:0]
	if v.DecimalData.Data != nil {
		v.DecimalData.Data = v.DecimalData.Data[:0]
	}
	v.BytesData.Reset()
	if len(v.BytesData.Offsets) > 0 {
		v.BytesData.Offsets = v.BytesData.Offsets[:1]
		v.BytesData.Offsets[0] = 0
	}
	if v.Type == TypeArray || v.Type == TypeMap {
		if len(v.Offsets) > 0 {
			v.Offsets = v.Offsets[:1]
			v.Offsets[0] = 0
		}
		if v.Child != nil {
			truncateVectorStorage(v.Child)
		}
	}
	for _, ch := range v.Children {
		truncateVectorStorage(ch)
	}
	v.Nulls.ResetNonNull(0)
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

// Compact materializes the selection vector into a contiguous batch using
// the typed nested-aware value copier. Returns the batch unchanged when no
// selection vector is set.
//
// Was previously a hand-rolled per-type switch whose ROW case wrote null
// child rows via SetNull alone — never advancing a string child's offset
// slot — so every later row in that child read back as concatenated
// garbage (same bug class as the windowCopyVectorRange nullable-BYTES fix;
// regression test TestCompact_RowChildNullableString).
func (b *RecordBatch) Compact() *RecordBatch {
	if b.Sel == nil {
		return b
	}
	n := len(b.Sel)
	out := NewRecordBatch(b.Schema, n)
	for j := range b.Schema {
		src := b.Columns[j]
		dst := out.Columns[j]
		for di, si := range b.Sel {
			dst.CopyValueFrom(di, src, int(si))
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
