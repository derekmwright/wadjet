package batch

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"time"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// formatIPv4 formats a uint32 IPv4 address as a string without allocating net.IP.
func formatIPv4(v uint32) string {
	var buf [15]byte // max "255.255.255.255"
	n := 0
	for i := 3; i >= 0; i-- {
		if i < 3 {
			buf[n] = '.'
			n++
		}
		octet := v >> (uint(i) * 8) & 0xFF
		if octet >= 100 {
			buf[n] = '0' + byte(octet/100)
			n++
			octet %= 100
			buf[n] = '0' + byte(octet/10)
			n++
			buf[n] = '0' + byte(octet%10)
			n++
		} else if octet >= 10 {
			buf[n] = '0' + byte(octet/10)
			n++
			buf[n] = '0' + byte(octet%10)
			n++
		} else {
			buf[n] = '0' + byte(octet)
			n++
		}
	}
	return string(buf[:n])
}

// formatMAC formats a uint64 (lower 48 bits) as a MAC address without allocating net.HardwareAddr.
func formatMAC(v uint64) string {
	const hex = "0123456789abcdef"
	var buf [17]byte // "aa:bb:cc:dd:ee:ff"
	for i := 0; i < 6; i++ {
		if i > 0 {
			buf[i*3-1] = ':'
		}
		b := byte(v >> uint((5-i)*8))
		buf[i*3] = hex[b>>4]
		buf[i*3+1] = hex[b&0xf]
	}
	return string(buf[:17])
}

// TypeID is an alias for the parquet TypeID used throughout the engine.
type TypeID = parquet.TypeID

const (
	TypeBool      = parquet.TypeBool
	TypeInt32     = parquet.TypeInt32
	TypeInt64     = parquet.TypeInt64
	TypeFloat32   = parquet.TypeFloat32
	TypeFloat64   = parquet.TypeFloat64
	TypeString    = parquet.TypeString
	TypeBytes     = parquet.TypeBytes
	TypeTimestamp = parquet.TypeTimestamp
	TypeIPv4     = parquet.TypeIPv4
	TypeIPv6     = parquet.TypeIPv6
	TypeCIDR     = parquet.TypeCIDR
	TypeMAC      = parquet.TypeMAC
	TypePort     = parquet.TypePort
	TypeProtocol = parquet.TypeProtocol
	TypeDuration = parquet.TypeDuration
	TypeUUID     = parquet.TypeUUID
	TypeDate     = parquet.TypeDate
	TypeDecimal  = parquet.TypeDecimal
	TypeArray    = parquet.TypeArray
	TypeRow      = parquet.TypeRow
	TypeMap      = parquet.TypeMap
	TypeVector   = parquet.TypeVector
)

// BytesColumn stores variable-length byte data (strings, binary) with zero
// per-row allocations using an offset/data layout.
type BytesColumn struct {
	Offsets []uint32 // len = num_rows + 1
	Data    []byte   // contiguous buffer

	// ShapeOnly marks a column decoded for its SHAPE only: Offsets carry
	// the real per-row byte lengths but Data was never written (the
	// lengths-only scan decode, internal/engine/scan/lengths_decode.go).
	// LENGTH()/octet_length(), IS [NOT] NULL and the empty-string
	// comparisons answer off Offsets and the null mask alone; any attempt
	// to read a VALUE is a planner-analysis bug and panics immediately
	// rather than returning a wrong answer. Copy paths propagate the flag
	// instead of moving bytes that do not exist.
	ShapeOnly bool
}

// NewBytesColumn creates a new BytesColumn with the given capacity.
// Pre-allocates offsets for positional access (all offsets start at 0 = empty strings).
// Data arena is lazily allocated: starts empty and grows on first use. Hot paths
// (scan BulkSet, gather PreAllocBytes) know the exact size they need, so eager
// pre-allocation just wastes memclr on unused capacity. For pooled batches, the
// grown capacity is retained across Reset cycles.
func NewBytesColumn(capacity int) BytesColumn {
	offsets := make([]uint32, capacity+1)
	return BytesColumn{
		Offsets: offsets,
	}
}

// PreAllocBytes ensures the Data arena has at least n bytes of capacity.
// Use this when the expected total byte size is known (e.g., from Parquet metadata)
// to avoid reallocations during sequential Set calls.
func (bc *BytesColumn) PreAllocBytes(n int) {
	if cap(bc.Data) < n {
		newData := make([]byte, len(bc.Data), n)
		copy(newData, bc.Data)
		bc.Data = newData
	}
}

// Set writes a value at positional index i. The BytesColumn must have been
// created with NewBytesColumn(capacity >= i+1). Values must be set in order
// (i = 0, 1, 2, ...) because later offsets depend on prior data length.
func (bc *BytesColumn) Set(i int, val []byte) {
	bc.Data = append(bc.Data, val...)
	bc.Offsets[i+1] = uint32(len(bc.Data))
}

// SetString writes a string value at positional index i. Same contract as
// Set (sequential i), but takes a string: `append(dst, s...)` copies
// straight out of the string, where Set's callers had to materialize a
// []byte(s) conversion first — one heap allocation per row on the
// string-producing projection paths.
func (bc *BytesColumn) SetString(i int, val string) {
	bc.Data = append(bc.Data, val...)
	bc.Offsets[i+1] = uint32(len(bc.Data))
}

// BulkSet copies a contiguous block of byte array data into the column,
// computing offsets from the source offset array. This replaces n individual
// Set calls with a single bulk append + offset arithmetic, reducing memmove
// overhead for Parquet page loading.
func (bc *BytesColumn) BulkSet(dstOffset int, srcData []byte, srcOffsets []uint32, n int) {
	baseOff := uint32(len(bc.Data))
	srcBase := srcOffsets[0]
	bc.Data = append(bc.Data, srcData[srcBase:srcOffsets[n]]...)
	for i := 0; i < n; i++ {
		bc.Offsets[dstOffset+i+1] = baseOff + (srcOffsets[i+1] - srcBase)
	}
}

// BulkCopy copies a contiguous range [srcOff, srcOff+count) from src into
// dst at [dstOff, dstOff+count). Uses a single Data append + offset arithmetic
// instead of per-element Set calls, reducing memmove overhead for batch merging.
func (dst *BytesColumn) BulkCopy(dstOff int, src *BytesColumn, srcOff, count int) {
	if src.ShapeOnly {
		dst.copyShapeRange(dstOff, src, srcOff, count)
		return
	}
	baseOff := uint32(len(dst.Data))
	srcStart := src.Offsets[srcOff]
	srcEnd := src.Offsets[srcOff+count]
	dst.Data = append(dst.Data, src.Data[srcStart:srcEnd]...)
	for i := 0; i < count; i++ {
		dst.Offsets[dstOff+i+1] = baseOff + (src.Offsets[srcOff+i+1] - srcStart)
	}
}

// SetFrom copies a single value from src at position si into dst at position di.
// Combines Value + Set into one call, avoiding the intermediate slice creation
// and reducing function call overhead in gather loops. Values must be set in
// order (di = 0, 1, 2, ...) because later offsets depend on prior data length.
func (dst *BytesColumn) SetFrom(di int, src *BytesColumn, si int) {
	if src.ShapeOnly {
		dst.copyShapeRange(di, src, si, 1)
		return
	}
	srcStart := src.Offsets[si]
	srcEnd := src.Offsets[si+1]
	dst.Data = append(dst.Data, src.Data[srcStart:srcEnd]...)
	dst.Offsets[di+1] = uint32(len(dst.Data))
}

// copyShapeRange propagates a shape-only source: the destination inherits
// the per-row lengths and the ShapeOnly mark, and no bytes move. Mixing a
// shape-only source into a destination that already holds real values would
// silently corrupt it, so that combination panics with the same diagnosis
// Value gives.
func (dst *BytesColumn) copyShapeRange(dstOff int, src *BytesColumn, srcOff, count int) {
	if len(dst.Data) > 0 {
		panic("batch: shape-only column copied into a destination holding values — some consumer of this column is not a shape consumer (planner analysis bug)")
	}
	dst.ShapeOnly = true
	cur := dst.Offsets[dstOff]
	for i := 0; i < count; i++ {
		cur += uint32(src.LengthAt(srcOff + i))
		dst.Offsets[dstOff+i+1] = cur
	}
}

// Value returns the byte slice at position i.
//
// Defensive against a gather-output hazard: when HashJoin's
// gatherBuildVector skips unmatched rows without calling
// BytesData.SetFrom, the destination Offsets may end up with
// Offsets[i+1] == 0 while Offsets[i] > 0, producing a malformed
// descending pair. Treat this as empty rather than panicking; the
// null bitmap alongside the column records the "no value" state
// authoritatively, and downstream filter / projection kernels already
// consult it.
func (bc *BytesColumn) Value(i int) []byte {
	if bc.ShapeOnly {
		panic("batch: value read from a shape-only BytesColumn — the scan decoded lengths only, so some consumer of this column is not a shape consumer (planner analysis bug)")
	}
	start := bc.Offsets[i]
	end := bc.Offsets[i+1]
	if end < start {
		return nil
	}
	return bc.Data[start:end]
}

// LengthAt returns the byte length of row i without reading the value. It
// is the only value-shaped accessor valid on a shape-only column, and it
// mirrors Value's defensive handling of the descending-offset hazard.
func (bc *BytesColumn) LengthAt(i int) int {
	start := bc.Offsets[i]
	end := bc.Offsets[i+1]
	if end < start {
		return 0
	}
	return int(end - start)
}

// StringValue returns the string at position i.
func (bc *BytesColumn) StringValue(i int) string {
	return string(bc.Value(i))
}

// UnsafeStringValue returns a zero-copy string view of the value at position i.
// The returned string shares the BytesColumn's backing buffer and is only valid
// while the BytesColumn is not modified or recycled. Use for transient comparisons
// in filter/sort kernels where the string is consumed immediately.
func (bc *BytesColumn) UnsafeStringValue(i int) string {
	b := bc.Value(i)
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// Len returns the number of values.
func (bc *BytesColumn) Len() int {
	return len(bc.Offsets) - 1
}

// Reset clears the bytes column for reuse.
func (bc *BytesColumn) Reset() {
	for i := range bc.Offsets {
		bc.Offsets[i] = 0
	}
	bc.Data = bc.Data[:0]
	bc.ShapeOnly = false
}

// MemBytes returns the heap bytes consumed by the offset and data slices.
//
// Offsets are sized by len (the logical rows+1), but the data arena is sized by
// cap: a pooled BytesColumn retains its grown arena capacity across Reset cycles
// (see NewBytesColumn), so cap(Data) is the true resident footprint. This is the
// honest byte count that replaces the b.Len*48 estimate in EstimateBatchBytes.
func (bc *BytesColumn) MemBytes() int64 {
	return int64(len(bc.Offsets))*4 + int64(cap(bc.Data)) // 4 = sizeof(uint32) offset
}

// Vector holds a single column of data. Uses typed slices instead of interface{}.
type Vector struct {
	Type        TypeID
	Len         int
	Nulls       Bitmap
	BoolData    []bool
	Int32Data   []int32
	Int64Data   []int64
	Float32Data []float32
	Float64Data []float64
	BytesData   BytesColumn
	DecimalData DecimalColumn // for TypeDecimal

	// VECTOR type: fixed-dimension float32 embeddings
	// Row i's vector: Float32Data[i*VectorDim : (i+1)*VectorDim]
	VectorDim int // VECTOR: dimensionality (number of float32 elements per row)

	// Nested type fields (ARRAY, ROW, MAP)
	Offsets    []int32   // ARRAY/MAP: offsets[i]..offsets[i+1] delimit child elements for row i
	Child      *Vector   // ARRAY: flat vector of all element values
	Children   []*Vector // ROW: one vector per field (same length as parent)
	FieldNames []string  // ROW: names of child fields

	// View (dictionary) form: when Base != nil this vector owns no typed
	// storage — logical row i is Base row Indices[i]. Nulls is the view's OWN
	// override bitmap (a null bit marks the row null regardless of Base; a
	// valid bit defers to Base's nullness through Indices). Base is always an
	// owned vector: NewViewVector composes indices when handed a view base, so
	// views never chain. Views are read-only and understood only by the
	// view-aware accessors (GetValue, CopyValueFrom-as-source, Flatten,
	// MemBytes); typed hot-path accessors (GetInt64, Int64Data[i], ...) fail
	// loud on a view because the typed slices are nil. See view.go.
	Base    *Vector
	Indices []uint32
}

// NewVector creates a new vector of the given type and length.
func NewVector(typ TypeID, length int) *Vector {
	return NewVectorWithScale(typ, length, 0)
}

// NewVectorWithScale creates a new vector with scale metadata (used for DECIMAL).
func NewVectorWithScale(typ TypeID, length int, scale int) *Vector {
	v := &Vector{
		Type:  typ,
		Len:   length,
		Nulls: NewBitmap(length),
	}
	switch typ {
	case TypeBool:
		v.BoolData = make([]bool, length)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		v.Int32Data = make([]int32, length)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		v.Int64Data = make([]int64, length)
	case TypeFloat32:
		v.Float32Data = make([]float32, length)
	case TypeFloat64:
		v.Float64Data = make([]float64, length)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData = NewBytesColumn(length)
	case TypeDecimal:
		v.DecimalData = NewDecimalColumn(length, scale)
	case TypeVector:
		// VectorDim is set by the caller via NewVectorVector or newVectorFromColumn
	case TypeArray, TypeMap:
		v.Offsets = make([]int32, length+1)
		// Child vector is set by the caller after construction
	case TypeRow:
		// Children and FieldNames are set by the caller after construction
	}
	return v
}

// EnsureLen grows the vector's backing storage so positions [0, n) are
// addressable for in-place writes (Set / index-assign), preserving existing
// values, defaulting new fixed-width slots to zero and new null bits to
// non-null. Backing arrays grow geometrically (via append) for amortized O(1)
// appends, so an append-style builder can grow a column across many source
// batches instead of pre-sizing to a worst-case capacity. Sets Len to n.
//
// Scalar, bytes, decimal and fixed-dim VECTOR columns are fully supported.
// Nested ARRAY/MAP element storage and ROW children are NOT grown here (their
// element storage is appended by SetValue); callers that build nested columns
// should use pre-sized batches. The hash-join accumulator path that relies on
// EnsureLen guards nested schemas to a pre-sized path for this reason.
func (v *Vector) EnsureLen(n int) {
	if n <= v.Len {
		return
	}
	if v.Base != nil {
		// Views are read-only; growing one for in-place writes is always a
		// caller bug. Fail loud rather than writing into nil typed slices.
		panic("batch: EnsureLen on a view vector — Flatten() first")
	}
	v.Nulls.EnsureLen(n)
	switch v.Type {
	case TypeBool:
		if cap(v.BoolData) >= n {
			v.BoolData = v.BoolData[:n]
		} else {
			g := make([]bool, n, growCap(cap(v.BoolData), n))
			copy(g, v.BoolData)
			v.BoolData = g
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		if cap(v.Int32Data) >= n {
			v.Int32Data = v.Int32Data[:n]
		} else {
			g := make([]int32, n, growCap(cap(v.Int32Data), n))
			copy(g, v.Int32Data)
			v.Int32Data = g
		}
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		if cap(v.Int64Data) >= n {
			v.Int64Data = v.Int64Data[:n]
		} else {
			g := make([]int64, n, growCap(cap(v.Int64Data), n))
			copy(g, v.Int64Data)
			v.Int64Data = g
		}
	case TypeFloat32:
		if cap(v.Float32Data) >= n {
			v.Float32Data = v.Float32Data[:n]
		} else {
			g := make([]float32, n, growCap(cap(v.Float32Data), n))
			copy(g, v.Float32Data)
			v.Float32Data = g
		}
	case TypeFloat64:
		if cap(v.Float64Data) >= n {
			v.Float64Data = v.Float64Data[:n]
		} else {
			g := make([]float64, n, growCap(cap(v.Float64Data), n))
			copy(g, v.Float64Data)
			v.Float64Data = g
		}
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		// Offsets need length n+1 (one trailing offset). Data grows separately
		// via Set/SetFrom appends.
		if cap(v.BytesData.Offsets) >= n+1 {
			v.BytesData.Offsets = v.BytesData.Offsets[:n+1]
		} else {
			g := make([]uint32, n+1, growCap(cap(v.BytesData.Offsets), n+1))
			copy(g, v.BytesData.Offsets)
			v.BytesData.Offsets = g
		}
	case TypeDecimal:
		if cap(v.DecimalData.Data) >= n {
			v.DecimalData.Data = v.DecimalData.Data[:n]
		} else {
			g := make([]Int128, n, growCap(cap(v.DecimalData.Data), n))
			copy(g, v.DecimalData.Data)
			v.DecimalData.Data = g
		}
	case TypeVector:
		if v.VectorDim > 0 {
			need := n * v.VectorDim
			if cap(v.Float32Data) >= need {
				v.Float32Data = v.Float32Data[:need]
			} else {
				g := make([]float32, need, growCap(cap(v.Float32Data), need))
				copy(g, v.Float32Data)
				v.Float32Data = g
			}
		}
	case TypeArray, TypeMap:
		if cap(v.Offsets) >= n+1 {
			v.Offsets = v.Offsets[:n+1]
		} else {
			g := make([]int32, n+1, growCap(cap(v.Offsets), n+1))
			copy(g, v.Offsets)
			v.Offsets = g
		}
	case TypeRow:
		for _, c := range v.Children {
			c.EnsureLen(n)
		}
	}
	v.Len = n
}

// growCap returns a new capacity >= need, at least doubling the old capacity so
// repeated EnsureLen growth amortizes to O(1) per appended row.
func growCap(old, need int) int {
	c := old
	if c == 0 {
		c = need
	}
	for c < need {
		c *= 2
	}
	return c
}

// NewVectorVector creates a new VECTOR column with fixed dimensionality.
// Storage: Float32Data of length * dim, where row i occupies [i*dim, (i+1)*dim).
func NewVectorVector(length, dim int) *Vector {
	v := NewVector(TypeVector, length)
	v.VectorDim = dim
	v.Float32Data = make([]float32, length*dim)
	return v
}

// VectorAt returns a slice of float32 values for row i of a VECTOR column.
func (v *Vector) VectorAt(i int) []float32 {
	if v.VectorDim <= 0 || i*v.VectorDim >= len(v.Float32Data) {
		return nil
	}
	return v.Float32Data[i*v.VectorDim : (i+1)*v.VectorDim]
}

// SetVector sets the float32 values for row i of a VECTOR column.
func (v *Vector) SetVector(i int, vals []float32) {
	if v.VectorDim <= 0 {
		return
	}
	copy(v.Float32Data[i*v.VectorDim:(i+1)*v.VectorDim], vals)
	v.Nulls.SetValid(i)
}

// NewArrayVector creates a new ARRAY vector with the given length and element type.
// The child vector starts with capacity 0; callers append elements and update offsets.
func NewArrayVector(length int, elemType TypeID) *Vector {
	v := NewVector(TypeArray, length)
	v.Child = NewVector(elemType, 0)
	return v
}

// NewRowVector creates a new ROW/STRUCT vector with named child fields.
func NewRowVector(length int, fieldNames []string, fieldTypes []TypeID) *Vector {
	v := NewVector(TypeRow, length)
	v.FieldNames = fieldNames
	v.Children = make([]*Vector, len(fieldNames))
	for i, ft := range fieldTypes {
		v.Children[i] = NewVector(ft, length)
	}
	return v
}

// NewMapVector creates a new MAP vector. Internally stored as ARRAY(ROW("key","value")).
func NewMapVector(length int, keyType, valueType TypeID) *Vector {
	v := NewVector(TypeMap, length)
	child := NewRowVector(0, []string{"key", "value"}, []TypeID{keyType, valueType})
	child.Type = TypeRow
	v.Child = child
	return v
}

// GetValue returns the value at position i as an interface{}.
// Note: returns boxed values for numeric types (unavoidable with any return type).
// Prefer typed accessors (GetInt64, GetFloat64, etc.) in hot paths.
func (v *Vector) GetValue(i int) any {
	if v.Nulls.IsNullFast(i) {
		return nil
	}
	if v.Base != nil {
		// View: own-null already checked above; defer to base through the
		// index (base applies its own null bitmap).
		return v.Base.GetValue(int(v.Indices[i]))
	}
	// Hot types first as if-chain for better branch prediction
	switch v.Type {
	case TypeInt64, TypeTimestamp:
		return v.Int64Data[i]
	case TypeFloat64:
		return v.Float64Data[i]
	case TypeString:
		return v.BytesData.StringValue(i)
	case TypeInt32:
		return v.Int32Data[i]
	case TypeBool:
		return v.BoolData[i]
	case TypeFloat32:
		return v.Float32Data[i]
	case TypeBytes:
		return v.BytesData.Value(i)
	case TypeIPv4:
		return formatIPv4(uint32(v.Int64Data[i]))
	case TypeIPv6:
		raw := v.BytesData.Value(i)
		if len(raw) == 16 {
			return net.IP(raw).String()
		}
		return ""
	case TypeCIDR:
		return v.BytesData.StringValue(i)
	case TypeMAC:
		return formatMAC(uint64(v.Int64Data[i]))
	case TypePort, TypeProtocol:
		return v.Int32Data[i]
	case TypeDuration:
		return v.Int64Data[i]
	case TypeUUID:
		raw := v.BytesData.Value(i)
		if len(raw) == 16 {
			return formatUUID(raw)
		}
		return ""
	case TypeDate:
		return FormatDate(v.Int32Data[i])
	case TypeDecimal:
		return v.DecimalData.Data[i].FormatDecimal(v.DecimalData.Scale)
	case TypeVector:
		if v.VectorDim <= 0 {
			return nil
		}
		dst := make([]float32, v.VectorDim)
		copy(dst, v.Float32Data[i*v.VectorDim:(i+1)*v.VectorDim])
		return dst
	case TypeArray, TypeMap:
		if v.Child == nil {
			return nil
		}
		start := int(v.Offsets[i])
		end := int(v.Offsets[i+1])
		elems := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			elems = append(elems, v.Child.GetValue(j))
		}
		return elems
	case TypeRow:
		if v.Children == nil {
			return nil
		}
		row := make(map[string]any, len(v.Children))
		for j, child := range v.Children {
			name := fmt.Sprintf("f%d", j)
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			row[name] = child.GetValue(i)
		}
		return row
	default:
		return nil
	}
}

// SetValue sets the value at position i from an interface{}.
// For string/bytes types, values must be set in sequential order (i = 0, 1, 2, ...).
func (v *Vector) SetValue(i int, val any) {
	if val == nil {
		// WriteNullAt advances variable-length bookkeeping for EVERY shape —
		// bytes offsets, ARRAY/MAP offsets, ROW children. The old inline
		// version handled only the flat bytes types, so a nil on a nested
		// column skipped its slot and every later row read back shifted
		// (the recurring offsets-on-NULL corruption class).
		v.WriteNullAt(i)
		return
	}
	v.Nulls.SetValid(i)
	switch v.Type {
	case TypeBool:
		// Checked, not val.(bool): every other case here tolerates a value of
		// the wrong shape, and an unchecked assertion made this one case a
		// process-wide panic instead — the same "a wrong type may cost a
		// wrong answer, never the server" rule the vec kernels follow (#310).
		switch tv := val.(type) {
		case bool:
			v.BoolData[i] = tv
		case int64:
			v.BoolData[i] = tv != 0
		case float64:
			v.BoolData[i] = tv != 0
		default:
			v.Nulls.SetNull(i)
		}
	case TypeInt32:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case float64:
			v.Int32Data[i] = int32(tv)
		}
	case TypeInt64, TypeTimestamp:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		}
	case TypeFloat32:
		switch tv := val.(type) {
		case float32:
			v.Float32Data[i] = tv
		case float64:
			v.Float32Data[i] = float32(tv)
		}
	case TypeFloat64:
		switch tv := val.(type) {
		case float64:
			v.Float64Data[i] = tv
		case float32:
			v.Float64Data[i] = float64(tv)
		case int64:
			v.Float64Data[i] = float64(tv)
		case int:
			v.Float64Data[i] = float64(tv)
		}
	case TypeString:
		switch tv := val.(type) {
		case string:
			v.BytesData.Set(i, []byte(tv))
		case []byte:
			v.BytesData.Set(i, tv)
		default:
			// Coerce non-string values to string representation
			v.BytesData.Set(i, []byte(fmt.Sprint(val)))
		}
	case TypeBytes:
		// Checked for the same reason as TypeBool above.
		switch tv := val.(type) {
		case []byte:
			v.BytesData.Set(i, tv)
		case string:
			v.BytesData.Set(i, []byte(tv))
		default:
			v.BytesData.Set(i, []byte(fmt.Sprint(val)))
		}
	case TypeIPv4:
		switch tv := val.(type) {
		case string:
			ip := net.ParseIP(tv)
			if ip == nil {
				return
			}
			ip4 := ip.To4()
			if ip4 == nil {
				return
			}
			v.Int64Data[i] = int64(binary.BigEndian.Uint32(ip4))
		case int64:
			v.Int64Data[i] = tv
		case int32:
			v.Int64Data[i] = int64(tv)
		default:
			return
		}
	case TypeIPv6:
		s, ok := val.(string)
		if !ok {
			v.BytesData.Set(i, nil)
			return
		}
		ip := net.ParseIP(s)
		if ip == nil {
			v.BytesData.Set(i, nil)
			return
		}
		ip6 := ip.To16()
		v.BytesData.Set(i, []byte(ip6))
	case TypeCIDR:
		s, ok := val.(string)
		if !ok {
			v.BytesData.Set(i, nil)
			return
		}
		v.BytesData.Set(i, []byte(s))
	case TypeMAC:
		switch tv := val.(type) {
		case string:
			hw, err := net.ParseMAC(tv)
			if err != nil || len(hw) != 6 {
				return
			}
			var n uint64
			for _, b := range hw {
				n = (n << 8) | uint64(b)
			}
			v.Int64Data[i] = int64(n)
		case int64:
			v.Int64Data[i] = tv
		default:
			return
		}
	case TypePort, TypeProtocol:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case float64:
			v.Int32Data[i] = int32(tv)
		}
	case TypeDuration:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		}
	case TypeUUID:
		switch tv := val.(type) {
		case string:
			raw := parseUUID(tv)
			v.BytesData.Set(i, raw)
		case []byte:
			v.BytesData.Set(i, tv)
		default:
			v.BytesData.Set(i, nil)
		}
	case TypeDate:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case string:
			v.Int32Data[i] = parseDateString(tv)
		}
	case TypeDecimal:
		switch tv := val.(type) {
		case Int128:
			v.DecimalData.Data[i] = tv
		case int64:
			v.DecimalData.Data[i] = Int128From(tv)
		case int:
			v.DecimalData.Data[i] = Int128From(int64(tv))
		case int32:
			v.DecimalData.Data[i] = Int128From(int64(tv))
		case float64:
			v.DecimalData.Data[i] = Int128FromFloat64(tv, v.DecimalData.Scale)
		case float32:
			v.DecimalData.Data[i] = Int128FromFloat64(float64(tv), v.DecimalData.Scale)
		case string:
			v.DecimalData.Data[i] = ParseDecimalString(tv, v.DecimalData.Scale)
		}
	case TypeVector:
		if v.VectorDim <= 0 {
			return
		}
		switch tv := val.(type) {
		case []float32:
			v.SetVector(i, tv)
		case []any:
			off := i * v.VectorDim
			for j := 0; j < v.VectorDim && j < len(tv); j++ {
				switch fv := tv[j].(type) {
				case float32:
					v.Float32Data[off+j] = fv
				case float64:
					v.Float32Data[off+j] = float32(fv)
				case int64:
					v.Float32Data[off+j] = float32(fv)
				}
			}
		}
	case TypeArray, TypeMap:
		if v.Child == nil {
			return
		}
		var elems []any
		switch tv := val.(type) {
		case []any:
			elems = tv
		case []map[string]any:
			// parquet-go returns []map[string]any for repeated groups
			elems = make([]any, len(tv))
			for j, m := range tv {
				// Unwrap single-key maps (LIST element wrapper: {"element": value})
				if len(m) == 1 {
					for _, v := range m {
						elems[j] = v
						break
					}
				} else {
					elems[j] = m
				}
			}
		default:
			return
		}
		start := v.Child.Len
		for _, elem := range elems {
			appendToVector(v.Child, elem)
		}
		v.Offsets[i] = int32(start)
		v.Offsets[i+1] = int32(v.Child.Len)
	case TypeRow:
		row, ok := val.(map[string]any)
		if !ok || v.Children == nil {
			return
		}
		for j, child := range v.Children {
			name := ""
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			child.SetValue(i, row[name])
		}
	}
}

// appendToVector appends a single value to a vector, growing its backing storage.
// Used by ARRAY/MAP SetValue to build up the child vector.
func appendToVector(v *Vector, val any) {
	idx := v.Len
	v.Len++
	v.Nulls = v.Nulls.Grow(v.Len)

	// Always grow typed storage (needed even for null to keep indices valid)
	switch v.Type {
	case TypeBool:
		v.BoolData = append(v.BoolData, false)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		v.Int32Data = append(v.Int32Data, 0)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		v.Int64Data = append(v.Int64Data, 0)
	case TypeFloat32:
		v.Float32Data = append(v.Float32Data, 0)
	case TypeFloat64:
		v.Float64Data = append(v.Float64Data, 0)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData.Offsets = append(v.BytesData.Offsets, v.BytesData.Offsets[len(v.BytesData.Offsets)-1])
	case TypeDecimal:
		v.DecimalData.Data = append(v.DecimalData.Data, Int128{})
	case TypeArray, TypeMap:
		v.Offsets = append(v.Offsets, v.Offsets[len(v.Offsets)-1])
	case TypeRow:
		for _, child := range v.Children {
			appendToVector(child, nil)
		}
	}

	if val == nil {
		v.Nulls.SetNull(idx)
		return
	}
	v.SetValue(idx, val)
}

// --- Typed accessors (zero-allocation hot path) ---

// GetInt64 returns the int64 value at position i. Returns (0, false) if null.
func (v *Vector) GetInt64(i int) (int64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Int64Data[i], true
}

// GetFloat64 returns the float64 value at position i. Returns (0, false) if null.
func (v *Vector) GetFloat64(i int) (float64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Float64Data[i], true
}

// GetInt32 returns the int32 value at position i. Returns (0, false) if null.
func (v *Vector) GetInt32(i int) (int32, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Int32Data[i], true
}

// GetFloat32 returns the float32 value at position i. Returns (0, false) if null.
func (v *Vector) GetFloat32(i int) (float32, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Float32Data[i], true
}

// GetBool returns the bool value at position i. Returns (false, false) if null.
func (v *Vector) GetBool(i int) (bool, bool) {
	if v.Nulls.IsNullFast(i) {
		return false, false
	}
	return v.BoolData[i], true
}

// GetString returns the string value at position i. Returns ("", false) if null.
func (v *Vector) GetString(i int) (string, bool) {
	if v.Nulls.IsNullFast(i) {
		return "", false
	}
	return v.BytesData.StringValue(i), true
}

// GetNumericFloat64 returns any numeric column value as float64 without boxing.
// Handles Int32, Int64, Float32, Float64, Timestamp types.
func (v *Vector) GetNumericFloat64(i int) (float64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	switch v.Type {
	case TypeInt64, TypeTimestamp:
		return float64(v.Int64Data[i]), true
	case TypeFloat64:
		return v.Float64Data[i], true
	case TypeInt32:
		return float64(v.Int32Data[i]), true
	case TypeFloat32:
		return float64(v.Float32Data[i]), true
	case TypeDecimal:
		return v.DecimalData.Data[i].ToFloat64(v.DecimalData.Scale), true
	default:
		return 0, false
	}
}

// memBytesMaxDepth bounds the recursion into nested type children so a
// pathological (or cyclic) schema cannot blow the stack while accounting.
const memBytesMaxDepth = 3

// MemBytes returns the heap bytes resident in this vector's backing storage:
// the null bitmap plus the typed data slice, recursing into nested children for
// ARRAY/MAP/ROW. It is the byte-true accounting primitive for the memory
// tracker, replacing the per-type b.Len*48 estimate in EstimateBatchBytes. It
// deliberately omits any operator-specific overhead (e.g. the HashJoin hash
// index charge) — that stays at the call site.
func (v *Vector) MemBytes() int64 {
	return v.memBytes(0)
}

func (v *Vector) memBytes(depth int) int64 {
	if v == nil || depth > memBytesMaxDepth {
		return 0
	}
	if v.Base != nil {
		// View: indices + own bitmap. Base storage is accounted by its
		// owner (build tracker / pinned probe input), never double-charged
		// here — the same rule that keeps shared broadcast-cache vectors
		// single-owner.
		return int64(len(v.Nulls.Words()))*8 + int64(len(v.Indices))*4
	}
	// Null bitmap: stored as []uint64 words, one bit per row.
	size := int64(len(v.Nulls.Words())) * 8 // 8 = sizeof(uint64) word

	switch v.Type {
	case TypeBool:
		size += int64(len(v.BoolData)) // 1 byte per bool
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		size += int64(len(v.Int32Data)) * 4
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		size += int64(len(v.Int64Data)) * 8 // IPv4/MAC are stored as int64
	case TypeFloat32:
		size += int64(len(v.Float32Data)) * 4
	case TypeFloat64:
		size += int64(len(v.Float64Data)) * 8
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		size += v.BytesData.MemBytes()
	case TypeDecimal:
		size += int64(len(v.DecimalData.Data)) * 16 // Int128 = {Hi int64, Lo uint64}
	case TypeVector:
		size += int64(len(v.Float32Data)) * 4 // Len*VectorDim float32s
	case TypeArray, TypeMap:
		size += int64(len(v.Offsets)) * 4 // int32 offsets
		size += v.Child.memBytes(depth + 1)
	case TypeRow:
		for _, c := range v.Children {
			size += c.memBytes(depth + 1)
		}
		for _, n := range v.FieldNames {
			size += int64(len(n)) + 16 // string header (ptr+len) + backing bytes
		}
	}
	return size
}

// String returns a debug representation of the vector.
func (v *Vector) String() string {
	return fmt.Sprintf("Vector{type=%v, len=%d, nulls=%d}", v.Type, v.Len, v.Nulls.NullCount())
}

// FormatValue formats any value for display, producing SQL-like text
// for nested types (arrays, rows, maps).
func FormatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch tv := v.(type) {
	case []float32:
		return formatFloat32Slice(tv)
	case []any:
		return formatArrayValue(tv)
	case map[string]any:
		return formatRowValue(tv)
	case string:
		return tv
	default:
		return fmt.Sprint(v)
	}
}

func formatFloat32Slice(v []float32) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%g", f))
	}
	buf.WriteByte(']')
	return buf.String()
}

func formatArrayValue(arr []any) string {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, elem := range arr {
		if i > 0 {
			buf.WriteString(", ")
		}
		if elem == nil {
			buf.WriteString("NULL")
		} else if s, ok := elem.(string); ok {
			buf.WriteByte('\'')
			buf.WriteString(s)
			buf.WriteByte('\'')
		} else {
			buf.WriteString(FormatValue(elem))
		}
	}
	buf.WriteByte(']')
	return buf.String()
}

func formatRowValue(row map[string]any) string {
	var buf strings.Builder
	buf.WriteByte('{')
	first := true
	for k, v := range row {
		if !first {
			buf.WriteString(", ")
		}
		first = false
		buf.WriteString(k)
		buf.WriteString(": ")
		if v == nil {
			buf.WriteString("NULL")
		} else if s, ok := v.(string); ok {
			buf.WriteByte('\'')
			buf.WriteString(s)
			buf.WriteByte('\'')
		} else {
			buf.WriteString(FormatValue(v))
		}
	}
	buf.WriteByte('}')
	return buf.String()
}

// formatUUID formats 16 raw bytes as a UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func formatUUID(b []byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
}

// parseUUID parses a UUID string into 16 raw bytes.
// Accepts "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" or "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".
func parseUUID(s string) []byte {
	// Remove dashes
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return nil
	}
	raw := make([]byte, 16)
	_, err := hex.Decode(raw, clean)
	if err != nil {
		return nil
	}
	return raw
}

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// FormatDate formats days-since-epoch as "2006-01-02".
func FormatDate(days int32) string {
	t := epochDate.AddDate(0, 0, int(days))
	return t.Format("2006-01-02")
}

// parseDateString parses "2006-01-02" to days since epoch.
func parseDateString(s string) int32 {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0
	}
	return int32(t.Sub(epochDate).Hours() / 24)
}

// --- Nested-aware typed copy primitives ---
//
// These three primitives are the kernel surface that lets Sort/Window
// columnar runs and the run merger carry ARRAY/MAP/ROW columns without
// boxing. They share one contract with BytesColumn: destinations are
// written SEQUENTIALLY (row 0, 1, 2, ...), and null rows still advance
// variable-length bookkeeping (array offsets, row children) — skipping a
// null would shift every later row's elements.

// NewVectorLike returns an empty (zero-row) vector with src's type and
// nested structure: child element types, ROW field names, VECTOR dim and
// DECIMAL scale. Element storage is appended by AppendFrom.
func NewVectorLike(src *Vector) *Vector {
	if src == nil {
		return nil
	}
	v := &Vector{Type: src.Type}
	switch src.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData = NewBytesColumn(0)
	case TypeDecimal:
		v.DecimalData = NewDecimalColumn(0, src.DecimalData.Scale)
	case TypeVector:
		v.VectorDim = src.VectorDim
	case TypeArray, TypeMap:
		v.Offsets = []int32{0}
		v.Child = NewVectorLike(src.Child)
	case TypeRow:
		v.Children = make([]*Vector, len(src.Children))
		for j, ch := range src.Children {
			v.Children[j] = NewVectorLike(ch)
		}
		v.FieldNames = append([]string(nil), src.FieldNames...)
	}
	return v
}

// AppendFrom appends src[si] to dst, growing dst by one row. Typed copy —
// no boxing, no string round-trips — recursive for nested types. dst's
// nested structure must match src's (build it with NewVectorLike).
func (v *Vector) AppendFrom(src *Vector, si int) {
	if src.Base != nil {
		// View source: mirror CopyValueFrom's redirect.
		if src.Nulls.IsNullFast(si) {
			appendToVector(v, nil)
			return
		}
		src, si = src.Base, int(src.Indices[si])
	}
	idx := v.Len
	v.Len++
	v.Nulls = v.Nulls.Grow(v.Len)
	isNull := src.Nulls.IsNull(si)
	if isNull {
		v.Nulls.SetNull(idx)
	}
	switch v.Type {
	case TypeBool:
		var x bool
		if !isNull {
			x = src.BoolData[si]
		}
		v.BoolData = append(v.BoolData, x)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		var x int32
		if !isNull {
			x = src.Int32Data[si]
		}
		v.Int32Data = append(v.Int32Data, x)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		var x int64
		if !isNull {
			x = src.Int64Data[si]
		}
		v.Int64Data = append(v.Int64Data, x)
	case TypeFloat32:
		var x float32
		if !isNull {
			x = src.Float32Data[si]
		}
		v.Float32Data = append(v.Float32Data, x)
	case TypeFloat64:
		var x float64
		if !isNull {
			x = src.Float64Data[si]
		}
		v.Float64Data = append(v.Float64Data, x)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		if isNull {
			v.BytesData.Offsets = append(v.BytesData.Offsets, uint32(len(v.BytesData.Data)))
		} else {
			start, end := src.BytesData.Offsets[si], src.BytesData.Offsets[si+1]
			v.BytesData.Data = append(v.BytesData.Data, src.BytesData.Data[start:end]...)
			v.BytesData.Offsets = append(v.BytesData.Offsets, uint32(len(v.BytesData.Data)))
		}
	case TypeDecimal:
		var x Int128
		if !isNull {
			x = src.DecimalData.Data[si]
		}
		v.DecimalData.Data = append(v.DecimalData.Data, x)
	case TypeVector:
		dim := v.VectorDim
		if dim > 0 {
			off := len(v.Float32Data)
			v.Float32Data = append(v.Float32Data, make([]float32, dim)...)
			if !isNull {
				copy(v.Float32Data[off:off+dim], src.Float32Data[si*dim:(si+1)*dim])
			}
		}
	case TypeArray, TypeMap:
		if v.Child == nil && src.Child != nil {
			v.Child = NewVectorLike(src.Child)
		}
		last := v.Offsets[len(v.Offsets)-1]
		if isNull || src.Child == nil {
			v.Offsets = append(v.Offsets, last)
		} else {
			start, end := src.Offsets[si], src.Offsets[si+1]
			for j := start; j < end; j++ {
				v.Child.AppendFrom(src.Child, int(j))
			}
			v.Offsets = append(v.Offsets, last+(end-start))
		}
	case TypeRow:
		if v.Children == nil && src.Children != nil {
			v.Children = make([]*Vector, len(src.Children))
			for j, ch := range src.Children {
				v.Children[j] = NewVectorLike(ch)
			}
			v.FieldNames = append([]string(nil), src.FieldNames...)
		}
		for j, child := range v.Children {
			if isNull || j >= len(src.Children) {
				appendToVector(child, nil)
			} else {
				child.AppendFrom(src.Children[j], si)
			}
		}
	}
}

// CopyValueFrom writes src[si] into position di of dst using typed access —
// no boxing, no string round-trips — for every column type including nested
// ARRAY/MAP/ROW. Fixed-width slots are indexed; variable-length storage
// (bytes data, array child elements, lazily-created row children) is
// appended, so writes must be SEQUENTIAL per column (di = 0, 1, 2, ...) —
// the same contract BytesColumn.Set has always had. Null source rows still
// advance offsets and children; skipping them would shift every later row.
//
// Destination shape is flexible per level: parent slots may be
// pre-allocated (NewRecordBatch with full nested schema) or append-built
// (NewVectorLike); ROW children handle both — indexed writes when
// pre-allocated, appends when built lazily.
func (v *Vector) CopyValueFrom(di int, src *Vector, si int) {
	if src.Base != nil {
		// View source: an own-null row becomes a null write (its index value
		// is meaningless); otherwise copy from the base row it references.
		if src.Nulls.IsNullFast(si) {
			v.WriteNullAt(di)
			return
		}
		src, si = src.Base, int(src.Indices[si])
	}
	isNull := src.Nulls.IsNull(si)
	if isNull {
		v.Nulls.SetNull(di)
	} else {
		v.Nulls.SetValid(di)
	}
	switch v.Type {
	case TypeBool:
		if !isNull {
			v.BoolData[di] = src.BoolData[si]
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		if !isNull {
			v.Int32Data[di] = src.Int32Data[si]
		}
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		if !isNull {
			v.Int64Data[di] = src.Int64Data[si]
		}
	case TypeFloat32:
		if !isNull {
			v.Float32Data[di] = src.Float32Data[si]
		}
	case TypeFloat64:
		if !isNull {
			v.Float64Data[di] = src.Float64Data[si]
		}
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		if isNull {
			v.BytesData.Set(di, nil)
		} else {
			v.BytesData.SetFrom(di, &src.BytesData, si)
		}
	case TypeDecimal:
		if !isNull {
			v.DecimalData.Data[di] = src.DecimalData.Data[si]
		}
	case TypeVector:
		dim := src.VectorDim
		if v.VectorDim == 0 {
			v.VectorDim = dim
		}
		if dim > 0 {
			need := (di + 1) * dim
			if len(v.Float32Data) < need {
				v.Float32Data = append(v.Float32Data, make([]float32, need-len(v.Float32Data))...)
			}
			if !isNull {
				copy(v.Float32Data[di*dim:(di+1)*dim], src.Float32Data[si*dim:(si+1)*dim])
			}
		}
	case TypeArray, TypeMap:
		if v.Child == nil && src.Child != nil {
			v.Child = NewVectorLike(src.Child)
		}
		var base int32
		if v.Child != nil {
			base = int32(v.Child.Len)
		}
		v.Offsets[di] = base
		if isNull || src.Child == nil {
			v.Offsets[di+1] = base
			return
		}
		start, end := src.Offsets[si], src.Offsets[si+1]
		for j := start; j < end; j++ {
			v.Child.AppendFrom(src.Child, int(j))
		}
		v.Offsets[di+1] = base + (end - start)
	case TypeRow:
		if v.Children == nil && src.Children != nil {
			v.Children = make([]*Vector, len(src.Children))
			for j, ch := range src.Children {
				v.Children[j] = NewVectorLike(ch)
			}
			v.FieldNames = append([]string(nil), src.FieldNames...)
		}
		for j, child := range v.Children {
			srcOK := !isNull && j < len(src.Children)
			if child.Len > di {
				// Pre-allocated parallel child (NewRecordBatch with nested
				// schema): indexed write.
				if srcOK {
					child.CopyValueFrom(di, src.Children[j], si)
				} else {
					child.WriteNullAt(di)
				}
			} else {
				// Append-built child (NewVectorLike): grow by one.
				if srcOK {
					child.AppendFrom(src.Children[j], si)
				} else {
					appendToVector(child, nil)
				}
			}
		}
	}
}

// WriteNullAt writes a null into position di of a pre-allocated vector,
// advancing variable-length bookkeeping (bytes offsets, array offsets, row
// children) so later sequential writes stay aligned. This is THE null-write
// primitive for indexed sequential writers — any writer that sets the null
// bit without advancing these slots corrupts every later row in the column.
func (v *Vector) WriteNullAt(di int) {
	v.Nulls.SetNull(di)
	switch v.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData.Set(di, nil)
	case TypeArray, TypeMap:
		var base int32
		if v.Child != nil {
			base = int32(v.Child.Len)
		}
		v.Offsets[di] = base
		v.Offsets[di+1] = base
	case TypeRow:
		for _, child := range v.Children {
			if child.Len > di {
				child.WriteNullAt(di)
			} else {
				appendToVector(child, nil)
			}
		}
	}
}
