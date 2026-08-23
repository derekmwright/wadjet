package batch

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Container-column payload codec.
//
// ARRAY, ROW, MAP and VECTOR are the four column shapes whose bytes are not
// a flat run of fixed-width or length-prefixed values: they carry element
// offsets, a child vector, ROW field order, or a per-column dimension, and
// none of that survives a schema that records only a name and a type byte.
// That is #397 — every distributed plan that had to move one of the four
// across a stage boundary failed at the WSHF encoder ("unsupported shuffle
// type: ARRAY"), while the single-process engine answered the same query.
//
// The layout is the exec columnar spill codec's nested framing
// (internal/engine/exec/join_spill.go writeNestedVector / readNestedVector)
// re-homed here, because the WSHF format has TWO independent decoders — the
// worker's (internal/worker/shuffle_format.go) and the coordinator's
// (internal/coordinator/shuffle_reader.go) — and a third hand-written copy
// of a recursive nested layout is exactly the shape that drifts. Both call
// this. batch is the layer that already owns the nested Vector shape
// (NewVectorLike, AppendFrom, CopyValueFrom), and it is below both callers,
// so there is no new dependency edge.
//
// The one deliberate difference from the spill codec is the BYTES leaf,
// which uses WSHF's null-skipping form (totalLen, concatenated data, n END
// offsets) instead of a straight copy of the offsets array: a null slot's
// offset pair can be a descending, malformed pair (see BytesColumn.Value),
// which a straight copy would carry into the reader.
//
// Row-level nulls of the COLUMN ITSELF are NOT in this payload — WSHF
// writes every column's null bitmap ahead of its data regardless of type.
// Nulls of a CHILD vector ARE, because nothing else records them. A NULL
// container and an EMPTY container therefore stay distinct: both encode
// Offsets[i] == Offsets[i+1], and only the null one carries the column
// bitmap bit.
const containerNilChild = 0xFF // type byte for a nil child (legal only at 0 elements)

// EncodeContainerColumn appends the payload for rows [0, n) of v to dst and
// returns the grown slice. v must be a canonical vector: no view
// indirection, storage exactly n rows wide, ARRAY/MAP offsets starting at 0.
// (*Vector).NewVectorLike + AppendFrom produces exactly that from any
// source, which is how the WSHF writer feeds this.
func EncodeContainerColumn(dst []byte, v *Vector, n int) ([]byte, error) {
	if v == nil {
		return dst, fmt.Errorf("batch: nil container column")
	}
	if v.Base != nil {
		return dst, fmt.Errorf("batch: container column %v is a view — flatten or gather before encoding", v.Type)
	}
	return encodeVectorBody(dst, v, n)
}

// DecodeContainerColumn reads a payload written by EncodeContainerColumn
// into v, which must already have v.Type set (the WSHF schema's type byte)
// but need not carry any nested structure: child types, ROW field names and
// the VECTOR dimension all ride in the payload. The payload must be
// consumed exactly — trailing bytes are a corruption, not slack.
func DecodeContainerColumn(payload []byte, v *Vector, n int) error {
	if v == nil {
		return fmt.Errorf("batch: nil container column")
	}
	r := &containerReader{data: payload}
	if err := decodeVectorBody(r, v, n); err != nil {
		return err
	}
	if r.pos != len(payload) {
		return fmt.Errorf("batch: container payload is %d bytes but the %v walk consumed %d",
			len(payload), v.Type, r.pos)
	}
	v.Len = n
	return nil
}

// IsContainerType reports whether t's WSHF payload is encoded by this codec
// rather than by a flat per-type arm.
func IsContainerType(t TypeID) bool {
	switch t {
	case TypeArray, TypeMap, TypeRow, TypeVector:
		return true
	}
	return false
}

// SyncContainerSchema copies the shape a container column's PAYLOAD just
// revealed back into the batch's schema — VECTOR dimension, ARRAY/MAP
// element type, ROW field list.
//
// The WSHF schema header records a name and a type byte and nothing else,
// so a decoded batch's schema says "VECTOR" with dimension 0 and "ROW" with
// no fields. The vectors themselves come out right (the payload carries the
// shape), but downstream operators size their OWN output from the schema of
// the batch they were handed: exec.Sort takes s.schema = b.Schema on its
// first Consume and then gathers into a vector built from it, which for a
// dimension-0 VECTOR is a nil Float32Data and a slice-bounds panic in
// gatherSortVector. That is #397's second face — the receiving operator is
// handed a shapeless container — and the payload is the authority to fix it
// with, since it is derived from the data rather than from a plan's guess.
//
// The batch gets its OWN schema slice when there is anything to patch: the
// decode-ahead reader decodes chunks CONCURRENTLY off one shared schema, so
// patching in place would be a data race even though every writer would
// store the same value.
func SyncContainerSchema(b *RecordBatch) {
	if b == nil {
		return
	}
	var patched []parquet.Column
	for ci := range b.Schema {
		if !IsContainerType(b.Schema[ci].Type) || ci >= len(b.Columns) {
			continue
		}
		if patched == nil {
			patched = append(patched, b.Schema...)
			b.Schema = patched
		}
		syncContainerShape(&patched[ci], b.Columns[ci])
	}
}

func syncContainerShape(col *parquet.Column, v *Vector) {
	if v == nil {
		return
	}
	switch v.Type {
	case TypeVector:
		col.Dimension = v.VectorDim
	case TypeArray, TypeMap:
		if v.Child == nil {
			return
		}
		elem := columnShapeOf(v.Child, "element")
		col.ElementType = &elem
	case TypeRow:
		if len(v.Children) == 0 {
			return
		}
		fields := make([]parquet.Column, len(v.Children))
		for j, ch := range v.Children {
			name := fmt.Sprintf("f%d", j)
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			fields[j] = columnShapeOf(ch, name)
		}
		col.Fields = fields
	}
}

// columnShapeOf describes a decoded vector as a schema column, recursively.
func columnShapeOf(v *Vector, name string) parquet.Column {
	col := parquet.Column{Name: name, Nullable: true}
	if v == nil {
		return col
	}
	col.Type = v.Type
	col.Scale = v.DecimalData.Scale
	syncContainerShape(&col, v)
	return col
}

// --- encode ---

func encodeVectorBody(dst []byte, v *Vector, n int) ([]byte, error) {
	switch v.Type {
	case TypeBool:
		if len(v.BoolData) < n {
			return dst, shortColumn(v.Type, len(v.BoolData), n)
		}
		for i := 0; i < n; i++ {
			var b byte
			if v.BoolData[i] {
				b = 1
			}
			dst = append(dst, b)
		}

	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		if len(v.Int32Data) < n {
			return dst, shortColumn(v.Type, len(v.Int32Data), n)
		}
		for i := 0; i < n; i++ {
			dst = binary.LittleEndian.AppendUint32(dst, uint32(v.Int32Data[i]))
		}

	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		if len(v.Int64Data) < n {
			return dst, shortColumn(v.Type, len(v.Int64Data), n)
		}
		for i := 0; i < n; i++ {
			dst = binary.LittleEndian.AppendUint64(dst, uint64(v.Int64Data[i]))
		}

	case TypeFloat32:
		if len(v.Float32Data) < n {
			return dst, shortColumn(v.Type, len(v.Float32Data), n)
		}
		for i := 0; i < n; i++ {
			dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(v.Float32Data[i]))
		}

	case TypeFloat64:
		if len(v.Float64Data) < n {
			return dst, shortColumn(v.Type, len(v.Float64Data), n)
		}
		for i := 0; i < n; i++ {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(v.Float64Data[i]))
		}

	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		// [totalLen u32] [concatenated data] [n end offsets u32].
		// Null rows contribute no bytes and repeat the running end, so a
		// malformed descending offset pair under a null never reaches the
		// wire (the same reason shuffleWriter.writeBytesData skips them).
		if len(v.BytesData.Offsets) < n+1 {
			return dst, shortColumn(v.Type, len(v.BytesData.Offsets)-1, n)
		}
		lenPos := len(dst)
		dst = binary.LittleEndian.AppendUint32(dst, 0)
		dataPos := len(dst)
		ends := make([]uint32, n)
		for i := 0; i < n; i++ {
			if !v.Nulls.IsNull(i) {
				dst = append(dst, v.BytesData.Value(i)...)
			}
			ends[i] = uint32(len(dst) - dataPos)
		}
		binary.LittleEndian.PutUint32(dst[lenPos:], uint32(len(dst)-dataPos))
		for _, e := range ends {
			dst = binary.LittleEndian.AppendUint32(dst, e)
		}

	case TypeDecimal:
		if len(v.DecimalData.Data) < n {
			return dst, shortColumn(v.Type, len(v.DecimalData.Data), n)
		}
		for i := 0; i < n; i++ {
			d := v.DecimalData.Data[i]
			dst = binary.LittleEndian.AppendUint64(dst, d.Lo)
			dst = binary.LittleEndian.AppendUint64(dst, uint64(d.Hi))
		}

	case TypeVector:
		// [dim u32] then n*dim float32s. dim is per-column metadata the
		// Vector carries outside the schema, so it must ride in the payload.
		dim := v.VectorDim
		if dim < 0 {
			return dst, fmt.Errorf("batch: VECTOR column has negative dimension %d", dim)
		}
		if len(v.Float32Data) < n*dim {
			return dst, shortColumn(v.Type, len(v.Float32Data), n*dim)
		}
		dst = binary.LittleEndian.AppendUint32(dst, uint32(dim))
		for i := 0; i < n*dim; i++ {
			dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(v.Float32Data[i]))
		}

	case TypeArray, TypeMap:
		// [n+1 offsets u32] [child vector]. MAP shares ARRAY's layout — its
		// child is a ROW(key, value) vector, so the recursion covers it.
		if len(v.Offsets) < n+1 {
			return dst, fmt.Errorf("batch: %v column has %d offsets, need %d", v.Type, len(v.Offsets), n+1)
		}
		if v.Offsets[0] != 0 {
			return dst, fmt.Errorf("batch: %v column offsets start at %d, not 0 — gather before encoding",
				v.Type, v.Offsets[0])
		}
		for i := 0; i <= n; i++ {
			dst = binary.LittleEndian.AppendUint32(dst, uint32(v.Offsets[i]))
		}
		childLen := int(v.Offsets[n])
		if v.Child == nil && childLen > 0 {
			return dst, fmt.Errorf("batch: %v column has %d child elements but no child vector", v.Type, childLen)
		}
		return appendChildVector(dst, v.Child, childLen)

	case TypeRow:
		// [numChildren u32] then per child [nameLen u16][name][child
		// vector]. Children are parallel arrays of the parent's length, so
		// field ORDER round-trips with the names.
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(v.Children)))
		for j, ch := range v.Children {
			var name string
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			dst = binary.LittleEndian.AppendUint16(dst, uint16(len(name)))
			dst = append(dst, name...)
			var err error
			if dst, err = appendChildVector(dst, ch, n); err != nil {
				return dst, err
			}
		}

	default:
		return dst, fmt.Errorf("batch: unsupported container element type %v", v.Type)
	}
	return dst, nil
}

// appendChildVector serializes a child vector: [typeID u8] [decimal scale
// u8?] [hasNulls u8] [null bitmap?] [body]. Mirrors the per-column framing
// of a top-level column minus the name and nullable flag, which exist only
// at the schema level.
func appendChildVector(dst []byte, child *Vector, m int) ([]byte, error) {
	if child == nil {
		if m > 0 {
			return dst, fmt.Errorf("batch: nil child vector with %d elements", m)
		}
		return append(dst, containerNilChild), nil
	}
	if child.Base != nil {
		return dst, fmt.Errorf("batch: child vector %v is a view — flatten before encoding", child.Type)
	}
	dst = append(dst, byte(child.Type))
	// DECIMAL children carry their scale for the same reason the top-level
	// WSHF header does: the body is raw scaled integers, and a reader
	// without the scale renders every value 10^scale too large.
	if child.Type == TypeDecimal {
		dst = append(dst, byte(child.DecimalData.Scale))
	}
	hasNulls := false
	for j := 0; j < m; j++ {
		if child.Nulls.IsNull(j) {
			hasNulls = true
			break
		}
	}
	if !hasNulls {
		dst = append(dst, 0)
		return encodeVectorBody(dst, child, m)
	}
	dst = append(dst, 1)
	bitmap := make([]byte, (m+7)/8)
	for j := 0; j < m; j++ {
		if child.Nulls.IsNull(j) {
			bitmap[j/8] |= 1 << (uint(j) % 8)
		}
	}
	dst = append(dst, bitmap...)
	return encodeVectorBody(dst, child, m)
}

func shortColumn(t TypeID, have, want int) error {
	return fmt.Errorf("batch: %v column holds %d values, need %d", t, have, want)
}

// --- decode ---

type containerReader struct {
	data []byte
	pos  int
}

func (r *containerReader) take(n int) ([]byte, error) {
	if n < 0 || len(r.data)-r.pos < n {
		return nil, fmt.Errorf("batch: container payload truncated: want %d bytes at offset %d of %d",
			n, r.pos, len(r.data))
	}
	out := r.data[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *containerReader) remaining() int { return len(r.data) - r.pos }

func (r *containerReader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *containerReader) u16() (int, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint16(b)), nil
}

func (r *containerReader) u32() (int, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return int(binary.LittleEndian.Uint32(b)), nil
}

func decodeVectorBody(r *containerReader, v *Vector, n int) error {
	switch v.Type {
	case TypeBool:
		raw, err := r.take(n)
		if err != nil {
			return err
		}
		v.BoolData = resizeCleared(v.BoolData, n)
		for i := 0; i < n; i++ {
			v.BoolData[i] = raw[i] == 1
		}

	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		raw, err := r.take(n * 4)
		if err != nil {
			return err
		}
		v.Int32Data = resizeCleared(v.Int32Data, n)
		for i := 0; i < n; i++ {
			v.Int32Data[i] = int32(binary.LittleEndian.Uint32(raw[i*4:]))
		}

	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		raw, err := r.take(n * 8)
		if err != nil {
			return err
		}
		v.Int64Data = resizeCleared(v.Int64Data, n)
		for i := 0; i < n; i++ {
			v.Int64Data[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
		}

	case TypeFloat32:
		raw, err := r.take(n * 4)
		if err != nil {
			return err
		}
		v.Float32Data = resizeCleared(v.Float32Data, n)
		for i := 0; i < n; i++ {
			v.Float32Data[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		}

	case TypeFloat64:
		raw, err := r.take(n * 8)
		if err != nil {
			return err
		}
		v.Float64Data = resizeCleared(v.Float64Data, n)
		for i := 0; i < n; i++ {
			v.Float64Data[i] = math.Float64frombits(binary.LittleEndian.Uint64(raw[i*8:]))
		}

	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		totalLen, err := r.u32()
		if err != nil {
			return err
		}
		data, err := r.take(totalLen)
		if err != nil {
			return err
		}
		rawOffs, err := r.take(n * 4)
		if err != nil {
			return err
		}
		v.BytesData.Data = append(v.BytesData.Data[:0], data...)
		if cap(v.BytesData.Offsets) < n+1 {
			v.BytesData.Offsets = make([]uint32, n+1)
		} else {
			v.BytesData.Offsets = v.BytesData.Offsets[:n+1]
		}
		v.BytesData.Offsets[0] = 0
		prev := uint32(0)
		for i := 0; i < n; i++ {
			end := binary.LittleEndian.Uint32(rawOffs[i*4:])
			if end < prev || int(end) > totalLen {
				return fmt.Errorf("batch: %v offsets not monotonic at %d (%d after %d, total %d)",
					v.Type, i, end, prev, totalLen)
			}
			v.BytesData.Offsets[i+1] = end
			prev = end
		}

	case TypeDecimal:
		raw, err := r.take(n * 16)
		if err != nil {
			return err
		}
		v.DecimalData.Data = resizeCleared(v.DecimalData.Data, n)
		for i := 0; i < n; i++ {
			v.DecimalData.Data[i] = Int128{
				Lo: binary.LittleEndian.Uint64(raw[i*16:]),
				Hi: int64(binary.LittleEndian.Uint64(raw[i*16+8:])),
			}
		}

	case TypeVector:
		dim, err := r.u32()
		if err != nil {
			return err
		}
		// take() before the allocation: a corrupt dimension must fail on the
		// bytes it does not have, not on a multi-gigabyte make().
		raw, err := r.take(n * dim * 4)
		if err != nil {
			return err
		}
		v.VectorDim = dim
		v.Float32Data = resizeCleared(v.Float32Data, n*dim)
		for i := 0; i < n*dim; i++ {
			v.Float32Data[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		}

	case TypeArray, TypeMap:
		rawOffs, err := r.take((n + 1) * 4)
		if err != nil {
			return err
		}
		if cap(v.Offsets) < n+1 {
			v.Offsets = make([]int32, n+1)
		} else {
			v.Offsets = v.Offsets[:n+1]
		}
		prev := int32(0)
		for i := 0; i <= n; i++ {
			off := int32(binary.LittleEndian.Uint32(rawOffs[i*4:]))
			if off < prev || (i == 0 && off != 0) {
				return fmt.Errorf("batch: %v offsets not monotonic at %d (%d after %d)", v.Type, i, off, prev)
			}
			v.Offsets[i] = off
			prev = off
		}
		childLen := int(v.Offsets[n])
		// Every element costs at least one byte in every leaf encoding, so
		// a child count larger than what is left is corruption — refuse
		// before sizing a vector from it.
		if childLen > r.remaining() {
			return fmt.Errorf("batch: %v claims %d child elements but only %d payload bytes remain",
				v.Type, childLen, r.remaining())
		}
		child, err := decodeChildVector(r, childLen)
		if err != nil {
			return err
		}
		v.Child = child

	case TypeRow:
		numChildren, err := r.u32()
		if err != nil {
			return err
		}
		// Each child costs at least its type byte, its hasNulls byte and a
		// 2-byte name length.
		if numChildren > r.remaining() {
			return fmt.Errorf("batch: ROW claims %d fields but only %d payload bytes remain",
				numChildren, r.remaining())
		}
		if numChildren == 0 {
			// Children stays NIL, not an empty slice: GetValue answers nil
			// for a nil Children and an empty map[string]any for an empty
			// one, so decoding a field-less ROW to []*Vector{} would turn
			// every NULL row into "map[]" on the far side of a shuffle
			// while the single-process engine answered NULL.
			v.Children, v.FieldNames = nil, nil
			break
		}
		v.Children = make([]*Vector, numChildren)
		v.FieldNames = make([]string, numChildren)
		for j := 0; j < numChildren; j++ {
			nameLen, err := r.u16()
			if err != nil {
				return err
			}
			nameBytes, err := r.take(nameLen)
			if err != nil {
				return err
			}
			v.FieldNames[j] = string(nameBytes)
			ch, err := decodeChildVector(r, n)
			if err != nil {
				return err
			}
			v.Children[j] = ch
		}

	default:
		return fmt.Errorf("batch: unsupported container element type %v", v.Type)
	}
	v.Len = n
	return nil
}

// decodeChildVector mirrors appendChildVector.
func decodeChildVector(r *containerReader, m int) (*Vector, error) {
	typeByte, err := r.u8()
	if err != nil {
		return nil, err
	}
	if typeByte == containerNilChild {
		if m > 0 {
			return nil, fmt.Errorf("batch: nil child marker with %d elements", m)
		}
		return nil, nil
	}
	typ := TypeID(typeByte)
	scale := 0
	if typ == TypeDecimal {
		sb, err := r.u8()
		if err != nil {
			return nil, err
		}
		scale = int(sb)
	}
	// Length 0: every leaf slice is sized by decodeVectorBody itself, and
	// the nested arms build their own children — pre-allocating m here
	// would size a vector from a length the payload has not yet backed.
	child := NewVectorWithScale(typ, 0, scale)
	child.Nulls = NewBitmap(m)
	child.Len = m
	hasNulls, err := r.u8()
	if err != nil {
		return nil, err
	}
	if hasNulls == 1 {
		bitmap, err := r.take((m + 7) / 8)
		if err != nil {
			return nil, err
		}
		for j := 0; j < m; j++ {
			if bitmap[j/8]&(1<<(uint(j)%8)) != 0 {
				child.Nulls.SetNull(j)
			}
		}
	} else if hasNulls != 0 {
		return nil, fmt.Errorf("batch: child hasNulls flag is %d, want 0 or 1", hasNulls)
	}
	if err := decodeVectorBody(r, child, m); err != nil {
		return nil, err
	}
	return child, nil
}
