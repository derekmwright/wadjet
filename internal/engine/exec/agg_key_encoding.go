// This file holds columnar and nested group-key byte encoding.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// serializeKey serializes group key values using the reusable buffer.
func serializeKey(buf []byte, vals []any) string {
	buf = buf[:0]
	for i, v := range vals {
		if i > 0 {
			buf = append(buf, 0)
		}
		buf = appendKeyValue(buf, v)
	}
	return string(buf)
}

// appendColumnValue appends a binary-encoded column value to buf for GROUP BY
// key construction. Uses fixed-width binary encoding for numeric types (no
// strconv text conversion), eliminating expensive int→decimal and float→string
// conversions in the hot path.
func appendColumnValue(buf []byte, v *batch.Vector, row int, typ batch.TypeID) []byte {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		val := v.Int64Data[row]
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		val := v.Int32Data[row]
		return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
	case batch.TypeFloat64:
		// keyFloat64bits, not Float64bits: every NaN payload folds onto one
		// NaN and -0.0 onto +0.0, because the comparator calls those pairs
		// EQUAL (kernel/float_order.go) and a key that split them would make
		// GROUP BY, DISTINCT and a hash join disagree with ORDER BY, with
		// `rank()`, and with the spilled merge key that already folds them
		// (#459).
		val := keyFloat64bits(v.Float64Data[row])
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeFloat32:
		val := keyFloat32bits(v.Float32Data[row])
		return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		data := v.BytesData.Value(row)
		l := uint16(len(data))
		buf = append(buf, byte(l), byte(l>>8))
		return append(buf, data...)
	case batch.TypeCIDR:
		// PostgreSQL's inet equality, not byte equality of the stored TEXT:
		// '10.0.0.1' and '10.0.0.1/32' are the SAME value there (#492), so
		// they must be the SAME key here too or GROUP BY, DISTINCT,
		// COUNT(DISTINCT) and a hash join all split one value into two
		// (#520) — kernel.CidrOrderKey is the canonical re-key every other
		// CIDR comparison site (filter, sort, MIN/MAX) already uses.
		key := kernel.CidrOrderKey(v.BytesData.UnsafeStringValue(row))
		l := uint16(len(key))
		buf = append(buf, byte(l), byte(l>>8))
		return append(buf, key...)
	case batch.TypeBool:
		if v.BoolData[row] {
			return append(buf, 1)
		}
		return append(buf, 0)
	case batch.TypeDecimal:
		// The value's canonical (unscaled, minimal scale) digits, exactly —
		// NOT its float64. A float64 holds ~16 significant digits and a
		// DECIMAL(38,10) holds 38, so keying on the double merged every pair
		// of values that agreed to 16 digits into one group / one distinct
		// value / a join match (#474). Scale-normalized so that 12.75 keys
		// alike whether the column declares scale 2 or scale 4, which is what
		// the comparator says and therefore what a cross-scale join needs.
		return batch.AppendDecimalKey(buf, v.DecimalData.Data[row], v.DecimalData.Scale)
	case batch.TypeVector:
		return appendVectorKey(buf, v, row)
	case batch.TypeArray, batch.TypeMap:
		return appendListKey(buf, v, row)
	case batch.TypeRow:
		return appendRowKey(buf, v, row)
	default:
		// Unreachable for a column type: every one of the 22 has an arm
		// above. Kept as a loud marker rather than a silent constant — the
		// constant '?' that used to live here collapsed every ARRAY, ROW, MAP
		// and VECTOR value into ONE key, so GROUP BY, DISTINCT, the set
		// operations, COUNT(DISTINCT), APPROX_DISTINCT and hash-join keys all
		// answered one group / one distinct value / a cross join.
		return append(buf, "\x00unsupported-key-type"...)
	}
}

// appendVectorKey encodes a VECTOR cell: its dimension, then every element's
// IEEE-754 bits. The dimension is part of the key because two vectors of
// different width are different values even when one is the other's prefix.
func appendVectorKey(buf []byte, v *batch.Vector, row int) []byte {
	dim := v.VectorDim
	buf = appendKeyUint32(buf, uint32(dim))
	if dim <= 0 {
		// A genuinely zero-width VECTOR column: every value IS the empty
		// vector, so one group is the right answer, not a lost one.
		return buf
	}
	base := row * dim
	if base+dim > len(v.Float32Data) {
		panic(malformedKeyColumn("VECTOR", "declares dimension %d but row %d needs %d floats and the column stores %d",
			dim, row, base+dim, len(v.Float32Data)))
	}
	for _, f := range v.Float32Data[base : base+dim] {
		// keyFloat32bits: the VECTOR element comparator folds NaN payloads
		// and -0.0 (kernel/float_order.go), and the boxed VECTOR key
		// (appendKeyFloats, sort.go) already folds them — this arm was the
		// last one keying raw IEEE bits.
		buf = appendKeyUint32(buf, keyFloat32bits(f))
	}
	return buf
}

// appendListKey encodes an ARRAY or MAP cell: the element count, then each
// element. MAP is stored as ARRAY(ROW(key,value)), so it rides the same code —
// and, like every other engine path over MAP, it treats entry ORDER as part of
// the value rather than canonicalising it.
//
// The length prefix is what makes the encoding injective: without it
// [["a"],["b"]] and [["a","b"]] would produce the same bytes.
func appendListKey(buf []byte, v *batch.Vector, row int) []byte {
	if row+1 >= len(v.Offsets) {
		panic(malformedKeyColumn(v.Type.String(), "row %d needs offsets[%d] but the column carries %d",
			row, row+1, len(v.Offsets)))
	}
	start, end := int(v.Offsets[row]), int(v.Offsets[row+1])
	if end < start {
		end = start
	}
	buf = appendKeyUint32(buf, uint32(end-start))
	if v.Child == nil {
		if end > start {
			panic(malformedKeyColumn(v.Type.String(), "row %d spans elements [%d,%d) but the column has no child vector",
				row, start, end))
		}
		return buf
	}
	for i := start; i < end; i++ {
		buf = appendNestedElem(buf, v.Child, i)
	}
	return buf
}

// malformedKeyColumn is what a container column whose STORAGE contradicts its
// own shape raises. The guards used to fall back to a constant key, which is
// the #408 defect wearing a different hat: a constant makes every row of the
// column one group, so GROUP BY, DISTINCT, the set operations and the hash
// joins all answer from a single key and the query returns a plausible wrong
// answer instead of failing. A FatalEvalPanic is the one way to be loud from
// here — appendColumnValue has no error return, and giving it one would put a
// check in the innermost key loop of every grouped query.
func malformedKeyColumn(typ, format string, args ...any) fatalEvalError {
	return fatalEvalError{sqlerr.New("XX000", "malformed %s group key column: "+format, append([]any{typ}, args...)...)}
}

// appendRowKey encodes a ROW cell: the field count, then each field at this
// row. Field NAMES are not encoded — every row of one column carries the same
// schema, so they add bytes and no discrimination.
func appendRowKey(buf []byte, v *batch.Vector, row int) []byte {
	buf = appendKeyUint32(buf, uint32(len(v.Children)))
	for _, ch := range v.Children {
		buf = appendNestedElem(buf, ch, row)
	}
	return buf
}

// appendNestedElem writes one nested element: a null flag, then the value.
// The flag is what keeps a NULL element distinct from a zero one, the same way
// the top-level key loops flag a NULL column value before calling
// appendColumnValue.
func appendNestedElem(buf []byte, child *batch.Vector, i int) []byte {
	if child == nil || i >= child.Len || child.Nulls.IsNullFast(i) {
		return append(buf, 1)
	}
	buf = append(buf, 0)
	return appendColumnValue(buf, child, i, child.Type)
}

func appendKeyUint32(buf []byte, val uint32) []byte {
	return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
}
