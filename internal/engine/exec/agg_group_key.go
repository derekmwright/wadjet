// This file holds typed group-key serialization, hashing, and decoding.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"encoding/binary"
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// keySerCol resolves one group-key column's typed accessors once per
// batch, for the generic path's typed lookup loop: per-row hashing and
// key verification run straight off the typed slices instead of
// serializing every row's key (ClickBench Q19: serialization +
// string-hash-table probing was ~30% of the profile).
type keySerCol struct {
	kind  byte // 0=missing 1=i64-class 2=i32-class 3=f64 4=f32 5=bytes-class 6=bool
	i64   []int64
	i32   []int32
	f64   []float64
	f32   []float32
	bools []bool
	bytes *batch.BytesColumn
	nulls *batch.Bitmap
}

// genericKeyBoxingDeferrable reports whether a group column of type t may
// skip consume-time boxing on the generic SoA path. Named rather than inline
// so the ONE type set is shared by the three functions that must agree about
// it — buildKeySerCols' kinds, serializeGroupKey's payloads and
// decodeSerializedKey's re-boxing — and so a gate can drive the real
// predicate instead of a copy of it
// (TestEveryGroupKeyProducerWritesTheSameBytes).
//
// The set is exactly the types that both round-trip through the binary key
// encoding losslessly AND box to a primitive whose reconstruction is trivial
// (GetValue parity). The network types, DATE and UUID box as FORMATTED
// STRINGS; DECIMAL boxes as one too, and its storage is an Int128 plus a
// scale rather than a flat typed slice.
func genericKeyBoxingDeferrable(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration,
		batch.TypeInt32, batch.TypePort, batch.TypeProtocol,
		batch.TypeFloat64, batch.TypeFloat32, batch.TypeBool,
		batch.TypeString, batch.TypeBytes:
		return true
	}
	return false
}

// buildKeySerCols resolves the group-key columns of one batch. dst is
// reused across batches (scratch on the sink). Callers gate on
// deferGenericKeyBoxing, whose type set exactly matches the kinds here.
func buildKeySerCols(dst []keySerCol, b *batch.RecordBatch, colIdx []int, colTypes []batch.TypeID) []keySerCol {
	dst = dst[:0]
	for ci, idx := range colIdx {
		if idx < 0 {
			dst = append(dst, keySerCol{kind: 0})
			continue
		}
		v := b.Columns[idx]
		c := keySerCol{nulls: &v.Nulls}
		switch colTypes[ci] {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			c.kind, c.i64 = 1, v.Int64Data
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
			c.kind, c.i32 = 2, v.Int32Data
		case batch.TypeFloat64:
			c.kind, c.f64 = 3, v.Float64Data
		case batch.TypeFloat32:
			c.kind, c.f32 = 4, v.Float32Data
		case batch.TypeString, batch.TypeBytes:
			c.kind, c.bytes = 5, &v.BytesData
		case batch.TypeBool:
			c.kind, c.bools = 6, v.BoolData
		default:
			// Unreachable under the deferGenericKeyBoxing gate; treated as
			// always-null so a gate/kind drift fails loudly in tests
			// (groups collapse) rather than corrupting memory.
			c.kind = 0
		}
		dst = append(dst, c)
	}
	return dst
}

// serializeGroupKey writes one row's group key into buf, byte-identical
// to the appendColumnValue-based serializeKey loop (null flag byte, then
// the typed payload). Only called once per NEW group on the typed path.
func serializeGroupKey(buf []byte, cols []keySerCol, row int) []byte {
	for i := range cols {
		c := &cols[i]
		if c.kind == 0 || c.nulls.IsNullFast(row) {
			buf = append(buf, 1)
			continue
		}
		buf = append(buf, 0)
		switch c.kind {
		case 1:
			v := uint64(c.i64[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
				byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
		case 2:
			v := uint32(c.i32[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
		case 3:
			v := keyFloat64bits(c.f64[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
				byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
		case 4:
			v := keyFloat32bits(c.f32[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
		case 5:
			data := c.bytes.Value(row)
			l := uint16(len(data))
			buf = append(buf, byte(l), byte(l>>8))
			buf = append(buf, data...)
		case 6:
			if c.bools[row] {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}
	return buf
}

// typedRowHash combines per-column hashes of one row's group key. Values
// hash from their typed storage (no serialization); NULL and missing
// columns contribute a fixed marker so (NULL, x) and (x, NULL) differ.
func typedRowHash(cols []keySerCol, row int) uint64 {
	h := uint64(0x9e3779b97f4a7c15)
	for i := range cols {
		c := &cols[i]
		var ch uint64
		if c.kind == 0 || c.nulls.IsNullFast(row) {
			ch = 0xdeadbeefdeadbeef
		} else {
			switch c.kind {
			case 1:
				ch = mix64(uint64(c.i64[row]))
			case 2:
				ch = mix64(uint64(uint32(c.i32[row])))
			case 3:
				ch = mix64(keyFloat64bits(c.f64[row]))
			case 4:
				ch = mix64(uint64(keyFloat32bits(c.f32[row])))
			case 5:
				ch = strHash([]byte(c.bytes.UnsafeStringValue(row)))
			case 6:
				if c.bools[row] {
					ch = 0xb001b001b001b001
				} else {
					ch = 0x0b000b000b000b00
				}
			}
		}
		h = mix64(h ^ ch)
	}
	return h
}

// serializedKeyMatchesRow reports whether a stored binary group key equals
// the key formed by this row — the typed path's chain-verification,
// equivalent to serializing the row and comparing bytes, without the
// serialization.
func serializedKeyMatchesRow(key string, cols []keySerCol, row int) bool {
	for i := range cols {
		c := &cols[i]
		if len(key) == 0 {
			return false
		}
		flag := key[0]
		key = key[1:]
		isNull := c.kind == 0 || c.nulls.IsNullFast(row)
		if isNull != (flag == 1) {
			return false
		}
		if isNull {
			continue
		}
		switch c.kind {
		case 1:
			if len(key) < 8 || int64(binary.LittleEndian.Uint64([]byte(key[:8]))) != c.i64[row] {
				return false
			}
			key = key[8:]
		case 2:
			if len(key) < 4 || int32(binary.LittleEndian.Uint32([]byte(key[:4]))) != c.i32[row] {
				return false
			}
			key = key[4:]
		case 3:
			if len(key) < 8 || binary.LittleEndian.Uint64([]byte(key[:8])) != keyFloat64bits(c.f64[row]) {
				return false
			}
			key = key[8:]
		case 4:
			if len(key) < 4 || binary.LittleEndian.Uint32([]byte(key[:4])) != keyFloat32bits(c.f32[row]) {
				return false
			}
			key = key[4:]
		case 5:
			// Compare the row's own wrapped-uint16 prefix + full data —
			// exact byte-equality with the serialized form, correct even
			// for >64KB values where the stored prefix wraps.
			data := c.bytes.UnsafeStringValue(row)
			l := uint16(len(data))
			if len(key) < 2 || key[0] != byte(l) || key[1] != byte(l>>8) {
				return false
			}
			key = key[2:]
			if len(key) < len(data) || key[:len(data)] != data {
				return false
			}
			key = key[len(data):]
		case 6:
			want := byte(0)
			if c.bools[row] {
				want = 1
			}
			if key[0] != want {
				return false
			}
			key = key[1:]
		}
	}
	return len(key) == 0
}

// decodeSerializedKeyIntoColumns parses a group's binary serialized key
// ([null-flag][typed payload] per column — serializeKey/processRow's
// format) and writes each column's value into the typed output vector at
// row. The hot-output counterpart of decodeSerializedKey: no `any` boxes.
func decodeSerializedKeyIntoColumns(key string, types []batch.TypeID, cols []*batch.Vector, row int) {
	for j, t := range types {
		if len(key) == 0 {
			// WriteNullAt, not bare SetNull: Bytes-class vectors are
			// offset-append storage — a skipped write desyncs every later
			// row's offsets in that column.
			cols[j].WriteNullAt(row)
			continue
		}
		flag := key[0]
		key = key[1:]
		if flag == 1 {
			cols[j].WriteNullAt(row)
			continue
		}
		switch t {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			v := int64(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
			writeIntKeyToColumn(cols[j], row, v, t)
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
			v := int64(int32(binary.LittleEndian.Uint32([]byte(key[:4]))))
			key = key[4:]
			writeIntKeyToColumn(cols[j], row, v, t)
		case batch.TypeFloat64:
			cols[j].Float64Data[row] = math.Float64frombits(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
		case batch.TypeFloat32:
			cols[j].Float32Data[row] = math.Float32frombits(binary.LittleEndian.Uint32([]byte(key[:4])))
			key = key[4:]
		case batch.TypeBool:
			cols[j].BoolData[row] = key[0] == 1
			key = key[1:]
		case batch.TypeString, batch.TypeBytes:
			l := int(uint16(key[0]) | uint16(key[1])<<8)
			key = key[2:]
			cols[j].BytesData.Set(row, []byte(key[:l]))
			key = key[l:]
		default:
			// deferGenericKeyBoxing excludes every other type at resolve;
			// reaching here means the gate and the decoder disagree.
			cols[j].Nulls.SetNull(row)
		}
	}
}

// decodeSerializedKey is the boxed-value variant for cold paths (spill
// drain cursor): parses the binary key into a fresh []any matching what
// consume-time boxing would have produced.
func decodeSerializedKey(key string, types []batch.TypeID) []any {
	vals := make([]any, len(types))
	for j, t := range types {
		if len(key) == 0 {
			continue
		}
		flag := key[0]
		key = key[1:]
		if flag == 1 {
			continue // nil
		}
		// Box types mirror Vector.GetValue exactly (int32 stays int32,
		// float32 stays float32, TypeBytes gives []byte) so the spill
		// cursor's tag dispatch sees the same shapes as eager boxing.
		switch t {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			vals[j] = int64(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
			vals[j] = int32(binary.LittleEndian.Uint32([]byte(key[:4])))
			key = key[4:]
		case batch.TypeFloat64:
			vals[j] = math.Float64frombits(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
		case batch.TypeFloat32:
			vals[j] = math.Float32frombits(binary.LittleEndian.Uint32([]byte(key[:4])))
			key = key[4:]
		case batch.TypeBool:
			vals[j] = key[0] == 1
			key = key[1:]
		case batch.TypeString:
			l := int(uint16(key[0]) | uint16(key[1])<<8)
			key = key[2:]
			vals[j] = key[:l]
			key = key[l:]
		case batch.TypeBytes:
			l := int(uint16(key[0]) | uint16(key[1])<<8)
			key = key[2:]
			vals[j] = []byte(key[:l])
			key = key[l:]
		}
	}
	return vals
}

// appendIntKeyRowFormat encodes one int-fast-path group key column exactly
// as processRow / consumeBatchGenericSoA encode it from a live batch row:
// a 0x00 not-null flag followed by appendColumnValue's fixed-width little-
// endian bytes for the column type. Int-mode keys are never null (a null
// key migrates the aggregate to the generic path before consumption).
func appendIntKeyRowFormat(buf []byte, key int64, typ batch.TypeID) []byte {
	buf = append(buf, 0)
	switch typ {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		v := int32(key)
		return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	default:
		// Int64 and every type stored in Int64Data (timestamp, ipv4, mac,
		// duration) — 8 bytes, matching appendColumnValue's int64 case.
		return append(buf,
			byte(key), byte(key>>8), byte(key>>16), byte(key>>24),
			byte(key>>32), byte(key>>40), byte(key>>48), byte(key>>56))
	}
}
