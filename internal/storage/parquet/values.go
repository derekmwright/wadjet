package parquet

import (
	"encoding/binary"
	"math"
	"unsafe"
)

// Values holds decoded Parquet page data as typed slices. This is our
// replacement for parquet-go's encoding.Values type, providing identical
// typed accessor methods without the external dependency.
//
// For fixed-width types (INT32, INT64, FLOAT, DOUBLE), the raw byte slice
// is reinterpreted as a typed slice via unsafe pointer casting — zero-copy,
// matching what parquet-go does internally via its unsafecast package.
//
// For variable-length types (BYTE_ARRAY), data holds the concatenated bytes
// and offsets holds the start positions (offsets[i]..offsets[i+1]).
type Values struct {
	physType PhysicalType
	data     []byte
	offsets  []uint32 // only for BYTE_ARRAY: len = numValues + 1
	count    int
}

// PlainInt32Values creates Values from a slice of int32.
func PlainInt32Values(vals []int32) Values {
	if len(vals) == 0 {
		return Values{physType: PhysicalInt32}
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), len(vals)*4)
	return Values{physType: PhysicalInt32, data: data, count: len(vals)}
}

// PlainInt64Values creates Values from a slice of int64.
func PlainInt64Values(vals []int64) Values {
	if len(vals) == 0 {
		return Values{physType: PhysicalInt64}
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), len(vals)*8)
	return Values{physType: PhysicalInt64, data: data, count: len(vals)}
}

// PlainFloat32Values creates Values from a slice of float32.
func PlainFloat32Values(vals []float32) Values {
	if len(vals) == 0 {
		return Values{physType: PhysicalFloat}
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), len(vals)*4)
	return Values{physType: PhysicalFloat, data: data, count: len(vals)}
}

// PlainFloat64Values creates Values from a slice of float64.
func PlainFloat64Values(vals []float64) Values {
	if len(vals) == 0 {
		return Values{physType: PhysicalDouble}
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(&vals[0])), len(vals)*8)
	return Values{physType: PhysicalDouble, data: data, count: len(vals)}
}

// ByteArrayValues creates Values from concatenated byte data and offsets.
func ByteArrayValues(data []byte, offsets []uint32) Values {
	n := 0
	if len(offsets) > 0 {
		n = len(offsets) - 1
	}
	return Values{physType: PhysicalByteArray, data: data, offsets: offsets, count: n}
}

// RawValues creates Values from raw bytes with a known type and count.
// Used for PLAIN-encoded fixed-width data where the bytes can be
// reinterpreted directly as typed slices.
func RawValues(typ PhysicalType, data []byte, count int) Values {
	return Values{physType: typ, data: data, count: count}
}

// Count returns the number of values.
func (v Values) Count() int { return v.count }

// PhysType returns the physical Parquet type of the values.
func (v Values) PhysType() PhysicalType { return v.physType }

// Int32 returns the data as a slice of int32.
// Valid only when physType is PhysicalInt32.
func (v Values) Int32() []int32 {
	if len(v.data) == 0 {
		return nil
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&v.data[0])), v.count)
}

// Int64 returns the data as a slice of int64.
// Valid only when physType is PhysicalInt64.
func (v Values) Int64() []int64 {
	if len(v.data) == 0 {
		return nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&v.data[0])), v.count)
}

// Float returns the data as a slice of float32.
// Valid only when physType is PhysicalFloat.
func (v Values) Float() []float32 {
	if len(v.data) == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&v.data[0])), v.count)
}

// Double returns the data as a slice of float64.
// Valid only when physType is PhysicalDouble.
func (v Values) Double() []float64 {
	if len(v.data) == 0 {
		return nil
	}
	return unsafe.Slice((*float64)(unsafe.Pointer(&v.data[0])), v.count)
}

// Boolean returns the data as packed boolean bytes.
// Each byte holds 8 boolean values, LSB first.
func (v Values) Boolean() []byte {
	return v.data
}

// ByteArray returns the concatenated byte data and per-value offsets.
// Value i is data[offsets[i]:offsets[i+1]].
func (v Values) ByteArray() ([]byte, []uint32) {
	return v.data, v.offsets
}

// --- PLAIN encoding decoders ---

// DecodePlainInt32 decodes PLAIN-encoded int32 values from raw bytes.
func DecodePlainInt32(data []byte, n int) Values {
	return RawValues(PhysicalInt32, data[:n*4], n)
}

// DecodePlainInt64 decodes PLAIN-encoded int64 values from raw bytes.
func DecodePlainInt64(data []byte, n int) Values {
	return RawValues(PhysicalInt64, data[:n*8], n)
}

// DecodePlainFloat decodes PLAIN-encoded float32 values from raw bytes.
func DecodePlainFloat(data []byte, n int) Values {
	return RawValues(PhysicalFloat, data[:n*4], n)
}

// DecodePlainDouble decodes PLAIN-encoded float64 values from raw bytes.
func DecodePlainDouble(data []byte, n int) Values {
	return RawValues(PhysicalDouble, data[:n*8], n)
}

// DecodePlainBoolean decodes PLAIN-encoded boolean values from raw bytes.
func DecodePlainBoolean(data []byte, n int) Values {
	byteCount := (n + 7) / 8
	return RawValues(PhysicalBoolean, data[:byteCount], n)
}

// DecodePlainByteArray decodes PLAIN-encoded BYTE_ARRAY values.
// Format: repeated [4-byte LE length][bytes...].
func DecodePlainByteArray(data []byte, n int) Values {
	offsets := make([]uint32, n+1)
	// First pass: compute total data size and build offset array.
	// The raw parquet format interleaves length prefixes with data, so we
	// need to strip the prefixes and pack data contiguously.
	totalDataSize := 0
	pos := 0
	for i := 0; i < n; i++ {
		if pos+4 > len(data) {
			// Truncated — return what we have
			offsets = offsets[:i+1]
			return ByteArrayValues(nil, offsets)
		}
		length := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		totalDataSize += length
		pos += length
	}

	// Second pass: copy contiguous data and set offsets.
	packed := make([]byte, totalDataSize)
	pos = 0
	dataPos := 0
	for i := 0; i < n; i++ {
		offsets[i] = uint32(dataPos)
		length := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		copy(packed[dataPos:], data[pos:pos+length])
		dataPos += length
		pos += length
	}
	offsets[n] = uint32(dataPos)
	return ByteArrayValues(packed, offsets)
}

// DecodePlainFixedLenByteArray decodes PLAIN-encoded FIXED_LEN_BYTE_ARRAY values.
func DecodePlainFixedLenByteArray(data []byte, n, typeLength int) Values {
	offsets := make([]uint32, n+1)
	for i := 0; i <= n; i++ {
		offsets[i] = uint32(i * typeLength)
	}
	return ByteArrayValues(data[:n*typeLength], offsets)
}

// --- Value extraction helpers for statistics ---

// Int32At returns the int32 at index i from the data.
func (v Values) Int32At(i int) int32 {
	off := i * 4
	return int32(binary.LittleEndian.Uint32(v.data[off:]))
}

// Int64At returns the int64 at index i from the data.
func (v Values) Int64At(i int) int64 {
	off := i * 8
	return int64(binary.LittleEndian.Uint64(v.data[off:]))
}

// FloatAt returns the float32 at index i.
func (v Values) FloatAt(i int) float32 {
	off := i * 4
	return math.Float32frombits(binary.LittleEndian.Uint32(v.data[off:]))
}

// DoubleAt returns the float64 at index i.
func (v Values) DoubleAt(i int) float64 {
	off := i * 8
	return math.Float64frombits(binary.LittleEndian.Uint64(v.data[off:]))
}
