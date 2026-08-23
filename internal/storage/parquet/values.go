package parquet

import (
	"encoding/binary"
	"fmt"
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

// typedValues reinterprets v.data as a []T of width-byte elements — the
// zero-copy cast every fixed-width accessor is built on — and is the ONE
// place that decides whether the cast is safe to make.
//
// Two conditions are checked, and neither is theoretical:
//
//   - the physical type must be the one the caller asked for. The row
//     reader decodes a column as the type the CATALOG names, not the type
//     the file stores, so a catalog INT64 over a file INT32 asked for
//     v.count int64s from a buffer holding v.count int32s — an unsafe.Slice
//     twice as long as its backing array, i.e. adjacent heap read straight
//     into query results. retypeFromCatalog now rejects that pairing before
//     it gets here; this is the backstop at the unsafe site itself.
//   - the bytes must be able to back v.count elements. Every constructor in
//     this file establishes that (the DecodePlain* decoders now REFUSE a
//     page body shorter than its declared value count instead of slicing
//     into it), so this is a second backstop, not the enforcement point.
//
// A short buffer yields nil, not a truncated slice. Truncating would answer
// a read of N values with fewer, which every caller silently accepts as "the
// rest were absent" — the same silent-wrong-answer class the physical-type
// refusal exists to prevent. nil is refusal: the read sites check it and
// report an error naming the column.
func typedValues[T any](v Values, want PhysicalType, width int) []T {
	if v.physType != want || v.count <= 0 || width <= 0 {
		return nil
	}
	if v.count > len(v.data)/width {
		return nil
	}
	return unsafe.Slice((*T)(unsafe.Pointer(&v.data[0])), v.count)
}

// Int32 returns the data as a slice of int32, or nil when the values are not
// PhysicalInt32.
func (v Values) Int32() []int32 { return typedValues[int32](v, PhysicalInt32, 4) }

// Int64 returns the data as a slice of int64, or nil when the values are not
// PhysicalInt64.
func (v Values) Int64() []int64 { return typedValues[int64](v, PhysicalInt64, 8) }

// Float returns the data as a slice of float32, or nil when the values are
// not PhysicalFloat.
func (v Values) Float() []float32 { return typedValues[float32](v, PhysicalFloat, 4) }

// Double returns the data as a slice of float64, or nil when the values are
// not PhysicalDouble.
func (v Values) Double() []float64 { return typedValues[float64](v, PhysicalDouble, 8) }

// Boolean returns the data as packed boolean bytes, or nil when the values
// are not PhysicalBoolean. Each byte holds 8 boolean values, LSB first.
func (v Values) Boolean() []byte {
	if v.physType != PhysicalBoolean {
		return nil
	}
	return v.data
}

// ByteArray returns the concatenated byte data and per-value offsets, or
// (nil, nil) when the values are not one of the byte-carrying physicals.
// Value i is data[offsets[i]:offsets[i+1]].
//
// The physical-type check is the same backstop the typed accessors get from
// typedValues, and it was the one accessor that did not have it. An INT64
// page handed to ByteArray answered with its raw eight-byte-per-value buffer
// and a nil offsets slice — and the byte-array copy paths read a nil offsets
// slice as "PLAIN encoding, four-byte length prefix per value", so a column
// of integers decoded into a STRING vector as whatever the length prefixes
// happened to say, with err == nil. The row reader refused the same file
// (checkPageDecodable asks PhysicalReadableAs first), so one corrupt
// annotation produced two different answers depending on which read path the
// table's schema shape selected.
//
// INT96 is included because it decodes through the same offset table as a
// twelve-byte fixed-length value.
func (v Values) ByteArray() ([]byte, []uint32) {
	switch v.physType {
	case PhysicalByteArray, PhysicalFixedLenByteArray, PhysicalInt96:
		return v.data, v.offsets
	}
	return nil, nil
}

// --- PLAIN encoding decoders ---
//
// n is the page header's declared value count and data is the page BODY.
// Nothing in the format makes them agree: the header is Thrift the reader
// trusted enough to parse, the body is however many bytes the chunk turned
// out to hold. Every one of these decoders used to slice data[:n*width]
// straight away, so a header claiming 1,048,576 values over a 1000-value
// body killed the process with a slice-bounds panic inside the scan
// errgroup — in a worker, that is the worker. They refuse instead, and the
// refusal names both numbers.

// plainBody returns the first n width-byte elements of a PLAIN page body,
// or an error when the body cannot back that many.
func plainBody(data []byte, n, width int, pt PhysicalType) ([]byte, error) {
	if n < 0 || width <= 0 {
		return nil, fmt.Errorf("%s page: invalid value count %d (element width %d)", pt, n, width)
	}
	// Division, not n*width: the product overflows for a hostile count.
	if n > len(data)/width {
		return nil, fmt.Errorf("%s page declares %d values (%d bytes at %d bytes each) but the page body holds %d bytes",
			pt, n, n*width, width, len(data))
	}
	return data[: n*width : n*width], nil
}

// DecodePlainInt32 decodes PLAIN-encoded int32 values from raw bytes.
func DecodePlainInt32(data []byte, n int) (Values, error) {
	body, err := plainBody(data, n, 4, PhysicalInt32)
	if err != nil {
		return Values{}, err
	}
	return RawValues(PhysicalInt32, body, n), nil
}

// DecodePlainInt64 decodes PLAIN-encoded int64 values from raw bytes.
func DecodePlainInt64(data []byte, n int) (Values, error) {
	body, err := plainBody(data, n, 8, PhysicalInt64)
	if err != nil {
		return Values{}, err
	}
	return RawValues(PhysicalInt64, body, n), nil
}

// DecodePlainFloat decodes PLAIN-encoded float32 values from raw bytes.
func DecodePlainFloat(data []byte, n int) (Values, error) {
	body, err := plainBody(data, n, 4, PhysicalFloat)
	if err != nil {
		return Values{}, err
	}
	return RawValues(PhysicalFloat, body, n), nil
}

// DecodePlainDouble decodes PLAIN-encoded float64 values from raw bytes.
func DecodePlainDouble(data []byte, n int) (Values, error) {
	body, err := plainBody(data, n, 8, PhysicalDouble)
	if err != nil {
		return Values{}, err
	}
	return RawValues(PhysicalDouble, body, n), nil
}

// DecodePlainBoolean decodes PLAIN-encoded boolean values from raw bytes.
// Eight values per byte, LSB first.
func DecodePlainBoolean(data []byte, n int) (Values, error) {
	if n < 0 {
		return Values{}, fmt.Errorf("BOOLEAN page: invalid value count %d", n)
	}
	byteCount := (n + 7) / 8
	if byteCount > len(data) {
		return Values{}, fmt.Errorf("BOOLEAN page declares %d values (%d bytes) but the page body holds %d bytes",
			n, byteCount, len(data))
	}
	return RawValues(PhysicalBoolean, data[:byteCount:byteCount], n), nil
}

// DecodePlainByteArray decodes PLAIN-encoded BYTE_ARRAY values.
// Format: repeated [4-byte LE length][bytes...].
//
// The length prefix is untrusted: the first pass validates that each value
// is actually present before the second pass copies it. Without that, a
// final prefix claiming 2 GiB passed the pass-one bounds test (which only
// checked room for the PREFIX) and the pass-two copy sliced past the end of
// the page.
func DecodePlainByteArray(data []byte, n int) (Values, error) {
	if n < 0 {
		return Values{}, fmt.Errorf("BYTE_ARRAY page: invalid value count %d", n)
	}
	offsets := make([]uint32, n+1)
	// First pass: compute total data size and build offset array.
	// The raw parquet format interleaves length prefixes with data, so we
	// need to strip the prefixes and pack data contiguously.
	totalDataSize := 0
	pos := 0
	for i := 0; i < n; i++ {
		if pos+4 > len(data) {
			return Values{}, fmt.Errorf("BYTE_ARRAY page declares %d values but the page body ends after %d", n, i)
		}
		length := int(binary.LittleEndian.Uint32(data[pos:]))
		// pos only ever grows, so a length that overshoots the page is
		// caught either by the NEXT iteration's prefix check or by the one
		// after the loop. That keeps the payload bound at one comparison
		// per PAGE instead of one per value, in a loop tight enough for
		// the difference to show on a string-heavy scan.
		pos += 4 + length
		totalDataSize += length
	}
	if pos > len(data) {
		return Values{}, fmt.Errorf("BYTE_ARRAY page's %d values span %d bytes but the page body holds %d",
			n, pos, len(data))
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
	return ByteArrayValues(packed, offsets), nil
}

// DecodePlainFixedLenByteArray decodes PLAIN-encoded FIXED_LEN_BYTE_ARRAY values.
func DecodePlainFixedLenByteArray(data []byte, n, typeLength int) (Values, error) {
	if n < 0 || typeLength < 0 {
		return Values{}, fmt.Errorf("FIXED_LEN_BYTE_ARRAY page: invalid value count %d at width %d", n, typeLength)
	}
	if typeLength == 0 {
		// A zero-width leaf carries no bytes at all; n zero-length values.
		return ByteArrayValues(nil, make([]uint32, n+1)), nil
	}
	body, err := plainBody(data, n, typeLength, PhysicalFixedLenByteArray)
	if err != nil {
		return Values{}, err
	}
	offsets := make([]uint32, n+1)
	for i := 0; i <= n; i++ {
		offsets[i] = uint32(i * typeLength)
	}
	return ByteArrayValues(body, offsets), nil
}

// --- Value extraction helpers for statistics ---

// The At accessors read one element out of the raw bytes. They are used on
// the statistics path, where the index comes from a scan over a page whose
// declared value count may not match the bytes it actually carries, so each
// one bounds-checks before it reads and answers zero past the end.

// Int32At returns the int32 at index i from the data, or 0 if i is past the end.
func (v Values) Int32At(i int) int32 {
	off := i * 4
	if i < 0 || off < 0 || off+4 > len(v.data) {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(v.data[off:]))
}

// Int64At returns the int64 at index i from the data, or 0 if i is past the end.
func (v Values) Int64At(i int) int64 {
	off := i * 8
	if i < 0 || off < 0 || off+8 > len(v.data) {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(v.data[off:]))
}

// FloatAt returns the float32 at index i, or 0 if i is past the end.
func (v Values) FloatAt(i int) float32 {
	off := i * 4
	if i < 0 || off < 0 || off+4 > len(v.data) {
		return 0
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(v.data[off:]))
}

// DoubleAt returns the float64 at index i, or 0 if i is past the end.
func (v Values) DoubleAt(i int) float64 {
	off := i * 8
	if i < 0 || off < 0 || off+8 > len(v.data) {
		return 0
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(v.data[off:]))
}
