package parquet

import (
	"encoding/binary"
	"testing"
)

// FuzzDecodePageValues drives a page BODY through every value decoder with a
// declared value count the body does not have to agree with.
//
// That disagreement is the whole point. A page header is Thrift the reader
// parsed; the body is however many bytes the chunk turned out to hold, and
// nothing in the format makes the two consistent. Every PLAIN decoder used
// to slice data[:n*width] before anything could check, so a header claiming
// 1,048,576 values over a 1000-value body was a slice-bounds panic — raised
// inside the scan errgroup, where in a worker process it is the worker.
//
// The property is: an error, never a panic. Plus, for a decode that
// SUCCEEDS, the values it hands back must actually number what it says they
// do — a short slice returned as a success is the silent-wrong-answer half
// of the same bug, since every unpack loop is bounded by len(src) and simply
// stops early.
func FuzzDecodePageValues(f *testing.F) {
	// The reviewer's inflated-num_values case: 1000 int64s, 2^20 declared.
	f.Add(uint8(2), uint8(0), 1<<20, 0, make([]byte, 1000*8))
	// The same body read honestly.
	f.Add(uint8(2), uint8(0), 1000, 0, make([]byte, 1000*8))
	// A BYTE_ARRAY value whose length prefix claims 2 GiB out of five bytes:
	// the pass-one bounds test only checked room for the PREFIX, so the
	// pass-two copy sliced past the end of the page.
	f.Add(uint8(6), uint8(0), 1, 0, []byte{0xFF, 0xFF, 0xFF, 0x7F, 'x'})
	// Truncated mid-length-prefix.
	f.Add(uint8(6), uint8(0), 2, 0, []byte{5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o', 3, 0})
	// FIXED_LEN_BYTE_ARRAY with a width the body cannot supply, and with a
	// degenerate width.
	f.Add(uint8(7), uint8(0), 4, 16, make([]byte, 8))
	f.Add(uint8(7), uint8(0), 4, 0, []byte{})
	// INT64 bytes decoded as the narrower physical, and the reverse: the
	// 1640089 review's physical-type mismatch, at the decoder.
	f.Add(uint8(1), uint8(0), 1000, 0, make([]byte, 1000*8))
	f.Add(uint8(2), uint8(0), 1000, 0, make([]byte, 1000*4))
	// BOOLEAN packs eight values per byte; a count past the packed bytes.
	f.Add(uint8(0), uint8(0), 1000, 0, []byte{0xFF})
	// Negative and zero counts.
	f.Add(uint8(2), uint8(0), -1, 0, make([]byte, 64))
	f.Add(uint8(2), uint8(0), 0, 0, []byte{})
	// DELTA_BINARY_PACKED whose header carries fewer values than the page
	// declares, and a truncated one.
	f.Add(uint8(2), uint8(2), 1000, 0, deltaHeader(128, 1, 4, 7))
	f.Add(uint8(1), uint8(2), 8, 0, []byte{0x80, 0x01, 0x04})
	// RLE / bit-packed bodies.
	f.Add(uint8(0), uint8(1), 1000, 0, []byte{0x02, 0x00})
	f.Add(uint8(1), uint8(1), 64, 0, []byte{0x03, 0x55, 0xAA})
	// DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY.
	f.Add(uint8(6), uint8(3), 4, 0, deltaHeader(128, 1, 4, 1))
	f.Add(uint8(6), uint8(4), 4, 0, deltaHeader(128, 1, 4, 1))

	f.Fuzz(func(t *testing.T, physRaw, encRaw uint8, n, typeLength int, body []byte) {
		// The count and width still come from a header, so negative values
		// stay in scope; only the sizes that would make the ALLOCATOR, not
		// the decoder, the thing under test are excluded.
		if n > 1<<20 || typeLength > 1<<12 || n < -8 || typeLength < -8 {
			return
		}
		phys := []PhysicalType{
			PhysicalBoolean, PhysicalInt32, PhysicalInt64, PhysicalInt96,
			PhysicalFloat, PhysicalDouble, PhysicalByteArray, PhysicalFixedLenByteArray,
		}[int(physRaw)%8]

		var (
			v   Values
			err error
		)
		switch int(encRaw) % 5 {
		case 0: // PLAIN
			switch phys {
			case PhysicalBoolean:
				v, err = DecodePlainBoolean(body, n)
			case PhysicalInt32:
				v, err = DecodePlainInt32(body, n)
			case PhysicalInt64:
				v, err = DecodePlainInt64(body, n)
			case PhysicalFloat:
				v, err = DecodePlainFloat(body, n)
			case PhysicalDouble:
				v, err = DecodePlainDouble(body, n)
			case PhysicalByteArray:
				v, err = DecodePlainByteArray(body, n)
			case PhysicalInt96:
				v, err = DecodePlainFixedLenByteArray(body, n, 12)
			default:
				v, err = DecodePlainFixedLenByteArray(body, n, typeLength)
			}
		case 1: // RLE / bit-packed — definition levels and dictionary indices
			if n < 0 || n > 100_000 {
				return
			}
			bitWidth := int(physRaw) % 33
			_, _ = DecodeRLEInt32(body, bitWidth, n)
			_, _, _ = DecodeRLEInt32WithLength(body, bitWidth, n)
			_ = DecodeBitPacked(body, bitWidth, n)
			return
		case 2: // DELTA_BINARY_PACKED
			if phys == PhysicalInt32 {
				v, err = DecodeDeltaBinaryPackedInt32(body, n)
			} else {
				v, err = DecodeDeltaBinaryPackedInt64(body, n)
			}
		case 3:
			v, err = DecodeDeltaLengthByteArray(body, n)
		default:
			v, err = DecodeDeltaByteArray(body, n)
		}
		if err != nil {
			return
		}

		// A successful decode must hand back as many values as it claims.
		checkValuesSelfConsistent(t, v)

		// And the accessors over it must stay inside their own bytes.
		for _, i := range []int{0, v.Count() - 1, v.Count(), -1, 1 << 20} {
			_, _, _, _ = v.Int32At(i), v.Int64At(i), v.FloatAt(i), v.DoubleAt(i)
		}
	})
}

func checkValuesSelfConsistent(t *testing.T, v Values) {
	t.Helper()
	if v.Count() <= 0 {
		return
	}
	switch v.PhysType() {
	case PhysicalInt32:
		if got := len(v.Int32()); got != v.Count() {
			t.Fatalf("INT32 page reports %d values but the accessor yields %d", v.Count(), got)
		}
	case PhysicalInt64:
		if got := len(v.Int64()); got != v.Count() {
			t.Fatalf("INT64 page reports %d values but the accessor yields %d", v.Count(), got)
		}
	case PhysicalFloat:
		if got := len(v.Float()); got != v.Count() {
			t.Fatalf("FLOAT page reports %d values but the accessor yields %d", v.Count(), got)
		}
	case PhysicalDouble:
		if got := len(v.Double()); got != v.Count() {
			t.Fatalf("DOUBLE page reports %d values but the accessor yields %d", v.Count(), got)
		}
	case PhysicalBoolean:
		if got := len(v.Boolean()) * 8; got < v.Count() {
			t.Fatalf("BOOLEAN page reports %d values but carries %d bits", v.Count(), got)
		}
	case PhysicalByteArray, PhysicalFixedLenByteArray:
		data, offs := v.ByteArray()
		if len(offs) != v.Count()+1 {
			t.Fatalf("BYTE_ARRAY page reports %d values but has %d offsets", v.Count(), len(offs))
		}
		for i := 0; i+1 < len(offs); i++ {
			if offs[i] > offs[i+1] || int(offs[i+1]) > len(data) {
				t.Fatalf("offset %d..%d runs outside %d bytes of value data",
					offs[i], offs[i+1], len(data))
			}
		}
	}
}

// deltaHeader builds a minimal DELTA_BINARY_PACKED header: block size,
// miniblock count, total values, first value. Enough to reach the block loop
// without hand-encoding varints in every seed.
func deltaHeader(blockSize, miniblocks, totalValues, firstValue int) []byte {
	var out []byte
	put := func(v uint64) {
		var buf [binary.MaxVarintLen64]byte
		out = append(out, buf[:binary.PutUvarint(buf[:], v)]...)
	}
	put(uint64(blockSize))
	put(uint64(miniblocks))
	put(uint64(totalValues))
	put(uint64((firstValue << 1) ^ (firstValue >> 63))) // zigzag
	return out
}
