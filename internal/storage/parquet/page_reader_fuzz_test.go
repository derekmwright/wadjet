package parquet

import (
	"testing"
)

// FuzzRLEDecode tests RLE/bit-packing hybrid decoding with arbitrary input.
// RLE-encoded data appears in definition levels, repetition levels, and
// dictionary indices — all from potentially untrusted Parquet files.
func FuzzRLEDecode(f *testing.F) {
	// Seed: single RLE run of value 0, count 1, bitWidth 1.
	f.Add([]byte{0x02, 0x00}, 1, 1)

	// Seed: bit-packed group.
	f.Add([]byte{0x03, 0x55}, 1, 4)

	// Seed: empty.
	f.Add([]byte{}, 0, 0)

	// Seed: bitWidth 0.
	f.Add([]byte{0x02, 0x00}, 0, 1)

	f.Fuzz(func(t *testing.T, data []byte, bitWidth int, numValues int) {
		if bitWidth < 0 || bitWidth > 32 || numValues < 0 || numValues > 10000 {
			return
		}
		// Must not panic.
		_, _ = DecodeRLEInt32(data, bitWidth, numValues)
	})
}

// FuzzRLEDecodeWithLength tests the length-prefixed RLE variant.
func FuzzRLEDecodeWithLength(f *testing.F) {
	// Seed: length-prefixed RLE data.
	f.Add([]byte{0x02, 0x00, 0x02, 0x00}, 1, 1)
	f.Add([]byte{}, 0, 0)

	f.Fuzz(func(t *testing.T, data []byte, bitWidth int, numValues int) {
		if bitWidth < 0 || bitWidth > 32 || numValues < 0 || numValues > 10000 {
			return
		}
		_, _, _ = DecodeRLEInt32WithLength(data, bitWidth, numValues)
	})
}

// FuzzDecodeBitPacked tests the bit-packing decoder with arbitrary input.
func FuzzDecodeBitPacked(f *testing.F) {
	f.Add([]byte{0x55, 0xAA}, 1, 8)
	f.Add([]byte{}, 0, 0)

	f.Fuzz(func(t *testing.T, data []byte, bitWidth int, n int) {
		if bitWidth < 0 || bitWidth > 32 || n < 0 || n > 10000 {
			return
		}
		_ = DecodeBitPacked(data, bitWidth, n)
	})
}

// FuzzDeltaBinaryPacked tests delta binary packed decoding with arbitrary input.
func FuzzDeltaBinaryPacked(f *testing.F) {
	f.Add([]byte{0x80, 0x01, 0x04, 0x00, 0x00}, 1)
	f.Add([]byte{}, 0)

	f.Fuzz(func(t *testing.T, data []byte, n int) {
		if n < 0 || n > 10000 {
			return
		}
		_, _ = DecodeDeltaBinaryPackedInt32(data, n)
		_, _ = DecodeDeltaBinaryPackedInt64(data, n)
	})
}

// FuzzDeltaLengthByteArray tests delta length byte array decoding.
func FuzzDeltaLengthByteArray(f *testing.F) {
	f.Add([]byte{0x80, 0x01, 0x04, 0x00, 0x00}, 1)
	f.Add([]byte{}, 0)

	f.Fuzz(func(t *testing.T, data []byte, n int) {
		if n < 0 || n > 10000 {
			return
		}
		_, _ = DecodeDeltaLengthByteArray(data, n)
	})
}

// FuzzDeltaByteArray tests delta byte array decoding.
func FuzzDeltaByteArray(f *testing.F) {
	f.Add([]byte{0x80, 0x01, 0x04, 0x00, 0x00}, 1)
	f.Add([]byte{}, 0)

	f.Fuzz(func(t *testing.T, data []byte, n int) {
		if n < 0 || n > 10000 {
			return
		}
		_, _ = DecodeDeltaByteArray(data, n)
	})
}
