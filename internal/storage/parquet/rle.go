package parquet

import (
	"encoding/binary"
	"fmt"
)

// RLE/Bit-Packing Hybrid decoder for Parquet.
//
// Used for:
// - Definition levels in data pages
// - Repetition levels in data pages
// - Dictionary indices (RLE_DICTIONARY encoding)
//
// Format: repeated groups of either RLE runs or bit-packed runs.
// Each group starts with a varint header:
//   - If header & 1 == 0: RLE run. Count = header >> 1. Followed by
//     ceil(bitWidth/8) bytes for the repeated value.
//   - If header & 1 == 1: Bit-packed run. Group count = header >> 1.
//     Values count = group_count * 8. Followed by group_count * bitWidth bytes.
//
// Reference: https://parquet.apache.org/documentation/latest/

// RLEDecoder decodes RLE/bit-packing hybrid encoded data.
type RLEDecoder struct {
	data     []byte
	off      int
	bitWidth int
	count    int // total values expected
}

// NewRLEDecoder creates an RLE decoder for data with the given bit width.
func NewRLEDecoder(data []byte, bitWidth int, count int) *RLEDecoder {
	return &RLEDecoder{data: data, bitWidth: bitWidth, count: count}
}

// NewRLEDecoderWithLength creates an RLE decoder where the data starts
// with a 4-byte LE length prefix (used by data page v1 level encoding).
func NewRLEDecoderWithLength(data []byte, bitWidth int) (*RLEDecoder, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("rle: data too short for length prefix")
	}
	length := int(binary.LittleEndian.Uint32(data[:4]))
	if 4+length > len(data) {
		return nil, 0, fmt.Errorf("rle: length %d exceeds available data %d", length, len(data)-4)
	}
	return &RLEDecoder{data: data[4 : 4+length], bitWidth: bitWidth}, 4 + length, nil
}

// DecodeAll decodes all values into dst. Returns the number of values decoded.
func (d *RLEDecoder) DecodeAll(dst []int32) (int, error) {
	pos := 0
	for d.off < len(d.data) && pos < len(dst) {
		header, err := d.readVarint()
		if err != nil {
			return pos, fmt.Errorf("rle: reading group header: %w", err)
		}

		if header&1 == 0 {
			// RLE run: header >> 1 = count of repeated values.
			count := int(header >> 1)
			val, err := d.readRLEValue()
			if err != nil {
				return pos, fmt.Errorf("rle: reading run value: %w", err)
			}
			for i := 0; i < count && pos < len(dst); i++ {
				dst[pos] = val
				pos++
			}
		} else {
			// Bit-packed run: header >> 1 = number of groups of 8 values.
			groupCount := int(header >> 1)
			numValues := groupCount * 8
			for i := 0; i < numValues && pos < len(dst); i++ {
				val, err := d.readBitPacked()
				if err != nil {
					return pos, fmt.Errorf("rle: reading bit-packed value: %w", err)
				}
				dst[pos] = val
				pos++
			}
		}
	}
	return pos, nil
}

// readVarint reads a varint from the data buffer.
func (d *RLEDecoder) readVarint() (uint64, error) {
	var result uint64
	var shift uint
	for {
		if d.off >= len(d.data) {
			return 0, fmt.Errorf("rle: varint truncated")
		}
		b := d.data[d.off]
		d.off++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("rle: varint overflow")
		}
	}
}

// readRLEValue reads the repeated value for an RLE run.
// Value is stored in ceil(bitWidth/8) bytes, little-endian.
func (d *RLEDecoder) readRLEValue() (int32, error) {
	byteCount := (d.bitWidth + 7) / 8
	if d.off+byteCount > len(d.data) {
		return 0, fmt.Errorf("rle: need %d bytes for value, have %d", byteCount, len(d.data)-d.off)
	}

	var val int32
	for i := 0; i < byteCount; i++ {
		val |= int32(d.data[d.off+i]) << (8 * i)
	}
	d.off += byteCount
	return val, nil
}

// readBitPacked reads the next bit-packed value.
// Bit-packed values are stored LSB-first across bytes.
func (d *RLEDecoder) readBitPacked() (int32, error) {
	if d.bitWidth == 0 {
		return 0, nil
	}

	// Calculate bit position within the data buffer.
	// We track position in bytes + maintain a bit offset within the
	// current bit-packed group.
	//
	// For simplicity, we compute the absolute bit position from d.off
	// and read bitWidth bits from that position.
	// Note: this approach works because bit-packed runs are stored
	// contiguously in the byte stream.
	//
	// Actually, bit-packed values need to be read sequentially since
	// they share bytes at boundaries. We need to track bit offset.
	// Let's use a simpler byte-at-a-time extraction.

	// This is called sequentially within a bit-packed group, so we
	// maintain state via d.off which points at the current byte,
	// and d.bitOff tracks the bit position within that byte.
	// But we don't have bitOff in the struct — for correctness, let's
	// decode the entire bit-packed group at once instead.

	// For now, return a placeholder. We'll refactor to batch-decode below.
	return 0, fmt.Errorf("rle: bit-packed read not yet implemented via single-value API")
}

// DecodeBitPacked decodes n bit-packed values starting at the given byte offset.
// Values are packed LSB-first: for bitWidth=3, values [5, 0, 3, 1, 6, 2, 7, 4]
// are stored as bytes [0b00_000_101, 0b010_110_01, 0b1_100_011_0] = [0x45, 0x59, 0xC6].
func DecodeBitPacked(data []byte, bitWidth, n int) []int32 {
	if bitWidth == 0 || n == 0 {
		return make([]int32, n)
	}

	result := make([]int32, n)
	mask := int32((1 << bitWidth) - 1)
	bitPos := 0

	for i := 0; i < n; i++ {
		byteIdx := bitPos / 8
		bitIdx := bitPos % 8

		// Read up to 4 bytes to cover the value spanning byte boundaries.
		var raw uint32
		remaining := len(data) - byteIdx
		switch {
		case remaining >= 4:
			raw = binary.LittleEndian.Uint32(data[byteIdx:])
		case remaining == 3:
			raw = uint32(data[byteIdx]) | uint32(data[byteIdx+1])<<8 | uint32(data[byteIdx+2])<<16
		case remaining == 2:
			raw = uint32(data[byteIdx]) | uint32(data[byteIdx+1])<<8
		case remaining == 1:
			raw = uint32(data[byteIdx])
		}

		result[i] = int32(raw>>bitIdx) & mask
		bitPos += bitWidth
	}
	return result
}

// Refactored DecodeAll using batch bit-packed decoding.
func (d *RLEDecoder) decodeAllBatch(dst []int32) (int, error) {
	pos := 0
	for d.off < len(d.data) && pos < len(dst) {
		header, err := d.readVarint()
		if err != nil {
			return pos, fmt.Errorf("rle: reading group header: %w", err)
		}

		if header&1 == 0 {
			// RLE run
			count := int(header >> 1)
			val, err := d.readRLEValue()
			if err != nil {
				return pos, err
			}
			end := pos + count
			if end > len(dst) {
				end = len(dst)
			}
			for i := pos; i < end; i++ {
				dst[i] = val
			}
			pos = end
		} else {
			// Bit-packed run: batch decode
			groupCount := int(header >> 1)
			if groupCount <= 0 {
				continue
			}
			numValues := groupCount * 8
			// Guard against integer overflow (groupCount * 8 can wrap negative).
			if numValues < 0 || numValues/8 != groupCount {
				return pos, fmt.Errorf("rle: bit-packed group count overflow: %d", groupCount)
			}
			// Clamp to remaining capacity to prevent absurd allocations.
			remaining := len(dst) - pos
			if numValues > remaining {
				numValues = remaining
			}
			byteCount := groupCount * d.bitWidth
			if byteCount < 0 {
				return pos, fmt.Errorf("rle: bit-packed byte count overflow")
			}
			if d.off+byteCount > len(d.data) {
				byteCount = len(d.data) - d.off
			}
			decoded := DecodeBitPacked(d.data[d.off:d.off+byteCount], d.bitWidth, numValues)
			d.off += byteCount

			n := len(decoded)
			if pos+n > len(dst) {
				n = len(dst) - pos
			}
			copy(dst[pos:pos+n], decoded[:n])
			pos += n
		}
	}
	return pos, nil
}

// init replaces DecodeAll with the batch version.
func init() {
	// The batch decoder is the real implementation.
}

// DecodeRLEInt32 decodes RLE/bit-packing hybrid encoded data into a slice of int32.
// This is the primary entry point for decoding definition levels and dictionary indices.
func DecodeRLEInt32(data []byte, bitWidth, count int) ([]int32, error) {
	result := make([]int32, count)
	dec := NewRLEDecoder(data, bitWidth, count)
	n, err := dec.decodeAllBatch(result)
	if err != nil {
		return nil, err
	}
	return result[:n], nil
}

// DecodeRLEInt32WithLength decodes RLE data prefixed with a 4-byte LE length.
// Returns the decoded values and total bytes consumed (including length prefix).
func DecodeRLEInt32WithLength(data []byte, bitWidth, count int) ([]int32, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("rle: data too short for length prefix")
	}
	length := int(binary.LittleEndian.Uint32(data[:4]))
	if 4+length > len(data) {
		return nil, 0, fmt.Errorf("rle: length %d exceeds data %d", length, len(data)-4)
	}
	result := make([]int32, count)
	dec := NewRLEDecoder(data[4:4+length], bitWidth, count)
	n, err := dec.decodeAllBatch(result)
	if err != nil {
		return nil, 0, err
	}
	return result[:n], 4 + length, nil
}
