package parquet

import (
	"fmt"
)

// DELTA_BINARY_PACKED encoding decoder.
//
// Used for INT32 and INT64 values. Encoding structure:
//   - Block size (varint, number of values per block, must be multiple of 128)
//   - Number of miniblocks per block (varint)
//   - Total value count (varint)
//   - First value (zigzag varint)
//   - For each block:
//     - Min delta (zigzag varint)
//     - Bit widths per miniblock (one byte each)
//     - Miniblock data (bit-packed deltas)
//
// Reference: https://parquet.apache.org/documentation/latest/

// DecodeDeltaBinaryPackedInt32 decodes DELTA_BINARY_PACKED data as int32 values.
func DecodeDeltaBinaryPackedInt32(data []byte, n int) (Values, error) {
	vals, err := decodeDeltaBinaryPacked(data, n)
	if err != nil {
		return Values{}, err
	}
	result := make([]int32, len(vals))
	for i, v := range vals {
		result[i] = int32(v)
	}
	return PlainInt32Values(result), nil
}

// DecodeDeltaBinaryPackedInt64 decodes DELTA_BINARY_PACKED data as int64 values.
func DecodeDeltaBinaryPackedInt64(data []byte, n int) (Values, error) {
	vals, err := decodeDeltaBinaryPacked(data, n)
	if err != nil {
		return Values{}, err
	}
	return PlainInt64Values(vals), nil
}

func decodeDeltaBinaryPacked(data []byte, n int) ([]int64, error) {
	if n == 0 {
		return nil, nil
	}

	off := 0
	readVarint := func() (uint64, error) {
		var result uint64
		var shift uint
		for {
			if off >= len(data) {
				return 0, fmt.Errorf("delta: varint truncated at %d", off)
			}
			b := data[off]
			off++
			result |= uint64(b&0x7F) << shift
			if b&0x80 == 0 {
				return result, nil
			}
			shift += 7
			if shift >= 64 {
				return 0, fmt.Errorf("delta: varint overflow")
			}
		}
	}

	readZigzag := func() (int64, error) {
		v, err := readVarint()
		if err != nil {
			return 0, err
		}
		return int64(v>>1) ^ -int64(v&1), nil
	}

	// Read header.
	blockSize, err := readVarint()
	if err != nil {
		return nil, fmt.Errorf("delta: reading block size: %w", err)
	}
	numMiniblocks, err := readVarint()
	if err != nil {
		return nil, fmt.Errorf("delta: reading miniblock count: %w", err)
	}
	totalValues, err := readVarint()
	if err != nil {
		return nil, fmt.Errorf("delta: reading total values: %w", err)
	}
	firstValue, err := readZigzag()
	if err != nil {
		return nil, fmt.Errorf("delta: reading first value: %w", err)
	}

	if totalValues == 0 {
		return nil, nil
	}

	count := int(totalValues)
	if n < count {
		count = n
	}

	result := make([]int64, 0, count)
	result = append(result, firstValue)
	if count == 1 {
		return result, nil
	}

	miniblockSize := int(blockSize) / int(numMiniblocks)
	if miniblockSize == 0 {
		miniblockSize = 1
	}
	remaining := count - 1

	for remaining > 0 {
		// Read block header: min delta + bit widths per miniblock.
		minDelta, err := readZigzag()
		if err != nil {
			return nil, fmt.Errorf("delta: reading min delta: %w", err)
		}

		bitWidths := make([]byte, numMiniblocks)
		if off+int(numMiniblocks) > len(data) {
			return nil, fmt.Errorf("delta: not enough data for bit widths")
		}
		copy(bitWidths, data[off:off+int(numMiniblocks)])
		off += int(numMiniblocks)

		for mb := 0; mb < int(numMiniblocks) && remaining > 0; mb++ {
			bw := int(bitWidths[mb])
			valsInMiniblock := miniblockSize
			if valsInMiniblock > remaining {
				valsInMiniblock = remaining
			}

			if bw == 0 {
				// All deltas are min_delta.
				for i := 0; i < valsInMiniblock; i++ {
					prev := result[len(result)-1]
					result = append(result, prev+minDelta)
				}
			} else {
				// Bit-packed deltas.
				bytesNeeded := (miniblockSize * bw + 7) / 8
				if off+bytesNeeded > len(data) {
					return nil, fmt.Errorf("delta: not enough data for miniblock")
				}
				deltas := DecodeBitPacked(data[off:off+bytesNeeded], bw, miniblockSize)
				off += bytesNeeded

				for i := 0; i < valsInMiniblock; i++ {
					prev := result[len(result)-1]
					result = append(result, prev+minDelta+int64(deltas[i]))
				}
			}
			remaining -= valsInMiniblock
		}
	}

	return result[:count], nil
}

// DELTA_LENGTH_BYTE_ARRAY encoding decoder.
//
// Structure:
//   - Lengths: DELTA_BINARY_PACKED encoded int32 values (one per byte array)
//   - Data: concatenated byte array values
//
// Reference: https://parquet.apache.org/documentation/latest/

// DecodeDeltaLengthByteArray decodes DELTA_LENGTH_BYTE_ARRAY data.
func DecodeDeltaLengthByteArray(data []byte, n int) (Values, error) {
	if n == 0 {
		return ByteArrayValues(nil, nil), nil
	}

	// Decode lengths using DELTA_BINARY_PACKED.
	lengths, err := decodeDeltaBinaryPacked(data, n)
	if err != nil {
		return Values{}, fmt.Errorf("delta_length_byte_array: decoding lengths: %w", err)
	}

	// Find where the length data ends by re-scanning the header.
	// We need to know the byte offset where the actual byte data starts.
	lengthDataEnd := findDeltaDataEnd(data, n)

	// Build offsets and packed data.
	offsets := make([]uint32, len(lengths)+1)
	var totalLen uint32
	for i, l := range lengths {
		offsets[i] = totalLen
		totalLen += uint32(l)
	}
	offsets[len(lengths)] = totalLen

	if lengthDataEnd+int(totalLen) > len(data) {
		return Values{}, fmt.Errorf("delta_length_byte_array: data truncated (need %d bytes at offset %d, have %d)",
			totalLen, lengthDataEnd, len(data)-lengthDataEnd)
	}

	packed := data[lengthDataEnd : lengthDataEnd+int(totalLen)]
	return ByteArrayValues(packed, offsets), nil
}

// findDeltaDataEnd scans the delta header to find the byte offset where
// the encoded length data ends (and the actual byte values begin).
func findDeltaDataEnd(data []byte, n int) int {
	off := 0
	readVarint := func() uint64 {
		var result uint64
		var shift uint
		for off < len(data) {
			b := data[off]
			off++
			result |= uint64(b&0x7F) << shift
			if b&0x80 == 0 {
				return result
			}
			shift += 7
		}
		return result
	}
	readZigzag := func() {
		readVarint()
	}

	blockSize := readVarint()
	numMiniblocks := readVarint()
	totalValues := readVarint()
	readZigzag() // first value

	remaining := int(totalValues) - 1
	miniblockSize := int(blockSize) / int(numMiniblocks)
	if miniblockSize == 0 {
		miniblockSize = 1
	}

	for remaining > 0 {
		readZigzag() // min delta

		bitWidths := make([]byte, numMiniblocks)
		if off+int(numMiniblocks) <= len(data) {
			copy(bitWidths, data[off:off+int(numMiniblocks)])
		}
		off += int(numMiniblocks)

		for mb := 0; mb < int(numMiniblocks) && remaining > 0; mb++ {
			bw := int(bitWidths[mb])
			valsInMiniblock := miniblockSize
			if valsInMiniblock > remaining {
				valsInMiniblock = remaining
			}
			if bw > 0 {
				bytesNeeded := (miniblockSize * bw + 7) / 8
				off += bytesNeeded
			}
			remaining -= valsInMiniblock
		}
	}

	return off
}

// DELTA_BYTE_ARRAY encoding decoder (incremental/prefix encoding).
//
// Structure:
//   - Prefix lengths: DELTA_BINARY_PACKED encoded int32 values
//   - Suffix lengths + data: DELTA_LENGTH_BYTE_ARRAY encoded suffixes
//
// Each value i = prefix(value[i-1], prefixLen[i]) + suffix[i]

// DecodeDeltaByteArray decodes DELTA_BYTE_ARRAY data.
func DecodeDeltaByteArray(data []byte, n int) (Values, error) {
	if n == 0 {
		return ByteArrayValues(nil, nil), nil
	}

	// Decode prefix lengths.
	prefixLengths, err := decodeDeltaBinaryPacked(data, n)
	if err != nil {
		return Values{}, fmt.Errorf("delta_byte_array: decoding prefix lengths: %w", err)
	}

	// Find where prefix length data ends.
	suffixStart := findDeltaDataEnd(data, n)

	// Decode suffixes.
	suffixValues, err := DecodeDeltaLengthByteArray(data[suffixStart:], n)
	if err != nil {
		return Values{}, fmt.Errorf("delta_byte_array: decoding suffixes: %w", err)
	}

	suffixData, suffixOffsets := suffixValues.ByteArray()

	// Reconstruct full values.
	var packed []byte
	offsets := make([]uint32, n+1)
	var prev []byte

	for i := 0; i < n; i++ {
		prefixLen := int(prefixLengths[i])
		suffix := suffixData[suffixOffsets[i]:suffixOffsets[i+1]]

		offsets[i] = uint32(len(packed))
		if prefixLen > 0 && prefixLen <= len(prev) {
			packed = append(packed, prev[:prefixLen]...)
		}
		packed = append(packed, suffix...)
		prev = packed[offsets[i]:]
	}
	offsets[n] = uint32(len(packed))

	return ByteArrayValues(packed, offsets), nil
}
