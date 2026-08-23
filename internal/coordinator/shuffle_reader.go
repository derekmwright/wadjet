package coordinator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/s2"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decompressShuffleData detects compressed shuffle data (magic "WSHC") and
// decompresses it back to raw WSHF format. Non-compressed data is returned as-is.
func decompressShuffleData(data []byte) ([]byte, error) {
	if len(data) >= 4 && data[0] == 'W' && data[1] == 'S' && data[2] == 'H' && data[3] == 'C' {
		r := s2.NewReader(bytes.NewReader(data[4:]))
		decoded, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("decompressing shuffle data: %w", err)
		}
		return decoded, nil
	}
	return data, nil
}

// readShuffleBatches reads the binary columnar shuffle format (WSHF) into RecordBatches.
// This is a copy of worker.shuffleReadBatches to avoid circular imports.
func readShuffleBatches(data []byte) ([]*batch.RecordBatch, error) {
	if len(data) < 10 || string(data[:4]) != "WSHF" {
		return nil, fmt.Errorf("invalid shuffle format")
	}

	pos := 4
	numChunks := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	numCols := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	schema := make([]parquet.Column, numCols)
	for i := 0; i < numCols; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		schema[i].Name = string(data[pos : pos+nameLen])
		pos += nameLen
		schema[i].Type = parquet.TypeID(data[pos])
		schema[i].Nullable = true
		pos++
		// Decimal columns carry scale+precision after the type byte (see
		// worker shuffleWriter.writeHeader) — without them the decoded
		// vector renders the raw scaled integer (fraction lost).
		if schema[i].Type == parquet.TypeDecimal {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("truncated decimal schema at column %d", i)
			}
			schema[i].Scale = int(data[pos])
			schema[i].Precision = int(data[pos+1])
			pos += 2
		}
	}

	batches := make([]*batch.RecordBatch, 0, numChunks)
	for chunk := uint32(0); chunk < numChunks; chunk++ {
		if pos+4 > len(data) {
			break
		}
		numRows := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if numRows == 0 {
			continue
		}

		b := batch.NewRecordBatch(schema, numRows)
		for ci := 0; ci < numCols; ci++ {
			var err error
			pos, err = readShuffleColumn(data, pos, b.Columns[ci], numRows, schema[ci].Type)
			if err != nil {
				return nil, fmt.Errorf("column %d chunk %d: %w", ci, chunk, err)
			}
		}
		batch.SyncContainerSchema(b)
		batches = append(batches, b)
	}
	return batches, nil
}

func readShuffleColumn(data []byte, pos int, vec *batch.Vector, numRows int, typ parquet.TypeID) (int, error) {
	bitmapWords := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	words := vec.Nulls.Words()
	for i := 0; i < bitmapWords && i < len(words); i++ {
		words[i] = binary.LittleEndian.Uint64(data[pos:])
		pos += 8
	}
	for i := len(words); i < bitmapWords; i++ {
		pos += 8
	}
	vec.Nulls.InvalidateCache()

	switch typ {
	case parquet.TypeBool:
		packedLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		for i := 0; i < numRows; i++ {
			vec.BoolData[i] = (data[pos+i/8]>>(uint(i)%8))&1 == 1
		}
		pos += packedLen
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		pos += 4 // skip dataLen
		for i := 0; i < numRows; i++ {
			vec.Int32Data[i] = int32(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
		}
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		pos += 4
		for i := 0; i < numRows; i++ {
			vec.Int64Data[i] = int64(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}
	case parquet.TypeFloat32:
		pos += 4
		for i := 0; i < numRows; i++ {
			vec.Float32Data[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
		}
	case parquet.TypeFloat64:
		pos += 4
		for i := 0; i < numRows; i++ {
			vec.Float64Data[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		totalDataLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		allData := data[pos : pos+totalDataLen]
		pos += totalDataLen
		offsets := make([]uint32, numRows)
		for i := 0; i < numRows; i++ {
			offsets[i] = binary.LittleEndian.Uint32(data[pos:])
			pos += 4
		}
		var prevOff uint32
		for i := 0; i < numRows; i++ {
			end := offsets[i]
			vec.BytesData.Set(i, allData[prevOff:end])
			prevOff = end
		}
	case parquet.TypeDecimal:
		pos += 4
		if vec.DecimalData.Data != nil {
			for i := 0; i < numRows; i++ {
				vec.DecimalData.Data[i].Lo = binary.LittleEndian.Uint64(data[pos:])
				pos += 8
				vec.DecimalData.Data[i].Hi = int64(binary.LittleEndian.Uint64(data[pos:]))
				pos += 8
			}
		} else {
			pos += numRows * 16
		}
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
		// [payloadLen u32][payload]. The payload codec is shared with the
		// worker's writer and reader (batch.EncodeContainerColumn /
		// DecodeContainerColumn) — this decoder is an independent copy of
		// the WSHF walk, and a hand-written second copy of a RECURSIVE
		// nested layout is what #397 must not leave behind.
		if pos+4 > len(data) {
			return pos, fmt.Errorf("truncated container length prefix at offset %d of %d", pos, len(data))
		}
		payloadLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if payloadLen < 0 || pos+payloadLen > len(data) {
			return pos, fmt.Errorf("container payload of %d bytes at offset %d exceeds the %d-byte chunk",
				payloadLen, pos, len(data))
		}
		if err := batch.DecodeContainerColumn(data[pos:pos+payloadLen], vec, numRows); err != nil {
			return pos, err
		}
		pos += payloadLen
	default:
		return pos, fmt.Errorf("unsupported type: %v", typ)
	}
	return pos, nil
}
