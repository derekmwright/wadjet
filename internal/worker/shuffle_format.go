package worker

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Columnar binary shuffle format — replaces Parquet for inter-stage shuffle data.
// Avoids per-row goparquet.Value allocation (nRows * nCols objects), alphabetical
// column reordering, and Parquet page/RLE encoding overhead.
//
// Format:
//   Magic "WSHF" (4 bytes)
//   NumChunks uint32 (4 bytes)
//   NumCols uint16 (2 bytes)
//   Schema: for each column:
//     NameLen uint16
//     Name []byte
//     TypeID uint8
//   Chunks: for each chunk:
//     NumRows uint32 (4 bytes)
//     For each column:
//       NullBitmapWords uint32 (number of uint64 words)
//       NullBitmap []uint64
//       DataLen uint32 (byte length of column data)
//       Data []byte (type-dependent raw data)

var shuffleMagic = [4]byte{'W', 'S', 'H', 'F'}

// shuffleWriter writes RecordBatch chunks in columnar binary format.
type shuffleWriter struct {
	w         io.Writer
	schema    []parquet.Column
	numChunks uint32
	buf       []byte // reusable scratch buffer
}

func newShuffleWriter(w io.Writer, schema []parquet.Column) *shuffleWriter {
	return &shuffleWriter{
		w:      w,
		schema: schema,
		buf:    make([]byte, 8),
	}
}

// writeHeader writes the file header (magic + placeholder chunk count + schema).
func (sw *shuffleWriter) writeHeader() error {
	// Magic
	if _, err := sw.w.Write(shuffleMagic[:]); err != nil {
		return err
	}
	// NumChunks placeholder (patched by Close if w is seekable, otherwise caller tracks)
	binary.LittleEndian.PutUint32(sw.buf[:4], 0)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	// NumCols
	binary.LittleEndian.PutUint16(sw.buf[:2], uint16(len(sw.schema)))
	if _, err := sw.w.Write(sw.buf[:2]); err != nil {
		return err
	}
	// Schema
	for _, col := range sw.schema {
		name := []byte(col.Name)
		binary.LittleEndian.PutUint16(sw.buf[:2], uint16(len(name)))
		if _, err := sw.w.Write(sw.buf[:2]); err != nil {
			return err
		}
		if _, err := sw.w.Write(name); err != nil {
			return err
		}
		sw.buf[0] = uint8(col.Type)
		if _, err := sw.w.Write(sw.buf[:1]); err != nil {
			return err
		}
	}
	return nil
}

// writeChunk writes a batch of selected rows as a columnar chunk.
// sel contains the row indices to write; if nil, writes all rows.
func (sw *shuffleWriter) writeChunk(cols []*batch.Vector, sel []uint32, numRows int) error {
	sw.numChunks++

	// NumRows
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(numRows))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}

	for ci := range sw.schema {
		vec := cols[ci]
		if err := sw.writeColumnData(vec, sel, numRows, sw.schema[ci].Type); err != nil {
			return fmt.Errorf("writing column %d (%s): %w", ci, sw.schema[ci].Name, err)
		}
	}
	return nil
}

func (sw *shuffleWriter) writeColumnData(vec *batch.Vector, sel []uint32, numRows int, typ parquet.TypeID) error {
	// Write null bitmap (gathered from selected rows)
	bitmapWords := (numRows + 63) / 64
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(bitmapWords))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}

	// Gather null bitmap for selected rows.
	// Wadjet convention: bit=1 means valid (not null), bit=0 means null.
	bitmapBuf := make([]uint64, bitmapWords)
	if sel != nil {
		for i, si := range sel {
			if !vec.Nulls.IsNull(int(si)) {
				bitmapBuf[i/64] |= 1 << (uint(i) % 64)
			}
		}
	} else {
		// Copy bitmap directly when no selection
		words := vec.Nulls.Words()
		copy(bitmapBuf, words[:min(len(words), bitmapWords)])
	}
	for _, word := range bitmapBuf {
		binary.LittleEndian.PutUint64(sw.buf[:8], word)
		if _, err := sw.w.Write(sw.buf[:8]); err != nil {
			return err
		}
	}

	// Write column data based on type
	switch typ {
	case parquet.TypeBool:
		return sw.writeBoolData(vec.BoolData, sel, numRows)
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return sw.writeInt32Data(vec.Int32Data, sel, numRows)
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return sw.writeInt64Data(vec.Int64Data, sel, numRows)
	case parquet.TypeFloat32:
		return sw.writeFloat32Data(vec.Float32Data, sel, numRows)
	case parquet.TypeFloat64:
		return sw.writeFloat64Data(vec.Float64Data, sel, numRows)
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		return sw.writeBytesData(&vec.BytesData, sel, numRows)
	case parquet.TypeDecimal:
		return sw.writeDecimalData(vec, sel, numRows)
	default:
		return fmt.Errorf("unsupported shuffle type: %v", typ)
	}
}

func (sw *shuffleWriter) writeInt32Data(data []int32, sel []uint32, numRows int) error {
	dataLen := uint32(numRows * 4)
	binary.LittleEndian.PutUint32(sw.buf[:4], dataLen)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		for _, si := range sel {
			binary.LittleEndian.PutUint32(sw.buf[:4], uint32(data[si]))
			if _, err := sw.w.Write(sw.buf[:4]); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			binary.LittleEndian.PutUint32(sw.buf[:4], uint32(data[i]))
			if _, err := sw.w.Write(sw.buf[:4]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *shuffleWriter) writeInt64Data(data []int64, sel []uint32, numRows int) error {
	dataLen := uint32(numRows * 8)
	binary.LittleEndian.PutUint32(sw.buf[:4], dataLen)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		for _, si := range sel {
			binary.LittleEndian.PutUint64(sw.buf[:8], uint64(data[si]))
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			binary.LittleEndian.PutUint64(sw.buf[:8], uint64(data[i]))
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *shuffleWriter) writeFloat32Data(data []float32, sel []uint32, numRows int) error {
	dataLen := uint32(numRows * 4)
	binary.LittleEndian.PutUint32(sw.buf[:4], dataLen)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		for _, si := range sel {
			binary.LittleEndian.PutUint32(sw.buf[:4], math.Float32bits(data[si]))
			if _, err := sw.w.Write(sw.buf[:4]); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			binary.LittleEndian.PutUint32(sw.buf[:4], math.Float32bits(data[i]))
			if _, err := sw.w.Write(sw.buf[:4]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *shuffleWriter) writeFloat64Data(data []float64, sel []uint32, numRows int) error {
	dataLen := uint32(numRows * 8)
	binary.LittleEndian.PutUint32(sw.buf[:4], dataLen)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		for _, si := range sel {
			binary.LittleEndian.PutUint64(sw.buf[:8], math.Float64bits(data[si]))
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			binary.LittleEndian.PutUint64(sw.buf[:8], math.Float64bits(data[i]))
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *shuffleWriter) writeBoolData(data []bool, sel []uint32, numRows int) error {
	// Pack bools into bytes (8 bools per byte)
	packedLen := (numRows + 7) / 8
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(packedLen))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	packed := make([]byte, packedLen)
	if sel != nil {
		for i, si := range sel {
			if data[si] {
				packed[i/8] |= 1 << (uint(i) % 8)
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			if data[i] {
				packed[i/8] |= 1 << (uint(i) % 8)
			}
		}
	}
	_, err := sw.w.Write(packed)
	return err
}

func (sw *shuffleWriter) writeBytesData(bc *batch.BytesColumn, sel []uint32, numRows int) error {
	// Gather all byte slices, write: totalDataLen + data + offsets
	// Format: uint32(totalDataLen) + data + uint32[numRows] (end offsets)
	var totalLen uint32
	offsets := make([]uint32, numRows)

	if sel != nil {
		for i, si := range sel {
			val := bc.Value(int(si))
			totalLen += uint32(len(val))
			offsets[i] = totalLen
		}
	} else {
		for i := 0; i < numRows; i++ {
			val := bc.Value(i)
			totalLen += uint32(len(val))
			offsets[i] = totalLen
		}
	}

	// Write total data length
	binary.LittleEndian.PutUint32(sw.buf[:4], totalLen)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}

	// Write concatenated data
	if sel != nil {
		for _, si := range sel {
			val := bc.Value(int(si))
			if len(val) > 0 {
				if _, err := sw.w.Write(val); err != nil {
					return err
				}
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			val := bc.Value(i)
			if len(val) > 0 {
				if _, err := sw.w.Write(val); err != nil {
					return err
				}
			}
		}
	}

	// Write offsets
	for _, off := range offsets {
		binary.LittleEndian.PutUint32(sw.buf[:4], off)
		if _, err := sw.w.Write(sw.buf[:4]); err != nil {
			return err
		}
	}
	return nil
}

func (sw *shuffleWriter) writeDecimalData(vec *batch.Vector, sel []uint32, numRows int) error {
	// Decimal: write as 16-byte Int128 pairs (lo, hi)
	dataLen := uint32(numRows * 16)
	binary.LittleEndian.PutUint32(sw.buf[:4], dataLen)
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if vec.DecimalData.Data == nil {
		// Write zeros
		zeros := make([]byte, dataLen)
		_, err := sw.w.Write(zeros)
		return err
	}
	if sel != nil {
		for _, si := range sel {
			d := vec.DecimalData.Data[si]
			binary.LittleEndian.PutUint64(sw.buf[:8], d.Lo)
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(sw.buf[:8], uint64(d.Hi))
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			d := vec.DecimalData.Data[i]
			binary.LittleEndian.PutUint64(sw.buf[:8], d.Lo)
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(sw.buf[:8], uint64(d.Hi))
			if _, err := sw.w.Write(sw.buf[:8]); err != nil {
				return err
			}
		}
	}
	return nil
}

// shuffleReadBatches reads all chunks from a binary shuffle file into RecordBatches.
func shuffleReadBatches(data []byte) ([]*batch.RecordBatch, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("shuffle file too small: %d bytes", len(data))
	}

	// Verify magic
	if data[0] != shuffleMagic[0] || data[1] != shuffleMagic[1] ||
		data[2] != shuffleMagic[2] || data[3] != shuffleMagic[3] {
		return nil, fmt.Errorf("invalid shuffle magic: %q", data[:4])
	}

	pos := 4
	numChunks := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	numCols := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// Read schema
	schema := make([]parquet.Column, numCols)
	for i := 0; i < numCols; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		schema[i].Name = string(data[pos : pos+nameLen])
		pos += nameLen
		schema[i].Type = parquet.TypeID(data[pos])
		schema[i].Nullable = true
		pos++
	}

	// Read chunks
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
			pos, err = readColumnData(data, pos, b.Columns[ci], numRows, schema[ci].Type)
			if err != nil {
				return nil, fmt.Errorf("reading column %d (%s) chunk %d: %w", ci, schema[ci].Name, chunk, err)
			}
		}
		batches = append(batches, b)
	}
	return batches, nil
}

func readColumnData(data []byte, pos int, vec *batch.Vector, numRows int, typ parquet.TypeID) (int, error) {
	// Read null bitmap
	bitmapWords := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	words := vec.Nulls.Words()
	for i := 0; i < bitmapWords && i < len(words); i++ {
		words[i] = binary.LittleEndian.Uint64(data[pos:])
		pos += 8
	}
	// Skip remaining bitmap words if vector bitmap is smaller
	for i := len(words); i < bitmapWords; i++ {
		pos += 8
	}

	// Read column data
	switch typ {
	case parquet.TypeBool:
		return readBoolData(data, pos, vec.BoolData, numRows)
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return readInt32Data(data, pos, vec.Int32Data, numRows)
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return readInt64Data(data, pos, vec.Int64Data, numRows)
	case parquet.TypeFloat32:
		return readFloat32Data(data, pos, vec.Float32Data, numRows)
	case parquet.TypeFloat64:
		return readFloat64Data(data, pos, vec.Float64Data, numRows)
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		return readBytesData(data, pos, &vec.BytesData, numRows)
	case parquet.TypeDecimal:
		return readDecimalData(data, pos, vec, numRows)
	default:
		return pos, fmt.Errorf("unsupported shuffle type: %v", typ)
	}
}

func readInt32Data(data []byte, pos int, dst []int32, numRows int) (int, error) {
	dataLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	_ = dataLen
	for i := 0; i < numRows; i++ {
		dst[i] = int32(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	}
	return pos, nil
}

func readInt64Data(data []byte, pos int, dst []int64, numRows int) (int, error) {
	dataLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	_ = dataLen
	for i := 0; i < numRows; i++ {
		dst[i] = int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
	}
	return pos, nil
}

func readFloat32Data(data []byte, pos int, dst []float32, numRows int) (int, error) {
	dataLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	_ = dataLen
	for i := 0; i < numRows; i++ {
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	}
	return pos, nil
}

func readFloat64Data(data []byte, pos int, dst []float64, numRows int) (int, error) {
	dataLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	_ = dataLen
	for i := 0; i < numRows; i++ {
		dst[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
	}
	return pos, nil
}

func readBoolData(data []byte, pos int, dst []bool, numRows int) (int, error) {
	packedLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	for i := 0; i < numRows; i++ {
		dst[i] = (data[pos+i/8]>>(uint(i)%8))&1 == 1
	}
	pos += packedLen
	return pos, nil
}

func readBytesData(data []byte, pos int, dst *batch.BytesColumn, numRows int) (int, error) {
	totalDataLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// Read concatenated data
	allData := data[pos : pos+totalDataLen]
	pos += totalDataLen

	// Read offsets
	offsets := make([]uint32, numRows)
	for i := 0; i < numRows; i++ {
		offsets[i] = binary.LittleEndian.Uint32(data[pos:])
		pos += 4
	}

	// Reconstruct BytesColumn
	var prevOff uint32
	for i := 0; i < numRows; i++ {
		end := offsets[i]
		dst.Set(i, allData[prevOff:end])
		prevOff = end
	}
	return pos, nil
}

func readDecimalData(data []byte, pos int, vec *batch.Vector, numRows int) (int, error) {
	dataLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	_ = dataLen
	if vec.DecimalData.Data == nil {
		pos += numRows * 16
		return pos, nil
	}
	for i := 0; i < numRows; i++ {
		vec.DecimalData.Data[i].Lo = binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		vec.DecimalData.Data[i].Hi = int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
	}
	return pos, nil
}

// isShuffleFormat returns true if the data starts with the shuffle magic bytes.
func isShuffleFormat(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == shuffleMagic[0] && data[1] == shuffleMagic[1] &&
		data[2] == shuffleMagic[2] && data[3] == shuffleMagic[3]
}
