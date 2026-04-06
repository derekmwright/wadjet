package worker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"unsafe"

	"github.com/klauspost/compress/s2"

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
	gatherBuf []byte // reusable buffer for gathering selected rows
}

func newShuffleWriter(w io.Writer, schema []parquet.Column) *shuffleWriter {
	return &shuffleWriter{
		w:      w,
		schema: schema,
		buf:    make([]byte, 8),
	}
}

// ensureGather returns a byte slice of at least n bytes from the reusable buffer.
func (sw *shuffleWriter) ensureGather(n int) []byte {
	if cap(sw.gatherBuf) < n {
		sw.gatherBuf = make([]byte, n)
	}
	return sw.gatherBuf[:n]
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
	// Write all bitmap words at once via direct memory reinterpretation.
	bitmapBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(bitmapBuf))), len(bitmapBuf)*8)
	if _, err := sw.w.Write(bitmapBytes); err != nil {
		return err
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
		return sw.writeBytesData(&vec.BytesData, &vec.Nulls, sel, numRows)
	case parquet.TypeDecimal:
		return sw.writeDecimalData(vec, sel, numRows)
	default:
		return fmt.Errorf("unsupported shuffle type: %v", typ)
	}
}

func (sw *shuffleWriter) writeInt32Data(data []int32, sel []uint32, numRows int) error {
	nbytes := numRows * 4
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(nbytes))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		gb := sw.ensureGather(nbytes)
		for i, si := range sel {
			binary.LittleEndian.PutUint32(gb[i*4:], uint32(data[si]))
		}
		_, err := sw.w.Write(gb[:nbytes])
		return err
	}
	// No selection: write slice memory directly (little-endian platforms).
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(data[:numRows]))), nbytes)
	_, err := sw.w.Write(raw)
	return err
}

func (sw *shuffleWriter) writeInt64Data(data []int64, sel []uint32, numRows int) error {
	nbytes := numRows * 8
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(nbytes))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		gb := sw.ensureGather(nbytes)
		for i, si := range sel {
			binary.LittleEndian.PutUint64(gb[i*8:], uint64(data[si]))
		}
		_, err := sw.w.Write(gb[:nbytes])
		return err
	}
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(data[:numRows]))), nbytes)
	_, err := sw.w.Write(raw)
	return err
}

func (sw *shuffleWriter) writeFloat32Data(data []float32, sel []uint32, numRows int) error {
	nbytes := numRows * 4
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(nbytes))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		gb := sw.ensureGather(nbytes)
		for i, si := range sel {
			binary.LittleEndian.PutUint32(gb[i*4:], math.Float32bits(data[si]))
		}
		_, err := sw.w.Write(gb[:nbytes])
		return err
	}
	// float32 IEEE 754 memory layout matches the wire format on little-endian.
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(data[:numRows]))), nbytes)
	_, err := sw.w.Write(raw)
	return err
}

func (sw *shuffleWriter) writeFloat64Data(data []float64, sel []uint32, numRows int) error {
	nbytes := numRows * 8
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(nbytes))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if sel != nil {
		gb := sw.ensureGather(nbytes)
		for i, si := range sel {
			binary.LittleEndian.PutUint64(gb[i*8:], math.Float64bits(data[si]))
		}
		_, err := sw.w.Write(gb[:nbytes])
		return err
	}
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(data[:numRows]))), nbytes)
	_, err := sw.w.Write(raw)
	return err
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

func (sw *shuffleWriter) writeBytesData(bc *batch.BytesColumn, nulls *batch.Bitmap, sel []uint32, numRows int) error {
	// Gather all byte slices, write: totalDataLen + data + offsets
	// Format: uint32(totalDataLen) + data + uint32[numRows] (end offsets)
	//
	// Null-bitmap-aware: skip bc.Value() for null rows because join output
	// (gatherBuildVector) may not call BytesData.Set for unmatched rows,
	// leaving Offsets[i+1]=0 while Offsets[i]>0 which panics on Value(i).
	var totalLen uint32
	offsets := make([]uint32, numRows)

	if sel != nil {
		for i, si := range sel {
			if nulls.IsNull(int(si)) {
				offsets[i] = totalLen
				continue
			}
			val := bc.Value(int(si))
			totalLen += uint32(len(val))
			offsets[i] = totalLen
		}
	} else {
		for i := 0; i < numRows; i++ {
			if nulls.IsNull(i) {
				offsets[i] = totalLen
				continue
			}
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
			if nulls.IsNull(int(si)) {
				continue
			}
			val := bc.Value(int(si))
			if len(val) > 0 {
				if _, err := sw.w.Write(val); err != nil {
					return err
				}
			}
		}
	} else {
		for i := 0; i < numRows; i++ {
			if nulls.IsNull(i) {
				continue
			}
			val := bc.Value(i)
			if len(val) > 0 {
				if _, err := sw.w.Write(val); err != nil {
					return err
				}
			}
		}
	}

	// Write all offsets at once via direct memory reinterpretation.
	offBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(offsets))), len(offsets)*4)
	_, err := sw.w.Write(offBytes)
	return err
}

func (sw *shuffleWriter) writeDecimalData(vec *batch.Vector, sel []uint32, numRows int) error {
	// Decimal: write as 16-byte Int128 pairs (lo, hi)
	nbytes := numRows * 16
	binary.LittleEndian.PutUint32(sw.buf[:4], uint32(nbytes))
	if _, err := sw.w.Write(sw.buf[:4]); err != nil {
		return err
	}
	if vec.DecimalData.Data == nil {
		zeros := make([]byte, nbytes)
		_, err := sw.w.Write(zeros)
		return err
	}
	if sel != nil {
		gb := sw.ensureGather(nbytes)
		for i, si := range sel {
			d := vec.DecimalData.Data[si]
			binary.LittleEndian.PutUint64(gb[i*16:], d.Lo)
			binary.LittleEndian.PutUint64(gb[i*16+8:], uint64(d.Hi))
		}
		_, err := sw.w.Write(gb[:nbytes])
		return err
	}
	// Int128 struct {Lo uint64; Hi int64} has matching little-endian layout.
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(vec.DecimalData.Data[:numRows]))), nbytes)
	_, err := sw.w.Write(raw)
	return err
}

// shuffleChunkReader iterates over chunks in a WSHF byte slice one at a time,
// allocating a single RecordBatch per Next call. Callers hold only one batch
// in memory at a time instead of materializing the entire file up front.
type shuffleChunkReader struct {
	data      []byte
	schema    []parquet.Column
	numChunks uint32
	chunk     uint32
	pos       int
}

// newShuffleChunkReader parses the WSHF header and returns a reader positioned
// at the first chunk. The caller retains ownership of data — it must remain
// valid for the lifetime of the reader.
func newShuffleChunkReader(data []byte) (*shuffleChunkReader, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("shuffle file too small: %d bytes", len(data))
	}
	if data[0] != shuffleMagic[0] || data[1] != shuffleMagic[1] ||
		data[2] != shuffleMagic[2] || data[3] != shuffleMagic[3] {
		return nil, fmt.Errorf("invalid shuffle magic: %q", data[:4])
	}

	pos := 4
	numChunks := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	numCols := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	schema := make([]parquet.Column, numCols)
	for i := 0; i < numCols; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("truncated schema at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen+1 > len(data) {
			return nil, fmt.Errorf("truncated schema name at column %d", i)
		}
		schema[i].Name = string(data[pos : pos+nameLen])
		pos += nameLen
		schema[i].Type = parquet.TypeID(data[pos])
		schema[i].Nullable = true
		pos++
	}

	return &shuffleChunkReader{
		data:      data,
		schema:    schema,
		numChunks: numChunks,
		pos:       pos,
	}, nil
}

// Next returns the next RecordBatch from the file, or (nil, nil) when all chunks
// have been consumed. Allocates exactly one RecordBatch per non-empty chunk.
func (r *shuffleChunkReader) Next() (*batch.RecordBatch, error) {
	for r.chunk < r.numChunks {
		if r.pos+4 > len(r.data) {
			return nil, nil
		}
		numRows := int(binary.LittleEndian.Uint32(r.data[r.pos:]))
		r.pos += 4
		r.chunk++

		if numRows == 0 {
			continue
		}

		b := batch.NewRecordBatch(r.schema, numRows)
		for ci := range r.schema {
			var err error
			r.pos, err = readColumnData(r.data, r.pos, b.Columns[ci], numRows, r.schema[ci].Type)
			if err != nil {
				return nil, fmt.Errorf("reading column %d (%s) chunk %d: %w", ci, r.schema[ci].Name, r.chunk-1, err)
			}
		}
		return b, nil
	}
	return nil, nil
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
	// Read null bitmap via bulk copy.
	bitmapWords := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	words := vec.Nulls.Words()
	copyWords := bitmapWords
	if copyWords > len(words) {
		copyWords = len(words)
	}
	if copyWords > 0 {
		dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(words[:copyWords]))), copyWords*8)
		copy(dstBytes, data[pos:pos+copyWords*8])
	}
	pos += bitmapWords * 8
	// Invalidate the HasNulls() cache since we overwrote bitmap words directly.
	vec.Nulls.InvalidateCache()

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
	pos += 4 // skip dataLen header
	nbytes := numRows * 4
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst[:numRows]))), nbytes)
	copy(dstBytes, data[pos:pos+nbytes])
	return pos + nbytes, nil
}

func readInt64Data(data []byte, pos int, dst []int64, numRows int) (int, error) {
	pos += 4
	nbytes := numRows * 8
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst[:numRows]))), nbytes)
	copy(dstBytes, data[pos:pos+nbytes])
	return pos + nbytes, nil
}

func readFloat32Data(data []byte, pos int, dst []float32, numRows int) (int, error) {
	pos += 4
	nbytes := numRows * 4
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst[:numRows]))), nbytes)
	copy(dstBytes, data[pos:pos+nbytes])
	return pos + nbytes, nil
}

func readFloat64Data(data []byte, pos int, dst []float64, numRows int) (int, error) {
	pos += 4
	nbytes := numRows * 8
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst[:numRows]))), nbytes)
	copy(dstBytes, data[pos:pos+nbytes])
	return pos + nbytes, nil
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

	// Read offsets via bulk copy.
	offsets := make([]uint32, numRows)
	offBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(offsets))), numRows*4)
	copy(offBytes, data[pos:pos+numRows*4])
	pos += numRows * 4

	// Bulk-copy all string data into the BytesColumn, then set offsets.
	dst.Data = append(dst.Data[:0], allData...)
	if cap(dst.Offsets) < numRows+1 {
		dst.Offsets = make([]uint32, numRows+1)
	} else {
		dst.Offsets = dst.Offsets[:numRows+1]
	}
	dst.Offsets[0] = 0
	copy(dst.Offsets[1:], offsets)
	return pos, nil
}

func readDecimalData(data []byte, pos int, vec *batch.Vector, numRows int) (int, error) {
	pos += 4 // skip dataLen header
	nbytes := numRows * 16
	if vec.DecimalData.Data == nil {
		return pos + nbytes, nil
	}
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(vec.DecimalData.Data[:numRows]))), nbytes)
	copy(dstBytes, data[pos:pos+nbytes])
	return pos + nbytes, nil
}

// isShuffleFormat returns true if the data starts with the shuffle magic bytes
// (either uncompressed WSHF or compressed WSHC).
func isShuffleFormat(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return (data[0] == shuffleMagic[0] && data[1] == shuffleMagic[1] &&
		data[2] == shuffleMagic[2] && data[3] == shuffleMagic[3]) ||
		(data[0] == compressedMagic[0] && data[1] == compressedMagic[1] &&
			data[2] == compressedMagic[2] && data[3] == compressedMagic[3])
}

// compressedMagic identifies a compressed WSHF payload.
var compressedMagic = [4]byte{'W', 'S', 'H', 'C'}

// CompressShuffleData compresses raw WSHF data using S2 streaming format.
// Returns data with "WSHC" magic prefix followed by the S2 stream.
// Uses streaming (not block) format to support arbitrarily large payloads.
// If the compressed output is not smaller, the original WSHF data is returned.
func CompressShuffleData(data []byte) []byte {
	if len(data) < 64 {
		return data // too small to benefit
	}
	var buf bytes.Buffer
	buf.Grow(len(data)/2 + 4)
	buf.Write(compressedMagic[:])
	w := s2.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return data
	}
	if err := w.Close(); err != nil {
		return data
	}
	// Only use compression if it saves at least 10%
	if buf.Len() >= len(data)*9/10 {
		return data
	}
	return buf.Bytes()
}

// DecompressShuffleData detects and decompresses a WSHC payload back to raw WSHF.
// If the data is already plain WSHF (or non-shuffle), it is returned unchanged.
func DecompressShuffleData(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return data, nil
	}
	if data[0] != compressedMagic[0] || data[1] != compressedMagic[1] ||
		data[2] != compressedMagic[2] || data[3] != compressedMagic[3] {
		return data, nil // not compressed
	}
	r := s2.NewReader(bytes.NewReader(data[4:]))
	decoded, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("decompressing shuffle data: %w", err)
	}
	return decoded, nil
}
