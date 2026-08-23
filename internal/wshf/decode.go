package wshf

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ParseHeader consumes the WSHF magic, chunk count and schema from c,
// leaving the cursor at the first chunk's row-count word.
func ParseHeader(c *Cursor) (schema []parquet.Column, numChunks uint32, err error) {
	magic, err := c.Take(4, "shuffle magic")
	if err != nil {
		return nil, 0, err
	}
	if [4]byte(magic) != MagicWSHF {
		return nil, 0, fmt.Errorf("invalid shuffle magic: %q", magic)
	}
	n, err := c.U32("chunk count")
	if err != nil {
		return nil, 0, err
	}
	numCols, err := c.U16("column count")
	if err != nil {
		return nil, 0, err
	}
	if int(numCols) > MaxCols {
		return nil, 0, fmt.Errorf("shuffle header: implausible column count %d", numCols)
	}
	schema = make([]parquet.Column, numCols)
	for i := range schema {
		nameLen, err := c.U16("schema name length")
		if err != nil {
			return nil, 0, fmt.Errorf("truncated schema at column %d: %w", i, err)
		}
		if int(nameLen) > MaxNameLen {
			return nil, 0, fmt.Errorf("shuffle schema: implausible name length %d at column %d", nameLen, i)
		}
		name, err := c.Take(int(nameLen), "schema name")
		if err != nil {
			return nil, 0, fmt.Errorf("truncated schema name at column %d: %w", i, err)
		}
		schema[i].Name = string(name)
		typ, err := c.U8("schema type")
		if err != nil {
			return nil, 0, fmt.Errorf("truncated schema type at column %d: %w", i, err)
		}
		schema[i].Type = parquet.TypeID(typ)
		schema[i].Nullable = true
		// Decimal columns carry scale+precision after the type byte (see
		// the writer's writeHeader) — without them the decoded vector
		// renders the raw scaled integer (fraction lost).
		if schema[i].Type == parquet.TypeDecimal {
			sp, err := c.Take(2, "decimal scale/precision")
			if err != nil {
				return nil, 0, fmt.Errorf("truncated decimal schema at column %d: %w", i, err)
			}
			schema[i].Scale = int(sp[0])
			schema[i].Precision = int(sp[1])
		}
	}
	return schema, n, nil
}

// ChunkReader iterates over the chunks in a WSHF byte slice one at a time,
// allocating a single RecordBatch per Next call. Callers hold only one
// batch in memory at a time instead of materializing the whole payload.
type ChunkReader struct {
	cur       Cursor
	schema    []parquet.Column
	numChunks uint32
	chunk     uint32
}

// NewChunkReader parses the WSHF header and returns a reader positioned at
// the first chunk. The caller retains ownership of data — it must remain
// valid for the lifetime of the reader (batches copy their bytes out, so
// the data may be released once the last batch is in hand).
func NewChunkReader(data []byte) (*ChunkReader, error) {
	if len(data) < HeaderLen {
		return nil, fmt.Errorf("shuffle payload too small: %d bytes", len(data))
	}
	c := NewCursor(data)
	schema, numChunks, err := ParseHeader(&c)
	if err != nil {
		return nil, err
	}
	return &ChunkReader{cur: c, schema: schema, numChunks: numChunks}, nil
}

// Schema is the decoded column schema.
func (r *ChunkReader) Schema() []parquet.Column { return r.schema }

// NumChunks is the chunk count the header promised.
func (r *ChunkReader) NumChunks() uint32 { return r.numChunks }

// Pos returns the reader's byte offset into the WSHF slice — everything
// below it has been fully decoded (batches copy column data out), so the
// drop-behind walk can discard those pages. Strictly monotonic.
func (r *ChunkReader) Pos() int { return r.cur.Pos() }

// Next returns the next RecordBatch, or (nil, nil) when all chunks have
// been consumed. Allocates exactly one RecordBatch per non-empty chunk.
func (r *ChunkReader) Next() (*batch.RecordBatch, error) {
	for r.chunk < r.numChunks {
		numRows, err := r.cur.U32("chunk row count")
		if err != nil {
			// We promised numChunks worth of data in the header but the
			// payload is too short to even hold the next chunk's row-count
			// word. Returning a silent EOF here would drop the remaining
			// rows on the floor — at SF100 that turned into wrong query
			// results (Q05 returning 0 rows when build cache files were
			// truncated). Surface it so the caller fails the task instead
			// of completing with missing data.
			return nil, fmt.Errorf("shuffle payload truncated: chunk %d/%d: %w", r.chunk, r.numChunks, err)
		}
		r.chunk++
		if numRows == 0 {
			continue
		}
		b, err := decodeChunkAt(&r.cur, r.schema, int(numRows), r.chunk-1)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	return nil, nil
}

// DecodeBatches decodes every chunk in a raw WSHF payload. Callers holding
// the whole payload in memory already (inline results, gather replies) use
// this; file-backed readers use ChunkReader.
func DecodeBatches(data []byte) ([]*batch.RecordBatch, error) {
	r, err := NewChunkReader(data)
	if err != nil {
		return nil, err
	}
	batches := make([]*batch.RecordBatch, 0, min(int(r.numChunks), 64))
	for {
		b, err := r.Next()
		if err != nil {
			return nil, err
		}
		if b == nil {
			return batches, nil
		}
		batches = append(batches, b)
	}
}

// DecodeChunk materializes one staged chunk's column segments (the bytes
// AFTER the row-count word) into a fresh RecordBatch. Shared by the serial
// stream path, the decode-ahead workers and the index-mode pread workers so
// they cannot diverge on payload interpretation; chunkIdx is for error text.
func DecodeChunk(schema []parquet.Column, numRows int, chunkBytes []byte, chunkIdx uint32) (*batch.RecordBatch, error) {
	c := NewCursor(chunkBytes)
	return decodeChunkAt(&c, schema, numRows, chunkIdx)
}

func decodeChunkAt(c *Cursor, schema []parquet.Column, numRows int, chunkIdx uint32) (*batch.RecordBatch, error) {
	if numRows < 0 || numRows > MaxRows {
		return nil, fmt.Errorf("shuffle chunk %d: implausible row count %d", chunkIdx, numRows)
	}
	// A row count is a claim about bytes that follow, and NewRecordBatch
	// allocates against it before a single one of them is read. Price the
	// chunk's floor first: the smallest encoding of numRows rows in this
	// schema. A count that cannot fit in what remains is corrupt, and
	// rejecting it here is what keeps a 4-byte field from asking for
	// gigabytes.
	need, err := minChunkBytes(schema, numRows)
	if err != nil {
		return nil, fmt.Errorf("shuffle chunk %d: %w", chunkIdx, err)
	}
	if c.Remaining() < need {
		return nil, fmt.Errorf("shuffle chunk %d: %d rows need at least %d bytes, %d remain",
			chunkIdx, numRows, need, c.Remaining())
	}
	b := batch.NewRecordBatch(schema, numRows)
	for ci := range schema {
		if err := ReadColumn(c, b.Columns[ci], numRows, schema[ci].Type); err != nil {
			return nil, fmt.Errorf("reading column %d (%s) chunk %d: %w", ci, schema[ci].Name, chunkIdx, err)
		}
	}
	batch.SyncContainerSchema(b)
	return b, nil
}

// minChunkBytes is the smallest number of bytes a chunk of numRows rows in
// this schema can occupy: the two length words per column plus the payload
// floor (exact for fixed-width types, the offsets array for byte columns,
// zero for the self-describing container payload).
func minChunkBytes(schema []parquet.Column, numRows int) (int, error) {
	total := 0
	for ci := range schema {
		want, err := FixedTypeLen(schema[ci].Type, numRows)
		if err != nil {
			return 0, fmt.Errorf("column %d: %w", ci, err)
		}
		switch want {
		case LenContainer:
			want = 0
		case LenBytes:
			want = numRows * 4 // end offsets; the data itself may be empty
		}
		total += 8 + want
		if total < 0 { // overflow on a hostile row count
			return 0, fmt.Errorf("chunk size overflow at column %d for %d rows", ci, numRows)
		}
	}
	return total, nil
}

// ReadColumn decodes one column segment into vec, advancing c past it.
// Every count and length is checked against the bytes that remain before
// it is used — this is the bounds-checked replacement for the two
// hand-copied unchecked walks (#422).
func ReadColumn(c *Cursor, vec *batch.Vector, numRows int, typ parquet.TypeID) error {
	// Null bitmap: word count, then the words. The writer emits exactly
	// (numRows+63)/64; accept one spare word, reject a claim beyond that.
	maxWords := (numRows+63)/64 + 1
	bitmapWords, err := c.Len32("bitmap word count", maxWords)
	if err != nil {
		return err
	}
	bitmap, err := c.Take(bitmapWords*8, "null bitmap")
	if err != nil {
		return err
	}
	words := vec.Nulls.Words()
	if n := min(bitmapWords, len(words)); n > 0 {
		dst := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(words[:n]))), n*8)
		copy(dst, bitmap[:n*8])
	}
	// The bitmap words were overwritten behind the Bitmap's back.
	vec.Nulls.InvalidateCache()

	// Typed payload: length prefix, then the payload. For fixed-width
	// types the prefix must agree with the row count exactly — the same
	// cross-check the streaming stage walk and the extent-index validator
	// apply, so a decoder cannot accept a chunk a walker would reject.
	want, err := FixedTypeLen(typ, numRows)
	if err != nil {
		return err
	}
	dataLen, err := c.Len32("column data length", MaxBytesLen)
	if err != nil {
		return err
	}
	if want >= 0 && dataLen != want {
		return fmt.Errorf("column data length %d != expected %d for %d rows of %v", dataLen, want, numRows, typ)
	}

	switch typ {
	case parquet.TypeBool:
		data, err := c.Take(dataLen, "bool data")
		if err != nil {
			return err
		}
		if len(vec.BoolData) < numRows {
			return fmt.Errorf("bool vector holds %d rows, chunk has %d", len(vec.BoolData), numRows)
		}
		for i := 0; i < numRows; i++ {
			vec.BoolData[i] = (data[i/8]>>(uint(i)%8))&1 == 1
		}
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		return readFixed(c, dataLen, vec.Int32Data, numRows, 4, "int32 data")
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		return readFixed(c, dataLen, vec.Int64Data, numRows, 8, "int64 data")
	case parquet.TypeFloat32:
		return readFixed(c, dataLen, vec.Float32Data, numRows, 4, "float32 data")
	case parquet.TypeFloat64:
		return readFixed(c, dataLen, vec.Float64Data, numRows, 8, "float64 data")
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		return readBytes(c, dataLen, &vec.BytesData, numRows)
	case parquet.TypeDecimal:
		return readDecimal(c, dataLen, vec, numRows)
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
		// [payloadLen u32][payload]. The nested shape (child element types,
		// ROW field order, VECTOR dimension) comes from the payload, not
		// from the WSHF schema, which carries only a name and a type byte —
		// so the vector NewRecordBatch built arrives here with no children
		// at all and the codec installs them.
		payload, err := c.Take(dataLen, "container payload")
		if err != nil {
			return err
		}
		return batch.DecodeContainerColumn(payload, vec, numRows)
	default:
		return fmt.Errorf("unsupported shuffle type: %v", typ)
	}
	return nil
}

// readFixed copies a fixed-width little-endian payload straight into the
// vector's backing array. dataLen was already cross-checked against
// numRows*width by the caller; the destination length is checked here
// because a vector sized from a different row count would otherwise
// overrun on the unsafe reinterpretation.
func readFixed[T int32 | int64 | float32 | float64](c *Cursor, dataLen int, dst []T, numRows, width int, what string) error {
	src, err := c.Take(dataLen, what)
	if err != nil {
		return err
	}
	if len(dst) < numRows {
		return fmt.Errorf("%s: vector holds %d rows, chunk has %d", what, len(dst), numRows)
	}
	if numRows == 0 {
		return nil
	}
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst[:numRows]))), numRows*width)
	copy(dstBytes, src)
	return nil
}

// readBytes decodes [dataLen u32][concatenated data][numRows × u32 end
// offsets]. The offsets are validated as they are copied: they must be
// non-decreasing and stay inside the data segment, because BytesColumn
// slices Data[Offsets[i]:Offsets[i+1]] on every read — an unchecked offset
// is a panic deferred to whichever operator touches the value first.
func readBytes(c *Cursor, dataLen int, dst *batch.BytesColumn, numRows int) error {
	allData, err := c.Take(dataLen, "bytes data")
	if err != nil {
		return err
	}
	offBytes, err := c.Take(numRows*4, "bytes offsets")
	if err != nil {
		return err
	}
	dst.Data = append(dst.Data[:0], allData...)
	if cap(dst.Offsets) < numRows+1 {
		dst.Offsets = make([]uint32, numRows+1)
	} else {
		dst.Offsets = dst.Offsets[:numRows+1]
	}
	dst.Offsets[0] = 0
	if numRows > 0 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst.Offsets[1:]))), numRows*4), offBytes)
	}
	prev := uint32(0)
	for i := 1; i <= numRows; i++ {
		off := dst.Offsets[i]
		if off < prev || off > uint32(dataLen) {
			return fmt.Errorf("bytes offset %d at row %d is outside [%d, %d]", off, i-1, prev, dataLen)
		}
		prev = off
	}
	return nil
}

// readDecimal decodes numRows × (Lo, Hi) little-endian pairs. Wire order is
// (Lo, Hi) — see the writer's writeDecimalData; the field order of the
// Int128 struct is the other way round, so this must stay an explicit
// per-field read and never a raw memcpy.
func readDecimal(c *Cursor, dataLen int, vec *batch.Vector, numRows int) error {
	src, err := c.Take(dataLen, "decimal data")
	if err != nil {
		return err
	}
	if vec.DecimalData.Data == nil {
		return nil // caller does not want the values, only the walk
	}
	if len(vec.DecimalData.Data) < numRows {
		return fmt.Errorf("decimal vector holds %d rows, chunk has %d", len(vec.DecimalData.Data), numRows)
	}
	for i := 0; i < numRows; i++ {
		off := i * 16
		vec.DecimalData.Data[i] = batch.Int128{
			Lo: binary.LittleEndian.Uint64(src[off:]),
			Hi: int64(binary.LittleEndian.Uint64(src[off+8:])),
		}
	}
	return nil
}

// ValidateChunkBytes walks one chunk's column segments in buf (the bytes
// AFTER the row-count word) and requires the walk to consume buf in full.
// Index-mode decode workers run it over their pread extent before decoding:
// the decoder is bounds-checked on its own, but "these bytes are exactly
// one chunk" is a stronger claim than "this decode did not run off the
// end", and an extent that is off by a column is a wrong answer, not a
// crash.
func ValidateChunkBytes(schema []parquet.Column, numRows int, buf []byte) error {
	c := NewCursor(buf)
	for ci := range schema {
		maxWords := (numRows+63)/64 + 1
		bitmapWords, err := c.Len32("bitmap word count", maxWords)
		if err != nil {
			return fmt.Errorf("column %d: %w", ci, err)
		}
		if err := c.Skip(bitmapWords*8, "null bitmap"); err != nil {
			return fmt.Errorf("column %d: %w", ci, err)
		}
		want, err := FixedTypeLen(schema[ci].Type, numRows)
		if err != nil {
			return fmt.Errorf("column %d: %w", ci, err)
		}
		dataLen, err := c.Len32("column data length", MaxBytesLen)
		if err != nil {
			return fmt.Errorf("column %d: %w", ci, err)
		}
		if want >= 0 && dataLen != want {
			return fmt.Errorf("column %d (%v): data length %d != expected %d for %d rows",
				ci, schema[ci].Type, dataLen, want, numRows)
		}
		if err := c.Skip(dataLen, "column data"); err != nil {
			return fmt.Errorf("column %d: %w", ci, err)
		}
		if want == LenBytes {
			if err := c.Skip(numRows*4, "bytes offsets"); err != nil {
				return fmt.Errorf("column %d: %w", ci, err)
			}
		}
	}
	if c.Remaining() != 0 {
		return fmt.Errorf("extent is %d bytes but the column walk consumed %d", c.Size(), c.Pos())
	}
	return nil
}
