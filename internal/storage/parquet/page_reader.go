package parquet

import (
	"fmt"
	"io"
)

// PageData holds the decoded contents of a single Parquet data page.
type PageData struct {
	NumValues        int
	NumRows          int
	NumNulls         int
	Data             Values  // decoded column values (non-null values only)
	DefinitionLevels []int32 // nil if column is required (no nulls)
	RepetitionLevels []int32 // nil for flat schemas (no nesting)

	// rawBuf is the decompressed page buffer that backs Data when the page
	// values alias the decompress output (PLAIN-encoded numeric/fixed-len
	// columns). nil when no pooled buffer needs to be returned (uncompressed
	// pages, dictionary-indexed pages, or codecs without a buffer pool).
	rawBuf []byte
	codec  CompressionCodec
}

// Release returns any pooled decompression buffer backing this page to its
// pool. Must be called once the page's values have been copied into their
// destination Vector — subsequent use of p.Data is undefined.
func (p *PageData) Release() {
	if p == nil || p.rawBuf == nil {
		return
	}
	ReleaseDecompressed(p.codec, p.rawBuf)
	p.rawBuf = nil
}

// DictionaryData holds the decoded contents of a dictionary page.
type DictionaryData struct {
	NumValues int
	Data      Values // dictionary entries
}

// ColumnPageReader reads pages from a single column chunk.
// It provides an iterator interface: call NextPage() until it returns nil.
//
// Two backing modes share the decode path:
//   - slice mode (NewColumnPageReader): data is a caller-held buffer of
//     the whole file, offsets are file-absolute, Close is a no-op.
//   - staged mode (NewColumnPageReaderAt): the chunk's byte range is
//     read from src into a pooled buffer on first use, offsets are
//     chunk-relative, and Close returns the buffer to the pool. No
//     decoded value may be referenced after Close — the same
//     copy-before-release contract PageData.Release already imposes
//     per page (uncompressed pages alias the chunk buffer directly).
type ColumnPageReader struct {
	data       []byte          // raw column chunk bytes
	off        int             // current read position
	endOff     int             // end of column data
	codec      CompressionCodec
	physType   PhysicalType
	typeLength int             // for FIXED_LEN_BYTE_ARRAY
	maxDefLevel int            // 0 if column is required
	maxRepLevel int            // 0 for flat schemas

	// Staged mode (docs/design/scan-pread-reads.md): the chunk is read
	// from src on first NextDictionary/NextPage instead of sliced from a
	// caller-held full-file buffer.
	src    io.ReaderAt // nil in slice mode
	srcOff int64       // file offset of the chunk's first byte
	srcLen int         // chunk byte length ([srcOff, srcOff+srcLen))
	owned  []byte      // pooled staging buffer; returned on Close
}

// NewColumnPageReader creates a page reader for a column chunk.
//
// Parameters:
//   - fileData: the entire file bytes (or the relevant region)
//   - cm: column metadata from the footer
//   - maxDefLevel: maximum definition level (0 for required columns)
//   - maxRepLevel: maximum repetition level (0 for flat schemas)
func NewColumnPageReader(fileData []byte, cm *ColumnMetaData, maxDefLevel, maxRepLevel int) *ColumnPageReader {
	startOff := int(cm.DataPageOffset)
	if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < cm.DataPageOffset {
		startOff = int(cm.DictionaryPageOffset)
	}
	endOff := startOff + int(cm.TotalCompressedSize)
	if endOff > len(fileData) {
		endOff = len(fileData)
	}

	// Determine type length for FIXED_LEN_BYTE_ARRAY.
	typeLength := 0
	if cm.Type == PhysicalFixedLenByteArray {
		// Type length should be inferred from schema; for now use data size / num_values.
		// This will be refined when the schema tree is available.
		typeLength = 0 // caller should set this
	}

	return &ColumnPageReader{
		data:        fileData,
		off:         startOff,
		endOff:      endOff,
		codec:       cm.Codec,
		physType:    cm.Type,
		typeLength:  typeLength,
		maxDefLevel: maxDefLevel,
		maxRepLevel: maxRepLevel,
	}
}

// NewColumnPageReaderAt creates a staged page reader: the column chunk's
// byte range [startOff, startOff+TotalCompressedSize), clamped to
// fileSize, is read from src into a pooled buffer on first use via one
// ranged read (pread on an *os.File). Decode then runs over heap bytes —
// no page faults inside decode goroutines, which is the point: a
// goroutine blocked in a read syscall parks at a GC-safe point, while
// one faulting on an mmap'd page stalls every STW in the process
// (docs/design/scan-pread-reads.md).
//
// The caller MUST Close the reader to return the buffer, and must not
// retain any decoded Values past Close.
func NewColumnPageReaderAt(src io.ReaderAt, fileSize int64, cm *ColumnMetaData, maxDefLevel, maxRepLevel int) *ColumnPageReader {
	startOff := cm.DataPageOffset
	if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < cm.DataPageOffset {
		startOff = cm.DictionaryPageOffset
	}
	endOff := startOff + cm.TotalCompressedSize
	if endOff > fileSize {
		endOff = fileSize
	}
	srcLen := 0
	if endOff > startOff {
		srcLen = int(endOff - startOff)
	}

	typeLength := 0 // FIXED_LEN_BYTE_ARRAY: caller sets via SetTypeLength

	return &ColumnPageReader{
		off:         0,
		endOff:      srcLen,
		codec:       cm.Codec,
		physType:    cm.Type,
		typeLength:  typeLength,
		maxDefLevel: maxDefLevel,
		maxRepLevel: maxRepLevel,
		src:         src,
		srcOff:      startOff,
		srcLen:      srcLen,
	}
}

// ensureData stages the chunk bytes in staged mode; a no-op in slice
// mode. Any read error surfaces to the NextPage/NextDictionary caller —
// a staged chunk must never silently read as empty.
func (r *ColumnPageReader) ensureData() error {
	if r.data != nil || r.src == nil || r.srcLen == 0 {
		return nil
	}
	buf := getChunkBuf(r.srcLen)
	if err := readAtFull(r.src, buf, r.srcOff); err != nil {
		putChunkBuf(buf)
		return fmt.Errorf("staging column chunk [%d, %d): %w", r.srcOff, r.srcOff+int64(r.srcLen), err)
	}
	preadChunks.Add(1)
	preadBytes.Add(int64(r.srcLen))
	r.data = buf
	r.owned = buf
	return nil
}

// SetTypeLength sets the type length for FIXED_LEN_BYTE_ARRAY columns.
func (r *ColumnPageReader) SetTypeLength(n int) {
	r.typeLength = n
}

// NextPage reads and decodes the next page. Returns nil at end of column.
func (r *ColumnPageReader) NextPage() (*PageData, error) {
	if err := r.ensureData(); err != nil {
		return nil, err
	}
	for r.off < r.endOff {
		ph, headerSize, err := DecodePageHeader(r.data[r.off:])
		if err != nil {
			return nil, fmt.Errorf("reading page header at offset %d: %w", r.off, err)
		}
		r.off += headerSize

		compressedData := r.data[r.off : r.off+int(ph.CompressedPageSize)]
		r.off += int(ph.CompressedPageSize)

		switch ph.Type {
		case PageDataV1:
			return r.decodeDataPageV1(ph, compressedData)
		case PageDataV2:
			return r.decodeDataPageV2(ph, compressedData)
		case PageDictionary:
			// Dictionary pages are handled separately via NextDictionary.
			// Skip for now — caller should call NextDictionary first.
			continue
		default:
			// Skip unknown page types (e.g., index pages).
			continue
		}
	}
	return nil, nil // end of column
}

// NextDictionary reads the dictionary page if present.
// Must be called before NextPage if the column uses dictionary encoding.
// Returns nil if no dictionary page exists.
func (r *ColumnPageReader) NextDictionary() (*DictionaryData, error) {
	if r.off >= r.endOff {
		return nil, nil
	}
	if err := r.ensureData(); err != nil {
		return nil, err
	}

	ph, headerSize, err := DecodePageHeader(r.data[r.off:])
	if err != nil {
		return nil, fmt.Errorf("reading page header: %w", err)
	}

	if ph.Type != PageDictionary {
		return nil, nil // not a dictionary page — rewind not needed since we didn't advance
	}

	r.off += headerSize
	compressedData := r.data[r.off : r.off+int(ph.CompressedPageSize)]
	r.off += int(ph.CompressedPageSize)

	// Decompress.
	pageData, err := Decompress(r.codec, compressedData, int(ph.UncompressedPageSize))
	if err != nil {
		return nil, fmt.Errorf("decompressing dictionary page: %w", err)
	}

	numValues := int(ph.DictionaryPageHeader.NumValues)
	vals := r.decodePlainValues(pageData, numValues)

	return &DictionaryData{NumValues: numValues, Data: vals}, nil
}

func (r *ColumnPageReader) decodeDataPageV1(ph *PageHeader, compressed []byte) (*PageData, error) {
	dph := ph.DataPageHeader
	if dph == nil {
		return nil, fmt.Errorf("data page v1 missing DataPageHeader")
	}

	// Decompress the entire page (levels + data are compressed together in v1).
	pageData, err := Decompress(r.codec, compressed, int(ph.UncompressedPageSize))
	if err != nil {
		return nil, fmt.Errorf("decompressing data page: %w", err)
	}

	numValues := int(dph.NumValues)
	off := 0

	// Decode repetition levels.
	var repLevels []int32
	if r.maxRepLevel > 0 {
		bitWidth := bitsRequired(r.maxRepLevel)
		decoded, consumed, err := DecodeRLEInt32WithLength(pageData[off:], bitWidth, numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding repetition levels: %w", err)
		}
		repLevels = decoded
		off += consumed
	}

	// Decode definition levels.
	var defLevels []int32
	numNulls := 0
	if r.maxDefLevel > 0 {
		bitWidth := bitsRequired(r.maxDefLevel)
		decoded, consumed, err := DecodeRLEInt32WithLength(pageData[off:], bitWidth, numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding definition levels: %w", err)
		}
		defLevels = decoded
		off += consumed

		// Count non-null values.
		for _, dl := range defLevels {
			if dl < int32(r.maxDefLevel) {
				numNulls++
			}
		}
	}

	// Remaining bytes are the encoded column data.
	valuesData := pageData[off:]
	nonNullCount := numValues - numNulls

	vals, err := r.decodeValues(valuesData, nonNullCount, dph.Encoding)
	if err != nil {
		// Return the pooled decompress buffer on the error path (mirrors the
		// v2 path); nothing references it once decode failed.
		ReleaseDecompressed(r.codec, pageData)
		return nil, fmt.Errorf("decoding v1 data: %w", err)
	}

	// Estimate numRows from numValues for flat schemas (no repetition levels).
	numRows := numValues

	return &PageData{
		NumValues:        numValues,
		NumRows:          numRows,
		NumNulls:         numNulls,
		Data:             vals,
		DefinitionLevels: defLevels,
		RepetitionLevels: repLevels,
		rawBuf:           pageData,
		codec:            r.codec,
	}, nil
}

func (r *ColumnPageReader) decodeDataPageV2(ph *PageHeader, compressed []byte) (*PageData, error) {
	dph := ph.DataPageHeaderV2
	if dph == nil {
		return nil, fmt.Errorf("data page v2 missing DataPageHeaderV2")
	}

	numValues := int(dph.NumValues)
	numRows := int(dph.NumRows)
	numNulls := int(dph.NumNulls)
	off := 0

	// In v2, repetition and definition levels are stored uncompressed
	// before the (optionally compressed) data section.

	// Decode repetition levels (uncompressed).
	var repLevels []int32
	repLen := int(dph.RepetitionLevelsByteLength)
	if repLen > 0 && r.maxRepLevel > 0 {
		bitWidth := bitsRequired(r.maxRepLevel)
		decoded, err := DecodeRLEInt32(compressed[off:off+repLen], bitWidth, numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding v2 repetition levels: %w", err)
		}
		repLevels = decoded
	}
	off += repLen

	// Decode definition levels (uncompressed).
	var defLevels []int32
	defLen := int(dph.DefinitionLevelsByteLength)
	if defLen > 0 && r.maxDefLevel > 0 {
		bitWidth := bitsRequired(r.maxDefLevel)
		decoded, err := DecodeRLEInt32(compressed[off:off+defLen], bitWidth, numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding v2 definition levels: %w", err)
		}
		defLevels = decoded
	}
	off += defLen

	// Decompress the data section (levels were NOT compressed in v2).
	dataSection := compressed[off:]
	var rawBuf []byte
	if dph.IsCompressed {
		decompressed, err := Decompress(r.codec, dataSection, int(ph.UncompressedPageSize)-repLen-defLen)
		if err != nil {
			return nil, fmt.Errorf("decompressing v2 data: %w", err)
		}
		dataSection = decompressed
		rawBuf = decompressed
	}

	nonNullCount := numValues - numNulls
	vals, err := r.decodeValues(dataSection, nonNullCount, dph.Encoding)
	if err != nil {
		ReleaseDecompressed(r.codec, rawBuf)
		return nil, fmt.Errorf("decoding v2 data: %w", err)
	}

	return &PageData{
		NumValues:        numValues,
		NumRows:          numRows,
		NumNulls:         numNulls,
		Data:             vals,
		DefinitionLevels: defLevels,
		RepetitionLevels: repLevels,
		rawBuf:           rawBuf,
		codec:            r.codec,
	}, nil
}

// decodeValues decodes column values using the specified encoding.
func (r *ColumnPageReader) decodeValues(data []byte, n int, enc Encoding) (Values, error) {
	switch enc {
	case EncodingPlain:
		return r.decodePlainValues(data, n), nil
	case EncodingRLEDictionary, EncodingPlainDictionary:
		if len(data) == 0 || n == 0 {
			return Values{physType: PhysicalInt32, count: 0}, nil
		}
		bitWidth := int(data[0])
		indices, err := DecodeRLEInt32(data[1:], bitWidth, n)
		if err != nil {
			return Values{}, fmt.Errorf("decoding dictionary indices: %w", err)
		}
		return PlainInt32Values(indices), nil
	case EncodingDeltaBinaryPacked:
		switch r.physType {
		case PhysicalInt32:
			return DecodeDeltaBinaryPackedInt32(data, n)
		case PhysicalInt64:
			return DecodeDeltaBinaryPackedInt64(data, n)
		default:
			return Values{}, fmt.Errorf("DELTA_BINARY_PACKED not supported for %s", r.physType)
		}
	case EncodingDeltaLengthByteArray:
		return DecodeDeltaLengthByteArray(data, n)
	case EncodingDeltaByteArray:
		return DecodeDeltaByteArray(data, n)
	case EncodingRLE:
		// RLE encoding for boolean columns.
		if r.physType == PhysicalBoolean {
			return DecodePlainBoolean(data, n), nil
		}
		return Values{}, fmt.Errorf("RLE encoding only supported for BOOLEAN, got %s", r.physType)
	default:
		return Values{}, fmt.Errorf("unsupported encoding: %s", enc)
	}
}

// decodePlainValues decodes PLAIN-encoded values based on the column's physical type.
func (r *ColumnPageReader) decodePlainValues(data []byte, n int) Values {
	if n == 0 {
		return Values{physType: r.physType}
	}
	switch r.physType {
	case PhysicalBoolean:
		return DecodePlainBoolean(data, n)
	case PhysicalInt32:
		return DecodePlainInt32(data, n)
	case PhysicalInt64:
		return DecodePlainInt64(data, n)
	case PhysicalFloat:
		return DecodePlainFloat(data, n)
	case PhysicalDouble:
		return DecodePlainDouble(data, n)
	case PhysicalByteArray:
		return DecodePlainByteArray(data, n)
	case PhysicalFixedLenByteArray:
		return DecodePlainFixedLenByteArray(data, n, r.typeLength)
	case PhysicalInt96:
		// INT96: 12 bytes per value, treat as fixed-length byte array.
		return DecodePlainFixedLenByteArray(data, n, 12)
	default:
		return Values{physType: r.physType}
	}
}

// bitsRequired returns the number of bits needed to represent value v.
func bitsRequired(v int) int {
	if v == 0 {
		return 0
	}
	bits := 0
	for v > 0 {
		bits++
		v >>= 1
	}
	return bits
}

// Close releases resources. Slice mode is a no-op (the data belongs to
// the caller); staged mode returns the pooled chunk buffer — after which
// no decoded Values from this reader may be used (uncompressed pages
// alias the buffer).
func (r *ColumnPageReader) Close() error {
	if r.owned != nil {
		putChunkBuf(r.owned)
		r.owned = nil
		r.data = nil
	}
	return nil
}

// Ensure ColumnPageReader satisfies io.Closer.
var _ io.Closer = (*ColumnPageReader)(nil)
