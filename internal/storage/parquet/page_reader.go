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
	Skipped          bool // payload never decompressed (NextPageMaybeSkip); only NumValues is meaningful
	Data             Values   // decoded column values (non-null values only)
	Encoding         Encoding // value encoding of THIS page (pages in one chunk can differ)
	DefinitionLevels []int32  // nil if column is required (no nulls)
	RepetitionLevels []int32  // nil for flat schemas (no nesting)

	// DictIndexRLE is the page's raw RLE/bit-packing hybrid index payload,
	// set only when the reader is in deferred-index mode
	// (DeferDictIndices) and this page is dictionary-encoded. Data is
	// empty in that case: the indices have NOT been expanded. Walk the
	// payload with RLERunIterator, or expand it with
	// ColumnPageReader.DecodeDeferredIndices.
	//
	// Aliases the page buffer with the same lifetime as Data — invalid
	// after Release.
	DictIndexRLE      []byte
	DictIndexBitWidth int

	// NullsFromLevels reports that NumNulls was COUNTED from the decoded
	// definition levels rather than taken from the page header. A consumer
	// that wants to conclude "no nulls, so value i belongs to row i" needs
	// that distinction: a v2 header's null count is the writer's claim
	// about the levels, not a fact derived from them.
	NullsFromLevels bool

	// rawBuf is the decompressed page buffer that backs Data when the page
	// values alias the decompress output (PLAIN-encoded numeric/fixed-len
	// columns). nil when no pooled buffer needs to be returned (uncompressed
	// pages, dictionary-indexed pages, or codecs without a buffer pool).
	rawBuf []byte
	codec  CompressionCodec
}

// IsDictEncoded reports whether this page's values are dictionary indices
// that must be resolved through the chunk's dictionary page. A chunk whose
// dictionary page overflowed the writer's size limit mixes dictionary-encoded
// and PLAIN pages, so resolution must be decided per page, never per chunk.
func (p *PageData) IsDictEncoded() bool {
	return p.Encoding == EncodingPlainDictionary || p.Encoding == EncodingRLEDictionary
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

	// Opt-in per-page buffer reuse (EnableScratch): definition-level and
	// dictionary-index slices are reused across NextPage calls, so a
	// returned PageData's DefinitionLevels/Data are INVALIDATED by the
	// next NextPage. Only callers that fully consume each page before
	// advancing (the native columnar reader) may enable this.
	scratchOn  bool
	defScratch []int32
	idxScratch []int32

	// Construction-time refusal. Every offset and length below is a number
	// out of the FOOTER — the file's own claim about where its bytes are —
	// and a column chunk whose claims do not fit the file cannot be read at
	// all. ColumnPages has no error return, so the refusal is carried here
	// and handed to the first NextPage/NextDictionary/DictionaryIfPure call.
	// Returning a reader that quietly yields no pages instead would answer
	// the query with an empty column, which is a different answer, given
	// without saying so.
	openErr error

	// Deferred dictionary indices (DeferDictIndices): dictionary-encoded
	// data pages keep their raw RLE payload on PageData instead of
	// expanding it to one int32 per value. pendingIdx* carry the payload
	// out of decodeValues to the PageData constructor.
	deferDictIdx  bool
	pendingIdxRLE []byte
	pendingIdxBW  int

	// Row budget (SetRowBudget): how many rows the ROW GROUP says this
	// chunk holds, and how many the pages walked so far have claimed. Zero
	// means the caller did not say, and nothing is enforced. See chargeRows.
	rowBudget int
	rowsSeen  int
}

// SetRowBudget tells the reader how many rows the row group holds, so a page
// header cannot claim more than the file elsewhere says exist. FileReader
// sets it for every reader it hands out; see chargeRows for what it buys.
func (r *ColumnPageReader) SetRowBudget(rows int) { r.rowBudget = rows }

// chargeRows holds a data page's declared value count to the rows the row
// group has left.
//
// num_values is a thrift i32 that decodeDataPageV1/V2 size an allocation from
// before anything looks at the page body: one int32 per value for the
// definition levels, and another per value for the dictionary indices. The
// header bound (MaxPageValues) leaves that at 64 MiB per column, and the scan
// fans out per column, so a twenty-byte page header still buys a lot of
// memory per corrupt file.
//
// The exact bound is the row group's own row count, and for a FLAT leaf it is
// exact: one value per row, and the chunk's pages sum to the row group's
// rows. Nested leaves have more values than rows — that is what repetition
// levels are for — so they keep only the header bound.
func (r *ColumnPageReader) chargeRows(ph *PageHeader) error {
	if r.rowBudget <= 0 || r.maxRepLevel > 0 {
		return nil
	}
	var n int
	switch {
	case ph.DataPageHeader != nil:
		n = int(ph.DataPageHeader.NumValues)
	case ph.DataPageHeaderV2 != nil:
		n = int(ph.DataPageHeaderV2.NumValues)
	default:
		return nil
	}
	if left := r.rowBudget - r.rowsSeen; n > left {
		return fmt.Errorf("page declares %d values but the row group has %d of its %d rows left",
			n, left, r.rowBudget)
	}
	r.rowsSeen += n
	return nil
}

// DeferDictIndices stops dictionary-encoded data pages from expanding
// their index stream: NextPage leaves PageData.Data empty and hands back
// the raw payload as PageData.DictIndexRLE instead. Only for callers that
// can consume runs (scan-level predicate evaluation); everyone else wants
// the default. Definition levels are still decoded normally.
func (r *ColumnPageReader) DeferDictIndices() { r.deferDictIdx = true }

// DecodeDeferredIndices expands a deferred dictionary-index payload into
// the reader's scratch buffer, giving the caller the exact slice NextPage
// would have produced without the deferral. Returns nil for pages with no
// deferred payload (PLAIN pages, all-null pages, deferral off).
func (r *ColumnPageReader) DecodeDeferredIndices(p *PageData) ([]int32, error) {
	if p == nil || p.DictIndexRLE == nil {
		return nil, nil
	}
	n := p.NumValues - p.NumNulls
	if n < 0 {
		return nil, fmt.Errorf("page reports %d nulls of %d values", p.NumNulls, p.NumValues)
	}
	var scratch []int32
	if r.scratchOn {
		scratch = r.idxScratch
	}
	indices, err := DecodeRLEInt32Into(scratch, p.DictIndexRLE, p.DictIndexBitWidth, n)
	if err != nil {
		return nil, fmt.Errorf("decoding dictionary indices: %w", err)
	}
	if r.scratchOn {
		r.idxScratch = indices
	}
	return indices, nil
}

// EnableScratch turns on per-page buffer reuse for this reader. See the
// field comment for the lifetime contract.
func (r *ColumnPageReader) EnableScratch() { r.scratchOn = true }

// SeedScratch enables buffer reuse AND seeds it with buffers pooled by
// the caller across readers — chunks with one or two large pages get no
// within-reader reuse, so cross-reader pooling is where the allocation
// win is. Retrieve the (possibly grown) buffers with TakeScratch before
// Close and return them to the caller's pool.
func (r *ColumnPageReader) SeedScratch(def, idx []int32) {
	r.scratchOn = true
	r.defScratch = def
	r.idxScratch = idx
}

// TakeScratch hands back the scratch buffers for caller-side pooling.
func (r *ColumnPageReader) TakeScratch() (def, idx []int32) {
	def, idx = r.defScratch, r.idxScratch
	r.defScratch, r.idxScratch = nil, nil
	return def, idx
}

// maxPageBodyBytes bounds what one page HEADER may claim about the size of
// its own decompressed body. UncompressedPageSize is a thrift i32 the
// decompressors pre-allocate from (`make([]byte, 0, size)`), so an
// unvalidated header could ask for two gigabytes per page out of a file a
// few hundred bytes long. A gibibyte is orders of magnitude above any page a
// writer produces and still small enough that a corrupt file cannot exhaust
// the process before the read fails.
const maxPageBodyBytes = 1 << 30

// chunkRange turns a column chunk's footer offsets into a byte range inside
// the file, refusing every claim the file cannot back.
//
// Nothing validated these before. DataPageOffset and DictionaryPageOffset are
// signed 64-bit thrift fields read straight out of the footer, and a negative
// one became a negative slice index at the very first read: `r.data[r.off:]`
// with r.off = -9025. Five of the six crashers the whole-file mutation fuzz
// found were exactly that, and the file only has to be off by one flipped
// byte in a varint to get there.
//
// The END used to be CLAMPED to the file rather than refused, on the belief
// that writers round TotalCompressedSize up. They do not. Every column chunk
// in 44 files written by pyarrow (four codecs, both format versions, with and
// without the page index), by parquet-go and by wadjet's own writer ends
// EXACTLY where the next one begins — worst inter-chunk gap zero, worst
// overlap zero — because the field is the sum of the chunk's page sizes,
// headers included. Clamping therefore bought nothing and cost a silent wrong
// answer: an overstated size reaches into the NEXT column's bytes, the page
// loop decodes them as this column's, and a 128-row chunk comes back with 64
// of its own values and 64 belonging to a neighbour, err == nil. An
// overstatement is now refused, here and (against its neighbours, which one
// chunk's metadata cannot see) in ValidateChunkLayout at open.
func chunkRange(cm *ColumnMetaData, fileSize int64) (start, end int64, err error) {
	start = cm.DataPageOffset
	if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < cm.DataPageOffset {
		start = cm.DictionaryPageOffset
	}
	end = start + cm.TotalCompressedSize
	// One test on the happy path, and the six messages in a frame of their
	// own. Building them here instead reserved 144 bytes of stack in a
	// function every column read calls before it has done anything, which
	// moved the scan goroutine's first stack growth to a deeper point and
	// cost ~3% of BenchmarkReadColumnar/rows=1000. See decodeOnePage.
	if fileSize < 0 || cm.DataPageOffset < 0 || cm.DictionaryPageOffset < 0 ||
		cm.TotalCompressedSize < 0 || start > fileSize || end > fileSize || end < start {
		return 0, 0, chunkRangeErr(cm, fileSize, start, end)
	}
	return start, end, nil
}

//go:noinline
func chunkRangeErr(cm *ColumnMetaData, fileSize, start, end int64) error {
	switch {
	case fileSize < 0:
		return fmt.Errorf("column chunk: file size %d is negative", fileSize)
	case cm.DataPageOffset < 0:
		return fmt.Errorf("column chunk: data_page_offset %d is negative", cm.DataPageOffset)
	case cm.DictionaryPageOffset < 0:
		return fmt.Errorf("column chunk: dictionary_page_offset %d is negative", cm.DictionaryPageOffset)
	case cm.TotalCompressedSize < 0:
		return fmt.Errorf("column chunk: total_compressed_size %d is negative", cm.TotalCompressedSize)
	case start > fileSize:
		return fmt.Errorf("column chunk starts at offset %d, past the end of a %d-byte file", start, fileSize)
	case end < start:
		return fmt.Errorf("column chunk at offset %d declares a total_compressed_size of %d, which overflows",
			start, cm.TotalCompressedSize)
	default:
		return fmt.Errorf("column chunk at offset %d declares %d compressed bytes, ending at %d, "+
			"past the end of a %d-byte file", start, cm.TotalCompressedSize, end, fileSize)
	}
}

// pageBody returns the compressed body of the page whose header ended at
// off, and the offset just past it. Both numbers come from the page header,
// which is thrift the reader trusted enough to parse and nothing more:
// CompressedPageSize is an i32 that may be negative or may run past the end
// of the chunk, and `r.data[r.off : r.off+int(ph.CompressedPageSize)]` was
// taking it at its word.
func (r *ColumnPageReader) pageBody(off int, ph *PageHeader) ([]byte, int, error) {
	size := int(ph.CompressedPageSize)
	if size < 0 || off > r.endOff || size > r.endOff-off {
		return nil, 0, fmt.Errorf("page at offset %d declares a %d-byte body but the chunk ends at %d",
			off, ph.CompressedPageSize, r.endOff)
	}
	if u := int(ph.UncompressedPageSize); u < 0 || u > maxPageBodyBytes {
		return nil, 0, fmt.Errorf("page at offset %d declares an uncompressed size of %d bytes",
			off, ph.UncompressedPageSize)
	}
	return r.data[off : off+size : off+size], off + size, nil
}

// nextHeader decodes the page header at off and returns it with the offset
// its body starts at. A header that does not fit inside the chunk is refused
// rather than advanced past.
func (r *ColumnPageReader) nextHeader(off int) (*PageHeader, int, error) {
	if off < 0 || off > len(r.data) {
		return nil, 0, fmt.Errorf("page offset %d is outside the %d bytes staged for this chunk", off, len(r.data))
	}
	ph, headerSize, err := DecodePageHeader(r.data[off:])
	if err != nil {
		return nil, 0, fmt.Errorf("reading page header at offset %d: %w", off, err)
	}
	if headerSize <= 0 || headerSize > r.endOff-off {
		return nil, 0, fmt.Errorf("page header at offset %d is %d bytes but the chunk ends at %d",
			off, headerSize, r.endOff)
	}
	return ph, off + headerSize, nil
}

// NewColumnPageReader creates a page reader for a column chunk.
//
// Parameters:
//   - fileData: the entire file bytes (or the relevant region)
//   - cm: column metadata from the footer
//   - maxDefLevel: maximum definition level (0 for required columns)
//   - maxRepLevel: maximum repetition level (0 for flat schemas)
func NewColumnPageReader(fileData []byte, cm *ColumnMetaData, maxDefLevel, maxRepLevel int) *ColumnPageReader {
	start, end, err := chunkRange(cm, int64(len(fileData)))
	if err != nil {
		return &ColumnPageReader{openErr: err, codec: cm.Codec, physType: cm.Type,
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel}
	}
	startOff, endOff := int(start), int(end)

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
	startOff, endOff, err := chunkRange(cm, fileSize)
	if err != nil {
		return &ColumnPageReader{openErr: err, codec: cm.Codec, physType: cm.Type,
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel}
	}
	srcLen := int(endOff - startOff)

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
	if r.openErr != nil {
		return r.openErr
	}
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
	return r.NextPageMaybeSkip(nil)
}

// NextPageMaybeSkip is NextPage with an optional pre-decompression skip:
// for each data page, shouldSkip is consulted with the page's row count
// (from the page HEADER — a tiny thrift parse, the same walk
// DictionaryIfPure does) BEFORE the payload is decompressed. When it
// returns true the payload is bypassed entirely and a PageData with
// Skipped=true and only NumValues set is returned — the caller must
// account for those rows itself. Only meaningful for flat columns
// (MaxRepLevel 0), where header NumValues equals the row count; callers
// gate on that. A nil shouldSkip is exactly NextPage.
func (r *ColumnPageReader) NextPageMaybeSkip(shouldSkip func(numRows int) bool) (*PageData, error) {
	if err := r.ensureData(); err != nil {
		return nil, err
	}
	for r.off < r.endOff {
		ph, bodyOff, err := r.nextHeader(r.off)
		if err != nil {
			return nil, err
		}
		compressedData, next, err := r.pageBody(bodyOff, ph)
		if err != nil {
			return nil, err
		}
		if err := r.chargeRows(ph); err != nil {
			return nil, err
		}
		r.off = next

		switch ph.Type {
		case PageDataV1:
			if shouldSkip != nil && ph.DataPageHeader != nil &&
				ph.DataPageHeader.NumValues > 0 && shouldSkip(int(ph.DataPageHeader.NumValues)) {
				return &PageData{NumValues: int(ph.DataPageHeader.NumValues), Skipped: true}, nil
			}
			return r.decodeDataPageV1(ph, compressedData)
		case PageDataV2:
			if shouldSkip != nil && ph.DataPageHeaderV2 != nil &&
				ph.DataPageHeaderV2.NumValues > 0 && shouldSkip(int(ph.DataPageHeaderV2.NumValues)) {
				return &PageData{NumValues: int(ph.DataPageHeaderV2.NumValues), Skipped: true}, nil
			}
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

	ph, bodyOff, err := r.nextHeader(r.off)
	if err != nil {
		return nil, err
	}

	if ph.Type != PageDictionary {
		return nil, nil // not a dictionary page — rewind not needed since we didn't advance
	}

	compressedData, next, err := r.pageBody(bodyOff, ph)
	if err != nil {
		return nil, err
	}
	r.off = next

	// Decompress.
	pageData, err := Decompress(r.codec, compressedData, int(ph.UncompressedPageSize))
	if err != nil {
		return nil, fmt.Errorf("decompressing dictionary page: %w", err)
	}

	if ph.DictionaryPageHeader == nil {
		return nil, fmt.Errorf("dictionary page has no DictionaryPageHeader")
	}
	numValues := int(ph.DictionaryPageHeader.NumValues)
	vals, err := r.decodePlainValues(pageData, numValues)
	if err != nil {
		return nil, fmt.Errorf("decoding dictionary page: %w", err)
	}

	return &DictionaryData{NumValues: numValues, Data: vals}, nil
}

// DictionaryIfPure walks the chunk's PAGE HEADERS (tiny thrift parses,
// no data-page decompression or decode) and returns the decoded
// dictionary when EVERY data page is dictionary-encoded. ok=false means
// the chunk is not provably pure-dictionary — either no dictionary page,
// a PLAIN fallback data page (writer dictionary overflow, the mixed-
// encoding class from the 2026-08 dict-fallback bug), or an unknown page
// type — and the caller MUST NOT draw any conclusion from the dictionary.
//
// The chunk-metadata Encodings list cannot answer this: writers list the
// dictionary PAGE's PLAIN encoding there too, so PLAIN-in-the-list is
// ambiguous between "has a dict page" and "has fallback data pages".
// Walking the actual headers is unambiguous.
//
// Must be called on a fresh reader (before NextDictionary/NextPage); the
// reader's position is not advanced. Returned dictionary Values may
// alias the reader's staged buffer — consume them before Close.
func (r *ColumnPageReader) DictionaryIfPure() (*DictionaryData, bool, error) {
	if err := r.ensureData(); err != nil {
		return nil, false, err
	}
	var dict *DictionaryData
	off := r.off
	for off < r.endOff {
		ph, bodyOff, err := r.nextHeader(off)
		if err != nil {
			return nil, false, err
		}
		body, next, err := r.pageBody(bodyOff, ph)
		if err != nil {
			return nil, false, err
		}
		off = next

		switch ph.Type {
		case PageDictionary:
			if dict != nil {
				return nil, false, nil // second dictionary page: malformed, be conservative
			}
			pageData, err := Decompress(r.codec, body, int(ph.UncompressedPageSize))
			if err != nil {
				return nil, false, fmt.Errorf("decompressing dictionary page: %w", err)
			}
			if ph.DictionaryPageHeader == nil {
				return nil, false, fmt.Errorf("dictionary page at offset %d has no DictionaryPageHeader", off)
			}
			n := int(ph.DictionaryPageHeader.NumValues)
			vals, err := r.decodePlainValues(pageData, n)
			if err != nil {
				return nil, false, fmt.Errorf("decoding dictionary page: %w", err)
			}
			dict = &DictionaryData{NumValues: n, Data: vals}
		case PageDataV1:
			if ph.DataPageHeader == nil {
				return nil, false, nil
			}
			if e := ph.DataPageHeader.Encoding; e != EncodingPlainDictionary && e != EncodingRLEDictionary {
				return nil, false, nil
			}
		case PageDataV2:
			if ph.DataPageHeaderV2 == nil {
				return nil, false, nil
			}
			if e := ph.DataPageHeaderV2.Encoding; e != EncodingPlainDictionary && e != EncodingRLEDictionary {
				return nil, false, nil
			}
		default:
			return nil, false, nil // unknown page type: conservative
		}
	}
	if dict == nil {
		return nil, false, nil
	}
	return dict, true, nil
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
		var scratch []int32
		if r.scratchOn {
			scratch = r.defScratch
		}
		decoded, consumed, err := DecodeRLEInt32WithLengthInto(scratch, pageData[off:], bitWidth, numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding definition levels: %w", err)
		}
		if r.scratchOn {
			r.defScratch = decoded
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
		NumValues:         numValues,
		NumRows:           numRows,
		NumNulls:          numNulls,
		Data:              vals,
		Encoding:          dph.Encoding,
		DefinitionLevels:  defLevels,
		RepetitionLevels:  repLevels,
		DictIndexRLE:      r.pendingIdxRLE,
		DictIndexBitWidth: r.pendingIdxBW,
		// v1 has no null count in its header: numNulls above is counted
		// from the levels this page actually carries.
		NullsFromLevels: true,
		rawBuf:          pageData,
		codec:           r.codec,
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
		var scratch []int32
		if r.scratchOn {
			scratch = r.defScratch
		}
		decoded, err := DecodeRLEInt32Into(scratch, compressed[off:off+defLen], bitWidth, numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding v2 definition levels: %w", err)
		}
		if r.scratchOn {
			r.defScratch = decoded
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
		NumValues:         numValues,
		NumRows:           numRows,
		NumNulls:          numNulls,
		Data:              vals,
		Encoding:          dph.Encoding,
		DefinitionLevels:  defLevels,
		RepetitionLevels:  repLevels,
		DictIndexRLE:      r.pendingIdxRLE,
		DictIndexBitWidth: r.pendingIdxBW,
		// v2 carries num_nulls in its header; it is the writer's claim
		// about the levels, not something derived from them. Only a page
		// with no levels at all is self-evidently null-free.
		NullsFromLevels: defLevels == nil,
		rawBuf:          rawBuf,
		codec:           r.codec,
	}, nil
}

// decodeValues decodes column values using the specified encoding.
func (r *ColumnPageReader) decodeValues(data []byte, n int, enc Encoding) (Values, error) {
	r.pendingIdxRLE, r.pendingIdxBW = nil, 0
	switch enc {
	case EncodingPlain:
		return r.decodePlainValues(data, n)
	case EncodingRLEDictionary, EncodingPlainDictionary:
		if len(data) == 0 || n == 0 {
			return Values{physType: PhysicalInt32, count: 0}, nil
		}
		bitWidth := int(data[0])
		if r.deferDictIdx {
			// Hand the raw index stream to the caller instead of
			// expanding it (see DeferDictIndices).
			r.pendingIdxRLE, r.pendingIdxBW = data[1:], bitWidth
			return Values{physType: PhysicalInt32, count: 0}, nil
		}
		var scratch []int32
		if r.scratchOn {
			scratch = r.idxScratch
		}
		indices, err := DecodeRLEInt32Into(scratch, data[1:], bitWidth, n)
		if err != nil {
			return Values{}, fmt.Errorf("decoding dictionary indices: %w", err)
		}
		if r.scratchOn {
			r.idxScratch = indices
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
			return DecodePlainBoolean(data, n)
		}
		return Values{}, fmt.Errorf("RLE encoding only supported for BOOLEAN, got %s", r.physType)
	default:
		return Values{}, fmt.Errorf("unsupported encoding: %s", enc)
	}
}

// decodePlainValues decodes PLAIN-encoded values based on the column's physical type.
//
// Every arm can fail: n is the page header's claim about a body whose length
// the header does not control. A page that claims more values than it carries
// is a corrupt (or hostile) file, and it is reported as one.
func (r *ColumnPageReader) decodePlainValues(data []byte, n int) (Values, error) {
	if n == 0 {
		return Values{physType: r.physType}, nil
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
		return Values{physType: r.physType}, nil
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
