package parquet

import (
	"fmt"
	"hash/crc32"
	"io"
	"strings"
)

// PageData holds the decoded contents of a single Parquet data page.
type PageData struct {
	NumValues        int
	NumRows          int
	NumNulls         int
	Skipped          bool     // payload never decompressed (NextPageMaybeSkip); only NumValues is meaningful
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
	data        []byte // raw column chunk bytes
	off         int    // current read position
	endOff      int    // end of column data
	codec       CompressionCodec
	physType    PhysicalType
	typeLength  int // for FIXED_LEN_BYTE_ARRAY
	maxDefLevel int // 0 if column is required
	maxRepLevel int // 0 for flat schemas

	// path is the chunk's column path out of the footer, carried only so a
	// refusal can name the column it is about. A reader's caller knows a
	// leaf INDEX; a person reading the error wants the name.
	path []string

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
	// means the caller did not say, and nothing is enforced. See chargeRows
	// for the per-page upper bound and checkColumnComplete for the
	// end-of-column reconciliation.
	//
	// rowsSeen counts a FLAT leaf's rows, charged from the page header
	// before decode (one value per row, skipped pages included). nestedRows
	// counts a nested leaf's rows the only way they can be counted — from
	// the repetition levels, after decode, one row per level-0 entry.
	rowBudget  int
	rowsSeen   int
	nestedRows int
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

// noteRows records a NESTED leaf's delivered rows. A nested leaf has more
// values than rows, so its header value counts say nothing about rows; the
// row boundaries are the repetition levels, one row per level-0 entry, and
// they are only knowable after the page is decoded. Flat leaves are already
// counted by chargeRows, before decode, and are not counted twice here.
func (r *ColumnPageReader) noteRows(p *PageData) {
	if r.rowBudget <= 0 || r.maxRepLevel == 0 || p == nil {
		return
	}
	r.nestedRows += p.NumRows
}

// checkColumnComplete reconciles what a chunk DELIVERED against what the row
// group says it holds, at the point the chunk runs out of pages.
//
// chargeRows already refuses a chunk that claims MORE rows than the row group
// has. The other direction was not checked at all, and it is the one a
// truncation produces: the page loop reaches the end of the chunk's byte
// range, returns a clean EOF, and every row the chunk never delivered is left
// at whatever the destination was allocated as — a NULL. A required INT64
// column of [11, 22] whose chunk length was cut to its first page read
// [{x:11}, {}] with a nil error (#892). The row group, the chunk metadata and
// the schema all said there were two non-null values; the reader invented the
// second's absence rather than saying the file could not supply it.
//
// This is checked once per chunk, when the pages are exhausted. A reader the
// caller abandons early is not reconciled — the caller asked for fewer pages,
// which is not the file contradicting itself. A page the caller SKIPS is
// counted, because chargeRows charges it from the header before the skip
// decision: a skipped page's rows are accounted for, not missing.
func (r *ColumnPageReader) checkColumnComplete() error {
	if r.rowBudget <= 0 {
		return nil
	}
	got := r.rowsSeen
	if r.maxRepLevel > 0 {
		got = r.nestedRows
	}
	if got == r.rowBudget {
		return nil
	}
	return columnShortErr(r.columnLabel(), got, r.rowBudget)
}

//go:noinline
func dictionaryPageInDataWalkErr(col string, off int) error {
	return fmt.Errorf("column %s: a dictionary page appears at offset %d in the data-page walk, "+
		"where a dictionary page cannot be (it is consumed only as a chunk's first page); "+
		"its rows were charged but produced no values (corrupt or contradictory parquet metadata)", col, off)
}

//go:noinline
func unknownPageTypeErr(col string, off int, t PageType) error {
	return fmt.Errorf("column %s: the page at offset %d declares %v, which this reader "+
		"cannot decode; its rows cannot be accounted for", col, off, t)
}

//go:noinline
func columnShortErr(col string, got, want int) error {
	verb := "delivers only"
	if got > want {
		verb = "delivers"
	}
	return fmt.Errorf("column %s: the row group holds %d rows but its column chunk %s %d "+
		"(the chunk ends before its declared rows: truncated data or contradictory metadata)",
		col, want, verb, got)
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

// chunkRange validates footer offsets and compressed size before slicing.
// Start at an earlier positive dictionary offset, else DataPageOffset.
// Reject negative file/offset/size, overflowing ranges and ends beyond the file;
// never clamp an overstated size into a plausible shorter chunk.
// ValidateChunkLayout separately checks neighbours and footer boundaries,
// which one chunk's metadata cannot establish.
// See docs/internals/parquet-column-chunk-byte-range.md for the design.
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

// columnLabel names the chunk in a refusal. The footer's path is
// dot-joined, the way every other parquet tool prints a leaf.
func (r *ColumnPageReader) columnLabel() string {
	if len(r.path) == 0 {
		return "?"
	}
	return strings.Join(r.path, ".")
}

// verifyPageCRC checks CRCSet PRESENCE, including checksum zero (#891).
// Use IEEE CRC-32 over stored compressed body bytes, excluding the header
// and including uncompressed v2 level sections.
// Check every decoded v1/v2/dictionary body on both read paths, including
// dictionaries used by DictionaryIfPure pruning.
// Do not touch/check NextPageMaybeSkip payloads: they produce no values.
// A corrupt skipped page may read clean while a whole-file read refuses;
// gates must cover both sides.
// See docs/internals/parquet-page-crc-check-boundary.md for the design.
func (r *ColumnPageReader) verifyPageCRC(off int, ph *PageHeader, body []byte) error {
	if !ph.CRCSet {
		return nil
	}
	if got := crc32.ChecksumIEEE(body); got != uint32(ph.CRC) {
		return pageCRCErr(r.columnLabel(), off, ph, got)
	}
	return nil
}

// pageCRCErr names the column, the page and both checksums. The offset is
// the page BODY's, in the same frame the reader is reading in: file-absolute
// in slice mode, chunk-relative in staged mode.
//
//go:noinline
func pageCRCErr(col string, off int, ph *PageHeader, got uint32) error {
	return fmt.Errorf("column %s: the %v at offset %d fails its own checksum: "+
		"header declares crc32 %#08x, the %d stored bytes hash to %#08x (corrupt parquet page)",
		col, ph.Type, off, uint32(ph.CRC), ph.CompressedPageSize, got)
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
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel, path: cm.PathInSchema}
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
		path:        cm.PathInSchema,
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
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel, path: cm.PathInSchema}
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
		path:        cm.PathInSchema,
		src:         src,
		srcOff:      startOff,
		srcLen:      srcLen,
	}
}

// NewColumnPageReaderIn is slice mode over a buffer holding only PART of the
// file: buf covers file bytes [base, base+len(buf)), and the chunk's range is
// translated into it. Used by row-group mode (rowgroup_bytes.go), where the
// file's bytes are held one row group at a time so each row group's memory is
// freed as soon as it has been decoded.
//
// Page access is the same zero-copy slicing the whole-file form does — no
// staging copy, no pooled chunk buffer, nothing to Close. A chunk whose range
// is not entirely inside buf is REFUSED: decoding it would read a neighbour's
// bytes as this column's values, which is the silent-wrong-answer shape
// chunkRange and ValidateChunkLayout exist to prevent.
func NewColumnPageReaderIn(buf []byte, base, fileSize int64, cm *ColumnMetaData, maxDefLevel, maxRepLevel int) *ColumnPageReader {
	start, end, err := chunkRange(cm, fileSize)
	if err != nil {
		return &ColumnPageReader{openErr: err, codec: cm.Codec, physType: cm.Type,
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel, path: cm.PathInSchema}
	}
	if end == start {
		// An EMPTY chunk has no bytes to read and its offset is whatever the
		// writer happened to leave behind — chunkExtent skips it for exactly
		// that reason, so it is not inside any row group's range and must not
		// be measured against one. A reader over no bytes yields no pages,
		// which is what the whole-file form does with the same metadata.
		return &ColumnPageReader{
			codec: cm.Codec, physType: cm.Type,
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel,
			path: cm.PathInSchema,
		}
	}
	off, endOff := start-base, end-base
	if base < 0 || off < 0 || endOff < off || endOff > int64(len(buf)) {
		return &ColumnPageReader{
			openErr: fmt.Errorf("column chunk spans [%d, %d), which is not inside the "+
				"%d bytes this row group holds at offset %d", start, end, len(buf), base),
			codec: cm.Codec, physType: cm.Type,
			maxDefLevel: maxDefLevel, maxRepLevel: maxRepLevel,
			path: cm.PathInSchema,
		}
	}
	return &ColumnPageReader{
		data:        buf,
		off:         int(off),
		endOff:      int(endOff),
		codec:       cm.Codec,
		physType:    cm.Type,
		maxDefLevel: maxDefLevel,
		maxRepLevel: maxRepLevel,
		path:        cm.PathInSchema,
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
			if err := r.verifyPageCRC(bodyOff, ph, compressedData); err != nil {
				return nil, err
			}
			p, err := r.decodeDataPageV1(ph, compressedData)
			if err != nil {
				return nil, err
			}
			r.noteRows(p)
			return p, nil
		case PageDataV2:
			if shouldSkip != nil && ph.DataPageHeaderV2 != nil &&
				ph.DataPageHeaderV2.NumValues > 0 && shouldSkip(int(ph.DataPageHeaderV2.NumValues)) {
				return &PageData{NumValues: int(ph.DataPageHeaderV2.NumValues), Skipped: true}, nil
			}
			if err := r.verifyPageCRC(bodyOff, ph, compressedData); err != nil {
				return nil, err
			}
			p, err := r.decodeDataPageV2(ph, compressedData)
			if err != nil {
				return nil, err
			}
			r.noteRows(p)
			return p, nil
		case PageDictionary:
			// A dictionary page belongs at the START of a column chunk and is
			// consumed by NextDictionary before this data-page walk begins —
			// every caller does exactly that (row reader, native scan, the
			// sel/lengths decodes, the row filter). Reaching one HERE is a
			// dictionary page in an impossible position: a second dictionary
			// page, or a data page whose top-level type field was relabeled
			// DICTIONARY_PAGE while it kept its data-page header and body.
			// chargeRows above has ALREADY charged this page's rows from that
			// surviving header, so the old `continue` accounted for rows whose
			// values were never produced and shifted every later page's values
			// onto this one's offsets: 300 required INT64 rows read back as
			// [128..299] then NULLs, nil error (#924, the residual of #907's
			// unknown-type refusal). The page CRC cannot catch it — crc covers
			// the body, not the header type — so the disposition has to.
			return nil, dictionaryPageInDataWalkErr(r.columnLabel(), bodyOff)
		default:
			// A page type this reader does not decode, in the middle of a
			// column chunk. This used to `continue`, and that was a silent
			// wrong answer: the page's rows are already CHARGED by
			// chargeRows above, so the chunk still reconciled against the
			// row group while the values of a whole page were never
			// produced — and every later page's values landed at the
			// skipped page's offsets. Found by the header bit-flip cell:
			// flipping one bit of the page header's `type` field turned a
			// DATA_PAGE into type -1, and a required INT64 column of
			// [0..299] read back as [128..299] followed by NULLs, nil
			// error. The checksum cannot catch it — crc covers the page
			// BODY, not the header — so the disposition has to.
			return nil, unknownPageTypeErr(r.columnLabel(), bodyOff, ph.Type)
		}
	}
	// End of column: the chunk has no more pages. Before reporting that as a
	// clean stop, the rows it delivered have to be the rows the row group
	// says it holds.
	if err := r.checkColumnComplete(); err != nil {
		return nil, err
	}
	return nil, nil
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

	if err := r.verifyPageCRC(bodyOff, ph, compressedData); err != nil {
		return nil, err
	}

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
	sawData := false
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
			// A dictionary page is only valid as the chunk's FIRST page, the
			// same placement the ordinary reader now enforces
			// (dictionaryPageInDataWalkErr). A second one, or one that follows a
			// data page, is a malformed chunk: decline to prune rather than draw
			// a conclusion from a dictionary in an impossible position (#924).
			if dict != nil || sawData {
				return nil, false, nil
			}
			// A row group is PRUNED from these values. A corrupt dictionary
			// prunes rows that belong in the answer, so this page's checksum
			// is verified exactly as the decoding walk verifies it.
			if err := r.verifyPageCRC(bodyOff, ph, body); err != nil {
				return nil, false, err
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
			sawData = true
			if ph.DataPageHeader == nil {
				return nil, false, nil
			}
			if e := ph.DataPageHeader.Encoding; e != EncodingPlainDictionary && e != EncodingRLEDictionary {
				return nil, false, nil
			}
		case PageDataV2:
			sawData = true
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
	//
	// A v1 level section is length-prefixed, so DecodeRLEInt32WithLength places
	// the value section by the PREFIX (consumed) and a short section cannot
	// shift it the way a v2 section can. What was never checked is the decoded
	// COUNT: the decoder sizes its destination to num_values and stops when the
	// prefixed bytes run out, so a section that encodes FEWER than num_values
	// levels — the degenerate case being a zeroed length prefix, which encodes
	// none — returned a short slice the caller read as "this page has zero
	// nulls", then decoded the value section out of the level bytes (#923, the
	// v1 twin of #891's v2 reconciliation). Exactly num_values levels, every
	// one within the schema maximum, on both streams.
	var repLevels []int32
	if r.maxRepLevel > 0 {
		bitWidth := bitsRequired(r.maxRepLevel)
		decoded, consumed, err := DecodeRLEInt32WithLength(pageData[off:], bitWidth, numValues)
		if err != nil {
			ReleaseDecompressed(r.codec, pageData)
			return nil, fmt.Errorf("decoding repetition levels: %w", err)
		}
		repLevels = decoded
		off += consumed
		if err := r.checkV1Levels("repetition", repLevels, numValues, r.maxRepLevel); err != nil {
			ReleaseDecompressed(r.codec, pageData)
			return nil, err
		}
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
			ReleaseDecompressed(r.codec, pageData)
			return nil, fmt.Errorf("decoding definition levels: %w", err)
		}
		if r.scratchOn {
			r.defScratch = decoded
		}
		defLevels = decoded
		off += consumed

		if err := r.checkV1Levels("definition", defLevels, numValues, r.maxDefLevel); err != nil {
			ReleaseDecompressed(r.codec, pageData)
			return nil, err
		}
		// Count non-null values (domain already checked by checkV1Levels).
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

	// Rows, not values. A flat leaf stores one value per row, so the two are
	// the same number. A NESTED leaf does not, and a v1 header carries no
	// row count at all — the row boundaries are in the repetition levels,
	// one row per level-0 entry. This used to report numValues for both,
	// which made PageData.NumRows a lie for every nested page and left the
	// chunk's rows uncountable (#892).
	numRows := numValues
	if r.maxRepLevel > 0 && repLevels != nil {
		numRows = countRowStarts(repLevels)
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

	// num_nulls is the one v2 count with no v1 counterpart, and the decode
	// sizes the VALUE section from it: nonNullCount = num_values - num_nulls.
	// It was taken at its word. A page claiming nulls it cannot have — a
	// REQUIRED leaf has no definition levels, so nothing can say WHICH value
	// is absent — then decoded fewer values than the page holds, and the
	// caller filled the shortfall with NULLs: a required INT64 column read
	// back with four holes in the middle, nil error, found by the header
	// bit-flip cell over a v2 fixture.
	if numNulls < 0 || numNulls > numValues {
		return nil, fmt.Errorf("column %s: data page v2 declares %d nulls of %d values",
			r.columnLabel(), numNulls, numValues)
	}
	if numNulls > 0 && (r.maxDefLevel == 0 || dph.DefinitionLevelsByteLength <= 0) {
		return nil, fmt.Errorf("column %s: data page v2 declares %d nulls but carries no "+
			"definition levels to place them (max definition level %d, %d level bytes)",
			r.columnLabel(), numNulls, r.maxDefLevel, dph.DefinitionLevelsByteLength)
	}

	// In v2, repetition and definition levels are stored uncompressed
	// before the (optionally compressed) data section.
	//
	// Both lengths are thrift i32 out of the page header, and both are used
	// as slice bounds before anything has looked at the bytes they describe:
	// `compressed[off:off+repLen]`, then `compressed[off:]`. A negative one
	// made `off` negative and the decode panicked with
	// "slice bounds out of range [-1:]"; one larger than the page ran into
	// the next page's bytes. §1's rule — bound it before it indexes anything
	// — had never been applied to these two fields.
	repLen := int(dph.RepetitionLevelsByteLength)
	defLen := int(dph.DefinitionLevelsByteLength)
	if repLen < 0 || defLen < 0 || repLen > len(compressed) || defLen > len(compressed)-repLen {
		return nil, fmt.Errorf("column %s: data page v2 declares %d repetition-level bytes and "+
			"%d definition-level bytes in a %d-byte page",
			r.columnLabel(), dph.RepetitionLevelsByteLength, dph.DefinitionLevelsByteLength,
			len(compressed))
	}
	// A leaf that HAS levels must carry them. Zero is the one length that
	// bounding cannot catch and that costs the most: the decode below is
	// guarded on `> 0`, so a zeroed length skips the level decode entirely,
	// `off` never advances, and every value in the page is read defLen bytes
	// early — a whole page of shifted values, nil error, and all three of the
	// v2 count cross-checks vacuous (num_nulls is 0, defLevels is nil,
	// num_rows equals num_values). One flipped bit in the header does it.
	if r.maxRepLevel > 0 && repLen <= 0 {
		return nil, fmt.Errorf("column %s: data page v2 on a leaf with repetition level %d "+
			"carries no repetition levels", r.columnLabel(), r.maxRepLevel)
	}
	if r.maxDefLevel > 0 && defLen <= 0 {
		return nil, fmt.Errorf("column %s: data page v2 on a leaf with definition level %d "+
			"carries no definition levels", r.columnLabel(), r.maxDefLevel)
	}

	// Decode repetition levels (uncompressed).
	//
	// Each section is held to BOTH of the things its declared length claims:
	// that it encodes num_values levels, and that it is exactly that many
	// bytes long. A v1 page gets the second for free — its sections are
	// length-prefixed, so the decoder derives the extent from the bytes and
	// reports what it consumed. A v2 page's extent comes from the header and
	// also PLACES the value section, so a length that disagrees with the
	// encoding moves every value after it. decodeLevelsConsumed reports both.
	var repLevels []int32
	if repLen > 0 && r.maxRepLevel > 0 {
		decoded, used, err := decodeLevelsConsumed(nil, compressed[off:off+repLen],
			bitsRequired(r.maxRepLevel), numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding v2 repetition levels: %w", err)
		}
		if err := r.checkV2LevelSection("repetition", repLen, used, len(decoded), numValues); err != nil {
			return nil, err
		}
		repLevels = decoded
	}
	off += repLen

	// Decode definition levels (uncompressed).
	var defLevels []int32
	if defLen > 0 && r.maxDefLevel > 0 {
		var scratch []int32
		if r.scratchOn {
			scratch = r.defScratch
		}
		decoded, used, err := decodeLevelsConsumed(scratch, compressed[off:off+defLen],
			bitsRequired(r.maxDefLevel), numValues)
		if err != nil {
			return nil, fmt.Errorf("decoding v2 definition levels: %w", err)
		}
		if r.scratchOn {
			r.defScratch = decoded
		}
		if err := r.checkV2LevelSection("definition", defLen, used, len(decoded), numValues); err != nil {
			return nil, err
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

	// A v2 header DECLARES num_rows. That declaration is the writer's claim,
	// and the repetition levels are the fact: a row starts at every level-0
	// entry. Where both exist they must agree, and a disagreement is the
	// file contradicting itself about how many rows this page carries — the
	// same class of self-contradiction the chunk-length truncation in #892
	// belongs to, and one the reader can settle without leaving the page.
	switch {
	case r.maxRepLevel > 0 && repLevels != nil:
		if fromLevels := countRowStarts(repLevels); fromLevels != numRows {
			ReleaseDecompressed(r.codec, rawBuf)
			return nil, fmt.Errorf("column %s: data page v2 declares %d rows but its repetition "+
				"levels start %d", r.columnLabel(), numRows, fromLevels)
		}
	case r.maxRepLevel == 0:
		// A flat leaf stores one value per row — nulls included, which is
		// what the definition levels are for — so a v2 header's two counts
		// have exactly one consistent pairing. Measured across parquet-go
		// and pyarrow v2 output: every flat page of both writers has
		// num_rows == num_values, no exceptions.
		if numRows != numValues {
			ReleaseDecompressed(r.codec, rawBuf)
			return nil, fmt.Errorf("column %s: data page v2 on a flat column declares %d rows "+
				"and %d values, which cannot both be true", r.columnLabel(), numRows, numValues)
		}
	}

	// And the null count against the levels that place them. The header's
	// number sizes the value section; the levels say which entries are
	// absent. A disagreement shifts every value after the first divergence,
	// so it is settled here rather than discovered as a wrong answer.
	if defLevels != nil {
		counted := 0
		for _, dl := range defLevels {
			if dl < int32(r.maxDefLevel) {
				counted++
			}
		}
		if counted != numNulls {
			ReleaseDecompressed(r.codec, rawBuf)
			return nil, fmt.Errorf("column %s: data page v2 declares %d nulls but its definition "+
				"levels mark %d", r.columnLabel(), numNulls, counted)
		}
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

// checkV2LevelSection holds a data page v2 level section to what its declared
// byte length claims. Measured across 89 v2 pages written by parquet-go and
// pyarrow — nested, flat, required, optional, all-null, split-row, both
// codecs — the decoder consumes the declared length EXACTLY and produces
// exactly num_values levels, with no exceptions: there is no writer slack to
// tolerate here.
func (r *ColumnPageReader) checkV2LevelSection(kind string, declared, used, got, want int) error {
	if used != declared {
		return fmt.Errorf("column %s: data page v2 declares %d %s-level bytes but the levels "+
			"encode in %d, so the value section does not start where the header says",
			r.columnLabel(), declared, kind, used)
	}
	if got != want {
		return fmt.Errorf("column %s: data page v2 declares %d values but its %s levels "+
			"decode to %d", r.columnLabel(), want, kind, got)
	}
	return nil
}

// checkV1Levels holds a data page v1 level section to the two things its
// declared num_values claims: that it decodes to exactly num_values levels, and
// that every level is within the schema maximum for the stream. Unlike the v2
// counterpart there is no consumed-bytes check — a v1 section is length-prefixed
// and the prefix, not the decode, places the value section — so the COUNT is the
// whole defence (#923). A level above the maximum is a second impossibility that
// means the bytes were misread as levels.
func (r *ColumnPageReader) checkV1Levels(kind string, levels []int32, want, maxLevel int) error {
	if len(levels) != want {
		return fmt.Errorf("column %s: data page v1 declares %d values but its %s levels decode to %d "+
			"(the level section is short, so the values would be read from the level bytes)",
			r.columnLabel(), want, kind, len(levels))
	}
	for _, l := range levels {
		if l < 0 || int(l) > maxLevel {
			return fmt.Errorf("column %s: data page v1 %s level %d is outside [0, %d]",
				r.columnLabel(), kind, l, maxLevel)
		}
	}
	return nil
}

// countRowStarts counts the rows a nested leaf's repetition levels describe.
// Level 0 means "a new row starts here"; every higher level continues the row
// before it. A row may span pages, and summing per page still totals right:
// the continuation page opens with a level above zero.
func countRowStarts(rep []int32) int {
	n := 0
	for _, rl := range rep {
		if rl == 0 {
			n++
		}
	}
	return n
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
