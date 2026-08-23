package scan

import (
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ReadFileBatchesNative reads all row groups from a Parquet file using our
// custom FileReader (no parquet-go dependency). Returns one RecordBatch per
// row group for schemas without unsupported types.
func ReadFileBatchesNative(fr *pqt.FileReader, schema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	return ReadFileBatchesNativeShard(fr, schema, selectedCols, 0, 1)
}

// ReadFileBatchesNativeShard reads only the row-group slice assigned to one
// shard. With shardCount=1 the behavior matches ReadFileBatchesNative.
//
// Row-group ownership: shardIdx i reads row groups in [i*total/count,
// (i+1)*total/count). When total < count, the early shards each read one
// row group and later shards read nothing — a degenerate but correct split.
func ReadFileBatchesNativeShard(fr *pqt.FileReader, schema []pqt.Column, selectedCols []string, shardIdx, shardCount int) ([]*batch.RecordBatch, error) {
	if shardCount < 1 {
		shardCount = 1
	}
	if shardIdx < 0 || shardIdx >= shardCount {
		return nil, nil
	}

	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}

	if HasUnsupportedColumnarTypes(readSchema) {
		return nil, fmt.Errorf("native reader does not support Array/Map types yet")
	}

	total := fr.NumRowGroups()
	startRg, endRg := rowGroupRangeForShard(total, shardIdx, shardCount)
	var batches []*batch.RecordBatch
	for rgIdx := startRg; rgIdx < endRg; rgIdx++ {
		b, err := ReadRowGroupNative(fr, rgIdx, readSchema, nil)
		if err != nil {
			return nil, err
		}
		if b != nil {
			batches = append(batches, b)
		}
	}
	return batches, nil
}

// rowGroupRangeForShard returns the half-open [start, end) row-group range
// for shardIdx out of shardCount total shards over total row groups. The
// union of ranges over [0, shardCount) covers exactly [0, total).
func rowGroupRangeForShard(total, shardIdx, shardCount int) (int, int) {
	if total <= 0 || shardCount <= 1 {
		return 0, total
	}
	start := shardIdx * total / shardCount
	end := (shardIdx + 1) * total / shardCount
	if shardIdx == shardCount-1 {
		end = total
	}
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	return start, end
}

// ReadRowGroupNative reads a row group using our custom page reader,
// bypassing parquet-go entirely for the data path.
func ReadRowGroupNative(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, pool *batch.BatchPool) (*batch.RecordBatch, error) {
	return ReadRowGroupNativeCached(fr, rgIdx, schema, pool, nil)
}

// ReadRowGroupNativeCached is ReadRowGroupNative with an optional decoded
// chunk cache: plain leaf columns whose (identity, row group, column, type)
// is cached skip decompress+decode and copy from the cache; fresh decodes
// are offered back for admission. A nil cache (or a reader without a
// CacheIdentity) is byte-identical to ReadRowGroupNative.
func ReadRowGroupNativeCached(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, pool *batch.BatchPool, cache *DecodedChunkCache) (*batch.RecordBatch, error) {
	return readRowGroupNative(fr, rgIdx, schema, pool, cache, nil, nil, nil)
}

// ReadRowGroupNativeBacked is ReadRowGroupNativeCached with the scan source's
// row-group backing pool: the decode writes into a backing a previous row
// group used, when the consumer released it and nobody claimed it. A nil
// backing (or the reuse kill switch off) is byte-identical to
// ReadRowGroupNativeCached. See BackingPool's ownership rule and
// docs/design/scan-output-backing-reuse.md.
func ReadRowGroupNativeBacked(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, cache *DecodedChunkCache, backing *BackingPool) (*batch.RecordBatch, error) {
	return readRowGroupNative(fr, rgIdx, schema, nil, cache, nil, nil, backing)
}

// ReadRowGroupNativeSel is ReadRowGroupNative under a partial scan-filter
// selection: eligible byte-array columns materialize only the rows in sel
// (ascending row indices; see sel_decode.go). A nil sel — or the sel-decode
// kill switch off — is identical to ReadRowGroupNative.
//
// Selectivity gate (metal-validated 2026-08-17): the sel path copies
// per selected value, the full path bulk-copies the page. At sparse
// selections the skipped values dominate (ClickBench Q22 −30% hot at
// ~0.1%); past ~25% selected the per-value loop loses to the single
// memcpy (Q28 +8s, Q29 +1.9s on `Referer <> ''`, which selects most
// rows) — those decode in full.
func ReadRowGroupNativeSel(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, pool *batch.BatchPool, sel []uint32) (*batch.RecordBatch, error) {
	return ReadRowGroupNativeShaped(fr, rgIdx, schema, pool, sel, nil)
}

// ReadRowGroupNativeShaped is ReadRowGroupNativeSel plus the set of columns
// (lowercased names) the planner proved are consumed for their SHAPE only.
// Those decode to per-row lengths with no value bytes materialized at all
// (lengths_decode.go). A nil/empty set — or the lengths-only kill switch off
// — is identical to ReadRowGroupNativeSel.
func ReadRowGroupNativeShaped(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, pool *batch.BatchPool, sel []uint32, shapeOnly map[string]bool) (*batch.RecordBatch, error) {
	if !selDecodeToggle.On() {
		sel = nil
	}
	if n := int(fr.RowGroupNumRows(rgIdx)); sel != nil && len(sel)*4 > n {
		sel = nil
	}
	if !lengthsOnlyToggle.On() {
		shapeOnly = nil
	}
	return readRowGroupNative(fr, rgIdx, schema, pool, nil, sel, shapeOnly, nil)
}

func readRowGroupNative(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, pool *batch.BatchPool, cache *DecodedChunkCache, sel []uint32, shapeOnly map[string]bool, backing *BackingPool) (*batch.RecordBatch, error) {
	// ARRAY/MAP leaves don't resolve by column name here; without this
	// guard the column lookup missed and the "schema evolution" branch
	// silently emitted ALL-NULL values for the array column — valid-looking
	// rows with the data gone. Callers must route such schemas to the
	// row-based fallback (readFileBatchesViaRows / readBatchViaRows).
	if HasUnsupportedColumnarTypes(schema) {
		return nil, fmt.Errorf("native reader does not support ARRAY/MAP columns")
	}
	numRows := int(fr.RowGroupNumRows(rgIdx))
	if numRows == 0 {
		return nil, nil
	}
	// The footer is validated on open, so this is a backstop rather than
	// the enforcement point — but it is one line in front of an allocation
	// sized entirely by a number out of the file.
	if numRows < 0 {
		return nil, fmt.Errorf("row group %d declares %d rows", rgIdx, numRows)
	}

	// Output backing: a released-and-unclaimed backing from a previous row
	// group when the scan source has one (BackingPool's ownership rule), else
	// the caller's BatchPool, else fresh. A reused backing is bit-identical to
	// a fresh one — ResetForWrite clears everything it re-slices — but keeps
	// its typed arrays and, decisively, its BYTES arena, so the PreAllocBytes
	// below stops re-requesting a multi-hundred-KB span per column per group.
	var b *batch.RecordBatch
	switch {
	case backing != nil:
		if b = backing.get(schema, numRows); b == nil {
			b = batch.NewRecordBatch(schema, numRows)
			backing.track(b, schema)
		}
	case pool != nil:
		b = pool.GetForSize(numRows)
	default:
		b = batch.NewRecordBatch(schema, numRows)
	}

	leaves := fr.Leaves()
	rg := fr.RowGroupMeta(rgIdx)
	if rg == nil {
		return nil, fmt.Errorf("row group %d metadata not found", rgIdx)
	}

	// Build leaf-name-to-index mapping for column lookup.
	leafByName := make(map[string]int, len(leaves))
	for i, leaf := range leaves {
		leafByName[leaf.Name] = i
	}
	// Also map by path for qualified name lookup.
	leafByPath := make(map[string]int, len(leaves))
	for i, leaf := range leaves {
		if len(leaf.Path) >= 2 {
			key := leaf.Path[len(leaf.Path)-2] + "." + leaf.Path[len(leaf.Path)-1]
			leafByPath[key] = i
		}
	}

	// Per-column reads write only to their own b.Columns[i] slot, and
	// FileReader is read-only after construction (immutable meta + leaves;
	// each ColumnPages call allocates a fresh ColumnPageReader). Parallelize
	// the per-column work to use idle worker cores: 2026-05-22 SF100 Q18
	// profile showed cachedFileStreamSource.Next at 22.7% cum CPU with the
	// per-column loop serial within a single fragment goroutine.
	limit := len(schema)
	if gp := runtime.GOMAXPROCS(0); limit > gp {
		limit = gp
	}
	g := new(errgroup.Group)
	g.SetLimit(limit)
	for i, col := range schema {
		i, col := i, col
		// ROW: read each child field as a separate leaf column.
		if col.Type == pqt.TypeRow && len(col.Fields) > 0 {
			g.Go(func() error {
				for j, field := range col.Fields {
					key := col.Name + "." + field.Name
					childIdx, ok := leafByPath[key]
					if !ok {
						for k := 0; k < numRows; k++ {
							b.Columns[i].Children[j].Nulls.SetNull(k)
						}
						continue
					}
					if err := readColumnNative(b.Columns[i].Children[j], fr, rgIdx, childIdx, numRows, field.Type); err != nil {
						return fmt.Errorf("reading ROW field %s.%s: %w", col.Name, field.Name, err)
					}
				}
				return nil
			})
			continue
		}

		colIdx, ok := leafByName[col.Name]
		if !ok {
			// Try aliases.
			found := false
			for _, alias := range columnAliases[col.Name] {
				if idx, ok := leafByName[alias]; ok {
					colIdx = idx
					found = true
					break
				}
			}
			if !found {
				// Column not in parquet file — leave as all nulls.
				// Run synchronously: trivial work, no decode.
				for j := 0; j < numRows; j++ {
					b.Columns[i].Nulls.SetNull(j)
				}
				continue
			}
		}

		ci := colIdx
		g.Go(func() error {
			key, cacheable := cache.keyFor(fr, rgIdx, ci, col)
			if cacheable && cache.fillFromCache(b.Columns[i], key, numRows) {
				return nil
			}
			if shapeOnly[strings.ToLower(col.Name)] && selEligibleLeaf(fr, ci, col.Type) {
				// Shape-only columns carry lengths, not values — never
				// offered to the decoded-chunk cache (a later full read
				// keyed the same way would get a valueless column).
				if err := readColumnNativeLengths(b.Columns[i], fr, rgIdx, ci, numRows); err != nil {
					return fmt.Errorf("reading column %s (lengths): %w", col.Name, err)
				}
				return nil
			}
			if len(sel) > 0 && selEligibleLeaf(fr, ci, col.Type) {
				// Sel-pruned columns are partial — never offered to the
				// cache (its key has no selection component).
				if err := readColumnNativeSel(b.Columns[i], fr, rgIdx, ci, numRows, sel); err != nil {
					return fmt.Errorf("reading column %s (sel): %w", col.Name, err)
				}
				return nil
			}
			if err := readColumnNative(b.Columns[i], fr, rgIdx, ci, numRows, col.Type); err != nil {
				return fmt.Errorf("reading column %s: %w", col.Name, err)
			}
			if cacheable {
				cache.Offer(key, b.Columns[i], numRows)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return b, nil
}

// readColumnNative reads all pages from a column chunk into a Vector using
// our custom ColumnPageReader.
func readColumnNative(vec *batch.Vector, fr *pqt.FileReader, rgIdx, colIdx, numRows int, catalogType pqt.TypeID) error {
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return fmt.Errorf("column %d not found in row group %d", colIdx, rgIdx)
	}
	defer pr.Close()
	// This loop fully consumes each page (values copied into vec) before
	// advancing, so per-page buffers are reused — and pooled ACROSS
	// column reads (chunks often hold only 1-3 large pages, so
	// within-reader reuse alone saves nothing). Per-page/per-chunk
	// allocation zeroing was a third of the narrow-scan floor.
	scr := colReadScratchPool.Get().(*colReadScratch)
	pr.SeedScratch(scr.def, scr.idx)
	drs := &scr.drs
	defer func() {
		scr.def, scr.idx = pr.TakeScratch()
		colReadScratchPool.Put(scr)
	}()

	leaves := fr.Leaves()
	fileType, maxDefLevel, coerce, perr := columnDecodePlan(leaves, colIdx, catalogType)
	if perr != nil {
		return perr
	}

	// Read dictionary if present.
	dict, err := pr.NextDictionary()
	if err != nil {
		return fmt.Errorf("reading dictionary: %w", err)
	}

	// Pre-size the bytes arena for the whole column chunk so the per-page
	// BulkSet appends never grow it: append-doubling re-memmoved the arena
	// log2(chunkBytes) times per chunk (31% of worker growslice CPU,
	// 2026-08-12 treatment profile). Plain chunks use the chunk metadata's
	// uncompressed size (an overestimate by page framing — a hint, not an
	// accounting figure); dictionary chunks expand to ~numRows × the mean
	// dictionary entry width.
	preallocBytesArena(vec, fr, rgIdx, colIdx, numRows, dict, catalogType)

	offset := 0
	for {
		page, err := pr.NextPage()
		if err != nil {
			return fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}

		pageRows := page.NumValues
		if pageRows == 0 {
			continue
		}
		// The row group's row count is the size every destination array was
		// allocated for; the page headers are the file's separate claim
		// about how many values the chunk carries, and nothing in the format
		// makes the two agree. A file whose pages declare more rows than the
		// row group used to walk `offset` straight past the end of the
		// vector and fault in the copy — vec.Int64Data[600:900] over a
		// 300-element array. The row reader already refuses the same shape.
		if pageRows < 0 || offset+pageRows > numRows {
			page.Release()
			return pageOverrunErr(leaves, colIdx, pageRows, offset, numRows)
		}

		if err := decodeOnePage(vec, offset, page, drs, dict, int32(maxDefLevel),
			fileType, catalogType, coerce); err != nil {
			page.Release()
			return leafErr(leaves, colIdx, err)
		}

		offset += pageRows
		page.Release()
	}

	if catalogType == pqt.TypeTimestamp && offset > 0 {
		rescaleTimestampChunk(vec, leaves, colIdx, offset)
	}

	return nil
}

// rescaleTimestampChunk converts a TIMESTAMP column written elsewhere from
// MICROS or NANOS to the engine's MILLIS, once per chunk rather than per
// page: it is one linear pass over data that is still hot in cache, it
// cannot be confused by dictionary pages (whose shared buffers must not be
// mutated), and it treats the direct and scatter copies identically. Millis
// files divide by 1 and return immediately, so our own files pay nothing.
//
// Non-inlined for readColumnNative's frame: inline, it keeps the leaf slice
// and the vector's typed array live all the way to the end of the page loop.
//
//go:noinline
func rescaleTimestampChunk(vec *batch.Vector, leaves []*pqt.SchemaNode, colIdx, offset int) {
	if colIdx >= len(leaves) {
		return
	}
	div := pqt.TimestampDivisorFromSchemaNode(leaves[colIdx])
	if div == 1 {
		return
	}
	n := offset
	if n > len(vec.Int64Data) {
		n = len(vec.Int64Data)
	}
	pqt.ScaleTimestampsToEngine(vec.Int64Data[:n], div)
}

// resolveNativeDictionary resolves INT32 dictionary indices to actual values.
// Indices are bounds-checked against the dictionary before any gather:
// corrupt or hostile files can carry out-of-range indices.
func resolveNativeDictionary(dict *pqt.DictionaryData, page pqt.Values, fileType pqt.TypeID) (pqt.Values, error) {
	return resolveNativeDictionaryScratch(nil, dict, page, fileType)
}

// colReadScratch bundles every per-column-read reusable buffer; pooled
// process-wide so the buffers amortize across row groups, files, and
// queries instead of being reallocated (and zeroed) per column chunk.
type colReadScratch struct {
	def []int32
	idx []int32
	drs dictResolveScratch
}

var colReadScratchPool = sync.Pool{New: func() any { return &colReadScratch{} }}

// dictResolveScratch holds gather buffers reused across the pages of one
// column read. Values returned from a scratch-backed resolve alias these
// buffers and are invalidated by the next resolve — callers must copy
// into their destination vector before advancing (readColumnNative does).
type dictResolveScratch struct {
	i64  []int64
	i32  []int32
	f64  []float64
	f32  []float32
	buf  []byte
	offs []uint32
}

// resolveNativeDictionaryScratch resolves INT32 dictionary indices to
// values, reusing s's buffers when non-nil (nil s = allocate fresh, the
// prior behavior for callers that retain the result).
func resolveNativeDictionaryScratch(s *dictResolveScratch, dict *pqt.DictionaryData, page pqt.Values, fileType pqt.TypeID) (pqt.Values, error) {
	indices := page.Int32()
	n := page.Count()
	// Bounds are verified IN the gather loops below (uint cast catches
	// negative and >= numValues in one compare) — the separate
	// ValidateDictIndices pre-pass re-read every index and was 8.5% of
	// the narrow-scan floor profile. Same failure contract: corrupt or
	// hostile indices return an error, never panic.
	if len(indices) < n {
		return pqt.Values{}, fmt.Errorf("dictionary page: %d indices for %d values", len(indices), n)
	}

	// DECIMAL is stored in four different physicals by the format; which
	// one this file used is a property of the dictionary page's bytes, not
	// of the type. Route it by physical rather than assuming INT64 (which
	// is only what OUR writer emits).
	if fileType == pqt.TypeDecimal {
		switch dict.Data.PhysType() {
		case pqt.PhysicalInt64:
			fileType = pqt.TypeInt64
		case pqt.PhysicalInt32:
			fileType = pqt.TypeInt32
		default:
			return resolveDictByteArrayScratch(s, dict, indices, n)
		}
	}

	switch fileType {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := dict.Data.Int64()
		var dst []int64
		if s != nil {
			if cap(s.i64) < n {
				s.i64 = make([]int64, n)
			}
			dst = s.i64[:n]
		} else {
			dst = make([]int64, n)
		}
		for i := 0; i < n; i++ {
			idx := indices[i]
			if uint(idx) >= uint(len(src)) {
				return pqt.Values{}, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, len(src))
			}
			dst[i] = src[idx]
		}
		return pqt.PlainInt64Values(dst), nil

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := dict.Data.Int32()
		var dst []int32
		if s != nil {
			if cap(s.i32) < n {
				s.i32 = make([]int32, n)
			}
			dst = s.i32[:n]
		} else {
			dst = make([]int32, n)
		}
		for i := 0; i < n; i++ {
			idx := indices[i]
			if uint(idx) >= uint(len(src)) {
				return pqt.Values{}, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, len(src))
			}
			dst[i] = src[idx]
		}
		return pqt.PlainInt32Values(dst), nil

	case pqt.TypeFloat64:
		src := dict.Data.Double()
		var dst []float64
		if s != nil {
			if cap(s.f64) < n {
				s.f64 = make([]float64, n)
			}
			dst = s.f64[:n]
		} else {
			dst = make([]float64, n)
		}
		for i := 0; i < n; i++ {
			idx := indices[i]
			if uint(idx) >= uint(len(src)) {
				return pqt.Values{}, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, len(src))
			}
			dst[i] = src[idx]
		}
		return pqt.PlainFloat64Values(dst), nil

	case pqt.TypeFloat32:
		src := dict.Data.Float()
		var dst []float32
		if s != nil {
			if cap(s.f32) < n {
				s.f32 = make([]float32, n)
			}
			dst = s.f32[:n]
		} else {
			dst = make([]float32, n)
		}
		for i := 0; i < n; i++ {
			idx := indices[i]
			if uint(idx) >= uint(len(src)) {
				return pqt.Values{}, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, len(src))
			}
			dst[i] = src[idx]
		}
		return pqt.PlainFloat32Values(dst), nil

	case pqt.TypeVector:
		// VECTOR stored as FIXED_LEN_BYTE_ARRAY: resolve dictionary indices.
		return resolveDictByteArrayScratch(s, dict, indices, n)

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		return resolveDictByteArrayScratch(s, dict, indices, n)

	default:
		// Fallback for unknown types: treat as byte array.
		return resolveDictByteArrayScratch(s, dict, indices, n)
	}
}

// resolveDictByteArray gathers byte-array dictionary entries for a page's
// indices. The output buffer is sized EXACTLY in a first pass over the
// indices: the append-from-nil growth this replaces re-memmoved the whole
// buffer log2(size) times per page and ranked 23% of worker growslice CPU
// in the 2026-08-12 treatment profile.
func resolveDictByteArray(dict *pqt.DictionaryData, indices []int32, n int) (pqt.Values, error) {
	return resolveDictByteArrayScratch(nil, dict, indices, n)
}

func resolveDictByteArrayScratch(s *dictResolveScratch, dict *pqt.DictionaryData, indices []int32, n int) (pqt.Values, error) {
	dictData, dictOffsets := dict.Data.ByteArray()
	numVals := len(dictOffsets) - 1
	total := 0
	for i := 0; i < n; i++ {
		idx := indices[i]
		// In-loop bounds check (the ValidateDictIndices pre-pass is gone):
		// corrupt/hostile indices must error, never panic or mis-slice.
		if uint(idx) >= uint(numVals) {
			return pqt.Values{}, fmt.Errorf("dictionary index %d out of range [0,%d)", idx, numVals)
		}
		total += int(dictOffsets[idx+1] - dictOffsets[idx])
	}
	var buf []byte
	var offsets []uint32
	if s != nil {
		if cap(s.buf) < total {
			s.buf = make([]byte, 0, total)
		}
		buf = s.buf[:0]
		if cap(s.offs) < n+1 {
			s.offs = make([]uint32, n+1)
		}
		offsets = s.offs[:n+1]
	} else {
		buf = make([]byte, 0, total)
		offsets = make([]uint32, n+1)
	}
	for i := 0; i < n; i++ {
		idx := indices[i]
		offsets[i] = uint32(len(buf))
		buf = append(buf, dictData[dictOffsets[idx]:dictOffsets[idx+1]]...)
	}
	offsets[n] = uint32(len(buf))
	if s != nil {
		s.buf = buf
	}
	return pqt.ByteArrayValues(buf, offsets), nil
}

// copyPageIntoVector dispatches one decoded page to the copy that matches its
// nullability and whether the catalog type needs a conversion.
//
// It is its own function rather than four branches in readColumnNative, and
// the error wrapping below is its own function too, because
// readColumnNative's frame sits on the stack of every per-column errgroup
// goroutine and those goroutines start small. Making the copies return
// errors grew that frame by 96 bytes, which was enough to add a second
// morestack + copystack per column read: 5% of a small-row-group scan,
// entirely in runtime.copystack, none of it in the decode. Keep the frame
// small — measure with `go build -gcflags=-S | grep readColumnNative`.
func copyPageIntoVector(vec *batch.Vector, offset int, data pqt.Values, defLevels []int32,
	maxDefLevel int32, hasNulls bool, n int, fileType, catalogType pqt.TypeID, coerce bool) error {
	if defLevels == nil || !hasNulls {
		if coerce {
			return copyNativeCoercedDirect(vec, offset, data, n, fileType, catalogType)
		}
		return copyNativeDataDirect(vec, offset, data, n, fileType)
	}
	if coerce {
		return copyNativeCoercedScatter(vec, offset, data, defLevels, maxDefLevel, n, fileType, catalogType)
	}
	return copyNativeDataScatter(vec, offset, data, defLevels, maxDefLevel, n, fileType)
}

// plainPrefixErr refuses a PLAIN byte-array page that ends inside a value.
// Non-inlined for the same frame-size reason as leafErr.
//
//go:noinline
func plainPrefixErr(typ pqt.TypeID, i, n, need, have int) error {
	return fmt.Errorf("type %s: PLAIN page ends at value %d of %d — the value needs %d bytes but the page body holds %d",
		typ, i, n, need, have)
}

// pageOverrunErr refuses a chunk whose page headers claim more rows than the
// row group declares. Non-inlined for the same frame-size reason as leafErr.
//
//go:noinline
func pageOverrunErr(leaves []*pqt.SchemaNode, colIdx, pageRows, offset, numRows int) error {
	return leafErr(leaves, colIdx, fmt.Errorf(
		"page declares %d values at row %d but the row group holds %d rows",
		pageRows, offset, numRows))
}

// decodeOnePage resolves one page's values (expanding dictionary indices
// when the page carries them) and copies them into the destination vector.
//
// Non-inlined, and it is the largest of readColumnNative's frame savings:
// the decoded pqt.Values is a 56-byte struct and copyPageIntoVector takes
// ten arguments including a copy of it, so doing this inline reserved all of
// that in the frame of a function that runs on a freshly-created errgroup
// goroutine. Past the runtime's initial stack size every column read pays a
// runtime.newstack + copystack — measured at +5.7% on
// BenchmarkReadColumnar/rows=1000, and reproducible on an untouched tree by
// adding 48 bytes of dead padding to readColumnNative. That is the whole
// mechanism: the frame, not the work.
//
// Dictionary resolution stays per PAGE rather than per chunk because a
// writer's dictionary overflow mixes dictionary-encoded and PLAIN pages
// within one chunk.
//
//go:noinline
func decodeOnePage(vec *batch.Vector, offset int, page *pqt.PageData, drs *dictResolveScratch,
	dict *pqt.DictionaryData, maxDefLevel int32, fileType, catalogType pqt.TypeID, coerce bool) error {
	data := page.Data
	if page.IsDictEncoded() {
		if dict == nil {
			return fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
		}
		var err error
		data, err = resolveNativeDictionaryScratch(drs, dict, data, fileType)
		if err != nil {
			return fmt.Errorf("resolving dictionary page: %w", err)
		}
	}
	return copyPageIntoVector(vec, offset, data, page.DefinitionLevels, maxDefLevel,
		page.NumNulls > 0, page.NumValues, fileType, catalogType, coerce)
}

// columnDecodePlan resolves everything readColumnNative needs to know about
// one leaf before it starts reading pages: the type the FILE recovers for
// it, its maximum definition level, and whether the values need converting
// on the way into the catalog's vector — refusing the pairings where they
// cannot get there at all.
//
// The copy paths switch on the FILE's type while writing into a vector
// allocated for the CATALOG's, so the two must agree on which typed array
// the values land in; storageClass answers that. A pairing that agrees is
// copied verbatim, the three CoercibleTo pairings are converted, and nothing
// else is decodable — the ones that used to reach the copy anyway did not
// fail, they indexed the wrong array and panicked.
//
// It is one non-inlined function, and that is load-bearing rather than
// tidy. readColumnNative's frame sits on the stack of every per-column
// errgroup goroutine, and those start at the runtime's minimum: doing this
// work inline grew the frame past what the initial stack holds, so EVERY
// column read paid an extra runtime.newstack + copystack. That measured as
// +7% on BenchmarkReadColumnar/rows=1000 with runtime.newstack going from
// 4.9% to 8.8% of a GOMAXPROCS=1 profile. Check the frame with
// `go build -gcflags=-S | grep readColumnNative STEXT` — it must stay at or
// under the 0x268 it was before this guard existed.
//
//go:noinline
func columnDecodePlan(leaves []*pqt.SchemaNode, colIdx int, catalogType pqt.TypeID) (
	fileType pqt.TypeID, maxDefLevel int, coerce bool, err error) {
	fileType = catalogType
	if colIdx < len(leaves) {
		fileType = pqt.TypeIDFromSchemaNode(leaves[colIdx])
		maxDefLevel = leaves[colIdx].MaxDefLevel
	}
	coerce = storageClass(fileType) != storageClass(catalogType)
	if coerce && !pqt.CoercibleTo(fileType, catalogType) {
		return fileType, maxDefLevel, false, leafErr(leaves, colIdx, fmt.Errorf(
			"schema declares %s but the file stores %s: refusing to decode the file's "+
				"values into a %s vector", catalogType, fileType, catalogType))
	}
	return fileType, maxDefLevel, coerce, nil
}

// preallocBytesArena sizes a bytes column's arena for the whole chunk up
// front, so the per-page BulkSet appends never grow it: append-doubling
// re-memmoved the arena log2(chunkBytes) times per chunk (31% of worker
// growslice CPU, 2026-08-12 treatment profile). Plain chunks use the chunk
// metadata's uncompressed size (an overestimate by page framing — a hint,
// not an accounting figure); dictionary chunks expand to ~numRows x the mean
// dictionary entry width.
//
// Non-inlined for the frame-size reason readColumnNative's comment gives:
// the estimate reaches into the dictionary values and the row-group
// metadata, and doing that inline keeps both live across the calls, which
// costs the caller stack it pays for on every column read.
//
//go:noinline
func preallocBytesArena(vec *batch.Vector, fr *pqt.FileReader, rgIdx, colIdx, numRows int,
	dict *pqt.DictionaryData, catalogType pqt.TypeID) {
	switch catalogType {
	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
	default:
		return
	}
	est := 0
	if dict != nil && dict.NumValues > 0 {
		if dictData, _ := dict.Data.ByteArray(); dictData != nil {
			est = numRows * (len(dictData)/dict.NumValues + 1)
		}
	} else if rg := fr.RowGroupMeta(rgIdx); rg != nil && colIdx < len(rg.Columns) {
		if cm := rg.Columns[colIdx].MetaData; cm != nil {
			est = int(cm.TotalUncompressedSize)
		}
	}
	if est > 0 {
		vec.BytesData.PreAllocBytes(est)
	}
}

// leafErr names the file leaf a decode error came from. Non-inlined for the
// same frame-size reason as copyPageIntoVector above.
//
//go:noinline
func leafErr(leaves []*pqt.SchemaNode, colIdx int, err error) error {
	name := ""
	if colIdx < len(leaves) {
		name = leaves[colIdx].Name
	}
	return fmt.Errorf("column %q: %w", name, err)
}

// pageSrcErr names a page whose decoded values cannot back the read the
// column's type is about to make: either the page's physical type is not the
// one that type decodes from, or the page decoded fewer values than the read
// needs. Both used to be a `src[:n]` over a nil or short slice — a
// slice-bounds panic raised inside the scan errgroup, which in a worker
// process is the worker.
//
// The two get one error because the caller cannot act on them differently:
// the file says something about this column that the file's own bytes do not
// support, and the only safe answer is to refuse the column.
func pageSrcErr(typ pqt.TypeID, want pqt.PhysicalType, data pqt.Values, need, got int) error {
	return fmt.Errorf("type %s decodes from %s pages but this page holds %s: %d of %d values available",
		typ, want, data.PhysType(), got, need)
}

// int64Src and its siblings are the ONLY way the copy paths below reach a
// typed slice: each returns the slice or the error, so no call site can
// index one it did not check. Both checks are per PAGE, never per value:
// the physical type, and (for the direct paths, which read n values
// straight through) the length. The scatter paths pass need=0 and are
// bounded by scatterRunsNative instead, which already walks the levels.
func int64Src(data pqt.Values, need int, typ pqt.TypeID) ([]int64, error) {
	if data.PhysType() != pqt.PhysicalInt64 {
		return nil, pageSrcErr(typ, pqt.PhysicalInt64, data, need, 0)
	}
	src := data.Int64()
	if len(src) < need {
		return nil, pageSrcErr(typ, pqt.PhysicalInt64, data, need, len(src))
	}
	return src, nil
}

func int32Src(data pqt.Values, need int, typ pqt.TypeID) ([]int32, error) {
	if data.PhysType() != pqt.PhysicalInt32 {
		return nil, pageSrcErr(typ, pqt.PhysicalInt32, data, need, 0)
	}
	src := data.Int32()
	if len(src) < need {
		return nil, pageSrcErr(typ, pqt.PhysicalInt32, data, need, len(src))
	}
	return src, nil
}

// byteArraySrc is the byte-array arm of the same family: the physical type
// is checked per PAGE, and a page that does not carry byte-array values is
// refused rather than read through a nil offsets table. A nil table is how
// the copy paths spell "PLAIN, four-byte length prefix per value", so an
// INT64 page reaching them decoded into a STRING vector as whatever those
// bytes happened to say — data rows, err == nil, while the row reader
// refused the same file.
func byteArraySrc(data pqt.Values, typ pqt.TypeID) ([]byte, []uint32, error) {
	switch data.PhysType() {
	case pqt.PhysicalByteArray, pqt.PhysicalFixedLenByteArray, pqt.PhysicalInt96:
		raw, offs := data.ByteArray()
		return raw, offs, nil
	}
	return nil, nil, pageSrcErr(typ, pqt.PhysicalByteArray, data, data.Count(), 0)
}

func float64Src(data pqt.Values, need int, typ pqt.TypeID) ([]float64, error) {
	if data.PhysType() != pqt.PhysicalDouble {
		return nil, pageSrcErr(typ, pqt.PhysicalDouble, data, need, 0)
	}
	src := data.Double()
	if len(src) < need {
		return nil, pageSrcErr(typ, pqt.PhysicalDouble, data, need, len(src))
	}
	return src, nil
}

func float32Src(data pqt.Values, need int, typ pqt.TypeID) ([]float32, error) {
	if data.PhysType() != pqt.PhysicalFloat {
		return nil, pageSrcErr(typ, pqt.PhysicalFloat, data, need, 0)
	}
	src := data.Float()
	if len(src) < need {
		return nil, pageSrcErr(typ, pqt.PhysicalFloat, data, need, len(src))
	}
	return src, nil
}

// copyNativeDataDirect copies non-null page data into a Vector.
func copyNativeDataDirect(vec *batch.Vector, offset int, data pqt.Values, n int, typ pqt.TypeID) error {
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src, err := int64Src(data, n, typ)
		if err != nil {
			return err
		}
		copy(vec.Int64Data[offset:offset+n], src[:n])

	case pqt.TypeDecimal:
		// DECIMAL is stored in four different physicals by the format;
		// which one this file used is read off the page, not assumed.
		// Without this case the switch fell through and every decimal
		// column scanned as zeros, silently (issue #144 suite finding).
		if err := decimalInto(vec.DecimalData.Data[offset:offset+n], data, 0, n); err != nil {
			return err
		}

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src, err := int32Src(data, n, typ)
		if err != nil {
			return err
		}
		copy(vec.Int32Data[offset:offset+n], src[:n])

	case pqt.TypeFloat64:
		src, err := float64Src(data, n, typ)
		if err != nil {
			return err
		}
		copy(vec.Float64Data[offset:offset+n], src[:n])

	case pqt.TypeFloat32:
		src, err := float32Src(data, n, typ)
		if err != nil {
			return err
		}
		copy(vec.Float32Data[offset:offset+n], src[:n])

	case pqt.TypeBool:
		if data.PhysType() != pqt.PhysicalBoolean {
			return pageSrcErr(typ, pqt.PhysicalBoolean, data, n, 0)
		}
		boolBytes := data.Boolean()
		// Every row carries a value on this path, so one check covers them
		// all. A missing bit used to read as false, which is a value, not
		// an absence.
		if len(boolBytes)*8 < n {
			return pageSrcErr(typ, pqt.PhysicalBoolean, data, n, len(boolBytes)*8)
		}
		for i := 0; i < n; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			vec.BoolData[offset+i] = (boolBytes[byteIdx] & (1 << bitIdx)) != 0
		}

	case pqt.TypeVector:
		// FIXED_LEN_BYTE_ARRAY: decode little-endian float32 bytes into Float32Data.
		rawData, offsets, err := byteArraySrc(data, typ)
		if err != nil {
			return err
		}
		dim := vec.VectorDim
		if dim <= 0 {
			break
		}
		if offsets != nil && len(offsets) < n+1 {
			return pageSrcErr(typ, pqt.PhysicalFixedLenByteArray, data, n, len(offsets)-1)
		}
		for i := 0; i < n; i++ {
			var val []byte
			if offsets != nil {
				val = rawData[offsets[i]:offsets[i+1]]
			} else if i*dim*4+dim*4 <= len(rawData) {
				val = rawData[i*dim*4 : (i+1)*dim*4]
			}
			dstOff := (offset + i) * dim
			for j := 0; j < dim && j*4+4 <= len(val); j++ {
				vec.Float32Data[dstOff+j] = math.Float32frombits(binary.LittleEndian.Uint32(val[j*4:]))
			}
		}

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		rawData, offsets, err := byteArraySrc(data, typ)
		if err != nil {
			return err
		}
		if offsets != nil {
			if len(offsets) < n+1 {
				return pageSrcErr(typ, pqt.PhysicalByteArray, data, n, len(offsets)-1)
			}
			vec.BytesData.BulkSet(offset, rawData, offsets, n)
		} else {
			// PLAIN encoding: 4-byte length prefix per value. Running out
			// of prefixes, or off the end of one value's bytes, used to
			// `break` — which leaves every remaining row holding whatever
			// the pooled vector had in it and returns nil. A page that ends
			// mid-value is a page the file cannot back; the rows after it
			// are not empty, they are unknown.
			pos := 0
			for i := 0; i < n; i++ {
				if pos+4 > len(rawData) {
					return plainPrefixErr(typ, i, n, pos, len(rawData))
				}
				length := int(binary.LittleEndian.Uint32(rawData[pos:]))
				pos += 4
				if length < 0 || pos+length > len(rawData) {
					return plainPrefixErr(typ, i, n, pos+length, len(rawData))
				}
				vec.BytesData.Set(offset+i, rawData[pos:pos+length])
				pos += length
			}
		}
	}
	return nil
}

// decimalInto decodes n DECIMAL values, starting at page value index
// srcStart, straight into dst — from whichever of the four physicals the
// format allows this file used.
//
// Parquet carries DECIMAL as INT32, INT64, BYTE_ARRAY or
// FIXED_LEN_BYTE_ARRAY, and TypeIDFromSchemaNode answers TypeDecimal for all
// four; this path used to hand every one of them to Values.Int64(), which
// reinterpreted bytes of the wrong width — and, for the narrower physicals,
// read past the end of the page buffer to find enough of them. The scan then
// refused anything but INT64 outright, which made every pyarrow-written
// decimal column unreadable on the native path. Both are fixed here: the
// page's own physical type picks the decode.
//
// It writes into the caller's slice rather than returning one so the three
// physicals cost what the INT64 case always cost — nothing per page.
func decimalInto(dst []batch.Int128, data pqt.Values, srcStart, n int) error {
	switch data.PhysType() {
	case pqt.PhysicalInt64:
		src, err := int64Src(data, srcStart+n, pqt.TypeDecimal)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			dst[i] = batch.Int128From(src[srcStart+i])
		}
	case pqt.PhysicalInt32:
		src, err := int32Src(data, srcStart+n, pqt.TypeDecimal)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			dst[i] = batch.Int128From(int64(src[srcStart+i]))
		}
	case pqt.PhysicalByteArray, pqt.PhysicalFixedLenByteArray:
		raw, offs := data.ByteArray()
		if len(offs) < srcStart+n+1 {
			return pageSrcErr(pqt.TypeDecimal, data.PhysType(), data, srcStart+n, len(offs)-1)
		}
		for i := 0; i < n; i++ {
			w := pqt.DecimalFromBytes(raw[offs[srcStart+i]:offs[srcStart+i+1]])
			dst[i] = batch.Int128{Hi: int64(w[0]), Lo: w[1]}
		}
	default:
		return fmt.Errorf("DECIMAL column: unsupported physical encoding %s", data.PhysType())
	}
	return nil
}

// copyNativeDataScatter copies nullable page data into a Vector using
// int32 definition levels from our custom page reader.
//
// The typed source is not fetched for n: scatterRunsNative walks n
// definition levels but only advances the source index on the ones that
// carry a value, so the source is checked for its PHYSICAL type here and
// bounded against the levels there — no pre-pass to count them.
func copyNativeDataScatter(vec *batch.Vector, offset int, data pqt.Values, defLevels []int32, maxDefLevel int32, n int, typ pqt.TypeID) error {
	if len(defLevels) < n {
		return fmt.Errorf("page declares %d values but decoded %d definition levels", n, len(defLevels))
	}
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src, err := int64Src(data, 0, typ)
		if err != nil {
			return err
		}
		return scatterRunsNative(defLevels, maxDefLevel, n, len(src), func(dstStart, srcStart, count int) {
			copy(vec.Int64Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeDecimal:
		// decimalInto reports per run; scatterRunsNative's callback cannot,
		// so the first failure is carried out.
		var decErr error
		if err := scatterRunsNative(defLevels, maxDefLevel, n, data.Count(), func(dstStart, srcStart, count int) {
			if decErr == nil {
				decErr = decimalInto(vec.DecimalData.Data[offset+dstStart:offset+dstStart+count],
					data, srcStart, count)
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		}); err != nil {
			return err
		}
		return decErr

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src, err := int32Src(data, 0, typ)
		if err != nil {
			return err
		}
		return scatterRunsNative(defLevels, maxDefLevel, n, len(src), func(dstStart, srcStart, count int) {
			copy(vec.Int32Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeFloat64:
		src, err := float64Src(data, 0, typ)
		if err != nil {
			return err
		}
		return scatterRunsNative(defLevels, maxDefLevel, n, len(src), func(dstStart, srcStart, count int) {
			copy(vec.Float64Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeFloat32:
		src, err := float32Src(data, 0, typ)
		if err != nil {
			return err
		}
		return scatterRunsNative(defLevels, maxDefLevel, n, len(src), func(dstStart, srcStart, count int) {
			copy(vec.Float32Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeBool:
		if data.PhysType() != pqt.PhysicalBoolean {
			return pageSrcErr(typ, pqt.PhysicalBoolean, data, n, 0)
		}
		boolBytes := data.Boolean()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				byteIdx := valIdx / 8
				bitIdx := uint(valIdx % 8)
				// n counts ROWS; only the non-null ones consume a bit, so
				// the bound is per value and cannot be hoisted. It was
				// already being tested here — it just yielded false, which
				// is a value, not an absence.
				if byteIdx >= len(boolBytes) {
					return pageSrcErr(typ, pqt.PhysicalBoolean, data, valIdx+1, len(boolBytes)*8)
				}
				vec.BoolData[offset+i] = (boolBytes[byteIdx] & (1 << bitIdx)) != 0
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeVector:
		// FIXED_LEN_BYTE_ARRAY with nullable scatter.
		rawData, offsets, err := byteArraySrc(data, typ)
		if err != nil {
			return err
		}
		dim := vec.VectorDim
		if dim <= 0 {
			break
		}
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				var val []byte
				if offsets != nil {
					if valIdx+1 >= len(offsets) {
						return pageSrcErr(typ, pqt.PhysicalFixedLenByteArray, data, valIdx+1, len(offsets)-1)
					}
					val = rawData[offsets[valIdx]:offsets[valIdx+1]]
				}
				dstOff := (offset + i) * dim
				for j := 0; j < dim && j*4+4 <= len(val); j++ {
					vec.Float32Data[dstOff+j] = math.Float32frombits(binary.LittleEndian.Uint32(val[j*4:]))
				}
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		rawData, offsets, err := byteArraySrc(data, typ)
		if err != nil {
			return err
		}
		valIdx := 0
		if offsets != nil {
			// The offsets can only run short of the levels on a corrupt
			// page; the loop is already per value, so the bound rides
			// along with it rather than costing a pre-pass.
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					if valIdx+1 >= len(offsets) {
						return pageSrcErr(typ, pqt.PhysicalByteArray, data, valIdx+1, len(offsets)-1)
					}
					start := offsets[valIdx]
					end := offsets[valIdx+1]
					vec.BytesData.Set(offset+i, rawData[start:end])
					valIdx++
				} else {
					vec.Nulls.SetNull(offset + i)
					vec.BytesData.Set(offset+i, nil)
				}
			}
		} else {
			// PLAIN encoding fallback. See copyNativeDataDirect: a short
			// prefix is a refusal, not a stopping point.
			pos := 0
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					if pos+4 > len(rawData) {
						return plainPrefixErr(typ, i, n, pos, len(rawData))
					}
					length := int(binary.LittleEndian.Uint32(rawData[pos:]))
					pos += 4
					if length < 0 || pos+length > len(rawData) {
						return plainPrefixErr(typ, i, n, pos+length, len(rawData))
					}
					vec.BytesData.Set(offset+i, rawData[pos:pos+length])
					pos += length
					valIdx++
				} else {
					vec.Nulls.SetNull(offset + i)
					vec.BytesData.Set(offset+i, nil)
				}
			}
		}
	}
	return nil
}

// scatterRunsNative is like scatterRunsTyped but for int32 definition levels.
//
// srcLen is how many values the page actually decoded. The levels say how
// many the page CLAIMS, and a corrupt file can make the two disagree — the
// scatter would then index past the end of the typed slice and panic inside
// the scan errgroup. The bound is checked once per RUN, before the run is
// handed over, rather than by pre-counting the defined levels: the levels
// are already being walked here, and a second pass over them was worth
// several percent of the nullable-scan floor.
func scatterRunsNative(
	defLevels []int32, maxDefLevel int32, n, srcLen int,
	onValid func(dstStart, srcStart, count int),
	onNull func(dstStart, count int),
) error {
	valIdx := 0
	i := 0
	for i < n {
		if defLevels[i] == maxDefLevel {
			runStart := i
			srcStart := valIdx
			for i < n && defLevels[i] == maxDefLevel {
				i++
				valIdx++
			}
			if valIdx > srcLen {
				return fmt.Errorf("page's definition levels mark %d values but only %d decoded", valIdx, srcLen)
			}
			onValid(runStart, srcStart, i-runStart)
		} else {
			runStart := i
			for i < n && defLevels[i] != maxDefLevel {
				i++
			}
			onNull(runStart, i-runStart)
		}
	}
	return nil
}

// copyNativeCoercedDirect converts non-null page data from fileType to catalogType.
func copyNativeCoercedDirect(vec *batch.Vector, offset int, data pqt.Values, n int, fileType, catalogType pqt.TypeID) error {
	switch {
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeInt32:
		src, err := int64Src(data, n, fileType)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			vec.Int32Data[offset+i] = int32(src[i])
		}
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeFloat64:
		src, err := int64Src(data, n, fileType)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			vec.Float64Data[offset+i] = float64(src[i])
		}
	case (fileType == pqt.TypeDate || fileType == pqt.TypeInt32) && catalogType == pqt.TypeString:
		src, err := int32Src(data, n, fileType)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			vec.BytesData.Set(offset+i, []byte(batch.FormatDate(src[i])))
		}
	default:
		return fmt.Errorf("unsupported type coercion: %v → %v", fileType, catalogType)
	}
	return nil
}

// copyNativeCoercedScatter converts nullable page data with type coercion.
func copyNativeCoercedScatter(vec *batch.Vector, offset int, data pqt.Values, defLevels []int32, maxDefLevel int32, n int, fileType, catalogType pqt.TypeID) error {
	if len(defLevels) < n {
		return fmt.Errorf("page declares %d values but decoded %d definition levels", n, len(defLevels))
	}
	switch {
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeInt32:
		src, err := int64Src(data, 0, fileType)
		if err != nil {
			return err
		}
		return scatterRunsNative(defLevels, maxDefLevel, n, len(src), func(dstStart, srcStart, count int) {
			for i := 0; i < count; i++ {
				vec.Int32Data[offset+dstStart+i] = int32(src[srcStart+i])
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeFloat64:
		src, err := int64Src(data, 0, fileType)
		if err != nil {
			return err
		}
		return scatterRunsNative(defLevels, maxDefLevel, n, len(src), func(dstStart, srcStart, count int) {
			for i := 0; i < count; i++ {
				vec.Float64Data[offset+dstStart+i] = float64(src[srcStart+i])
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})
	case (fileType == pqt.TypeDate || fileType == pqt.TypeInt32) && catalogType == pqt.TypeString:
		src, err := int32Src(data, 0, fileType)
		if err != nil {
			return err
		}
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				if valIdx >= len(src) {
					return pageSrcErr(fileType, pqt.PhysicalInt32, data, valIdx+1, len(src))
				}
				vec.BytesData.Set(offset+i, []byte(batch.FormatDate(src[valIdx])))
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
				vec.BytesData.Set(offset+i, nil)
			}
		}
	default:
		return fmt.Errorf("unsupported type coercion: %v → %v", fileType, catalogType)
	}
	return nil
}
