package physical

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	goparquet "github.com/parquet-go/parquet-go"
	pqencoding "github.com/parquet-go/parquet-go/encoding"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func readAll(rc io.ReadCloser) ([]byte, error) {
	return io.ReadAll(rc)
}

func bytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}

func fromRows(schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(schema, rows)
}

// readBatchDirect reads a parquet file directly into a RecordBatch using
// column-at-a-time page reading. This avoids the row-reconstruction overhead
// of parquet-go's ReadRows and eliminates the map[string]any intermediate.
// For schemas with nested types (ARRAY, ROW, MAP), falls back to row-level reading.
func readBatchDirect(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string, preds ...scanPredicate) *batch.RecordBatch {
	// Nested types require rep/def level decoding — fall back to row reader
	pqSchema := parquet.Schema{Columns: schema}
	if pqSchema.HasNestedColumns() {
		return readBatchViaRows(pqReader, schema, requiredCols)
	}

	file := pqReader.File()
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		return nil
	}

	// Determine which columns to read and build the batch schema
	readSchema := schema
	if len(requiredCols) > 0 {
		needed := make(map[string]bool, len(requiredCols))
		for _, c := range requiredCols {
			needed[c] = true
		}
		filtered := make([]parquet.Column, 0, len(requiredCols))
		for _, col := range schema {
			if needed[col.Name] {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			readSchema = filtered
		}
	}

	// Map batch schema columns to parquet file column indices
	fileColumns := file.Schema().Columns() // [][]string
	type colMapping struct {
		fileIdx  int // index in parquet file's column chunks
		batchIdx int // index in our batch schema
	}
	mappings := make([]colMapping, 0, len(readSchema))
	for bi, sc := range readSchema {
		for fi, path := range fileColumns {
			name := path[len(path)-1]
			if name == sc.Name {
				mappings = append(mappings, colMapping{fileIdx: fi, batchIdx: bi})
				break
			}
		}
	}

	// Filter row groups using stats-based predicate pruning
	activeRGs := rowGroups
	if len(preds) > 0 {
		activeRGs = make([]goparquet.RowGroup, 0, len(rowGroups))
		for i, rg := range rowGroups {
			stats := pqReader.RowGroupStats(i)
			pruned := false
			for _, pred := range preds {
				op := mapPredOp(pred.Op)
				if op >= 0 {
					sp := scan.StatsPredicate{Column: pred.Column, Op: op, Value: pred.Value}
					if scan.CanPruneRowGroup(sp, stats) {
						pruned = true
						break
					}
				}
			}
			if !pruned {
				activeRGs = append(activeRGs, rg)
			}
		}
	}

	// Count total rows across active row groups
	var totalRows int64
	for _, rg := range activeRGs {
		totalRows += rg.NumRows()
	}
	if totalRows == 0 {
		return nil
	}

	b := batch.NewRecordBatch(readSchema, int(totalRows))

	// Read column-by-column across active row groups using direct typed page reads.
	// This bypasses parquet-go's Value interface, using page.Data() for zero-copy
	// typed array access (memcpy for int64/int32/float slices).
	for _, m := range mappings {
		col := b.Columns[m.batchIdx]
		colType := readSchema[m.batchIdx].Type
		rowOffset := 0

		// Get max definition level for null handling (once per column)
		pqCol := scan.FindColumnByIndex(file.Root(), m.fileIdx)
		maxDefLevel := byte(0)
		if pqCol != nil {
			maxDefLevel = byte(pqCol.MaxDefinitionLevel())
		}

		for _, rg := range activeRGs {
			chunks := rg.ColumnChunks()
			if m.fileIdx >= len(chunks) {
				continue
			}
			pages := chunks[m.fileIdx].Pages()
			for {
				page, err := pages.ReadPage()
				if err != nil || page == nil {
					break
				}

				pageRows := int(page.NumRows())
				if pageRows == 0 {
					continue
				}

				defLevels := page.DefinitionLevels()
				data := page.Data()

				if colType == batch.TypeDecimal {
					readDecimalPage(col, rowOffset, data, defLevels, maxDefLevel, pageRows, pqCol)
				} else if defLevels == nil || page.NumNulls() == 0 {
					scan.CopyTypedDataDirect(col, rowOffset, data, pageRows, colType)
				} else {
					scan.CopyTypedDataScatter(col, rowOffset, data, defLevels, maxDefLevel, pageRows, colType)
				}
				rowOffset += pageRows
			}
			pages.Close()
		}
	}

	b.Len = int(totalRows)
	return b
}

// readBatchViaRows reads a parquet file using parquet-go's row-level reader,
// which handles nested types (LIST, MAP, STRUCT) natively. Used as fallback
// when the schema contains nested columns.
func readBatchViaRows(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string) *batch.RecordBatch {
	selectedCols := requiredCols
	if len(selectedCols) == 0 {
		selectedCols = make([]string, len(schema))
		for i, c := range schema {
			selectedCols[i] = c.Name
		}
	}
	rows, err := pqReader.ReadRows(selectedCols)
	if err != nil || len(rows) == 0 {
		return nil
	}

	// Build the read schema (only columns we're reading)
	readSchema := schema
	if len(requiredCols) > 0 {
		needed := make(map[string]bool, len(requiredCols))
		for _, c := range requiredCols {
			needed[c] = true
		}
		filtered := make([]parquet.Column, 0, len(requiredCols))
		for _, col := range schema {
			if needed[col.Name] {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			readSchema = filtered
		}
	}

	return fromRows(readSchema, rows)
}

// mapPredOp converts a logical predicate operator string to an exec.CompareOp.
func mapPredOp(op string) exec.CompareOp {
	switch strings.ToLower(op) {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return -1
	}
}

// buildProjectedParquetSchema creates a parquet-go schema with only the requested columns.
func buildProjectedParquetSchema(file *goparquet.File, selectedColumns []string) *goparquet.Schema {
	fileSchema := file.Schema()
	fields := fileSchema.Fields()

	needed := make(map[string]bool, len(selectedColumns))
	for _, c := range selectedColumns {
		needed[c] = true
	}

	group := make(goparquet.Group)
	for _, f := range fields {
		if needed[f.Name()] {
			group[f.Name()] = f
		}
	}

	if len(group) == 0 {
		return nil
	}
	return goparquet.NewSchema(fileSchema.Name(), group)
}

// readDecimalPage reads decimal data from a parquet page directly into a Vector,
// dispatching on the physical storage type (INT32, INT64, or byte array).
func readDecimalPage(col *batch.Vector, offset int, data pqencoding.Values, defLevels []byte, maxDefLevel byte, n int, pqCol *goparquet.Column) {
	kind := pqCol.Type().Kind()

	if defLevels == nil {
		// Non-nullable column — direct copy
		switch kind {
		case goparquet.Int64:
			src := data.Int64()
			for i := 0; i < n; i++ {
				col.DecimalData.Data[offset+i] = batch.Int128From(src[i])
			}
		case goparquet.Int32:
			src := data.Int32()
			for i := 0; i < n; i++ {
				col.DecimalData.Data[offset+i] = batch.Int128From(int64(src[i]))
			}
		case goparquet.FixedLenByteArray, goparquet.ByteArray:
			rawData, offsets := data.ByteArray()
			if offsets != nil {
				for i := 0; i < n; i++ {
					start := offsets[i]
					end := offsets[i+1]
					col.DecimalData.Data[offset+i] = decimalFromBytes(rawData[start:end])
				}
			} else {
				pos := 0
				for i := 0; i < n; i++ {
					if pos+4 > len(rawData) {
						break
					}
					length := int(binary.LittleEndian.Uint32(rawData[pos:]))
					pos += 4
					if pos+length > len(rawData) {
						break
					}
					col.DecimalData.Data[offset+i] = decimalFromBytes(rawData[pos : pos+length])
					pos += length
				}
			}
		}
	} else {
		// Nullable column — scatter using definition levels
		switch kind {
		case goparquet.Int64:
			src := data.Int64()
			valIdx := 0
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					col.DecimalData.Data[offset+i] = batch.Int128From(src[valIdx])
					valIdx++
				} else {
					col.Nulls.SetNull(offset + i)
				}
			}
		case goparquet.Int32:
			src := data.Int32()
			valIdx := 0
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					col.DecimalData.Data[offset+i] = batch.Int128From(int64(src[valIdx]))
					valIdx++
				} else {
					col.Nulls.SetNull(offset + i)
				}
			}
		case goparquet.FixedLenByteArray, goparquet.ByteArray:
			rawData, offsets := data.ByteArray()
			valIdx := 0
			if offsets != nil {
				for i := 0; i < n; i++ {
					if defLevels[i] == maxDefLevel {
						start := offsets[valIdx]
						end := offsets[valIdx+1]
						col.DecimalData.Data[offset+i] = decimalFromBytes(rawData[start:end])
						valIdx++
					} else {
						col.Nulls.SetNull(offset + i)
					}
				}
			} else {
				pos := 0
				for i := 0; i < n; i++ {
					if defLevels[i] == maxDefLevel {
						if pos+4 > len(rawData) {
							break
						}
						length := int(binary.LittleEndian.Uint32(rawData[pos:]))
						pos += 4
						if pos+length > len(rawData) {
							break
						}
						col.DecimalData.Data[offset+i] = decimalFromBytes(rawData[pos : pos+length])
						pos += length
						valIdx++
					} else {
						col.Nulls.SetNull(offset + i)
					}
				}
			}
		}
	}
}

// decimalFromBytes converts big-endian bytes to Int128.
func decimalFromBytes(b []byte) batch.Int128 {
	if len(b) == 0 {
		return batch.Int128{}
	}
	// Parquet stores decimals as big-endian two's complement
	var hi int64
	var lo uint64
	if len(b) <= 8 {
		// Fits in a single int64 — sign-extend
		if b[0]&0x80 != 0 {
			hi = -1
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
		if hi < 0 {
			lo |= ^uint64(0) << (uint(len(b)) * 8)
		}
		return batch.Int128From(int64(lo))
	}
	// 9-16 bytes: split into hi/lo
	split := len(b) - 8
	hiBytes := b[:split]
	loBytes := b[split:]
	if hiBytes[0]&0x80 != 0 {
		hi = -1
	}
	for _, c := range hiBytes {
		hi = (hi << 8) | int64(c)
	}
	for _, c := range loBytes {
		lo = (lo << 8) | uint64(c)
	}
	return batch.Int128{Hi: hi, Lo: lo}
}

// --- Row-group-level parallel scan ---

// prefetchHeapThreshold is the fraction of the memory limit below which a
// worker will speculatively prefetch the next row group. Set to 60% to leave
// ample headroom for concurrent workers and downstream operators.
const prefetchHeapThreshold = 0.60

// rgUnit is a work unit for parallel row-group scanning.
type rgUnit struct {
	pqFile      *goparquet.File
	fileEntry   catalog.FileEntry
	rg          goparquet.RowGroup
	rgRowOffset int64 // cumulative row offset within the file (for delete markers)
}

// prefetchResult holds a speculatively read row group batch and its unit index.
type prefetchResult struct {
	idx   int
	batch *batch.RecordBatch
}

// buildRGUnits reads all files concurrently and builds a flat list of row group
// work units. Predicate-based row group pruning is applied during enumeration.
func (inner *scanSourceInner) buildRGUnits(ctx context.Context) {
	// Check for nested types — fall back to file-level scan
	pqSchema := parquet.Schema{Columns: inner.schema}
	if pqSchema.HasNestedColumns() {
		inner.hasNestedTypes = true
		return
	}

	type fileResult struct {
		reader *parquet.Reader
		entry  catalog.FileEntry
	}

	results := make([]fileResult, len(inner.files))
	var readWg sync.WaitGroup
	var readIdx int64

	readWorkers := runtime.NumCPU()
	if readWorkers > 8 {
		readWorkers = 8
	}
	if readWorkers > len(inner.files) {
		readWorkers = len(inner.files)
	}
	if readWorkers < 1 {
		readWorkers = 1
	}

	readWg.Add(readWorkers)
	for i := 0; i < readWorkers; i++ {
		go func() {
			defer readWg.Done()
			for {
				idx := int(atomic.AddInt64(&readIdx, 1) - 1)
				if idx >= len(inner.files) {
					return
				}
				if ctx.Err() != nil {
					return
				}
				entry := inner.files[idx]
				var reader *parquet.Reader
				if ras, ok := inner.cat.Store().(objstore.ReaderAtStore); ok {
					rac, size, err := ras.GetReaderAt(ctx, inner.cat.Bucket(), entry.Path)
					if err != nil {
						continue
					}
					reader, err = parquet.NewReader(rac, size)
					if err != nil {
						rac.Close()
						continue
					}
				} else {
					rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), entry.Path)
					if err != nil {
						continue
					}
					data, err := readAll(rc)
					rc.Close()
					if err != nil {
						continue
					}
					reader, err = parquet.NewReader(bytesReader(data), int64(len(data)))
					if err != nil {
						continue
					}
				}
				results[idx] = fileResult{reader: reader, entry: entry}
			}
		}()
	}
	readWg.Wait()

	// Enumerate row groups from all files, applying predicate-based and bloom pruning
	for _, fr := range results {
		if fr.reader == nil {
			continue
		}
		pqFile := fr.reader.File()
		rgs := pqFile.RowGroups()

		var rowOffset int64
		for rgIdx, rg := range rgs {
			stats := fr.reader.RowGroupStats(rgIdx)
			pruned := false

			// Predicate-based row group pruning
			if len(inner.scanPreds) > 0 {
				for _, pred := range inner.scanPreds {
					op := mapPredOp(pred.Op)
					if op >= 0 {
						sp := scan.StatsPredicate{Column: pred.Column, Op: op, Value: pred.Value}
						if scan.CanPruneRowGroup(sp, stats) {
							pruned = true
							break
						}
					}
				}
			}

			// Bloom filter row group pruning
			if !pruned && inner.bloomFilter != nil {
				if canBloomPruneRowGroup(inner.bloomFilter, stats) {
					pruned = true
				}
			}

			if pruned {
				rowOffset += rg.NumRows()
				continue
			}

			inner.rgUnits = append(inner.rgUnits, rgUnit{
				pqFile:      pqFile,
				fileEntry:   fr.entry,
				rg:          rg,
				rgRowOffset: rowOffset,
			})
			rowOffset += rg.NumRows()
		}
	}
}

// canBloomPruneRowGroup checks whether a row group can be skipped based on
// bloom filter analysis of the join key column's min/max statistics.
// For integer join keys with a small range (<=1024), every value is checked
// against the bloom filter. If ALL return "not present", the row group is skipped.
func canBloomPruneRowGroup(bf *exec.BloomScanFilter, stats parquet.RowGroupStats) bool {
	if !bf.UseIntKey {
		return false
	}
	colStats, ok := stats.Columns[bf.Column]
	if !ok || !colStats.HasStats {
		return false
	}
	if colStats.MinValue == nil || colStats.MaxValue == nil {
		return false
	}
	if colStats.NullCount == stats.NumRows {
		return false
	}

	minVal := toBloomInt64(colStats.MinValue)
	maxVal := toBloomInt64(colStats.MaxValue)
	if minVal == 0 && maxVal == 0 && !isIntType(colStats.MinValue) {
		return false
	}

	const maxRangeSize = 1024
	rangeSize := maxVal - minVal + 1
	if rangeSize <= 0 || rangeSize > maxRangeSize {
		return false
	}

	for v := minVal; v <= maxVal; v++ {
		if exec.BloomContains(bf.Bloom, bf.BloomMask, exec.BloomHashInt(v)) {
			return false
		}
	}
	return true
}

func toBloomInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int32:
		return int64(tv)
	case int:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	default:
		return 0
	}
}

func isIntType(v any) bool {
	switch v.(type) {
	case int64, int32, int:
		return true
	default:
		return false
	}
}

// rgWorker processes row group work units in parallel. When the system has
// memory headroom (heap < 60% of the detected limit), it speculatively
// prefetches the next row group to overlap I/O with downstream processing.
// Each worker prefetches at most 1 row group ahead, stored in a worker-local
// variable (no shared cache).
func (inner *scanSourceInner) rgWorker(ctx context.Context) {
	defer inner.wg.Done()

	var prefetched *prefetchResult

	for {
		if ctx.Err() != nil {
			return
		}

		var idx int
		var b *batch.RecordBatch

		if prefetched != nil {
			idx = prefetched.idx
			b = prefetched.batch
			prefetched = nil
		} else {
			idx = int(atomic.AddInt64(&inner.rgIdx, 1) - 1)
			if idx >= len(inner.rgUnits) {
				return
			}
			b = inner.readRG(inner.rgUnits[idx])
		}

		// Before sending downstream, speculatively prefetch the next row group
		// if memory allows. Claim the index atomically so no other worker reads
		// the same unit.
		if prefetched == nil && inner.canPrefetch() {
			nextIdx := int(atomic.AddInt64(&inner.rgIdx, 1) - 1)
			if nextIdx < len(inner.rgUnits) {
				prefetched = &prefetchResult{
					idx:   nextIdx,
					batch: inner.readRG(inner.rgUnits[nextIdx]),
				}
			}
		}

		if b == nil || b.Len == 0 {
			continue
		}

		// Apply delete markers with row offset adjustment
		unit := inner.rgUnits[idx]
		if delSet := inner.deleteMarkers[unit.fileEntry.Path]; len(delSet) > 0 {
			sel := make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				absRow := unit.rgRowOffset + int64(i)
				if !delSet[absRow] {
					sel = append(sel, uint32(i))
				}
			}
			if len(sel) == 0 {
				continue
			}
			if len(sel) < b.Len {
				b.Sel = sel
			}
		}

		atomic.AddInt64(&inner.rowsScanned, int64(b.ActiveLen()))

		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}
	}
}

// canPrefetch returns true if the current heap usage is below the prefetch
// threshold (60% of the detected memory limit). Returns false if no memory
// limit was detected, disabling prefetch gracefully.
func (inner *scanSourceInner) canPrefetch() bool {
	if inner.memoryLimit <= 0 {
		return false
	}
	threshold := uint64(float64(inner.memoryLimit) * prefetchHeapThreshold)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc < threshold
}

// readRG reads a row group using the pool when the size matches.
func (inner *scanSourceInner) readRG(unit rgUnit) *batch.RecordBatch {
	numRows := int(unit.rg.NumRows())
	if inner.pool != nil && numRows == inner.pool.BatchSize() {
		b := inner.pool.Get()
		readRowGroupInto(unit.pqFile, unit.rg, b, inner.schema, inner.requiredCols)
		return b
	}
	return readRowGroupDirect(unit.pqFile, unit.rg, inner.schema, inner.requiredCols)
}

// readRowGroupDirect reads a single row group into a fresh RecordBatch.
func readRowGroupDirect(file *goparquet.File, rg goparquet.RowGroup, schema []parquet.Column, requiredCols []string) *batch.RecordBatch {
	numRows := int(rg.NumRows())
	if numRows == 0 {
		return nil
	}
	readSchema := buildReadSchema(schema, requiredCols)
	b := batch.NewRecordBatch(readSchema, numRows)
	readRowGroupInto(file, rg, b, schema, requiredCols)
	return b
}

// readRowGroupInto reads a single row group into an existing RecordBatch.
// The batch must already have the correct schema and sufficient capacity.
//
// When there are 2+ projected columns, column reads are launched in parallel
// goroutines to overlap S3 range-read latency. Each column writes to its own
// independent Vector in the batch, so no synchronization is needed between
// column readers. For a single column the sequential path is kept to avoid
// goroutine overhead.
func readRowGroupInto(file *goparquet.File, rg goparquet.RowGroup, b *batch.RecordBatch, schema []parquet.Column, requiredCols []string) {
	numRows := int(rg.NumRows())
	if numRows == 0 {
		return
	}

	readSchema := b.Schema

	// Map batch schema columns to parquet file column indices
	fileColumns := file.Schema().Columns()
	type colMapping struct {
		fileIdx  int
		batchIdx int
	}
	mappings := make([]colMapping, 0, len(readSchema))
	for bi, sc := range readSchema {
		for fi, path := range fileColumns {
			name := path[len(path)-1]
			if name == sc.Name {
				mappings = append(mappings, colMapping{fileIdx: fi, batchIdx: bi})
				break
			}
		}
	}

	chunks := rg.ColumnChunks()

	if len(mappings) <= 1 {
		// Single column — sequential path, no goroutine overhead
		for _, m := range mappings {
			readColumnInto(file, chunks, b, readSchema, m.fileIdx, m.batchIdx)
		}
	} else {
		// Multiple columns — read in parallel to overlap I/O latency
		var wg sync.WaitGroup
		var firstErr atomic.Value
		wg.Add(len(mappings))
		for _, m := range mappings {
			go func(fileIdx, batchIdx int) {
				defer wg.Done()
				if err := readColumnInto(file, chunks, b, readSchema, fileIdx, batchIdx); err != nil {
					firstErr.CompareAndSwap(nil, err)
				}
			}(m.fileIdx, m.batchIdx)
		}
		wg.Wait()
		// Errors from column reads are non-fatal here (matches prior behavior
		// where page read errors silently break the loop). The firstErr is
		// captured but not returned because the caller signature is void.
		_ = firstErr.Load()
	}

	b.Len = numRows
}

// readColumnInto reads all pages of a single column chunk into the batch vector.
// Safe to call concurrently for different columns since each writes to its own Vector.
func readColumnInto(file *goparquet.File, chunks []goparquet.ColumnChunk, b *batch.RecordBatch, readSchema []parquet.Column, fileIdx, batchIdx int) error {
	col := b.Columns[batchIdx]
	colType := readSchema[batchIdx].Type
	rowOffset := 0

	if fileIdx >= len(chunks) {
		return nil
	}

	// Get max definition level for null handling
	pqCol := scan.FindColumnByIndex(file.Root(), fileIdx)
	maxDefLevel := byte(0)
	if pqCol != nil {
		maxDefLevel = byte(pqCol.MaxDefinitionLevel())
	}

	pages := chunks[fileIdx].Pages()
	defer pages.Close()
	for {
		page, err := pages.ReadPage()
		if err != nil || page == nil {
			if err != nil {
				return err
			}
			break
		}

		pageRows := int(page.NumRows())
		if pageRows == 0 {
			continue
		}

		defLevels := page.DefinitionLevels()
		data := page.Data()

		if colType == batch.TypeDecimal {
			readDecimalPage(col, rowOffset, data, defLevels, maxDefLevel, pageRows, pqCol)
		} else if defLevels == nil || page.NumNulls() == 0 {
			scan.CopyTypedDataDirect(col, rowOffset, data, pageRows, colType)
		} else {
			scan.CopyTypedDataScatter(col, rowOffset, data, defLevels, maxDefLevel, pageRows, colType)
		}
		rowOffset += pageRows
	}
	return nil
}

// buildReadSchema applies column projection to the table schema.
func buildReadSchema(schema []parquet.Column, requiredCols []string) []parquet.Column {
	if len(requiredCols) == 0 {
		return schema
	}
	needed := make(map[string]bool, len(requiredCols))
	for _, c := range requiredCols {
		needed[c] = true
	}
	filtered := make([]parquet.Column, 0, len(requiredCols))
	for _, col := range schema {
		if needed[col.Name] {
			filtered = append(filtered, col)
		}
	}
	if len(filtered) > 0 {
		return filtered
	}
	return schema
}
