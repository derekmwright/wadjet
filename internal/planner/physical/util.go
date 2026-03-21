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

// rgUnit is a work unit for parallel row-group scanning.
type rgUnit struct {
	pqFile      *goparquet.File
	fileEntry   catalog.FileEntry
	rg          goparquet.RowGroup
	rgRowOffset int64 // cumulative row offset within the file (for delete markers)
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

	readWorkers := runtime.NumCPU() * 2
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

	// Enumerate row groups from all files, applying predicate-based pruning
	for _, fr := range results {
		if fr.reader == nil {
			continue
		}
		pqFile := fr.reader.File()
		rgs := pqFile.RowGroups()

		var rowOffset int64
		for rgIdx, rg := range rgs {
			// Predicate-based row group pruning
			if len(inner.scanPreds) > 0 {
				stats := fr.reader.RowGroupStats(rgIdx)
				pruned := false
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
				if pruned {
					rowOffset += rg.NumRows()
					continue
				}
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

// rgWorker processes row group work units in parallel.
// Each worker claims units atomically and kicks off prefetches for upcoming
// units so their S3 data is already downloaded when a worker gets to them.
func (inner *scanSourceInner) rgWorker(ctx context.Context) {
	defer inner.wg.Done()

	for {
		idx := int(atomic.AddInt64(&inner.rgIdx, 1) - 1)
		if idx >= len(inner.rgUnits) {
			return
		}
		if ctx.Err() != nil {
			return
		}

		// Kick off async prefetch for a future row group. Each worker
		// prefetches the unit that is N positions ahead (where N is the
		// prefetch semaphore capacity), so when it loops back around
		// the data is already in the cache.
		inner.startPrefetch(ctx, idx)

		// Check if this unit was already prefetched
		unit := inner.rgUnits[idx]
		var b *batch.RecordBatch
		if cached, ok := inner.prefetchCache.LoadAndDelete(idx); ok {
			b = cached.(*batch.RecordBatch)
		} else {
			b = inner.readRG(unit)
		}

		if b == nil || b.Len == 0 {
			continue
		}

		// Apply delete markers with row offset adjustment
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

// startPrefetch asynchronously pre-reads an upcoming row group into the cache.
// The target index is `currentIdx + prefetchDistance` where prefetchDistance
// equals the semaphore capacity. This ensures each worker's next-likely unit
// is already being fetched by the time it finishes its current unit.
func (inner *scanSourceInner) startPrefetch(ctx context.Context, currentIdx int) {
	if inner.prefetchSem == nil {
		return
	}

	target := currentIdx + cap(inner.prefetchSem)
	if target >= len(inner.rgUnits) {
		return
	}

	// Don't prefetch if already cached
	if _, loaded := inner.prefetchCache.Load(target); loaded {
		return
	}

	// Try to acquire semaphore without blocking — if all slots are busy,
	// other goroutines are already prefetching so skip this one.
	select {
	case inner.prefetchSem <- struct{}{}:
	default:
		return
	}

	go func(idx int) {
		defer func() { <-inner.prefetchSem }()

		if ctx.Err() != nil {
			return
		}
		// Re-check: another worker may have already read or prefetched this unit
		if _, loaded := inner.prefetchCache.Load(idx); loaded {
			return
		}
		// Don't prefetch if a worker has already passed this index
		if int(atomic.LoadInt64(&inner.rgIdx)) > idx {
			return
		}
		unit := inner.rgUnits[idx]
		b := inner.readRG(unit)
		if b != nil && b.Len > 0 {
			inner.prefetchCache.Store(idx, b)
		}
	}(target)
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

	// readColumn reads all pages for a single column mapping into the batch.
	readColumn := func(m colMapping) {
		col := b.Columns[m.batchIdx]
		colType := readSchema[m.batchIdx].Type
		rowOffset := 0

		if m.fileIdx >= len(chunks) {
			return
		}

		// Get max definition level for null handling
		pqCol := scan.FindColumnByIndex(file.Root(), m.fileIdx)
		maxDefLevel := byte(0)
		if pqCol != nil {
			maxDefLevel = byte(pqCol.MaxDefinitionLevel())
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

	if len(mappings) >= 4 {
		// Parallel column reads — each column targets a different vector in the
		// batch so there are no data races. Each ReadPage triggers an S3 range
		// read, so overlapping the I/O across columns hides latency.
		var colWg sync.WaitGroup
		colWg.Add(len(mappings))
		for _, m := range mappings {
			go func(m colMapping) {
				defer colWg.Done()
				readColumn(m)
			}(m)
		}
		colWg.Wait()
	} else {
		for _, m := range mappings {
			readColumn(m)
		}
	}

	b.Len = numRows
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
