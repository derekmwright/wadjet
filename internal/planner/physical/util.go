package physical

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func readAll(rc io.ReadCloser) ([]byte, error) {
	return io.ReadAll(rc)
}

// readBufPool reuses []byte buffers across queries to reduce allocation
// pressure on the GC. At SF1, file reads allocate ~7.6GB cumulatively
// across 22 TPC-H queries — pooling cuts this by 60-70% since the same
// Parquet files are re-read by multiple queries.
var readBufPool sync.Pool

// getReadBuf returns a []byte of at least size bytes from the pool,
// or allocates a new one.
func getReadBuf(size int) []byte {
	if v := readBufPool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

// putReadBuf returns a buffer to the pool for reuse. Only pool buffers
// above 64KB to avoid polluting the pool with tiny allocations.
func putReadBuf(buf []byte) {
	if cap(buf) >= 64*1024 {
		readBufPool.Put(buf[:cap(buf)])
	}
}

// readAllSized reads an io.ReadCloser into a pre-allocated buffer when the
// file size is known. Avoids io.ReadAll's grow-from-512-bytes pattern which
// causes O(log n) reallocations and copies for large files (e.g., 40MB Parquet
// file triggers ~17 doublings with ~80MB of intermediate copy overhead).
//
// When pooled is true, the returned buffer comes from readBufPool and must be
// returned via putReadBuf when no longer needed (after the parquet reader is
// closed). When false, the buffer is a fresh allocation.
func readAllSized(rc io.ReadCloser, sizeBytes int64, pooled bool) ([]byte, error) {
	if sizeBytes <= 0 {
		return io.ReadAll(rc)
	}
	var buf []byte
	if pooled {
		buf = getReadBuf(int(sizeBytes))
	} else {
		buf = make([]byte, sizeBytes)
	}
	n, err := io.ReadFull(rc, buf)
	if err == io.ErrUnexpectedEOF {
		// File was smaller than expected — return what we got
		return buf[:n], nil
	}
	if err != nil {
		if pooled {
			putReadBuf(buf)
		}
		return nil, err
	}
	// Read any remaining data beyond sizeBytes. This handles the case
	// where the catalog entry has an incorrect (stale) SizeBytes. Parquet
	// files have their footer at the end — truncating silently produces
	// unreadable files that are then silently skipped by buildRGUnits.
	extra, _ := io.ReadAll(rc)
	if len(extra) > 0 {
		// Extra data means size was wrong — can't reuse the pooled buffer
		// since append may reallocate.
		combined := make([]byte, n+len(extra))
		copy(combined, buf[:n])
		copy(combined[n:], extra)
		if pooled {
			putReadBuf(buf)
		}
		return combined, nil
	}
	return buf[:n], nil
}

func bytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}

func fromRows(schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(schema, rows)
}

// readBatchDirect reads a parquet file directly into a RecordBatch using
// column-at-a-time page reading via the native FileReader. This avoids
// parquet-go entirely. For schemas with nested types (ARRAY, MAP), falls
// back to row-level reading.
func readBatchDirect(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string, preds ...scanPredicate) *batch.RecordBatch {
	// Nested types: fall back to row reader
	pqSchema := parquet.Schema{Columns: schema}
	if pqSchema.HasNestedColumns() {
		return readBatchViaRows(pqReader, schema, requiredCols)
	}

	fr := pqReader.FileReader()
	numRGs := fr.NumRowGroups()
	if numRGs == 0 {
		return nil
	}

	readSchema := buildReadSchema(schema, requiredCols)

	// Collect active (non-pruned) row group indices.
	activeRGs := make([]int, 0, numRGs)
	for rgIdx := 0; rgIdx < numRGs; rgIdx++ {
		if len(preds) > 0 {
			stats := pqReader.RowGroupStats(rgIdx)
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
			if pruned {
				continue
			}
		}
		activeRGs = append(activeRGs, rgIdx)
	}

	if len(activeRGs) == 0 {
		return nil
	}

	// Count total rows across active row groups.
	var totalRows int64
	for _, rgIdx := range activeRGs {
		totalRows += fr.RowGroupNumRows(rgIdx)
	}
	if totalRows == 0 {
		return nil
	}

	// Read each active row group via the native reader and merge.
	var batches []*batch.RecordBatch
	for _, rgIdx := range activeRGs {
		b, err := scan.ReadRowGroupNative(fr, rgIdx, readSchema, nil)
		if err != nil {
			continue
		}
		if b != nil {
			batches = append(batches, b)
		}
	}

	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return batches[0]
	}

	result := batch.NewRecordBatch(readSchema, int(totalRows))
	offset := 0
	for _, b := range batches {
		for j := range readSchema {
			copyVecRange(result.Columns[j], offset, b.Columns[j], 0, b.Len)
		}
		offset += b.Len
	}
	result.Len = offset
	return result
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
	fileEntry    catalog.FileEntry
	rgRowOffset  int64 // cumulative row offset within the file (for delete markers)
	nativeReader *parquet.FileReader
	rgIndex      int // row group index within the native reader
	numRows      int64
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
		reader       *parquet.Reader
		nativeReader *parquet.FileReader
		entry        catalog.FileEntry
	}

	results := make([]fileResult, len(inner.files))
	var failedFiles atomic.Int64
	var firstErr atomic.Value // stores first error for diagnostics
	var readWg sync.WaitGroup
	var readIdx int64

	readWorkers := runtime.NumCPU()
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
				// Always download entire file: a single S3 GET is far cheaper
				// than N range reads per column page during rgWorker processing.
				rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), entry.Path)
				if err != nil {
					if failedFiles.Add(1) == 1 {
						firstErr.Store(fmt.Errorf("get %s: %w", entry.Path, err))
					}
					continue
				}
				data, err := readAllSized(rc, entry.SizeBytes, true)
				rc.Close()
				if err != nil {
					if failedFiles.Add(1) == 1 {
						firstErr.Store(fmt.Errorf("read %s: %w", entry.Path, err))
					}
					continue
				}
				inner.trackPooledBuf(data)
				reader, err := parquet.NewReaderFromBytes(data)
				if err != nil {
					if failedFiles.Add(1) == 1 {
						firstErr.Store(fmt.Errorf("parquet %s (%d bytes): %w", entry.Path, len(data), err))
					}
					continue
				}
				// The Reader wraps a FileReader using data directly (zero-copy).
				results[idx] = fileResult{reader: reader, nativeReader: reader.FileReader(), entry: entry}
			}
		}()
	}
	readWg.Wait()

	if failed := failedFiles.Load(); failed > 0 {
		inner.failedFiles = int(failed)
		if v := firstErr.Load(); v != nil {
			inner.firstFileErr = v.(error)
		}
	}

	// Enumerate row groups from all files, applying predicate-based and bloom pruning
	for _, fr := range results {
		if fr.reader == nil {
			continue
		}
		nativeRdr := fr.nativeReader
		if nativeRdr == nil {
			// If native reader failed, use the Reader's built-in FileReader.
			nativeRdr = fr.reader.FileReader()
		}
		numRGs := nativeRdr.NumRowGroups()

		var rowOffset int64
		for rgIdx := 0; rgIdx < numRGs; rgIdx++ {
			rgNumRows := nativeRdr.RowGroupNumRows(rgIdx)
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

			// Dynamic min/max range row group pruning
			if !pruned && len(inner.dynamicFilter) > 0 {
				if canRangePruneRowGroup(inner.dynamicFilter, stats) {
					pruned = true
				}
			}

			if pruned {
				rowOffset += rgNumRows
				continue
			}

			inner.rgUnits = append(inner.rgUnits, rgUnit{
				fileEntry:    fr.entry,
				rgRowOffset:  rowOffset,
				nativeReader: nativeRdr,
				rgIndex:      rgIdx,
				numRows:      rgNumRows,
			})
			rowOffset += rgNumRows
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

// canRangePruneRowGroup checks whether a row group can be skipped based on
// dynamic min/max range filters from the hash join build side. A row group
// is pruned if its value range has no overlap with the build-side range for
// any filter column.
func canRangePruneRowGroup(ranges []exec.DynamicRange, stats parquet.RowGroupStats) bool {
	for _, r := range ranges {
		colStats, ok := stats.Columns[r.Column]
		if !ok || !colStats.HasStats {
			continue
		}
		if colStats.MinValue == nil || colStats.MaxValue == nil {
			continue
		}
		if colStats.NullCount == stats.NumRows {
			continue
		}
		// No overlap: row group max < build min, or row group min > build max
		if scan.CompareValues(colStats.MaxValue, r.MinValue) < 0 {
			return true
		}
		if scan.CompareValues(colStats.MinValue, r.MaxValue) > 0 {
			return true
		}
	}
	return false
}

// rgWorker processes row group work units in parallel. Each worker
// speculatively prefetches one row group ahead AFTER sending the current
// batch downstream. This overlaps I/O with consumer processing without
// delaying batch delivery.
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

		// Send batch downstream FIRST, then prefetch while consumer processes.
		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}

		// Prefetch next row group while consumer processes the current one.
		// Each worker holds at most 1 extra row group (~10MB at SF10).
		// 8 workers * 1 RG = ~80MB overhead, negligible on a 32GB machine.
		if prefetched == nil {
			nextIdx := int(atomic.AddInt64(&inner.rgIdx, 1) - 1)
			if nextIdx < len(inner.rgUnits) {
				prefetched = &prefetchResult{
					idx:   nextIdx,
					batch: inner.readRG(inner.rgUnits[nextIdx]),
				}
			}
		}
	}
}

// readRG reads a row group using the native page decoder.
func (inner *scanSourceInner) readRG(unit rgUnit) *batch.RecordBatch {
	if unit.nativeReader == nil {
		return nil
	}
	b, err := scan.ReadRowGroupNative(unit.nativeReader, unit.rgIndex, inner.readSchema(), inner.pool)
	if err != nil {
		return nil
	}
	return b
}

// copyVecRange copies count values from src[srcOff..] to dst[dstOff..] using
// bulk typed copies. One type switch per column instead of per row.
func copyVecRange(dst *batch.Vector, dstOff int, src *batch.Vector, srcOff, count int) {
	if src.Nulls.HasNulls() {
		for i := 0; i < count; i++ {
			if src.Nulls.IsNullFast(srcOff + i) {
				dst.Nulls.SetNull(dstOff + i)
			}
		}
	}
	switch dst.Type {
	case batch.TypeBool:
		copy(dst.BoolData[dstOff:dstOff+count], src.BoolData[srcOff:srcOff+count])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		copy(dst.Int32Data[dstOff:dstOff+count], src.Int32Data[srcOff:srcOff+count])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		copy(dst.Int64Data[dstOff:dstOff+count], src.Int64Data[srcOff:srcOff+count])
	case batch.TypeFloat32:
		copy(dst.Float32Data[dstOff:dstOff+count], src.Float32Data[srcOff:srcOff+count])
	case batch.TypeFloat64:
		copy(dst.Float64Data[dstOff:dstOff+count], src.Float64Data[srcOff:srcOff+count])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.BulkCopy(dstOff, &src.BytesData, srcOff, count)
	case batch.TypeDecimal:
		copy(dst.DecimalData.Data[dstOff:dstOff+count], src.DecimalData.Data[srcOff:srcOff+count])
	}
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
