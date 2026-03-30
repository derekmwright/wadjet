// Package scan provides table scanning with 3-level predicate pushdown.
package scan

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/storage/partition"
)

// fileReadPool reuses read buffers across Parquet file reads to avoid
// repeated large allocations and GC pressure during sequential scans.
var fileReadPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024)) // 4MB default
	},
}

// ScanStats tracks scan statistics.
type ScanStats struct {
	TotalPartitions  int
	PrunedPartitions int
	TotalFiles       int
	PrunedFiles      int
	TotalRowGroups   int
	PrunedRowGroups  int
	RowsScanned      int64
}

// PartitionFilter filters partitions based on partition key values.
type PartitionFilter map[string]string

// Scanner performs table scans with 3-level predicate pushdown.
type Scanner struct {
	cat             *catalog.Catalog
	tableName       string
	selectedColumns []string
	partFilter      PartitionFilter
	rowFilter       exec.Predicate
	statsPredicates []StatsPredicate // Level 2: row-group stats predicates
	logger          *slog.Logger

	// Internal state
	files         []scanFile
	fileIdx       int
	stats         ScanStats
	schema        []pqt.Column
	useReaderAt   bool // true if store supports random access (column pruning)
	deleteMarkers map[string]map[int64]bool // file path -> set of row indices to skip
	scanPool      *batch.BatchPool // reuse batch allocations across files

	// Async prefetch: downloads files ahead while current is being decoded.
	// Only used when NOT using ReaderAt path.
	prefetchCh  chan prefetchedFile
	prefetchIdx int // next file index to start prefetching

	// ReaderAt prefetch: opens next files' ReaderAt handles in the background
	// to overlap S3 metadata reads with decode of the current file.
	raPrefetchCh  chan prefetchedReaderAt
	raPrefetchIdx int
}

type prefetchedReaderAt struct {
	file scanFile
	ra   objstore.ReaderAtCloser
	size int64
	err  error
}

type prefetchedFile struct {
	file scanFile
	buf  *bytes.Buffer
	err  error
}

type scanFile struct {
	path       string
	partValues map[string]string
}

// NewScanner creates a new scanner for the given table.
func NewScanner(cat *catalog.Catalog, tableName string) *Scanner {
	return &Scanner{
		cat:       cat,
		tableName: tableName,
		logger:    slog.Default(),
	}
}

// WithColumns sets the columns to select (projection pushdown).
func (s *Scanner) WithColumns(cols []string) *Scanner {
	s.selectedColumns = cols
	return s
}

// WithPartitionFilter sets the partition filter (Level 1: partition pruning).
func (s *Scanner) WithPartitionFilter(filter PartitionFilter) *Scanner {
	s.partFilter = filter
	return s
}

// WithRowFilter sets the row-level filter predicate (Level 3: row-level evaluation).
func (s *Scanner) WithRowFilter(pred exec.Predicate) *Scanner {
	s.rowFilter = pred
	return s
}

// WithStatsPredicates sets predicates for Level 2 row-group pruning.
func (s *Scanner) WithStatsPredicates(preds []StatsPredicate) *Scanner {
	s.statsPredicates = preds
	return s
}

// Stats returns scan statistics after scanning.
func (s *Scanner) Stats() ScanStats {
	return s.stats
}

func (s *Scanner) Init(ctx context.Context) error {
	manifest, err := s.cat.GetManifest(ctx, s.tableName)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	tableMeta, err := s.cat.GetTable(ctx, s.tableName)
	if err != nil {
		return fmt.Errorf("reading table meta: %w", err)
	}
	s.schema = tableMeta.Schema.Columns

	s.stats.TotalPartitions = len(manifest.Partitions)

	// Load delete markers for merge-on-read deletes
	if len(manifest.DeleteMarkers) > 0 {
		s.deleteMarkers = make(map[string]map[int64]bool, len(manifest.DeleteMarkers))
		for _, dm := range manifest.DeleteMarkers {
			idxSet := make(map[int64]bool, len(dm.RowIndices))
			for _, idx := range dm.RowIndices {
				idxSet[idx] = true
			}
			s.deleteMarkers[dm.FilePath] = idxSet
		}
	}

	// Level 1: Partition pruning
	for _, part := range manifest.Partitions {
		if len(s.partFilter) > 0 && !partition.MatchesFilter(part.Values, map[string]string(s.partFilter)) {
			s.stats.PrunedPartitions++
			continue
		}

		for _, file := range part.Files {
			s.stats.TotalFiles++
			s.files = append(s.files, scanFile{
				path:       file.Path,
				partValues: part.Values,
			})
		}
	}

	s.logger.Info("scan plan",
		"table", s.tableName,
		"partitions", s.stats.TotalPartitions,
		"pruned_partitions", s.stats.PrunedPartitions,
		"files_to_scan", len(s.files),
	)

	// Use random-access reads whenever the store supports it. Even without
	// column projection, ReaderAt avoids buffering the entire file in memory
	// before decode — parquet-go reads metadata first, then only the needed
	// column chunks via targeted ReadAt calls (S3 range requests).
	_, s.useReaderAt = s.cat.Store().(objstore.ReaderAtStore)

	// Create batch pool for reuse across files — avoids re-allocating vectors
	// for every file read. 128K rows covers the default row group size.
	scanSchema := s.schema
	if len(s.selectedColumns) > 0 {
		scanSchema = filterSchema(s.schema, s.selectedColumns)
	}
	s.scanPool = batch.NewBatchPool(scanSchema, 128*1024)

	// Start prefetching files ahead to hide S3 latency.
	if len(s.files) > 0 {
		if s.useReaderAt {
			// ReaderAt path: prefetch opens S3 handles and reads metadata ahead.
			depth := 2
			if len(s.files) < depth {
				depth = len(s.files)
			}
			s.raPrefetchCh = make(chan prefetchedReaderAt, depth)
			for i := 0; i < depth; i++ {
				s.startReaderAtPrefetch(ctx, i)
			}
			s.raPrefetchIdx = depth
		} else {
			// Full-download path: prefetch downloads entire files ahead.
			depth := 4
			if len(s.files) < depth {
				depth = len(s.files)
			}
			s.prefetchCh = make(chan prefetchedFile, depth)
			for i := 0; i < depth; i++ {
				s.startPrefetch(ctx, i)
			}
			s.prefetchIdx = depth
		}
	}

	return nil
}

// startPrefetch downloads file at idx in a background goroutine.
func (s *Scanner) startPrefetch(ctx context.Context, idx int) {
	file := s.files[idx]
	go func() {
		buf := fileReadPool.Get().(*bytes.Buffer)
		buf.Reset()

		rc, _, err := s.cat.Store().Get(ctx, s.cat.Bucket(), file.path)
		if err != nil {
			s.prefetchCh <- prefetchedFile{file: file, buf: buf, err: err}
			return
		}
		_, err = io.Copy(buf, rc)
		rc.Close()
		s.prefetchCh <- prefetchedFile{file: file, buf: buf, err: err}
	}()
}

// startReaderAtPrefetch opens a ReaderAt handle for the file at idx in the
// background. For S3, this initiates the connection and downloads metadata.
func (s *Scanner) startReaderAtPrefetch(ctx context.Context, idx int) {
	file := s.files[idx]
	go func() {
		ras := s.cat.Store().(objstore.ReaderAtStore)
		ra, size, err := ras.GetReaderAt(ctx, s.cat.Bucket(), file.path)
		s.raPrefetchCh <- prefetchedReaderAt{file: file, ra: ra, size: size, err: err}
	}()
}

func (s *Scanner) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for s.fileIdx < len(s.files) {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("scan cancelled: %w", err)
		}

		file := s.files[s.fileIdx]
		s.fileIdx++

		var b *batch.RecordBatch
		var err error

		if s.useReaderAt && s.raPrefetchCh != nil {
			// ReaderAt path with prefetch
			var pra prefetchedReaderAt
			select {
			case pra = <-s.raPrefetchCh:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			// Start prefetching the next file
			if s.raPrefetchIdx < len(s.files) {
				s.startReaderAtPrefetch(ctx, s.raPrefetchIdx)
				s.raPrefetchIdx++
			}

			if pra.err != nil {
				if pra.err == objstore.ErrNotFound {
					s.logger.Warn("file not found, skipping", "path", pra.file.path)
					continue
				}
				return nil, fmt.Errorf("opening file %s: %w", pra.file.path, pra.err)
			}

			b, err = s.decodeReaderAt(ctx, pra.ra, pra.size, pra.file)
			pra.ra.Close()
		} else if s.useReaderAt {
			b, err = s.readFileReaderAt(ctx, file)
		} else {
			// Full-download path with async prefetch
			var pf prefetchedFile
			select {
			case pf = <-s.prefetchCh:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			if s.prefetchIdx < len(s.files) {
				s.startPrefetch(ctx, s.prefetchIdx)
				s.prefetchIdx++
			}

			if pf.err != nil {
				fileReadPool.Put(pf.buf)
				if pf.err == objstore.ErrNotFound {
					s.logger.Warn("file not found, skipping", "path", pf.file.path)
					continue
				}
				return nil, fmt.Errorf("reading file %s: %w", pf.file.path, pf.err)
			}

			b, err = s.decodeFile(ctx, pf.buf, pf.file)
			fileReadPool.Put(pf.buf)
		}

		if err != nil {
			return nil, fmt.Errorf("reading file %s: %w", file.path, err)
		}
		if b == nil || b.ActiveLen() == 0 {
			continue
		}
		return b, nil
	}
	return nil, nil
}

func (s *Scanner) Close() error { return nil }

// readFileReaderAt opens a Parquet file via random-access reads and decodes only
// the requested column chunks. For S3 stores, this issues targeted range requests
// instead of downloading the entire file.
func (s *Scanner) readFileReaderAt(ctx context.Context, file scanFile) (*batch.RecordBatch, error) {
	ras := s.cat.Store().(objstore.ReaderAtStore)
	ra, size, err := ras.GetReaderAt(ctx, s.cat.Bucket(), file.path)
	if err != nil {
		if err == objstore.ErrNotFound {
			s.logger.Warn("file not found, skipping", "path", file.path)
			return nil, nil
		}
		return nil, err
	}
	defer ra.Close()

	return s.decodeReaderAt(ctx, ra, size, file)
}

// decodeReaderAt decodes a Parquet file from a pre-opened ReaderAt handle.
func (s *Scanner) decodeReaderAt(ctx context.Context, ra io.ReaderAt, size int64, file scanFile) (*batch.RecordBatch, error) {
	fr, err := pqt.OpenFileReader(ra, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet reader: %w", err)
	}

	return s.decodeRowGroupsNative(ctx, fr, file)
}

// decodeFile decodes a Parquet file from a pre-downloaded buffer into a RecordBatch.
func (s *Scanner) decodeFile(ctx context.Context, buf *bytes.Buffer, file scanFile) (*batch.RecordBatch, error) {
	data := buf.Bytes()

	fr, err := pqt.OpenFileReaderFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("opening parquet reader: %w", err)
	}

	return s.decodeRowGroupsNative(ctx, fr, file)
}

// decodeRowGroupsNative reads row groups using our custom FileReader,
// applying stats-based pruning and row-level filtering.
func (s *Scanner) decodeRowGroupsNative(ctx context.Context, fr *pqt.FileReader, file scanFile) (*batch.RecordBatch, error) {
	schema := s.schema
	if len(s.selectedColumns) > 0 {
		schema = filterSchema(s.schema, s.selectedColumns)
	}

	numRGs := fr.NumRowGroups()
	s.stats.TotalRowGroups += numRGs

	var batches []*batch.RecordBatch
	for rgIdx := 0; rgIdx < numRGs; rgIdx++ {
		if len(s.statsPredicates) > 0 {
			rgStats := fr.RowGroupStats(rgIdx)
			pruned := false
			for _, pred := range s.statsPredicates {
				if CanPruneRowGroup(pred, rgStats) {
					pruned = true
					break
				}
			}
			if pruned {
				s.stats.PrunedRowGroups++
				continue
			}
		}

		b, err := ReadRowGroupNative(fr, rgIdx, schema, s.scanPool)
		if err != nil {
			return nil, fmt.Errorf("reading row group %d: %w", rgIdx, err)
		}
		if b == nil {
			continue
		}
		s.stats.RowsScanned += int64(b.Len)
		batches = append(batches, b)
	}

	return s.mergeBatchesAndFilter(ctx, schema, batches, file)
}

// decodeRowGroups reads row groups from a parquet reader, applying stats-based
// pruning and row-level filtering. Delegates to the native FileReader path.
func (s *Scanner) decodeRowGroups(ctx context.Context, reader *pqt.Reader, file scanFile) (*batch.RecordBatch, error) {
	fr := reader.FileReader()
	return s.decodeRowGroupsNative(ctx, fr, file)
}

// mergeBatchesAndFilter merges row-group batches into a single RecordBatch,
// applies delete markers and row-level filtering. Shared by both the native
// and parquet-go decode paths.
func (s *Scanner) mergeBatchesAndFilter(ctx context.Context, schema []pqt.Column, batches []*batch.RecordBatch, file scanFile) (*batch.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, nil
	}

	var result *batch.RecordBatch
	if len(batches) == 1 {
		result = batches[0]
	} else {
		totalRows := 0
		for _, b := range batches {
			totalRows += b.Len
		}
		if s.scanPool != nil {
			result = s.scanPool.GetForSize(totalRows)
		} else {
			result = batch.NewRecordBatch(schema, totalRows)
		}
		offset := 0
		for _, b := range batches {
			for j := range schema {
				copyVectorRange(result.Columns[j], offset, b.Columns[j], 0, b.Len)
			}
			offset += b.Len
		}
		// Release intermediate batches back to pool
		for _, b := range batches {
			b.Release()
		}
	}

	// Apply delete markers: skip rows marked for deletion (merge-on-read)
	if delSet := s.deleteMarkers[file.path]; len(delSet) > 0 {
		sel := make([]uint32, 0, result.Len)
		for i := 0; i < result.Len; i++ {
			if !delSet[int64(i)] {
				sel = append(sel, uint32(i))
			}
		}
		if len(sel) == 0 {
			return nil, nil
		}
		if len(sel) < result.Len {
			result.Sel = sel
		}
	}

	if s.rowFilter != nil {
		filter := exec.NewFilter(s.rowFilter)
		filtered, err := filter.Execute(ctx, result)
		if err != nil {
			return nil, fmt.Errorf("applying filter: %w", err)
		}
		return filtered, nil
	}

	return result, nil
}

// copyVectorRange copies count values from src[srcOff..] to dst[dstOff..] using
// bulk typed copies. One type switch per column instead of per row.
func copyVectorRange(dst *batch.Vector, dstOff int, src *batch.Vector, srcOff, count int) {
	// Copy null bitmap — dst starts as all-non-null from Reset/NewRecordBatch
	if src.Nulls.HasNulls() {
		for i := 0; i < count; i++ {
			if src.Nulls.IsNullFast(srcOff + i) {
				dst.Nulls.SetNull(dstOff + i)
			}
		}
	}
	// Bulk copy typed data
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

func filterSchema(schema []pqt.Column, selected []string) []pqt.Column {
	selSet := make(map[string]bool, len(selected))
	for _, s := range selected {
		selSet[s] = true
	}
	var result []pqt.Column
	for _, col := range schema {
		if selSet[col.Name] {
			result = append(result, col)
		}
	}
	return result
}
