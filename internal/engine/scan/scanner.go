// Package scan provides table scanning with 3-level predicate pushdown.
package scan

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	pqt "github.com/derekmwright/caelum/internal/storage/parquet"
	"github.com/derekmwright/caelum/internal/storage/partition"
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
	files     []scanFile
	fileIdx   int
	stats     ScanStats
	schema    []pqt.Column
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

	return nil
}

func (s *Scanner) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for s.fileIdx < len(s.files) {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("scan cancelled: %w", err)
		}

		file := s.files[s.fileIdx]
		s.fileIdx++

		b, err := s.readFile(ctx, file)
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

func (s *Scanner) readFile(ctx context.Context, file scanFile) (*batch.RecordBatch, error) {
	rc, info, err := s.cat.Store().Get(ctx, s.cat.Bucket(), file.path)
	if err != nil {
		if err == objstore.ErrNotFound {
			s.logger.Warn("file not found, skipping", "path", file.path)
			return nil, nil
		}
		return nil, err
	}
	defer rc.Close()

	buf := fileReadPool.Get().(*bytes.Buffer)
	buf.Reset()
	if _, err := io.Copy(buf, rc); err != nil {
		fileReadPool.Put(buf)
		return nil, fmt.Errorf("reading file data: %w", err)
	}
	data := buf.Bytes()
	_ = info

	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		fileReadPool.Put(buf)
		return nil, fmt.Errorf("opening parquet reader: %w", err)
	}

	// Determine schema for the batch
	schema := s.schema
	if len(s.selectedColumns) > 0 {
		schema = filterSchema(s.schema, s.selectedColumns)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()

	// Level 2: Row-group pruning using column min/max stats
	numRGs := len(rgs)
	s.stats.TotalRowGroups += numRGs

	var batches []*batch.RecordBatch
	for rgIdx := 0; rgIdx < numRGs; rgIdx++ {
		// Stats-based pruning
		if len(s.statsPredicates) > 0 {
			rgStats := reader.RowGroupStats(rgIdx)
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

		// Columnar read: directly into RecordBatch, no map[string]any
		b, err := readRowGroupColumnar(rgs[rgIdx], schema, pqFile)
		if err != nil {
			return nil, fmt.Errorf("reading row group %d: %w", rgIdx, err)
		}
		if b == nil {
			continue
		}
		s.stats.RowsScanned += int64(b.Len)
		batches = append(batches, b)
	}

	// Return read buffer to pool — all batch data is now in columnar vectors
	fileReadPool.Put(buf)

	if len(batches) == 0 {
		return nil, nil
	}

	// For now, concatenate row groups into a single batch
	// (most files have a single row group at our target file sizes)
	var result *batch.RecordBatch
	if len(batches) == 1 {
		result = batches[0]
	} else {
		// Multiple row groups — merge into one batch
		totalRows := 0
		for _, b := range batches {
			totalRows += b.Len
		}
		result = batch.NewRecordBatch(schema, totalRows)
		offset := 0
		for _, b := range batches {
			for j := range schema {
				for i := 0; i < b.Len; i++ {
					copyVectorValue(result.Columns[j], offset+i, b.Columns[j], i)
				}
			}
			offset += b.Len
		}
	}

	// Level 3: Row-level filtering
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

// copyVectorValue copies a single value between vectors using typed access.
func copyVectorValue(dst *batch.Vector, di int, src *batch.Vector, si int) {
	if src.Nulls.IsNullFast(si) {
		dst.Nulls.SetNull(di)
		switch dst.Type {
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			dst.BytesData.Set(di, nil)
		}
		return
	}
	dst.Nulls.SetValid(di)
	switch dst.Type {
	case batch.TypeBool:
		dst.BoolData[di] = src.BoolData[si]
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		dst.Int32Data[di] = src.Int32Data[si]
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		dst.Int64Data[di] = src.Int64Data[si]
	case batch.TypeFloat32:
		dst.Float32Data[di] = src.Float32Data[si]
	case batch.TypeFloat64:
		dst.Float64Data[di] = src.Float64Data[si]
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.Set(di, src.BytesData.Value(si))
	case batch.TypeDecimal:
		dst.DecimalData.Data[di] = src.DecimalData.Data[si]
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
