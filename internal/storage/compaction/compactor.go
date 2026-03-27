// Package compaction merges small Parquet files within a partition into
// larger files, reducing S3 list overhead and scan file-open costs.
package compaction

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Config controls compaction trigger thresholds and limits.
type Config struct {
	// MinFiles is the minimum file count per partition to trigger compaction.
	MinFiles int
	// MaxFileSizeBytes is the average size below which compaction triggers.
	MaxFileSizeBytes int64
	// MaxFilesPerPass caps the number of files merged in one compaction pass
	// to bound memory usage.
	MaxFilesPerPass int
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		MinFiles:         10,
		MaxFileSizeBytes: 32 * 1024 * 1024, // 32 MB
		MaxFilesPerPass:  50,
	}
}

// Compactor merges small Parquet files per partition.
type Compactor struct {
	catalog *catalog.Catalog
	logger  *slog.Logger
	config  Config
}

// New creates a compactor.
func New(cat *catalog.Catalog, logger *slog.Logger, cfg Config) *Compactor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Compactor{catalog: cat, logger: logger, config: cfg}
}

// Result summarizes one compaction pass for a table.
type Result struct {
	Table              string
	PartitionsCompacted int
	FilesRemoved       int
	FilesCreated       int
	RowsMerged         int64
	BytesBefore        int64
	BytesAfter         int64
}

// CompactTable runs compaction for all partitions of a table. When a partition
// has more files than can be merged in one pass, multiple passes run back-to-back
// until the partition is fully compacted (no 5-minute wait between passes).
func (c *Compactor) CompactTable(ctx context.Context, tableName string) (*Result, error) {
	tableMeta, err := c.catalog.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table metadata: %w", err)
	}

	result := &Result{Table: tableName}

	// Multi-pass loop: re-read manifest after each pass to pick up changes,
	// and continue until no partitions need compaction.
	for pass := 0; ; pass++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		manifest, err := c.catalog.GetManifest(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("getting manifest: %w", err)
		}
		deleteSet := buildDeleteSet(manifest.DeleteMarkers)

		compactedAny := false
		for _, part := range manifest.Partitions {
			if !c.shouldCompact(part) {
				continue
			}

			maxFiles := c.adaptivePassSize(part)
			files := part.Files
			if len(files) > maxFiles {
				files = files[:maxFiles]
			}

			merged, err := c.mergeFiles(ctx, files, tableMeta.Schema, deleteSet)
			if err != nil {
				c.logger.Warn("compaction failed for partition",
					"table", tableName, "partition", part.Path, "error", err)
				continue
			}

			if len(merged.rows) == 0 {
				oldPaths := filePaths(files)
				if err := c.catalog.RemoveFiles(ctx, tableName, oldPaths); err != nil {
					return nil, fmt.Errorf("removing empty partition files: %w", err)
				}
				c.deleteFromStore(ctx, oldPaths)
				result.FilesRemoved += len(files)
				result.PartitionsCompacted++
				compactedAny = true
				continue
			}

			newPath := fmt.Sprintf("%s/compacted_%d.parquet", part.Path, time.Now().UnixNano())
			written, err := c.writeMergedFile(ctx, newPath, tableMeta.Schema, merged.rows)
			if err != nil {
				return nil, fmt.Errorf("writing merged file: %w", err)
			}

			oldPaths := filePaths(files)
			if err := c.catalog.RemoveFiles(ctx, tableName, oldPaths); err != nil {
				return nil, fmt.Errorf("removing old files from manifest: %w", err)
			}

			newEntry := catalog.FileEntry{
				Path:        newPath,
				SizeBytes:   written.size,
				NumRows:     int64(len(merged.rows)),
				CreatedAt:   time.Now().UTC(),
				ColumnStats: written.columnStats,
			}
			if err := c.catalog.AddFiles(ctx, tableName, part.Values, part.Path, []catalog.FileEntry{newEntry}); err != nil {
				return nil, fmt.Errorf("adding merged file to manifest: %w", err)
			}

			c.deleteFromStore(ctx, oldPaths)

			result.PartitionsCompacted++
			result.FilesRemoved += len(files)
			result.FilesCreated++
			result.RowsMerged += int64(len(merged.rows))
			result.BytesBefore += merged.bytesBefore
			result.BytesAfter += written.size
			compactedAny = true

			c.logger.Info("compacted partition",
				"table", tableName,
				"partition", part.Path,
				"pass", pass,
				"files_merged", len(files),
				"rows", len(merged.rows),
			)
		}

		if !compactedAny {
			break // no partitions needed compaction — done
		}
	}

	return result, nil
}

// adaptivePassSize returns the max files to merge in one pass, scaling up for
// small files where memory usage per file is low. Targets ~256 MB in-memory
// per pass but never goes below the configured MaxFilesPerPass.
func (c *Compactor) adaptivePassSize(part catalog.PartitionEntry) int {
	n := len(part.Files)
	if n == 0 {
		return c.config.MaxFilesPerPass
	}
	var totalSize int64
	for _, f := range part.Files {
		totalSize += f.SizeBytes
	}
	avgSize := totalSize / int64(n)
	if avgSize <= 0 {
		avgSize = 1
	}

	// For small files, scale up to fit ~256 MB in memory per pass.
	const targetBytes = 256 * 1024 * 1024
	maxFiles := int(targetBytes / avgSize)
	if maxFiles < c.config.MaxFilesPerPass {
		maxFiles = c.config.MaxFilesPerPass
	}
	if maxFiles > n {
		maxFiles = n
	}
	return maxFiles
}

// shouldCompact returns true if a partition has too many files or files are too small.
func (c *Compactor) shouldCompact(part catalog.PartitionEntry) bool {
	n := len(part.Files)
	if n < c.config.MinFiles {
		return false
	}
	var totalSize int64
	for _, f := range part.Files {
		totalSize += f.SizeBytes
	}
	avgSize := totalSize / int64(n)
	return avgSize < c.config.MaxFileSizeBytes
}

type mergeResult struct {
	rows        []map[string]any
	bytesBefore int64
}

func (c *Compactor) mergeFiles(ctx context.Context, files []catalog.FileEntry, schema parquet.Schema, deleteSet map[string]map[int64]bool) (*mergeResult, error) {
	var allRows []map[string]any
	var bytesBefore int64

	for _, f := range files {
		bytesBefore += f.SizeBytes

		rc, info, err := c.catalog.ReadFile(ctx, f.Path)
		if err != nil {
			return nil, fmt.Errorf("reading file %s: %w", f.Path, err)
		}

		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("reading file data %s: %w", f.Path, err)
		}
		_ = info

		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("opening parquet %s: %w", f.Path, err)
		}

		rows, err := reader.ReadRows(schema.ColumnNames())
		if err != nil {
			return nil, fmt.Errorf("reading rows from %s: %w", f.Path, err)
		}

		// Apply delete markers
		if deleted, ok := deleteSet[f.Path]; ok && len(deleted) > 0 {
			filtered := make([]map[string]any, 0, len(rows))
			for i, row := range rows {
				if !deleted[int64(i)] {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		}

		allRows = append(allRows, rows...)
	}

	return &mergeResult{rows: allRows, bytesBefore: bytesBefore}, nil
}

type writeResult struct {
	size        int64
	columnStats map[string]catalog.FileColumnStats
}

func (c *Compactor) writeMergedFile(ctx context.Context, path string, schema parquet.Schema, rows []map[string]any) (*writeResult, error) {
	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating writer: %w", err)
	}
	if err := w.WriteRows(rows); err != nil {
		return nil, fmt.Errorf("writing rows: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}

	data := buf.Bytes()
	size := int64(len(data))
	_, err = c.catalog.Store().Put(ctx, c.catalog.Bucket(), path, bytes.NewReader(data), size, "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("uploading merged file: %w", err)
	}

	// Extract column stats from the written file
	colStats := extractColumnStats(data)

	return &writeResult{size: size, columnStats: colStats}, nil
}

// extractColumnStats reads Parquet metadata to extract per-column min/max/null stats.
func extractColumnStats(data []byte) map[string]catalog.FileColumnStats {
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	nrg := reader.NumRowGroups()
	if nrg == 0 {
		return nil
	}

	merged := make(map[string]catalog.FileColumnStats)
	for i := 0; i < nrg; i++ {
		rgs := reader.RowGroupStats(i)
		for col, cs := range rgs.Columns {
			if !cs.HasStats {
				continue
			}
			cur, ok := merged[col]
			if !ok {
				cur = catalog.FileColumnStats{
					MinValue:  cs.MinValue,
					MaxValue:  cs.MaxValue,
					NullCount: cs.NullCount,
				}
			} else {
				cur.NullCount += cs.NullCount
				if cs.MinValue != nil && (cur.MinValue == nil || parquet.CompareNative(cs.MinValue, cur.MinValue) < 0) {
					cur.MinValue = cs.MinValue
				}
				if cs.MaxValue != nil && (cur.MaxValue == nil || parquet.CompareNative(cs.MaxValue, cur.MaxValue) > 0) {
					cur.MaxValue = cs.MaxValue
				}
			}
			merged[col] = cur
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func (c *Compactor) deleteFromStore(ctx context.Context, paths []string) {
	for _, p := range paths {
		if err := c.catalog.Store().Delete(ctx, c.catalog.Bucket(), p); err != nil {
			c.logger.Warn("failed to delete old file", "path", p, "error", err)
		}
	}
}

func filePaths(files []catalog.FileEntry) []string {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.Path
	}
	return paths
}

func buildDeleteSet(markers []catalog.DeleteMarker) map[string]map[int64]bool {
	ds := make(map[string]map[int64]bool)
	for _, m := range markers {
		if ds[m.FilePath] == nil {
			ds[m.FilePath] = make(map[int64]bool)
		}
		for _, idx := range m.RowIndices {
			ds[m.FilePath][idx] = true
		}
	}
	return ds
}
