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

// CompactTable runs compaction for all partitions of a table.
func (c *Compactor) CompactTable(ctx context.Context, tableName string) (*Result, error) {
	tableMeta, err := c.catalog.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table metadata: %w", err)
	}

	manifest, err := c.catalog.GetManifest(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting manifest: %w", err)
	}

	// Build delete marker lookup: filepath → set of row indices
	deleteSet := buildDeleteSet(manifest.DeleteMarkers)

	result := &Result{Table: tableName}

	for _, part := range manifest.Partitions {
		if !c.shouldCompact(part) {
			continue
		}

		files := part.Files
		if len(files) > c.config.MaxFilesPerPass {
			files = files[:c.config.MaxFilesPerPass]
		}

		merged, err := c.mergeFiles(ctx, files, tableMeta.Schema, deleteSet)
		if err != nil {
			c.logger.Warn("compaction failed for partition",
				"table", tableName, "partition", part.Path, "error", err)
			continue
		}

		if len(merged.rows) == 0 {
			// All rows were deleted — just remove the old files
			oldPaths := filePaths(files)
			if err := c.catalog.RemoveFiles(ctx, tableName, oldPaths); err != nil {
				return nil, fmt.Errorf("removing empty partition files: %w", err)
			}
			c.deleteFromStore(ctx, oldPaths)
			result.FilesRemoved += len(files)
			result.PartitionsCompacted++
			continue
		}

		// Write merged file
		newPath := fmt.Sprintf("%s/compacted_%d.parquet", part.Path, time.Now().UnixNano())
		written, err := c.writeMergedFile(ctx, newPath, tableMeta.Schema, merged.rows)
		if err != nil {
			return nil, fmt.Errorf("writing merged file: %w", err)
		}

		// Atomic manifest update: remove old, add new
		oldPaths := filePaths(files)
		if err := c.catalog.RemoveFiles(ctx, tableName, oldPaths); err != nil {
			return nil, fmt.Errorf("removing old files from manifest: %w", err)
		}

		newEntry := catalog.FileEntry{
			Path:      newPath,
			SizeBytes: written.size,
			NumRows:   int64(len(merged.rows)),
			CreatedAt: time.Now().UTC(),
		}
		if err := c.catalog.AddFiles(ctx, tableName, part.Values, part.Path, []catalog.FileEntry{newEntry}); err != nil {
			return nil, fmt.Errorf("adding merged file to manifest: %w", err)
		}

		// Async-safe: delete old files from object store after manifest update
		c.deleteFromStore(ctx, oldPaths)

		result.PartitionsCompacted++
		result.FilesRemoved += len(files)
		result.FilesCreated++
		result.RowsMerged += int64(len(merged.rows))
		result.BytesBefore += merged.bytesBefore
		result.BytesAfter += written.size

		c.logger.Info("compacted partition",
			"table", tableName,
			"partition", part.Path,
			"files_merged", len(files),
			"rows", len(merged.rows),
		)
	}

	return result, nil
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
	size int64
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

	size := int64(buf.Len())
	_, err = c.catalog.Store().Put(ctx, c.catalog.Bucket(), path, bytes.NewReader(buf.Bytes()), size, "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("uploading merged file: %w", err)
	}

	return &writeResult{size: size}, nil
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
