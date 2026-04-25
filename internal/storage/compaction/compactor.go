// Package compaction merges small Parquet files within a partition into
// larger files, reducing S3 list overhead and scan file-open costs.
package compaction

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/storage/partition"
)

// compactedFilePath returns the S3 key for a compaction output file. Mirrors
// partition.Strategy.FilePath for the compacted_<nanos>.parquet chunk id: for
// unpartitioned tables the path is "<tables/name>/compacted_X.parquet"; for
// Hive-partitioned tables it is "<tables/name>/<hive-path>/compacted_X.parquet".
//
// Prior to this helper the compactor wrote "%s/compacted_%d.parquet" using the
// manifest's partition path as the prefix, which is empty for unpartitioned
// tables — yielding a leading-slash key at the bucket root and silently
// orphaning data after the old files were deleted from the manifest.
func compactedFilePath(tableName, partPath string) string {
	prefix := partition.TablePrefix(tableName)
	if partPath == "" {
		return fmt.Sprintf("%s/compacted_%d.parquet", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s/%s/compacted_%d.parquet", prefix, partPath, time.Now().UnixNano())
}

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

	// gcMu protects gcInProgress to prevent double GC rewrites of the same file.
	gcMu         sync.Mutex
	gcInProgress map[string]bool
}

// New creates a compactor.
func New(cat *catalog.Catalog, logger *slog.Logger, cfg Config) *Compactor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Compactor{
		catalog:      cat,
		logger:       logger,
		config:       cfg,
		gcInProgress: make(map[string]bool),
	}
}

// tryAcquireGCLock attempts to mark a file as GC-in-progress. Returns true if
// the lock was acquired, false if another goroutine is already rewriting it.
func (c *Compactor) tryAcquireGCLock(filePath string) bool {
	c.gcMu.Lock()
	defer c.gcMu.Unlock()
	if c.gcInProgress[filePath] {
		return false
	}
	c.gcInProgress[filePath] = true
	return true
}

func (c *Compactor) releaseGCLock(filePath string) {
	c.gcMu.Lock()
	defer c.gcMu.Unlock()
	delete(c.gcInProgress, filePath)
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

			newPath := compactedFilePath(tableName, part.Path)
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

// ForceCompactFile rewrites a single data file, applying any pending delete
// markers for that file. Used by delete marker GC to physically purge deleted
// rows from files whose markers have aged out.
//
// Safety invariants:
//   - Write-before-delete: the new file is written to the object store before
//     the old file is removed. On partial failure, the old file may become an
//     orphan in S3, but data is never lost.
//   - Scoped marker removal: only the specific row indices that were applied
//     during the rewrite are removed from the manifest. Concurrent DELETEs
//     that add new indices between GC scan and rewrite are preserved.
//   - Atomic manifest swap: old file removal, new file addition, and marker
//     cleanup happen in a single CAS operation via SwapFileForGC.
//   - Per-file lock: prevents double GC rewrite if two sweeps overlap.
func (c *Compactor) ForceCompactFile(ctx context.Context, tableName string, filePath string, gcIndices map[int64]bool) error {
	if !c.tryAcquireGCLock(filePath) {
		c.logger.Info("force compact: skipping, GC already in progress",
			"table", tableName, "file", filePath)
		return nil
	}
	defer c.releaseGCLock(filePath)

	tableMeta, err := c.catalog.GetTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("getting table metadata: %w", err)
	}

	manifest, err := c.catalog.GetManifest(ctx, tableName)
	if err != nil {
		return fmt.Errorf("getting manifest: %w", err)
	}

	// Find the file entry and its partition
	var targetFile *catalog.FileEntry
	var partPath string
	var partValues map[string]string
	for _, p := range manifest.Partitions {
		for i := range p.Files {
			if p.Files[i].Path == filePath {
				targetFile = &p.Files[i]
				partPath = p.Path
				partValues = p.Values
				break
			}
		}
		if targetFile != nil {
			break
		}
	}
	if targetFile == nil {
		return nil // file no longer exists, nothing to do
	}

	// Use the GC-scanned indices as the authoritative set of what to apply.
	// This prevents TOCTOU: concurrent DELETEs that add new indices between
	// the GC scan and this rewrite are NOT included and will survive the swap.
	appliedIndices := gcIndices
	if len(appliedIndices) == 0 {
		return nil // no delete markers for this file, nothing to do
	}

	deleteSet := buildDeleteSet(manifest.DeleteMarkers)

	// Merge the single file (which applies delete markers internally)
	merged, err := c.mergeFiles(ctx, []catalog.FileEntry{*targetFile}, tableMeta.Schema, deleteSet)
	if err != nil {
		return fmt.Errorf("reading file for rewrite: %w", err)
	}

	// If all rows were deleted, atomically remove old file + applied markers
	if len(merged.rows) == 0 {
		if err := c.catalog.SwapFileForGC(ctx, tableName, filePath, nil, partValues, partPath, appliedIndices); err != nil {
			return fmt.Errorf("removing fully-deleted file: %w", err)
		}
		// Best-effort cleanup of old file from object store (orphan is safe)
		c.deleteFromStore(ctx, []string{filePath})
		c.logger.Info("force compact: file fully deleted",
			"table", tableName, "file", filePath)
		return nil
	}

	// Write-before-delete: write the new file FIRST so data is never lost.
	// On failure here, nothing in the manifest has changed — no data loss.
	// Mirror compactedFilePath: use the table prefix so unpartitioned tables
	// (empty partPath) don't land at the bucket root.
	newPath := ""
	if partPath == "" {
		newPath = fmt.Sprintf("%s/rewrite_%d.parquet", partition.TablePrefix(tableName), time.Now().UnixNano())
	} else {
		newPath = fmt.Sprintf("%s/%s/rewrite_%d.parquet", partition.TablePrefix(tableName), partPath, time.Now().UnixNano())
	}
	written, err := c.writeMergedFile(ctx, newPath, tableMeta.Schema, merged.rows)
	if err != nil {
		return fmt.Errorf("writing rewritten file: %w", err)
	}

	newEntry := catalog.FileEntry{
		Path:        newPath,
		SizeBytes:   written.size,
		NumRows:     int64(len(merged.rows)),
		CreatedAt:   time.Now().UTC(),
		ColumnStats: written.columnStats,
	}

	// Atomic manifest swap: remove old file + add new file + remove applied markers
	if err := c.catalog.SwapFileForGC(ctx, tableName, filePath, &newEntry, partValues, partPath, appliedIndices); err != nil {
		// New file is orphaned in S3 — safe, data not lost. Log for cleanup.
		c.logger.Warn("force compact: manifest swap failed, orphaned new file",
			"table", tableName, "new_file", newPath, "error", err)
		return fmt.Errorf("swapping file in manifest: %w", err)
	}

	// Best-effort cleanup of old file from object store
	c.deleteFromStore(ctx, []string{filePath})

	c.logger.Info("force compact: file rewritten",
		"table", tableName,
		"old_file", filePath,
		"new_file", newPath,
		"rows_before", targetFile.NumRows,
		"rows_after", len(merged.rows),
	)
	return nil
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
