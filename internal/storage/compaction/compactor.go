// Package compaction merges small Parquet files within a partition into
// larger files, reducing S3 list overhead and scan file-open costs.
package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/storage/partition"
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
	return partitionedOutputPath(tableName, partPath, "compacted")
}

// partitionedOutputPath builds "<base>_<nanos>.parquet" under the partition's
// directory. Some writers store the partition path already table-prefixed
// (the harness datagen primes "tables/<name>/"); blindly joining
// prefix+partPath then yields "tables/orders/tables/orders//compacted_*" —
// consistent (write, manifest, and read all use it) but wrong. A prefixed
// partPath is treated as the full base.
func partitionedOutputPath(tableName, partPath, base string) string {
	prefix := partition.TablePrefix(tableName)
	dir := prefix
	if partPath != "" {
		if strings.HasPrefix(partPath, prefix+"/") || partPath == prefix {
			dir = strings.TrimSuffix(partPath, "/")
		} else {
			dir = prefix + "/" + strings.TrimSuffix(partPath, "/")
		}
	}
	return fmt.Sprintf("%s/%s_%d.parquet", dir, base, time.Now().UnixNano())
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
	// DeleteGrace is how long a compacted-away file stays physically present
	// in the object store after its manifest entry is removed. In-flight
	// tasks hold file lists resolved at DISPATCH time; deleting the bytes
	// the instant the manifest swaps races every running query against the
	// compactor (observed 2026-06-11: first successful mid-benchmark
	// compaction at SF10 deleted chunks under three dispatched scan tasks →
	// "object not found" ×5 → circuit breaker open → every later query
	// failed). Mirrors DefaultGCMinAge's reasoning. Zero uses
	// DefaultDeleteGrace; negative deletes immediately (tests).
	DeleteGrace time.Duration
}

// DefaultDeleteGrace keeps compacted-away bytes alive long enough for any
// in-flight query dispatched against the old manifest to finish reading them.
const DefaultDeleteGrace = 30 * time.Minute

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		MinFiles:         10,
		MaxFileSizeBytes: 32 * 1024 * 1024, // 32 MB
		MaxFilesPerPass:  50,
		DeleteGrace:      DefaultDeleteGrace,
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

	// delMu protects pendingDeletes: files whose manifest entries are gone
	// but whose bytes wait out DeleteGrace before physical deletion (see
	// Config.DeleteGrace). Process-local: a crash leaves orphans, which the
	// store-side reasoning elsewhere in this package already treats as safe.
	delMu          sync.Mutex
	pendingDeletes []pendingDelete
}

type pendingDelete struct {
	path string
	at   time.Time
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
//
// Failed names the partitions whose merge did not run. It is part of the
// result rather than only of the log because the counters below count only
// the partitions that DID compact, so a caller reading them alone cannot tell
// a clean run from one where a partition failed on every pass (#435).
type Result struct {
	Table string
	// PartitionsCompacted counts successful MERGES, not distinct partitions:
	// a partition compacted over three passes counts three times.
	PartitionsCompacted int
	FilesRemoved        int
	FilesCreated        int
	RowsMerged          int64
	BytesBefore         int64
	BytesAfter          int64
	Failed              []PartitionFailure

	// PassLimitReached reports that the multi-pass loop stopped at
	// maxCompactionPasses with the table still shrinking. It is a flag on a
	// SUCCESSFUL result rather than an error: every one of those passes had
	// to remove more files than it created to get there (the progress rule
	// in CompactTable), so the work is real and committed, and calling the
	// call a failure would discard a correct result and — through the
	// background sweep's `continue` on error — skip the table's
	// delete-marker GC as well. The next sweep picks the table up where
	// this one left off.
	PassLimitReached bool
}

// PartitionFailure is one partition whose merge failed, and why.
type PartitionFailure struct {
	Partition string
	Err       error
}

// CompactionFailed is the aggregate error CompactTable returns when at least
// one partition's merge failed.
//
// One error for the whole table, not the first failure, because a merge
// failure is scoped to the partition whose files could not be read: #435
// correctly made a failed merge visible to the caller, but by RETURNING at the
// first one, so a single drifted partition froze compaction of every other
// partition in the table — and, since the background sweep `continue`s to the
// next table on an error from here, the table's delete-marker GC with it.
//
// Compacted lets a caller tell a partial run from a total one without
// inspecting the Result: zero means nothing in this table compacted, non-zero
// means the named partitions failed while the rest went through.
type CompactionFailed struct {
	Table     string
	Failures  []PartitionFailure
	Compacted int
}

// Partial reports whether some partitions compacted despite the failures.
func (e *CompactionFailed) Partial() bool { return e.Compacted > 0 }

func (e *CompactionFailed) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "compacting table %q: %d of %d partitions failed",
		e.Table, len(e.Failures), len(e.Failures)+e.Compacted)
	for _, f := range e.Failures {
		fmt.Fprintf(&b, "; partition %s: %v", partitionLabel(f.Partition), f.Err)
	}
	return b.String()
}

// Unwrap exposes the individual causes to errors.Is and errors.As.
func (e *CompactionFailed) Unwrap() []error {
	errs := make([]error, len(e.Failures))
	for i, f := range e.Failures {
		errs[i] = f.Err
	}
	return errs
}

// mergeError marks the one failure that is scoped to a single partition: the
// merge itself, which READS and REWRITES that partition's data and touches
// nothing else. Every other error in a pass — the manifest, the object store —
// is a condition on the whole call, so the caller distinguishes them by type
// rather than by position.
type mergeError struct{ err error }

func (m *mergeError) Error() string { return m.err.Error() }
func (m *mergeError) Unwrap() error { return m.err }

// CompactTable runs compaction for all partitions of a table. When a partition
// has more files than can be merged in one pass, multiple passes run back-to-back
// until the partition is fully compacted (no 5-minute wait between passes).
//
// A partition whose merge fails does not stop the others: the failure is
// recorded in Result.Failed, the partition is skipped for the rest of the
// call, and the aggregate *CompactionFailed comes back at the end. See that
// type for why the first-failure return this replaced was too blunt.
func (c *Compactor) CompactTable(ctx context.Context, tableName string) (*Result, error) {
	tableMeta, err := c.catalog.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table metadata: %w", err)
	}

	result := &Result{Table: tableName}

	// A partition that failed is not retried on a later pass of this same
	// call: the manifest entry it failed on is untouched, so the merge would
	// read the same bytes and fail identically, once per pass.
	failed := make(map[string]bool)

	// Multi-pass loop: re-read manifest after each pass to pick up changes,
	// and continue until no partitions need compaction.
	//
	// The loop's only exit used to be `compactedAny == false`, which assumes
	// every pass makes progress. shouldCompact's two-file floor is what makes
	// that true; the two bounds below are the belt to that brace, because the
	// cost of being wrong here is a call that never returns while it rewrites
	// and deletes objects forever (#436).
	for pass := 0; ; pass++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if pass >= maxCompactionPasses {
			// Not a loop: the progress rule below forced every one of those
			// passes to remove more files than it created, so this is a
			// table still genuinely shrinking. Stop, flag it, and let the
			// next sweep continue — see Result.PassLimitReached (#436).
			c.logger.Warn("compaction pass limit reached, stopping with work outstanding",
				"table", tableName, "passes", pass,
				"files_removed", result.FilesRemoved, "files_created", result.FilesCreated)
			result.PassLimitReached = true
			break
		}

		manifest, err := c.catalog.GetManifest(ctx, tableName)
		if err != nil {
			return result, fmt.Errorf("getting manifest: %w", err)
		}
		// Progress is measured on what THIS pass did, not on the manifest's
		// total: an ingester writing into the table between passes would
		// otherwise read as a stall.
		removedBefore, createdBefore := result.FilesRemoved, result.FilesCreated
		deleteSet := buildDeleteSet(manifest.DeleteMarkers)

		compactedAny := false
		for _, part := range manifest.Partitions {
			if failed[part.Path] || !c.shouldCompact(part) {
				continue
			}

			files := part.Files
			if maxFiles := c.adaptivePassSize(part); len(files) > maxFiles {
				files = files[:maxFiles]
			}

			err := c.mergeGroup(ctx, tableName, tableMeta.Schema, part, files, deleteSet, result)
			var me *mergeError
			if errors.As(err, &me) {
				// The merge is the step that READS and REWRITES the data, and
				// it is the only failure here scoped to one partition: no data
				// is lost, because the inputs are removed only after a
				// successful write. Record it, skip this partition, keep
				// going (#435).
				c.logger.Error("compaction failed for partition",
					"table", tableName, "partition", part.Path, "error", me.err)
				result.Failed = append(result.Failed, PartitionFailure{Partition: part.Path, Err: me.err})
				failed[part.Path] = true
				continue
			}
			if err != nil {
				return result, err
			}
			compactedAny = true

			c.logger.Info("compacted partition",
				"table", tableName,
				"partition", part.Path,
				"pass", pass,
				"files_merged", len(files),
			)
		}

		if !compactedAny {
			break // no partitions needed compaction — done
		}
		if removed, created := result.FilesRemoved-removedBefore, result.FilesCreated-createdBefore; removed <= created {
			return result, fmt.Errorf(
				"compacting table %q: pass %d merged %d files into %d and so reduced nothing — "+
					"a pass that does not shrink the partition would repeat forever",
				tableName, pass, removed, created)
		}
	}

	if len(result.Failed) > 0 {
		return result, &CompactionFailed{
			Table:     tableName,
			Failures:  result.Failed,
			Compacted: result.PartitionsCompacted,
		}
	}
	return result, nil
}

// mergeGroup merges one group of a partition's files into a single new file
// and swaps the manifest, folding the outcome into result.
//
// The caller classifies the error rather than the position: *mergeError is the
// read-and-rewrite step failing on THIS partition's bytes, with its inputs
// untouched; anything else is the manifest or the object store, which is not a
// per-partition condition and ends the call.
func (c *Compactor) mergeGroup(ctx context.Context, tableName string, schema parquet.Schema,
	part catalog.PartitionEntry, files []catalog.FileEntry,
	deleteSet map[string]map[int64]bool, result *Result) error {

	newPath := compactedFilePath(tableName, part.Path)
	written, err := c.mergeAndWriteFiles(ctx, files, schema, deleteSet, newPath)
	if err != nil {
		// Unadorned: CompactionFailed names the table and the partition, and
		// PartitionFailure carries the partition alongside this cause, so a
		// prefix here would only repeat itself in the aggregate message.
		return &mergeError{err: err}
	}

	// Write-before-delete: mergeAndWriteFiles has already uploaded newPath (or
	// written nothing, when every row was delete-filtered away).
	oldPaths := filePaths(files)
	if err := c.catalog.RemoveFiles(ctx, tableName, oldPaths); err != nil {
		return fmt.Errorf("removing old files from manifest: %w", err)
	}

	if written.rowsWritten == 0 {
		c.deleteFromStore(ctx, oldPaths)
		result.FilesRemoved += len(files)
		result.PartitionsCompacted++
		return nil
	}

	newEntry := catalog.FileEntry{
		Path:        newPath,
		SizeBytes:   written.size,
		NumRows:     written.rowsWritten,
		CreatedAt:   time.Now().UTC(),
		ColumnStats: written.columnStats,
	}
	if err := c.catalog.AddFiles(ctx, tableName, part.Values, part.Path, []catalog.FileEntry{newEntry}); err != nil {
		return fmt.Errorf("adding merged file to manifest: %w", err)
	}

	c.deleteFromStore(ctx, oldPaths)

	result.PartitionsCompacted++
	result.FilesRemoved += len(files)
	result.FilesCreated++
	result.RowsMerged += written.rowsWritten
	result.BytesBefore += written.bytesBefore
	result.BytesAfter += written.size
	return nil
}

// maxCompactionPasses bounds the pass loop absolutely. Each pass merges at
// least two files into one, so a partition of N files settles in fewer than N
// passes and the adaptive pass size makes it far fewer; a table needing more
// than this is not converging.
//
// Reaching it is reported as Result.PassLimitReached, not as an error: see
// that field for why 1024 passes of committed work is not a failure.
const maxCompactionPasses = 1024

// partitionLabel names the unpartitioned partition in an error message, where
// an empty string reads as a missing value.
func partitionLabel(path string) string {
	if path == "" {
		return "(unpartitioned)"
	}
	return path
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
//
// TWO files is the real precondition, independent of MinFiles: merging one
// file produces one file, so the pass makes no progress and the loop below
// finds the same condition on the next iteration. With Config.MinFiles == 1 —
// an exported field of an exported type, so reachable by configuration and not
// only by a test — that is an unbounded stream of rewrite/upload/delete cycles
// against the object store from a call that never returns (#436).
func (c *Compactor) shouldCompact(part catalog.PartitionEntry) bool {
	n := len(part.Files)
	if n < 2 || n < c.config.MinFiles {
		return false
	}
	var totalSize int64
	for _, f := range part.Files {
		totalSize += f.SizeBytes
	}
	avgSize := totalSize / int64(n)
	return avgSize < c.config.MaxFileSizeBytes
}

type mergedWrite struct {
	rowsWritten int64
	bytesBefore int64
	size        int64
	columnStats map[string]catalog.FileColumnStats
}

// mergeAndWriteFiles streams a compaction pass: each input file's rows are
// read, delete-filtered, and appended to the output writer — which flushes
// row groups incrementally to a local temp file — so peak heap is ONE input
// file's boxed rows plus one in-flight row group, regardless of merge-set
// size. The previous mergeFiles/writeMergedFile pair materialized the whole
// merge set as []map[string]any AND the entire output file in a
// bytes.Buffer: the 256 MB-of-compressed-input pass target expanded to
// >3 GB live and OOM-killed a 4 GiB edge coordinator mid-query (2026-06-11
// edge validation, SF10 Q03).
//
// When every row is delete-filtered away (rowsWritten == 0) nothing is
// uploaded and size/columnStats stay zero — callers drop the inputs.
// Write-before-delete ordering is preserved: the upload happens here,
// before any caller touches the manifest.
func (c *Compactor) mergeAndWriteFiles(ctx context.Context, files []catalog.FileEntry, schema parquet.Schema, deleteSet map[string]map[int64]bool, newPath string) (*mergedWrite, error) {
	tmp, err := os.CreateTemp("", "wadjet-compact-*.parquet")
	if err != nil {
		return nil, fmt.Errorf("creating compaction temp file: %w", err)
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	cfg := parquet.DefaultWriterConfig()
	w, err := parquet.NewWriter(tmp, schema, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating writer: %w", err)
	}

	res := &mergedWrite{}
	for _, f := range files {
		res.bytesBefore += f.SizeBytes

		rc, _, err := c.catalog.ReadFile(ctx, f.Path)
		if err != nil {
			return nil, fmt.Errorf("reading file %s: %w", f.Path, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("reading file data %s: %w", f.Path, err)
		}

		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("opening parquet %s: %w", f.Path, err)
		}
		// Merge one row group at a time: ReadRows materializes the whole
		// file as []map[string]any (~10x the on-disk bytes), and the
		// background sweep runs concurrently with query execution — the
		// 2026-08-10 SF10 coordinator memcg OOM was this exact path
		// holding 1.4 GB live mid-suite. The writer flushes its own row
		// groups as it goes, so peak memory is one input row group.
		// Delete markers index rows file-wide; base carries the offset.
		cols := schema.ColumnNames()
		deleted := deleteSet[f.Path]
		var base int64
		for rg := 0; rg < reader.NumRowGroups(); rg++ {
			// ReadRowGroupAs, not ReadRowGroup: the TABLE's schema is the
			// authority on eight types a parquet file cannot describe
			// (IPv4, IPv6, MAC, PORT, PROTOCOL, DURATION, BYTES, UUID —
			// buildLeafSchemaElement writes no logical annotation for any
			// of them). Read without it, an IPv6 comes back as a Go string
			// of sixteen raw bytes, which the writer below hands
			// net.ParseIP and REFUSES — compaction could not read back its
			// own output. The catalog knows what the file cannot say.
			rows, err := reader.ReadRowGroupAs(rg, schema.Columns, cols)
			if err != nil {
				return nil, fmt.Errorf("reading row group %d from %s: %w", rg, f.Path, err)
			}
			groupRows := int64(len(rows))
			if len(deleted) > 0 {
				filtered := make([]map[string]any, 0, len(rows))
				for i, row := range rows {
					if !deleted[base+int64(i)] {
						filtered = append(filtered, row)
					}
				}
				rows = filtered
			}
			base += groupRows
			if len(rows) == 0 {
				continue
			}
			if err := w.WriteRows(rows); err != nil {
				return nil, fmt.Errorf("writing rows from %s: %w", f.Path, err)
			}
			res.rowsWritten += int64(len(rows))
		}
	}

	if res.rowsWritten == 0 {
		// Every row deleted: discard the temp (deferred), upload nothing.
		return res, nil
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}
	fi, err := tmp.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat merged file: %w", err)
	}
	res.size = fi.Size()
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewinding merged file: %w", err)
	}
	if _, err := c.catalog.Store().Put(ctx, c.catalog.Bucket(), newPath, tmp, res.size, "application/octet-stream"); err != nil {
		return nil, fmt.Errorf("uploading merged file: %w", err)
	}

	res.columnStats = extractColumnStatsAt(tmp, res.size)
	return res, nil
}

// extractColumnStats reads Parquet metadata to extract per-column min/max/null stats.
func extractColumnStats(data []byte) map[string]catalog.FileColumnStats {
	return extractColumnStatsAt(bytes.NewReader(data), int64(len(data)))
}

// extractColumnStatsAt reads per-column min/max/null stats from the parquet
// footer via a ReaderAt — no whole-file buffer needed (the compactor passes
// its on-disk temp file directly).
func extractColumnStatsAt(ra io.ReaderAt, size int64) map[string]catalog.FileColumnStats {
	reader, err := parquet.NewReader(ra, size)
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

// deleteFromStore schedules paths for physical deletion after DeleteGrace.
// The manifest entries are already gone, so no NEW query can see them; the
// grace keeps the bytes readable for queries dispatched against the old
// manifest. A non-positive grace deletes immediately (tests).
func (c *Compactor) deleteFromStore(ctx context.Context, paths []string) {
	grace := c.config.DeleteGrace
	if grace == 0 {
		grace = DefaultDeleteGrace
	}
	if grace < 0 {
		for _, p := range paths {
			if err := c.catalog.Store().Delete(ctx, c.catalog.Bucket(), p); err != nil {
				c.logger.Warn("failed to delete old file", "path", p, "error", err)
			}
		}
		return
	}
	now := time.Now()
	c.delMu.Lock()
	for _, p := range paths {
		c.pendingDeletes = append(c.pendingDeletes, pendingDelete{path: p, at: now})
	}
	n := len(c.pendingDeletes)
	c.delMu.Unlock()
	c.logger.Info("deferred physical deletion of compacted files",
		"files", len(paths), "grace", grace, "pending_total", n)
}

// FlushDeferredDeletes physically deletes every pending file older than
// DeleteGrace. Called from the background sweep; safe to call any time.
// Returns the number of files deleted.
func (c *Compactor) FlushDeferredDeletes(ctx context.Context) int {
	grace := c.config.DeleteGrace
	if grace <= 0 {
		grace = DefaultDeleteGrace
	}
	cutoff := time.Now().Add(-grace)

	c.delMu.Lock()
	var due, keep []pendingDelete
	for _, pd := range c.pendingDeletes {
		if pd.at.Before(cutoff) {
			due = append(due, pd)
		} else {
			keep = append(keep, pd)
		}
	}
	c.pendingDeletes = keep
	c.delMu.Unlock()

	deleted := 0
	for _, pd := range due {
		// Recreated-object guard: if the object at this path is NEWER than
		// the deferral, it is not the file we compacted away — someone
		// recreated the path (re-ingest, test datagen with deterministic
		// names). Deleting it would destroy live data: a stale compactor
		// surviving its harness (orphaned container, 2026-06-11 edge rig)
		// silently consumed a freshly regenerated dataset through exactly
		// this hole. Skip and drop the entry; the new object has its own
		// manifest entry and lifecycle.
		if info, err := c.catalog.Store().Head(ctx, c.catalog.Bucket(), pd.path); err == nil && info.LastModified.After(pd.at) {
			c.logger.Warn("skipping deferred deletion: object recreated since deferral",
				"path", pd.path, "deferred_at", pd.at, "object_modified", info.LastModified)
			continue
		}
		if err := c.catalog.Store().Delete(ctx, c.catalog.Bucket(), pd.path); err != nil {
			c.logger.Warn("failed to delete old file", "path", pd.path, "error", err)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		c.logger.Info("flushed deferred deletions", "files", deleted)
	}
	return deleted
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

	// Write-before-delete: the streaming merge uploads the new file FIRST
	// (inside mergeAndWriteFiles), so on failure nothing in the manifest has
	// changed — no data loss. partitionedOutputPath handles both empty and
	// already-table-prefixed partition paths.
	newPath := partitionedOutputPath(tableName, partPath, "rewrite")

	// Merge the single file (applies delete markers internally; uploads to
	// newPath unless every row was deleted).
	written, err := c.mergeAndWriteFiles(ctx, []catalog.FileEntry{*targetFile}, tableMeta.Schema, deleteSet, newPath)
	if err != nil {
		return fmt.Errorf("rewriting file: %w", err)
	}

	// If all rows were deleted, nothing was uploaded — atomically remove the
	// old file + applied markers.
	if written.rowsWritten == 0 {
		if err := c.catalog.SwapFileForGC(ctx, tableName, filePath, nil, partValues, partPath, appliedIndices); err != nil {
			return fmt.Errorf("removing fully-deleted file: %w", err)
		}
		// Best-effort cleanup of old file from object store (orphan is safe)
		c.deleteFromStore(ctx, []string{filePath})
		c.logger.Info("force compact: file fully deleted",
			"table", tableName, "file", filePath)
		return nil
	}

	newEntry := catalog.FileEntry{
		Path:        newPath,
		SizeBytes:   written.size,
		NumRows:     written.rowsWritten,
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
		"rows_after", written.rowsWritten,
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
