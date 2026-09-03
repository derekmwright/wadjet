package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AnalyzeTable computes HyperLogLog sketches over every column of every
// file in the named table and writes them back into the manifest's
// FileColumnStats.HLL field. Idempotent — re-running ANALYZE replaces
// existing HLLs with freshly computed ones.
//
// Used when a table's data was pre-staged (e.g., the SF10/SF100 EC2
// deploy buckets) without going through the ingest path, so HLL never
// got collected at write time. The planner's NDV estimator then has
// real distinct-count data instead of falling back to min/max-range
// heuristics or FK-naming.
//
// Strategy: for each file, download the parquet bytes, decode row
// groups via the existing parquet.Reader API, hash every column value
// into a per-(file, column) HLL. After all files of one table are
// processed, persist the augmented manifest.
//
// Cost: one full table scan, decompressed but not joined. SF10 lineitem
// (60 chunks × 1M rows × 16 cols) takes 1-2 minutes serial. Cheap
// relative to a single query at the same scale; expected to run once
// per data load.
//
// Returns the count of files analyzed and any error from the first
// failed file. Files that fail (corrupt, missing) are logged and
// skipped — partial coverage is better than total failure.
func (c *Catalog) AnalyzeTable(ctx context.Context, name string) (int, error) {
	// loadManifest, not GetManifest: this rewrites the manifest in place
	// (sketch keys onto every FileEntry, RGMetaKey, UpdatedAt) before
	// persisting it, and GetManifest's result is shared with every
	// concurrent reader holding the same revision.
	manifest, err := c.loadManifest(name)
	if err != nil {
		return 0, fmt.Errorf("analyze %s: load manifest: %w", name, err)
	}
	if manifest == nil {
		return 0, fmt.Errorf("analyze %s: table not found", name)
	}

	// Parallelize across files. Each computeFileSketches is independent
	// (separate S3 fetch, separate parquet decode, separate HLLs +
	// samplers per file). For SF10 lineitem with 60 chunks × ~30s
	// decode-each, serial ANALYZE takes ~30 min — unacceptable at
	// startup. Parallel with NumCPU workers cuts to ~30s/numCPU on the
	// coordinator instance. For SF100 with 1000+ files the win is
	// proportionally larger.
	//
	// We don't share state across workers — each goroutine produces a
	// (partitionIdx, fileIdx, sketches) tuple via a channel. The main
	// goroutine drains and applies the sketches to the manifest in the
	// order produced (order doesn't matter for correctness, since each
	// file's HLL is per-file).
	type fileResult struct {
		pi, fi   int
		sketches *fileColumnSketches
		err      error
	}
	type fileJob struct {
		pi, fi int
		path   string
	}

	// Build job queue.
	var jobs []fileJob
	for pi, part := range manifest.Partitions {
		for fi, f := range part.Files {
			jobs = append(jobs, fileJob{pi: pi, fi: fi, path: f.Path})
		}
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	concurrency := runtime.NumCPU()
	if concurrency > len(jobs) {
		concurrency = len(jobs)
	}
	if concurrency < 1 {
		concurrency = 1
	}

	jobCh := make(chan fileJob, len(jobs))
	resCh := make(chan fileResult, len(jobs))
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				if err := ctx.Err(); err != nil {
					resCh <- fileResult{pi: j.pi, fi: j.fi, err: err}
					continue
				}
				sk, err := computeFileSketches(ctx, c.store, c.bucket, j.path)
				resCh <- fileResult{pi: j.pi, fi: j.fi, sketches: sk, err: err}
			}
		}()
	}
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)
	go func() {
		wg.Wait()
		close(resCh)
	}()

	analyzed := 0
	rgMetaFiles := make([]FileRGMeta, 0, len(jobs))
	sketched := make(map[string]sketchedFile, len(jobs))
	for r := range resCh {
		if r.err != nil {
			return analyzed, fmt.Errorf("analyze %s: file %s: %w", name, manifest.Partitions[r.pi].Files[r.fi].Path, r.err)
		}
		sk := r.sketches
		if sk == nil {
			continue
		}
		// Row-group metadata is captured even for files with no
		// sketch-supported columns — the scan-side pruning fast path
		// needs every file covered or it falls back to a footer read.
		if len(sk.rgStats) > 0 {
			rgMetaFiles = append(rgMetaFiles, FileRGMeta{
				Path:   manifest.Partitions[r.pi].Files[r.fi].Path,
				Groups: sk.rgStats,
			})
		}
		if len(sk.hlls) == 0 {
			continue
		}
		f := manifest.Partitions[r.pi].Files[r.fi]
		// Build entries for the per-file sketches bundle. Also clear any
		// legacy inline HLL/Sample bytes on the FileColumnStats so the
		// manifest stays small. Min/Max/NullCount stay inline because
		// they're small and are read at plan time before sketch fetches.
		entries := make([]FileSketchesEntry, 0, len(sk.hlls))
		if f.ColumnStats == nil {
			f.ColumnStats = make(map[string]FileColumnStats, len(sk.hlls))
		}
		for col, h := range sk.hlls {
			hllBytes := h.Bytes()
			var sampleBytes []byte
			if sampler := sk.samplers[col]; sampler != nil {
				vals, total, tc := sampler.Snapshot()
				if len(vals) > 0 {
					sampleBytes = SampleBytes(vals, total, tc)
				}
			}
			entries = append(entries, FileSketchesEntry{Column: col, HLL: hllBytes, Sample: sampleBytes})
			// Clear any legacy inline bytes — they're now externalized.
			cs := f.ColumnStats[col]
			cs.HLL = nil
			cs.Sample = nil
			f.ColumnStats[col] = cs
		}
		key, err := c.putFileSketches(ctx, name, f.Path, entries)
		if err != nil {
			return analyzed, fmt.Errorf("analyze %s: upload sketches for %s: %w", name, f.Path, err)
		}
		f.SketchesKey = key
		manifest.Partitions[r.pi].Files[r.fi] = f
		// Keyed by PATH, not by (partition, file) index: the manifest this
		// commits into is re-read under CAS below, and a concurrent writer
		// can have changed both indices by then. A path is immutable
		// (#494), which is why it is the join key.
		sketched[f.Path] = sketchedFile{key: f.SketchesKey, stats: f.ColumnStats}
		analyzed++
	}

	// Persist the row-group metadata blob BEFORE the manifest so any
	// reader that sees the new RGMetaKey finds the blob present. (The
	// reverse race — old manifest, new blob — is safe by construction:
	// blob entries are keyed by immutable file paths.)
	rgMetaKey := ""
	if len(rgMetaFiles) > 0 {
		k, err := c.PutTableRGMeta(ctx, name, rgMetaFiles)
		if err != nil {
			return analyzed, fmt.Errorf("analyze %s: %w", name, err)
		}
		rgMetaKey = k
	}

	// Commit through the same revision CAS every other manifest writer
	// uses, re-reading and re-attaching on a mismatch.
	//
	// This used to be a blind Put. ANALYZE reads the manifest, then spends
	// MINUTES decoding every file (1-2 min for SF10 lineitem, longer at
	// SF100), then writes back whatever it read at the start — so an
	// AddFiles that committed anywhere in that window was overwritten and
	// the files it registered vanished from the manifest. Ingest flushes
	// on a 60-second cadence by default, so the window is not theoretical
	// (#830). The catalog is built on revision CAS (CLAUDE.md §Storage);
	// this was the one manifest writer outside it.
	if err := c.commitAnalyzed(name, sketched, rgMetaKey); err != nil {
		return analyzed, fmt.Errorf("analyze %s: persist manifest: %w", name, err)
	}
	return analyzed, nil
}

// sketchedFile is what ANALYZE computed for one file, joined back onto the
// manifest by the file's immutable path.
type sketchedFile struct {
	key   string
	stats map[string]FileColumnStats
}

// commitAnalyzed re-reads the manifest under CAS and re-attaches the
// sketches ANALYZE computed, so files another writer added during the scan
// survive. Files that are gone by commit time are simply not found — their
// sketch blob is orphaned, which the object store's own lifecycle handles
// and which is strictly better than resurrecting a removed file.
func (c *Catalog) commitAnalyzed(name string, sketched map[string]sketchedFile, rgMetaKey string) error {
	c.invalidateManifestCache(name)
	key := c.key("manifest." + name)
	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		data, rev, err := c.kv.Get(key)
		if err != nil {
			return err
		}
		var fresh PartitionManifest
		if err := json.Unmarshal(data, &fresh); err != nil {
			return fmt.Errorf("unmarshaling manifest: %w", err)
		}
		for pi := range fresh.Partitions {
			for fi := range fresh.Partitions[pi].Files {
				upd, ok := sketched[fresh.Partitions[pi].Files[fi].Path]
				if !ok {
					continue
				}
				fresh.Partitions[pi].Files[fi].SketchesKey = upd.key
				fresh.Partitions[pi].Files[fi].ColumnStats = upd.stats
			}
		}
		if rgMetaKey != "" {
			fresh.RGMetaKey = rgMetaKey
		}
		// Bump the manifest version: ANALYZE changed its content (sketch
		// keys, cleared inline bytes), and cross-process consumers key
		// derived-stats caches on UpdatedAt.
		fresh.UpdatedAt = time.Now().UTC()
		updated, err := json.Marshal(fresh)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}
		if _, err = c.kv.Update(key, updated, rev); err == ErrRevisionMismatch {
			casBackoff(attempt)
			c.invalidateManifestCache(name)
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("manifest update failed after %d CAS retries (table %q)", maxRetries, name)
}

// fileColumnSketches bundles HLL and reservoir-sample collectors per
// column, produced by computeFileSketches in one decode pass.
type fileColumnSketches struct {
	hlls     map[string]*HLL
	samplers map[string]*ReservoirSampler
	colTypes map[string]parquet.TypeID
	// rgStats is the file's per-row-group footer metadata (row counts +
	// column min/max/null), captured for the table RG-metadata blob so
	// scans can enumerate and prune row groups without re-reading
	// footers over the network (see rgmeta.go).
	rgStats []parquet.RowGroupStats
}

// computeFileSketches reads a parquet file's data and builds per-column
// HLLs + reservoir samples in one decode pass. Skips nested types
// (Array/Row/Map) where the value→hash encoding isn't well-defined.
//
// Uses ReadRowGroup which materializes each row as a map[string]any —
// the same value representation the ingest path uses, so produced
// stats are byte-compatible across collection sites.
func computeFileSketches(ctx context.Context, store objstore.Store, bucket, path string) (*fileColumnSketches, error) {
	rc, _, err := store.Get(ctx, bucket, path)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("parquet reader: %w", err)
	}
	schema := reader.Schema()

	out := &fileColumnSketches{
		hlls:     make(map[string]*HLL, len(schema.Columns)),
		samplers: make(map[string]*ReservoirSampler, len(schema.Columns)),
		colTypes: make(map[string]parquet.TypeID, len(schema.Columns)),
	}
	// The columns actually sketched, in schema order, are also the columns
	// READ: ReadRowGroup assembles the nested containers it is asked for
	// (#428), and this pass discards every one of them — IsHLLSupportedType
	// says no to ARRAY, ROW and MAP because the value→hash encoding is not
	// well defined for them. Projecting keeps that "skip" from costing a
	// full recursive assembly of every container in the file.
	var sketchCols []string
	for _, col := range schema.Columns {
		if !IsHLLSupportedType(col.Type) {
			continue
		}
		out.hlls[col.Name] = &HLL{}
		out.samplers[col.Name] = NewReservoirSampler(SampleDefaultSize)
		out.colTypes[col.Name] = col.Type
		sketchCols = append(sketchCols, col.Name)
	}

	nrg := reader.NumRowGroups()
	out.rgStats = make([]parquet.RowGroupStats, nrg)
	for rg := 0; rg < nrg; rg++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out.rgStats[rg] = reader.RowGroupStats(rg)
		if len(sketchCols) == 0 {
			// Nothing to sketch: the footer stats above are the whole
			// answer, and a projection of no columns would read them all.
			continue
		}
		rows, err := reader.ReadRowGroup(rg, sketchCols)
		if err != nil {
			return nil, fmt.Errorf("row group %d: %w", rg, err)
		}
		for _, row := range rows {
			for col, h := range out.hlls {
				v, ok := row[col]
				if !ok || v == nil {
					continue
				}
				AddValueToHLL(h, v, out.colTypes[col])
				out.samplers[col].Add(v)
			}
		}
	}
	return out, nil
}
