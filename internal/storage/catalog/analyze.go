package catalog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
	manifest, err := c.GetManifest(ctx, name)
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
		pi, fi    int
		sketches  *fileColumnSketches
		err       error
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
	for r := range resCh {
		if r.err != nil {
			return analyzed, fmt.Errorf("analyze %s: file %s: %w", name, manifest.Partitions[r.pi].Files[r.fi].Path, r.err)
		}
		sk := r.sketches
		if sk == nil || len(sk.hlls) == 0 {
			continue
		}
		f := manifest.Partitions[r.pi].Files[r.fi]
		if f.ColumnStats == nil {
			f.ColumnStats = make(map[string]FileColumnStats, len(sk.hlls))
		}
		for col, h := range sk.hlls {
			cs := f.ColumnStats[col]
			cs.HLL = h.Bytes()
			if sampler := sk.samplers[col]; sampler != nil {
				vals, total, tc := sampler.Snapshot()
				if len(vals) > 0 {
					cs.Sample = SampleBytes(vals, total, tc)
				}
			}
			f.ColumnStats[col] = cs
		}
		manifest.Partitions[r.pi].Files[r.fi] = f
		analyzed++
	}

	c.invalidateManifestCache(name)
	if err := c.putJSON(c.key("manifest."+name), manifest); err != nil {
		return analyzed, fmt.Errorf("analyze %s: persist manifest: %w", name, err)
	}
	return analyzed, nil
}

// fileColumnSketches bundles HLL and reservoir-sample collectors per
// column, produced by computeFileSketches in one decode pass.
type fileColumnSketches struct {
	hlls     map[string]*HLL
	samplers map[string]*ReservoirSampler
	colTypes map[string]parquet.TypeID
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
	for _, col := range schema.Columns {
		if !IsHLLSupportedType(col.Type) {
			continue
		}
		out.hlls[col.Name] = &HLL{}
		out.samplers[col.Name] = NewReservoirSampler(SampleDefaultSize)
		out.colTypes[col.Name] = col.Type
	}

	nrg := reader.NumRowGroups()
	for rg := 0; rg < nrg; rg++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := reader.ReadRowGroup(rg, nil)
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

