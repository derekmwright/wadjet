package catalog

import (
	"bytes"
	"context"
	"fmt"
	"io"

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

	analyzed := 0
	for pi, part := range manifest.Partitions {
		for fi, f := range part.Files {
			if err := ctx.Err(); err != nil {
				return analyzed, err
			}
			sk, err := computeFileSketches(ctx, c.store, c.bucket, f.Path)
			if err != nil {
				return analyzed, fmt.Errorf("analyze %s: file %s: %w", name, f.Path, err)
			}
			if sk == nil || len(sk.hlls) == 0 {
				continue
			}
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
			manifest.Partitions[pi].Files[fi] = f
			analyzed++
		}
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

