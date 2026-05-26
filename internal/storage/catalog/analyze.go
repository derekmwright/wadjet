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
			hlls, err := computeFileHLLs(ctx, c.store, c.bucket, f.Path)
			if err != nil {
				return analyzed, fmt.Errorf("analyze %s: file %s: %w", name, f.Path, err)
			}
			if len(hlls) == 0 {
				continue
			}
			if f.ColumnStats == nil {
				f.ColumnStats = make(map[string]FileColumnStats, len(hlls))
			}
			for col, h := range hlls {
				cs := f.ColumnStats[col]
				cs.HLL = h.Bytes()
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

// computeFileHLLs reads a parquet file's data and builds per-column
// HLLs. Skips nested types (Array/Row/Map) where NDV isn't well-defined.
//
// Uses ReadRowGroup which materializes each row as a map[string]any —
// the same value representation that the ingest path operates on, so
// the produced HLLs are byte-compatible (same hash → same registers).
func computeFileHLLs(ctx context.Context, store objstore.Store, bucket, path string) (map[string]*HLL, error) {
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

	hlls := make(map[string]*HLL, len(schema.Columns))
	colTypes := make(map[string]parquet.TypeID, len(schema.Columns))
	for _, col := range schema.Columns {
		if !IsHLLSupportedType(col.Type) {
			continue
		}
		hlls[col.Name] = &HLL{}
		colTypes[col.Name] = col.Type
	}

	nrg := reader.NumRowGroups()
	for rg := 0; rg < nrg; rg++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// ReadRowGroup yields []map[string]any — same shape as ingest's
		// raw rows. selectedColumns=nil reads all columns at once;
		// per-column reads would amplify decode passes.
		rows, err := reader.ReadRowGroup(rg, nil)
		if err != nil {
			return nil, fmt.Errorf("row group %d: %w", rg, err)
		}
		for _, row := range rows {
			for col, h := range hlls {
				v, ok := row[col]
				if !ok || v == nil {
					continue
				}
				AddValueToHLL(h, v, colTypes[col])
			}
		}
	}
	return hlls, nil
}

