# Catalog analyze sketch publication

Source: internal/storage/catalog/analyze.go — func (c *Catalog) AnalyzeTable(ctx context.Context, name string) (int, error) {, moved 2026-09-11 (#1026)
Superseded: ANALYZE now computes files concurrently, stores sketches in referenced object-store blobs, and returns the first received file error instead of logging and skipping failed files.

AnalyzeTable computes HyperLogLog sketches over every column of every
file in the named table and writes them back into the manifest's
FileColumnStats.HLL field. Idempotent — re-running ANALYZE replaces
existing HLLs with freshly computed ones.

Used when a table's data was pre-staged (e.g., the SF10/SF100 EC2
deploy buckets) without going through the ingest path, so HLL never
got collected at write time. The planner's NDV estimator then has
real distinct-count data instead of falling back to min/max-range
heuristics or FK-naming.

Strategy: for each file, download the parquet bytes, decode row
groups via the existing parquet.Reader API, hash every column value
into a per-(file, column) HLL. After all files of one table are
processed, persist the augmented manifest.

Cost: one full table scan, decompressed but not joined. SF10 lineitem
(60 chunks × 1M rows × 16 cols) takes 1-2 minutes serial. Cheap
relative to a single query at the same scale; expected to run once
per data load.

Returns the count of files analyzed and any error from the first
failed file. Files that fail (corrupt, missing) are logged and
skipped — partial coverage is better than total failure.
