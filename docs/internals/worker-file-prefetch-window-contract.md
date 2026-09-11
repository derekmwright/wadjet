# Worker file prefetch window contract

Source: internal/worker/scan_prefetch.go — type filePrefetcher struct {, moved 2026-09-11 (#1026)
Superseded: Streaming shuffle inputs are now prefetched through their tier-aware path when enabled; eligibility is no longer parquet-only.

filePrefetcher downloads a source's upcoming S3 parquet files to the
spill dir while the current file decodes. Before it existed, the scan
path was strictly serial per task: one full-object GET blocked in
io.Copy until the entire file landed on NVMe, then decode ran with the
connection idle — on a standalone box that capped effective S3 read
parallelism at MaxConcurrent streams and produced the 2026-07-05 SF10
cold-S3 finding (suite 20m15s vs DuckDB httpfs 2m51s, same instance).

Design constraints:
  - Strictly best-effort: any failure (miss, transient error, open
    failure on the temp) makes the consumer fall through to the
    untouched tiered open path in openNextFile. Prefetch can therefore
    never change results, only overlap I/O with decode.
  - Parquet keys only. Shuffle inputs (.wshf / partition=) resolve via
    the LocalStageCache / NATS-KV / peer tiers, which are either local
    or explicitly preferred over the durable S3 copy; blind-GETting
    them here would race the producer's async upload for no benefit.
  - Delivery is by file index and the consumer takes indices in order.
    The byte window admits the lowest not-yet-taken index regardless of
    occupancy: workers can finish downloads out of order, so without
    the bypass the window could fill with later files while the one the
    consumer is blocked on cannot start — a deadlock, not just a stall.
  - Whole-object GETs, no ranged reads: per-column ranged reads were
    tried 2026-03-20 and reverted the same day for S3 throttling
    (fe52a79); file-granularity requests at this fan-out are the shape
    S3 likes.
