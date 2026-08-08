# Single-pass drop-behind + refault-sensor streaming discount

Status: shipped (default ON), 2026-08-08. Kill switches:
`WADJET_DROP_BEHIND=0` (all drop-behind mechanisms),
`WADJET_REFAULT_STREAM_DISCOUNT=0` (sensor discount only).

## Problem

Steady (run-2) SF100 suites ran 60-86% slower than their own cold runs on
byte-identical plans (both arms of the 2026-08-08 pair; visible milder on
08-06). Diagnosis in docs/benchmarks/steady-slower-than-cold-2026-08-08.md:

1. By run 2 the page cache is saturated (per-worker NVMe base-table cache
   ~26 GB against ~10-12 GB of RAM left beside the pool budget). Every
   single-pass byte the engine writes or reads — shuffle scratch, spill
   runs, cache populate copies — evicts a reusable page, and steady-state
   re-reads of cache files are workingset refaults *by kernel accounting*.
2. The §9 refault sensor (scan-decode-pipelining.md) reads that designed
   behavior as displacement thrash: 11k-71k pages/s samples, ~14-25
   activation episodes per worker, 230-460s of decode-ahead pressure
   stalls per worker per run, with the episode-cap re-arm loophole
   (quiet sample → two hot samples → fresh episode) making the cap bound
   one episode but not the regime.

Two complementary fixes, both architecture-level (no threshold tuning):

## Fix 1 — drop-behind on single-pass I/O

Single-pass bytes must not compete with reusable pages. Mechanisms, all
gated on `WADJET_DROP_BEHIND` (default on):

- **Spill-class writers decoupled from `--bounded-dirty-writes`**
  (`diskio.NewWriter(f, Spill)` — sort/window runs, join build/probe
  partitions, aggregate partial state, raw-row spill, base-table
  populate/peer-spool copies): windowed async writeback + FADV_DONTNEED
  behind the write cursor. The bdw flag already defaults true, so this
  is a semantic decoupling (a Spill file is single-pass by definition —
  its drop treatment should not turn off with the dirty-bounding knob),
  not a behavior change on default deployments. It also means the write
  side was ALREADY active during the pathological 08-08 runs — the new
  leverage is on the read side below. KeepResident writers (imminently
  re-read: cache downloads, stage sinks) stay behind the flag.
- **`diskio.DropBehindReader`** on single-pass read-backs
  (`openSpillBatchReader` — sort runs, window runs, join spill;
  `openPartialSpillReader`; `ReadSpilledRows`): FADV_DONTNEED one full
  window behind the cursor, whole-file drop at EOF. The
  empty-PARTITION-BY window two-pass evaluator re-reads its runs once
  more from NVMe — accepted; in the regimes where drop-behind matters the
  runs would not have stayed resident anyway.
- **WSHF mmap walk drop-behind** (`cachedFileStreamSource.dropBehindWalk`):
  a downloaded shuffle temp is written once, walked once by the chunk
  reader, then unlinked. The walk MADV_DONTNEEDs fully-decoded 8 MiB
  windows behind the strictly-monotonic cursor (batches copy column data
  out, so behind-pages are dead). Owned temps only (`localPath != ""`):
  LocalStageCache-owned files may be re-walked or peer-served.
- **MADV_SEQUENTIAL on scan mmaps** (all five mmap sites in
  stream_source.go): doubled readahead for the forward walk; behind-pages
  become preferred reclaim victims under pressure.

Not covered deliberately: stage-sink outputs and cache downloads
(KeepResident, read imminently), LocalStageCache-owned files (multi-
reader), base-table cache read mmaps (multi-pass across queries — the
kernel LRU decides; their refaults are fix 2's business).

## Fix 2 — refault-sensor streaming discount

The sensor's founding premise — "healthy streaming reads are first-touch
faults, refaults ≈ 0/s" — only holds while files are read for the FIRST
time. Steady-state hit-opens stream ~19 MB/s ≈ 4800 pages/s of
legitimate refaults, well above the 1000/s threshold, so fix 1 alone
cannot quiet the sensor.

`memory.SetPageCacheStreamingSource` (registered at worker startup)
supplies the base-table cache's `hit_bytes + peer_serve_bytes` cumulative
counter; each sensor sample converts the same-interval byte delta to
pages/s and subtracts it before thresholding. Genuine displacement thrash
(15k-95k pages/s measured) still clears the threshold by an order of
magnitude. The source over-counts slightly (hit-opens count whole-file
sizes; projection-pruned scans fault fewer pages) — acceptable, the
separation argument needs only order-of-magnitude accuracy.

## Rollout markers (wlogs)

- `drop-behind stats` / `(final)`: `write_drop_bytes`, `read_drop_bytes`
  (mmap-walk drops fold into read).
- `scan decode-ahead stats`: new `refault_discount` (pages/s subtracted
  at the last sample) beside `refault_rate`.

## Verdict criteria (SF100, benchmark_runs=2, fix vs 753ac51)

- Run-2 `pressure_stall_ms` ≈ 0 and `refault_activations` ≈ 0.
- Run-2 wall ≤ run-1 wall (the historical cache-less relationship).
- Rows identical both arms; drop-behind markers nonzero.
- `WADJET_REFAULT_PRESSURE_RATE=0` remains the sensor-off A/B lever to
  separate sensor tax from raw I/O cost if the pair is ambiguous.
