# Steady runs are slower than cold runs — diagnosis (2026-08-08)

## The anomaly

Same-window SF100 pairs on 2026-08-08 (results/20260808-144825 ctl
1fd166a / 155850 trt 90eeb17, 2 runs each): BOTH arms' run 2 was far
slower than their own run 1 — ctl 503→796s (+58%), trt 447→832s
(+86%) — on byte-identical plans and row-identical outputs
(task-result row totals: 3.62B run 1 vs 3.60B run 2). The 08-06
best-ever window showed the same inversion milder (cold 406 / steady
460). "Steady" is the headline number quoted against Trino, and it is
the regime a long-lived cluster actually runs in.

## Attribution (wlogs, both arms agree)

1. **Decode-ahead pressure stalls: ~90s (run 1) → ~780-990s (run 2)**
   summed across workers; per-query the stalls land on the biggest
   run-2 losers (Q02 203-249s, Q21 117-154s, Q04 132s, Q17 116s, Q03
   118s). Run-1 stalls are ≈0 everywhere.
2. **The page-cache refault sensor drives them**: run-2 sample rates
   hit 11k-71k pages/s (the sensor's calibrated "thrash" range),
   ~14-25 activation episodes per worker, thousands of episode-cap
   ignores. Each honored episode collapses decode-ahead for up to the
   10s cap (refault-sensor v3); sustained ambient refaulting re-arms
   episodes endlessly (a quiet sample deactivates → two hot samples
   re-arm a fresh episode), so the cap bounds one episode but not the
   regime. Per worker the stall totals reached 230-460s.
3. **Token stalls triple** (377→1354s ctl) — downstream of the
   collapsed window depth, not an independent cause.
4. **Fragment phase totals** inflate ~2190s→3800s spread across
   src/ops/sink — a broad pressure regime (GC/heap backpressure and
   slower I/O included), not a single choke point.

## Root cause

The sensor's founding premise — "healthy streaming reads are
first-touch faults; refaults ≈ 0/s; genuine thrash is 15k+/s — four
orders of magnitude of separation" (scan-decode-pipelining.md §9) —
**only holds while files are being read for the first time.** After
run 1:

- Each worker's NVMe base-table cache holds ~26 GB (ctl full
  replication) against ~10-12 GB of RAM available for page cache
  beside the 20 GB pool budget. Steady-state scans of cached files
  are *by kernel accounting* workingset refaults — the pages were
  cached during populate and evicted since.
- Run 1 additionally enjoys an empty page cache at boot: its reads
  consume just-written S3/peer bytes (effectively RAM), and its
  shuffle/spill churn has free headroom to grow into. By run 2 the
  page cache is saturated; every read/write evicts something and the
  LRU churns. Refault bursts (up to 273 MB/s equivalent) exceed the
  base-table streaming rate (~19 MB/s of cache-file opens), so the
  churn is the whole file working set — shuffle scratch, spill runs,
  cache files — not just table reads.

So in the steady regime the engine's own designed behavior generates
exactly the signal the sensor interprets as displacement thrash, and
the "protection" (collapsing decode-ahead width) taxes every scan
while relieving nothing (v3's own finding, now shown to be the
*permanent* steady-state, not an edge).

Not peer-tier related: both arms identical; the inversion predates
2026-08-08 (visible 08-06; likely present since the base-table cache
made run-1 reads page-cache-hot, 07-13).

## Fix directions (architecture, ranked)

1. **Drop-behind for single-pass I/O**: shuffle files, spill runs,
   and base-table populate writes are written once, read once (or
   never), then deleted. posix_fadvise(POSIX_FADV_DONTNEED) after
   consumption/write (and MADV_SEQUENTIAL on scan mmaps) stops
   single-pass data from evicting reusable pages. Attacks the churn
   itself; benefits every reader; sensor signal becomes clean again.
2. **Self-streaming-aware sensor**: subtract the engine's own
   expected streaming page-in rate (base-table cache hit-open rate is
   already tracked) from the refault rate before thresholding — the
   sensor should never classify the system's designed steady-state as
   thrash. Cheaper than (1) but treats the symptom (stalls), not the
   displacement.
3. Re-examine the collapse response: under sustained ambient
   pressure, collapsing width costs wall with no relief (measured
   again here); the episode-cap re-arm loophole needs a regime-level
   backoff (e.g. exponential episode suppression) if (1)/(2) don't
   quiet the signal.

Validation shape: SF100 single-arm, benchmark_runs=2, fix vs main;
verdict on run-2 pressure_stall_ms ≈ 0, refault activations ~0, and
run-2 wall ≤ run-1 wall (the historical cache-less relationship),
rows identical. A zero-EC2 repro is plausible: SF1 harness under a
docker memory cap (spawn-wrapper) sized so cache+scratch > RAM.
