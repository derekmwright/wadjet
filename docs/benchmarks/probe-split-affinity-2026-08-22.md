# Probe-split peer-tier evidence: the SF100 window-3 base-table ledger (2026-08-22)

Evidence memo behind the fix in `docs/design/scan-affinity.md` §"Probe-split
affinity (landed 2026-08-22)". That note names the mechanism and the fix;
this memo is the base-table cache ledger and task census the note's
numbers are drawn from. Read the design note for the mechanism — it is not
repeated here.

**Source:** the same five-arm SF100 window-3 deploy as
`docs/benchmarks/sf100-window3-analysis-2026-08-22.md` (§2, §8.3), one EC2
deploy, coord c7g.2xlarge + 3× c7gd.4xlarge, 4 runs/arm (run 1 cold).

| arm | engine | switch | run id | wlogs |
|---|---|---|---|---|
| A base | `23abd8e` (v0.16.0-correctness) | — | `20260822-053854` | `results/w3base/wlogs/` |
| B cand | `69aecbb` (waves 1+2+3) | — | `20260822-055421` | `results/w3cand/wlogs/` |
| C waitoff | `69aecbb` | `WADJET_DURABLE_WAIT_BACKOFF=0`+`WADJET_INTERM_PEER_HINTS=0`(no-op, coordinator-side — §0 of the window-3 memo) | `20260822-060827` | `results/w3waitoff/wlogs/` |
| D reuseoff | `69aecbb` | `WADJET_VECTOR_REUSE=0` | `20260822-062217` | `results/w3reuseoff/wlogs/` |
| E prefetchoff | `69aecbb` | `WADJET_PREFETCH_AT_INIT=0` | `20260822-063617` | `results/w3prefetchoff/wlogs/` |

None of these five arms carries the probe-split-affinity fix — it landed
after this window. This memo documents the pre-fix behavior that motivated
it; §D gives the grep recipe for the post-fix shape.

## Method

Run boundaries: each arm's coordinator log (`coord-journal.gz`) carries
exactly 88 `msg="stage-DAG dispatch" query=<hash>` lines per arm, dispatched
strictly in TPC-H Q01..Q22 order (cross-checked against `coord-benchmark.log`'s
`Q01 Pricing Summary Report`-style result lines and query-name text —
dispatch index 0 is `Q01`, index 21 is `Q22`, index 22 restarts at `Q01` of
run 2, etc.). Run *n*'s start = the dispatch timestamp of that run's `Q01`.
A task or cache-stats line is assigned to (run, query) by the latest
dispatch timestamp at or before it. Every `fragment task phases` line for a
probe-split stage in this memo's census reproduces the same firing counts
per arm as `sf100-window3-analysis-2026-08-22.md` §2.3 (31/30/29/31/31
total, steady-run breakdown 3/6/3/2/3) — an exact match, so the mapping is
sound.

**Stage-id correction.** The design note names Q08→`join-2` and
Q17→`join-19`. Walking `dispatchComputeStage` in this window's coordinator
log (all five arms agree) gives a different pairing: Q08 and Q09 share the
local stage id `join-6` (both are the sole `probe_split=true broadcast_join`
in their respective plans — TPC-H Q08/Q09 share the lineitem-part-supplier
join shape, so the physical planner's per-query stage counter lands on the
same number by coincidence), Q17's probe-split join is `join-2`, and
`join-19` belongs to Q02 (which also has a second probe-split join,
`join-4`). This matches the earlier `straggler-tier-verdict-2026-08-16.md`,
which also calls Q08's stage `join-6`. The tables below use the verified
mapping: **Q08/Q09 → `join-6`, Q17 → `join-2`, Q02 → `join-19`/`join-4`**.

## A. Base-table cache: S3 misses flat, peer tier still filling through run 3

Cluster-wide (Σ 3 workers) per-run deltas of `misses`/`miss_bytes` and
`peer_hits`/`peer_bytes`, from `base-table cache stats` lines (cumulative
counters, ~once/minute/worker; deltas anchored at 0 at process start):

| arm | | run1 (cold) | run2 | run3 | run4 |
|---|---|---|---|---|---|
| A base | misses / miss_bytes | 94 / 24.5 GB | 0 / 0 | 0 / 0 | 0 / 0 |
| | peer_hits / peer_bytes | 142 / 33.4 GB | 44 / 11.6 GB | 23 / 6.0 GB | 0 / 0 |
| B cand | misses / miss_bytes | 98 / 26.1 GB | **2 / 0.3 GB** | 0 / 0 | 0 / 0 |
| | peer_hits / peer_bytes | 98 / 20.6 GB | 45 / 12.5 GB | 50 / 13.7 GB | 13 / 3.9 GB |
| C waitoff | misses / miss_bytes | 103 / 26.3 GB | 0 / 0 | 0 / 0 | 0 / 0 |
| | peer_hits / peer_bytes | 124 / 27.4 GB | 58 / 16.5 GB | 11 / 2.8 GB | 3 / 0.7 GB |
| D reuseoff | misses / miss_bytes | 101 / 25.2 GB | 0 / 0 | 0 / 0 | 0 / 0 |
| | peer_hits / peer_bytes | 145 / 35.1 GB | 40 / 10.6 GB | 0 / 0 | 7 / 1.5 GB |
| E prefetchoff | misses / miss_bytes | 96 / 24.4 GB | 0 / 0 | 0 / 0 | 0 / 0 |
| | peer_hits / peer_bytes | 113 / 29.4 GB | 49 / 13.5 GB | 6 / 1.5 GB | 13 / 3.3 GB |

**Misses are 0 in every arm from run 2 on**, with one exception: B cand
run 2 shows 2 misses / 0.3 GB on one worker (`w74ca91`) — the only nonzero
steady-run miss delta across 5 arms × 3 runs × 3 workers (44 cells). The
cache never evicts in this window (`evictions=0` on every worker, every
arm, at the final stats line), so a run-2+ miss can only be a key the
cluster has genuinely never touched — first-touch residue, not a re-read.

**Peer traffic is still substantial in run 2, tapers in run 3, and is
smallest (0–13 hits) in run 4** — consistent with a cache that is still
converging toward full replication (106 entries = the whole scanned
working set once every worker holds every base-table file it will ever be
handed). It does not reach exactly 0 in run 4 in four of five arms; the
residual 3–13 hits/run there are the same mechanism, on whichever files
that run's placement drew to a non-owning worker.

Cumulative totals per worker at the end of run 4 (misses, peer_hits,
peer_bytes, resident entries/bytes, `peer_serves`/`readthroughs` — this
worker serving *other* workers, and reading through *for* them):

| arm | worker | misses | peer_hits | peer_bytes | entries | bytes | peer_serves | readthroughs |
|---|---|---|---|---|---|---|---|---|
| A base | 106052 | 39 | 61 | 15.1 GB | 105 | 25.7 GB | 87 | 10 |
| | 778791 | 31 | 70 | 17.2 GB | 106 | 26.0 GB | 70 | 7 |
| | 921d1a | 24 | 78 | 18.8 GB | 104 | 25.4 GB | 52 | 4 |
| B cand | 3b92f2 | 33 | 64 | 16.1 GB | 101 | 24.9 GB | 73 | 9 |
| | 74ca91 | 33 | 70 | 17.1 GB | 106 | 26.0 GB | 69 | 8 |
| | e545a8 | 34 | 72 | 17.5 GB | 105 | 25.8 GB | 64 | 4 |
| C waitoff | b51f8b | 33 | 72 | 17.7 GB | 106 | 26.0 GB | 60 | 3 |
| | e9c43a | 37 | 50 | 12.4 GB | 90 | 21.4 GB | 80 | 8 |
| | fdd94e | 33 | 74 | 17.3 GB | 106 | 26.0 GB | 56 | 2 |
| D reuseoff | 66c61c | 37 | 58 | 14.6 GB | 97 | 23.8 GB | 72 | 5 |
| | 8ffe7c | 27 | 70 | 17.5 GB | 98 | 24.1 GB | 53 | 4 |
| | aae2e7 | 37 | 64 | 15.1 GB | 103 | 25.3 GB | 67 | 6 |
| E prefetchoff | 372514 | 30 | 74 | 18.7 GB | 106 | 26.0 GB | 64 | 4 |
| | 9ac5f8 | 36 | 62 | 15.1 GB | 106 | 26.0 GB | 88 | 11 |
| | ca2833 | 30 | 76 | 18.3 GB | 106 | 26.0 GB | 60 | 4 |

Every worker in every arm ends with a base-table cache that spans nearly
the full catalog it touches (97–106 entries, 21–26 GB) but with different
sets, since placement (not content) determines residency — hence peer
serving stays nonzero even after 4 full suite runs.

## B. Straggler census: every steady-run firing is a probe-split broadcast join

Census of `fragment task phases` lines with `acq_prefetch_ms ≥ 3000`,
restricted to runs 2–4 (17 total across 5 arms), joined against
`dispatchComputeStage`'s `probe_split` flag for that exact (run, query,
stage_id):

| arm | run | Q | worker | stage | files | acq_prefetch_ms | ms/file | bucket Δpeer_hits | bucket Δpeer_bytes | MB/s |
|---|---|---|---|---|---|---|---|---|---|---|
| A base | 2 | Q09 | 921d1a | join-6 | 4 | 7 477 | 1 869 | 10 | 2 787 MB | 149.1 |
| A base | 2 | Q17 | 106052 | join-2 | 2 | 4 827 | 2 414 | 14 | 3 770 MB | 111.6 |
| A base | 2 | Q17 | 778791 | join-2 | 3 | 9 180 | 3 060 | 14 | 3 593 MB | 83.9 |
| B cand | 2 | Q09 | 3b92f2 | join-6 | 2 | 6 324 | 3 162 | 13 | 3 678 MB | 89.5 |
| B cand | 2 | Q09 | e545a8 | join-6 | 3 | 8 822 | 2 941 | 19 | 5 146 MB | 92.1 |
| B cand | 2 | Q17 | 74ca91 | join-2 | 3 | 7 142 | 2 381 | 13 | 3 676 MB | 118.8 |
| B cand | 2 | Q17 | 3b92f2 | join-2 | 4 | 7 775 | 1 944 | 15 | 4 093 MB | 140.4 |
| B cand | 3 | Q09 | e545a8 | join-6 | 3 | 7 986 | 2 662 | 14 | 3 970 MB | 106.5 |
| B cand | 4 | Q08 | 74ca91 | join-6 | 3 | 6 603 | 2 201 | 13 | 3 853 MB | 134.7 |
| C waitoff | 2 | Q08 | b51f8b | join-6 | 3 | 6 805 | 2 268 | 15 | 4 093 MB | 120.3 |
| C waitoff | 2 | Q08 | fdd94e | join-6 | 3 | 8 390 | 2 797 | 19 | 5 321 MB | 100.1 |
| C waitoff | 2 | Q17 | fdd94e | join-2 | 3 | 6 799 | 2 266 | 12 | 3 375 MB | 124.1 |
| D reuseoff | 2 | Q09 | aae2e7 | join-6 | 2 | 5 803 | 2 902 | 13 | 3 654 MB | 96.9 |
| D reuseoff | 2 | Q09 | 66c61c | join-6 | 3 | 7 905 | 2 635 | 13 | 3 495 MB | 102.0 |
| E prefetchoff | 2 | Q09 | ca2833 | join-6 | 3 | 7 821 | 2 607 | 13 | 3 498 MB | 103.2 |
| E prefetchoff | 2 | Q09 | 9ac5f8 | join-6 | 3 | 8 087 | 2 696 | 14 | 3 972 MB | 105.3 |
| E prefetchoff | 2 | Q09 | 372514 | join-6 | 3 | 9 018 | 3 006 | 14 | 4 147 MB | 98.5 |

**17/17 steady-run firings are on a `probe_split=true broadcast_join`
stage. Zero are anything else** — no hash join, no aggregate, no scan
outside a probe-split. All 17 are `join-6` (Q08 or Q09) or `join-2` (Q17);
Q02's `join-19`/`join-4` never draws a steady-run firing in this window
(it fires only in run 1, files=1, ≤5.3 s — smaller broadcast, cheaper
fallback).

**Every one coincides with a `peer_hits` increment on that same worker**,
bucketed by the task's *start* time (`completion time − elapsed_ms`) rather
than its completion time — the acquisition stall is front-loaded in the
task (src phase precedes ops/sink), so bucketing by completion time missed
one of 17 by one minute-boundary; bucketing by start time gets 17/17.
Granularity is the cache stats' native ~60 s cadence, so "coincides" means
same-minute, not same-millisecond.

**Per-file cost.** `acq_prefetch_ms / acq_prefetch_files` ranges
1 869–3 162 ms/file (Σ 126 764 ms / 50 files = 2 535 ms/file average).
Cross-checked against the bucket's `peer_bytes / peer_hits` (bytes moved
per file that minute, on that worker): 257–296 MB/file, mean 278.1 MB —
matching the design note's independently-stated SF100 probe-file size
(283 MB) to within 2%. Dividing gives an **effective peer-tier throughput
of 84–149 MB/s, mean 110 MB/s** across the 17 firings. This is well under
NIC or NVMe line rate, consistent with the design note's account of what
`acq_prefetch_ms` actually paid for on these tasks: the file prefetcher's
`Get` triggers the base-table cache's peer populate (spool + fsync onto
the cache volume), and — in this pre-fix code — the prefetcher then copied
that same file a second time into its own spill dir (`streamToSpill`,
`internal/worker/scan_prefetch.go`) with a second fsync, serialized behind
a 256 MB `scanPrefetchWindowBytes` window smaller than the file itself
(`internal/worker/scan_prefetch.go:53`). The measured 110 MB/s is the
double-copy-plus-fsync path, not the tier's raw transfer speed.

## C. What this rules out

- **Not S3.** Misses are 0 on every worker in every arm from run 2 on
  (one 2-file exception, itself first-touch). If any steady-run straggler
  were an S3 read, `misses`/`miss_bytes` would move in that minute; they
  do not.
- **Not the durable wait.** `durable_waits` never appears in any
  `fragment task phases` line across all 5 arms (0 occurrences) — the
  field only prints when > 0 (`internal/worker/src_acq_stats.go`
  `attrs()`), so it is genuinely zero everywhere, not merely unlogged.
- **`acq_peer_files = 0` on these tasks is not evidence the peer tier was
  idle — it is the wrong ledger.** `srcAcqStats`'s `acqPeer` tier
  (`internal/worker/src_acq_stats.go:21`, counted at
  `internal/worker/stream_source.go:615-637`) fires only for **stage-output**
  files the coordinator hinted via `peers.hintFor` — shuffle/exchange
  reads from `WADJET_INTERM_PEER_HINTS`, a different subsystem from the
  base-table cache. A probe file is a base-table parquet object; its
  acquisition tier is `acqPrefetch` or `acqBaseCache`
  (`internal/worker/stream_source.go:507-565`), and whichever tier resolves
  it, the per-task line records only the wall it cost — not whether the
  fetch *underneath* was a peer transfer or a real S3 GET. The base-table
  cache's own peer counters (`peer_hits`/`peer_bytes`, §A above) are the
  only ledger that distinguishes them, and they are the ledger that moved.
  `sf100-window3-analysis-2026-08-22.md` §8.3 read `acq_peer_files=0` as
  ruling out "the peer tier" generally; it rules out only the
  stage-output peer tier, and this memo's §A/§B show the base-table peer
  tier is exactly where the bytes went.
- **Not eviction/thrash.** `evictions=0` on every worker, every arm, at
  every stats line in the window (§A) — the cache is monotonically
  filling, never discarding, so "still filling" is the whole story; there
  is no second cause competing with residency.

## D. Expected shape after the fix

Verbatim from `docs/design/scan-affinity.md` §"Probe-split affinity
(landed 2026-08-22)" — the grep recipe for the next window's analyst:

- `dispatchComputeStage … probe_split=true probe_affine=true` on
  join-6/join-2/join-19 (or whichever local ids that plan assigns).
- `published tasks … placement=affine:3` in place of `binpack:3` on those
  stages.
- Steady-run `peer_hits` deltas near zero on the join-* minutes from run 2
  on (remaining peer traffic = late-materialization gathers and builds,
  not probe files).
- `acq_prefetch_ms ≥ 3 s` firings in steady runs → 0 on probe-split
  stages.
- Q09/Q08/Q17 steady-run spread collapsing to the fast mode (no more
  binpack-lottery bimodality).
- `peer_fetch_ms` present on the `base-table cache stats` line, and one
  `base-table cache: peer fetch` line per transfer with bytes and ms —
  new fields this fix adds, absent from every log in this memo.
- Rows and value signatures identical to this window's runs.
