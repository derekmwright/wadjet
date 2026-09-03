# Benchmarks — index

Every performance measurement this project stands behind has a dated memo in
this directory: the hardware, the scale factor, what was measured, how the
answers were validated, and the mechanism the numbers were attributed to. This
page is the index. Two things on it have no memo behind them and say so
where they appear: the ClickBench *placement* (scored against an external
listing clone that is not committed here, so it is not recomputable from this
tip at all) and the local process measurements at the end of this page. A few
other figures come from a committed results JSON or from arithmetic over a
memo's own table rather than from a memo sentence; each says so in place.

The methodology those memos follow — same-window pairs, mechanism metrics
deciding and walls corroborating, kill-switch-on-treatment controls, rows
gating everything — is
[ADR-0011](../adr/0011-performance-measurement-methodology.md).

## Units

| Term | What it means here |
|---|---|
| **suite wall** | seconds; the sum of the individual query walls in one pass of the suite (22 queries for TPC-H, 43 for ClickBench). Not wall-clock of the whole session. |
| **cold** | run 1 of a session on a freshly deployed cluster: empty page cache, empty NVMe base-table cache, empty decoded-chunk cache. |
| **steady** | mean of runs 2–4 of the same session on the same warm cluster. Where a memo quotes one number, it is the steady mean; the best and worst single steady run are given beside it. |
| **hot** (ClickBench only) | `min(run2, run3)` per query, the official ClickBench definition; cold there is per-query with the page cache dropped before each query. |
| **rows** | 88/88 = the 22 TPC-H results across all 4 runs matching `benchmarks/tpch/baseline-sf100.json` row counts. A zero-row result fails the run. |
| **vsig** | wadjet's own value signature: per-column sums over a query's result rows (`internal/harness/valuesig.go:12-14`), compared **cross-arm** at `ValueSigRelTol = 1e-6` (`valuesig.go:31`). It says two arms of the same engine computed the same numbers; DuckDB is not involved. |
| **fingerprint** | a separate ground-truth file, `benchmarks/tpch/fingerprint-sf100.json`, holding row counts, column names and opaque digests — its own note says *values are deliberately absent*. Checked by an opt-in untimed pass (`cmd/tpch-bench/main.go:92`, `-fingerprint`), not by the timed suite. |

## Current headline numbers

| Suite | Date / version | Hardware | Scale | Cold | Steady | Validation | Memo |
|---|---|---|---|---:|---:|---|---|
| TPC-H, distributed | 2026-09-02 · v0.18.12 | coord `c7g.2xlarge` + 3× `c7gd.4xlarge` (16 vCPU / 32 GB / NVMe), Parquet on S3 us-east-2 | SF100 (600,037,902 lineitem rows, verified in [trino-comparison-2026-08-14.md](trino-comparison-2026-08-14.md) on the same never-regenerated bucket) | 159.68 s | **125.32 s** (best single steady 123.81 s) | 88/88 row counts vs `baseline-sf100.json`; cross-arm `vsig` agreement on 20 of the 21 queries that emit one (Q20 emits none — both its columns are strings; Q01's signature moved because AVG over an integer now declares `DECIMAL(38,4)`) | [sf100-baseline-v0.18.12-2026-09-02.md](sf100-baseline-v0.18.12-2026-09-02.md) |
| ClickBench, single node | 2026-08-22 · v0.17.0-clawback | `c6a.4xlarge` (16 vCPU / 32 GB), 500 GB gp2 — the official listing hardware | 100M-row `hits`, 14.7 GB Parquet queried in place | 161.5 s | 84.6 s (hot) | 43/43 completed, zero nulls; the cell-exact DuckDB comparison is a **separate** gate (`TestHitsCorrectness` against `benchmarks/clickbench/baseline-duckdb-hits1m.json`) over a 1M-row `hits` part — `cmd/clickbench-bench` runs no DuckDB comparison during the 100M run | `benchmarks/clickbench/results-c6a-20260822-v0170.json`, and the same two sums are recorded in [sf100-window3-analysis-2026-08-22.md:467](sf100-window3-analysis-2026-08-22.md) for engine `69aecbb` (per-query table in the [root README](../../README.md#clickbench-single-node-official-spec)) |
| TPC-H vs Trino 470 (FTE) | 2026-08-14 | identical 4-node shape for both engines | SF100 | tie (+2 % on sum, −7 % on geomean) | **198.5 s vs 221.2 s** (−10 %) — means of runs **3–4**, that memo's own steady definition; on this page's r2–4 definition the same runs read 199.83 vs 223.37. Per-query geomean −19 %; 12/22 queries won | 88/88 row counts wadjet vs `baseline-sf100.json`; Trino 87/88 with the known q15 CTE float-tie flip, and the lineitem count (600,037,902) verified on the Trino side (`trino-comparison-2026-08-14.md:20`; no memo records a wadjet-side `COUNT(*)` of it, though `sf100-window-analysis-2026-08-22.md:84` records the same row count for wadjet's own repartition stage). Trino's 87/88 is the memo's own figure; its q15 note (`:79`) records 0/1/0/1 across four runs, which is two failing cells rather than one | [trino-comparison-2026-08-14.md](trino-comparison-2026-08-14.md) |
| TPC-H on exact DECIMAL | 2026-08-29 | one local machine, single process, interleaved arms | SF1 | — | geomean DECIMAL/FLOAT64 **1.1334** over the 20 measured queries | exact against DuckDB, both fixtures | [tpch-decimal-baseline-2026-08-29.md](tpch-decimal-baseline-2026-08-29.md) |

The ClickBench placement quoted in the root README (combined #41, hot #66,
cold #17, 2026-08-22) is computed by `benchmarks/clickbench/rank.py`, which
reproduces the official scoring formula against a local clone of the ClickBench
repository. The field is 136 entries: the 135 published non-fake
`c6a.4xlarge` rows plus our own, which `rank.py` appends before scoring
(`rank.py:23-25`, denominator printed at `:68`). It is our own run scored the
official way, not an upstream listing entry; the suite has not been re-run
since v0.17.0-clawback, and the listing snapshot it was scored against is not
committed in this repository, so the placement is not re-derivable from this
tip alone.

### SF100 suite-wall history, same topology

Same hardware shape (coord `c7g.2xlarge` + 3× `c7gd.4xlarge`, SF100 Parquet on
S3 in us-east-2) across windows. Cross-window numbers are context, never
evidence (ADR-0011 rule 1) — the same-window control is in each memo.

Each row is that window's **default-configuration arm** — the candidate that
shipped — so the series is like-for-like. Other arms in the same windows sometimes ran
faster — window 5's `WADJET_PREFETCH_CACHE_SKIP=0` arm at 165.69 s cold /
136.03 s best steady, and window 3's arm C at a
141.4 s best steady run (`sf100-window3-analysis-2026-08-22.md:81`; note that
arm C was a *replicate* of the candidate, not a switch arm — its
coordinator-side toggle never reached the process, §0 of the same memo).
Window 3's `WADJET_VECTOR_REUSE=0` and `WADJET_PREFETCH_AT_INIT=0` arms had
best steady runs of 144.3 s and 145.7 s (their steady means are 147.7 s and
151.7 s, `:82`). That is why a record
claim has to name its arm.

| Date | Engine (arm) | Cold (r1) | Best single steady run | Memo |
|---|---|---:|---:|---|
| 2026-08-14 | `c3204b7`, single arm (stall family closed) | 268.5 s | 212.1 s | [stall-family-postmortem-2026-08-14.md](stall-family-postmortem-2026-08-14.md) |
| 2026-08-14 | `b88159e`, single arm (Trino window) | 252.9 s | 187.2 s | [trino-comparison-2026-08-14.md](trino-comparison-2026-08-14.md) |
| 2026-08-16 | `4892d76`, single arm | 201.8 s | 158.6 s | [straggler-tier-verdict-2026-08-16.md](straggler-tier-verdict-2026-08-16.md) |
| 2026-08-22 | `69aecbb` arm B (v0.17.0-clawback lineage) | 179.4 s | 152.9 s | [sf100-window3-analysis-2026-08-22.md](sf100-window3-analysis-2026-08-22.md) |
| 2026-08-22 | `6f4905e` arm B | 177.17 s | 141.99 s | [sf100-window4-analysis-2026-08-22.md](sf100-window4-analysis-2026-08-22.md) |
| 2026-08-23 | `550bb20` arm B | 167.81 s | 137.52 s | [sf100-window5-analysis-2026-08-23.md](sf100-window5-analysis-2026-08-23.md) |
| 2026-08-23 | `be5fcf1` arm B | 166.74 s | 134.40 s | [sf100-window6-analysis-2026-08-23.md](sf100-window6-analysis-2026-08-23.md) |
| 2026-09-02 | `8b693f30` arm B (v0.18.12) | 159.68 s | 123.81 s | [sf100-baseline-v0.18.12-2026-09-02.md](sf100-baseline-v0.18.12-2026-09-02.md) |

## Reproducing

### Correctness and small scales (no cloud, no S3)

```bash
# All 22 TPC-H queries at SF0.01 — the CI gate (~5 s)
go test -v -run TestTPCHQueries ./benchmarks/tpch/

# SF1 in one process (data generated into an in-memory store; ~1 min)
TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/

# The DECIMAL(15,2) fixture: correctness, then the measurement
go test -run 'TestTPCHQueriesDecimal|TestTPCHDecimalDeclaredTypes' ./benchmarks/tpch/
TPCH_SCALE=1 go test -v -run TestTPCHDecimalPerformanceBaseline -timeout 60m ./benchmarks/tpch/

# Optimization-invariance oracle: every corpus query with each kill switch
# individually disabled, results asserted identical (~30 s)
go test -run TestTPCHOptimizationInvariance ./benchmarks/tpch/

# ClickBench answers, cell-exact against DuckDB (needs a hits part + /tmp/duckdb)
WADJET_HITS_PART=hits_0.parquet WADJET_CLICKBENCH_DUCKDB=1 \
  go test -run TestHitsCorrectness ./benchmarks/clickbench/

# ClickBench ranking for a results.json, official formula
python3 benchmarks/clickbench/rank.py /path/to/ClickBench \
  benchmarks/clickbench/results-c6a-20260822-v0170.json c6a.4xlarge
```

### Real cluster on one machine (the local harness)

The harness spawns a real coordinator and worker *processes* against a local
file-backed store, so distribution-affecting changes get a gate before any
cloud spend (ADR-0011 rule 5 — local runs are correctness screens, never
performance evidence).

```bash
task harness:smoke     # SF0.01, two queries (~30 s)
task harness:local     # SF0.01 small slice, full gate (~1-2 min)
task harness:large     # large slice, forces spill (~5-10 min)

# Or directly, with a generated scale factor:
go run ./cmd/tpch-harness --mode=local --scale-factor=1 --workers=2 \
  --wadjet-bin=/path/to/wadjet --no-compare
```

Its preflight refuses to start unless the run volume has **20 GB free** and
free RAM covers `GOMEMLIMIT × workers + 2 GB` (`internal/harness/preflight.go`),
and it fails if a stray `wadjet` process is still running.

### Full cloud matrix

```bash
cd deploy/benchmark/terraform && tofu apply -var-file=sf100-distributed.tfvars
cd deploy/benchmark/terraform-clickbench && tofu apply   # official ClickBench spec
```

Instance shapes are pinned in the tfvars (that is what makes windows
comparable), the deploy uses SSM rather than SSH, and the SF100 profile runs
`--data-plane=grpc`.

## Local process measurements (2026-09-03)

Two claims in the root README are about the *process*, not the suite, and are
measured here rather than in a cloud window. Machine: AMD Ryzen 9 5900X
(12 cores / 24 threads), 31 GiB RAM, Linux 5.15 (WSL2) on an ext4 virtual
disk, Go 1.26.1, engine at `a9ec63b8` (v0.18.21), binary built to a scratch
directory. This is a developer box, not the benchmark topology, and the
numbers are process footprints — not a suite performance claim.

**Startup to PostgreSQL-wire-ready.** Five sequential starts of
`wadjet serve --mode=standalone --storage-type=file` (embedded NATS +
JetStream + coordinator + worker), each with its own data, NATS and spill
directories. A probe sends a real PostgreSQL `StartupMessage` to `--pg-addr`
in a poll loop and stops the clock on the server's first response byte:
**median 43 ms**, range 37–126 ms (n=5; the 126 ms outlier is the first start,
reading a cold 62 MB binary). Resident set 3 s after ready: 44.4–47.1 MiB
(median 46.0 MiB).

**Idle footprint by role.** `--mode=coordinator` and a `--mode=worker` process
attached to it over NATS, no tables and no queries, sampled 15 s after start:
coordinator 48.1 MiB RSS, worker 33.4 MiB RSS (`VmHWM` equals `VmRSS` at that
point, so those are also the peaks since exec).

**Peak memory of an SF1 suite in one process.** `TestTPCHQueriesLarge` at
`TPCH_SCALE=1` under `/usr/bin/time -v`, so the peak is that one process's:

One run per arm (n=1), and both arms were replicated independently on the same
box by the arc's reviewer; the replicate is in the second pair of columns.

| Arm | Result | Σ 22 query times | Peak RSS | replicate Σ / peak |
|---|---|---:|---:|---:|
| default (nothing bounding it) | 22/22 pass | 6.99 s | 2 257 MiB | 7.27 s / 2 249 MiB |
| `GOMEMLIMIT=1GiB` | 22/22 pass, exactly 10 heap-pressure spills in both runs | 35.02 s | 1 378 MiB | 23.16 s / 1 352 MiB |

Peak RSS reproduces within 2 %; the query-time cost of the cap does not — it is
3.2× in one run and 5.0× in the other, so treat the slowdown as a range, not a
figure.

The test holds the dataset itself in an in-memory object store — 551.8 MiB of
Parquet in 90 objects, the same fixture size the DECIMAL memo records — so
that much of each peak is data, not engine, and the capped arm pays GC cost a
worker reading from disk or S3 would not. The embedded API's `MemoryBudget` is per QUERY and
defaults to unlimited, so neither arm ran under a budget: the capped arm's
ten spills came from the heap-pressure valve reacting to `GOMEMLIMIT`. Its log
line, quoted whole, carries the engine's own caveat —
`WARN heap-pressure spill triggered (likely tracker accounting gap)
heap_alloc_mb=1009 reclaimable_mb=0 threshold_mb=972 gomemlimit_mb=1024` — so
what the run demonstrates is the relief valve of
[ADR-0006](../adr/0006-never-oom-memory-model.md) firing, with the engine
itself flagging the accounting that led it there as suspect. What the pair shows is that the process degrades to slowdown, not
death, when memory binds, and that the answers do not change. The
unconstrained arm's 2 257 MiB is what the workload used when nothing bounded
it — not a floor.

Reproduce: build to a scratch path (never `go build -o wadjet` at the repo
root — `wadjet/` is a package directory), then

```bash
go test -c -o /tmp/tpch.test ./benchmarks/tpch/
TPCH_SCALE=1 /usr/bin/time -v /tmp/tpch.test -test.run TestTPCHQueriesLarge -test.v
TPCH_SCALE=1 GOMEMLIMIT=1GiB /usr/bin/time -v /tmp/tpch.test -test.run TestTPCHQueriesLarge -test.v
```

## Memo index

Newest first. "SF100 window" means the pinned 4-node shape above.

| Memo | Date | Hardware / scale | What it measured |
|---|---|---|---|
| [sf100-baseline-v0.18.12-2026-09-02.md](sf100-baseline-v0.18.12-2026-09-02.md) | 2026-09-02 | SF100 window, 2 arms × 4 runs | The v0.18.12 baseline: the stage DAG's base-table reads were full-width; fixing that took decoded scan bytes −37.3 %, worker CPU −20.0 %, steady suite −9.7 % same-window. Per-query table: [`…-per-query.txt`](sf100-baseline-v0.18.12-2026-09-02-per-query.txt) |
| [tpch-decimal-baseline-2026-08-29.md](tpch-decimal-baseline-2026-08-29.md) | 2026-08-29 | local, single process, SF1 | First measurement of exact-DECIMAL TPC-H against the FLOAT64 fixture (geomean 1.1334), and what the new fixture found in the gates |
| [sf100-window6-analysis-2026-08-23.md](sf100-window6-analysis-2026-08-23.md) | 2026-08-23 | SF100 window, 3 arms | Scan output backing reuse; why 453 GiB/run less allocation did not move the heap lock. Arm B: cold 166.74 s, steady 135.24 s |
| [sf100-window5-analysis-2026-08-23.md](sf100-window5-analysis-2026-08-23.md) | 2026-08-23 | SF100 window, 6 arms | Row-bound group layout, coordinator peer stage reads, and a corrected cold-win attribution |
| [sf100-window4-analysis-2026-08-22.md](sf100-window4-analysis-2026-08-22.md) | 2026-08-22 | SF100 window, 3 arms | Ownership-aware probe-split placement, with its kill switch as the control arm |
| [clickbench-window-2026-08-22-per-query.txt](clickbench-window-2026-08-22-per-query.txt) | 2026-08-22 | `c6a.4xlarge`, 3 arms | Per-query ClickBench cold/hot with the two-level index switched off in the third arm |
| [probe-split-affinity-2026-08-22.md](probe-split-affinity-2026-08-22.md) | 2026-08-22 | SF100 window (window-3 deploy) | The base-table cache ledger and task census behind the probe-split affinity fix |
| [sf100-window3-analysis-2026-08-22.md](sf100-window3-analysis-2026-08-22.md) | 2026-08-22 | SF100 window, 5 arms | Durable-wait backoff, vector reuse, prefetch-at-init — arm B cold 179.4 s, best steady 152.9 s. Per-query: [`…-per-query.txt`](sf100-window3-2026-08-22-per-query.txt) |
| [sf100-window2-analysis-2026-08-22.md](sf100-window2-analysis-2026-08-22.md) | 2026-08-22 | SF100 window, 4 arms | Decode admission and stage-sink accumulation, isolated by switch on one binary. Per-query: [`…-per-query.txt`](sf100-window2-2026-08-22-per-query.txt) |
| [sf100-window-analysis-2026-08-22.md](sf100-window-analysis-2026-08-22.md) | 2026-08-22 | SF100 window, 3 arms | Two-level group index at the load-factor crossing, plus expression lazy-resolution guards. Per-query: [`…-per-query.txt`](sf100-window-2026-08-22-per-query.txt) |
| [profile-attribution-2026-08-21.md](profile-attribution-2026-08-21.md) | 2026-08-21 | SF100 profiles (3 workers merged) | CPU-profile attribution of the v0.16.0-correctness regression, with a corrected baseline |
| [high-card-aggregation-gap-2026-08-17.md](high-card-aggregation-gap-2026-08-17.md) | 2026-08-17 | `c6a.4xlarge` + local `hits_0.parquet` | ClickBench Q33-class high-cardinality grouping: 3 454 ns/row/core measured against published field references, and the attack order |
| [straggler-tier-verdict-2026-08-16.md](straggler-tier-verdict-2026-08-16.md) | 2026-08-16 | SF100 window, 4 runs | Named the run-level slow mode: the file prefetcher's download wait paid synchronously in `src.Next`. Walls 201.8 / 172.7 / 161.4 / 158.6 s |
| [straggler-mode-attribution-2026-08-16.md](straggler-mode-attribution-2026-08-16.md) | 2026-08-16 | 7 same-config SF100 windows, logs only | Attributed the straggler mode to src-side input acquisition; exonerated the token pool, the reader mode and donation state |
| [shuffle-index-readahead-confirm-2026-08-16.md](shuffle-index-readahead-confirm-2026-08-16.md) | 2026-08-16 | SF100 window, 4 runs | Confirm window for the extent index + scanner readahead: suite record 155.9 s at R4, and a failed pre-registered R2 judgment re-attributed to the straggler mode |
| [shuffle-index-3arm-2026-08-15.md](shuffle-index-3arm-2026-08-15.md) | 2026-08-15/16 | SF100 window, 3 arms | Extent-index A/B: the index costs the early-warm regime; decode-worker ceiling lift 4→8 rejected |
| [shuffle-extent-index-sf100-2026-08-15.md](shuffle-extent-index-sf100-2026-08-15.md) | 2026-08-15 | SF100 window | The extent index deleted the serial stage floor (per-worker stage walk 30–34 s → 0.2–1.4 s) while the wall question stayed open |
| [shuffle-da-token-donation-sf100-2026-08-15.md](shuffle-da-token-donation-sf100-2026-08-15.md) | 2026-08-15 | SF100 window | Decode-ahead token donation: engaged at scale (96.6k donations), two record runs, deep-starvation gap unclosed |
| [shuffle-da-claim-donation-sf100-2026-08-15.md](shuffle-da-claim-donation-sf100-2026-08-15.md) | 2026-08-15 | SF100 window | Claim-path donation kept: first window with no warm q08 slow mode; residual re-attributed to stage-walk cost |
| [shuffle-da-pressure-exemption-2026-08-15.md](shuffle-da-pressure-exemption-2026-08-15.md) | 2026-08-15 | SF100 window | Pressure-stall class eliminated (92 s/worker → 0) at zero cost; the pacer moved to the CPU-token pool |
| [shuffle-decode-ahead-sf100-2026-08-15.md](shuffle-decode-ahead-sf100-2026-08-15.md) | 2026-08-15 | SF100 window, 4 runs | Shuffle decode-ahead as default: Q08 −25 %, Q09 −30 %, the probe-input width plateau half closed |
| [log-clog-seizure-2026-08-15.md](log-clog-seizure-2026-08-15.md) | 2026-08-15 | SF100 worker logs + specimens | The last "GC seizure" residual was a log-pipeline saturation freeze, not GC mark cost |
| [q08-width-plateau-attribution-2026-08-15.md](q08-width-plateau-attribution-2026-08-15.md) | 2026-08-15 | SF100 window, 2 runs | The join-6 width plateau is paced by its single-threaded producer; the token pool and width gate exonerated with counters |
| [q08-sink-attribution-2026-08-15.md](q08-sink-attribution-2026-08-15.md) | 2026-08-15 | 3 same-day SF100 windows | Bounded the whole join-6 exchange-sink cost at ~2 s of Q08 wall — the sink was never the lever |
| [stall-family-postmortem-2026-08-14.md](stall-family-postmortem-2026-08-14.md) | 2026-08-14 | SF100 window, 4 suites | The dispatch-stall family closed: two mechanisms (ReadMemStats STW storms, journald log jam), zero watchdog firings after, walls 268.5 / 264.9 / 212.1 / 214.2 s |
| [trino-comparison-2026-08-14.md](trino-comparison-2026-08-14.md) | 2026-08-14 | identical 4-node shape, both engines, same day | The verdict flip: wadjet steady 198.5 s vs Trino 470 FTE 221.2 s, geomean −19 %, 12/22 won; also records that Trino spill + FTE are incompatible |
| [frozen-spin-root-cause-2026-08-14.md](frozen-spin-root-cause-2026-08-14.md) | 2026-08-14 | 58 stall specimens across 4 SF100 windows | Frozen-spin drag attributed to GC-cycle seizures at the GOGC=100 trigger point |
| [gap-closing-diagnosis-2026-08-14.md](gap-closing-diagnosis-2026-08-14.md) | 2026-08-14 | local analysis of the Trino window | Diagnosis of the three post-flip residuals (Q08 2.07×, Q11 1.63×, Q17 1.45× vs Trino) |
| [shared-subplan-pair-2026-08-14.md](shared-subplan-pair-2026-08-14.md) | 2026-08-14 | SF100 pair, 4 runs each | Shared-subplan dedup: walls unjudgeable (stall storms in both arms), plan-shape and correctness verdicts in, three fixes shipped off the evidence |
| [width-growcap-3arm-2026-08-14.md](width-growcap-3arm-2026-08-14.md) | 2026-08-14 | SF100 window, 3 arms | Morsel width + geometric growth composed: cold 368.7 s → 257.3 s (−30.2 %) |
| [lever-ranking-2026-08-12.md](lever-ranking-2026-08-12.md) | 2026-08-12 | SF100 window with profilers on | The profile-weighted ranking of remaining levers, first clean one after the frozen-spin fixes |
| [rebaseline-2026-08-10.md](rebaseline-2026-08-10.md) | 2026-08-10 | identical shapes, wadjet vs Trino 470 pipelined | Post-populate re-baseline (Trino ahead 1.81× cold at that point) and the Q22 dispatch-stall discovery |
| [cache-on-reprofile-2026-08-09.md](cache-on-reprofile-2026-08-09.md) | 2026-08-09 | SF100 window, cache-on default | Re-derived the steady residual on the shipped base-table-cache default: fault-throughput-bound scan admission |
| [network-bound-diagnosis-2026-08-09.md](network-bound-diagnosis-2026-08-09.md) | 2026-08-09 | SF100 windows, 5 hypotheses | EC2 network-allowance throttling plus a silently disabled base-table cache; fixing the default produced the largest single SF100 improvement on record at the time |
| [steady-slower-than-cold-2026-08-08.md](steady-slower-than-cold-2026-08-08.md) | 2026-08-08 | SF100 pairs, 2 runs each | Diagnosis of steady runs finishing slower than cold ones on byte-identical plans |
| [window-variance-2026-07-27.md](window-variance-2026-07-27.md) | 2026-07-27 | 57 SF100 run logs | Quantified how far one suite wall moves on the same binary across windows — the noise band that makes cross-window deltas context rather than evidence |
| [trino-comparison-2026-07-25.md](trino-comparison-2026-07-25.md) | 2026-07-25 | identical 4-node shape, both engines | The first Trino 470 comparison: Trino ahead 1.96–2.87× (superseded by the 2026-08-14 memo) |

## Related

- [ADR-0011](../adr/0011-performance-measurement-methodology.md) — the measurement rules and why each exists
- [ADR-0013](../adr/0013-correctness-gates-and-their-boundaries.md) — the correctness gates, and the classes of legal nondeterminism that are not defects
- [Performance Tuning](../tuning.md) — memory budgets, spill tuning, environment profiles
- [Performance Bottlenecks](../performance-bottlenecks.md) — known engine bottlenecks and their status
- [docs/design/](../design/) — the per-lever design notes the windows above were run to judge
