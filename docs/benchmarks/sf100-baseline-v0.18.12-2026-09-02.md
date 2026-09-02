# SF100 baseline for v0.18.12 — the DAG's base-table reads stopped being full-width (2026-09-02)

**Window:** 2026-09-02 21:09–22:00 UTC, two sequential EC2 deploys (one per arm, `tofu destroy`
between), coord `c7g.2xlarge` + 3× `c7gd.4xlarge` workers (16 vCPU / 32 GB / NVMe each), SF100
TPC-H Parquet on S3 in us-east-2, `deploy/benchmark/terraform/sf100-distributed.tfvars`,
`benchmark_runs=4` per arm (run 1 cold, runs 2–4 steady on the same warm cluster).
`block_profile_rate=10000` and `mutex_profile_fraction=5` pinned in the same tfvars for both arms.

| arm | engine | run id | wall clock (EDT) |
|---|---|---|---|
| **A control** | `550bb20` | `20260902-210947` | 17:09–17:20 |
| **B candidate** | `8b693f3` (main `8b693f30`, the v0.18.12 candidate) | `20260902-215039` | 17:50–18:00 |

Engine sha read back from each run's own NATS banner (`Git: [550bb20]` / `Git: [8b693f3]`), not
from the deploy script.

> ### ★ The control is *not* the binary behind the README's current numbers — correct this first
>
> The run brief described `550bb20` as "v0.18.0's benchmark binary … the same binary as the
> README's current numbers from window 6". Both halves are wrong, and the error changes what B−A
> means:
>
> - `550bb20a fix(coordinator): bound and sniff the coordinator's peer-tier stage reads` is an
>   ancestor of the `v0.18.0` tag (`2c64488b`, commit `39b3e074`) **by 69 commits**. It is a
>   v0.18.0 release candidate, not v0.18.0.
> - Window 6's arm B — the run the README publishes, `20260823-020311` — is **`be5fcf1`**
>   (banner-verified). The 69 commits between the two include the whole parquet-hardening wave and,
>   materially here, the scan row-group output backing pool
>   (`7890eb51 perf(scan,worker): pool the row-group output backing behind release+claim`,
>   `2858f9b1`, `be5fcf1e`), which window 6 measured at −453 GiB/run of allocation and −4.06 s
>   steady.
>
> So **B − A is "main over a v0.18.0 release candidate", and it contains one lever that v0.18.0
> itself already shipped.** Everywhere that matters below, the cross-window `be5fcf1` column is
> quoted alongside — labelled cross-window, never differenced as an A/B — so the reader can see
> which part of B−A was already in v0.18.0 and which part is new. §5 shows the answer: the win in
> this memo is **not** the backing pool.

**No re-tuning.** A pickaxe over the range found no change to any spill threshold, memory-budget
default, `DefaultBatchSize`, `BroadcastBytesThreshold`, `shuffleBuildThreshold`, partition count,
probe-split, skew-split, prefetch depth or `--local-fastpath-bytes` default. Every hit was a test,
a comment, or gofmt realignment.

## 0. Correctness first

22/22 `OK` on every run of both arms. Row counts are identical in all 88 × 2 cells (§7 of the
per-query file). Value signatures (`vsig`, per-column sums at `%.9e`, compared with
`ValueSigRelTol = 1e-6`) agree cross-arm on **20 of the 21 queries that emit one** — Q20 emits none
because both its output columns are strings, which is correct behaviour, not a gap.

Two signature facts the "zero mismatches" summary did not carry, both recorded here because a gate
that passes narrowly is worth naming:

| query | column | control | candidate | relative | verdict |
|---|---|---|---|---|---|
| Q19 | `c0` | `5.985878903e+08` ×3, `…904e+08` ×1 | `5.985878903e+08` ×3, `…904e+08` ×1 | 1.7e-10 | **within-arm**, both arms, ADR-0013 class 9 |
| Q01 | `c6` (`avg_qty`) | `1.020010031e+02` ×4 | `1.020011000e+02` ×4 | **9.50e-7** | cross-arm, **95 % of the 1e-6 tolerance** |

Q19 is the ADR's own worked example of legal float re-association and it flakes identically on both
arms — not a defect and not an arm difference.

**Q01 `c6` is not nondeterminism at all — it is the numeric arc landing, and it is correct.**
`1.020011000e+02` is the sum of four `avg_qty` values each carrying exactly **four decimals**;
`1.020010031e+02` is the same four values at float64 width. Four decimals is
`batch.AvgScale(0) = 4`, the scale ADR-0012 item 9 gives AVG over an **integer** input. Three
measurements close the chain:

1. **The harness is exonerated.** `internal/harness/valuesig.go` has *no diff* between `550bb20`
   and `8b693f30`, and `cmd/tpch-bench/` has no diff at all — the two arms computed the signature
   with identical code. The only change under `internal/harness/` in the range is `micros.go`.
2. **`l_quantity` is `INT64` in the SF100 data.** The pre-seeded bucket's Parquet
   (`s3://wadjet-bench-sf100-use2/lineitem/0_0.parquet`, `created_by: Polars`, written
   **2026-03-24** and never regenerated — the harness logs `Using pre-seeded data … set
   GENERATE_DATA=1 to regenerate`) declares `l_quantity` physical `INT64`. The repo's own
   generator declares it `TypeFloat64` (`benchmarks/tpch/schema.go:95`). **My earlier note that
   this fixture is FLOAT64 was wrong; it describes the generator, not the bytes SF100 reads.**
   §6.1 is the rest of that divergence.
3. **AVG over an integer reaches the signature at scale 4.** Driving the type-matrix fixture
   through the identical `GetNumericFloat64` + `ValueSigAccum` path `cmd/tpch-bench` uses on the
   distributed arm, at `8b693f30`: `avg(c_f64)` → declared `FLOAT64`, reads
   `833.16253330600534`; `avg(c_i64)` → declared **`DECIMAL precision=38 scale=4`**, reads
   `2499158148.4129`; `avg(c_i32)` → same. The signature carries whatever scale the column
   declares.

The commit is **`3f35e71b fix(exec): SUM and AVG over integers answer PostgreSQL's types`**
(2026-09-02, `aggregate.go` / `kernel/agg.go` / `plan.go` / `avg_decompose.go`) — it landed the day
of the run. `aggIntegerOutputType` returns DECIMAL for `avg` over `TypeInt32`/`TypeInt64` and
`ok=false` for every other input, so nothing about FLOAT64 columns moved: `c3`/`c4`/`c5`
(`l_extendedprice`-derived, genuinely DOUBLE) are identical between arms, and `c2` (`sum_qty`) and
`c9` (`count_order`) are identical because SUM(int64) → DECIMAL **scale 0** and COUNT are the same
integers either way. Only the one column whose scale narrowed moved. **Both arms are exact to the
digits they keep; the candidate keeps four** — ADR-0012 item 9's declared class, deliberately a
fixed `+4` rather than PostgreSQL's magnitude-dependent scale.

So this is not a defect. It is, however, **a gate gap worth stating**: the fingerprint's
`ValueSigRelTol = 1e-6` compares magnitude only, so a *declared-scale narrowing* and a *value
corruption* of the same size are indistinguishable to it, and this one passed at 95 % of budget by
luck of the distribution. See §6.1.

## 1. Totals — the four metric families

### 1.1 Wall

| | A control (`550bb20`) | B candidate (`8b693f30`) | B−A | *cross-window* `be5fcf1` |
|---|---|---|---|---|
| r1 / r2 / r3 / r4 | 171.77 / 139.31 / 136.92 / 140.18 | 159.68 / 123.81 / 126.24 / 125.89 | | 166.74 / 136.06 / 135.26 / 134.40 |
| **cold (r1)** | 171.77 | **159.68** | **−12.09 (−7.0 %)** | *166.74* |
| **steady mean (r2–4)** | 138.80 | **125.32** | **−13.49 (−9.7 %)** | *135.24* |
| **best single steady run** | 136.92 | **123.81** | −13.11 | *134.40* |
| worst single steady run (tail) | 140.18 | 126.24 | −13.94 | *136.06* |
| steady spread (max−min) | 2.39 | 2.43 | +0.04 | *1.66* |
| suite total (4 runs) | 588.18 | **535.63** | **−52.56 (−8.9 %)** | *572.47* |

B is faster on the **mean and the tail alike** (−13.5 / −13.9 s) with the spread unchanged, so this
is a level shift, not a tail repair.

### 1.2 CPU

From the per-worker Go CPU profiles saved at end of run (`worker-worker-*-cpu.prof`); each is one
profile spanning the whole four-run suite, so the split is per suite, not per query.

| | A control | B candidate | B−A |
|---|---|---|---|
| profile Duration (s) | 592.90 | 539.78 | −53.12 |
| worker CPU-s, whole suite (Σ 3 w) | 12 575.7 | **10 058.6** | −2 517.1 |
| **worker CPU-s per suite run** | 3 143.9 | **2 514.6** | **−629.3 (−20.0 %)** |
| ↳ utilisation of 48 vCPU over the profile window | 44.2 % | 38.8 % | −5.4 pp |

Independently, from the workers' own `task completed` lines (Σ 3 workers × 4 runs, task **counts
identical** except where noted):

| stage_type | n (A/B) | Σ duration A (s) | Σ duration B (s) | B−A |
|---|---|---|---|---|
| **`broadcast_join`** | 168 / 168 | 469.0 | **242.4** | **−48.3 %** |
| `hash_join` | 2 592 / 2 592 | 1 685.6 | 1 670.4 | −0.9 % |
| `aggregate` | 292 / 276 | 579.3 | 574.4 | −0.9 % |
| **`final_aggregate`** | 988 / 988 | 388.8 | **424.6** | **+9.2 %** |
| `sort` | 28 / 28 | 0.6 | 0.5 | −9.2 % |
| (scan / exchange, untyped) | 1 872 / 1 828 | 1 382.4 | 1 414.3 | +2.3 % |
| **all** | 5 940 / 5 880 | 4 505.7 | 4 326.6 | −4.0 % |

CPU-s per run falls 20.0 % while wall falls 9.7 %, so **utilisation drops**: work was removed, not
wait. That is the opposite of windows 5–6, where utilisation rose because wait was removed.

### 1.3 Network and requests (suite = 4 runs, Σ 3 workers; binary units)

From the workers' final cumulative `shuffle io stats` / `streaming shuffle read stats`.

| | A control | B candidate | B−A |
|---|---|---|---|
| shuffle **peer** files / bytes | 44 169 / 92.16 GiB | 41 867 / 89.65 GiB | −5.2 % / −2.7 % |
| shuffle **s3** files / bytes · `peer_fallthroughs` | 0 / 0 · 0 | 0 / 0 · 0 | — |
| shuffle **local** files / bytes | 22 768 / 121.81 GiB | 21 580 / 123.93 GiB | −5.2 % / +1.7 % |
| S3 `upload_done` PUTs / bytes | 29 443 / 97.13 GiB | 29 520 / 92.04 GiB | +0.3 % / −5.2 % |
| `upload_cancelled` / bytes | 33 817 / 145.34 GiB | 30 700 / 148.95 GiB | −9.2 % / +2.5 % |
| `upload_failed` / `upload_elided` | 0 / 0 | 0 / 0 | — |
| `wshz` (zstd envelope) files / bytes | 10 656 / 24.65 GiB | 10 424 / 23.98 GiB | −2.2 % / −2.7 % |
| streaming reads / `file_pread` files / bytes | 451 / 66 486 / 307.85 GiB | 410 / 63 037 / 308.11 GiB | −9.1 % / −5.2 % / **+0.1 %** |
| base-table **peer** fetches / bytes | 43 / 4.97 GiB | 39 / 3.71 GiB | −9.3 % / −25.4 % |
| coordinator task-result replies / payload, per steady run | 1 484 / 73.12 GiB | 1 469 / 74.48 GiB | −1.0 % / +1.9 % |

**Every wire row moves ≤ 5.2 %, and the largest single stream (`file_pread_bytes`) moves +0.1 %.**
The stage DAG is byte-for-byte the same plan: for each of the seven biggest movers the coordinator
emits an identical stage list, identical task counts and identical per-stage payload bytes (§3).
The win is not on the wire.

**Not derivable this window, and what would capture it:** ENA / NIC counters and NVMe read+write
totals (window 6 quoted both) were not sampled — the harness logs neither `ethtool -S` nor
`/proc/diskstats`. Adding a sample at each run boundary would restore them. Per-run splits of every
counter in this table are also unavailable: these are cumulative counters read at end of suite, and
the intermediate `shuffle io stats` samples are not aligned to run boundaries.

### 1.4 Allocation, heap and GC

From the per-worker heap profiles (`alloc_space` / `alloc_objects`, cumulative since process start)
and the workers' `worker stats` `gc_delta` / `gc_pause_delta_ms` samples.

| | A control | B candidate | B−A |
|---|---|---|---|
| **alloc per suite run** (GiB, Σ 3 w) | 1 804.4 | **1 135.9** | **−668.5 (−37.1 %)** |
| **allocation EVENTS per suite run** (M) | 6 271.1 | 6 194.3 | **−76.8 (−1.2 %)** |
| ↳ mean allocation size | 309 B | **197 B** | −36 % |
| GC cycles (Σ 3 w, suite) | 1 435 | **1 006** | −429 (−29.9 %) |
| GC pause total / mean per cycle (ms) | 5 642 / 3.93 | 7 521 / **7.48** | +1 879 / +3.55 |
| total mutex delay, s/suite | 653.6 | 625.4 | −28.2 (−4.3 %) |
| ↳ per worker-run | 54.5 | 52.1 | −2.4 |
| total block delay, hrs/suite | 69.71 | 67.92 | −1.79 (−2.6 %) |
| peak `alloc_mb` / `rss_mb` (max worker) | 7 130 / 11 741 | 6 859 / 11 251 | −271 / −490 |
| heap-pressure profiles written (Σ 3 w) | 34 | 34 | 0 |

**A third of the bytes and none of the allocations** — the same signature window 6 found for the
backing pool, and §2 shows it has the same kind of cause here (large column buffers) but a
different mechanism. Fewer, longer GC pauses is the expected consequence of a smaller allocation
rate against a live set that did not shrink; total pause time rises 1.9 s over a suite whose wall
fell 52.6 s.

### 1.5 Per-query steady mean (r2–4), seconds

Min/max are over the three steady runs. The `be5fcf1` column is **cross-window** (2026-08-23,
different deploy) and is printed for orientation only — it is never differenced as an A/B.

| Q | rows | A ctl | A min–max | B cand | B min–max | B−A | % | *xw `be5fcf1`* |
|---|---:|---:|---|---:|---|---:|---:|---:|
| Q01 | 4 | 3.776 | 3.567–4.130 | 3.473 | 3.444–3.518 | −0.302 | −8.0 | *3.476* |
| Q02 | 100 | 4.048 | 3.878–4.369 | 3.657 | 3.310–3.946 | −0.391 | −9.7 | *4.160* |
| Q03 | 10 | 9.135 | 8.977–9.302 | 9.206 | 8.849–9.403 | +0.071 | +0.8 | *9.222* |
| Q04 | 5 | 5.281 | 5.167–5.404 | 5.373 | 5.282–5.543 | +0.092 | +1.7 | *5.056* |
| Q05 | 5 | 5.782 | 5.676–5.957 | 6.042 | 5.911–6.155 | +0.259 | +4.5 | *5.966* |
| Q06 | 1 | 0.809 | 0.676–0.948 | 0.895 | 0.851–0.965 | +0.086 | +10.6 | *0.767* |
| Q07 | 4 | 4.423 | 4.085–4.729 | 4.800 | 4.414–5.394 | +0.377 | +8.5 | *4.210* |
| **Q08** | 2 | 15.395 | 15.057–15.872 | **10.391** | 10.161–10.680 | **−5.004** | **−32.5** | *15.018* |
| **Q09** | 175 | 15.162 | 14.979–15.495 | **11.254** | 11.073–11.543 | **−3.907** | **−25.8** | *14.164* |
| Q10 | 20 | 10.898 | 10.561–11.124 | 11.489 | 11.150–11.939 | +0.591 | +5.4 | *11.113* |
| Q11 | 92 698 | 2.737 | 2.625–2.891 | 2.959 | 2.923–3.028 | +0.222 | +8.1 | *2.605* |
| **Q12** | 2 | 4.780 | 4.777–4.785 | **3.487** | 3.427–3.599 | **−1.293** | **−27.1** | *4.641* |
| Q13 | 100 | 5.548 | 5.427–5.658 | 5.229 | 5.195–5.263 | −0.319 | −5.7 | *5.294* |
| Q14 | 1 | 1.893 | 1.800–2.027 | 1.712 | 1.504–1.946 | −0.181 | −9.5 | *1.808* |
| Q15 | 1 | 1.710 | 1.606–1.814 | 1.801 | 1.461–2.427 | +0.091 | +5.3 | *1.763* |
| **Q16** | 27 840 | 5.703 | 5.556–5.874 | **3.453** | 3.377–3.593 | **−2.250** | **−39.5** | *5.126* |
| **Q17** | 1 | 5.912 | 5.559–6.127 | **1.593** | 1.208–2.265 | **−4.319** | **−73.1** | *5.630* |
| **Q18** | 100 | 10.580 | 10.406–10.689 | **12.018** | 11.444–12.376 | **+1.438** | **+13.6** | *10.515* |
| Q19 | 1 | 3.727 | 3.713–3.749 | 3.696 | 3.546–3.782 | −0.031 | −0.8 | *3.770* |
| Q20 | 17 971 | 9.660 | 9.359–9.960 | 9.244 | 9.016–9.402 | −0.416 | −4.3 | *9.165* |
| **Q21** | 100 | 9.107 | 8.932–9.205 | **11.038** | 10.545–11.644 | **+1.930** | **+21.2** | *8.977* |
| Q22 | 7 | 2.737 | 2.680–2.838 | 2.506 | 2.277–2.699 | −0.231 | −8.4 | *2.796* |
| **Σ** | | **138.804** | | **125.315** | | **−13.488** | **−9.7** | *135.241* |

### 1.6 Cold run (r1), seconds

| Q | A | B | B−A | | Q | A | B | B−A |
|---|---:|---:|---:|---|---|---:|---:|---:|
| Q01 | 25.159 | 24.781 | −0.378 | | Q12 | 4.774 | 3.489 | −1.285 |
| Q02 | 11.582 | 10.929 | −0.653 | | Q13 | 5.391 | 5.056 | −0.335 |
| Q03 | 15.191 | 15.020 | −0.171 | | Q14 | 1.681 | 1.834 | +0.153 |
| Q04 | 5.373 | 5.696 | +0.323 | | Q15 | 1.879 | 1.384 | −0.495 |
| Q05 | 5.769 | 5.567 | −0.202 | | Q16 | 5.368 | 3.172 | −2.196 |
| Q06 | 0.645 | 0.729 | +0.084 | | Q17 | 5.452 | 1.291 | −4.161 |
| Q07 | 4.036 | 4.459 | +0.423 | | Q18 | 11.060 | 11.827 | +0.767 |
| Q08 | 15.970 | 10.609 | −5.361 | | **Q19** | 3.452 | **6.895** | **+3.443** |
| Q09 | 14.610 | 11.342 | −3.268 | | Q20 | 9.508 | 9.371 | −0.137 |
| Q10 | 10.715 | 10.554 | −0.161 | | Q21 | 9.229 | 10.768 | +1.539 |
| Q11 | 2.391 | 2.748 | +0.357 | | Q22 | 2.538 | 2.158 | −0.380 |
| | | | | | **Σ** | **171.774** | **159.680** | **−12.094** |

**Q19's cold +3.443 s is a single cold fragment, not a steady-state property** (steady runs agree to
0.8 %). The candidate's Q19 cold spent 5.597 s between DAG dispatch and the `join-2` dispatch
against the control's 2.649 s, and exactly one `fragment task phases` line crosses the reporting
threshold in that interval — `stage_id=scan-0 elapsed_ms=5286 acq_prefetch_miss=1
prefetch_lead_ms=0` — with no counterpart in the control's window. A first-touch prefetch that got
no lead time. One task in one cold run; no further attribution is available and none is claimed.

## 2. ★ The mechanism: the DAG's base-table reads were full-width, and now are not

### 2.1 What the scan counters say

`scan decode-ahead query stats`, Σ 3 workers × 4 runs:

| counter | A control | B candidate | B−A |
|---|---:|---:|---:|
| **`decode_bytes`** | **565.24 GiB** | **354.39 GiB** | **−37.3 %** |
| **`decode_ms`** | 2 967 040 | 1 543 398 | **−48.0 %** |
| row `groups` decoded | 208 058 | 180 970 | −13.0 % |
| `pressure_stalls` | 12 050 | **174** | −98.6 % |
| `window_fulls` | 55 991 | 35 712 | −36.2 % |
| `token_stalls` / `token_stall_ms` | 193 880 / 1 551 454 | 230 328 / 1 562 686 | +18.8 % / +0.7 % |

Read the first three rows together. Row groups decoded fall 13 % but decoded **bytes** fall 37 % —
so the saving is **bytes per row group**, i.e. fewer columns, not fewer groups. That is a
projection narrowing, and nothing else produces that ratio.

Three independent measurements agree to within a percentage point, which is what makes this an
attribution rather than a correlation:

| | B−A |
|---|---|
| `decode_bytes` (scan counters) | **−37.3 %** |
| heap `alloc_space` per suite run (pprof) | **−37.1 %** |
| `decode_ms` (scan counters) | **−48.0 %** |
| `broadcast_join` Σ task duration (worker `task completed`) | **−48.3 %** |

And the allocation is bytes-only (objects −1.2 %), which is what a narrower set of decoded column
buffers looks like and is not what a change in small-object churn looks like.

Per stage, `decode_bytes` in GiB over the suite:

| stage | A | B | B−A |
|---|---:|---:|---:|
| `join-2` (broadcast-join probe; Q12/Q16/Q17/Q19/…) | 95.47 | **17.77** | **−81.4 %** |
| `join-6` (broadcast-join probe; Q08/Q09) | 133.59 | **63.55** | **−52.4 %** |
| `scan-0` | 147.08 | 131.06 | −10.9 % |
| `scan-1` | 64.20 | 52.29 | −18.6 % |
| `join-3` (Q20) | 10.78 | 0.80 | −92.6 % |
| `join-19` (Q02) | 11.36 | 2.10 | −81.5 % |
| `scan-4` / `scan-7` | 20.80 / 18.25 | 15.60 / 14.49 | −25.0 % / −20.6 % |

The `join-*` rows — stages that read a base table directly as a broadcast join's probe side — lose
**52–93 %** of their decoded bytes. The plain `scan-*` rows lose 11–25 %. The stages that read a
base table *from inside a compute stage* are the ones that were paying full width.

### 2.2 The commits

- **`75acddbc fix(coordinator): send the scan projection to a DAG stage's base-table reads`**
  (2026-08-23, `internal/coordinator/execute_stage_dag.go`). Every `OpShuffleSource` was emitted
  with no `Columns` field at all. The worker-side plumbing existed end to end
  (`OpSpec.Columns` → `sourceForAliasWithProjection` → `cachedFileStreamSource.projectColumns` →
  `finishParquetState`) and simply always received an empty list, so a compute stage whose
  dependency is a pass-through leaf scan **read base-table Parquet at full width on the DAG** while
  the single-process path projected.
- **`c15e3a01 perf(worker): narrow parquet projection to the intersection instead of reverting to
  full width`** (2026-08-23, `internal/worker/stream_source.go`). `finishParquetState` reverted to
  the full file schema the moment any one requested name was absent — and a synthetic name
  (`RowCountOnlyColumn`, a materialized ORDER BY name, a HAVING marker, a pre-projected scalar
  alias) is almost always absent. It now narrows to the intersection.

Both land on the `lineitem`-reading probe stages, which is exactly the set that won. Both are dated
2026-08-23, the same day as the `v0.18.0` tag, and **both are inside `v0.18.0..HEAD`** — they are
not in the control and not in window 6's `be5fcf1` either (§5).

### 2.3 The second-order effects, and one that is not there

`pressure_stalls` falling 12 050 → 174 (−98.6 %) is downstream of §2.1: decoding a third fewer
bytes keeps the decode-ahead ring far from the memory-pressure sensor. The decoded-row-group cache
follows the same way — `misses` 752 081 → 446 895 (−40.6 %), `admitted` −36.5 %, `evictions`
−51.4 %, hit rate **36.7 % → 47.0 %** — because narrower entries mean more of them fit under the
same 6 143 MB cap (`size_mb` and `cap_mb` are identical in both arms).

**One hypothesis tested and rejected.** `4879558a fix(exec): an ungrouped aggregate never buffers
its input rows` is exactly Q17's shape and was the strongest a-priori candidate for Q17's 3.7×. It
is not the cause: Q17's *aggregate* stages are 0.070 s (`aggregate-6`) and 0.078 s
(`final_aggregate-7`) in the control, and 0.056 / 0.015 s in the candidate. There are only 78 ms
there to win. Q17's entire delta is its `join-2` probe (§3).

## 3. Per-stage localisation of every named mover

Coordinator `dispatching compute stage` → `compute stage complete` wall, steady mean of runs 2–4.
The stage graph, task counts and per-stage payload bytes are identical between arms for all seven.

| Q | stage | type | A | B | B−A | query B−A | share |
|---|---|---|---:|---:|---:|---:|---:|
| Q08 | `join-6` | `broadcast_join` | 13.551 | 8.157 | **−5.394** | −5.004 | 108 % |
| Q17 | `join-2` | `broadcast_join` | 5.543 | 1.001 | **−4.542** | −4.319 | 105 % |
| Q09 | `join-6` | `broadcast_join` | 6.889 | 3.412 | **−3.478** | −3.907 | 89 % |
| Q20 | `join-3` | `broadcast_join` | 5.783 | 2.186 | **−3.596** | −0.416 | (offset elsewhere) |
| Q16 | `join-2` | `broadcast_join` | 3.762 | 1.313 | **−2.449** | −2.250 | 109 % |
| Q12 | `join-2` | `broadcast_join` | 2.679 | 1.211 | **−1.468** | −1.293 | 114 % |
| Q02 | `join-19` | `broadcast_join` | 3.476 | 2.150 | −1.326 | −0.391 | (offset elsewhere) |

Compute-stage wall summed by `stage_type` over all 22 queries (steady mean r2–4):

| stage_type | n | A (s) | B (s) | B−A |
|---|---:|---:|---:|---:|
| **`broadcast_join`** | 14 | 46.682 | **25.533** | **−21.150 (−45.3 %)** |
| `sort` | 7 | 0.175 | 0.169 | −0.006 (−3.4 %) |
| `aggregate` | 8 | 0.771 | 0.855 | +0.084 (+10.9 %) |
| **`final_aggregate`** | 17 | 12.072 | **13.010** | **+0.938 (+7.8 %)** |
| **`hash_join`** | 27 | 41.593 | **43.504** | **+1.910 (+4.6 %)** |
| **total** | 73 | 101.293 | 83.070 | **−18.223 (−18.0 %)** |

The coordinator's stage wall (−45.3 % on `broadcast_join`) and the workers' own task durations
(−48.3 %, §1.2) are two independent instruments on the same operator, and they agree.

The win is **size-graded**, which is what a per-byte cost removal looks like: every
`broadcast_join` whose control wall was ≥ 2.679 s got 38–82 % faster (7 of 7), while the three
between 1.0 and 2.2 s went +0.6 % to +29.6 %. The small ones pay a fixed per-stage cost they do not
amortise; see §4.

Phase split over all 22 queries (DAG dispatch → first compute-stage dispatch, vs the rest):

| | A | B | B−A |
|---|---:|---:|---:|
| scan + exchange phase, Σ 22 queries | 27.247 | 28.543 | +1.296 |
| compute phase, Σ 22 queries | 106.824 | 92.275 | **−14.550** |

## 4. The two regressions

### 4.1 Q21 (+1.930 s, +21.2 %) — attributed to bytes, mechanism named but not proven

Cumulative offsets from the query's first coordinator event, steady mean r2–4:

| event | A | B | interval B−A |
|---|---:|---:|---:|
| `dispatching compute stage join-4` | 1.568 | 1.708 | +0.140 |
| `compute stage complete join-4` | 3.238 | 3.464 | +0.086 |
| `shuffle side complete stage-exchange-repartition-11` | 7.778 | 9.191 | **+1.225** |
| `compute stage complete join-12` | 9.056 | 10.544 | +0.076 |
| `compute stage complete final_aggregate-18` | 9.091 | 11.015 | **+0.435** |
| `gather: fused wait returned` | 9.102 | 11.024 | +0.003 |

Per-stage payload bytes, steady mean — and **exactly one stage moves**:

| stage | A | B | B−A |
|---|---:|---:|---:|
| **`scan-0`** | 1 119.7 MiB | **1 269.2 MiB** | **+13.4 %** |
| `scan-1` / `scan-5` / `scan-7` | 566.4 / 1.5 / 0.0 MiB | identical | ±0.0 % |
| `join-4` / `join-12` / `final_aggregate-18` | 381.3 / 9.4 / 0.1 MiB | identical | ±0.0 % |
| `shuffle-stage-exchange-repartition-11` | 1 551.5 MiB | 1 551.5 MiB | −0.0 % |

`scan-0` is the `lineitem` scan feeding `exchange-repartition-11`, with the semi-anti build filter
`sabf-join-12-0` (key `l_orderkey`, bloom) applied — both arms log
`semi_anti_build_filter: marked join=join-12 join_type=semi build_scan=scan-9 … bloom`. **The
candidate's bloom lets 13.4 % more bytes through, and the +1.225 s sits precisely in the interval
where those extra bytes are produced and repartitioned.** Bytes and wall agree on where and how
much.

The commits that changed this bloom, in range: **`d338f6b5 fix(exec): give the reverse bloom one
key encoding, and make a broken one loud`** (2026-08-25) — whose body names Q21's third bloom, the
one on the inner join to orders, as installing an *empty* bloom on its parent commit — plus
`69c2ee12 fix(exec): run the bloom self-check on the first batch, not on a rejection rate` and
`7249d087 fix(exec): ask the storage predicate, not the encoding one, for the bloom fast path`.

**Stated as a limit:** this is a named candidate, not a proven cause. The control answers Q21 with
100 rows and a signature identical to the candidate's, so the control was *not* running
`d338f6b5`'s empty bloom — whatever changed, it changed between two arms that both answer
correctly. Settling it needs a one-lever A/B on the reverse bloom, not this window's data.
`final_aggregate-18`'s +0.435 s is the same cross-query `final_aggregate` tax as §4.2 and is not
Q21-specific.

### 4.2 Q18 (+1.438 s, +13.6 %) — unattributed; zero bytes moved

Interval deltas, steady mean r2–4. The coordinator event sequence is identical across all six
traces (17 events), so the intervals are directly comparable and account for the whole query delta:

| interval | A | B | B−A |
|---|---:|---:|---:|
| three `shuffle side complete` intervals | 0.275 / 1.118 / 2.685 | 0.864 / 1.721 / 2.026 | +0.589 / +0.603 / **−0.659** |
| `final_aggregate-7` (24 tasks, the high-cardinality group-by) | 4.198 | 4.320 | +0.122 |
| `join-10` / `join-13` / `join-17` | 0.896 / 0.773 / 0.552 | 1.049 / 0.897 / 0.703 | +0.154 / +0.124 / +0.151 |
| **`final_aggregate-19`** | 0.068 | **0.388** | **+0.320** |
| `sort-20` + gather | 0.009 | 0.034 | +0.025 |
| **total** | 10.576 | 12.013 | **+1.437** |

And the payload bytes on the three large shuffles are **identical to −0.0 %** (2 325.8 / 432.8 /
4 077.7 MiB in both arms); only the sub-MiB `final_aggregate` outputs move at all. So Q18's
regression **moves no bytes and is spread across every stage** — a broad per-row/per-group compute
tax of +12 to +28 % on each of three hash joins, not one localised stage.

**This is unattributed** — but §0 changed which candidate leads, and the leading one is now
mechanically grounded rather than speculative.

**Lead: `3f35e71b fix(exec): SUM and AVG over integers answer PostgreSQL's types`.** §0 establishes
that `l_quantity` is **`INT64`** in the SF100 data, not the FLOAT64 the repo's generator declares.
Q18's hot aggregate is `sum(l_quantity)` grouped by `l_orderkey` over the full `lineitem` — the
`HashAggregate/group_by=l_orderkey` whose operator peak the workers report at 6 MB × 24 tasks — and
after `3f35e71b` that SUM runs on the **exact Int128 carrier** instead of a float64 accumulator,
because `exec.aggIntExact` is now true for `TypeInt64`. CLAUDE.md's own note on the arc records the
batch int64 sum kernel as **~85 % slower in micro-benchmarks**. That is an unconditional per-row
cost on precisely the stage that regressed, and it is the hypothesis the run brief raised and I
wrongly dismissed on a FLOAT64 reading of the fixture. It also predicts the cross-query
`final_aggregate` tax of §4.3, which should concentrate on integer-summing aggregates.

Three others in range add unconditional per-row or per-group cost on the same path, and this
window's data does not discriminate among any of the four:

- `beb40c50 perf(exec): break fibHash's stride collapse, and fix the back-shift it exposed` — four
  extra xorshifts on every int-keyed hash probe, plus both `Delete` back-shift loops now run Knuth
  Algorithm R to an empty slot; `HashAggregate`'s partial-drain path deletes each drained group key.
- `b73ac3e3 test(exec): make a spill gate able to reach, and to prove it reached, the path it
  tests` — one relaxed atomic load (`forceAggDrainEvery.Load()`) per `Consume`, unconditionally,
  even with the knob disarmed.
- `f88b0f73 fix(exec): give the float predicates and the primary hash keys PostgreSQL's NaN order`
  — `math.Float64bits` → `keyFloat64bits` at five per-row sites including `serializeGroupKey`; Q18
  also groups on `o_totalprice`, a genuine DOUBLE, so it pays this too.

**What would attribute it:** run the four as single-lever A/Bs, `3f35e71b` first. Note that its
cost is **only visible on data whose quantity column is an integer**, so no local gate built from
`benchmarks/tpch/schema.go` can reproduce it — §6.1.

### 4.3 The `final_aggregate` tax

`final_aggregate` is +9.2 % in worker task-seconds (388.8 → 424.6 over 988 identical tasks) and
+7.8 % in coordinator stage wall over 17 stages. It is visible wherever the stage is small enough
for a fixed cost to show: Q21 `final_aggregate-18` 0.035 → 0.469 s, Q18 `final_aggregate-19` 0.068
→ 0.388 s, Q03 `final_aggregate-10` 0.221 → 0.498 s, Q02 `final_aggregate-21` 0.093 → 0.283 s.
Same candidates as §4.2; same missing A/B. It is the counterweight that turns §3's −21.2 s of
`broadcast_join` into −13.5 s of suite wall.

## 5. Q08 and Q09: a new level, not a lottery draw

Q08 and Q09 are on record as bimodal. The mode counts for this window:

| | Q08 fast / total | Q09 fast / total |
|---|---|---|
| A control (`550bb20`) | **0 / 4** | **0 / 4** |
| B candidate (`8b693f30`) | **4 / 4** | **4 / 4** |

But the candidate's level is not the historical fast mode. Across every window recorded in
`docs/benchmarks` on this topology, no Q08 value — steady mean or single run — falls below 14.7 s:
window 3 arm B min 14.7 (single run), window 4 steady means 15.58–17.80 with a single-run min of
15.52, window 5 steady means 15.44–17.87, window 6 arm B min 14.724, this window's control min
15.057. The candidate reads **10.161–10.680 s in four runs out of four**. Q09 is the same shape
against a 14.0 s floor, candidate 11.073–11.543.

What the journals say differs: **nothing in dispatch, and everything in decode.** Q08's `join-6`
carries identical flags in both arms (`stage_type=broadcast_join num_tasks=3 probe_split=true
probe_affine=true late_mat=true inputs_aliases=4`, `placement=affine:3`, `primary_cache_files=5
fused_count=2 cluster_cache_reads_estimate=33`), an identical 16-stage DAG, identical 84
task-results and identical 1 528.5 MiB of payload. The `join-6` stage's suite `decode_bytes` falls
**133.59 → 63.55 GiB** and its wall falls 13.551 → 8.157 s. Q09 is the same story at 81 tasks and
10 149 MiB.

This also settles which arc owns the win. Window 6's `be5fcf1` — the binary the README currently
publishes, which **has** the scan backing-reuse pool — read Q08 15.018 s and Q17 5.630 s steady,
statistically the same as this window's control on both. The backing pool did not move these
queries; the projection fix did. *(Cross-window comparison, different deploy: quoted as
orientation. It is load-bearing only because the effect is 4.6 s on a 5.6 s stage, far outside the
≤ 3 % cross-deploy reproduction window 6 established for its own four families.)*

## 6. Fixture and deploy facts, named so they are not read as arm effects

### 6.1 ★ The SF100 data is not the schema the repo declares

The pre-seeded SF100 bucket was written by **Polars on 2026-03-24** and is never regenerated
(`GENERATE_DATA=0` is the default and the harness logs it). Its `lineitem` schema and the repo's
`benchmarks/tpch/schema.go` `LineItemSchema` disagree on **nine of sixteen columns**:

| column | SF100 Parquet (what the benchmark reads) | `LineItemSchema` (what every local gate builds) |
|---|---|---|
| `l_orderkey`, `l_partkey`, `l_suppkey`, `l_linenumber` | `INT64` | `TypeInt32` |
| **`l_quantity`** | **`INT64`** | **`TypeFloat64`** |
| `l_shipdate`, `l_commitdate`, `l_receiptdate` | `INT32` / logical `Date` | `TypeString` |
| `l_extendedprice`, `l_discount`, `l_tax` | `DOUBLE` | `TypeFloat64` (agree) |

This is not a defect in either place, but it has three consequences that this window made visible
and that nothing in CI would have:

1. **SF100 exercises integer SUM/AVG on `l_quantity`; no local gate does.** The exact-Int128
   carrier and `batch.AvgScale(0)` reach TPC-H at SF100 and nowhere else in the corpus — which is
   why §0's signature change and §4.2's leading regression candidate both surfaced in a benchmark
   memo rather than in the numeric arc's own gates.
2. **The DECIMAL fixture does not cover it either.** `TPCH_DECIMAL=1` makes the eight monetary
   columns exact; `l_quantity` stays FLOAT64 in *both* repo fixtures by design
   (`benchmarks/tpch/schema_decimal.go:21`). So neither the FLOAT64 nor the DECIMAL arm produces
   the integer-quantity shape SF100 runs.
3. **The value-fingerprint gate cannot see the difference between a narrowed scale and a wrong
   number.** `CompareValueSigs` compares per-column sums with a relative tolerance
   (`ValueSigRelTol = 1e-6`); a declared-scale narrowing from float64 width to four decimals is a
   magnitude change like any other. Q01 `c6` moved 9.50e-7 — it passed at 95 % of budget on the
   luck of this distribution, and one more group or a different rounding split would have failed it
   as a corruption. The gate is doing its job; it is simply not an instrument for declared types.

Issue filed for (1)–(3); the fix is to make the SF100 fixture's typing an asserted fact rather than
an accident of a March generator run, and to give the value signature a declared-type component.

### 6.2 Deploy-scoped confounds

- **Rendezvous placement differs between deploys** because the worker ids differ. The control's
  `lineitem` assignment was the more skewed of the two, and the scan-affinity balancer sheds
  accordingly: control `max_share_before=1.29`, sheds 4 files / 1 189 169 887 B; candidate
  `max_share_before=1.21`, sheds 3 files / 892 744 133 B. This is why several stages carry 14 tasks
  in the control and 13 in the candidate, and why `scan-affinity byte-balance` fires for Q02/Q11/Q16
  in the control and not the candidate. **Post-balance both arms are within 9 % / 6 % of fair**, so
  the residual imbalance is ~3 % of one worker's share — it cannot account for a 4.5 s stage
  collapse, and it would reproduce on either binary given the same instance ids.
- The `scalar substitution` literal `2385908.0562999994` → `2385908.0563` is **not** evidence of a
  typing change. `formatScalar`'s `parquet.TypeFloat64` arm is byte-identical across the range
  (`strconv.FormatFloat(v,'f',-1,64)`, shortest-round-trip, so identical bits print identically),
  and its DECIMAL arm is reachable only under `TPCH_DECIMAL=1`. This is Q15's `max(total_revenue)`
  over `sum(l_extendedprice*(1-l_discount))` differing by 1–2 ulp — ADR-0013's class 9, whose
  worked example is this exact expression. Q15's wall moved +0.091 s.

## 7. What this window could not measure

| signal | why not | what would capture it |
|---|---|---|
| **per-query CPU-seconds** | the worker CPU profile is one whole-suite profile per worker; only the coordinator gets per-query profiles, and it does ~1 % of the work | a per-task CPU counter on the `task completed` line, or per-query worker profiles |
| **spill engagement** | `spill` appears in both arms' journals only inside directory paths (`/mnt/nvme/spill/w0/…`); no operator spill event is logged either side | log the `spill_engagement` counters in `worker stats`; until then the spill hypothesis is testable only by the type-matrix sweep, not by SF100 |
| **ENA / NIC counters, NVMe read+write** | not sampled by the harness this window (window 6 quoted NVMe) | sample `/proc/diskstats` and `/proc/net/dev` at each run boundary |
| **per-run splits of the worker IO/GC counters** | cumulative counters read at end of suite; intermediate samples are not aligned to run boundaries | emit one `shuffle io stats` + `worker stats` line at each run boundary |
| **which of §4.2's four commits owns Q18** | all four are unconditional per-row costs on one path; one window cannot separate them | four single-lever A/Bs, `3f35e71b` first, on data whose quantity column is an INTEGER (§6.1) |

## 8. Verdict

**Wall** −9.7 % steady (138.80 → 125.32 s), −7.0 % cold, −8.9 % on the suite; mean and tail move
together. **CPU** −20.0 % per suite run (3 143.9 → 2 514.6 CPU-s), utilisation down 5.4 pp — work
removed, not wait. **Wire** flat: every counter within 5.2 %, the largest stream within 0.1 %, no
S3 shuffle fallback on either arm. **Allocation** −37.1 % of bytes with −1.2 % of objects, GC
cycles −29.9 % with pause total +1.9 s.

One mechanism carries the win and is attributed on three agreeing instruments: the stage DAG's
base-table reads were issued with an empty projection and reverted to full width on any unmatched
name, and `75acddbc` + `c15e3a01` fixed both halves — `decode_bytes` −37.3 %, `alloc_space`
−37.1 %, `decode_ms` −48.0 %, `broadcast_join` task-seconds −48.3 %, `broadcast_join` stage wall
−45.3 %. Two regressions run against it: Q21 +1.9 s, attributed to 13.4 % more bytes past the
semi-anti bloom on `scan-0` with `d338f6b5` as the named but unproven cause; and Q18 +1.4 s plus a
+7.8 % `final_aggregate` tax, **unattributed** — zero bytes move, and four commits that each add
unconditional per-row cost remain undiscriminated, led by `3f35e71b`'s exact Int128 SUM carrier
over a `l_quantity` that is genuinely `INT64` in this data.

**Correctness is clean, and one finding is not about performance at all.** 22/22 on every run, rows
identical in every cell. Q01's `avg_qty` signature moved because AVG over an integer now declares
`DECIMAL(38,4)` — `3f35e71b` working as ADR-0012 item 9 specifies, verified by reproducing the
declared type and the signature read on both paths. What that exposed is §6.1: **the SF100 data's
`lineitem` disagrees with the repo's own `LineItemSchema` on nine of sixteen columns**, most
importantly `l_quantity` (`INT64` vs `TypeFloat64`), because the bucket was written in March and is
never regenerated. SF100 is therefore the only place in the corpus that exercises integer SUM/AVG
on TPC-H — which is why both a signature change and this window's leading regression candidate
appeared here first and in no gate. The value-fingerprint gate cannot help: it compares magnitude,
so a narrowed declared scale and a corrupted value of the same size look alike, and Q01 passed at
95 % of its tolerance budget on the luck of the distribution.

## Artefacts

- Per-query per-run walls, both arms:
  [`sf100-baseline-v0.18.12-2026-09-02-per-query.txt`](sf100-baseline-v0.18.12-2026-09-02-per-query.txt)
- Run outputs: `results/20260902-210947/` (control), `results/20260902-215039/` (candidate) in
  `s3://wadjet-bench-sf100-use2`, each with `profiles/` (per-worker cpu/heap/block/mutex/goroutine
  plus per-query coordinator CPU and per-run heap).
- Prior windows: [window 4](sf100-window4-analysis-2026-08-22.md),
  [window 5](sf100-window5-analysis-2026-08-23.md), [window 6](sf100-window6-analysis-2026-08-23.md).
