# SF100 window-6 three-arm analysis — scan output backing reuse, and why 453 GiB/run of allocation did not move the heap lock (2026-08-23)

**Window:** 2026-08-23 01:48–02:28 UTC, three sequential EC2 deploys (one per arm, `tofu destroy`
between), coord c7g.2xlarge + 3× c7gd.4xlarge, `sf100-distributed.tfvars`, SF100 TPC-H, runs = 4
per arm (run 1 cold). Same hardware and configuration as windows 3–5. `block_profile_rate=10000`
and `mutex_profile_fraction=5` pinned in the same tfvars for all three arms.

| arm | engine | switch | run id | dir |
|---|---|---|---|---|
| **A base** | `9a8b564` (v0.17.0-clawback) | — | `20260823-014839` | `results/w6base/` |
| **B cand** | `be5fcf1` | — | `20260823-020311` | `results/w6cand/` |
| **C reuseoff** | `be5fcf1` | `WADJET_SCAN_BACKING_REUSE=0` | `20260823-021631` | `results/w6reuseoff/` |

Coordinator banner sha verified per deploy (`vcs.modified=false` on both S3-staged artifacts); for C
the switch was read back out of the live coordinator's and two workers' `/proc/<pid>/environ`, and on
B a mid-run `worker stats` line carried `backing_hits=7320 backing_misses=3814` — armed on the metal,
not just in the binary. **The same-window A/B for lever 3 is B vs C**; arm A also carries window 5's
five levers, so B−A is the arc delta, not the lever's.

**What `be5fcf1` adds over `550bb20`** (w5's candidate): `fa22f72`, `Vector.GetValue` copies on the
`TypeBytes` arm (#391, correctness); and **lever 3**, the scan row-group output backing pool
(`7890eb5`/`2858f9b`/`be5fcf1`, `WADJET_SCAN_BACKING_REUSE`, worker-side) — ownership statement and
predicted SF100 shape in `docs/design/scan-output-backing-reuse.md`, ADR-0016 amendment.

> **Cross-window reference, with the caveat (different deploy).** Window 5's arm B (`550bb20`) read
> alloc 1 798.9 GiB/run, 1 427 GC cycles, `runtime.mallocgc` 23.1 s/worker-run, steady 138.70 s. This
> window's C — the same lever set with the scan pool off — reads 1 845.5 GiB/run (+2.6 %), 1 422
> cycles (−0.4 %), 22.2 s/worker-run (−3.9 %), steady 139.30 s (+0.4 %): four families reproduce
> across deploys to ≤ 3 %. That licenses the cross-window column; it does not make B−(w5 B) an A/B.

## 1. Totals — the four metric families

### 1.1 Wall

| | A base | B cand | C reuseoff |
|---|---|---|---|
| r1 / r2 / r3 / r4 | 203.67 / 145.38 / 144.55 / 146.37 | 166.74 / 136.06 / 135.26 / 134.40 | 169.63 / 139.71 / 139.08 / 139.10 |
| **cold (r1)** | 203.67 | **166.74** (−36.93 vs A) | 169.63 |
| **steady mean (r2–4)** | 145.43 | **135.24** (−7.0 % vs A) | 139.30 |
| **mode-normalised** (Σ best of r2–4) | 138.26 | **131.11** | 134.07 |
| Σ excess over own mode, per steady run | 7.17 | **4.13** | 5.22 |
| suite `total_seconds` | 639.97 | 572.47 | 587.52 |

**B − C = −4.06 s steady (−2.9 %), −2.96 s mode-normalised, −2.89 s cold**, B faster on 19 of 22
queries. Mode and mean both move, so the win is not tail-only.

**Tail.** Straggler census on **total** acquisition wall, any tier, any stage (≥ 3 000 ms —
`acq_prefetch_ms` alone cannot see a stall that migrated to `acq_basecache` or `acq_s3`):

| arm | firings | Σ | scan-* | join-* | max | runs 2–4 |
|---|---|---|---|---|---|---|
| A base | 34 | 340.7 s | 22 / 262.1 s | **12 / 78.6 s** | 17.8 s | **0** |
| B cand | 24 | 254.3 s | 21 / 244.4 s | **3 / 9.9 s** | 17.8 s | **0** |
| C reuseoff | 24 | 253.6 s | 22 / 247.4 s | 2 / 6.2 s | 18.7 s | **0** |

Every firing is in run 1. The `join-*` column is w5's probe-split-affinity effect reproducing
(78.6 → 9.9 s), not lever 3; B and C are indistinguishable on it, as they must be.

### 1.2 Per-query steady mean (r2–4), seconds

| Q | rows | A | B | C | B−C | | Q | rows | A | B | C | B−C |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Q01 | 4 | 3.732 | 3.476 | 3.795 | −0.319 | | Q12 | 2 | 5.276 | 4.641 | 4.826 | −0.185 |
| Q02 | 100 | 4.741 | 4.160 | 4.269 | −0.109 | | Q13 | 100 | 5.515 | 5.294 | 5.527 | −0.233 |
| Q03 | 10 | 9.256 | 9.222 | 8.889 | **+0.333** | | Q14 | 1 | 2.025 | 1.808 | 2.046 | −0.238 |
| Q04 | 5 | 5.009 | 5.056 | 5.169 | −0.113 | | Q15 | 1 | 2.616 | 1.763 | 2.027 | −0.264 |
| Q05 | 5 | 5.788 | 5.966 | 6.051 | −0.085 | | Q16 | 27840 | 5.704 | 5.126 | 5.456 | −0.330 |
| Q06 | 1 | 0.821 | 0.767 | 0.896 | −0.129 | | Q17 | 1 | 6.538 | 5.630 | 5.686 | −0.056 |
| Q07 | 4 | 4.179 | 4.210 | 4.483 | −0.273 | | Q18 | 100 | 13.446 | 10.515 | 10.878 | −0.363 |
| Q08 | 2 | 15.419 | 15.018 | 15.342 | −0.324 | | Q19 | 1 | 3.618 | 3.770 | 3.680 | +0.090 |
| Q09 | 175 | 15.381 | 14.164 | 14.556 | −0.392 | | Q20 | 17971 | 9.401 | 9.165 | 9.446 | −0.281 |
| Q10 | 20 | 11.442 | 11.113 | 11.295 | −0.182 | | Q21 | 100 | 9.578 | 8.977 | 9.563 | **−0.586** |
| Q11 | 92698 | 3.211 | 2.605 | 2.712 | −0.107 | | Q22 | 7 | 2.738 | 2.796 | 2.704 | +0.092 |
| | | | | | | | **Σ** | | **145.43** | **135.24** | **139.30** | **−4.06** |

No query carries the win; it is −0.1 to −0.4 s spread over 19 queries — the signature of a
per-row-group cost removed from every scan, which is what the lever is.

### 1.3 CPU

| | A | B | C |
|---|---|---|---|
| worker CPU-s, whole suite (Σ 3 w) | 12 807.6 | 12 193.1 | 12 399.2 |
| **worker CPU-s per suite run** | 3 201.9 | **3 048.3** | 3 099.8 |
| ↳ utilisation of 48 vCPU over the profile window | 41.4 % | **44.1 %** | 43.6 % |

B − C = **−51.5 CPU-s/run (−1.7 %)** — the design note predicted flat ("the memclr is the same; the
allocation is gone"). It is not, and §2.3 says why.

### 1.4 Network (suite = 4 runs, Σ 3 workers; pprof/binary units)

| | A | B | C |
|---|---|---|---|
| base-table **peer** transfers / bytes | 206 / **46.93 GiB** | 38 / 3.60 GiB | 39 / 3.83 GiB |
| shuffle **peer** files / bytes | 43 354 / 90.94 GiB | 43 421 / 92.43 GiB | 44 175 / 93.79 GiB |
| shuffle **s3** files / bytes · `peer_fallthroughs` | 0 / 0 · 0 | 0 / 0 · 0 | 0 / 0 · 0 |
| S3 `upload_done` PUTs / bytes | 33 807 / 101.79 GiB | 27 807 / 93.90 GiB | 29 667 / 97.05 GiB |
| **Σ inter-node (peer) bytes** | **137.87 GiB** | **96.03 GiB** | 97.61 GiB |
| NVMe read / write (GiB) | 448.4 / 564.5 | 259.0 / 493.8 | 284.5 / 495.2 |
| scalar-substitution tier / Σ wait_ms | (no telemetry) | peer × 12 / 46 | peer × 12 / 54 |

B−C on the wire is ≤ 1.6 % on every row: **lever 3 is worker-local and has no wire footprint**, as
designed. The A→B shape is window 5's (base-table peer 206 → 38 transfers, Σ inter-node −30 %) and
reproduces to within 1.2 % of window 5's own A→B.

### 1.5 Heap

| | A | B | C | B−C |
|---|---|---|---|---|
| **alloc per suite run** (GiB, Σ 3 w) | 1 878.4 | **1 392.3** | 1 845.5 | **−453.2 (−24.6 %)** |
| **allocation EVENTS per suite run** (M) | 6 345.5 | **6 332.8** | 6 323.1 | **+9.7 (+0.15 %)** |
| ↳ mean allocation size | 318 B | **236 B** | 313 B | −25 % |
| heap-lock family flat (`runtime.unlock` + `_LostContendedRuntimeLock`), s/suite | 590.1 | 553.6 | 519.6 | **+34.0** |
| ↳ per worker-run | 49.2 | 46.1 | 43.3 | +2.8 |
| `runtime.mallocgc` cum (w5's metric), s/worker-run | 24.4 | 23.6 | 22.2 | +1.4 |
| `sync.(*Mutex).Unlock` flat, s/suite | 148.9 | **86.5** | 128.5 | **−42.0** |
| **total mutex delay**, s/suite | 741.4 | **641.9** | 654.1 | **−12.2** |
| GC cycles (Σ 3 w, suite) | 1 483 | **1 159** | 1 422 | −263 (−18.5 %) |
| GC pause total / mean per cycle (ms) | 8 311 / 5.60 | 7 914 / **6.83** | 7 579 / 5.33 | +335 / +1.50 |
| minor faults, suite (M) | 277.4 | **257.8** | 270.6 | −12.8 (−4.7 %) |
| peak `alloc_mb` (max worker, any run) | 15 492 | **15 244** | 16 410 | −1 166 |
| peak `rss_mb` (max worker, any run) | 21 184 | **17 557** | 20 792 | −3 235 |

Read the first three rows together: **the lever removed a quarter of the bytes and none of the
allocations** — that is the whole of §3.

## 2. Lever 3, direct A/B (B vs C)

### 2.1 Engagement and reuse rate

`backing_hits` / `backing_misses` / `backing_claimed`, FINAL cumulative, Σ 3 workers:
**109 094 / 57 978 / 0** → **hit rate 65.3 %**. A and C emit no `backing_*` group (it is logged only
when the sum is non-zero), so C is the intended 0-hit control and A predates the counter.

**`backing_claimed = 0`: the retention veto never fired once in 167 072 releases.** Design note §3.3
predicted `claimed ≈ misses` on build/Sort-fed fragments; here every armed source feeds a
late-materialisation probe or a filter/project chain, none of which claim. The 34.7 % of misses are
therefore **not** retaining consumers — they are the pool being empty at `get` time (the ring
launches the next decode before the parent's `retire` fires) plus `Recycle`'s `MaxIdle=4` /
`MaxIdleBytes=512 MiB` refusals.

**Where the misses cost.** Under `scan.readRowGroupNative` (the mint/reset frame; `func2` is the
per-column errgroup and roots separately) alloc_space is C 1 893.3 → B 657.4 GiB, so B/C = **0.347**
against a miss fraction of 57 978/167 072 = **0.347**. Measured, not assumed: **a hit allocates
essentially nothing** (`resizeCleared` flat 5.1 GiB in B against 1.4 in C — 36 KB/hit) and **a miss
costs a full mint, hits and misses size-representative to within ~7 %** — the caps are not selecting
against the wide shapes, the pool is simply empty when the ring is busy. Another 164 GiB/run sits
behind the miss rate; `get` matches shape only (`backing_pool.go:220–232`, LIFO, no capacity
comparison) and the binding cap is 512 MiB against ~280 MB `lineitem` groups.

### 2.2 Which allocators moved (heap profiles, `-sample_index=alloc_space` / `alloc_objects`)

| allocator | A GiB | B GiB | C GiB | B−C | A Mobj | B Mobj | C Mobj | B−C obj |
|---|---|---|---|---|---|---|---|---|
| `batch.NewVectorWithScale` (flat) | 2 204.7 | **1 144.5** | 2 199.1 | **−1 054.7** | 45.79 | 44.53 | 45.53 | **−1.00 (−2.2 %)** |
| `BytesColumn.PreAllocBytes` (flat) | 780.2 | **277.0** | 779.6 | **−502.6** | 1.36 | 1.21 | 1.36 | −0.15 (−11 %) |
| `batch.NewBytesColumn` (flat) | 269.8 | **85.9** | 267.2 | −181.3 | 3.44 | 3.33 | 3.52 | −0.19 (−5 %) |
| `batch.NewBitmap` (flat) | 62.0 | 37.0 | 61.5 | −24.6 | 76.99 | 76.66 | 77.39 | −0.73 (−0.9 %) |
| `scan.readRowGroupNative` (cum) | 1 893.8 | **657.4** | 1 893.3 | −1 235.9 | 8.4 | 5.7 | 8.8 | −3.1 (−35 %) |
| `scan.readRowGroupNative.func2` (cum) | 2 175.2 | **1 530.2** | 2 087.7 | −557.5 | 373.3 | 368.7 | 370.2 | −1.5 (−0.4 %) |
| `scan.readColumnNative` (cum) | 2 112.7 | 1 500.6 | 2 034.1 | −533.5 | 372.4 | 368.0 | 369.4 | −1.4 |
| `parquet.getZstdBuf` (flat) | 433.3 | 406.1 | 405.8 | +0.3 | 0.54 | 0.50 | 0.50 | ±0 |

The two scan frames account for **−1 793.4 GiB of the suite's −1 812.7 GiB** — 98.9 % of the saving
is in the decode path and nothing outside it moved; `resizeCleared` is the only new allocator, at
5.1 GiB.

### 2.3 Which CPU moved

Added (Σ 3 w, suite): `scan.(*BackingPool).get` **+25.5 CPU-s**, of which `Vector.ResetForWrite` is
+25.1 (C 7.2 → B 32.3). Removed: `makeslice` −47.8, `mallocgcLarge` −46.9, `memclrNoHeapPointersChunked`
−35.9, `NewVectorWithScale` −36.5, `NewRecordBatch` −35.5, `newVectorFromColumn` −35.3,
`copyNativeDataDirect` −38.5. Net at the roots: **`readRowGroupNative.func2` −93.2, `readColumnNative`
−78.4 CPU-s** per suite. So "CPU flat, the memclr is the same" is wrong in the engine's favour: the
reused arena removes `PreAllocBytes`'s `make`+`copy` *and* the zeroing of a fresh multi-hundred-MB
span, and writing into an already-faulted arena costs 12.8 M fewer minor faults over the suite.

### 2.4 Measured against `scan-output-backing-reuse.md` §6.2, line by line

Predictions were anchored on window 3's arm B (`69aecbb`); the same-window control is C.

| signal | w3 anchor | predicted | **C (off)** | **B (on)** | verdict |
|---|---|---|---|---|---|
| `readRowGroupNative.func2` heap-lock | 130.8 s | **40–70 s** | 111.5 | **90.2** | **miss** (−19 %, not −47…−69 %) |
| ↳ `readColumnNative` | 100.9 s | 25–45 s | 79.6 | 67.9 | miss |
| ↳ `NewVectorWithScale` / `newVectorFromColumn` | 61.5 / 44.0 | 15–25 / 5–15 | 64.8 / 43.0 | **84.5 / 67.8** | **wrong sign** |
| `batch.NewRecordBatch` | 43.8 s | < 10 s | 43.0 | **67.7** | **wrong sign** |
| `parquet.ColumnPageReader` page buffers | 94.6 s | unchanged | 73.5 | **65.8** | better than predicted |
| alloc_space | — | **−350…−550 GiB/run** | 1 845.5 | **1 392.3** | **hit** (−453.2, mid-band) |
| ↳ `readRowGroupNative` cum | 546.7 GiB/run | −60…−80 % | 473.3 | **164.4** | **hit** (−65.3 %) |
| ↳ `PreAllocBytes` | 195.1 GiB/run | −70…−90 % | 194.9 | **69.3** | just short (−64.5 %) |
| worker mutex delay | 65.9 s/worker-run | **−5…−9 s/worker-run** | 54.5 | 53.5 | **miss** (−1.0) |
| GC cycles | 1 466 | −5…−12 % | 1 422 | **1 159** | **beat** (−18.5 %) |
| worker CPU | — | flat | 3 099.8 | **3 048.3** | beat (−1.7 %) |
| wall | — | neutral-to-slightly-better | 139.30 | **135.24** | hit (−2.9 %) |

(Row 5's w3 anchor and the deploy summary's `ColumnPageReader.*` row are substring sums over nested
frames; the exact root frame, `NextPage`/`NextPageMaybeSkip`, is what is quoted here.) **Every byte,
GC, CPU and wall prediction landed or was beaten; every lock-delay prediction missed and two came back
with the wrong sign** — they were derived by scaling lock delay with bytes, and §3 is why that fails.

## 3. ★ The counter-direction signal: 25 % less allocation, +34.0 s on the heap-lock family

`runtime.unlock` + `_LostContendedRuntimeLock` flat, Σ 3 w × 4 runs: A 590.1, **B 553.6**, **C 519.6**.
Four candidates were put to the profiles; three are refuted by measurement.

**(1) The pool's own `BackingPool.mu` — refuted, by name.** Its unlock sites appear in B's mutex
profile at line granularity: `get` (`backing_pool.go:240`) **229.5 µs** and `Recycle` (`:323`/`:328`)
**46.6 µs**, Σ 3 w × 4 runs = **0.276 ms** — 0.00005 % of the family. (`ResetForWrite` under `get`
(`:243`) adds 17.5 ms of *runtime* lock; nothing named `backing` appears in C.)

**(2) More large allocations from the cap policy — refuted, by count.** Total allocation events are
**flat**: B 25 331.0 M vs C 25 292.2 M (+0.15 %); the dominant allocator loses 48 % of its bytes and
**2.2 % of its calls** (§2.2). The small-object side of the lock is flat to 0.6 % (`mcentral.grow`
96.8 vs 96.0 s, `mcache.refill` 99.3 vs 99.4, `cacheSpan` 99.0 vs 99.4) and the large side did *less*
work (`mallocgcLarge` CPU 149.3 vs 196.2 s, −24 %); §2.1 shows the misses are size-representative.

**(3) GC interaction — confirmed, and small.** B runs 263 fewer cycles but each pause is 1.50 ms
longer (6.83 vs 5.33 ms): the idle set is live heap the collector must trace. GC-side runtime-lock
holders rise with it — `sweepone` 25.0 vs 17.7 s, `newMarkBits` 11.0 vs 8.0, `gcBgMarkWorker` 9.4 vs
2.8, sweeper `freeSpan` 40.8 vs 34.7 — **≈ +18 s/suite, +1.5 s/worker-run**, against −21.2 s at the
lever's target caller.

**(4) Measurement — partly confirmed.** `mutex_profile_fraction=5` is identical across arms, so the
rate is not it — but two components of the family are not engine work. `runtime.unlock` flat, +27.2 s,
by holder:

| holder | B | C | Δ | what lock |
|---|---|---|---|---|
| `runtime.(*mheap).allocSpan` | 266.35 | 253.25 | **+13.10** | mheap |
| `sysmon retake → incidlelocked` | 24.38 | 8.43 | **+15.95** | **sched.lock — not the heap** |
| `sweep → freeSpan.func2` | 40.80 | 34.66 | +6.14 | mheap (GC) |
| `selunlock` / `selparkcommit` | 42.09 | 35.05 | +7.04 | chan/select |
| `newMarkBits` | 11.04 | 8.01 | +3.03 | mheap (GC) |
| `runtime.saveBlockEventStack` | 1.30 | 17.65 | **−16.35** | **block profiler's own bucket lock** |
| `wakep` | 7.99 | 10.39 | −2.40 | sched |
| Σ listed (+ `scavengeOne` +0.84) | | | **+26.5** (observed +27.2) | |

`_LostContendedRuntimeLock` (+6.7 s) is delay the runtime could not attribute to a stack. So of the
headline +34.0 s, **+16.0 s is `sched.lock` under sysmon, −16.4 s is the block profiler's own
instrumentation and +6.7 s is unattributed** — leaving ≈ +22 s genuinely on the heap lock, +18 s of
which §3(3) accounts for on the GC side.

**What is left — OPEN RESIDUAL.** `mheap.allocSpan` holds **+13.1 s** longer in B, and the
`readRowGroupNative → NewRecordBatch` edge carries **36.35 s in B against 14.16 s in C** on one third
the mints — 627 µs of induced delay per mint against 84.8 µs. Best available mechanism: selection bias
in *which* mints survive — the pool absorbs the decodes that start while the ring is quiet and leaves
those that start while every decode worker is busy, exactly when the lock is already queued, and
contention delay is superlinear in instantaneous concurrency. **This window cannot confirm it, and the
arm difference is not separable from worker spread:** per-worker family is B **165.6 / 153.8 / 234.2**
against C **191.2 / 159.8 / 168.6**, so two of B's three workers sit below every C worker and one
(`6c1358e9`, also B's highest `allocSpan` hold, 109.7 s against its siblings' 75.1 / 81.5) carries the
whole difference. It does not change the verdict: the metric that is not a subset moved the other way
— **total mutex delay is lower in B (641.9 vs 654.1 s)**, `sync.Mutex` contention falls on all three
workers (28.9 / 27.8 / 29.8 s against C's 47.7 / 39.7 / 41.1), and peak RSS falls 3.2 GB.

**Instrumentation that would settle it**, all worker-side and cheap: (1) an allocation-COUNT series on
the 30 s `worker stats` tick (`runtime/metrics` `/gc/heap/allocs:objects`) beside `alloc_mb` — bytes
and acquisitions are different quantities and only one drives the lock, and we have no time series for
the one that does; (2) **`backing_idle_mb`**, which design note §4.3 specifies and which appears on
**no** `worker stats` line in this window, leaving §3(3)'s live-heap contribution inferred rather than
measured; (3) `GODEBUG=gctrace=1` on one worker per arm, which turns §3(3) from four correlated
symbols into a direct reading of live heap at cycle start.

## 4. Sanity

* **88/88 query executions `ok:true` in every arm** — 264 total, zero `ok:false`, zero FAILED / panic
  lines. **Row counts identical** across 3 arms × 4 runs × 22 queries and equal to the SF100 baseline
  (Q02 = 100, Q09 = 175, Q11 = 92 698, Q16 = 27 840, Q20 = 17 971).
* **Value signatures identical** for all 21 emitting queries across all 12 samples (Q20 emits none, as
  at `9a8b564` and in w5). **No Q19 last-digit flicker this window** — `c0:5.985878903e+08` in all 12,
  against w5's two E-arm ULP flips (ADR-0013 class 9).
* **Zero** retries (`attempt>=2`), task failures, reaps, `peer_fallthroughs`, shuffle-s3 bytes,
  `upload_failed`, evictions and spills in all three arms; `majflt = 3` per arm. Worker
  `level=ERROR`: 0; coordinator: the same 3 NATS `Client parser ERROR, state=0` lines at worker connect.
* Coordinator engagement is identical between B and C where it should be: 292 `dispatchComputeStage`
  lines, 56 `probe_split=true`, 32 `agg_row_bound` dispatches with byte-identical per-stage sums except
  `final_aggregate-7`'s ±8-row wobble; the 32-vs-44 `probe_affine=true` split is the rendezvous draw.
* **ClickBench not run** — single-node; lever 3 is worker-fragment-path only and the single-process
  `exec.Pipeline` is out of scope (design note §5). Owed at the release tag.

## 5. Verdict

### 5.1 Lever 3 — keep default-on

| family | reading | verdict |
|---|---|---|
| **wall** | steady −4.06 s (−2.9 %), mode-normalised −2.96 s, cold −2.89 s; 19 of 22 queries faster; no query regresses more than +0.33 s | **for** |
| **CPU** | −51.5 CPU-s/run (−1.7 %); `readRowGroupNative.func2` −93.2 CPU-s/suite; the pool's own cost is +25.5 | **for** |
| **wire** | ≤ 1.6 % on every ledger row — no footprint, as designed | **neutral** |
| **alloc / heap-lock / GC** | alloc −453.2 GiB/run (−24.6 %), GC −263 cycles (−18.5 %), peak RSS −3.2 GB, total mutex −12.2 s; heap-lock family +34.0 s, **§3 open residual**, inside worker spread | **for, with §3 named** |

**Keep it on.** It is the first lever in the arc that moves allocation *and* the stopwatch in the same
direction, without touching the wire or the answers (`backing_claimed = 0`, 264/264 `ok`, 21/21 value
signatures stable). The heap-lock counter-signal is bounded and decomposed and does not survive the
total-mutex and per-worker readings: a residual to instrument, not a reason to hold the switch.

### 5.2 Arc scoreboard — v0.17.0 (`9a8b564`, arm A) → main (`be5fcf1`, arm B), same window

| family | v0.17.0 | main | Δ |
|---|---|---|---|
| **wall** steady / cold / mode-normalised | 145.43 / 203.67 / 138.26 s | **135.24 / 166.74 / 131.11 s** | **−7.0 % / −18.1 % / −5.2 %** |
| **wall** tail: firings r1 / r2–4; Σ excess over mode | 34 / 0; 7.17 s per run | **24 / 0; 4.13 s per run** | −10 / 0; −42 % |
| **CPU** worker CPU-s per run (utilisation) | 3 201.9 (41.4 %) | **3 048.3 (44.1 %)** | −4.8 % (+2.7 pt) |
| **network** Σ inter-node; base-table peer transfers; PUTs | 137.87 GiB; 206; 33 807 | **96.03 GiB; 38; 27 807** | **−30 %**; −82 %; −18 % |
| **heap** alloc/run; alloc events/run; heap-lock per worker-run; GC cycles | 1 878.4 GiB; 6 345.5 M; 49.2 s; 1 483 | **1 392.3 GiB; 6 332.8 M; 46.1 s; 1 159** | **−25.9 %**; −0.2 %; −6.3 %; **−21.9 %** |

### 5.3 Next levers

1. **★ Parquet page buffers — the remaining 57 % of scan `B/op`** (design note §6.1), now the largest
   un-pooled scan class: `ColumnPageReader.NextPage` holds **65.8 s** of runtime-lock delay per suite
   (5.5 s/worker-run) behind `DecodeBitPacked` 425.0 + `getZstdBuf` 406.1 + `DecodePlainByteArray`
   416.7 GiB/suite. Inside the safety-critical package: round-trip and fuzz burden first.
2. **★ Judge the next allocation lever on acquisitions, not bytes** (§3) — 24.6 % of bytes, 0.15 % of
   events. Land instrumentation item 1 before the next pooling lever states a prediction.
3. **Raise the 65.3 % hit rate** (§2.1): another 164 GiB/run sits behind the pool being empty when the
   ring is busy. Seams are capacity-aware `get` (shape-only today) and the 512 MiB cap against ~280 MB
   `lineitem` groups. Expect bytes and GC, not lock delay.
4. **Give `readFinalResults` a tier/wait line** (owed since w5 §4; still 12 `tier=` lines per arm, all
   scalar) — half of lever 4's read surface remains unobservable.
5. **Q21's whole-cluster idle**, present in every arm and not a scalar site; Q21 is also this window's
   largest single B−C move (−0.586 s).
6. **The w5 F-arm Q08 compute-inflation mode** did not fire in any arm here (Q08 steady spread ≤ 0.4 s
   in all three), so it stays unnamed and unreproduced.
