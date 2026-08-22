# SF100 window-4 three-arm analysis — ownership-aware probe-split placement (2026-08-22)

**Window:** 2026-08-22 21:43–22:23 UTC, three sequential EC2 deploys (one per arm, `tofu destroy`
between), coord c7g.2xlarge + 3× c7gd.4xlarge, `sf100-distributed.tfvars`, SF100 TPC-H,
runs = 4 per arm (run 1 cold). Same hardware and configuration as window 3
(`docs/benchmarks/sf100-window3-analysis-2026-08-22.md`, **w3**).

| arm | engine | switch | run id | dir |
|---|---|---|---|---|
| **A base** | `9a8b564` (v0.17.0-clawback) | — | `20260822-214300` | `results/w4base/` |
| **B cand** | `6f4905e` | — | `20260822-215749` | `results/w4cand/` |
| **C affoff** | `6f4905e` | `WADJET_PROBE_SPLIT_AFFINITY=0` | `20260822-221204` | `results/w4affoff/` |

Binaries verified from each arm's coordinator NATS banner: A `[9a8b564]`, B/C `[6f4905e]`;
`vcs.modified=false` on the staged artifacts. The switch reached the **coordinator** this time
(d97eed6 fixed the `extra_env` seam w3 §0 found): arm C logs `probe_affine=false` on all 292
`dispatchComputeStage` lines against B's 44 `true`.

Working files: `scratchpad/w4/load.py` (coordinator dispatch map), `w4/tasks.py` (worker task →
(run, query) binding), `w4/{census,ledger,wire,stages,detail}.txt`.

> **Mapping validation.** Each arm's coordinator journal carries exactly 88 `stage-DAG dispatch`
> lines in Q01..Q22 order (4 runs × 22). Worker tasks bind to (run, query) through
> `executing task query_id=st-<stage>-<qhash>`. Cross-check: the worker envelope (first task start
> → last task completion) matches the harness wall within **±0.25 s on all 22 queries in every
> arm** (Σ|Δ| per suite: A 0.9 s, B 0.3 s, C 0.5 s), the residual being gathers that start before
> the query clock. Idle time *inside* the envelope is a separate quantity, used in §7.

---

## 0. What `6f4905e` changed, and what each arm isolates

Four changes ship in the commit. Only (a) has a kill switch **in the deployed binary**:

| | change | side | switch at 6f4905e | A | B | C |
|---|---|---|---|---|---|---|
| **(a)** | `probeSplitAffineSets`: group probe files by rendezvous owner, one task per owner, `Task.AffinityWorkerID` | coordinator | `WADJET_PROBE_SPLIT_AFFINITY` | off | **on** | off |
| **(b)** | affinity tier ahead of locality in `pickWorkerFor` | coordinator | none | off | on | on |
| **(c)** | prefetcher skips its spill copy when `Get` populated the base-table cache | worker | none | off | on | on |
| **(d)** | `peer_fetch_ms` + one `base-table cache: peer fetch` line per transfer | worker | — (telemetry) | off | on | on |

**C is the (a)-only control.** It is *not* a control for (b) or (c) — those are on in both cand
arms. That is the central limitation of this window and it drives §5. Switches for both have since
been added (`WADJET_AFFINITY_BEFORE_LOCALITY`, `internal/coordinator/scheduler.go:319`;
`WADJET_PREFETCH_CACHE_SKIP`, `internal/worker/scan_prefetch.go:56`) — after these binaries were
cut, so the per-switch arms §9.2 asks for are a deploy, not new code.

---

## 1. Totals — wall and wire

| | A base | B cand | C affoff |
|---|---|---|---|
| suite wall r1 / r2 / r3 / r4 | 197.87 / 152.49 / 147.24 / 143.67 | 177.17 / 141.99 / 147.35 / 147.87 | 174.72 / 145.20 / 145.84 / 146.30 |
| **cold (r1)** | 197.87 | **177.17 (−20.70)** | **174.72 (−23.15)** |
| **steady mean (r2–r4)** | 147.80 | **145.74 (−1.4 %)** | **145.78 (−1.4 %)** |
| mode-normalised (best steady run per query) | 138.60 | 138.55 | 139.26 |
| steady mean: only Q07/08/09/17 · all others | 44.92 · 102.88 | **42.34** · 103.40 | 43.00 · 102.78 |
| suite task-seconds, steady mean | 1 078.4 | **1 064.8 (−1.3 %)** | 1 087.4 (+0.8 %) |
| worker CPU per suite run (Σ 3 w) | 3 190.2 s | 3 153.4 s | 3 224.8 s |
| ↳ utilisation of 16 vCPU/worker | 41.2 % | 42.5 % | 43.6 % |

**Per query** — steady mean (r2–r4), mode-normalised (best of r2–r4), and cold r1, seconds:

| Q | rows | A st | B st | C st | B−A | C−A | A best | B best | C best | A r1 | B r1 | C r1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Q01 | 4 | 3.834 | 3.555 | 3.531 | −0.28 | −0.30 | 3.619 | 3.480 | 3.468 | 23.62 | 25.69 | 22.35 |
| Q02 | 100 | 4.491 | 4.219 | 4.777 | −0.27 | +0.29 | 3.959 | 4.056 | 4.445 | 10.97 | 9.22 | 10.16 |
| Q03 | 10 | 8.964 | 9.078 | 8.859 | +0.11 | −0.11 | 8.906 | 8.736 | 8.733 | 15.12 | 14.51 | 15.21 |
| Q04 | 5 | 5.099 | 5.390 | 5.451 | +0.29 | +0.35 | 5.006 | 5.261 | 5.040 | 5.07 | 5.22 | 5.38 |
| Q05 | 5 | 5.832 | 6.122 | 5.995 | +0.29 | +0.16 | 5.532 | 6.002 | 5.780 | 6.04 | 6.37 | 5.84 |
| Q06 | 1 | 0.894 | 0.806 | 0.871 | −0.09 | −0.02 | 0.809 | 0.753 | 0.848 | 0.69 | 0.62 | 0.69 |
| Q07 | 4 | 4.483 | 4.970 | 4.645 | **+0.49** | +0.16 | 3.962 | 3.328 | 3.675 | 4.92 | 4.81 | 4.10 |
| Q08 | 2 | 17.803 | 15.862 | 15.882 | **−1.94** | **−1.92** | 15.601 | 15.584 | 15.577 | 20.29 | 15.52 | 16.47 |
| Q09 | 175 | 16.265 | 15.230 | 15.803 | **−1.04** | −0.46 | 15.243 | 15.039 | 15.335 | 25.58 | 14.92 | 16.70 |
| Q10 | 20 | 11.001 | 11.256 | 11.027 | +0.25 | +0.03 | 10.733 | 10.929 | 10.780 | 11.21 | 11.83 | 10.90 |
| Q11 | 92698 | 3.157 | 2.957 | 2.942 | −0.20 | −0.22 | 2.637 | 2.650 | 2.753 | 2.94 | 2.64 | 2.97 |
| Q12 | 2 | 5.258 | 4.941 | 4.957 | −0.32 | −0.30 | 4.875 | 4.853 | 4.926 | 5.99 | 4.72 | 5.23 |
| Q13 | 100 | 5.682 | 5.650 | 5.640 | −0.03 | −0.04 | 5.519 | 5.604 | 5.460 | 5.60 | 5.46 | 5.27 |
| Q14 | 1 | 1.742 | 2.015 | 1.984 | +0.27 | +0.24 | 1.615 | 1.909 | 1.844 | 1.82 | 2.17 | 2.09 |
| Q15 | 1 | 2.284 | 2.495 | 2.364 | +0.21 | +0.08 | 2.067 | 2.337 | 2.104 | 2.38 | 2.05 | 2.51 |
| Q16 | 27840 | 5.659 | 5.414 | 5.639 | −0.25 | −0.02 | 5.480 | 5.254 | 5.204 | 5.66 | 6.53 | 5.12 |
| Q17 | 1 | 6.372 | 6.274 | 6.672 | −0.10 | +0.30 | 6.077 | 5.836 | 6.074 | 13.07 | 6.41 | 6.65 |
| Q18 | 100 | 13.923 | 13.945 | 12.977 | +0.02 | −0.95 | 12.445 | 12.244 | 12.253 | 12.01 | 12.63 | 12.00 |
| Q19 | 1 | 3.580 | 3.819 | 3.759 | +0.24 | +0.18 | 3.475 | 3.609 | 3.636 | 3.53 | 3.72 | 3.57 |
| Q20 | 17971 | 9.440 | 9.360 | 9.395 | −0.08 | −0.05 | 9.228 | 9.306 | 9.186 | 9.30 | 9.12 | 9.16 |
| Q21 | 100 | 9.342 | 9.041 | 9.564 | −0.30 | +0.22 | 9.291 | 8.923 | 9.320 | 9.40 | 10.07 | 9.69 |
| Q22 | 7 | 2.695 | 3.339 | 3.044 | **+0.64** | +0.35 | 2.518 | 2.860 | 2.818 | 2.65 | 2.95 | 2.65 |
| **Σ** | | **147.80** | **145.74** | **145.78** | **−2.06** | **−2.02** | **138.60** | **138.55** | **139.26** | **197.87** | **177.17** | **174.72** |

**Wire, Σ 3 workers over the whole 4-run suite** — the metric that does not show up in a mean wall
(this cluster's failure mode is ENA-credit starvation surfacing as roaming tails, not a mean shift):

| wire ledger | A base | B cand | C affoff | B − A |
|---|---|---|---|---|
| **base-table peer files / bytes** | 210 / **51.65 GB** | **42 / 4.98 GB** | 206 / 50.78 GB | **−168 / −46.67 GB** |
| ↳ of which in steady runs r2–r4 | 47 / 12.19 GB | **0 / 0.00 GB** | 61 / 16.62 GB | −47 / −12.19 GB |
| base-table S3 miss files / bytes | 89 / 23.22 GB | 99 / 25.69 GB | 94 / 24.50 GB | +10 / +2.47 GB |
| base-table readthrough bytes | 3.55 GB | 3.45 GB | 2.23 GB | −0.10 GB |
| shuffle peer files / bytes | 44 127 / 98.95 GB | 44 118 / 99.52 GB | 41 199 / 92.21 GB | −9 / +0.57 GB |
| shuffle s3 bytes / `peer_fallthroughs` | 0 / 0 | 0 / 0 | 0 / 0 | — |
| S3 upload_done bytes | 105.00 GB | 106.52 GB | 99.75 GB | +1.52 GB |
| **Σ inter-node (peer) bytes** | **150.60 GB** | **104.50 GB** | 142.99 GB | **−46.10 GB (−31 %)** |
| nvme_read / nvme_write bytes | 470.2 / 604.8 GB | **296.1 / 531.2 GB** | 418.8 / 549.6 GB | −174.2 / −73.6 GB |
| base-table cache resident at suite end | 316 entries / 77.69 GB | **148 / 31.03 GB** | 312 / 76.83 GB | −168 / −46.66 GB |

**−46.1 GB of inter-node traffic per suite (−11.5 GB per run) is a keeper result on its own**, and
it is (a)'s alone: C, with (a) off and (b)+(c) on, moves 143.0 GB — A's number. Affinity also halves
steady-state cache duplication (148 entries vs 316): each probe file lives on one worker's NVMe
instead of being replicated to all three by placement drift.

---

## 2. Mechanism (a): the placement is exactly what the design note predicted

14 `probe_split=true` stages per run (56 per arm). Stage→query map from walking
`dispatchComputeStage` (all three arms agree; matches the w3 correction in
`probe-split-affinity-2026-08-22.md`): **Q08/Q09 → `join-6`, Q17 → `join-2`**, plus Q02
`join-19`/`join-4`, Q07 `join-4`, Q10/Q11/Q12/Q16/Q19 `join-2`, Q20 `join-3`/`join-13`/`join-15`.
`published tasks … placement=` on the batch whose `count` equals `num_tasks` (same in all 4 runs
of each arm unless noted):

| query / stage | A base | B cand | C affoff |
|---|---|---|---|
| **Q08 `join-6`** | `binpack:3` | **`affine:3`** | `binpack:3` |
| **Q09 `join-6`** | `binpack:3` | **`affine:3`** | `binpack:3` |
| **Q17 `join-2`** | `binpack:3` | **`affine:3`** | `binpack:3` |
| Q02 `join-19`, Q10/Q12/Q16 `join-2`, Q20 `join-3`/`join-13` | `binpack:3` | **`affine:3`** | `binpack:3` |
| Q02 `join-4` | `binpack:3` (r2 `binpack:2,local:1`) | **`affine:3`** | `binpack:2,local:1` |
| **Q11 `join-2`** | **`binpack:2,local:1`** (4/4) | **`affine:3`** | **`binpack:2,local:1`** (4/4) |
| Q07 `join-4`, Q19 `join-2`, Q20 `join-15` | `binpack:3` / `binpack:2,local:1` | `binpack:*` (`probe_affine=false`, floor fallback) | same as A |

**44/44 `probe_affine=true` dispatches produced `placement=affine:3`. Zero exceptions.** Q11
`join-2` and Q02 `join-4` are the ADR-0008-amendment case made visible: locality drags exactly one
task per batch onto the broadcast build's producer (`local:1`) in A **and in C**, and only (a)+(b)
together turn it into `affine:3`.

> **The deploy summary's `affine:14` / `affine:10` / `affine:9` counts are an artefact.** That
> histogram charged each dispatch the *first* `published tasks` line after it, and a query's
> leaf-scan fan-out is published between the previous query's last `dispatchComputeStage` and the
> next one — so a 14-task `scan-0` fan-out got charged to the preceding `join-2`. Matching on
> `count == num_tasks` removes it. Suite-wide, A places 1 328 scan-fan-out tasks `affine` and B/C
> 1 344; that +16 is a *task-count* difference (A's `scan-0` fan-out is 13 tasks, B/C's 14; A's
> `exchange-repartition-7-src` 10, B/C's 9) from the per-deploy rendezvous draw. See §5.

`scan-affinity byte-balance` fires on the new affine probe sets in B only, and shows why they need
it: Q08/Q09 `join-6` and Q17 `join-2` over lineitem (17.78 GB) shed 7 files, `max_share`
1.42 → 1.09; **Q10 `join-2` (812.7 MB) draws `max_share_before=3.00`** — every probe file on one
owner — and sheds 3 to reach 1.44. The floor guard fires on Q07 `join-4`, Q19 `join-2`,
Q20 `join-15`: `probe_affine=false`, even split + binpack, byte-identical to A.

---

## 3. Mechanism (a): the base-table peer ledger

Per-transfer `base-table cache: peer fetch` lines (exact timestamps; B/C only — the line does not
exist at 9a8b564, so A is the ±60 s per-minute `base-table cache stats` ledger):

| arm | r1 | r2 | r3 | r4 | suite |
|---|---|---|---|---|---|
| A base | 163 / 39.46 GB | 35 / 9.06 GB | 0 | 12 / 3.13 GB | 210 / 51.65 GB |
| **B cand** | **42 / 4.98 GB / 97.7 s** | **0** | **0** | **0** | **42 / 4.98 GB** |
| C affoff | 145 / 34.17 GB / 253.9 s | 56 / 15.36 GB / 69.4 s | 5 / 1.26 GB / 2.4 s | 0 | 206 / 50.78 GB |

By table, the removal is precisely the probe files: **lineitem** 126 transfers / 35.6 GB in C → 7 /
2.1 GB in B, **orders** 28 / 7.0 GB → 0, **partsupp** 18 / 5.8 GB → 1 / 0.4 GB. What survives in B
is 10 customer + 10 part + 10 supplier + 2 nation + 2 region — the late-materialisation gathers and
small broadcast builds the design note said would remain. S3 `misses` are 89/99/94 in run 1 and
**0 in every steady run of every arm**; `evictions=0` everywhere, so nothing is re-read.

---

## 4. Acquisition tiers on the join fragments

Σ acquisition wall over all logged `fragment task phases` lines on `join-*` stages (Σ 3 workers;
the 5 s log floor means every run-1 and steady-run probe-split task is present):

| run | A base | B cand | C affoff |
|---|---|---|---|
| 1 (cold) | **59.6 s** — 100 % `acq_prefetch` | **6.3 s** — `acq_prefetch` | **12.2 s** — 100 % `acq_basecache` |
| 2 | **12.0 s** — `acq_prefetch` | **0.0 s** | 2.8 s — `acq_basecache` |
| 3 / 4 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| scan-* run 1 | 273.7 s (246.2 pf / 0.0 bc) | 247.3 s (195.3 pf / 27.2 bc) | 256.0 s (216.1 pf / 16.4 bc) |

Task level, Q09 `join-6` (3 tasks, all runs, `src_ms` / tier):

| | A base | B cand | C affoff |
|---|---|---|---|
| r1 | src 10.8–11.0 s, `acq_prefetch_files=3–5 ms=9 234–10 154` | src 1.36–1.38 s, `acq_basecache_files=1 ms=0`, `acq_prefetch_miss=1` | src 0.93–1.01 s, `acq_basecache_files=1–2 ms=335–594`, `acq_prefetch_miss=1–2` |
| r2–r4 | src 0.68–4.12 s, `acq_basecache ms=0–1` | src 1.06–1.69 s, `acq_basecache ms=0` | src 0.74–1.86 s, `acq_basecache ms=0–1` |
| max task | 17.45 / 9.15 / 7.70 / 6.98 | **6.82 / 6.76 / 6.92 / 6.98** | 8.28 / 7.56 / 7.07 / 7.61 |

Q08 `join-6` max task: A 18.42 / 20.06 / 13.38 / 13.70 → B 13.62 / 13.66 / 13.43 / 14.00,
C 14.49 / 13.41 / 13.23 / 14.27. Q17 `join-2` max task: A 12.60 / 5.94 / 5.82 / 6.01 →
B 5.56 / 5.90 / 5.41 / 5.88, C 5.65 / **7.08** / 6.08 / 5.54 (C's r2 outlier is its 56-transfer
peer minute, `acq_basecache_ms` 847/945/1 005 on the three tasks).

**Straggler census** — tasks whose **total** acquisition wall ≥ 3 000 ms, any tier, any stage. (The
deploy summary's 28/20/18 filtered on `acq_prefetch_ms` alone, which by construction cannot see a
stall that migrated to `acq_basecache` — every C firing did.)

| arm | run 1 | runs 2–4 | run-1 Σ | run-1 join-* Σ | run-1 scan-* Σ |
|---|---|---|---|---|---|
| A base | 32 | **2** (Q08 `join-6` r2, 12.0 s) | 330.8 s | **57.1 s** | 273.7 s |
| B cand | 24 | **0** | 253.6 s | **6.3 s** | 247.3 s |
| C affoff | 25 | **0** | 263.6 s | **7.6 s** | 256.0 s |

Both cand arms reach the predicted "zero probe-split firings in steady runs". **Only B reaches it
without moving the bytes**: C's steady runs still carry 61 peer transfers / 16.6 GB — the transfer
happens, the prefetcher just overlaps it. That distinction is invisible in wall and decisive on wire.

---

## 5. ★ Attribution of the cold win: not (a); (c), with (b) unmeasured

From §1's cold column, the win is three queries: **Q09 −10.67 (B) / −8.88 (C), Q17 −6.66 / −6.42,
Q08 −4.77 / −3.83** — Σ −22.10 and −19.13, i.e. 107 % and 83 % of each arm's total cold delta
(everything else nets +1.40 and −4.02, largest single items Q01 +2.07/−1.27, Q02 −1.75/−0.82,
Q12 −1.26/−0.75). **C has (a) off and gets it. So the cold win is not (a).**

**Between (b) and (c), the evidence names (c):**

1. **(b) has no independent footprint in this window.** Scan fan-out placement counts are
   *identical* between A and C on the tier that (b) reorders: `local` 149 in A, `local` 149 in C
   (B 160). A leaf scan carries an affinity hint and no locality hints, so the two orders agree;
   the affine counts differ (1 328 vs 1 344) only through the per-deploy task-count draw (§2).
   And no task in arm C carries `AffinityWorkerID` on a probe-split stage — every one is
   `binpack:3`/`binpack:2,local:1` — so (b) has nothing to act on there. (b)'s only observable
   effect in this window is the `local:1 → affine:3` flip on Q11 `join-2` / Q02 `join-4`, which
   requires (a) to supply the hint.
2. **(c) leaves a direct, per-task signature and it is present in exactly the arms that win.**
   In A every cold join fragment resolves at `acq_prefetch` (59.6 s, zero base-cache). In C every
   one resolves at `acq_basecache` with `acq_prefetch_miss` set and **zero** `acq_prefetch_files`
   (12.2 s) — the prefetcher's `Get` populated the cache, the post-`Get` `HasCachedPath` re-probe
   returned `skipped`, and the consumer mmapped the cache file in place
   (`internal/worker/scan_prefetch.go:239`, `internal/worker/stream_source.go:507-565`). The tier
   migrated; the transfer did not disappear (C still moved 34.2 GB peer in run 1).
3. **The removed cost is the second copy plus its window, not the transfer.** Pre-fix the take
   blocks until `streamToSpill` has written and fsynced a 283 MB copy behind a
   `scanPrefetchWindowBytes = 256 MB` window *smaller than one file*, so the prefetcher cannot run
   ahead and every file serializes on the consumer's critical path; post-fix it returns as soon as
   the cache holds the object and the 4-way `scanPrefetchConcurrency` pipeline covers the next file
   behind the current one's decode. Corroboration: block profile `filePrefetcher.fetch` cum
   **0.55 hr (A) → 0.38 (B) / 0.39 (C)** — B and C together, as (c) predicts, not B alone —
   with `BaseTableCache.Put` unchanged (0.92 / 0.97 / 0.93 hr; the spool still happens), nvme_write
   604.8 → 531.2 / 549.6 GB and nvme_read 470.2 → 296.1 / 418.8 GB.

**Quantified split of the join-fragment cold stall (Σ 3 workers, run 1): 59.6 s (A) → 12.2 s (C),
i.e. 47.4 s = 89 % is (c); 12.2 → 6.3 s, i.e. 5.9 s = 11 % is (a).** In steady runs (a) takes the
rest: A 12.0 s / C 2.8 s / **B 0.0 s**.

**Open residual — the B-vs-C cold gap (C is 2.45 s faster cold).** The three arms are three
deploys with different worker IDs, hence different rendezvous draws on the same catalog. The
lineitem draw: A `max_share_before=1.26` shed 4 files, **B 1.42 shed 7**, C 1.12 shed 1. Cold Q01
is the whole-table S3 first touch and its wall tracks the worst worker's byte share — B's
`0413b8` ran four `scan-0` tasks at 19.4–19.8 s of prefetch against C's worst at 16.0–18.0 s, and
Q01 r1 is B 25.69 / A 23.62 / C 22.35. That is a mechanism, but it is a **confound of any
cross-deploy cold comparison** and it is not removable without a fixed worker-ID assignment.

---

## 6. Q18 — `final_aggregate-7`, unchanged, still w3 lever #2

`final_aggregate-7` (24 tasks, `placement=binpack:16,local:8` — **identical string in all three
arms**, so untouched by this commit):

| arm | max task / Σ task-s, r1 / r2 / r3 / r4 | tail run(s) | tail worker |
|---|---|---|---|
| A base | 3.43/66.6 · **5.54**/67.3 · **5.84**/69.4 · 3.08/65.2 | r2, r3 | `046249` (Σ 24.1), `0e163f` (Σ 25.5) |
| B cand | 3.56/67.5 · 3.41/65.3 · **6.37**/72.7 · **6.11**/70.0 | r3, r4 | `0911a7` (Σ 29.3, two tasks 6.37+5.80), `0413b8` (Σ 25.2) |
| C affoff | 2.98/65.4 · 3.30/67.0 · **5.86**/69.1 · 3.01/64.9 | r3 | `089331` (Σ 24.3) |

Same shape as w3 §2.6 and w2: **an intra-stage tail on one worker** — Σ task-seconds +6–11 %, max
task roughly doubling, the run's two slowest tasks on the same box. Incidence over three steady
runs: A 2, B 2, C 1 — flat; Q18 steady means 13.92 / 13.95 / 12.98. **Untouched by this commit and
still the second-largest bimodal source in the suite.**

---

## 7. What moved the other way

Steady-mean regressions ≥ +0.2 s, B vs A: Q22 +0.64, Q07 +0.49, Q04 +0.29, Q05 +0.29, Q14 +0.27,
Q10 +0.25, Q19 +0.24, Q15 +0.21 (Σ +2.68). **Every one of them also appears in C**
(Q04 +0.35, Q22 +0.35, Q14 +0.24, Q21 +0.22, Q19 +0.18, Q07 +0.16, Σ +2.4), so none of them is (a).

* **Q22 (+0.64) is a coordinator stall, and it is pre-existing.** The wall equals the worker
  envelope, but cluster-*idle* inside the envelope is A 0.02 s / B 0.56 s / C 0.22 s. Located
  exactly: B r4, `shuffle side complete` for `exchange-repartition-3` at 22:08:35.939, both
  dependencies satisfied, and `scalar substitution stage_id=join-4 placeholder=scalar_1
  producer=final_aggregate-6` does not log until 22:08:36.995 — **1.06 s of whole-cluster idle
  waiting for the coordinator to read one 80-byte scalar-subquery result out of S3**
  (`src→dispatch` +0.54 s in B r3, +1.06 s in B r4, +0.55 s in C r3, ≤ +0.04 s in all six A runs;
  `dispatch→first exec` ≤ 0.02 s everywhere). There are exactly 3 scalar-substitution sites per run
  — Q11 `final_aggregate-8`, Q15 `join-9`, Q22 `join-4` — and those are precisely the three queries
  with nonzero cluster-idle in **every** arm (Q11 0.40/0.39/0.22, Q15 0.53/0.51/0.51); total
  cluster-idle per steady suite run A 1.5 s, B 2.1 s, C 1.7 s. Which site draws the long wait
  varies; the wait is structural. **New lever, §9.3.**
* **Q07 (+0.49) is its `join-4` probe-split stage — the floor-fallback one.** `probe_affine=false`
  in B, so placement and slicing are byte-identical to A's (`binpack:3`, 4/4 runs). Σ task-s over
  its 3 tasks A 2.0/4.2/2.6 vs B 1.1/5.5/4.2 (r2/r3/r4), max task A 0.79/2.07/1.12 vs B
  0.46/2.99/2.14, C 0.53/1.61/2.08; span A 1.35 s / B 1.91 / C 1.44. Both cand arms are up, and no
  acquisition line fires (every task < 5 s), so the phases ledger cannot see inside it.
  **Open residual** — owed: a phases line on sub-floor `probe_split=true` tasks.
* **Q04/Q05/Q10/Q14/Q15/Q19 (+0.2 to +0.35)** have no single max-task mover; steady-mean
  task-seconds move with them (Q04 +2.97, Q14 +0.62, Q15 +0.62, Q19 +1.12 task-s), so this is work,
  not tail. Suite task-seconds are A 1 078.4 / B 1 064.8 / C 1 087.4 — the group sits inside a ±2 %
  band with the cand arms on opposite sides of A. **Open residual**; no per-stage mechanism is
  visible in this window's instrumentation.

---

## 8. Sanity

* **88/88 query executions `OK` in every arm** (264 total), zero failures. **Row counts identical**
  across all 12 runs and equal to the SF100 baseline (Q02 = 100, Q09 = 175, Q11 = 92 698,
  Q16 = 27 840, Q20 = 17 971).
* **Value signatures identical** for 21 of 22 queries across all 12 runs. Q19 shows the known
  last-digit float-order flicker (`c0:5.985878903e+08` vs `…904e+08`), once in each arm, in a
  different run each time — ADR-0013's legal-nondeterminism class, not a defect.
* **Zero retries** (`attempt>=2` = 0 lines), zero task failures (`success=false` = 0), zero reaps,
  zero `peer_fallthroughs` / `readthrough_fails` / `upload_failed`, zero spills, zero
  `ErrInputLost` / `MissingInputKey`, zero timeouts — all three arms.
* **Zero `durable_waits`** on every `fragment task phases` line in all three arms (the field prints
  when > 0 and escalates the task to INFO) — as in w3.
* Warning mix unchanged and the same in all arms (`accounting drift high/critical`,
  `dataplane connection ended`, `decoded-rowgroup cache pressure shed`); 3 coordinator `ERROR`s per
  arm, all the NATS `Client parser ERROR, state=0` each worker emits at connect.
* **ClickBench was not run this window** — single-node, and nothing in `6f4905e` touches a
  single-process path. Owed at the next release tag, not per window.

---

## 9. Verdict and levers

### 9.1 Keep every default

**Ship `6f4905e` with `WADJET_PROBE_SPLIT_AFFINITY`, `WADJET_AFFINITY_BEFORE_LOCALITY` and
`WADJET_PREFETCH_CACHE_SKIP` all default-on.**

* **Wall:** cold −20.7 s (−10.5 %); steady −2.1 s (−1.4 %), the Q08/Q09/Q17 group −2.6 s of it.
  Small at steady state, and honestly so — w3 already banked most of the wall.
* **Wire:** **−46.1 GB of inter-node traffic per suite (−31 %)**, all of it (a), all in the
  base-table tier, and **−12.2 GB of it in steady runs, where A and C keep re-drawing transfers
  B has made unnecessary**; cache duplication halves (316 → 148 resident entries). On a cluster
  whose known failure mode is ENA-credit starvation this is the result of the window, and it is
  invisible in the mean wall.
* **Variance:** steady-run probe-split stragglers 2 → 0; Q09 `join-6` max task 6.76–6.98 s across
  all four runs of B against A's 6.98–17.45. The w3 "Q09 lottery" is closed as a mechanism.
* **Correctness:** rows and value signatures identical, zero failures, zero retries (§8).
* No arm justifies a flip: C is A's wire profile at B's wall, which is strictly worse.

### 9.2 The next window needs two arms this one could not run

| arm | switch | question it answers |
|---|---|---|
| **D** | `WADJET_PREFETCH_CACHE_SKIP=0` (worker) | Confirms §5's 89 % attribution of the cold win to (c). Expect the join-fragment cold acquisition to return to `acq_prefetch` at ~50–60 s and cold Q08/Q09/Q17 to return toward A. |
| **E** | `WADJET_AFFINITY_BEFORE_LOCALITY=0` (coordinator) | Bounds (b), currently **unmeasured**. Expect Q11 `join-2` / Q02 `join-4` to fall back to `binpack:2,local:1` and the rest to be unchanged; if it is more than that, this window under-credited it. |

Both switches exist in the working tree; both need a deploy, not code. Run them in the **same
window** as a fresh A/B — §5's B-vs-C cold gap is a per-deploy rendezvous draw and cross-window
cold comparison is not sound.

### 9.3 Updated lever queue

1. **★ New: give scalar-subquery substitution the treatment `ed83bb9` gave gather-merge inputs.**
   §7 measures **1.5–2.1 s of whole-cluster idle per steady suite run, in every arm**, spent with
   the coordinator blocked reading an 80-byte producer output out of S3 while both dependencies are
   already satisfied — 1.06 s on Q22 r4 alone, three sites per run (Q11, Q15, Q22). A peer hint or
   a local read there should take it to zero, as the merge-input hint took 13.45 s → 0.11 s (w3 §3.1).
2. **Stop bucketing the Q18/Q20 unbounded sinks** (w2 lever #4, w3 lever 2, unchanged). §6:
   `final_aggregate-7` still draws a one-worker tail in 5 of 9 steady runs across the three arms,
   worth Q18 −1.5 to −2.3 s **and** the removal of the last bimodal source in the suite.
3. **The remaining un-owned base-table reader is the late-materialisation gather and the broadcast
   build.** B's residual 42 peer transfers / 5.0 GB are all customer/part/supplier/nation/region —
   small, but the last base-table bytes crossing the wire, placed without reference to residency.
4. **Pool the scan row-group output backing** (`readRowGroupNative.func2` / `readColumnNative` /
   `newVectorFromColumn` / `ColumnPageReader`) — w3 lever 3, still the #1 named heap-lock caller.
5. **Instrumentation owed:** a `fragment task phases` line on sub-5 s `probe_split=true` tasks
   (§7 could not see inside Q07 `join-4`), and per-stage `dependency-satisfied → dispatch` timing
   on the coordinator (§7 had to reconstruct it from log-line timestamps).
