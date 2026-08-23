# SF100 window-5 six-arm analysis — row-bound layout, coordinator peer stage reads, and a corrected cold-win attribution (2026-08-23)

**Window:** 2026-08-23 00:11–01:33 UTC, six sequential EC2 deploys (one per arm, `tofu destroy`
between), coord c7g.2xlarge + 3× c7gd.4xlarge, `sf100-distributed.tfvars`, SF100 TPC-H,
runs = 4 per arm (run 1 cold). Same hardware and configuration as windows 3 and 4.

| arm | engine | switch | run id | dir |
|---|---|---|---|---|
| **A base** | `9a8b564` (v0.17.0-clawback) | — | `20260823-001136` | `results/w5base/` |
| **B cand** | `550bb20` | — | `20260823-002630` | `results/w5cand/` |
| **C rowboff** | `550bb20` | `WADJET_TWO_LEVEL_ROW_BOUND=0` | `20260823-004028` | `results/w5rowboff/` |
| **D peeroff** | `550bb20` | `WADJET_COORD_PEER_READS=0` | `20260823-005414` | `results/w5peeroff/` |
| **E skipoff** | `550bb20` | `WADJET_PREFETCH_CACHE_SKIP=0` | `20260823-010819` | `results/w5skipoff/` |
| **F afloff** | `550bb20` | `WADJET_AFFINITY_BEFORE_LOCALITY=0` | `20260823-012229` | `results/w5afloff/` |

Coordinator banner sha verified per deploy (A `[9a8b564]`, B–F `[550bb20]`, `vcs.modified=false`);
for C–F the switch value was read back out of the live coordinator's `/proc/<pid>/environ`, and for
E out of a worker's too. `WADJET_PROBE_SPLIT_AFFINITY` is **on in all six arms**, A included by
absence (the switch does not exist at `9a8b564`; its behaviour there is "off").

> **Mapping validation.** Each arm's journal carries exactly 88 `stage-DAG dispatch` lines in
> Q01..Q22 order; worker tasks bind to (run, query) through `executing task
> query_id=st-<stage>-<qhash>`. The worker envelope (first task start → last completion) matches the
> harness wall within **±0.25 s on all 22 queries in every arm** (Σ(wall − envelope) per steady
> suite: A −0.2, B −0.4, C −0.2, D −0.7, E −0.6, F −0.3 s).

---

## 0. What `550bb20` contains, and what each arm isolates

| | change | side | switch | isolated by |
|---|---|---|---|---|
| **(a)** | probe-split affinity `6f4905e` — one task per rendezvous owner, `Task.AffinityWorkerID` | coordinator | `WADJET_PROBE_SPLIT_AFFINITY` | (on everywhere; w4 arm C) |
| **(b)** | affinity tier ahead of locality in `pickWorkerFor` | coordinator | `WADJET_AFFINITY_BEFORE_LOCALITY` | **F** |
| **(c)** | prefetcher skips its spill copy when `Get` populated the base-table cache | worker | `WADJET_PREFETCH_CACHE_SKIP` | **E** |
| **(2)** | lever 2 — row-bound aggregate layout at construction (`28e8c08`+`4399715`) | worker | `WADJET_TWO_LEVEL_ROW_BOUND` | **C** |
| **(4)** | lever 4 — tiered coordinator stage reads kv→peer→s3 (`421f0e3`+`550bb20`) | coordinator | `WADJET_COORD_PEER_READS` | **D** |

Window 4 could not isolate (b) or (c); this window does, and it re-opens w4 §5's attribution,
which was drawn from an arm set where (a) and (c) were never varied independently — §2.

---

## 1. Totals — the four metric families

### 1.1 Wall

| | A base | B cand | C rowboff | D peeroff | E skipoff | F afloff |
|---|---|---|---|---|---|---|
| r1 / r2 / r3 / r4 | 204.96 / 156.40 / 145.09 / 141.99 | 167.81 / 137.52 / 140.14 / 138.44 | 168.12 / 140.24 / 139.53 / 140.58 | 171.34 / 141.80 / 142.02 / 144.51 | 165.69 / 136.03 / 138.25 / 138.85 | 170.00 / 139.19 / 148.12 / 143.88 |
| **cold (r1)** | 204.96 | **167.81 (−37.15)** | 168.12 | 171.34 | **165.69** | 170.00 |
| **steady mean (r2–4)** | 147.82 | **138.70 (−6.2 %)** | 140.12 (+1.42) | 142.78 (+4.08) | **137.71 (−0.99)** | 143.73 (+5.03) |
| **mode-normalised** (Σ best of r2–4) | 139.79 | 134.87 | 134.40 | 137.17 | **133.67** | 137.45 |
| Σ excess over own mode, per steady run | **8.03** | **3.84** | 5.72 | 5.61 | 4.05 | 6.28 |
| suite `total_seconds` | 648.43 | 583.91 | 588.48 | 599.67 | 578.81 | 601.19 |
| suite task-seconds, steady mean (Σ 3 w) | 1 072.5 | **1 036.8** | 1 072.8 | **1 036.8** | 1 031.7 | 1 049.1 |

**Tail.** Straggler census on **total** acquisition wall, any tier, any stage (≥ 3 000 ms — the
deploy summary filtered `acq_prefetch_ms` alone, which by construction cannot see a stall that
migrated to `acq_basecache` or `acq_s3`):

| arm | r1 firings | r1 Σ | r1 scan-* Σ | r1 join-* Σ | r2–4 firings | r2–4 Σ |
|---|---|---|---|---|---|---|
| A base | 30 | 305.4 s | 247.5 s | **57.9 s** | **2** (Q08 `join-6` r2) | **12.5 s** |
| B cand | 22 | 251.1 s | 241.8 s | **9.3 s** | 0 | 0 |
| C rowboff | 23 | 259.5 s | 250.1 s | 9.4 s | 0 | 0 |
| D peeroff | 27 | 260.1 s | 253.7 s | 6.5 s | 0 | 0 |
| E skipoff | 22 | 250.6 s | 240.7 s | 9.9 s | **1** (Q03 `scan-5` r2, `acq_s3` 6 967 ms — a base-table file first touched in run 2) | 7.0 s |
| F afloff | 22 | 243.9 s | 234.2 s | 9.8 s | 0 | 0 |

Bimodal queries, steady spread (max − min over r2–4): **Q08** A **7.54 s** (22.77 / 15.61 / 15.23),
F **5.78 s** (15.52 / 21.30 / 15.80), B 1.10, C 0.48, D 0.62, E 0.96. **Q11** A **1.62 s**
(4.35 / 3.68 / 2.73), all cand arms ≤ 0.61. A's Q08 mode is an acquisition straggler
(`acq_prefetch_ms` 5 573 / 6 926 on two tasks); F's is not (§5).

### 1.2 Per-query steady mean (r2–4), seconds

| Q | rows | A | B | C | D | E | F | B−A | C−B | D−B | E−B | F−B |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Q01 | 4 | 3.660 | 3.618 | 3.626 | 3.535 | 3.452 | 3.651 | −0.04 | +0.01 | −0.08 | −0.17 | +0.03 |
| Q02 | 100 | 4.358 | 4.033 | 4.395 | 4.147 | 3.971 | 4.641 | −0.33 | +0.36 | +0.11 | −0.06 | +0.61 |
| Q03 | 10 | 8.840 | 9.398 | 8.964 | 9.070 | 9.540 | 8.890 | +0.56 | −0.43 | −0.33 | +0.14 | −0.51 |
| Q04 | 5 | 5.436 | 5.326 | 5.327 | 5.235 | 5.286 | 5.434 | −0.11 | +0.00 | −0.09 | −0.04 | +0.11 |
| Q05 | 5 | 6.036 | 6.084 | 6.042 | 6.004 | 5.875 | 5.945 | +0.05 | −0.04 | −0.08 | −0.21 | −0.14 |
| Q06 | 1 | 0.908 | 0.898 | 0.937 | 0.773 | 0.777 | 0.853 | −0.01 | +0.04 | −0.13 | −0.12 | −0.04 |
| Q07 | 4 | 4.740 | 4.425 | 4.185 | 4.785 | 4.121 | 4.876 | −0.32 | −0.24 | +0.36 | −0.30 | +0.45 |
| Q08 | 2 | 17.867 | 16.215 | 15.444 | 15.985 | 15.701 | 17.540 | **−1.65** | −0.77 | −0.23 | −0.51 | **+1.33** |
| Q09 | 175 | 15.168 | 14.488 | 14.608 | 14.910 | 14.550 | 15.395 | −0.68 | +0.12 | +0.42 | +0.06 | +0.91 |
| Q10 | 20 | 11.152 | 10.690 | 11.185 | 11.548 | 11.348 | 11.633 | −0.46 | +0.50 | +0.86 | +0.66 | +0.94 |
| Q11 | 92698 | 3.586 | 2.564 | 2.589 | 2.979 | 2.710 | 2.660 | **−1.02** | +0.02 | **+0.42** | +0.15 | +0.10 |
| Q12 | 2 | 5.461 | 4.829 | 4.782 | 4.827 | 4.911 | 4.907 | −0.63 | −0.05 | −0.00 | +0.08 | +0.08 |
| Q13 | 100 | 5.563 | 5.677 | 5.570 | 5.846 | 5.467 | 5.696 | +0.11 | −0.11 | +0.17 | −0.21 | +0.02 |
| Q14 | 1 | 1.963 | 1.974 | 1.907 | 1.953 | 1.854 | 2.069 | +0.01 | −0.07 | −0.02 | −0.12 | +0.10 |
| Q15 | 1 | 2.727 | 2.109 | 2.040 | 2.612 | 1.595 | 1.580 | −0.62 | −0.07 | **+0.50** | −0.51 | −0.53 |
| Q16 | 27840 | 5.557 | 5.361 | 5.713 | 5.484 | 6.026 | 6.048 | −0.20 | +0.35 | +0.12 | +0.67 | +0.69 |
| Q17 | 1 | 6.675 | 5.671 | 5.559 | 6.147 | 5.665 | 6.003 | **−1.00** | −0.11 | +0.48 | −0.01 | +0.33 |
| Q18 | 100 | 12.329 | 10.616 | 12.100 | 10.613 | 10.587 | 10.538 | **−1.71** | **+1.48** | −0.00 | −0.03 | −0.08 |
| Q19 | 1 | 3.738 | 3.706 | 3.662 | 3.763 | 3.503 | 3.617 | −0.03 | −0.04 | +0.06 | −0.20 | −0.09 |
| Q20 | 17971 | 9.190 | 9.278 | 9.355 | 9.394 | 9.143 | 9.507 | +0.09 | +0.08 | +0.12 | −0.14 | +0.23 |
| Q21 | 100 | 10.096 | 8.999 | 9.323 | 10.029 | 8.891 | 9.558 | **−1.10** | +0.32 | **+1.03** | −0.11 | +0.56 |
| Q22 | 7 | 2.775 | 2.742 | 2.806 | 3.138 | 2.738 | 2.692 | −0.03 | +0.06 | **+0.40** | −0.00 | −0.05 |
| **Σ** | | **147.82** | **138.70** | **140.12** | **142.78** | **137.71** | **143.73** | **−9.12** | **+1.42** | **+4.08** | **−0.99** | **+5.03** |

### 1.3 CPU

| | A | B | C | D | E | F |
|---|---|---|---|---|---|---|
| worker CPU-s, whole suite (Σ 3 w) | 12 844.2 | 12 418.1 | 12 446.1 | 12 351.6 | 12 363.9 | 12 525.0 |
| **worker CPU-s per suite run** | **3 211.1** | **3 104.5** | 3 111.5 | 3 087.9 | 3 091.0 | 3 131.3 |
| ↳ utilisation of 48 vCPU | 41.0 % | 44.0 % | 43.8 % | 42.5 % | **44.2 %** | 43.1 % |

CPU-s per run drops 3.3 % from A to B while wall drops 6.2 %: part of B's win is work removed
(lever 2, §3) and part is wait removed (§2, §4) — which is why utilisation rises.

### 1.4 Network (suite = 4 runs, Σ 3 workers; bytes GiB)

| | A | B | C | D | E | F |
|---|---|---|---|---|---|---|
| base-table **peer** transfers / bytes | 206 / **47.20** | 41 / **4.38** | 39 / 3.82 | 41 / 4.27 | **36 / 2.96** | 41 / 4.33 |
| base-table **S3 miss** GETs / bytes | 92 / 22.53 | 98 / 24.25 | 98 / 24.76 | 102 / 24.29 | 107 / 25.81 | 100 / 24.49 |
| base-table **readthrough** reads / bytes | 23 / 3.71 | 17 / 2.61 | 17 / 2.09 | 18 / 3.04 | 13 / 1.53 | 21 / 2.77 |
| shuffle **peer** files / bytes | 43 039 / 90.53 | 44 138 / 90.50 | 43 128 / 90.68 | 44 237 / 92.58 | 44 269 / 91.63 | 44 152 / 92.01 |
| shuffle **s3** files / bytes · `peer_fallthroughs` | 0 / 0 · 0 | 0 / 0 · 0 | 0 / 0 · 0 | 0 / 0 · 0 | 0 / 0 · 0 | 0 / 0 · 0 |
| S3 `upload_done` PUTs / bytes | 32 613 / 102.27 | 30 167 / 94.19 | 29 175 / 92.53 | 30 870 / 97.88 | 28 964 / 93.83 | 30 244 / 97.16 |
| **Σ inter-node (peer) bytes** | **137.72** | **94.88 (−31 %)** | 94.50 | 96.86 | 94.59 | 96.34 |
| nvme read / write (GiB) | 426.0 / 560.8 | 278.1 / 483.0 | 283.5 / 479.5 | 272.3 / 490.0 | 258.0 / 491.4 | 275.9 / 491.3 |
| base-table cache resident at suite end | 312 / 71.45 | 147 / 28.64 | 145 / 28.08 | 147 / 28.53 | 142 / 27.22 | 147 / 28.59 |

The −42.8 GiB of inter-node traffic is entirely base-table peer bytes and entirely (a)'s (shuffle
peer bytes are flat to ±2 %). What survives in every cand arm is the same set — customer / part /
supplier / nation / region, 34 of B's 41 transfers, each replicated to two peers — plus 1–4
lineitem / orders / partsupp files. **E moved zero lineitem base-table bytes all suite.**

### 1.5 Heap

| | A | B | C | D | E | F |
|---|---|---|---|---|---|---|
| alloc per suite run (GiB, Σ 3 w) | 1 838.7 | **1 798.9** | 1 806.4 | 1 796.8 | **1 793.5** | 1 801.2 |
| heap-lock delay (`runtime.mallocgc` cum in mutex prof), s/suite | 305.8 | **277.0** | 279.1 | 299.6 | 280.3 | 288.6 |
| ↳ per worker-run | 25.5 | **23.1** | 23.3 | 25.0 | 23.4 | 24.0 |
| total mutex delay, s/suite | 692.7 | **617.0** | 656.1 | 721.1 | 656.2 | 696.8 |
| GC cycles (Σ 3 w, suite) | 1 454 | 1 427 | 1 441 | 1 442 | 1 427 | 1 442 |
| GC pause total, ms (Σ 3 w, suite) | 7 208 | 8 770 | 8 794 | 8 087 | 8 716 | 7 735 |
| peak `alloc_mb` (max worker) | 15 236 | 15 179 | 16 873 | 15 133 | 16 048 | 16 699 |

The named heap-lock caller is unchanged and still #1: `scan.readRowGroupNative.func2` holds
**105.9 s of B's 277.0 s** of heap-lock delay (A 140.8, C 116.6, D 115.2, E 107.1, F 102.5) — lever
3's target, not in these binaries. GC pause total is dominated by one worker per arm (B 4 651 /
2 777 / 1 342 ms) and does not separate the arms.

---

## 2. ★ Cold-win attribution, revisited — (a) and (c) are redundant on the probe fragments

Window 4 attributed its −20.7 s cold win **89 % to (c)** (per-task tier migration from
`acq_prefetch` to `acq_basecache`) and **11 % to (a)**, from an arm set in which (a) was off in the
arm that isolated (c) and (c) was on in both cand arms. This window varies them the other way, and
the conclusion changes.

**Run-1 `join-*` acquisition wall by query (ms, Σ 3 workers, from `fragment task phases`):**

| arm | Q02 `join-19` | Q08 `join-6` | Q09 `join-6` | Q17 `join-2` | Σ |
|---|---|---|---|---|---|
| A base | 11 598 (pf) | 17 804 (pf) | 16 231 (pf) | 12 236 (pf) | **57 869** |
| B cand — (a)+(c) | 9 337 (pf) | **0** | 4 (bc) | **0** | **9 341** |
| C rowboff | 9 367 (pf) | 0 | 0 | — | 9 367 |
| D peeroff | 6 481 (pf) | 0 | 0 | 0 | 6 481 |
| **E skipoff — (a) only, (c) OFF** | 9 853 (pf) | **0** | **0** | **0** | **9 853** |
| F afloff | 9 753 (pf) | 0 | 0 | 0 | 9 753 |

The residual in every cand arm is **entirely Q02 `join-19`**, the one probe-split stage whose
byte-balance floor guard fires (`probe_affine=false` in all six arms), so it is placed by binpack
and its probe files do cross the wire. The three **affine** stages — Q08/Q09 `join-6`, Q17 `join-2`
— cost **46.3 s of acquisition in A and 0.0 s in E**.

**Per-task, arm E, run 1** ((c) off, so the prefetcher would pay the double copy if it had to
transfer): every Q08 `join-6` task and both logged Q09 `join-6` / Q17 `join-2` tasks open with
`acq_prefetch_miss=1 acq_basecache_files=1 acq_basecache_ms=0`, zero `acq_prefetch_files`, `src_ms`
49–56 ms on Q08. There is no transfer for the double copy to make cheap: (a) placed the task on the
file's rendezvous owner, the leaf scan had already put that file in that worker's base-table cache
earlier in the same run, and the take resolves against the local cache at 0 ms. The ledger agrees —
**zero lineitem base-table peer transfers in E for the whole suite** (§1.4).

**Corrected attribution.** (a) and (c) are **redundant cold mechanisms on probe-split fragments**:
(a) removes the transfer, (c) makes a transfer that happens cheap. Alone:

| mechanism, alone | run-1 `join-*` acquisition Σ | base-table peer bytes, suite |
|---|---|---|
| neither (w5 A) | 57.9 s | 47.2 GiB |
| **(c) only** (w4 C: (a) off, (c) on) | 12.2 s — **−47.4 s**, all tier-migrated to `acq_basecache` | 50.8 GiB — **unchanged** |
| **(a) only** (w5 E: (a) on, (c) off) | 9.9 s — **−48.0 s**, and 0.0 s on the three affine stages | 3.0 GiB — **−94 %** |
| both (w5 B) | 9.3 s | 4.4 GiB |

(The two "only" rows come from different windows and therefore different rendezvous draws — w4's
bytes are as published there, in GB — so the *wire* columns are not directly subtractable; the
acquisition columns are, because both are measured against their own window's `9a8b564` arm, which
agrees to 3 % across the two windows: 59.6 s and 57.9 s.)

Window 4's "89 % (c)" was measuring a saving that (a) also delivers by itself, and delivers more
completely: (c) leaves 12.2 s of base-cache waits and every byte on the wire; (a) leaves neither.
Both windows are consistent — either lever suffices for the cold wall, only (a) touches the wire.

**Neither lever is measurable on the cold scan class**, which is bandwidth-bound first touch:
run-1 `scan-*` acquisition Σ is A 247.5 / B 241.8 / C 250.1 / D 253.7 / E 240.7 / F 234.2 s, and (c)
off (E) shows the tier signature exactly — **zero** `acq_basecache` on run-1 scans against B's 24.3 s
and C's 12.5 s — with no total-wall difference.

**Recommendation: both stay default-on.** (a) only covers probe-split probe files. (c) covers every
reader that still crosses the wire — the late-materialisation gathers and broadcast builds that are
34 of B's 41 remaining transfers, and cold first touch — and window 4 measured what it is worth when
a transfer does happen. This window cannot re-measure that, because (a) removed the transfers it
would have applied to.

---

## 3. Lever 2 — row-bound aggregate layout (C isolates it)

Engagement is unambiguous: `agg_row_bound_total`/`agg_row_bound_tasks` appear on 32 dispatches in
all five `550bb20` arms (0 in A) and are byte-identical between B and C — **the coordinator emits
the bound in C too; the switch acts on the worker.** The worker-side marker is
`two_level_born_flat`, a per-task delta on `task completed`:

| | A base | C rowboff | B cand | D | E | F |
|---|---|---|---|---|---|---|
| tasks with born-flat / Σ born-flat | 92 / 874 | 93 / 920 | **881 / 9 225** | 874 / 9 238 | 878 / 9 247 | 870 / 9 183 |
| stages carrying it | 3 shuffle stages only | 3 shuffle stages only | + all 7 `final_aggregate-*` (5 833 of them on `final_aggregate-7`) | same | same | same |

**`final_aggregate-7` (Q18 site, 48 task completions per run), per-run:**

| arm | Σ task-s r1 / r2 / r3 / r4 | mean task ms | max task s | per-worker Σ span |
|---|---|---|---|---|
| A base | 64.1 / 66.1 / 64.6 / 66.3 | 1 336–1 380 | 3.06–3.24 | 0.61–1.24 |
| C rowboff | 65.9 / 67.1 / 63.8 / 64.1 | 1 330–1 398 | 3.02–3.06 | 0.17–1.24 |
| **B cand** | **46.6 / 45.4 / 47.3 / 47.3** | **946–985** | **2.03–2.52** | 0.50–1.80 |
| D / E / F | 45.5–46.9 / 45.9–47.4 / 46.0–46.7 | 949–987 | 1.98–2.19 | 0.11–1.02 |

Q18 wall follows exactly: A 12.24/12.21/12.66/12.12, C 12.06/12.21/11.62/12.47, B 10.85/10.50/10.85/10.50,
D/E/F 10.10–10.87 — **−1.7 s of Q18, the largest single per-query effect in the window, and the whole
of C−B's +1.42 s steady penalty.** Q20's site `final_aggregate-12` (26 tasks, bound 19.5 M rows) is
**unmoved**: Σ 9.0–10.3 s per run in *every* arm, Q20 steady 9.14–9.51 s across all six.

**The w3 §2.6 / w4 §6 Q18 one-worker tail did not recur in any arm this window**, C and A included:
no run has a max task near the 5.5–6.4 s those windows recorded, and the cross-worker Σ span is
≤ 1.80 s on a 15–22 s base everywhere. So this window cannot test whether the row bound removes that
tail; what it shows is the stage moving down wholesale — **−29 % task-seconds, −33 % max task**.

**Where the saving is: CPU, not heap.** Worker CPU profiles (Σ 3 workers, whole suite):

| symbol | A base | C rowboff | B cand |
|---|---|---|---|
| `convertIntHashTableToTwoLevel` | 70.2 s | 71.5 s | **absent** |
| `(*intTwoLevelTable).GetOrInsertAt` | 65.9 s | 66.9 s | **absent** |
| `(*intHashTable).GetOrInsert` (flat probe) | 20.0 s | 20.0 s | 85.9 s |
| **Σ int-aggregate probe + convert** | **156.0 s** | **158.4 s** | **85.9 s (−70.1 s/suite, −17.5 CPU-s/run)** |

Heap: allocation under `convertIntHashTableToTwoLevel` goes **4 564 MB (A) / 4 617 MB (C) → 10 MB (B)**
per suite, i.e. −1.14 GiB per run. But suite-level `allocPackedSubEntries` is **flat** (A 41.1, B 41.7,
C 41.3, D 40.8, E 42.5, F 41.8 GiB) and `(*intHashTable).allocEntries` is flat to 0.05 % (249.2 GiB
everywhere): the 41 GiB of two-level sub-entry allocation belongs to the *packed* (multi-key) tables
in the shuffle partial aggregations, which the row bound does not reach. **Lever 2 is a CPU lever
that happens to save 1.1 GiB/run, not a heap lever.**

---

## 4. Lever 4 — coordinator peer stage reads (D isolates it)

The tier engaged exactly as `docs/design/coordinator-stage-reads.md` predicted, and the new
`tier=`/`wait_ms=` attributes settle the mechanism the design note could only infer:

| arm | tiers over the 12 substitution sites | Σ wait_ms |
|---|---|---|
| A base | (no telemetry at `9a8b564`) | — |
| B cand | peer × 11, kv × 1 | **40** |
| C / E / F | peer × 11–12, kv × 0–1 | 45 / 27 / 11 |
| **D peeroff** | **s3 × 12** | **4 635** |

**The gap is the read.** Measuring `last preceding stage-complete → scalar substitution` on the
coordinator journal reproduces `wait_ms` to the millisecond in every arm (D r3 Q11: gap 0.578 s,
`wait_ms=577`; D r2 Q15: 0.547 s / 547). D's twelve s3-tier waits fall on exactly three values:
**38–61 ms** (the `Get` answers first try), **542–577 ms** (one 500 ms re-poll plus the GET), and
**1 060 ms** (two). That confirms the design note's inferred quantization: the cost is
`fetchStageOutputData`'s 500 ms re-poll loop missing a poll while the producer's async S3 upload
lands. A's untiered gaps sit on the same grid (1.582 / 1.092 / 0.547 / 0.545 / 0.544 s).

**Whole-cluster idle inside the worker envelope, steady mean per run** (zero tasks running anywhere):

| Q | A | B | C | D | E | F |
|---|---|---|---|---|---|---|
| Q11 | 0.924 | 0.007 | 0.015 | **0.411** | 0.018 | 0.008 |
| Q15 | 0.562 | 0.009 | 0.010 | **0.564** | 0.015 | 0.011 |
| Q22 | 0.024 | 0.013 | 0.006 | **0.404** | 0.010 | 0.008 |
| Q21 | 0.364 | 0.341 | 0.365 | 0.373 | 0.353 | 0.362 |
| **total** | **2.203** | **0.539** | 0.591 | **1.951** | 0.576 | 0.564 |

A → B removes **1.66 s per steady run** of whole-cluster idle; D puts **1.41 s** of it back. The
per-query walls agree to within 0.01 s: Q11 +0.42, Q15 +0.50, Q22 +0.40 = **+1.31 s/run** of D−B.
Q21's 0.34–0.37 s is present in every arm including B and is not a scalar site — it is structural
and unexplained, unchanged since w4.

**The other +2.77 s/run of D−B is an open residual.** Suite task-seconds are **identical** between
D and B (1 036.8 both), so it is not work at the suite level; wall − envelope is ≈ 0 on every query
in both arms, so it is not a post-envelope coordinator read; no stage shows an acquisition or
straggler signature. Mode-normalised, D−B is only **+2.31 s**, so about 1.8 s/run of the steady-mean
gap sits in tail rather than mode (D's excess-over-own-mode is 5.61 s/run against B's 3.84). The
largest single non-scalar item, Q21 +1.03 s/run, does carry +3.57 task-seconds of its own, spread
over `scan-0` (+1.84), `join-4` (+1.78) and `join-12` (+1.28) with no named mover inside them.
**Instrumentation owed:** `readFinalResults` → `fetchResultData` is the second coordinator-side read
site the tier now serves and it emits **no tier/wait line at all** (grep finds exactly 12 `tier=`
lines per arm, all scalar). Until it does, half of lever 4's surface is unobservable and this
residual cannot be attributed or excluded.

---

## 5. Affinity-before-locality (F) — no placement footprint, and an unexplained +5.03 s

**(b) changed nothing that this window can see in placement.** Matching every `published tasks` line
to its dispatch by `count == num_tasks` (516 of 668 lines match; the rest are leaf fan-outs published
between dispatches — the w4 §2 artefact):

* Placement token totals, suite: **F = affine 1 020 / binpack 3 983 / local 229 — identical to C**,
  the arm with (b) **on**. (A 924/4 090/218, B 1 020/4 006/206, D 1 056/3 968/208, E 1 056/3 979/197.)
* Per stage, **C and F differ on 2 of 131 placement keys**, both a single run flipping
  `binpack:3` ↔ `binpack:2,local:1` (Q02 `join-4`, Q22 `final_aggregate-6`) — the same run-to-run
  variation A shows internally.
* Probe-split placements are **byte-identical between B and F**: both draw 32 `probe_affine=true`
  dispatches, both leave Q11 `join-2` and Q02 `join-4` at `binpack:2,local:1` and Q20 `join-13`/`join-15`
  at binpack. The two flips w4 credited to (b) (`local:1 → affine:3` on Q11 `join-2` / Q02 `join-4`)
  **cannot occur in this window's B**, because B's rendezvous draw tripped the byte-balance floor
  guard on exactly those stages, so there was no affinity hint for (b) to reorder.
* The 32-vs-44 `probe_affine=true` split across arms ({B, C, F} = 32, {D, E} = 44) is a per-deploy
  rendezvous draw, not a switch effect: it does not track any switch, and the two arms sharing the
  "better" draw are the fastest (E, −0.99 vs B) and the slowest-but-one (D, +4.08 vs B).

**F's +5.03 s is therefore not attributable to (b) by this window's evidence.** It decomposes as
+2.44 s of tail and +2.59 s of mode:

* **The r3 outlier (148.12 s) is one task.** F r3's excess over F's own per-query mode is 10.67 s, of
  which **Q08 = 5.78 s**. It is **not** a straggler by the census (zero acquisition firings in F r2–4):
  worker `03e07f`'s Q08 `join-6` task ran 19.21 s in r3 against 13.60 / 13.83 / 13.88 s in its other
  three runs on **identical input** — same 1 463 449 rows, same 49 643 morsels, same `k=15` width,
  `src_ms=267`, `acq_basecache_ms=0`. `process_ms` inflated 146 178 → 228 240 (+56 %) and
  `width_wait_ms` 6 260 → 11 810. No GC signature (`gc_delta` 22, `gc_pause_delta_ms` 26 in that
  30 s tick). The one coincident signal is I/O: that minute is the worker's **peak NVMe minute of
  the suite** (22.3 GiB written vs 12–20 in adjacent minutes, 12.2 GiB read) with `psi_io_some`
  6 865 ms against a 3 085–3 500 ms baseline. Causal direction is not settled — the task's own sink
  writes into that minute — so this is an **open residual** with a named signature: *single-task
  compute inflation, identical input and width, zero acquisition wait, no GC, coincident with peak
  local NVMe pressure.*
* **The mode half is diffuse.** Largest per-stage steady task-second diffs F−B are Q09 `join-10`
  +3.27, Q03 `join-4` +2.77, Q10 `join-10` +2.54 — all 24-task partitioned stages, ≤ +0.14 s per
  task, with offsetting negatives (Q01 `scan-0` −1.98, Q15 `join-9` −1.55). **Open residual.**

**E−B = −0.99 s steady / −1.20 s mode-normalised is also unattributed.** No stage-level diff exceeds
1.2 s over a 24-task stage, and there is no mechanism by which turning (c) *off* can remove work — it
can only add a copy. It is not evidence for flipping the default.

---

## 6. Q19 value-signature flicker — ADR-0013 class 9

Q19's `c0` reads `5.985878903e+08` in 22 of 24 runs and `5.985878904e+08` in **E r3 and E r4**. This
is the known last-digit float-order flicker: a `SUM` of `DOUBLE` over a partitioned aggregate whose
merge order is not fixed, differing in the final ULP. **ADR-0013's legal-nondeterminism class 9
(floating-point summation order), not a defect** — row counts and all other value signatures are
identical across all 24 runs (§7). It appeared once in each arm in window 4 and twice in one arm
here; the incidence carries no arm signal.

---

## 7. Sanity

* **88/88 query executions `ok:true` in every arm** — 528 total, zero `ok:false`, zero FAILED /
  ERROR / panic lines in any per-query file. **Row counts identical** across all 6 arms × 4 runs ×
  22 queries (0 mismatches) and equal to the SF100 baseline: Q02 = 100, Q09 = 175, Q11 = 92 698,
  Q16 = 27 840, Q20 = 17 971. **Value signatures identical** for 21 of 22 queries; Q19 as in §6.
* **Zero** retries (`attempt>=2`, coordinator and workers), task failures (`success=false`), reaps,
  `ErrInputLost` / `MissingInputKey`, `peer_fallthroughs`, `readthrough_fails`, `upload_failed`,
  shuffle-s3 bytes, evictions, and `durable_waits` — all six arms. **Zero spills**: the only `spill`
  matches in the worker logs are the NVMe scratch directory path in the startup banner.
* Worker `level=ERROR`: 0. Coordinator `level=ERROR`: the same 3 per arm, all the NATS
  `Client parser ERROR, state=0` each worker emits at connect.
* **ClickBench not run** — single-node; nothing in `550bb20` touches a single-process path. Owed at
  the next release tag, not per window.

---

## 8. Verdict

### 8.1 Defaults — keep all five on

| lever | switch | evidence | verdict |
|---|---|---|---|
| (a) probe-split affinity | `WADJET_PROBE_SPLIT_AFFINITY` | §2: −48.0 s of run-1 join acquisition alone; −42.8 GiB inter-node; base-table peer 206 → 41 transfers | **keep** |
| (b) affinity before locality | `WADJET_AFFINITY_BEFORE_LOCALITY` | §5: no placement footprint in this window (F token-identical to C); F's +5.03 s is tail + diffuse mode, unattributed | **keep** (no evidence to flip; it is the tier order (a) needs when the floor guard does not fire) |
| (c) prefetcher cache-skip | `WADJET_PREFETCH_CACHE_SKIP` | §2: redundant with (a) on probe fragments, worth −47.4 s alone (w4) on readers (a) does not cover | **keep** |
| (2) row-bound layout | `WADJET_TWO_LEVEL_ROW_BOUND` | §3: Q18 −1.7 s, `final_aggregate-7` −29 % task-seconds, −70 CPU-s/suite, −1.14 GiB alloc/run; no query regresses | **keep** |
| (4) coordinator peer reads | `WADJET_COORD_PEER_READS` | §4: −1.66 s/run of whole-cluster idle, tier `peer` on 11/12 sites, `wait_ms` 40 total vs 4 635 | **keep** |

### 8.2 Arc scoreboard — v0.17.0 (`9a8b564`, arm A) → main (`550bb20`, arm B), same window

| family | v0.17.0 | main | Δ |
|---|---|---|---|
| **wall** steady mean / cold / mode-normalised | 147.82 / 204.96 / 139.79 s | **138.70 / 167.81 / 134.87 s** | **−6.2 % / −18.1 % / −3.5 %** |
| **wall** tail: straggler firings r1 / r2–4; Σ excess over mode | 30 / 2; 8.03 s per run | **22 / 0; 3.84 s per run** | −8 / −2; −52 % |
| **CPU** worker CPU-s per suite run (utilisation) | 3 211.1 (41.0 %) | **3 104.5 (44.0 %)** | −3.3 % (+3.0 pt) |
| **network** Σ inter-node bytes; base-table peer transfers; S3 GETs / PUTs | 137.72 GiB; 206; 92 / 32 613 | **94.88 GiB; 41; 98 / 30 167** | **−31 %**; −80 %; +6 / −7.5 % |
| **heap** alloc per run; heap-lock delay per worker-run; GC cycles | 1 838.7 GiB; 25.5 s; 1 454 | **1 798.9 GiB; 23.1 s; 1 427** | −2.2 %; −9.4 %; −1.9 % |

### 8.3 Next window

1. **★ Lever 3 — scan output backing reuse** (`7890eb5`/`2858f9b`/`be5fcf1`, `WADJET_SCAN_BACKING_REUSE`),
   merged after this window. It is the only lever aimed at the largest named heap-lock caller:
   `readRowGroupNative.func2` holds **105.9 s of B's 277.0 s** of heap-lock delay and
   `batch.NewVectorWithScale` is 30 % of all allocation. Its arm needs **heap + mutex profiles as
   primary metrics** — the wall effect may be under the mode spread, and alloc GiB/run paired with
   heap-lock delay s/worker-run is the reading that matters. Baseline for it is arm B of this window:
   alloc 1 798.9 GiB/run, heap-lock 277.0 s/suite, `readRowGroupNative.func2` 105.9 s, GC 1 427 cycles.
2. **Give `readFinalResults` a tier/wait line** (§4). Half of lever 4's read surface is invisible, and
   D's +2.77 s/run residual cannot be attributed or excluded without it.
3. **Q21's 0.34–0.37 s of whole-cluster idle per run, in every arm including the baseline** (§4).
   It is not a scalar site and has survived three windows unexamined; it is now the largest
   remaining structural idle in the suite.
4. **A phases line on sub-5 s tasks** (owed since w4 §9.3). §2's zero-acquisition claim for the
   affine stages rests on the peer ledger because five of the twelve cand-arm probe-split tasks per
   run never cross the 5 s logging floor.
5. **Name the Q08 one-task compute-inflation mode** (§5). It fired once in F (r3, +5.78 s) and is
   distinct from the acquisition mode (a) closed; the NVMe-pressure coincidence is a lead, not a
   finding, and it needs a per-morsel timing breakdown to settle.
