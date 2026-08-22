# SF100 window-3 five-arm analysis — 2026-08-22

**Window:** 2026-08-22 05:38–07:10 UTC, one EC2 deploy, coord c7g.2xlarge + 3× c7gd.4xlarge,
SF100 TPC-H, runs = 4 per arm (run 1 cold). Same hardware and same day as windows 1 and 2
(`docs/benchmarks/sf100-window-analysis-2026-08-22.md`, `…-window2-…`), referenced as **w1** / **w2**.

| arm | engine | switch | run id | dir |
|---|---|---|---|---|
| **A base** | `23abd8e` (v0.16.0-correctness) | — | `20260822-053854` | `results/w3base/` |
| **B cand** | `69aecbb` (waves 1+2+3) | — | `20260822-055421` | `results/w3cand/` |
| **C waitoff** | `69aecbb` | `WADJET_DURABLE_WAIT_BACKOFF=0` + `WADJET_INTERM_PEER_HINTS=0` | `20260822-060827` | `results/w3waitoff/` |
| **D reuseoff** | `69aecbb` | `WADJET_VECTOR_REUSE=0` | `20260822-062217` | `results/w3reuseoff/` |
| **E prefetchoff** | `69aecbb` | `WADJET_PREFETCH_AT_INIT=0` | `20260822-063617` | `results/w3prefetchoff/` |

Binaries verified from each arm's `coord-benchmark.log` NATS banner: base `[23abd8e]`, B–E `[69aecbb]`.

Working files: `scratchpad/w3.py` (per-task loader), `scratchpad/coord3.py` (coordinator
placement / affinity), `scratchpad/decode3.py` (periodic counter deltas), `scratchpad/diff3/`
(merged worker `{cpu,block,mutex,heap}` profiles + `cp.py` / `mx.py` / `hp.py`).

> **Mapping validation.** Every worker task is bound to a (run, query) by the coordinator's
> `stage-DAG dispatch` order; cross-checked against the harness `query_completed` walls, the
> task envelope agrees within 0.6 s for **351 of 352** (arm, run, query) cells in A–C (the miss is
> cand Q17 r2, 14.63 vs 13.57, where a gather starts before the query clock).

---

## 0. ★ Arm C was not a switch arm — the bench cannot turn off a coordinator-side toggle

`WADJET_INTERM_PEER_HINTS` is read on the **coordinator**
(`internal/coordinator/peer_locations.go:33`, at `optswitch.Register` time, from that process's env).
The bench's `extra_env` seam reaches the **workers only**:

```
deploy/benchmark/terraform/main.tf:686-691   # worker launch script
    # Generic env-arm seam (var.extra_env): systemd-run --scope workers
    # inherit this shell's environment, so exports here reach the worker
    # binary -- /etc/environment does NOT (see comment above).
    export ${k}="${v}"

deploy/benchmark/terraform/main.tf:359-361   # build script (coordinator AND workers)
    echo "${k}=${v}" >> /etc/environment      # <- no export; never enters the process env
```

`coordinator_user_data` (main.tf:464–583) exports ~40 named `WADJET_*` vars and **does not contain
the `extra_env` loop**. Confirmed against the live `terraform.tfstate` user-data for the arm
deployed while this was written: the coordinator's script carries only
`echo "WADJET_VECTOR_REUSE=0" >> /etc/environment`; each worker's carries the `echo` **and**
`export WADJET_VECTOR_REUSE="0"`.

**Behavioural proof that the hint stayed ON in arm C.** The interm inputs are the only S3 reads
the shuffle-IO ledger records, and they vanish in *every* `69aecbb` arm, C included:

| arm | `s3_files` (Σ 3 workers, 4 runs) | `s3_bytes` | gather-merge Σ per steady run |
|---|---|---|---|
| A base | **425** | 354 700 B (≈835 B/file) | **13.45 s** |
| B cand | **0** | 0 | 0.11 s |
| **C waitoff** | **0** | 0 | **0.10 s** |
| D reuseoff | **0** | 0 | 0.14 s |
| E prefetchoff | **0** | 0 | 0.19 s |

Of C's two switches, `INTERM_PEER_HINTS=0` never reached the process that reads it, and
`DURABLE_WAIT_BACKOFF=0` (worker-side, so it *did* apply) had nothing to act on: with the hint on,
`awaitDurableObject` is never entered — `durable_waits` is absent from **every** `fragment task
phases` line in all four `69aecbb` arms (the field prints whenever > 0, and
`srcAcqStats.notable()` escalates such a task to INFO regardless of the 5 s log floor).

**C is a replicate of B, not a switch arm.** D and E, whose switches are worker-side, *are* real
A/Bs — `prefetch_started_before_build` is 39/39 in D and **0/39** in E, so E's switch demonstrably
took effect.

*Action:* add the `extra_env` loop to `coordinator_user_data`, or every coordinator-side kill
switch will silently no-op in a bench A/B.

---

## 1. Totals

| | A base | B cand | C waitoff | D reuseoff | E prefetchoff |
|---|---|---|---|---|---|
| suite wall r1 / r2 / r3 / r4 | 210.3 / 185.6 / 175.8 / 166.4 | 179.4 / 162.9 / 152.9 / 153.3 | 188.3 / 159.8 / 141.4 / 144.8 | 198.6 / 153.8 / 144.3 / 145.2 | 199.5 / 158.6 / 145.7 / 150.7 |
| **steady mean (r2–r4)** | **175.9** | **156.4** (−11.1 %) | 148.7 | 147.7 | 151.7 |
| steady mean, excl. Q07/08/09/17 | 118.7 | 105.3 | 101.1 | 103.0 | 106.7 |
| steady mean, only Q07/08/09/17 | 57.2 | 51.0 | 47.6 | 44.8 | 45.0 |
| mode-normalised (best steady run per query) | 162.1 | 141.3 | 137.6 | 138.5 | 141.6 |
| **suite task-seconds, steady mean** | **1 193.7** | **1 090.6** | **1 086.8** | **1 086.8** | **1 098.4** |
| worker CPU per suite run | 3 303.0 s | **3 188.5 s** (−3.5 %) | 3 184.4 s | 3 185.0 s | 3 204.0 s |
| utilisation of 48 vCPU | 39.1 % | 42.5 % | 44.6 % | 44.9 % | 44.0 % |
| mutex delay, 3 w × 4 runs | 1 471.9 s | **790.9 s** (−46 %) | 633.1 s | 948.2 s | 624.5 s |
| block (parked) total | 270 741 s | 251 657 s | 259 618 s | 254 230 s | 257 387 s |
| alloc_space, 4 runs | 8 618.8 GB | **7 518.8 GB** (−12.8 %) | 7 564.9 GB | 8 561.1 GB | 7 569.3 GB |
| GC cycles (Σ `gc_delta`) | 1 701 | 1 466 | 1 448 | 1 623 | 1 473 |

**The four `69aecbb` arms did the same work to within 1 %** (suite task-seconds 1 086.8–1 098.4).
Their suite walls spread by 8.7 s. That spread is tail, not work — §2.

---

## 2. ★ THE Q09 QUESTION — verdict: **lottery, not causal**

### 2.1 The whole Q09 delta is one stage, `join-6`

Every other Q09 stage is identical across arms and runs (scan-1 ≈0.3–0.9 s, the two repartitions
≈0.9 s, join-10 3.5–3.8 s span, join-14 2.6–2.8 s span, merge ≈0.01 s in B–E). `join-6`, the
3-task broadcast join, max task duration per run:

| arm | r1 | r2 | r3 | r4 | Q09 steady mean |
|---|---|---|---|---|---|
| A base | 18.82 | 15.57 | 9.25 | 8.47 | 20.6 |
| **B cand** | 6.60 | **15.55** | **14.68** | 8.55 | 20.7 |
| C waitoff | 6.71 | 7.85 | 7.62 | 7.58 | **15.6** |
| **D reuseoff** | **16.86** | **14.68** | 7.17 | 7.14 | 17.6 |
| **E prefetchoff** | 6.64 | **15.78** | 7.13 | 6.75 | 17.9 |

### 2.2 What distinguishes a slow Q09 run: the acquisition tier, and only that

`fragment task phases` + `morsel parallel fragment done` for Q09 `join-6`, all five arms:

| | fast mode | slow mode |
|---|---|---|
| `rows` | 10 769 438 / 10 774 669 / 11 092 440 | **identical** |
| `dispenser_parents` | 772 / 795 | **identical** |
| `dispenser_morsels` | 45 081 / 46 461 | **identical** |
| `process_ms` (Σ 15 consumers) | 27 846 – 30 991 | 30 089 – 33 129 |
| `ops_ms` | 26 343 – 31 201 | 27 389 – 31 201 |
| **`acq_basecache_files` / `_ms`** | **1 / 0–1 ms** | 0–1 / 0 ms |
| **`acq_prefetch_files` / `_ms`** | **0 / —** | **2–4 / 5 803–9 344 ms** |
| `acq_peer_files`, `acq_s3_files` | 0, 0 | **0, 0** |
| `prefetch_started_before_build` / `lead_ms` | 1 / 410–612 | **1 / 449–612** (B/C/D), 0 / 0 (E) |
| `durable_waits` | absent (0) | **absent (0)** |
| `src_ms` | 361 – 2 503 | **6 602 – 10 970** |
| `consumer_dry_wait_ms` (Σ 15 consumers) | 57 125 – 90 340 | **156 682 – 193 123** |

**Same rows, same row groups, same morsels, same work.** The only thing that changes is where the
file opens resolve: base-table NVMe cache at 0–1 ms, or the file prefetcher at 2–3 s per file. The
15 consumers then starve — `consumer_dry_wait_ms` doubles — and the task's wall doubles.

It is not the durable wait (0 everywhere, in every arm), not the peer tier (`acq_peer_files` = 0),
not placement (§2.4), and not prefetch-at-Init (the lead is 450–610 ms in the slow tasks that have
it, and E's slow tasks have none at all yet behave identically).

### 2.3 Incidence is flat across all five arms — the mode is a draw

Census of every task in the window with `acq_prefetch_ms ≥ 3 000`:

| arm | total firings | in r1 (cold) | in steady r2–r4 | which steady firings |
|---|---|---|---|---|
| A base | **31** | 28 | 3 | Q09 r2 ×1, Q17 r2 ×2 |
| B cand | **30** | 24 | 6 | **Q09 r2 ×2, Q09 r3 ×1**, Q17 r2 ×2, Q08 r4 ×1 |
| C waitoff | **29** | 26 | 3 | Q08 r2 ×2, Q17 r2 ×1 |
| D reuseoff | **31** | 29 | 2 | **Q09 r2 ×2** |
| E prefetchoff | **31** | 28 | 3 | **Q09 r2 ×3** |

Five arms, **29–31 firings each (±3 %)**. What varies is which query the draw lands on. Three
arms with three different switches nominally off — C (a no-op), D (vector reuse), E
(prefetch-at-Init) — and Q09 draws the slow mode in D and E as readily as in B. C is the only arm
that escaped it, and C is configurationally identical to B.

### 2.4 Placement and dispatch are constant

* `published tasks` for Q09 `join-6`: `placement=binpack:3`, `spread=<w>:1,<w>:1,<w>:1` in **every
  arm and run** — one task per worker, no clumping. (The bench does run
  `WADJET_LOCALITY_PLACEMENT=1` and `WADJET_AFFINITY_BYTE_BALANCE=1`, `sf100-distributed.tfvars:57`;
  `pickLocalityWorkerFrom` co-locates only when every hint on a task names one worker, which never
  holds for a 3-way probe split.)
* `fused/probe-split broadcast cache load` for `join-6` is byte-identical in all 20 (arm × run)
  instances: `num_tasks=3 fused_count=2 primary_cache_files=1 fused_cache_files_total=10
  cluster_cache_reads_estimate=33`.
* `scan-affinity byte-balance` for Q09's two source stages is **constant within each arm across
  all four runs** (cand 2+2 shed files every run, waitoff 1+2, prefetchoff 1+1, reuseoff 3+2) —
  so shedding is deterministic per deploy and is not the run-to-run variable either.
* Merge/`-interm-` placement is irrelevant: those stages run *after* `join-6` and take 0.00–0.08 s
  in B–E.

### 2.5 Peer-serve contention: not a factor

The hint moved 425 files per 4 runs onto the peer tier — **835 bytes each, 354.7 kB total**:

| | base | B cand | C waitoff | D reuseoff |
|---|---|---|---|---|
| shuffle `peer_files` / `peer_bytes` | 39 572 / 91.9 GB | 42 483 / 96.7 GB | 44 903 / 99.3 GB | 43 744 / 96.1 GB |
| shuffle `s3_files` / `s3_bytes` | 425 / 354.7 kB | 0 / 0 | 0 / 0 | 0 / 0 |
| `peer_fallthroughs` | 0 | 0 | 0 | 0 |
| base-table-cache `peer_serves` | 209 (51.1 GB) | 206 (50.7 GB) | 196 (47.4 GB) | 192 (47.2 GB) |

The interm traffic is **0.25 % of peer files and 0.0004 % of peer bytes**; base-table peer serving
is unchanged (209 → 206); no fetch ever fell through. There is no contention here to find.

### 2.6 Q18 — a second, independent draw

The B-vs-C gap on Q18 (13.9 vs 12.0 steady) is **not** `repartition-11`, the born-flat partial
aggregate, which is fixed identically in all four `69aecbb` arms (3.34–4.46 s span against base's
10.48–11.55 s). It is `final_aggregate-7`, the *unbounded* bucketed aggregate:

| arm | final_aggregate-7 span r1/r2/r3/r4 | Σ task-s r3 | max task r3 |
|---|---|---|---|
| base | 5.07 / 4.94 / 5.17 / 5.26 | 59.2 | 2.69 |
| **B cand** | 5.59 / 5.77 / **8.73** / **7.72** | 70.2 | **5.88** |
| C waitoff | 5.72 / 5.68 / 5.94 / 6.24 | 66.3 | 3.05 |
| D reuseoff | 5.84 / 5.61 / 5.86 / 6.25 | 66.0 | 3.20 |

Σ task-seconds moves 6 %; the max task doubles, and both slow tasks in r3 land on the same worker
(`i-0d83b5`, also the one that drew Q09's slow mode in r2 and r3) — an intra-stage tail on one
worker, not extra work. w2 recorded the identical shape in its cand arm (7.69 / 7.91 in r1/r4
against sibling arms' 5.8): a standing property of the bucketed unbounded sink, **w2 lever #4's
territory**, not a wave-3 effect.

### 2.7 Verdict

**Lottery.** The interm-peer-hint / durable-wait-backoff changes cannot be causing Q09's slow
mode, for four independent reasons, in decreasing order of force:

1. **The hint was on in arm C too** (§0). B and C are the same effective configuration, so the
   B↔C Q09 difference has no switch to attribute it to.
2. **Arms D and E reproduce the Q09 slow mode** with two other, unrelated toggles off — D twice
   (r1, r2), E once but on all three tasks (r2).
3. **The mechanism is in a different subsystem** (§2.2): `acq_prefetch_ms` on the probe-side
   base-table read, with `durable_waits` = 0 and `acq_peer_files` = 0 everywhere. Nothing the hint
   or the backoff touches is on that path.
4. **Incidence is flat** at 29–31 firings across five arms (§2.3).

**Recommendation: `WADJET_INTERM_PEER_HINTS` stays default-on** — it is the largest single wave-3
lever (§3.1, −13.3 s per suite run). **`WADJET_DURABLE_WAIT_BACKOFF` stays default-on but earns no
credit**: it is unexercised, because the hint removes every wait it was built to shorten. It
remains correct as the fallback when a producing peer is gone.

---

## 3. Where B's −11 % comes from, per switch and per query

Steady-mean per-query delta base → cand, decomposed against the gather-merge (interm) stage's own
contribution (max merge-task duration per query per run, steady mean):

| Q | base | cand | Δ | base merge | cand merge | Δ merge | residual |
|---|---|---|---|---|---|---|---|
| Q07 | 9.19 | 4.23 | **−4.95** | 4.45 | 0.01 | −4.44 | −0.51 |
| Q18 | 18.82 | 13.89 | **−4.93** | 0.00 | 0.00 | 0.00 | **−4.93** |
| Q05 | 9.61 | 6.19 | **−3.42** | 3.45 | 0.01 | −3.45 | +0.03 |
| Q04 | 7.14 | 5.31 | −1.83 | 1.77 | 0.04 | −1.73 | −0.10 |
| Q12 | 6.42 | 5.14 | −1.28 | 0.00 | 0.00 | 0.00 | −1.28 |
| Q22 | 3.67 | 2.66 | −1.02 | 0.42 | 0.01 | −0.41 | −0.60 |
| Q01 | 4.31 | 3.53 | −0.78 | 0.14 | 0.01 | −0.13 | −0.65 |
| Q17 | 9.48 | 8.73 | −0.75 | 0.31 | 0.00 | −0.31 | −0.44 |
| Q06 | 1.38 | 0.82 | −0.56 | 0.14 | 0.01 | −0.13 | −0.43 |
| Q08 | 17.93 | 17.37 | −0.56 | 0.99 | 0.01 | −0.98 | +0.42 |
| Q16 | 6.21 | 5.73 | −0.48 | 0.00 | 0.00 | 0.00 | −0.48 |
| Q14 | 2.35 | 1.94 | −0.41 | 0.41 | 0.01 | −0.40 | −0.01 |
| Q03 / Q19 / Q20 / Q21 / Q13 | | | −0.26 / −0.18 / −0.19 / −0.04 / +0.05 | 0 | 0 | 0 | same |
| Q02 | 4.68 | 4.90 | +0.21 | 0.00 | 0.00 | 0.00 | +0.21 |
| Q11 | 3.13 | 3.47 | +0.34 | 0.00 | 0.00 | 0.00 | +0.34 |
| Q15 | 1.97 | 2.42 | +0.44 | 0.00 | 0.00 | 0.00 | +0.44 |
| Q10 | 11.12 | 12.07 | +0.95 | 0.00 | 0.00 | 0.00 | +0.95 |
| Q09 | 20.61 | 20.70 | +0.09 | 1.37 | 0.01 | −1.36 | **+1.45** |
| **Σ** | **175.9** | **156.4** | **−19.55** | **13.45** | **0.11** | **−13.34** | **−6.21** |

### 3.1 Peer hints for gather-merge inputs (`ed83bb9`) — **−13.3 s/suite run, 68 % of the win**

* Merge-stage Σ per steady suite run: **13.45 s (base) → 0.11 / 0.10 / 0.14 / 0.19 s (B/C/D/E)**,
  −99.2 %.
* Per query the wall delta tracks the merge cost 1:1 — Q07 −4.44 of −4.95; Q05 −3.45 of −3.42;
  Q04 −1.73 of −1.83; Q14 −0.40 of −0.41; Q22 −0.41 of −1.02.
* base's own merge durations still show w2's 500 ms grid — 0.57 / 0.62 / 0.70 / 0.93 / 0.97 /
  1.21 / 1.25 / 1.73 / 1.76 / 1.80 / 2.22 / 2.25 / 2.29 / 3.27 / 3.83 / 4.27 / 4.79 — and there is
  nothing left to quantise in B–E.
* Corroborated end-to-end by the ledger: **`s3_files` 425 → 0**, `s3_bytes` 354.7 kB → 0,
  `peer_fallthroughs` 0, `durable_waits` 0.
* **w2 lever #1 delivered, and beyond estimate** (w2 predicted −5 to −8 s from backoff, "up to
  −12 s with hints"; measured −13.3 s from hints alone).

### 3.2 Vector-backing reuse (`69aecbb`, B vs **D** — a real worker-side A/B)

| signal (3 workers × 4 runs) | B cand | D reuseoff | Δ | base |
|---|---|---|---|---|
| `HashJoinProbe.emitViewOutput` alloc **cum** | **47.9 GB** | 1 095.3 GB | **−95.6 %** | 1 089.8 |
| `HashJoinProbe.Execute` alloc **cum** | 100.6 GB | 1 136.3 GB | −91.1 % | 1 130.9 |
| `exec.gatherBuildVector` alloc **cum** | **4.5 GB** | 588.0 GB | **−99.2 %** | 587.2 |
| `BytesColumn.PreAllocBytes` alloc flat | 780.2 GB | 1 362.0 GB | −42.7 % | 1 363.0 |
| `batch.NewColumnVector` alloc cum | 1.8 GB | 243.6 GB | −99.3 % | 242.8 |
| **total alloc_space** | **7 518.8 GB** | 8 561.1 GB | **−1 042 GB = −260 GB / suite run** | 8 618.8 |
| **mutex delay total** | **790.9 s** | 948.2 s | **−157.3 s = −13.1 s / worker / suite run** | 1 471.9 |
| ↳ heap-lock family (`runtime.unlock` + `_LostContended`) | 681.5 s | 838.0 s | −156.5 s | 897.7 |
| ↳ `HashJoinProbe.Execute` mutex cum | **16.8 s** | 194.8 s | −91 % | 201.5 |
| ↳ `emitViewOutput` mutex cum | **6.9 s** | 185.3 s | −96 % | 193.4 |
| ↳ `gatherBuildVector` mutex cum | **0.5 s** | 80.1 s | −99 % | 82.2 |
| GC cycles | 1 466 | 1 623 | −9.7 % | 1 701 |
| worker CPU per suite run | 3 188.5 s | 3 185.0 s | **+0.1 % (flat)** | 3 303.0 |
| suite task-seconds, steady | 1 090.6 | 1 086.8 | +0.3 % (flat) | 1 193.7 |

The commit predicted ~273 GB/run on that path; **measured −260 GB/run**, and the switch restores
it to base's level exactly (8 561.1 vs base 8 618.8). Heap-lock share of all mutex delay: 88.4 %
in D, 86.2 % in B — still #1, but 13 s/worker/run less of it.

> **On D's wall.** D is 8.7 s *faster* than B at steady state while doing identical task-seconds
> with strictly more allocation and more lock waiting. B lost Q09 (+3.1 s vs D) and Q18 (+1.9 s)
> to the two draws in §2.3/§2.6. **Vector reuse is wall-neutral in this window** — as its commit
> predicted — and is justified on allocation, lock delay and GC.

### 3.3 Prefetch-at-Init (`b7c50e8`, B vs **E** — the direct A/B) — worth ~350 ms per join fragment

| arm | class | n tasks | `started_before_build=1` | Σ lead | mean lead | Σ `acq_prefetch_ms` |
|---|---|---|---|---|---|---|
| B cand | join-* | 38 | **38 / 38** | 13.1 s | **344 ms** | 72.4 s |
| C waitoff | join-* | 39 | 39 / 39 | 13.7 s | 350 ms | 70.0 s |
| D reuseoff | join-* | 39 | 39 / 39 | 13.5 s | 346 ms | 77.1 s |
| **E prefetchoff** | join-* | 39 | **0 / 39** | **0.0 s** | **0 ms** | **78.8 s** |
| B / C / D / E | scan-* | 18 / 18 / 20 / 19 | 18 / 18 / 20 / **0** | ≈0 | 2–4 ms / **0** | 235.6 / 228.0 / 235.5 / 229.6 |

* **The switch works and the change fires on 100 % of prefetching tasks**, but the lead it buys is
  **~344 ms**: the take is reached ~350 ms after the prefetcher spawns, so the build load is only
  ~0.35–0.6 s long and that is all the overlap that exists. On scan fragments the lead is 0–14 ms,
  as expected — a scan's first act is to open its file.
* Against E, Σ `acq_prefetch_ms` on join fragments falls 78.8 → 72.4 s over 3 workers × 4 runs
  (**−0.53 s per worker per suite run**), and suite task-seconds fall 1 098.4 → 1 090.6
  (**−7.8 task-s per suite run**, consistent with Σ lead ≈ 3.3 task-s/run plus E's extra draw).
  On the 18 non-bimodal queries E is 106.7 s and B 105.3 s.
* **It does not remove the straggler mode.** E fires 31 times, B 30, base 31 (§2.3), and B's slow
  Q09 tasks carry `prefetch_lead_ms` 461–607 against `acq_prefetch_ms` 6 324–8 822. w2 lever #3
  predicted "kills the Q08/Q09/Q17 slow mode, 7–9 s each time it fires". **Refuted**: the cost is
  the cold transfer, which is bandwidth-bound, not the start moment.
* The cold-run gains once attributed to it are the same draw: base r1 Q09 `join-6` took the
  prefetch tier on all three tasks (`acq_prefetch_ms` 7.9 / 8.3 / 10.3 s); B r1 and **E r1** both
  hit `acq_basecache` at 0 ms and both finished Q09 cold in ~14 s. Q09 cold: base 29.0, B 14.2,
  C 14.4, D 24.3, **E 14.6** — with the switch off.

### 3.4 Durable-wait geometric backoff (`796839e`) — did not fire

`durable_waits` is absent from every `fragment task phases` line in B, C, D and E (the field
prints when > 0 and escalates a sub-5 s task to INFO precisely so it cannot hide). With the hint
landing first, `awaitDurableObject` is unreachable on this workload. **Zero measured effect,
positive or negative.**

### 3.5 Wave-1/2 carry-over, still visible against base

* `repartition-11` (Q18 capped partial aggregate): base span 10.48–11.55 s → 3.34–4.46 s in all
  four `69aecbb` arms. Every partial-agg task still reports `born_flat=true conversions=0
  group_ceiling=2 917 776 flushes=5` on a 50 M-row input.
* expr wave 1: `wrapCompiledFilter` 47.7 → **0** CPU-s, `CmpTemporalLit.EvalBool` 35.7 → **0**,
  replaced by `expr.FilterPredicate.func1/2` 57.7 + 15.9; `ColRef.resolve` 63.3 → 20.2.

---

## 4. CPU / heap / GC — base vs B, with the D and E controls

Flat CPU by package, `-nodefraction=0`, CPU-s over 3 workers × 4 runs:

| package | base | **B cand** | C waitoff | D reuseoff | Δ(B−base) per suite run |
|---|---|---|---|---|---|
| **TOTAL** | **13 212.1** | **12 753.9** | 12 737.7 | 12 739.9 | **−114.5 (−3.5 %)** |
| go-runtime / stdlib | 4 224.0 | 4 151.9 | 4 158.7 | 4 228.7 | −18.0 |
| `internal/engine/exec` | 3 282.2 | 3 148.9 | 3 131.5 | 3 126.9 | −33.3 |
| `compress` (zstd/s2) | 3 023.5 | 2 925.2 | 2 908.8 | 2 873.6 | −24.6 |
| `internal/engine/expr` | 840.0 | 716.1 | 718.7 | 715.2 | −31.0 |
| `internal/engine/batch` | 680.8 | 681.6 | 685.9 | 683.7 | +0.2 |
| `internal/storage/parquet` | 523.6 | 530.2 | 537.9 | 523.7 | +1.7 |
| `internal/worker` | 353.6 | 309.9 | 310.0 | 305.9 | −10.9 |
| `internal/engine/scan` | 280.7 | 286.3 | 282.7 | 278.8 | +1.4 |

Notable frames (CPU-s, 4 runs): `intTwoLevelTable.GetOrInsertAt` 393.7 → 65.6;
`convertIntHashTableToTwoLevel` flat 2.4 → 79.6 (its *cum* moved, not its work);
`wrapCompiledFilter` 47.7 → 0; `CmpTemporalLit.EvalBool` 35.7 → 0; `FilterPredicate.func1/2`
0 → 57.7 / 15.9; `gatherBuildVector` 537.5 → 591.1 (the gather now writes into reused backing —
the CPU is the same copy, the allocation is gone).

**Heap.** alloc_space 8 618.8 → 7 518.8 GB (−12.8 %, **−275 GB per suite run**), essentially all of
it the join-emit path (§3.2). Top allocators on B, per suite run: `NewVectorWithScale` 551.9 GB,
`NewRecordBatch` (cum) 593.7 GB, `readRowGroupNative` (cum) 546.7 GB, `PreAllocBytes` 195.1 GB
(base 340.8), `parquet.DecodeBitPacked` 118.7 GB, `getZstdBuffer` 109.4 GB, `HashJoin.arena`
103.4 GB.

**GC: better, cheaply.** `gc_delta` 1 701 → 1 466 (−13.8 %; D 1 623, so ~10 pts of that is vector
reuse). CPU-side GC frames flat: `madvise` 62.0 → 58.9, `memclrNoHeapPointers` 206.8 → 182.3,
`mallocgc*` +1.7 total, `markrootSpans` 3.0 → 2.1.

---

## 5. Admission counters — now readable, and the policy fires hard

The w2 gap is closed: `4bd828a` put the five fields on the periodic `worker stats` line, 22 samples
per worker per arm. Final cumulative values (per worker, 4 runs):

| arm | capacity | decode_reserve | decode_admits | decode_bypasses | decode_holdbacks |
|---|---|---|---|---|---|
| B cand | 14 | 3 | 662 464 / 669 716 / 655 336 | 85 944 / 91 143 / 91 678 | **232 803 / 248 569 / 247 988** |
| C waitoff | 14 | 3 | 673 552 / 691 529 / 685 315 | 79 369 / 87 251 / 84 031 | 220 411 / 235 523 / 235 970 |

~729 k holdbacks against ~1.99 M admits per arm (3 workers × 4 runs): the policy **holds back
~24 %** of decode-token requests, with ~12 % bypassing the reserve.

Direction vs w2 unchanged — the decoder runs ahead of its consumers rather than behind them
(scan decode-ahead, Σ 3 workers, 4 runs):

| | base | B cand | C waitoff | w1/w2 base | w2 cand |
|---|---|---|---|---|---|
| `token_stall_ms` | 2 379.0 s | **1 510.2 s** (−36.5 %) | 1 579.2 s | 2 540.0 s | 1 513 s |
| `window_full_ms` | 143.8 s | **397.4 s** (+176 %) | 375.1 s | 183.7 s | 384 s |
| `token_stalls` count | 310 543 | 182 825 | 190 676 | — | 181 288 |
| `decode_ms` | 2 807.6 s | 3 079.0 s | 3 133.4 s | 3 220 s | — |
| `decode_bytes` | 584.2 GB | 608.4 GB | 609.3 GB | — | — |

Morsel dispenser, Σ over all fragments (s, 3 workers × 4 runs):

| | base | B cand | C waitoff | D reuseoff |
|---|---|---|---|---|
| scan-* Σ `consumer_dry_wait_ms` | 8 879.9 | **5 824.0** (−34 %) | 5 609.3 | 5 401.8 |
| scan-* Σ `width_wait_ms` | 2 365.9 | **5 313.1** (+125 %) | 5 342.4 | 5 601.4 |
| ALL Σ `process_ms` | 7 649.8 | 6 604.3 (−13.7 %) | 6 486.2 | 6 745.9 |
| ALL Σ fragment `elapsed_ms` | 2 621.9 | 2 501.0 (−4.6 %) | 2 495.3 | 2 490.8 |

Same conversion as w2 §2.3: starvation → token queueing, work unchanged.
`dispenser_producer_wait` is still ~0 (0.6–4.2 s over the whole window).

---

## 6. Wait side, re-ranked on B

Mutex delay 790.9 s over 3 workers × 4 runs = **65.9 s per worker per suite run** (base 122.7,
D 79.0, C 52.8, E 52.0).

| # | site | B cand | share | per worker-run | base | D reuseoff |
|---|---|---|---|---|---|---|
| **1** | **Go heap lock** (`runtime.unlock` + `_LostContendedRuntimeLock`) | **681.5 s** | **86.2 %** | **56.8 s** | 897.7 | 838.0 |
| | ↳ `mheap.allocSpan` | 275.1 | 34.8 % | 22.9 | 493.5 | 466.8 |
| | ↳ `mcentral.grow` | 104.5 | 13.2 % | 8.7 | 167.7 | 162.1 |
| **2** | `sync.Mutex.Unlock` — all application mutexes | 104.9 s | 13.3 % | 8.7 | 570.9 | 102.6 |
| | ↳ `partitionedShuffleSink.*` | 16.7 | 2.1 % | 1.4 | 12.9 | 11.8 |
| | ↳ `unpartitionedStageSink.Consume` | 5.1 | 0.6 % | 0.4 | **462.3** | 1.4 |

Heap-lock **callers** on B, by cum — the join side is fixed, the scan side is what is left:

| caller | B cand | D reuseoff | base | what it allocates |
|---|---|---|---|---|
| `runtime.makeslice` (everything below) | 211.5 s | 413.0 | 423.3 | every `make` > 32 kB |
| **`scan.readRowGroupNative.func2`** | **130.8 s** | 141.9 | 144.9 | per-column decode output |
| ↳ `scan.readColumnNative` | 100.9 | 111.3 | 117.2 | |
| ↳ `batch.NewVectorWithScale` | 61.5 | 124.4 | 130.8 | |
| ↳ `batch.newVectorFromColumn` | 44.0 | 105.6 | 111.5 | |
| `parquet.ColumnPageReader.*` | 94.6 | 104.5 | 109.2 | page buffers |
| `batch.NewRecordBatch` | 43.8 | 43.3 | 45.2 | batch shells |
| `exec.HashJoinProbe.Execute` | **16.8** | 194.8 | 201.5 | **fixed by `69aecbb`** |
| `batch.BytesColumn.PreAllocBytes` | **10.2** | 90.8 | 92.9 | **fixed by `69aecbb`** |
| `exec.gatherBuildVector` | **0.5** | 80.1 | 82.2 | **fixed by `69aecbb`** |

Block profile (parked, s over 3 w × 4 runs) unchanged in shape: `uploadManager.acquireSlot`
123 275 (49 %, background by design), `runtime.selectgo` 211 728, `consumeMorsels` 17 803
(base 19 430), `widthGate.claim` 5 970 (base 3 151 — the admission holdback, as in w2),
`filePrefetcher.take` 675 (base 695), `unpartitionedStageSink.Consume` 257 (base 632).

---

## 7. Sanity

* **Row counts identical** across all five arms: 22 unique (query, rows) pairs, pairwise diffs
  empty. Q02 = 100, Q09 = 175, Q11 = 92 698, Q16 = 27 840, Q20 = 17 971.
* **Value signatures identical except one known float-order case.** 21 unique `vsig` lines per arm
  in A, C, D, E; B has 22 — the extra being `Q19 vsig c0:5.985878904e+08` in one of its four runs
  against `…903` in the other three. Same 1-in-10-significant-digits SUM-order nondeterminism w1
  recorded on Q19; ADR-0013's legal-nondeterminism class, not a defect. The other 21 signatures
  are byte-identical across all 20 runs.
* **Zero task failures** (`success=false` = 0, coordinator and worker journals, all arms).
* **Zero retries** — every coordinator `task result` line is `attempt=1` (5 876–5 984 lines/arm).
* **No peer fetch failures anywhere**: `peer_fallthroughs=0`, `readthrough_fails=0`,
  `upload_failed=0`, all arms.
* **No `ErrInputLost` / `MissingInputKey`**, no spills, no timeouts, no deadline-exceeded.
* Warning mix is the usual one and identical across arms (`accounting drift high/critical`,
  `dataplane connection ended`, `decoded-rowgroup cache pressure shed`); the 3 coordinator
  `ERROR`s per arm are the NATS client parser line each worker emits at connect.
* **ClickBench (single node, `69aecbb`)**: cold 161.5 s / hot 84.6 s vs release 162.3 / 85.2.
  Per-query hot ratios 0.85–1.09 (best q36 −15 %, worst q13 2.44 → 2.67 s). Flat — expected, since
  the eager probe-emit path ClickBench uses is untouched by `69aecbb`.

---

## 8. Attribution, levers, verdict

### 8.1 Per-switch attribution

| change | switch | its own signal | wall |
|---|---|---|---|
| **peer hints for gather-merge inputs** (`ed83bb9`) | `WADJET_INTERM_PEER_HINTS` | merge Σ 13.45 → 0.11 s/run; `s3_files` 425 → 0; `durable_waits` 0 | **−13.3 s per suite run (68 % of −19.6 s)** |
| **born-flat bounded sinks** (`dcc95a8`, w2) | `WADJET_TWO_LEVEL_BORN_FLAT` | Q18 `repartition-11` span 11.0 → 3.9 s; conversions 0; flushes 5 | **−4.9 s (Q18)** |
| **expr wave 1** | — | expr −31.0 CPU-s/run; 2 frames to zero | inside the residual |
| **vector-backing reuse** (`69aecbb`) | `WADJET_VECTOR_REUSE` | alloc −260 GB/run; heap-lock −13.1 s/worker-run; `emitViewOutput` alloc −95.6 %; GC −10 % | **neutral** (D control) |
| **prefetch-at-Init** (`b7c50e8`) | `WADJET_PREFETCH_AT_INIT` | 38/38 join tasks start before build, mean lead **344 ms**; E gives 0/39 | **−7.8 task-s/run, ≈−0.5 s/worker-run**; straggler mode unchanged |
| **durable-wait backoff** (`796839e`) | `WADJET_DURABLE_WAIT_BACKOFF` | **never fired** (0 durable waits, every arm) | — |
| **admission counters** (`4bd828a`) | — | turned §5 into a grep | — |
| decode admission (w2) | `WADJET_DECODE_ADMISSION` | 729 k holdbacks; token_stall −36.5 %, window_full +176 % | neutral (as w2) |

### 8.2 Release verdict on `69aecbb`

**Ship it as v0.17.0 with every switch at its current default. No flip is warranted.**

* **−11.1 % steady suite** against the release (175.9 → 156.4 s); **−16.0 %** mode-normalised
  (162.1 → 141.3); **−3.5 %** worker CPU; **−8.6 %** suite task-seconds; **−12.8 %** allocation;
  **−46 %** worker mutex delay. Runs 3–4 of C/D (141.4 / 144.3 / 144.8 / 145.2 s) are the fastest
  SF100 suite runs recorded, beating the 2026-08-16 record of 155.9 s.
* Rows identical, value signatures identical bar the known Q19 float-order case, zero failures,
  zero retries, zero peer-fetch failures, ClickBench flat.
* **`WADJET_INTERM_PEER_HINTS` default-on** — the largest lever of the wave; the Q09 suspicion
  against it is refuted four ways (§2.7).
* **`WADJET_VECTOR_REUSE` default-on** — wall-neutral, but −260 GB/run of allocation,
  −13 s/worker/run of heap-lock waiting and −10 % GC cycles at zero CPU cost, with the D control
  restoring every number exactly. Keep the switch for the invariance oracle.
* **`WADJET_PREFETCH_AT_INIT` default-on** — E shows it is worth ~7.8 task-s/run and costs nothing;
  it is simply smaller than advertised, and it is not the straggler fix.
* **`WADJET_DURABLE_WAIT_BACKOFF` default-on** — unexercised here, correct as the dead-producer
  fallback; no evidence either way, so no reason to change a safer default.

### 8.3 What the next arc should attack

**The single biggest variance source is now the cold base-table read on broadcast-join probe
fragments** — the "straggler tier". Sized from this window: 29–31 firings per arm, 5.8–9.3 s of
`src_ms` each, 2–3 of them in steady runs, moving 4–6 s of steady suite wall per arm and making
every n=3 arm comparison unreadable (Q07/Q08/Q09/Q17 are 45–57 s of a 148–176 s suite).

**What the evidence now says the mechanism is — and is not.**

*Is not:* the durable wait (0 in every arm), the peer tier (`acq_peer_files` = 0 on every affected
task), placement (`binpack:3`, identical in all 20 arm×run instances), the dispatch decision
(`fused/probe-split broadcast cache load` byte-identical everywhere), affinity shedding
(constant within an arm across all four runs), CPU tokens (`token_stall` on these tasks is
sub-second), the amount of work (same rows, same 772/795 row-group parents, same 45 081/46 461
morsels, same `process_ms`), or the prefetcher's **start moment** (E proves the recoverable
overlap is ~344 ms, because the build load it hides behind is only ~0.35–0.6 s long).

*Is:* **a worker being handed a base-table file it does not hold locally**, and paying a 2–3 s
cold S3 read per file (2–4 files, 5.8–9.3 s per task) while its 15 morsel consumers idle
(`consumer_dry_wait_ms` 57–90 k → 157–193 k). The base-table cache never evicts in this window
(`evictions=0`, 90–106 entries, 21–26 GB, 24–39 misses per worker over four runs), so it is still
*filling*; each remaining miss costs a straggler.

**Two concrete attacks, in order:**

1. **Make probe-split placement cache-aware.** The coordinator already keeps a per-file worker
   registry (it is what `annotateTaskPeerLocations` reads, and what made §3.1 possible) and the
   base-table cache already has a working peer tier (206 `peer_serves`, 50.7 GB, 0 fallthroughs in
   B). Yet `join-6`'s three tasks are placed `binpack:3` with no reference to who holds the probe
   files, and every slow read went to the **prefetch/S3** tier with `acq_peer_files = 0`. The
   analogous fix for stage outputs (`ed83bb9`) was worth −13.3 s/run this window. Two variants,
   cheapest first: (a) hint the probe-split assignment with base-table cache residency, so a file
   goes to the worker that already has it; (b) if it must move, let the miss resolve at the peer
   tier instead of S3 — a sibling's NVMe is ~10× closer than the object store.
   *Owed instrumentation:* the base-table cache miss path does not log whether a sibling held the
   key. One field (`peer_candidate=true/false`) on the miss makes (b) a one-grep decision.
2. **Stop bucketing the Q18/Q20 unbounded sinks** (w2 lever #4, unchanged). `final_aggregate-7` is
   5.6–6.3 s clean, 7.7–8.7 s when it draws a tail, against 4.14 s with the index off — worth
   Q18 −1.5 to −2.3 s **and** the removal of a second bimodal source. Still blocked on the owed
   clean ClickBench interleaved A/B.

Third, and unrelated to variance: **pool the scan row-group output backing**
(`readRowGroupNative.func2` / `readColumnNative` / `newVectorFromColumn` / `ColumnPageReader`) —
the half of the vector-reuse lever `69aecbb` deliberately deferred. It is now the #1 named heap-lock
caller at 130.8 s (10.9 s per worker per suite run) behind ~1 140 GB/run of allocation, and the
join half of the same lever just delivered −13.1 s/worker-run with the switch to prove it.

Fourth, process, not perf: **fix the bench `extra_env` seam for the coordinator** (§0) — it
silently voided one arm of this window and will void any future coordinator-side A/B.
