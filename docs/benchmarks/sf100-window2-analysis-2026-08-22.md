# SF100 window-2 four-arm analysis — 2026-08-22

**Window:** 2026-08-22 03:10–04:10 UTC, one EC2 deploy, coord c7g.2xlarge + 3× c7gd.4xlarge,
SF100 TPC-H, runs = 4 per arm (run1 cold). Same hardware and same day as window 1
(`docs/benchmarks/sf100-window-analysis-2026-08-22.md`), which is referenced throughout as **w1**.

| arm | engine | switch | run id | dir |
|---|---|---|---|---|
| **A base** | `23abd8e` (v0.16.0-correctness) | — | `20260822-031043` | `results/w2base/` |
| **B cand** | `27a8d10` (wave 1 + 2) | — | `20260822-032608` | `results/w2cand/` |
| **C admoff** | `27a8d10` | `WADJET_DECODE_ADMISSION=0` | `20260822-034116` | `results/w2admoff/` |
| **D sinkoff** | `27a8d10` | `WADJET_STAGE_SINK_ACCUM=0` | `20260822-035615` | `results/w2sinkoff/` |

B, C and D are the **same binary**; every B↔C difference is decode admission and nothing else,
every B↔D difference is the stage-sink accumulator and nothing else.

> **Data gap:** the **base arm's worker journals were not collected** (`results/w2base/` has
> `coord-journal.gz` + profiles but no `wlogs/`). Every base number below is therefore either
> (a) from the coordinator journal — which carries per-stage dispatch/complete timestamps and so
> supports the whole Q18 stage decomposition — or (b) from the base **worker profiles**, which
> were captured. Per-task/per-fragment log fields for base come from **w1**'s base arm (identical
> engine `23abd8e`); every such use is flagged, and where both windows have base the two agree
> (Q18 `final_aggregate-7` span: w1 5.15–5.39 s, w2 5.07–5.23 s).

Working files: `scratchpad/diff2/` (merged `w2*-{cpu,block,mutex,heap}.prof` + `-flat/-cum` text,
`cp.py`, `mx.py`, `hp.py`), `scratchpad/{stages2,coordstage,qsec,onestage,morsel2,da2,daq,daqs,phases,sinkms,ssp,tl}.py`.

---

## 0. Totals

| | A base | B cand | C admoff | D sinkoff |
|---|---|---|---|---|
| suite wall r1 / r2 / r3 / r4 | 229.3 / 177.5 / 176.9 / 166.0 | 220.9 / **162.4 / 163.4 / 160.7** | 211.5 / 176.4 / 163.9 / 158.4 | 210.1 / 171.3 / 164.2 / 161.9 |
| **steady mean (r2–r4)** | **173.5** | **162.2** | 166.2 | 165.7 |
| steady mean, excl. Q07/Q08/Q09 (the bimodal three, §7.2) | 127.4 | **117.8** | 117.5 | 120.6 |
| worker CPU, 3 workers × 4 runs | 13 371.5 s | 12 969.6 s | 12 984.5 s | 12 877.6 s |
| **worker CPU per suite run** | **3 342.9 s** | **3 242.4 s** (−3.0 %) | 3 246.1 s | 3 219.4 s |
| utilization of 48 vCPU | 36.9 % | 38.0 % | 37.9 % | 37.7 % |
| suite task-seconds, steady run | *(no wlogs)* | **1 099.6** | 1 133.7 | 1 132.0 |
| mutex delay, 3 w × 4 runs | 1 493.3 s | **1 112.3 s** | 1 106.1 s | 1 439.0 s |
| block (parked) total | 272 151 s | 277 302 s | 278 664 s | 274 438 s |
| alloc_space, 4 runs | 8 683.1 GB | 8 593.0 GB | 8 625.2 GB | 8 590.1 GB |

**Headline: cand is −6.5 % on the steady mean and −3.0 % on worker CPU against the release.**
Per-query, 58 % of the 11.3 s suite gain is Q18 alone (−6.6 s); the rest is spread thin
(Q09 −2.7, Q08 −1.2, Q16 −1.1, Q01 −0.6, Q21 −0.6, …) and is partly offset by Q07 **+2.2 s**,
which is a mode lottery, not a regression (§7.1).

Neither kill-switch arm moves the suite: C and D are within run scatter of B, and once the three
bimodal queries are removed C is **−0.3 s over 19 queries** (8/19 slower) and D is **+2.8 s**, of
which **+2.2 s is Q17 alone** in one run (another mode lottery — D hit Q17's slow mode in r1 *and*
r2). Both switches are **wall-neutral in this window**; §2 and §3 show that their internal target
signals nonetheless moved exactly as predicted.

---

## 1. Q18 — born-flat delivered the whole partial-agg win; the residual is the FINAL aggregate

### 1.1 The stage decomposition (coordinator journal — comparable across all four arms + w1)

Q18's critical path is `repartition-11` (the capped partial aggregate) → `final_aggregate-7`
→ join-10 → join-13 → join-17 → sort-20. Stage spans, per run, steady mean in bold:

| arm | window | repartition-11 span | final_aggregate-7 span | tail | query wall |
|---|---|---|---|---|---|
| base | w2 | 11.63 / 12.52 / 11.62 / 11.80 → **11.98** | 5.10 / 5.17 / 5.23 / 5.07 → **5.16** | ~2.1 | **19.3** |
| base | w1 | 10.29 / 11.14 / 10.35 / 10.64 → **10.71** | 5.39 / 5.20 / 5.15 / 5.24 → **5.20** | ~2.1 | 18.8 |
| **cand** | w2 | 3.92 / 3.68 / 3.92 / 3.86 → **3.82** | 7.69 / 6.17 / 5.47 / 7.91 → **6.52** | ~2.2 | **12.7** |
| admoff | w2 | 3.83 / 3.93 / 4.06 / 4.25 → **4.08** | 5.91 / 5.90 / 5.81 / 5.67 → **5.79** | ~2.3 | 12.8 |
| sinkoff | w2 | 4.18 / 3.62 / 3.74 / 4.13 → **3.83** | 5.62 / 5.81 / 6.13 / 5.72 → **5.89** | ~2.2 | 12.4 |
| bisect (`TWO_LEVEL_HT=0`) | w1 | 3.86 / 4.06 / 3.98 / 4.33 → **4.12** | 4.17 / 4.21 / 4.08 / 4.13 → **4.14** | ~2.0 | 10.9 |

**The partial-aggregate stage is fixed, exactly to the index-off arm.** 11.98 s → 3.82 s, i.e.
−8.2 s, against bisect's 4.12 s. Three independent arms of the new binary (B/C/D) land on
3.82–4.08 s. Per-task: 2.25 s mean (cand) vs 2.30 s (bisect w1) vs 7.10 s (base w1).

**The whole residual vs bisect is `final_aggregate-7`**, the query's *unbounded* final aggregate:

```
Δ(cand − bisect) = +2.38 s (final_aggregate-7) − 0.30 s (repartition-11) + ~0.3 s (tail/window)
                 ≈ +1.8 s   →  12.7 vs 10.9
```

That splits into two measurable pieces, both same-window controlled:

* **the bucketed index at all: +1.0 s** — base 5.16–5.20 s (old count gate) vs bisect 4.14 s
  (index off). Both windows put base's `final_aggregate-7` at 5.15–5.39 s, so this is not drift.
* **`1a39e1e`'s load-factor gate: +0.6 to +1.4 s on top of that** — admoff 5.79 / sinkoff 5.89 /
  cand 6.52 against base's 5.16. (cand's extra 0.6 s over its two sibling arms is not the gate —
  same binary — it is run-to-run: cand's r1/r4 were 7.7/7.9 s.)

### 1.2 The new logging settles both open questions

`shuffle partial agg` now carries `born_flat` / `group_ceiling` / `conversions` / `cap_mb`, and
`task completed` carries the two-level counters. Both answers are unambiguous.

**(a) Do flat-born tasks flush 5× or 8×? → 5, exactly like the index-off arm.**
Identical 50 000 000-row task, every arm, all 4 runs:

| arm | in_rows | out_rows | **flushes** | born_flat | conversions | group_ceiling |
|---|---|---|---|---|---|---|
| base (w1) | 50 000 000 | 12 50x xxx | **8** | n/a | — | n/a |
| bisect (w1, index OFF) | 50 000 000 | 12 49x xxx | **5** | n/a | 0 | n/a |
| **cand / admoff / sinkoff (w2)** | 50 000 000 | 12 49x–12 50x xxx | **5** | **true** | **0** | **2 917 776** |

Every partial-agg task in all three w2 arms of the new binary reports `born_flat=true`,
`conversions=0`, `group_ceiling=2 917 776` (< `G*` = 4 M, as the commit predicted), and the flush
count tracks input size linearly (50 M→5, 44 M→4, 30 M→3, 24 M→3, 20 M→2, 10 M→1).
**So the unlocalized base-vs-bisect 5→8 flush asymmetry was a property of the bucketed layout
itself charging the same 128 MB `StateBytes` budget, and being born flat removes it.** There is
nothing left to inflate on the flat form — the flat form *is* the 5-flush form. The residual
open item from `dcc95a8`'s body is closed **for bounded sinks**; it can only still exist on
sinks that are actually bucketed, which by construction have no epoch cap for it to shrink.

**(b) Where did the conversions go? → to the unbounded sinks, and only Q18 and Q20 have any.**
`task completed` two-level counters, summed over 4 runs:

| query / stage | conversions (cand) | conversions (admoff) | born_flat | Σ task-s |
|---|---|---|---|---|
| **Q18 `final_aggregate-7`** | **378** | **384** | 0 | 264.5 |
| Q20 `final_aggregate-9` | 356 | 310 | 0 | 94.5 |
| Q20 `scan-4` | 209 | 201 | 0 | 162.3 |
| Q20 `join-3` | 22 | 52 | 0 | 30.2 |
| Q18 `repartition-11` | **0** | **0** | 816 | 126.3 |
| Q18 `repartition-8` / `-16` | 0 | 0 | 72 / 24 | 30.7 / 3.6 |
| Q11 `final_aggregate-8` | 4 | 4 | 0 | 2.3 |

Everything else in the suite is 0/0/0. (`two_level_*` are worker-wide atomics snapshotted around
`Execute`, so with concurrency they cross-attribute *within* a stage; the per-stage totals are
exact because only these two queries produce conversions at all.)

### 1.3 CPU confirms it end to end

Merged worker CPU (3 workers, 4 runs; per suite run = ÷4):

| frame | base | cand | admoff | sinkoff | note |
|---|---|---|---|---|---|
| `intTwoLevelTable.GetOrInsertAt` flat | 398.6 | **65.5** | 67.1 | 66.2 | −83 % — the bounded sinks stopped probing a bucketed table |
| `convertIntHashTableToTwoLevel` **cum** | 322.0 | **84.1** | 80.5 | 79.6 | −74 %; what is left is Q18-final + Q20 |
| `cappedPartialAgg.consume` **cum** | 372.0 | **97.3** | 97.5 | 98.2 | w1 bisect was 100.4 → cand ≡ index-off |
| `HashAggregate.consumeBatchIntGroup` **cum** | 456.6 | **179.8** | 181.6 | 179.8 | w1 bisect 192.8 → same |
| `intHashTable.{Get,GetOrInsertNoGrow,PutNoGrow}` flat | 1 028.7 | 1 049.2 | 1 056.5 | 1 056.1 | the flat work it costs instead: **+5 CPU-s/run** |
| `allocIntSubEntries` alloc_space | 62.5 GB | **11.5 GB** | 11.6 | 11.5 | −82 % of the bucket allocator's Go-heap churn |

Net: `internal/engine/exec` is **−56.4 CPU-s per suite run**, of which the group index is ~−52.

---

## 2. Admission (B vs C — same binary, `WADJET_DECODE_ADMISSION=0`)

### 2.1 Did it fire? Yes — but the counter that was built to say so was never captured

`decode_admits / decode_bypasses / decode_holdbacks` are emitted from
`Worker.logFinalScanStats()` (`internal/worker/worker.go:1371`), which runs **only from
`Stop()`/drain**. The bench collects the journal before the workers drain, so **no
`cpu token admission stats (final)` line exists in any arm's `wlogs/`** — nor do any of the other
`(final)` lines. *Fix for the next run: emit these on the periodic `worker stats` line (72/arm)
instead of only at drain.* The verdict below therefore rests on the periodic
`scan/shuffle decode-ahead stats` lines, which are cumulative and were captured.

### 2.2 The predicted signals all moved, in the predicted directions

Scan decode-ahead, summed over 3 workers, **steady runs r2–r4 only** (per-query attribution):

| | **B cand** | **C admoff** | Δ | w1 base (12 runs) |
|---|---|---|---|---|
| `token_stall_ms` | **1 041.1 s** | 1 407.3 s | **−26.0 %** | 2 540.0 s (4 runs) |
| `window_full_ms` | **285.4 s** | 155.4 s | **+83.7 %** | 183.7 s |
| `decode_ms` | 1 874.9 s | 1 927.6 s | −2.7 % | 3 220.0 s |
| **token_stall / (decode + stalls)** | **32.5 %** | **40.3 %** | −7.8 pts | 41.6 % |

Whole-run cumulative (all 4 runs) tells the same story: token_stall 1 513 s (B) vs 2 037 s (C) vs
2 540 s (w1 base); window_full 384 s vs 219 s vs 184 s; `token_stalls` count 181 288 vs 256 091.

The morsel dispenser, Σ over 2 860 fragments per arm:

| class | metric | **B cand** | **C admoff** | w1 base |
|---|---|---|---|---|
| scan-* | Σ `consumer_dry_wait_ms` | **5 245.4 s** | 9 456.4 s (**−44.5 %**) | 9 591.7 s |
| scan-* | parked % of k·elapsed | **27.8 %** | 47.3 % | 46.0 % |
| scan-* | Σ `width_wait_ms` | **5 118.6 s** | 1 994.9 s (**+157 %**) | 2 276.5 s |
| scan-* | Σ elapsed | **1 256.9 s** | 1 331.5 s (−5.6 %) | 1 390.4 s |
| ALL | Σ dry / Σ width | 12 435.7 / 5 873.6 | 16 845.2 / 2 609.2 | 16 794.3 / 3 119.9 |
| ALL | effective width | 2.63 / 15 | 2.60 / 15 | 2.88 / 15 |
| ALL | `width_dry_parks` / `width_fed_parks` | 284 730 / 95 094 | 272 436 / 94 330 | (field is new) |
| ALL | `dispenser_producer_wait_ms` | 5.5 s | 0.0 s | 0.0 s |

The single cleanest instance is **Q08 `join-6`**, whose decode reads 17.9 GB in *both* arms:

| Q08 join-6, per run | B cand | C admoff |
|---|---|---|
| `token_stall_ms` | **22.2 s** | 53.1 s (−58 %) |
| `window_full_ms` | **57.5 s** | 20.7 s (+178 %) |
| `decode_ms` | **71.4 s** | 82.9 s (−14 %) |
| `decode_bytes` | 17.9 GB | 17.9 GB |

Same bytes, decoder wall down 11.5 s/run, and the stall converted into ring-full — i.e. the
decoder now runs ahead of its consumers instead of behind them. **The fix fired.**

### 2.3 So where did the time go, and why is the suite flat?

It moved from one parked state to another, and the total parked time barely changed:

```
Δdry(scan)      −4 211 s     consumers no longer starve
Δwidth_wait     +3 124 s     …they now queue for a token behind the decode reserve
Δtoken_stall      −524 s     decoders admitted
Δwindow_full      +165 s     decoders now hit a full ring instead
Δdecode_ms         −42 s     (flat — same work)
Δfragment elapsed  −98 s  (−3.7 %, i.e. −8 s per worker per suite run)
```

That is the honest reading of `window_full ⇒ ring-bound ⇒ consumers`: **the pipeline moved from
decode-starved to roughly balanced, and the fragments it un-starved were not the critical path.**
Σ`process_ms` (the actual work) fell only 2.3 %, worker CPU is unchanged between B and C
(12 969.6 vs 12 984.5 s, +0.1 % — it is a scheduling change, as advertised), and the suite wall
difference over the 19 non-bimodal queries is −0.3 s.

Corroborating signal from D: **the sink fix and the admission fix interact.** With
`STAGE_SINK_ACCUM=0`, consumers blocked in `Consume` keep holding their CPU token, and scan
token_stall rises from 1 513 s (B) to 1 819 s (D) with admission still on — halfway back to
admoff's 2 037 s. Whatever the sink change is worth, part of it is decode admission.

### 2.4 Bimodality of Q09/Q08/Q07 — a pre-existing mode, not admission (mostly)

`Q09 join-6` is a 3-task broadcast join with two clean modes. Per-task durations:

| arm | r1 | r2 | r3 | r4 |
|---|---|---|---|---|
| cand | 12.6 / 15.1 / 15.7 | 6.8 / 6.9 / 7.3 | 6.9 / 6.9 / 6.9 | 6.9 / 6.9 / 8.1 |
| admoff | 6.8 / 15.6 / 16.2 | 7.5 / **15.5 / 15.7** | 6.9 / 7.0 / 7.6 | 6.8 / 6.8 / 7.0 |
| base (w1) | 15.0 / 15.1 / 16.7 | 8.2 / 9.5 / 9.8 | 8.7 / 8.8 / 9.6 | 8.2 / 8.5 / 9.3 |
| cand (w1) | 8.3 / 16.3 / 17.7 | 8.3 / 9.4 / 9.7 | 7.9 / 8.3 / 8.4 | 7.8 / 8.0 / 8.2 |
| bisect (w1) | 8.1 / 14.0 / 16.4 | 8.2 / 8.5 / 8.5 | 8.5 / 8.9 / 9.9 | 8.1 / 9.2 / 9.4 |

The slow mode is ≈2× the clean one, it fires in **every** arm and **every** cold run, and it
predates the change. **It is not the token pool:** Q09 `join-6` token_stall is 0.2–0.8 s in both
arms and decode_ms is identical (76 s vs 75 s). `fragment task phases` names it outright —

```
slow task: src_ms=9 660  ops_ms=30 104  acq_prefetch_files=3  acq_prefetch_ms=8 879
fast task: src_ms=  600  ops_ms=28 693  acq_basecache_files=1 acq_prefetch_miss=1
```

— it is the **un-overlapped prefetch-take wait** from the 2026-08-16 straggler verdict
(`docs/benchmarks/straggler-tier-verdict-2026-08-16.md`), absent from w1's window and back in
this one. Base-table-cache hit ⇒ 0.6 s of src; prefetch path ⇒ 8.9 s of src. Q08 `join-6` and
Q17 `join-2` are the same shape.

Steady-run incidence (Q08+Q09, 3 steady runs each): B **0/6**, C 2/6, w1 base 1/6, w1 cand 1/6,
w1 bisect 1/6.
**With n=3 runs per arm this is not evidence that admission shifts the mode probability**, and
the mechanism above says it should not. Do not credit admission with Q09's −2.7 s.

---

## 3. Sink (B vs D — same binary, `WADJET_STAGE_SINK_ACCUM=0`)

**The target site collapsed by 99.5 %, and the switch restores it exactly.**
`unpartitionedStageSink.Consume`, mutex delay (cum), 3 workers × 4 runs:

| arm | delay | share of all mutex | per worker per suite run |
|---|---|---|---|
| A base | **477.7 s** | 32.0 % | 39.8 s |
| **B cand** | **2.2 s** | **0.19 %** | **0.18 s** |
| C admoff | 1.7 s | 0.15 % | 0.14 s |
| **D sinkoff** | **506.0 s** | **35.2 %** | **42.2 s** |

(w1's base measured 524.5 s / 32.6 % / 39–44 s per worker-run — the same number.)

Total worker mutex delay follows: 1 493.3 (A) → **1 112.3 (B)** → 1 106.1 (C) → 1 439.0 (D),
i.e. **−31 s of lock waiting per worker per suite run.** The sink's own instrument agrees:
Σ`sink_ms` over `fragment task phases` is **58.8 s (B) vs 805.9 s (D)** — 13.7×, and on `join-6`
alone 35.1 s vs 765.9 s (1.5 s vs 31.9 s per fragment across its 15 consumers).

**And the wall did not move.** Same fragments, same runs:

| | B cand | D sinkoff |
|---|---|---|
| `join-6` Σ fragment elapsed (24 fragments) | 283.6 s | 286.2 s (+0.9 %) |
| `join-6` Σ ops_ms | 2 159.7 s | 2 098.5 s |
| join-* Σ `consumer_dry_wait_ms` | 6 874.4 s | 5 992.1 s |
| join-* Σ `process_ms` | 3 504.4 s | 4 235.9 s |
| suite steady mean, excl. Q07/08/09/Q17 | 110.6 s | 111.2 s (admoff 110.7, base 120.6) |

So: with the lock, the consumers' time is charged to `process_ms` (blocked *inside* the push)
and dry-wait is correspondingly lower; without it, they go dry instead. **The serialized memcpy
was real and is gone, but it was never the pacer** — exactly what the commit's own local
measurement predicted ("wall time is flat locally… the SF100 A/B is the verdict"). The verdict is
that it is flat at SF100 too, on wall, while removing a third of the worker's lock waiting and
(§2.3) returning CPU tokens to the decoders sooner.

No side effects: the partitioned sink path is untouched (Σ`append_ms` 871.8 s (B) vs 873.4 s (D)),
shuffle byte volumes and file counts match, worker CPU is within 0.7 %.

---

## 4. CPU / heap / GC — base vs cand

Package rollup, flat CPU (`-nodefraction=0`; w1's table used pprof's default 0.5 % node drop,
which silently zeroed frames just under the threshold — the numbers below supersede it):

| package | base % | cand % | Δpts | Δ CPU-s / suite run |
|---|---|---|---|---|
| go-runtime | 29.600 | 30.582 | +0.982 | +2.1 |
| `internal/engine/exec` | 24.646 | 23.669 | −0.976 | **−56.4** |
| `compress` (zstd/s2) | 22.689 | 23.464 | +0.775 | +2.3 |
| `internal/engine/expr` | 6.262 | 4.916 | −1.346 | **−49.9** |
| `expr.FilterPredicate` | 0.000 | 0.571 | +0.571 | +18.5 |
| `worker.compileFilterExprs.wrapCompiledFilter` | 0.372 | 0.000 | −0.372 | −12.4 |
| `internal/engine/batch` | 5.036 | 5.073 | +0.037 | −3.9 |
| `internal/storage/parquet` | 4.036 | 4.077 | +0.041 | −2.7 |
| `internal/worker` | 2.213 | 2.346 | +0.133 | +2.1 |
| **TOTAL** | | | | **−100.5 CPU-s per suite run (−3.0 %)** |

* **expr frames: confirmed gone or shrunk.** `CmpTemporalLit.EvalBool` 37.5 → **0**,
  `In.EvalBool` 5.1 → **0**, `wrapCompiledFilter.func1` 48.6 → **0**, replaced by
  `expr.FilterPredicate.func1/func2` (57.4 + 15.7 with the leaves inlined into them);
  `ColRef.resolve` 62.3 → 18.7 (−70 %), `BinOpFloat64.EvalFloat64` 64.4 → 42.1,
  `ColRef.EvalInt64` 76.8 → 56.1, `BinOpFloat64.resolveOpCode` 10.5 → 0.6.
  **Net expr family −43.8 CPU-s/run**, matching w1's −44.8.
* **Conversion frames: NOT gone — moved.** `convertIntHashTableToTwoLevel` flat 2.9 → 75.4 (its
  *cum* fell 322.0 → 84.1 because base paid it under `GetOrInsertAt`). Per run the conversion
  work is 80.5 → 21.0 CPU-s, and 100 % of what remains is Q18-final + Q20 (§1.2).
* **What grew:** flat-table probing (`intHashTable.Get` +17.2, `GetOrInsertNoGrow` +24.0,
  `packedHashTable.GetOrInsertNoGrow` +11.3 over 4 runs ⇒ **+5 CPU-s/run**) — the price of being
  born flat, 1/10 of what it buys. `compress` +2.3 CPU-s/run and `go-runtime` +2.1 are noise
  (decode_bytes are identical to 0.1 %).
* **Heap:** alloc_space 8 683.1 → 8 593.0 GB (−1.0 %, −22.5 GB per suite run). The only
  frame that moves materially is `allocIntSubEntries` 62.5 → 11.5 GB (−82 %) plus
  `allocPackedSubEntries` 50.2 → 41.6. The #1 allocator is unchanged and unrelated:
  `batch.NewVectorWithScale` at **2 373.8 GB / 4 runs = 593 GB per suite run**, then
  `BytesColumn.PreAllocBytes` at 341 GB/run.
* **GC: no change at all.** `runtime.madvise` 62.5 → 62.2, `memclrNoHeapPointers` 205.2 → 208.4,
  `mallocgc*` +3.4 total, `markrootSpans` 3.1 → 2.5, `gcDrain` 0.1 → 0.1 (all CPU-s over 4 runs).

---

## 5. Wait side, re-ranked on cand

Mutex delay = time *other* goroutines waited; 1 112.3 s over 3 workers × 4 runs =
**92.7 s per worker per suite run**, against ~1 080 CPU-s of work per worker-run.

| # | site | cand | share | per worker-run | vs base |
|---|---|---|---|---|---|
| **1** | **`runtime.unlock` (Go heap lock family)** | **753.9 s** | **67.8 %** | **62.8 s** | 681.8 |
| | ↳ `mheap.allocSpan` | 528.8 | 47.5 % | 44.1 | 527.4 |
| | ↳ `mcentral.grow` | 182.4 | 16.4 % | 15.2 | 197.7 |
| **2** | `runtime._LostContendedRuntimeLock` (same family, unattributable) | 223.6 s | 20.1 % | 18.6 | 205.8 |
| **3** | `sync.Mutex.Unlock` — **all** application mutexes | 130.9 s | 11.8 % | 10.9 | **601.2** |
| | ↳ `unpartitionedStageSink.Consume` | 2.2 | 0.19 % | 0.18 | 477.7 |
| | ↳ `partitionedShuffleSink.*` | 17.0 | 1.5 % | 1.4 | 13.3 |

**Yes — the Go heap lock is now the top lever, and it is 88 % of all mutex delay
(977.5 s of 1 112.3 = 81.4 s per worker per suite run).** With the sink removed there is no
application lock left worth naming. Its callers, by cum:

| caller | cand | share of mutex | what it allocates |
|---|---|---|---|
| `HashJoinProbe.Execute` | 227.2 s | 20.4 % | join output |
| ↳ `HashJoinProbe.emitViewOutput` | 215.5 s | 19.4 % | |
| ↳ `exec.gatherBuildVector` | 91.5 s | 8.2 % | build-side gather |
| `scan.readRowGroupNative.func2` | 170.2 s | 15.3 % | per-column decode |
| ↳ `batch.NewVectorWithScale` | 146.6 s | 13.2 % | |
| ↳ `batch.newVectorFromColumn` | 124.4 s | 11.2 % | |
| ↳ `BytesColumn.PreAllocBytes` | 103.8 s | 9.3 % | string backing |
| `parquet.decodeDataPageV1` | 118.2 s | 10.6 % | page buffers |
| `runtime.makeslice` (all of the above) | 467.4 s | 42.0 % | every `make` > 32 KB |

Quantified: **2.15 TB of allocation per suite run, ~934 GB of it through the two vector-backing
frames**, is what produces 81 s/worker-run of heap-lock waiting. This is the `SetFrom` /
vector-pooling lever, unchanged from w1 and now unobstructed by anything else.

Block profile (parked time) is unchanged in shape: `uploadManager.acquireSlot` 143 025 s (51.6 %,
by design, background — §7.5 of w1 still holds for the *upload* queue itself), `consumeMorsels`
17 282 s (6.2 %, down from base's 19 318), `widthGate.claim` **5 813 s (cand) vs 2 541 s (admoff)
vs 6 682 s (sinkoff)** — the admission holdback is visible here too — and
`unpartitionedStageSink.Consume` 215 s vs base's 622 s.

---

## 6. Sanity

* **Row counts identical** across all four arms and all four runs — the (query, rows) set has
  exactly 22 unique entries per arm and diffs empty pairwise.
* **Value signatures identical** across all four arms and all four runs — 21 unique `vsig` lines
  per arm, pairwise diffs empty. w1's `Q19 vsig …904 vs …903` float-order nondeterminism did
  **not** recur in any arm.
* **Zero task failures** (`success=false` = 0 in coordinator and worker journals, all arms).
* **Zero retries** — every one of the 5 936 coordinator `task result` lines is `attempt=1`, all
  four arms.
* **No stalls, no spills, no timeouts.** Warning mix is the usual one and identical across arms
  (`accounting drift high/critical`, `dataplane connection ended`,
  `decoded-rowgroup cache pressure shed`); the 3 coordinator `ERROR`s per arm are the NATS client
  parser line each worker emits at connect.

---

## 7. Two things this window found that were not on the list

### 7.1 ★ The gather-merge tail is ~13 s of pure critical-path *waiting* per suite run

`final_aggregate-N-merge-{0,1,2}` (query id `st-final_aggregate-N-interm-<hash>`) are single-task
stages that emit **4 rows** and sit on the critical path after the last join. Their durations are
either ~0.01 s or a multiple of ~0.5 s. Max merge duration per query (the stage is 3 parallel
tasks, so the max is the critical path), steady mean of r2–r4:

| Q | cand | admoff | sinkoff | base (w1) |
|---|---|---|---|---|
| Q07 | **4.47** | 4.64 | 4.50 | 2.70 (0.02 in 2 of 4 runs) |
| Q05 | 3.83 | 2.97 | 2.92 | 3.29 |
| Q09 | 2.44 | 1.51 | 2.61 | 1.18 |
| Q04 | 1.75 | 1.90 | 1.90 | 1.77 |
| Q08 | 1.08 | 0.90 | 1.14 | 0.83 |
| Q17/Q14/Q22/Q01/Q06 | 0.03–0.43 | | | |
| **Σ per suite run** | **14.5 s** | 13.3 s | 14.0 s | 12.0 s |

The observed values cluster at 0.01, 0.18, 0.74, 1.25, 1.75, 2.25, 2.77, 3.34, 3.83, 4.30, 4.81 —
**a 0.5 s quantum**, which is `durableWaitPoll = 500 * time.Millisecond` in
`internal/worker/peer_exchange.go:271`. The path is
`stream_source.go:607` → S3 `Get` returns `ErrNotFound` → `awaitDurableObject` re-polls every
500 ms (up to `durableWaitTotal` = 15 s) for a stage output whose **background upload has not
landed yet**, publishing `SubjectUploadRelease` on entry — and
`releasing deferred stage-output uploads` duly fires **93× per run** in cand.

The coordinator trace confirms the shape: in Q07 the merge tasks are published in the same
millisecond the producing stage completes, and the gather returns either +0.03 s (fast) or
+4.5 s (slow) later. **Same query, same rows, same plan.** This is also the mechanism behind
Q07's suite-level "regression" in §0 (base got the fast mode in 2 of 4 runs).

This is the largest single recoverable block in the window and it is not a hot-path problem:
it is a 500 ms poll standing in for an event. Two fixes, in order of cost:
1. **back off from ~25 ms instead of a flat 500 ms** (a bounded geometric ramp keeps the same
   15 s ceiling and the same failure semantics) — recovers most of the quantization overshoot,
   ~5–8 s per suite run, ~10 lines;
2. **give the merge stage's inputs a peer hint** — the producing worker still has the file on
   local NVMe. The counters say the merge tasks never even try it: `peer_fallthroughs = 0` in
   every arm (no inline peer fetch ever failed), so `awaitDurableObject` is only reachable with
   an empty peer address and a bare query fetch token — i.e. **no hint existed**. The inline
   peer path is essentially unused overall (`peer_hits` = 368 against `peer_files` = 42 466,
   almost all of which the prefetcher contributes). Hint coverage for `-interm-` inputs removes
   the wait rather than shortening it.

Also worth reconsidering: w1 §7.5 concluded the upload admission queue is benign because nothing
waits on it. **Something does** — just not the *producer*. The consumer waits, one poll interval
at a time. `uploadSlotsBusy` should not be widened blindly, but the release path should be
measured: a `wait_ms` + `polls` counter on `awaitDurableObject` would make this a one-grep answer
next window.

### 7.2 The suite has (at least) four bimodal queries, and they dominate any n=3 arm comparison

| query | modes (steady wall) | mechanism | seen in |
|---|---|---|---|
| Q07 | 4.1 vs 8.9 s | gather-merge durable wait (§7.1) | all arms, both windows |
| Q08 | ~17.5 vs ~25 s | `join-6` prefetch-take wait (`acq_prefetch_ms` 8–9 s) | all arms |
| Q09 | ~18 vs ~26 s | same | all arms |
| Q17 | ~6.8 vs ~14 s | `join-2`, same shape (3-task broadcast join, max task 13.1 s) | all arms |

Together they are ~46 s of the 162 s steady suite, and a single mode flip is worth 4–8 s — larger
than any switch effect measured here. **Arm comparisons at n=3 must exclude them or report them
separately**; §0 does both.

---

## 8. Attribution, levers, verdict

### 8.1 Per-switch attribution

| change | its own signal | wall |
|---|---|---|
| **born-flat bounded sinks** (`dcc95a8`, `WADJET_TWO_LEVEL_BORN_FLAT`) | Q18 partial-agg stage 11.98 → 3.82 s; conversions 8/task → **0**; flushes 8 → **5**; `cappedPartialAgg.consume` CPU −74 %; `allocIntSubEntries` −82 % | **Q18 −6.6 s, = 58 % of the suite gain** |
| **expr wave 1** (`87c0b30`+`29cd11f`+`1441ca4`) | expr family −43.8 CPU-s/run; 4 frames to zero | inside the remaining −4.7 s, spread over ~10 queries |
| **decode admission** (`27a8d10`, `WADJET_DECODE_ADMISSION`) | token_stall −26 %, window_full +84 %, scan dry-wait −44.5 %, scan fragment elapsed −5.6 %, CPU-neutral | **neutral** (−0.3 s over 19 queries) |
| **stage-sink accum** (`1b0819c`, `WADJET_STAGE_SINK_ACCUM`) | sink mutex delay −99.5 % (477.7 → 2.2 s), Σ`sink_ms` −93 %, total worker mutex −31 s/worker-run, decode token_stall −17 % vs D | **neutral** (+0.6 s over 18 queries excl. Q17) |
| logging (`d13eff7`) | turned §1.2 into two greps | — |

### 8.2 Next levers

| # | lever | measured size | confidence |
|---|---|---|---|
| **1** | **Kill the 500 ms `awaitDurableObject` poll on gather-merge inputs** (§7.1): geometric backoff from ~25 ms, then peer-hint coverage for `-interm-` inputs | **12–14.5 s of critical path per suite run**; realistically −5 to −8 s (backoff alone), up to −12 s with hints | high — mechanism is quantized in the data, code site identified |
| **2** | **Pool the scan-output and join-gather vector backing** (`NewVectorWithScale` / `PreAllocBytes` / `gatherBuildVector` / `emitViewOutput`) to cut > 32 KB spans | 81 s/worker/suite-run of heap-lock delay, 934 GB/run of allocation behind it; even a third is ~25 s/worker-run of contention | high on the diagnosis, medium on conversion to wall |
| 3 | **Start the prefetcher at source Init** (the standing 2026-08-15 item) — kills the Q08/Q09/Q17 slow mode | 7–9 s of query wall each time it fires; fired in 3 of 12 steady query-runs here | high |
| 4 | **Stop bucketing the Q18/Q20 unbounded sinks too** — `final_aggregate-7` is 4.14 s flat vs 5.16 s (old gate) vs 5.79–6.52 s (new gate), and it is the entire Q18 residual | Q18 −1.5 to −2.3 s; Q20 task-seconds −7.7 % (w1) | high on SF100; **blocked on the owed clean ClickBench interleaved A/B** — the only evidence for bucketing unbounded sinks is a ClickBench arm the w1 addendum already declared confounded |

### 8.3 Verdict on `27a8d10`

**Ship it as the v0.17 candidate. Nothing needs holding back behind its switch.**

* −6.5 % steady suite (173.5 → 162.2 s), −3.0 % worker CPU, −1.0 % allocation, and the biggest
  single-query win of the campaign (Q18 19.3 → 12.7 s, with its partial-agg stage now identical
  to the index-off arm).
* Rows and value signatures are bit-identical across all four arms and all 16 runs; zero
  failures, zero retries.
* **`WADJET_TWO_LEVEL_BORN_FLAT` stays default-on** — it is the whole headline.
* **`WADJET_STAGE_SINK_ACCUM` stays default-on.** Wall-neutral, but it removes a third of the
  worker's lock waiting, removes the largest application mutex in the profile outright, and
  measurably returns CPU tokens to the decoders sooner (§2.3). It costs nothing: CPU, allocation
  and the partitioned-sink path are all unchanged. Keep the switch for the invariance oracle.
* **`WADJET_DECODE_ADMISSION` stays default-on.** Wall-neutral in this window, but every signal
  it was built to move, moved, and moved in the predicted direction (token stall −26 %, ring-full
  +84 %, consumer starvation −44.5 %, CPU flat). It converted a closed starvation loop into a
  balanced one; that the balanced pipeline's fragments were not the critical path is a fact about
  where the critical path is (§7.1, §5), not a reason to revert. It should be re-measured once
  lever #1 lands, since the merge wait is currently hiding fragment-level gains.
* **Ship the missing instrumentation with it:** move `logFinalScanStats()`'s counters
  (`decode_admits/bypasses/holdbacks`, and every other `(final)` line) onto the periodic
  `worker stats` line — this window could not read the one counter the admission commit
  built for it — and add `wait_ms`/`polls` to `awaitDurableObject`.

