# SF100 same-window three-arm analysis — 2026-08-22

**Window:** 2026-08-22 00:57–01:40 UTC, one EC2 deploy, coord c7g.2xlarge + 3× c7gd.4xlarge,
SF100 TPC-H, runs=4 per arm (run1 cold).

| arm | engine | run id | switch |
|---|---|---|---|
| **base** | `23abd8e` (v0.16.0-correctness) | `20260822-005709` | — |
| **cand** | `1441ca4` (main) | `20260822-011301` | — |
| **bisect** | `1441ca4` | `20260822-012853` | `WADJET_TWO_LEVEL_HT=0` |

Candidate = base + `87c0b30` + `29cd11f` + `1441ca4` (expr: lazy-resolution guards out of the
row loop, compile-time filter boolean protocol) + `1a39e1e` (exec: two-level group index
converts at the load-factor crossing) + `62ed42d` (bench profile capture only).
**bisect is the SAME BINARY as cand** — it differs only by the kill switch, so every
difference between cand and bisect is the two-level group index and nothing else.

Working files: `scratchpad/diff/` (`{base,cand,bis}-cpu.prof`, `-block.prof`, `-mutex.prof`,
`*-top-flat.txt`, `*-top-cum.txt`, `cmp.py`, `pkg.py`), `scratchpad/{perq,conc,q18b}.py`.

---

## 0. Totals

| | base | cand | bisect |
|---|---|---|---|
| CPU profile window / worker | 761.87 s | 756.33 s | 715.30 s |
| worker CPU, 3 workers merged, 4 runs | 13 469.75 s | 13 209.17 s | 13 015.84 s |
| **worker CPU per suite run** | **3 367.4 s** | **3 302.3 s** | **3 254.0 s** |
| utilization of 48 vCPU | 36.8 % | 36.4 % | 37.9 % |
| alloc_space, 4 runs | 8.24 TB | 8.22 TB | — |
| suite wall r1 / r2 / r3 / r4 | 225.4 / 184.4 / 179.7 / 168.0 | 217.0 / 191.5 / 184.4 / 159.3 | 198.6 / 173.8 / 173.8 / 164.6 |
| **steady mean (r2–r4)** | **177.4 s** | **178.4 s** | **170.7 s** |

> The "suite −5 %" headline is run4 only. On the steady mean of all three warm runs the
> candidate is **+0.6 % against base**, not −5 %: the expr wins are real but the Q18 tax
> cancels them. The bisect arm is the only one that is actually faster (−3.7 % vs base).

### Per-query task-seconds (sum of every worker task duration inside each query's window, 4 runs)

A far less noisy signal than suite wall — it sums ~5 900 per-task durations from the worker
journals rather than one stopwatch per query.

| Q | base | cand | bisect | cand−base | bis−base |
|---|---|---|---|---|---|
| Q01 | 371.2 | 342.1 | 337.3 | −7.8 % | −9.1 % |
| Q02 | 208.0 | 200.9 | 190.7 | −3.4 % | −8.3 % |
| Q03 | 416.2 | 426.8 | 431.8 | +2.5 % | +3.7 % |
| Q04 | 264.1 | 250.0 | 266.1 | −5.3 % | +0.7 % |
| Q05 | 260.6 | 248.3 | 247.7 | −4.7 % | −5.0 % |
| Q06 | 36.6 | 38.4 | 32.7 | +5.0 % | −10.5 % |
| Q07 | 167.5 | 167.1 | 177.0 | −0.3 % | +5.7 % |
| Q08 | 317.8 | 306.5 | 300.0 | −3.6 % | −5.6 % |
| Q09 | 492.7 | 467.1 | 492.5 | −5.2 % | −0.0 % |
| Q10 | 366.6 | 370.7 | 383.7 | +1.1 % | +4.6 % |
| Q11 | 77.5 | 71.7 | 73.5 | −7.5 % | −5.2 % |
| Q12 | 135.0 | 122.5 | 125.0 | −9.3 % | −7.4 % |
| Q13 | 230.8 | 214.7 | 224.9 | −7.0 % | −2.5 % |
| Q14 | 90.1 | 82.9 | 90.4 | −7.9 % | +0.4 % |
| Q15 | 65.2 | 63.1 | 65.7 | −3.2 % | +0.8 % |
| Q16 | 62.7 | 62.2 | 65.0 | −0.9 % | +3.6 % |
| Q17 | 91.2 | 91.2 | 91.1 | +0.1 % | −0.0 % |
| **Q18** | **772.9** | **914.8** | **450.0** | **+18.4 %** | **−41.8 %** |
| Q19 | 150.1 | 131.3 | 134.8 | −12.5 % | −10.2 % |
| Q20 | 371.2 | 359.3 | 342.5 | −3.2 % | −7.7 % |
| Q21 | 379.7 | 349.8 | 362.3 | −7.9 % | −4.6 % |
| Q22 | 108.9 | 102.1 | 107.9 | −6.3 % | −1.0 % |
| **TOT** | **5 436.5** | **5 383.5** | **4 992.5** | **−1.0 %** | **−8.2 %** |

Q18 is the only query whose sign is consistent and outside scatter in every run of every arm.
Outside Q18 the three arms differ by a few percent in both directions; the bisect arm's mild
uniform inflation on some queries (Q04/Q07/Q10/Q14 +4–6 %) tracks its 7 % shorter wall — the
same background upload/compaction load compressed into less time (its mutex delay is the
highest of the three, §2). The one non-Q18 query where the index is measurably negative is
**Q20 (`GROUP BY l_partkey,l_suppkey`, the PACKED two-level mode): bisect −7.7 % vs base.**

---

## 1. Q18 — mechanism

### 1.1 The regression is one stage, and the whole query's cost is that stage

Q18's DAG: `shuffle-stage-exchange-repartition-11` repartitions lineitem
(600 037 902 rows) by `l_orderkey`, running the **capped partial aggregation**
(`worker.cappedPartialAgg`, 128 MB epochs) on the producer side.

Per-run stage measurements (all 4 runs of each arm, from `msg="task completed"`):

| arm | tasks | mean task | max task | **stage span** | stage Σ | query wall |
|---|---|---|---|---|---|---|
| **bisect** | 14 | **2.22–2.47 s** | 2.66–3.35 s | **3.79–4.31 s** | 31–35 s | **10.21–11.16 s** |
| **base** | 14 | 6.94–7.51 s | 9.07–10.64 s | 9.99–10.94 s | 97–105 s | 18.41–19.18 s |
| **cand** | 13 | 10.13–10.31 s | 12.56–13.02 s | 15.01–15.99 s | 132–134 s | 23.85–25.52 s |

Every other stage in Q18 is flat across the arms (join-10/13/17, final_aggregate-7,
repartition-8/16, scan-1 all within noise). **The stage span delta IS the query wall delta**:
+5.0…+5.4 s cand−base against a query delta of +5.3…+6.8 s; −6.2 s base−bisect against
−7.9 s. Stage concurrency is 8.3–9.9 tasks (measured Σ/span), so
`Δquery_wall ≈ Δtask-seconds / ~9` reproduces all six deltas.

### 1.2 The index halves the epoch, so the aggregate flushes 60–80 % more often

`msg="shuffle partial agg"` for the **identical 50 000 000-row task**:

| arm | in_rows | out_rows | **flushes** | groups/epoch |
|---|---|---|---|---|
| bisect | 50 000 000 | 12 497 812 | **5** | ≈ 2.50 M |
| base | 50 000 000 | 12 502 089 | **8** (+60 %) | ≈ 1.56 M |
| cand | 50 000 000 | 12 501 261 | **9** (+80 %) | ≈ 1.39 M |

Same input, same output cardinality, but the bucketed index costs the epoch 38–44 % of its
usable size against the same 128 MB `StateBytes` budget. Tracked per-epoch operator peak
(`operator_peaks`, max over closed instances of `HashAggregate/group_by=l_orderkey`):

| arm | observed epoch peaks |
|---|---|
| bisect | 0 MB ×53, 3 MB ×21, **13 MB ×39** |
| base | 25/26 MB ×35, 51 MB ×34, **285 MB ×39**, 293 MB ×5 |
| cand | 13/15/24 MB ×30, 288–293 MB ×52, **423 MB ×24** |

Two consequences, and the second is the one that costs:
1. **more epochs**, so more table builds and more finalize/emit cycles per task;
2. **one conversion per epoch that crosses 1 M live** — so the flat→bucketed rehash is paid
   7–9 times per task instead of once, and the epoch it converts in is over ~0.4 M groups
   later. The conversion is never amortized; that is the "irreducible worst case of a bare
   size threshold" the code comment describes, hit on *every* epoch rather than occasionally.

### 1.3 What `1a39e1e` changed, and why it made it worse

CPU (merged 3-worker profiles; **per suite run** = total ÷ 4):

| frame | base | cand | bisect |
|---|---|---|---|
| `convertIntHashTableToTwoLevel` **cum** | 79.5 s | **122.4 s (+54 %)** | 0 |
| ↳ its own flat samples | 0.7 s | 108.0 s | 0 |
| ↳ via `intTwoLevelTable.GetOrInsertAt` | 78.3 s | 0 | 0 |
| ↳ via `intTwoLevelTable.growSub` | 0 | **13.8 s** | 0 |
| `intTwoLevelTable.growSub` **flat** | 5.1 s | **15.4 s (+204 %)** | 0 |
| `intTwoLevelTable.GetOrInsert` (real probes) cum | 21.1 s | 19.7 s | 0 |
| `cappedPartialAgg.consume` cum | 91.5 s | **126.5 s** | 25.1 s |
| `HashAggregate.consumeBatchIntGroup` cum | 112.5 s | **147.2 s** | 48.2 s |

The commit's premise is that converting at the flat table's 70 % load factor pays the
conversion **instead of** the doubling it displaces. The profile says it does not, and the
reason is the commit's own destination sizing:

* the destination is `subCapForFlatSlots(len(flat.entries))` — the flat table's **own** slot
  count split 256 ways. Converting at the load-factor crossing therefore scatters ~0.7 × slots
  entries into exactly `slots` slots, i.e. **the bucketed table is born AT 70 % load**;
* the conversion loop's own `if s.size*10 > len(s.entries)*7 { t.growSub(b) }` then fires for
  every one of the 256 buckets before the loop ends. **`growSub` now takes 76.0 % of its
  samples from inside `convertIntHashTableToTwoLevel`** (`-peek`), against 0 % in base.

So the doubling was not displaced — it was pulled *into* the conversion and a larger scatter
was added in front of it. Per epoch:

| | scatter | + per-bucket rehash | total entry moves |
|---|---|---|---|
| base (converts at ~1.0 M into 2.1 M slots, 48 % load) | 1.00 M | 1.47 M (later, outside convert) | 2.47 M |
| cand (converts at ~1.47 M into 2.1 M slots, 70 % load) | 1.47 M | 1.47 M (inside convert) | 2.94 M |

and the scatter itself is dearer because it runs against a filling table: mean insert probes
over [0, 0.70] load ≈ 2.17 vs ≈ 1.46 over [0, 0.48]. Predicted cost ratio
(1.47 × 2.17 + 1.47 × 1.46) / (1.00 × 1.46 + 1.47 × 1.46) = **1.48**; measured conversion CPU
ratio **1.54**. The mechanism is quantitatively closed.

Per-entry conversion cost, derived from the stage table (Δmean task ÷ conversions per task):

* base: (6.96 − 2.25) s ÷ 7 conversions ÷ 1.00 M entries = **673 ns/entry**
* cand: (10.13 − 2.25) s ÷ 8 conversions ÷ 1.47 M entries = **670 ns/entry**

Two independent arms agree to 0.5 %. **The design comment budgets "~25–30 ns per live entry".
The measured cost at SF100 is 22–27× that.** That single number is the root miscalculation:
the model was taken from a single-table micro-benchmark on an idle desktop; in production
8–10 tasks per cluster run this scatter concurrently, each streaming a 33 MB source into a
33–67 MB destination with random access, against one shared 32 MB L3. It is not a
cache-resident rehash at these sizes — it is saturated DRAM/TLB traffic.

### 1.4 Ruled out

* **GC / allocation / off-heap arena.** alloc_space is identical (8.24 vs 8.22 TB over 4 runs);
  `runtime.madvise` +0.012 pts, `memclrNoHeapPointers` −0.015 pts, `mallocgc*` +0.04 pts,
  `markrootSpans` +0.0015 pts. The arena change did what it claimed — Go-heap churn from
  `allocIntSubEntries`+`allocPackedSubEntries` fell 28.4 → 23.3 GB per run (−18 %) — but that
  is 1.1 % of the run's allocation and produced no measurable GC change. **The shared
  reservation is not the regression and is worth keeping on its own merits.**
* **The expr commits.** They are net **negative** CPU everywhere (§3) and Q18's regression
  survives in a stage (`cappedPartialAgg`) whose profile is 96 % hash-table frames. bisect
  carries both expr commits and is 42 % faster on Q18 than base.
* **Skew / task split.** cand splits the stage into 13 tasks vs 14 (scan-affinity byte
  balance), worth ≈ +7.7 % per-task rows. Adjusted, the per-task time is still +35 %. Total
  epochs across the stage are invariant to the split (same rows, same epoch size), so the
  conversion count does not move with it.
* **Errors / retries / spill.** None in any arm (§4).

---

## 2. Wait side — first SF100 block + mutex profiles

### 2.1 Mutex (delay = time OTHER goroutines waited; the stack shown is the lock HOLDER)

Totals over 4 runs, 3 workers: base **1 527.0 s**, cand **1 435.4 s**, bisect **1 628.1 s**.
Per worker per suite run that is **127 s / 120 s / 136 s**, against 1 122 / 1 101 / 1 085 CPU-s
of actual work — i.e. **lock waiting is ~11–12 % of the CPU the worker consumes**, and ~4 % of
its 16-vCPU capacity.

| site | base | cand | bisect | share (cand) | what it is |
|---|---|---|---|---|---|
| `runtime.unlock` ← `mheap.allocSpan` | 501.3 s | 490.3 s | 553.5 s | 34.1 % | **global heap lock on large-span allocation** |
| `worker.(*unpartitionedStageSink).Consume` | 524.5 s | 467.6 s | 518.6 s | 32.6 % | **one `sync.Mutex` serialising every morsel worker's row append** |
| `runtime._LostContendedRuntimeLock` | 217.0 s | 200.2 s | 221.9 s | 13.9 % | unattributable runtime-lock contention (same lock family) |
| ↳ of which via `batch.NewVectorWithScale` | 143.2 s | 133.4 s | 152.4 s | 9.3 % | scan/decode vector allocation |
| ↳ of which via `BytesColumn.PreAllocBytes` | 99.0 s | 93.6 s | 109.6 s | 6.5 % | string column backing |
| ↳ of which via `exec.gatherBuildVector` | 86.3 s | 83.4 s | 96.9 s | 5.8 % | join gather output |
| `partitionedShuffleSink.appendAndMaybeFlush` | 20.4 s | 11.7 s | 14.5 s | **0.8 %** | the old 64 % lever — **now dead** |

Two headline corrections to the standing lever list:

* **`partitionedShuffleSink.appendAndMaybeFlush` is no longer the dominant mutex.** It was
  64 % of mutex block pre-campaign; it is **0.8 %** here. The double-buffered flush fixed it.
* **The dominant application lock is now `unpartitionedStageSink.Consume`** — 95.8 % of its
  delay is `sync.Mutex.Unlock`, i.e. real contention, not `Cond.Wait`. The double-buffered
  flush already moved the s2 encode outside the lock, but `appendBatchRowsBulk` — the row
  memmove into the accumulator — still runs *under* it, so every morsel worker in a
  linear-parallel fragment serialises on one memcpy.
* **The single largest mutex site overall is the Go runtime's heap lock** (`mheap.allocSpan`,
  34 % + most of the 14 % lost-contention bucket). This is the price of 2.06 TB of allocation
  per run in large spans; every one of the wadjet frames under it is a >32 KB `make`.

### 2.2 Block (goroutine parked time)

Totals over 4 runs, 3 workers: base **79.14 hrs**, cand **74.64 hrs**, bisect **77.73 hrs**.

| site | base | cand | bisect | share | classification |
|---|---|---|---|---|---|
| `uploadManager.acquireSlot` | 136 399 s | 124 565 s | 140 029 s | 47.9 % | **by design** — the QoS gate throttles background uploads to `uploadSlotsBusy=2` while a foreground task runs (8 when idle). Deep queue, deliberate. |
| `consumeMorsels` | 20 268 s | 19 362 s | 19 240 s | 7.1 % | morsel workers idle: 75.9 % `selectgo` (**waiting for the next morsel** = source starvation), 15.7 % `widthGate.claim` (token admission), 8.4 % downstream push |
| `partitionedShuffleSink.appendPartition` | 5 650 s | 5 766 s | 5 893 s | 2.0 % | 83 % inside `writeDirectChunk` |
| `DecodeAheadIter.decodeLoop` | 5 244 s | 4 833 s | 4 906 s | 1.8 % | decode-ahead ring full/empty |
| `objstore.MinIOStore.Put` | 4 139 s | 4 064 s | 3 991 s | 1.5 % | S3 round-trips |
| `shuffleDecodeAhead.worker` | 2 859 s | 2 823 s | 2 923 s | 1.0 % | shuffle decode ring |
| `widthGate.claim` | 3 053 s | 2 986 s | 3 011 s | 1.1 % | width tokens |
| **`filePrefetcher.take`** | **745 s** | **727 s** | **699 s** | **0.26 %** | the 08-16 straggler tier — **not present in this window** |

Remaining ~30 % is structurally-idle polling goroutines (`dispatchLoop` 6 655 s,
`startLongTaskWatcher` 5 363 s, `taskPeakHeapTracker` 5 263 s, `heartbeatLoop` 2 320 s).

**Nothing on the wait side moved between arms** — every site is within ±10 % across all three,
with the sign following each arm's wall (shorter wall ⇒ denser contention). The two-level
change is a pure CPU/critical-path change on the aggregate stage.

**The un-overlapped prefetch-take wait named by the 08-16 straggler verdict is absent from
this window** (0.26 % of block delay, 62 s per worker-run). Either the runs were warm enough
that the prefetch was covered, or that tier only appears in the degraded straggler mode. It is
not a lever in these four runs.

---

## 3. Where the wins came from — the expr commits

Package rollup, flat CPU (`internal/engine/expr` + the filter adapters that live in `worker`):

| bucket | base | cand | Δ per run |
|---|---|---|---|
| `internal/engine/expr` (incl. `FilterPredicate` closures) | 839.3 s | 711.3 s | **−32.0 s** |
| `worker.compileFilterExprs.wrapCompiledFilter.func1` | 51.1 s | 0 | **−12.8 s** |
| **total** | **890.4 s** | **711.3 s** | **−44.8 CPU-s per suite run (−1.33 pts)** |

Frame by frame (flat, s over 4 runs), exactly as predicted by the two commits:

| frame | base | cand | note |
|---|---|---|---|
| `ColRef.resolve` | 62.4 | 19.2 | −69 %; `sync.Once.Do` → double-checked atomic, now inlines |
| `sync.(*Once).Do` | 8.5 | 0.2 | gone from the row loop |
| `ColRef.Eval` / `EvalInt64` / `EvalFloat64` | 103.1 / 77.3 / 44.3 | 80.2 / 55.2 / 37.3 | −22 % / −29 % / −16 % |
| `BinOpFloat64.EvalFloat64` | 66.1 | 41.0 | −38 %; delegate re-inlined |
| `BinOpFloat64.resolveOpCode` | 10.3 | 0.6 | opcode resolved with the mode |
| `BinOpNumeric.resolveMode` | 10.3 | 3.0 | −71 % |
| `CmpTemporalLit.EvalBool` | 35.6 | **0** | collapsed into `FilterPredicate` at compile time |
| `In.EvalBool` / `Like.EvalBool` | 5.6 / 5.8 | **0 / 0** | ditto |
| `CmpTemporalLit.EvalBoolNull` | 63.8 | 47.6 | −25 % (called directly, no wrapper frame) |
| `worker…wrapCompiledFilter.func1` | 49.9 | **0** | replaced by `expr.FilterPredicate.func1/2` (59.2 + 15.2, the leaves inlined *into* it) |

**The whole CPU tax the correctness campaign was costed at in the 2026-08-21 memo
(+0.95 pts ≈ +33 CPU-s/run) has been reclaimed and then some (−1.33 pts ≈ −45 CPU-s/run).**
It shows up in the filter-heavy queries: Q19 −12.5 %, Q12 −9.3 %, Q14 −7.9 %, Q21 −7.9 %,
Q01 −7.8 %, Q11 −7.5 %, Q13 −7.0 %, Q22 −6.3 %, Q09 −5.2 %, Q05 −4.7 % task-seconds.

**`1a39e1e`'s own stated intent — reduce two-level conversion CPU — failed.** Conversion CPU
went **up 54 %** (79.5 → 122.4 CPU-s/run) and the probe benefit it was bought for shrank
(21.1 → 19.7 CPU-s/run). Group-index CPU across the whole suite:

| flat CPU per suite run | base | cand | bisect |
|---|---|---|---|
| two-level frames (int + packed) | 128.5 | 160.9 | **0** |
| flat `intHashTable.*` | 283.4 | 282.7 | 319.9 |
| flat `packedHashTable.*` | 20.7 | 21.8 | 39.3 |
| **total group index** | **432.6** | **465.4** | **359.2** |

Turning the index off adds 55.1 CPU-s/run of flat-table work and removes 128.5–160.9, for a
net **−73.4 CPU-s/run vs base, −106.2 vs cand** (−2.2 % / −3.2 % of worker CPU).

---

## 4. Sanity

* **Row counts identical** across all three arms for all 22 queries (diff of the per-query
  `N rows` column: empty).
* **Value signatures identical** base ↔ bisect for all 22 queries, all 4 runs.
  One difference in cand: `Q19 vsig c0:5.985878904e+08` in 3 of 4 runs vs `…903e+08`
  elsewhere. Relative delta 1.7e-10, at the 10th and last printed significant digit of a
  `sum(l_extendedprice*(1-l_discount))` over 600 M rows. **bisect runs the same binary as cand
  and reproduces base's value in all 4 runs**, so this cannot be a code-level semantic change —
  it is float summation-order nondeterminism across parallel partial aggregation
  (ADR-0013 legal-nondeterminism class). Worth a note, not an investigation.
* **Zero task failures** in any arm (`success=false` count = 0 × 3).
* **No retries, no timeouts, no stalls, no spills.** The `retry|timeout|stall` grep hits are
  field names in `msg="scan decode-ahead query stats"`; the `spill` hits are `spillDir`
  config. Warning mix identical across arms (`accounting drift high/critical`,
  `dataplane connection ended`, `decoded-rowgroup cache pressure shed`). The 3 coordinator
  `ERROR`s per arm are the NATS client parser line every worker emits at connect.

---

## 5. What justified the structure, and what it is actually worth

`e93edd8` (G6, 2026-08-17) shipped the two-level index on this evidence
(`docs/benchmarks/high-card-aggregation-gap-2026-08-17.md`, item G6(c)):

> Consume-path measurement (5900X, near-unique, min of 5): single-int 8M groups
> **783 → 758 ms (−3.2 %)**, packed-two-int64 8M **1144 → 1170 ms (+2.2 %)**; peak RSS filling
> 32M int keys 1809 → 1685 MB.

That is: **−3.2 % on one single-threaded micro-shape, +2.2 % (a loss) on the other, and −7 %
peak RSS.** Nothing at SF100 scale, nothing concurrent, nothing with a capped epoch. The memo
itself already recorded the residual that the SF100 numbers now confirm — a two-level lookup
costs ~33 % more than a flat one at 8 M entries because the sub-table header is a dependent
load in front of the entry load.

Measured cost of that structure on SF100, three arms, same window:

| | base | cand | bisect |
|---|---|---|---|
| Q18 wall, steady mean | 18.8 s | 24.7 s | **10.9 s** |
| Q18 task-seconds per run | 193.2 | 228.7 | **112.5** |
| Q18 partial-agg stage, mean task | 6.96 s | 10.13 s | **2.25 s** |
| suite task-seconds per run | 1 359.1 | 1 345.9 | **1 248.1** |
| suite steady-mean wall | 177.4 s | 178.4 s | **170.7 s** |

The bisect arm's Q18 (10.9 s) is **at or below the 2026-08-16 record (11.0 s)**, and it gets
there while carrying the expr commits. ClickBench, meanwhile, is flat between the release and
the candidate (cold 160.6 vs 162, hot 83.9 vs 85) — the shape the structure was built for is
not visibly paying for it either — but the two-level-OFF arm now shows ClickBench is paying
handsomely: OFF costs it **+20 % hot** (§8). The structure earns its keep on **unbounded**
aggregates and loses 3-4x on **bounded** (capped-epoch) ones; that distinction, not the corpus,
is the fix.

---

## 6. Recommendations (superseded in part by §8 — read §8 for the final position)

> **Superseded.** §6.3 recommended defaulting `WADJET_TWO_LEVEL_HT` to OFF, conditional on the
> then-pending ClickBench arm. That arm landed (run `20260822-014659`, c6a.4xlarge, 3 tries):
> index OFF costs ClickBench **cold 172.5 s / hot 101.1 s** against the candidate's 160.6 / 83.9
> and the release's 162.3 / 85.2 — **+20 % hot, broad across 26 of 43 queries**. The global flip
> is **rejected**. §6.1's mechanism and §6.2's wait-side levers stand as written; the final
> disposition of the index is **§8**.

### 6.1 Q18 — the architectural fix

The load-factor gate is not the defect and neither is the threshold. **The defect is that a
flat→bucketed conversion exists at all on a shape whose table is torn down every 1.4–2.5 M
groups.** Converting is a full extra rehash of everything live; at SF100 that rehash measures
**~675 ns/entry**, 22–27× the 25–30 ns the policy was calibrated on, because in production it
runs on 8–10 tasks concurrently against a shared L3. No placement of the conversion point can
repay a cost that large inside one 128 MB epoch — base converts too early and pays it 8 times,
cand converts too late and pays 1.5× as much 9 times, and both lose to never converting by
3–4× on the stage.

In order:

1. **Never convert. Decide the layout once, at sink construction, and build bucketed or flat
   from the start.** `TwoLevelDirectBuilds` already exists for the NDV-hint case. Where no hint
   is available, the decision can be made from what the sink already knows *before* it is
   expensive: the capped partial aggregate knows its own byte cap and therefore its own maximum
   group count — an epoch that cannot exceed ~2.5 M groups can never reach the size where
   bucketing wins, and should be born flat and stay flat. That is a property of the operator's
   configuration, not a runtime bet, and it removes Q18's entire tax without a threshold.
2. **Charge the index honestly against the epoch budget, or stop charging it.** The bucketed
   form costs the epoch 38–44 % of its capacity (5 → 8 → 9 flushes on the same input, tracked
   peak 13 → 285 → 423 MB). Whatever the accounting asymmetry is between the flat table's
   off-heap reservation and the bucketed form's arrays, it is currently converting a structural
   choice into a **60–80 % increase in the number of epochs**, which is what multiplies the
   conversion count. This is worth confirming in `groupMemoryUsage`/the tracker path
   independently of (1) — it would also apply to any future layout change.
3. **If a conversion must stay, size the destination at the DOUBLED capacity.** The commit
   rejected this on a single-threaded 16 M-group micro-bench (2 204 vs 2 058 ms, +8.2 %), but
   that bench cannot see what the SF100 profile does: at flat capacity the conversion lands the
   table at 70 % load and `growSub` fires for all 256 buckets *inside the conversion loop*
   (76 % of `growSub` samples). The micro-bench's +8.2 % is measuring a DRAM-bound scatter on
   an idle core; the production cost is a scatter *plus* an immediate full re-grow. This is the
   fallback, not the fix.
4. **Do not touch `twoLevelConvertAt`.** Raising it is a threshold tweak that moves Q18's
   epochs under the bar by luck; the next shape with 3 M groups per epoch reproduces the whole
   thing.

**Also ship, independently of the above:** the shared off-heap arena from `1a39e1e` (correct on
its own evidence, −18 % Go-heap churn from the bucket allocators, no measurable cost), and
export `exec.TwoLevelConversions` into the `task completed` log line. The counter exists and is
incremented in four places but is never read anywhere — surfacing it would have turned this
whole investigation into one grep.

### 6.2 Top-5 wait-side sites and the lever each implies

Per worker per suite run.

| # | site | cost | lever |
|---|---|---|---|
| 1 | **Go runtime heap lock** (`mheap.allocSpan` + most of `_LostContendedRuntimeLock`) | ~60 s mutex delay/worker/run (34 %+14 % of mutex) | Cut large-span allocation, don't tune the lock. The callers are `NewVectorWithScale` (9.3 %), `PreAllocBytes` (6.5 %), `gatherBuildVector` (5.8 %) — i.e. **pool/reuse the scan-output and join-gather vector backing**, which is the same target as the standing `SetFrom` lever. 2.06 TB/run through >32 KB spans is the input to this number. |
| 2 | **`unpartitionedStageSink.Consume` mutex** | 39–44 s/worker/run (32.6 % of mutex), 95.8 % real `Mutex.Unlock` contention | Move `appendBatchRowsBulk` **out of the lock**: give each morsel consumer its own accumulator and take the sink lock only to hand off a full buffer — exactly the shape that took `partitionedShuffleSink.appendAndMaybeFlush` from 64 % to 0.8 %. Highest-confidence structural win on the wait side. |
| 3 | **Morsel starvation** (`consumeMorsels` → `selectgo`, 75.9 % of its block) | 1 217–1 302 s/worker/run parked | Morsel workers are waiting for input, not for the sink. This is the 37 %-utilization story. Lever: deeper source-side read-ahead / earlier prefetcher start (the 08-15 "start the prefetcher at Init" item), and larger morsel batching where the source is a shuffle read. |
| 4 | **Upload-slot admission** (`uploadManager.acquireSlot`) | 10 380–11 670 s/worker/run parked (47.9 % of block) | **Working as designed** (`uploadSlotsBusy=2` while a foreground task runs). Do not "fix" this by widening it blindly. The number to watch is whether any task's *completion* waits on it — `shuffle task completed (async upload pending)` fires 664× per arm (166/run), so the queue is normally drained off the critical path. Worth an explicit measurement before touching. |
| 5 | **Width-token admission** (`widthGate.claim`) | 249–254 s/worker/run parked (15.7 % of `consumeMorsels` block) | Constant across all three arms and across the 08-16 window — a stable admission cost, not a regression. Lowest priority of the five. |

Not a lever in this window: `filePrefetcher.take` (0.26 % of block delay). The 08-16 straggler
tier did not appear in any of these 12 runs.

### 6.3 Verdict on `1441ca4`

**Split it.**

* **KEEP `87c0b30` + `29cd11f` + `1441ca4`'s expr half.** −44.8 CPU-s per suite run
  (−1.33 pts), visible on 10 queries, zero row-set risk, and it repays the entire measured CPU
  tax of the correctness campaign. This is the arc's real win and it is currently invisible
  because Q18 eats it.
* **HOLD `1a39e1e`'s conversion-point change behind the switch — and go further: default
  `WADJET_TWO_LEVEL_HT` to OFF.** The evidence for turning the whole structure off is stronger
  than the evidence for either gate:
  * it is the only arm that is faster on the steady mean (170.7 s vs 177.4 / 178.4);
  * it is the only arm whose suite task-seconds fall materially (−8.2 %);
  * it restores Q18 to the 08-16 record (10.9 vs 11.0 s) — a 42 % improvement on the query;
  * it also improves Q20, the packed-mode high-cardinality query (−7.7 % task-seconds);
  * it costs nothing measurable anywhere else (per-query deltas outside Q18/Q20 are within run
    scatter and have no consistent sign);
  * the structure's own shipping evidence was −3.2 % on one micro-shape and **+2.2 % on the
    other**, single-threaded, with no capped epoch;
  * rows and value signatures are bit-identical to base in all four runs.
  * **RESOLVED — the ClickBench arm landed and rejects the flip.** Index OFF costs ClickBench
    hot +20 % (83.9 → 101.1 s), broad across 26 of 43 queries. The answer is therefore 6.1(1) —
    decide the layout at construction from the sink's own bounds — **not** a global default
    flip. See §8.

Shipping the candidate as-is would ship a suite that is 0.6 % *slower* than the release on the
steady mean while carrying a −1.33 pt CPU win it never gets to spend.

---

## 7. Follow-up: what the morsel producer is doing (wait-side #3), and the upload queue (#4)

### 7.1 Producer:consumer shape — 1 : 15

`runFragmentLinearParallel` (executor_fragment.go:1287-1296) starts **exactly one producer
goroutine** per task — `go func() { prodErr <- d.run(prodCtx, src) }()` — feeding **k
consumers** through a `chan morsel` of capacity `2k`. Decode is deliberately single-threaded
*inside the source* ("WSHF chunk / parquet row-group state machines keep their unguarded
cursors"), but the source is not the decoder: `scan.readRowGroupNative` fans out per COLUMN on
its own errgroup, and the shuffle side has `shuffleDecodeAhead.worker` goroutines. So the real
shape is *one dispatch goroutine* in front of *a decode-ahead pool*.

Measured k = **15.0** on every one of the 8 412 morsel-parallel fragments across the three arms
(c7gd.4xlarge, `defaultCPUTokenCapacity = GOMAXPROCS-2 = 14`, so 1 free baseline + 14 tokens).

### 7.2 The dispenser's own instrument answers it outright

`msg="morsel parallel fragment done"` / `"...breaker consume done"` are logged unconditionally
(2 812 / 2 788 / 2 812 fragments per arm, 4 runs). Σ over all fragments:

| arm | Σelapsed | k | Σk·elapsed | Σprocess | Σdry | Σwidth_wait | **Σproducer_wait** | eff. width | consumer parked | producer blocked |
|---|---|---|---|---|---|---|---|---|---|---|
| base | 2 713.7 s | 15 | 40 705.8 s | 7 828.4 s | 16 794.3 s | 3 119.9 s | **0.0 s** | **2.88 / 15** | 41.3 % | **0.00 %** |
| cand | 2 618.2 s | 15 | 39 273.5 s | 7 398.3 s | 15 931.2 s | 3 044.0 s | **0.1 s** | **2.83 / 15** | 40.6 % | **0.00 %** |
| bisect | 2 543.5 s | 15 | 38 152.8 s | 7 614.4 s | 15 788.2 s | 3 075.3 s | **0.0 s** | **2.99 / 15** | 41.4 % | **0.00 %** |

By stage class (base): scan-* eff. width **2.44**/15 with 46.0 % parked; join-* **4.12**/15 with
44.3 %; \*aggregate\* **0.63**/15.

Two facts settle the direction:

* **`dispenser_producer_wait_ms` = 0.0 s out of 2 713.7 s of fragment elapsed — 0.00 %.** The
  producer was NEVER blocked in `admit` on the dispenser's in-flight byte budget. The consumers
  are never what holds it back.
* **`window_full_ms` on the decode side is 183.7 s against `decode_ms` 3 220.0 s — 2.9 %**
  (shuffle: 12.9 s against 73.0 s). The decode-ahead ring is essentially never full either.

So this is **not** consumer over-provisioning in the benign sense of "the producer can't hand
off fast enough to a queue that is already full". The queue is *empty* 41 % of the time and
*full* 3 % of the time. The producer is the constraint.

### 7.3 The producer is neither CPU-saturated nor I/O-bound — it is **admission-starved**

Scan and shuffle decode-ahead counters, 3 workers × 4 runs (final cumulative snapshot per
worker):

| | base | cand | bisect |
|---|---|---|---|
| scan `decode_ms` (real decode work) | 3 220.0 s | 3 015.4 s | 3 000.9 s |
| scan **`token_stall_ms`** | **2 540.0 s** | **2 320.9 s** | **2 310.1 s** |
| scan `window_full_ms` (ring full = consumer-limited) | 183.7 s | 169.2 s | 144.5 s |
| scan `pressure_stall_ms` | 169.4 s | 91.0 s | 166.4 s |
| scan `decode_bytes` | 609.0 GB | 609.3 GB | 599.1 GB |
| **token_stall / (decode + all stalls)** | **41.6 %** | **41.5 %** | **41.1 %** |
| shuffle `decode_ms` / **`token_stall_ms`** / `pread_ms` | 73.0 / **167.6** / 101.9 s | 71.5 / **159.3** / 107.2 s | 73.0 / **167.5** / 92.0 s |
| **shuffle token_stall share** | **66.1 %** | **65.4 %** | **66.1 %** |

Cross-check against CPU: `decode_ms` ÷ 4 = 805 CPU-s/run, and the decode frames in the CPU
profile (zstd `decodeSync` 547 + `storage/parquet` 130 + `engine/scan` 70) sum to ≈ 747
CPU-s/run — the decoders are CPU-bound *while they are allowed to run*. I/O is not the
constraint either: shuffle `pread_ms` 102 s against 73 s of decode, and `filePrefetcher.take`
is 0.26 % of all block delay.

**The producer is queued behind the CPU-token pool, and the policy that queues it is explicit
in the code.** `cpu_tokens.go`:

* capacity = `GOMAXPROCS-2` = **14** per worker, worker-wide;
* morsel consumers use the **blocking** `enqueueWaiter` path, and are **FIFO with strict
  priority**: *"a queued consumer holds an admitted morsel, so feeding it beats widening
  decode"*;
* the decode-ahead side uses **`TryAcquire` only** (`scan/decode_ahead.go:547`,
  `worker/shuffle_decode_ahead.go:430`), which **returns 0 whenever any consumer is queued**
  (`cpu_tokens.go:74-75`).

That is a self-reinforcing starvation loop, and every term of it is measured here:

1. decoders are shut out of tokens (41–66 % of their wall) → the morsel channel drains;
2. consumers go dry (41 % parked, effective width 2.9 of 15) → the few that get a morsel queue
   for a token (`Σwidth_wait` = 3 120 s, 40 % of `Σprocess`);
3. any queued consumer makes `TryAcquire` return 0 → decoders stay shut out → back to (1).

Per worker per run this is ~1.5 decoder goroutines actually decoding and ~1.2 more queued,
against 15-wide consumer fans that are idle 41 % of the time, on a 14-token pool shared by
~4 concurrent tasks (`dispatch_concurrency=12` cluster-wide).

### 7.4 Therefore — the lever

**Not** more scan-side parallelism (the decoders already cannot get tokens for the parallelism
they have). **Not** deeper prefetch or readahead (I/O is 0.26 % of block delay and
`window_full` says the ring is not starved of space). **Not** benign over-provisioning either —
the parked consumer time costs nothing *by itself*, but the token demand of a 15-wide fan whose
effective width is 2.9 is exactly what starves the decoders under the strict-priority rule.

**The lever is admission-policy inversion in `cpuTokens`:**

* the priority comment is written for a *full* channel ("a queued consumer holds an admitted
  morsel"), but the measured regime is an *empty* one — `producer_wait = 0.00 %`,
  `window_full = 2.9 %`, `consumer dry = 41 %`. When the channel is empty, feeding the queued
  consumer is precisely what cannot happen until a decoder runs;
* concrete change: make the decode-ahead side a first-class waiter (or give it a small
  **reserved** floor of tokens the consumer FIFO cannot take), and gate consumer widening on
  the dispenser's own occupancy — `d.inFlight` / `len(d.ch)` are already tracked, and
  `consumerDryNs` vs `widthGate.claimWaitNs` is the discriminator the code comment
  (`morsel_dispenser.go:86-92`) already names as "whichever dominates names the plateau's
  pacer". Here dry-wait dominates claim-wait 16 794 s to 3 120 s — **the dispenser, not the
  token pool, is pacing the consumers, and the token pool is pacing the dispenser**;
* second-order: size k from the producer's measured feed rate rather than from
  `cpuTokens.Capacity()`, so a fragment does not claim 15-wide fans it will use 2.9 of.

Expected magnitude: the decode side is currently losing 2 540 s (scan) + 168 s (shuffle) of
wall per 4 runs to token stalls, i.e. **~226 s per worker per suite run against a ~180 s
suite wall** — the single largest recoverable block in this window, an order of magnitude
larger than anything in §1. It should be the next arc, ahead of the group-index work.

### 7.5 Sanity on wait-side #4 — the upload queue is background, confirmed

`acquireSlot` (upload_manager.go:681) is called from `runJob`, which `startJob`
(upload_manager.go:662-668) launches on a **detached goroutine** (`go func(){ … }()`). The
shuffle task path (`executor.go:1526-1565`) finalizes every partition file on local disk, hands
the S3 PUTs to the manager via `StartTask`, logs `shuffle task completed (async upload
pending)` and **returns immediately**. Nothing in the query path waits: `uploadManager.Flush`
(the `inflight.Wait()` at upload_manager.go:584) and `Drain` are called **only from
`Worker.Drain` at worker shutdown** (worker.go:1277, 1320) — there is no per-query wait.

The upload-manager counters prove it empirically:

| | base | cand | bisect |
|---|---|---|---|
| uploads completed | 42 624 (121 GiB) | 40 803 (120 GiB) | 39 541 (122 GiB) |
| **uploads CANCELLED (never ran)** | **20 242 (107 GiB)** | **21 477 (109 GiB)** | **21 597 (101 GiB)** |
| uploads failed | **0** | **0** | **0** |
| `upload_yield_ms` (admission wait) | 77 976 s | 69 520 s | 79 698 s |
| `upload_pause_ms` (in-flight pause gate) | **0 s** | **0 s** | **0 s** |

**32 % of all queued uploads — 107 GiB — were cancelled outright, with zero failures and no
effect on any query's rows or wall.** If that admission queue sat on a critical path, discarding
a third of it would have been visible. It is not. Note also the anti-correlation with wall:
bisect has the *shortest* suite wall and the *highest* `acquireSlot` block time (140 029 s), which
is what background queueing looks like — the same uploads compressed into less time.

**Verdict: benign. Do not widen `uploadSlotsBusy`.** The one path that *is* on the critical
path is the sync fallback at executor.go:1546-1552 (taken only when local adoption fails, e.g.
cross-device rename), and it deliberately bypasses `acquireSlot` entirely. `upload_pause_ms=0`
confirms the in-flight pause gate never engaged in any arm.

---

## 8. Final recommendation

ClickBench settles the remaining question. Turning the index off costs **+20 % hot
(83.9 → 101.1 s)** and **+7 % cold**, and it is broad, not Q33-specific: **26 of 43 queries are
≥ 20 % slower hot** (Q26 +91 %, Q29 +98 %, Q20 +60 %, Q30 +55 %, Q15 +46 %, Q8 +50 %). So:

| corpus | aggregate shape | index verdict |
|---|---|---|
| **ClickBench** | standalone / final aggregate, **unbounded** — one index for the whole input | **big win** (−20 % hot) |
| **SF100 Q18** | `cappedPartialAgg`, **bounded** — index finalized + rebuilt every 128 MB | **3–4× loss** on the stage |

A global flip is wrong. The discriminator is not the corpus and not a size threshold — it is
**whether the index outlives one epoch**, and that is known at sink construction.

### (1) The decision rule, at sink construction

**Rule.** A `HashAggregate` whose owner will finalize and rebuild it on a byte cap is **born
flat and never converts**. Its maximum group count is `Gmax ≈ C / s` (C = epoch byte cap,
s = per-group state: key SoA + accumulator arrays + index slot at the load factor). Bucket only
if `Gmax ≥ G*`. Unbounded aggregates (final/standalone) keep today's path, including the
NDV-hint direct build.

**Calibration from this window.** Q18: C = 128 MB, s ≈ 24 B of key+accums + ~23 B of index at
70 % load ⇒ `Gmax ≈ 2.5 M` — which is exactly what the bisect arm measures (12 497 812 out_rows
÷ 5 flushes = 2.50 M groups/epoch). At `Gmax` = 2.5 M the bucketed form loses 3–4× on the
stage. ClickBench's winning sinks are unbounded (~6 M groups per partitioned sink on Q33 and no
teardown). **Set `G* = 4 M` for bounded sinks** — derived from the two measurements, not tuned
against a target. Note that `twoLevelConvertAt` (1 M) is the wrong number to compare against
here: it is a *live-count* crossover for a table that will keep growing, and a bounded sink's
table by construction will not.

**Robust form, if the `s` estimate is thought too fragile:** a bounded sink is flat
unconditionally. That is defensible on its own — an index that is rebuilt every epoch can never
amortize a conversion, and the bucketed probe is ~33 % dearer for the epoch's whole remainder
(the residual `two_level_hash.go` already records). The `Gmax` form is preferred because it
keeps the door open for a future large `C`.

**Code sites.**

| what | file:line | note |
|---|---|---|
| capped partial agg constructs its aggregate | `internal/worker/shuffle_partial_agg.go:147-159` (`ensureAgg` → `exec.NewHashAggregate(p.groupBy, p.aggs)`) | the cap is already in hand: `p.capBytes`, set at `newCappedPartialAgg` / `newCappedPartialAggPartitioned` (:69, :78), default `defaultPartialAggCapBytes = 128 << 20` (:63-67). Plumb it in — a `SetGroupBound(n int)` / `SetEpochCap(bytes int64)` setter called **before `Init`** (feedback_setter_before_start). |
| layout decision (int mode) | `internal/engine/exec/aggregate.go:1490-1500` — the `twoLevelToggle.On() && h.GroupNDVHint > 0 && intHTInitSize >= twoLevelConvertAt` branch that does `newIntTwoLevelTable(...)` + `TwoLevelDirectBuilds.Add(1)` | add the bound test here; a bounded sink falls to the `else` (flat) arm. |
| layout decision (packed mode) | `internal/engine/exec/aggregate.go:1538-1548` | same change, same shape. |
| conversion gate | `internal/engine/exec/aggregate.go:2611-2676` (`convertsToTwoLevel`, `maybeConvertIntIndex`, `maybeConvertPackedIndex`) | a bounded sink must return false unconditionally — cheapest correct place is one field test at the top of `convertsToTwoLevel`. |
| merge-path conversions | `internal/engine/exec/aggregate.go:4983` (`mergeIntGroupSoA`), `:5137` (`mergePackedGroupSoA`) | they call the same gate, so they inherit the rule — verify, don't duplicate. |

This is worth ~80 task-seconds per suite run on Q18 alone (193.2 → 112.5) and costs ClickBench
nothing: ClickBench aggregates are unbounded and take the unchanged path.

### (2) The accounting asymmetry

**One defect is definite, new in `1a39e1e`, and it is a double count — not real footprint.**
`intTwoLevelTable.MemoryUsage()` (and the identical `packedTwoLevelTable.MemoryUsage()`) in
`internal/engine/exec/two_level_hash.go`:

```go
n := int64(len(t.arena))                 // <- the WHOLE shared reservation, always
for i := range t.subs {
    if !t.subs[i].arena {                // <- plus every bucket that has LEFT it
        n += int64(cap(t.subs[i].entries))
    }
}
```

`releaseArenaSlot` only zeroes `t.arena` when the **last** bucket leaves (`arenaLive == 0`).
While `0 < arenaLive < 256` the vacated fraction of the arena is charged **twice** — once as
arena, once as the bucket's new array — an over-charge of up to one full arena (33.5 MB at
8 192 slots/bucket, 67 MB after the doubling). `growSub` is exactly the path that creates this
window, and `1a39e1e` moved 256 `growSub` calls *inside* the conversion, so the window is
entered on every conversion. **Fix: charge `arenaLive * capPerSub` instead of `len(t.arena)`.**
This is the cand-vs-base delta: epoch peaks 293 → 423 MB, flushes 8 → 9.

**The base-vs-bisect gap (5 → 8 flushes, tracked peak 13 → 285 MB) is a second, separate
effect and I have not localized it.** It is not the arena (base has none), and the two forms'
slot counts are provably equal at every live count (`subCapFor(1.0 M)` = 8 192/bucket =
2.097 M slots = a flat table's `2^21` at the same point; both double at 1.468 M). It is charged
through `HashAggregate.StateBytes` (`aggregate.go:3907`) → `groupMemoryUsage`
(`aggregate.go:516`), which sums `intGroupIndex.MemoryUsage()` and `intTwoLevel.MemoryUsage()`.
**Do not guess it from logs — one instrumented run settles it:** log `StateBytes()`,
`intIndexLen()`, and each index's `MemoryUsage()` at every `cappedPartialAgg.flush`, for one
Q18 task, with `WADJET_TWO_LEVEL_HT` on and off. If rule (1) lands first this stops mattering
for Q18, but it will bite the next bounded sink, so it should still be closed.

### (3) Keep the `1a39e1e` gate. Do not revert, do not adopt doubled-capacity.

Definite recommendation: **keep the load-factor gate and the flat-capacity destination.**

* The gate's only measured effect on the corpus that will **still** convert after rule (1) —
  unbounded aggregates — is **positive**: ClickBench hot 83.9 s (cand, new gate) vs 85.2 s
  (release, old gate), cold 160.6 vs 162.3.
* The conversion path stays load-bearing there and cannot simply be deleted: the memo already
  records that every ClickBench run has `GroupNDVHint = 0`, so those sinks reach the bucketed
  form **only** by converting. `TwoLevelDirectBuilds` never fires on that corpus.
* The whole SF100 penalty of the new gate lives inside the capped-epoch case, which rule (1)
  removes entirely. Reverting to the old count gate would trade a measured ClickBench win for
  nothing.
* **Doubled-capacity destination: no.** It was rejected on a same-window measurement
  (`BenchmarkAggIntCappedEpochs`: flat-capacity 2 058 ms vs doubled 2 204 ms, +8.2 %), and the
  in-loop `growSub` storm that makes flat-capacity look bad on SF100 is *legitimate* work on an
  unbounded sink — it is the doubling the table genuinely owed. My §1.3 framing of it as "the
  fallback" was correct for the capped case and is moot once the capped case never converts.
  Revisit only if a ClickBench CPU profile shows `growSub`-from-`convert` taking the same 76 %
  share it takes on SF100.

Ordering matters: **land (1) first**, then (2), then re-profile ClickBench. Do not touch the
gate.

### (4) Keep, unconditionally

* **The shared off-heap arena** (`allocOffheapArena` / `newIntTwoLevelTableSub` /
  `newPackedTwoLevelTableSub`). Correct on its own evidence: Go-heap churn from the bucket
  allocators 28.4 → 23.3 GB per run (−18 %), one `MADV_HUGEPAGE` mapping instead of 256
  4 KiB-paged ones, and no measurable GC or `madvise` cost anywhere in the profile. Its only
  problem is the accounting double count in (2), which is a two-line fix, not a reason to drop
  it.
* **Export `exec.TwoLevelConversions` and `exec.TwoLevelDirectBuilds`** into the `task
  completed` log line (`internal/worker/worker.go:2119-2136`, alongside `operator_peaks`).
  Both counters exist and are incremented in four places each; neither is read anywhere. Every
  quantitative claim in §1 had to be reconstructed from CPU-profile edge weights and
  `flushes=` counts because those two integers are not printed.

**Net position.** Ship the expr commits (−44.8 CPU-s/run, −1.33 pts, ten queries). Ship the
arena. Add rule (1) and the accounting fix; keep the gate. That combination should give the
candidate's ClickBench number (83.9 hot), the bisect arm's Q18 (≈ 10.9 s), and the expr win at
the same time — none of which the current `1441ca4` delivers together.

---

## Appendix — reproduction

```bash
cd scratchpad/diff
# merged per-arm profiles
go tool pprof -proto -output base-cpu.prof ../results/base/profiles/worker-worker-*-cpu.prof
# frame-level diff (flat or cum), regex-filtered
python3 cmp.py base-top-flat.txt cand-top-flat.txt flat 'TwoLevel|convert' 30
# package rollup
python3 pkg.py base-top-flat.txt cand-top-flat.txt
# per-query task-seconds, three arms
python3 ../perq.py
# Q18 partial-agg stage concurrency
python3 ../conc.py
# epoch counts
zcat ../results/*/wlogs/wlog-*.gz | grep 'msg="shuffle partial agg"' | grep in_rows=50000000
```

## Addendum (2026-08-22, post-implementation) — ClickBench bisect arm is confounded

`WADJET_TWO_LEVEL_HT=0` gates exactly three lines in `aggregate.go` (the int and
packed NDV-hint direct builds and `convertsToTwoLevel`); it does not gate
partitioned aggregation, parallel emit, the off-heap arena, packed keys, or the
two-level DISTINCT rewrite. The ClickBench "index OFF" arm (run 20260822-014659)
slowed 27/43 queries by a run-wide ~25% — including scalar aggregates (Q2, Q3,
Q29), a filtered `COUNT(*)` (Q20), queries with no aggregate at all (Q23–Q26),
and string-keyed GROUP BYs that `two_level_hash.go` never converts (Q33/Q34) —
while the clearest packed high-card shape (Q35) moved +2.3%. That shift is
instance/run drift, not the switch. The ClickBench two-level win is therefore
UNPROVEN in either direction; a clean same-instance interleaved ON/OFF
ClickBench bisect is still owed. §8's design (bounded ⇒ born flat, unbounded
unchanged) does not depend on it — shipped as dcc95a8 + d13eff7; see
docs/benchmarks/high-card-aggregation-gap-2026-08-17.md §G6 for the derivation
(G* = 4M, Gmax = C/s) and the accounting finding (real RSS, fixed via
MADV_DONTNEED on departed buckets, not an over-charge).
