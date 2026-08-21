# SF100 v0.16.0-correctness regression — CPU profile attribution

**Date:** 2026-08-21 · **Release engine:** 23abd8e · **Method:** merged 3-worker CPU profiles,
fraction-of-total comparison (module path normalized `citc-tech`→`derekmwright`, `(inline)`
suffixes stripped), plus `-peek` call-graph walks and alloc_space heap diff.

Working files: `scratchpad/diff/` (`new-cpu-merged.prof`, `old14-cpu-merged.prof`,
`old-cpu-merged.prof`, `cmp.py`, `pkg.py`, `*-top-cum.txt`).

---

## 0. Baseline correction — read this first

The task named the baseline as "engine ~bb07f06, 2026-08-12". Those are two different
states:

- `bb07f06` is dated **2026-08-15 21:05** (`docs(tpch): straggler tier named`). It is the
  commit the **08-16 000900 record run** was built from — the run whose numbers the task
  quotes verbatim (155.9 s ≈ 2m36 steady, **Q09 16.1 s**).
- `~/wadjet-artifacts/20260812-reprofile/` is from **2026-08-12**, three days and one major
  perf arc *earlier*. It also predates the extent-index/readahead win, and its run 2 is a
  **degraded straggler run** (9m30.5s vs run 1's 5m57.5s). It is not a valid absolute
  reference; it is only usable for composition.

So the real regression window is `bb07f06..23abd8e` = **236 commits**, and it contains
**two** arcs, not one:

| arc | dates | contents |
|---|---|---|
| **Perf round-4 wave 2** | 08-16 → 08-17 | partitioned parallel aggregation, packed 128-bit composite group keys, **two-level group index (e93edd8)**, off-heap group tables, chunked key arena, sel-decode, lengths-only decode, prepared regexp, two-level distinct rewrite, LIMIT pushdown |
| **Correctness campaign** | 08-18 → 08-21 | ~60 fixes: three-valued logic, BinOpNumeric, bounded probe fan-out, drain-productivity gate, outer-join residuals, window-on-DAG, set ops on DAG, … |

**No local worker CPU profile of the bb07f06 (2m36) state exists.** The perf arc is
therefore a suspect on *equal footing* with the correctness campaign — and the profile says
it is the larger CPU mover.

To bound that, I added a second, closer-in-time control:

- **`old14`** = `~/wadjet-artifacts/20260814-trino-compare/wadjet-20260814-121720/` — 4 runs,
  clean, best steady **3m07.169s**, same instance class. Primary control.
- **`old`** = the 08-12 reprofile set. Secondary; the lever ranking in
  `docs/benchmarks/lever-ranking-2026-08-12.md` is built on it.

---

## 1. Totals

| | release (23abd8e) | control (08-14) | 08-12 reprofile |
|---|---|---|---|
| profile window (per worker) | 414.51 s | 855.84 s | 930.9 s |
| suite runs covered | 2 | 4 | 2 |
| **worker CPU-s (3 workers merged)** | **6 904.10** | **15 041.46** | 8 213.58 |
| **CPU-s per suite run** | **3 452.1** | **3 760.4** | 4 106.8 |
| avg wall per run | 207.3 s | 213.1 s | 464.0 s |
| utilization of 48 vCPU | 34.7 % | 36.6 % | 18.4 % |
| per-worker CPU imbalance (max/mean) | 1.039 | 1.016 | 1.08 |
| alloc_space per run | **2.09 TB** | 3.24 TB | — |
| `runtime.gcBgMarkWorker` (cum) | **0.45 %** | 0.81 % | 2.85 % |
| `runtime.asyncPreempt` (flat) | **0.27 %** | 0.88 % | 1.20 % |

Window arithmetic checks out on both sides (08-14: 252.9+202.4+209.9+187.2 = 852.4 s ≈
855.8 s; release: 414.5 s ≈ 225 s cold + 189 s steady), so the per-run normalization is sound.

> **The release build does 8 % LESS worker CPU per suite run, allocates 36 % less, and
> spends half the GC CPU of the 08-14 control — while running the suite 21 % slower than the
> 08-16 record.** No CPU-work explosion exists to find.

**Stated limitation:** the release profile mixes cold + steady in one window; the controls
mix 4 and 2 runs respectively. Fractions handle this. What fractions cannot handle is that
neither control is the 2m36 state — see §0.

---

## 2. Package rollup (flat CPU, % of total worker CPU)

| package | release | 08-12 | 08-14 | Δ vs 08-14 | s/run rel | s/run 08-14 |
|---|---|---|---|---|---|---|
| go-runtime | 30.762 | 38.209 | 33.039 | −2.278 | 1061.9 | 1242.4 |
| `engine/exec` | **25.189** | 18.817 | 22.418 | **+2.771** | 869.5 | 843.0 |
| compress (zstd/s2) | 21.922 | 23.595 | 23.196 | −1.274 | 756.8 | 872.2 |
| `engine/expr` | **6.015** | 4.384 | 5.158 | **+0.858** | 207.7 | 194.0 |
| `engine/batch` | 5.203 | 3.937 | 4.785 | +0.418 | 179.6 | 179.9 |
| `storage/parquet` | 3.940 | 4.747 | 4.267 | −0.327 | 136.0 | 160.5 |
| `worker` | 2.669 | 2.989 | 2.462 | +0.206 | 92.1 | 92.6 |
| `engine/scan` | 2.050 | 1.779 | 2.274 | −0.224 | 70.8 | 85.5 |

Only two wadjet packages grew materially: **`engine/exec` (+2.77 pts)** and
**`engine/expr` (+0.86 pts)**. Everything else fell.

---

## 3. Top-15 flat growers (fraction of total worker CPU)

Flat, not cum — cum is dominated by call-graph reshaping (`ChainDriver`, `nextProbeChunk`,
`executeIncomingTaskDelivery` are **renames**, not new work).

| Δ pts vs 08-14 | rel % | 08-12 % | 08-14 % | rel s/run | 08-14 s/run | frame | origin |
|---|---|---|---|---|---|---|---|
| **+2.899** | 2.899 | — | — | **100.1** | 0.0 | `exec.(*intTwoLevelTable).GetOrInsertAt` | perf arc e93edd8 |
| +1.616 | 1.616 | — | — | 55.8 | 0.0 | `parquet.decodeBitPackedRange` | perf arc (replaces `DecodeBitPacked`, −1.674) |
| +1.560 | 1.560 | — | — | 53.8 | 0.0 | `scan.resolveDictByteArrayScratch` | perf arc (replaces `resolveDictByteArray`, −1.800) |
| +0.673 | 4.259 | 2.989 | 3.585 | 147.0 | 134.8 | `exec.gatherBuildVector` | mix effect (see §5.2) |
| +0.667 | 0.667 | — | — | 23.0 | 0.0 | `exec.(*packedTwoLevelTable).GetOrInsertAt` | perf arc |
| +0.535 | 0.535 | — | — | 18.5 | 0.0 | `exec.(*packedHashTable).GetOrInsertNoGrow` | perf arc 1157974 |
| +0.467 | 0.467 | — | — | 16.1 | 0.0 | `scan.resolveNativeDictionaryScratch` | perf arc (replaces `resolveNativeDictionary`, −0.449) |
| **+0.458** | 0.458 | — | — | 15.8 | 0.0 | `expr.(*CmpTemporalLit).EvalBoolNull` | **campaign 01193fb** |
| **+0.453** | 0.453 | — | — | 15.6 | 0.0 | `expr.(*In).EvalBoolNull` | **campaign 01193fb** |
| +0.403 | 0.465 | 0.098 | 0.062 | 16.1 | 2.3 | `s2.load32` | shuffle compress (net s2 down) |
| +0.366 | 1.226 | 0.839 | 0.860 | 42.3 | 32.3 | `batch.(*BytesColumn).SetFrom` | mix effect (join gather) |
| +0.155 | 0.155 | — | — | 5.3 | 0.0 | `exec.mergeFlatAccumRow` | perf arc (arena) |
| +0.137 | 0.137 | — | — | 4.7 | 0.0 | `exec.(*intTwoLevelTable).growSub` | perf arc |
| **+0.109** | 0.109 | — | — | 3.8 | 0.0 | `expr.(*BinOpNumeric).EvalFloat64` | **campaign a8618a6** |
| **+0.079** | 0.079 | — | — | 2.7 | 0.0 | `expr.(*BinOpNumeric).resolveMode` | **campaign a8618a6** |

Largest declines: `parquet.DecodeBitPacked` −1.674, `scan.resolveDictByteArray` −1.800,
`exec.(*intHashTable).Get` −1.075, `zstd.decodeSync` −0.497, `runtime.memclrNoHeapPointers`
−0.486, `runtime.asyncPreempt` −0.609, `runtime.tryDeferToSpanScan` −0.129.

---

## 4. Named suspects — verdicts

### 4.1 BoolNullExpr three-valued filter evaluation — **CONFIRMED, ≈0.3–0.6 pts**

**Mechanism.** `01193fb fix(expr): PostgreSQL semantics for three-valued logic…` made
`EvalBoolNull` the semantic definition and turned `EvalBool` into a thin wrapper:

```go
func (e *CmpTemporalLit) EvalBool(b *batch.RecordBatch, row int) bool {
    v, null := e.EvalBoolNull(b, row)
    return v && !null
}
```

The per-row leaf therefore **split into two frames and stopped inlining into the filter
loop**. The work did not double — it moved and paid one extra call + a two-value return per
row.

| node (flat pts) | 08-14 | release `EvalBool` | release `EvalBoolNull` | net |
|---|---|---|---|---|
| `CmpTemporalLit` | 0.452 | 0.259 | 0.458 | **+0.265** |
| `In` | 0.427 | 0.040 | 0.453 | **+0.066** |
| `Like` | 0.036 | 0.004 | 0.039 | +0.007 |
| `Cmp` | 0.323 | 0.326 | (n/a) | +0.003 |

Whole WHERE-clause subtree (`worker.compileFilterExprs.wrapCompiledFilter.func1`, cum):
**8.84 % → 9.46 %, +0.62 pts ≈ 21 CPU-s/run.** `Filter.Execute`'s own flat cost *fell*
(0.84 → 0.78) — the loop is fine, the predicate leaf is the cost.

**Verdict:** real, measured, ≈0.6 % of worker CPU. Cannot account for +33 s.
**No kill switch exists** (this is semantics, correctly).

### 4.2 Outer-join per-candidate `Residual` + per-build-row matched tracking (#358) — **EXONERATED**

Three independent lines of evidence:

1. **The symbols are not in the profile at any sample count.** All 3 410 nodes of the
   release merge were enumerated with `-nodefraction=0`. `Residual`, `markMatchedBuildEntries`,
   `refMatched`, `FlushUnmatched`, `arenaMatched` — zero samples. TPC-H SF100 does not take
   the outer-join residual path (Q13's LEFT JOIN does not materialize one).
2. **The fast paths the Residual would disable are still on.** `join.go:2373` gates
   `inlineInt` on `h.Residual == nil`, and `join.go:2236` gates `lazyKeys` (late
   materialization) on the same. Both are live: `inlineIntProbe` = 27.4 % of probe CPU
   (5.16 % of total), `emitViewOutput` (the view/late-mat emitter) = 66.2 % of probe CPU.
3. **The bounded-output refactor that shipped with it does not bite.**
   `MaxProbeOutputRows = 16 × 2048 = 32768`; TPC-H per-batch fan-out never reaches it.
   `HashJoinProbe.NextOutput` (the resume entry) carries **0.72 %** of total CPU vs
   `Execute`'s 18.32 % — resumption fires on <4 % of probe calls. `nextProbeChunk` is a
   *rename* of the old `Execute` body (the old `Execute.func1` closure, 0.65 %, is gone and
   `genericProbe`, 0.64 %, replaced it at the same weight).

### 4.3 `FatalEvalPanic` recover frames on hot eval paths — **EXONERATED**

The recover sites are `Pipeline.Run` (per pipeline), `ChainDriver.Push` (per **batch** —
2048 rows), and the parallel-worker goroutine wrapper (per goroutine). None is per-row.

Profile confirms: **no** `runtime.gopanic`, `runtime.deferreturn`, or `runtime.gorecover`
samples on the release side, and `runtime.tryDeferToSpanScan` **fell** 0.241 → 0.112 pts.
Defer cost went *down*.

### 4.4 Plan-time validation walks — **EXONERATED**

Every `*[Vv]alidate*` frame in the release worker profile sums to **< 0.003 %** of CPU
(`worker.validateShuffleChunkBytes` 0.0017 %, `parquet.ValidateHeader` 0.00014 %, …).

---

## 5. What the diff actually surfaced

### 5.1 ★ Two-level group index **conversion** — the single largest new CPU consumer

`e93edd8 perf(exec): two-level group index for the int and packed key modes (G6)`
(2026-08-17 — **inside the regression window**, perf arc).

Call graph, release side:

```
exec.(*intTwoLevelTable).GetOrInsertAt      flat 200.18s (2.899%)  cum 215.64s (3.12%)
  ← convertIntHashTableToTwoLevel  156.66s  72.65%   ← REHASH, not lookup
  ← (*intTwoLevelTable).GetOrInsert  43.51s 20.18%
  ← (*HashAggregate).intGroupPhase1TwoLevel 15.47s  7.17%   ← the only actual lookups

convertIntHashTableToTwoLevel               cum 159.22s (2.31%)
  ← (*HashAggregate).maybeConvertIntIndex   135.60s  85.17%  (end-of-batch)
  ← (*HashAggregate).mergeIntGroupSoA        23.62s  14.83%  (merge path)
```

- **Cost: 159.22 s cum = 2.31 pts = 79.6 CPU-s per suite run, spent rehashing an
  already-built flat table.**
- **Benefit: 15.47 s = 0.22 pts = 7.7 CPU-s per run of two-level lookups.**
- **≈10:1 overhead-to-benefit on SF100 TPC-H.**

The packed side is fine — only 3.55 % of `packedTwoLevelTable.GetOrInsertAt` comes from
conversion (it is built bucketed from the NDV hint). **The defect is the int side.**

The design comment in `two_level_hash.go` claims the 1 M default keeps "every TPC-H shape"
flat. SF100 disproves it. Q18's inner
`SELECT l_orderkey, sum(l_quantity) FROM lineitem GROUP BY l_orderkey` runs 150 M rows over
a **near-unique int key** — millions of groups per partial sink — and *both* gates in
`convertsToTwoLevel` pass unconditionally:

```go
func convertsToTwoLevel(live, newGroups, rows int) bool {
    return twoLevelToggle.On() && live >= twoLevelConvertAt && newGroups*4 >= rows
}
```

For a near-unique key `newGroups == rows`, so the "still filling" growth test — the thing
meant to veto a losing bet — **can never veto**. Q18 is the worst regression in the suite
(+87 %) and is exactly this shape.

**Group-index layer, net flat:** aggregate-side hash-table cost went ≈4.05 pts (08-14:
`intHashTable.Get`-from-agg 1.22 + `GetOrInsertNoGrow` 1.64 + `GetOrInsert` 0.44 + `grow`
0.75) → ≈6.13 pts (release: intTwoLevel 3.04 + packedTwoLevel 0.72 + packedHashTable 0.63 +
intHashTable residue 1.75). **≈ +2.1 pts ≈ +72 CPU-s/run.**

*(Caller split verified: 08-14 `intHashTable.Get` was 69.8 % join / 26.9 % dual-int agg;
release is 96.9 % join / 0.8 % agg — the aggregate's flat table is gone, replaced by the
two-level structures. So this is not a join-mix artifact.)*

**Tempering:** `HashAggregate.Consume` runs 49.2 % under `cappedPartialAgg.consume`
(per-task shuffle-stage partial agg) and 47.5 % under `runBreakerConsumeParallel.func1` —
i.e. inside *parallel* work. Wall impact is bounded by the aggregate stage's critical path,
not by the full 80 CPU-s.

**Kill switch: `WADJET_TWO_LEVEL_HT=0`** (`internal/engine/exec/two_level_hash.go`).
Also `WADJET_TWO_LEVEL_AT=<n>` to raise the threshold.

### 5.2 `BinOpNumeric` resolves its mode **per row** via `sync.Once`

`a8618a6` / `01193fb` (campaign, #369). All three entry points open with `e.resolveMode(b)`:

```go
func (e *BinOpNumeric) Eval(b, row)        { e.resolveMode(b); … }
func (e *BinOpNumeric) EvalInt64(b, row)   { e.resolveMode(b); … }
func (e *BinOpNumeric) EvalFloat64(b, row) { e.resolveMode(b); … }

func (e *BinOpNumeric) resolveMode(b *batch.RecordBatch) { e.modeOnce.Do(func(){ … }) }
```

That is a `sync.Once.Do` (atomic load + branch) **per row**, plus an extra indirection layer
to the float delegate. Profile cost: `resolveMode` 0.079 + `Eval` 0.067 + `EvalFloat64`
0.109, and the delegate lost its inline (`BinOpFloat64.EvalFloat64` 0.362 → 0.460, +0.098).
**≈ +0.35 pts ≈ 12 CPU-s/run.**

**Kill-switch caveat:** `WADJET_INT_ARITH=0` exists — but it only flips *which mode* is
resolved. `resolveMode` still runs per row. **It will not bisect this cost away.**

### 5.3 Join-gather growth is a mix effect, not a mechanism

`emitViewOutput` 10.62 → 12.46 pts and `gatherBuildVector` 9.67 → 11.43 pts look alarming
but are composition-identical on both sides:

| callee of `gatherBuildVector` | release | 08-14 |
|---|---|---|
| `BytesColumn.SetFrom` | 58.03 % | 58.57 % |
| `BytesColumn.PreAllocBytes` | 2.55 % | 2.04 % |
| `BytesColumn.BulkCopy` | 1.77 % | 1.55 % |

Same shape, same per-call cost. The fraction rose because the release run's wall is
relatively more concentrated in join-heavy queries (Q09/Q18). Same story for
`Project.Execute` (2.74 → 3.47 pts; callee mix identical, `buildAggInputProjection.func3`
66.6 % → 70.1 %).

`batch.Vector.SetValue` (from `9537043 make Vector.SetValue loud`) is 5.36 % → 5.41 % of
`Project.Execute` — **no measurable cost from making it error.**

### 5.4 Things that got CHEAPER (so we don't chase them)

zstd decompress 19.08 → 17.60 pts; shuffle upload (`uploadManager.startJob`) 5.57 → 5.00;
s2 encode 2.77 → 2.76; `DecodeBitPacked`→`decodeBitPackedRange` net −0.06; dictionary
resolve net −0.22; GC halved; allocation −36 %/run. **Shuffle volume did not grow.**

### 5.5 Structural non-findings (checked, clean)

- No cross-join / nested-loop degeneration (`CrossJoin`, `nextCrossChunk`, `NestedLoop`,
  `SortMergeJoin` — zero samples both sides).
- Morsel parallelism structurally unchanged: `consumeMorsels` splits 75.1 % /
  `runFragmentLinearParallel.func6` and 24.8 % / `runBreakerConsumeParallel.func6` on the
  release side vs 74.0 % / 26.0 % on 08-14.
- `widthGate.claim`/`yield` costs are identical noise on both sides (tokens are not the
  regression — consistent with the 08-16 straggler attribution).
- Prefetcher and extent index are alive in the release build
  (`filePrefetcher.run` 2.26 %, `streamingShuffleReader.tryEnableExtentIndex` present).
  The 08-15/16 perf work was **not** accidentally reverted by the campaign.

---

## 6. The honest headline

**The correctness campaign is responsible for ≈ +0.95 pts (≈ 33 CPU-s per suite run,
≈ 1 % of worker CPU):** three-valued filter dispatch (+0.34–0.62) and `BinOpNumeric`
per-row mode resolution (+0.35). That is the entire measurable CPU tax of ~60 fixes. It
cannot produce +33 s of wall.

**The largest CPU-composition regression in the window belongs to the 08-16/17 perf arc**,
not the campaign: the two-level group index conversion, ≈ +2.1 pts / ≈ +72 CPU-s per run,
of which 79.6 CPU-s is rehash with 7.7 CPU-s of lookups to show for it.

**And even that does not explain the wall.** Worker CPU per run *fell* 8 %, allocation fell
36 %, GC halved, parallel structure is unchanged, worker imbalance is 1.04. At **34.7 %
utilization of 48 vCPU the suite is bound by waiting, not by instructions.** Going
189 s → 156 s at the release's CPU volume requires ~21 % more *overlap*, not less CPU.

**The instrument to close this is missing.** The release capture has CPU + heap only. The
08-12 capture had `profsnap/` with **block and goroutine base/final deltas per worker**
(the 08-12 block delta is 25.4 hrs of `delay`, dominated by `selectgo` 126 %, `Cond.Wait`
21.9 %, `chanrecv2` 18.4 %). Without the release-side counterpart, the wait-side story
cannot be attributed at all — and the memory index already names *un-overlapped
prefetch-take wait* and *shuffle-sink mutex 64 % block* as the standing levers there.

---

## 7. Ranked recommendations

Semantics are never weakened — every item below is a dispatch-shape or policy change.

1. **Re-instrument first.** Re-run SF100 with block + goroutine snapshots
   (`~/wadjet-artifacts/20260812-reprofile/profsnap.sh` already does base/final deltas per
   worker). The +33 s lives in the waiting and is invisible to a CPU profile. Everything
   below is worth doing regardless, but none of it is sized to +33 s.

2. **Fix the two-level conversion bet** (biggest CPU lever; 2.3 pts → ~0.2 pts).
   Bisect with `WADJET_TWO_LEVEL_HT=0` on an SF100 arm to confirm the wall share, then, in
   order of preference:
   - **(a)** Build bucketed *directly* from the planner's NDV hint (`TwoLevelDirectBuilds`
     already exists) whenever cardinality is known, so the rehash never happens at all.
     This is the architectural fix — the conversion exists only because the decision is
     being made too late.
   - **(b)** Make the growth test *predictive* rather than instantaneous: a near-unique key
     satisfies `newGroups*4 >= rows` on every batch, so the gate that was meant to veto a
     losing bet cannot fire. Require projected remaining rows to roughly double the table.
   - **(c)** Last resort: raise `twoLevelConvertAt` above SF100's per-sink group counts.
     This is a threshold tweak, not an architectural fix — prefer (a).

3. **Hoist `BinOpNumeric.resolveMode` out of the row loop** (+0.35 pts, free to reclaim).
   The plan now types projections (`cd92b79` declares scalar return types at registration;
   `2aa11fe`/`0e8c936` type computed projections), so the int/float mode is decidable at
   **compile time**. Failing that, resolve once per batch in a prepare/reset hook instead of
   per row through `sync.Once`. Values are unchanged either way — this only moves *when* the
   decision is made. Note `WADJET_INT_ARITH=0` does **not** remove this cost.

4. **Give the three-valued comparators a batch-level entry point** (+0.3–0.6 pts).
   Keep `EvalBoolNull` as the semantic definition (ADR-0012 stands), but let the filter
   compile to a kernel that fills a result vector + null bitmap per batch instead of calling
   a per-row wrapper that no longer inlines. This is the project's own "typed kernels, no
   per-row dispatch in hot paths" rule applied to the new node shape — it recovers the tax
   *and* removes the de-inlining, with bit-identical semantics.

5. **Then the standing lever:** `gatherBuildVector` → `BytesColumn.SetFrom` is 4.26 pts flat
   / 11.43 pts cum — the largest single steady-state consumer on both sides and already the
   named lever in the perf-clawback kickoff memo. Unchanged by this arc; still the biggest
   prize.

## 8. Kill switches relevant to bisection

| switch | env | relevance |
|---|---|---|
| `two-level-ht` | `WADJET_TWO_LEVEL_HT=0` | **★ primary** — removes the 2.3-pt conversion cost |
| (threshold) | `WADJET_TWO_LEVEL_AT=<n>` | raises the 1 M conversion point |
| `packed-keys` | `WADJET_PACKED_KEYS=0` | reverts composite group keys to the dual-int path |
| `partitioned-agg` | `WADJET_PARTITIONED_AGG=0` | reverts partitioned parallel aggregation (d9125a9) |
| `parallel-emit` | `WADJET_PARALLEL_EMIT=0` | reverts parallel emit of adopted partitions |
| `int-arith` | `WADJET_INT_ARITH=0` | ⚠ flips the mode only — does **not** remove per-row `resolveMode` |
| `sel-decode` / `lengths-only-decode` | `WADJET_SEL_DECODE=0` / `WADJET_LENGTHS_ONLY_DECODE=0` | the 08-17 scan-arc changes (`resolveDictByteArrayScratch`, `decodeBitPackedRange`) |
| `two-level-distinct` | `WADJET_TWO_LEVEL_DISTINCT=0` | the round-4 wave-2 distinct rewrite |
| `fastpath-strict` | `WADJET_FASTPATH_STRICT=0` | #308 — not implicated here (distributed path) |

No switch exists for three-valued logic, the bounded probe protocol, or the drain-productivity
gate — those are semantics/robustness and correctly unswitched.

Every switch above is covered by the optimization-invariance oracle
(`TestTPCHOptimizationInvariance`), so flipping one for a bisection run is safe by construction.
