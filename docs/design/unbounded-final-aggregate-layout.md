# Group-index layout for UNBOUNDED final aggregates — the input-row bound

**Status:** implemented 2026-08-22. Kill switch `WADJET_TWO_LEVEL_ROW_BOUND=0`.
**Extends:** ADR-0014 (group-index layout is decided at sink construction) — see its
2026-08-22 amendment.
**Code:** `internal/engine/exec/two_level_hash.go` (`twoLevelAmortizeMultiple`,
`twoLevelMinAmortizeRows`, `rowBoundToggle`),
`internal/engine/exec/aggregate.go` (`SetInputRowBound`, `indexLayoutStaysFlat`),
`internal/distributed/messages.go` (`OpSpec.InputRowBound`),
`internal/coordinator/execute_stage_dag.go` (`aggregateInputRowBound`,
`buildAggregateFragment`), `internal/worker/executor_fragment.go`
(`buildFragmentHashAggregate`).

---

## 1. What was left open

ADR-0014 settled the layout question for **bounded** sinks: an aggregate its owner
finalizes and rebuilds every `C` bytes (`worker.cappedPartialAgg`) can hold at most
`Gmax = C/s` groups and cannot outlive one epoch, so it is born flat and never
converts. It deliberately left **unbounded** sinks — final and standalone aggregates —
on the unchanged adaptive path, and named the residual:

> Q18's *unbounded* `final_aggregate-7` is untouched and remains the residual
> (4.14 s index-off vs 5.2–6.5 s bucketed).

Two SF100 windows measured that residual, same-window, on several arms of the same
binary (`docs/benchmarks/sf100-window2-analysis-2026-08-22.md` §1.1 and §8.2 lever #4;
`…-window3-…` §2.6 and §8.3 item 2):

| arm | Q18 `final_aggregate-7` span (steady) |
|---|---|
| index OFF (`WADJET_TWO_LEVEL_HT=0`, w1 bisect) | **4.14 s** |
| base, old count gate | 5.16–5.20 s (both windows) |
| `1a39e1e` load-factor gate | 5.79–6.52 s clean, **7.7–8.7 s when it draws a tail** |

It is the whole of Q18's remaining gap to the index-off arm, it is worth
−1.5 to −2.3 s of steady suite wall, and window 3 showed it is also a **second
bimodal source**: Σ task-seconds moves 6 % but the max task doubles (2.69 → 5.88 s).
Q20's `final_aggregate-9` is the same shape and moved the same way (w1: −7.7 %
task-seconds with the index off). Nothing else in the suite converts at all: over four
runs the conversion counters are Q18-final 378, Q20-final 356, Q20 `scan-4` 209,
Q11-final 4, everything else **0**.

## 2. Mechanism — why the unbounded final aggregate is bucketed, and why that loses

`boundedIndexStaysFlat` returned `(false, 0)` the moment it saw no epoch cap, so a final
aggregate went straight onto the adaptive path. There, `convertsToTwoLevel` fires when
two conditions hold at the end of a batch:

1. `live >= twoLevelConvertAt` (1 M) — the structural size crossover, and
2. `(live+incoming)*10 > slots*7` — the table is one batch from its 70 % load factor,
   i.e. from a doubling the conversion can displace.

Both tests look **backwards**, at what the table already holds. Neither looks forward at
what is left. But a conversion is paid once, in full, at the moment it fires — ~675 ns
per live entry in production, 22–27× the 25–30 ns it was calibrated on, because 8–10
tasks run the scatter concurrently against one shared 32 MB L3 (ADR-0014) — and it is
repaid *only* by the work that follows it: later probes that miss less, and later
rehashes that are 256 cache-resident scatters instead of one DRAM-wide one.

Q18's `final_aggregate-7` is the shape with the least possible "afterwards":

* **Merge mode ⇒ rows ≈ groups.** Its input is already partial-aggregated, one row per
  group per upstream producer, so the index is probed about *once per group*. A
  scan-level aggregate at the same cardinality is probed 17–20× per group — that is
  where cheaper lookups have something to be cheap about.
* **The conversion lands near the end of the fill.** The first load-factor crossing
  past 1 M live is at ~1.4 M; the sink finishes at a per-partition ~6.25 M (24
  partitions, `HashPartitionCount(3) = 24`, over ~150 M partial rows), and each morsel
  clone holds a fraction of that. Several clones convert at ~1.4 M and then stop
  growing. Nothing is displaced and nothing is repaid — plus the conversion targets the
  flat table's own slot count, so the bucketed table is born at 70 % load and every
  bucket regrows immediately (the `growSub` storm, 76 % of whose samples came from
  inside the conversion).

So the layout was chosen by a bet whose payoff term was never in the gate. **And the
payoff term is exactly known**: the coordinator already has the producing stage's
`StageOutput.PartitionRows`, reduced across its tasks, and a final aggregate task reads a
named contiguous partition range of it. Every aggregate emits at most one group per
input row, so that sum bounds both the rows the index will probe and the groups it will
hold — exactly, not as an estimate.

## 3. Decision rule

Same shape as ADR-0014's: **the layout is a property of the sink's declared
configuration, decided once before the first row.** There are now two bounds, evaluated
in one place (`HashAggregate.indexLayoutStaysFlat`), either of which pins the index flat
for life (merge paths included):

```
epoch byte cap C  ⇒  Gmax = C / perGroupStateBytes
                     flat  iff Gmax < G* (= twoLevelBoundedMinGroups, 4 M)     [ADR-0014]

input row bound R ⇒  flat  iff R < R* (= twoLevelAmortizeMultiple × twoLevelConvertAt,
                                          8 × 1 M = 8 M)                       [this memo]
```

`R` is the **row** count, not the group count, and that is what keeps the shapes the
structure was built for on the adaptive path: a high-cardinality scan reads far more
rows than it holds groups, so it clears `R*` long before its group count matters, while
a merge aggregate has `R ≈ G` and clears it only when it is genuinely huge.

**Derivation of `R*` — three measurements, bracketing it:**

| shape | rows | groups | bucketed vs flat | verdict under R* = 8 M |
|---|---|---|---|---|
| `BenchmarkAggIntCardinalitySweep`, near-unique 4 M | 16 M | 4 M | **+25/+31 %** (loss) | — (bound only applies where declared) |
| SF100 Q18 `final_aggregate-7`, per task | ~6.25 M | ~6.25 M | **+25 to +57 %** (loss) | flat ✓ |
| `BenchmarkAggIntCardinalitySweep`, near-unique 16 M | 16 M | 16 M | −4.1/−11 % (win) | adaptive ✓ |
| ClickBench Q33-class scan aggregate | ~100 M | ~6 M | win (the structure's reason to exist) | adaptive ✓ |

8 M sits inside the bracket (above the 6.25 M production loss, below the 16 M measured
win). It is expressed as a **multiple of `twoLevelConvertAt`** rather than as a second
absolute number so the two halves of the gate stay calibrated together — including under
the `WADJET_TWO_LEVEL_AT` override the corpus oracles use to exercise the bucketed path
at group counts nowhere near a million. `TestRStarBracketsTheMeasurements` pins all
three numbers, so moving the multiple has to argue with them.

**Two safety properties the rule rests on.**

* **Monotone.** A declared bound can only *remove* a conversion, never add one, so no
  shape can become bucketed that was not bucketed before
  (`TestRowBoundIsMonotone` checks the switch-on arm against the switch-off arm across
  four thresholds and six bounds).
* **Exact bounds only.** The error that costs is a bound that reads LOW where the truth
  is high — it would pin a genuinely high-cardinality index flat. So every uncertain
  case yields 0 = unknown, and the adaptive path is unchanged: more than one dependency,
  a non-partitioned upstream, a worker that reported no `PartitionRows`, a misaligned
  vector, or a task whose inputs came from probe-split / skew-split / round-robin file
  grouping rather than from `partitionRangeForWorker`. Estimates are deliberately not
  routed here — neither the planner's `InputRowHint` nor `GroupNDVHint`. Over-stating is
  free (it keeps the adaptive path), which is why a morsel clone inherits its parent's
  whole-task bound rather than a divided one.

## 4. Plumbing

```
producing stage (exchange-repartition)
  → worker reports PartitionRowCounts()             (executor.go:1536)
  → StageOutput.PartitionRows                       (coordinator/stage_output.go)
  → aggregateInputRowBound(stage, inputs, w, tasks) (execute_stage_dag.go)
  → OpSpec.InputRowBound                            (distributed/messages.go)
  → HashAggregate.SetInputRowBound  BEFORE Init     (worker/executor_fragment.go)
  → indexLayoutStaysFlat, once, in resolveIndices   (exec/aggregate.go)
```

Nothing new is measured, transported or estimated: `PartitionRows` has existed since the
skew-split work, and this is the second consumer of it.

## 5. Observability

* `two_level_born_flat` on the worker's `task completed` line already counts every sink
  whose layout was pinned at construction; the row-bound rule increments the same
  counter, and `two_level_conversions` for the affected stages must go to **0**.
* `HashAggregate.IndexFlatReason()` distinguishes `"epoch-cap"` from `"row-bound"`.
* The coordinator's `dispatchComputeStage` line carries `agg_task_row_bound` for
  aggregate stages — the exact per-task figure the decision was taken from. Together
  with the counter above, "did the unbounded final aggregate take the flat layout" is a
  one-grep answer per run. (ADR-0014's lesson: a counter nobody can read is a counter
  nobody reads.)

## 6. Expected shape on the next SF100 window

Predictions, to be checked against the run rather than assumed:

* `dispatchComputeStage stage_id=final_aggregate-7` logs `agg_task_row_bound ≈ 6.2 M`
  (< R*), and Q18 `final_aggregate-7` reports **`two_level_conversions = 0`** where it
  reported 378 over four runs.
* Q18 `final_aggregate-7` span **5.2–6.5 s → ~4.1 s**, the index-off arm's number, and
  the 7.7–8.7 s tail mode disappears: the max task should track Σ task-seconds instead
  of doubling. Query wall Q18 **12.7 → ~10.9–11.2 s**.
* Q20 `final_aggregate-9` conversions 356 → 0; task-seconds −5 to −8 %.
  Q20 `scan-4` (209 conversions) is **untouched** — it is a scan-level aggregate whose
  row count is large, exactly the shape the rule leaves alone, and its own verdict is
  still open.
* Suite steady mean −1.5 to −2.3 s, all of it Q18 + Q20; every other query's counters
  unchanged (they never converted).
* Rows and SF100 value fingerprints bit-identical: the layout is value-preserving.

## 7. What the ClickBench check must show

**Nothing must move.** The bound is produced only by the coordinator's DAG dispatch, and
the ClickBench harness runs the embedded single-process API (`wadjet.Open`,
`benchmarks/clickbench/hits_exec_test.go`), so `OpSpec.InputRowBound` is never set and
`SetInputRowBound` is never called on that path — the switch is inert there **by
construction**, not by threshold. The arm to run is therefore a no-op sanity check
rather than an A/B:

* `WADJET_HITS_PART=<hits part> go test -run TestHitsOptimizationInvariance ./benchmarks/clickbench/`
  must pass with `two-level-row-bound` in the enumerated toggle list. **Run
  2026-08-22 against `hits_0.parquet`: PASS, 75 queries × 24 configurations, the
  new toggle among them.**
* A hot/cold ClickBench pair should land within run scatter of the release
  (v0.17.0: cold 161.5 / hot 84.6), and Q33's `two_level_conversions` /
  `two_level_direct_builds` must be unchanged. **Not run here** — the timed suite
  needs the full 100-part `hits` set and only one part is present on this machine.
  Owed before the next release.
* Any movement on Q33-class shapes would mean the bound reached a path this memo says it
  cannot reach, and is a bug, not a tuning question.

**This memo does not depend on the owed clean ClickBench interleaved A/B** that w2 lever
#4 was blocked on. That A/B asked "does bucketing pay for ClickBench at all"; this change
does not touch ClickBench, and it keeps bucketing wherever the row count says the
conversion can be repaid. The A/B is still owed for the separate question of whether
`twoLevelConvertAt` itself is calibrated.

**Not verified here:** the timed ClickBench suite and the SF100 predictions in §6. The
invariance arm ran and passed; the wall-clock claims rest on a deploy, as they must.

## 8. Rejected alternatives

* **Raise `twoLevelConvertAt`.** The threshold tweak ADR-0014 already rejected for the
  bounded case, for the same reason: it moves Q18 under the bar by luck and the next
  shape with 3 M groups per sink reproduces the whole regression. It also cannot
  separate Q18-final (6.25 M rows, 6.25 M groups, a loss) from ClickBench Q33 (~6 M
  groups per sink, a win) — **group count alone cannot tell those two apart**, which is
  precisely why the rule is stated in rows.
* **Never convert; keep only NDV-hint direct builds.** Tempting, because every measured
  bucketed *win* on the DAG could in principle be a direct build. But the fragment path
  sets no `GroupNDVHint` at all today, so this would remove the bucketed layout from
  distributed execution entirely — a much larger claim than the evidence supports, and
  the global index-off arm it rests on is the one ADR-0014 rejected on design grounds.
* **Divide the bound by the clone count.** `cloneNDVDivisor` does exactly this for the
  NDV presize, but only for PARTITIONED clones, which own disjoint 1/k slices. Morsel
  clones in `runBreakerConsumeParallel` take dynamic row slices and can each see the full
  key set, so dividing would under-state — the one direction that costs. Clones inherit
  the whole-task bound.
* **Feed the single-process planner's row estimate in.** `findScanRowEstimate` /
  `groupKeyNDVEstimate` are estimates; an underestimate pins a huge index flat. The rule
  takes exact bounds only, which is also what makes the ClickBench claim in §7 provable
  rather than measured.
* **Decide at the conversion point instead ("convert only if ≥ ρ·live rows remain").**
  This is the same arithmetic, but it re-opens the runtime bet ADR-0014 closed, and the
  quantity a clone would need — its own remaining share of the input — is exactly what a
  dynamically-fed clone does not know. Construction time is where the information is.

## 9. Micro-benchmarks

`BenchmarkAggIntFinalMerge` (added with this change) is the unbounded counterpart of
`BenchmarkAggIntCappedEpochs`: one aggregate, no epoch cap, rows = groups, at the SF100
per-task cardinalities. Three arms in one interleaved window, `-benchtime 1x -count 7`,
5900X:

| shape | arm | sec/op | conv/op | B/op | allocs/op |
|---|---|---|---|---|---|
| 6.25 M (Q18 per task) | flat (index off) | 628.5 m ± 21 % | 0 | 11.15 Ki | 24 |
| | adaptive (before) | 593.8 m ± 7 % | **1** | **448.0 Mi** | 793 |
| | rowbound (after) | 580.2 m ± 24 % | **0** | **11.15 Ki** | 24 |
| 2.3 M (Q20 per task) | flat (index off) | 181.4 m ± 3 % | 0 | 11.15 Ki | 24 |
| | adaptive (before) | 198.4 m ± 4 % | **1** | **64.02 Mi** | 281 |
| | rowbound (after) | 182.4 m ± 3 % | **0** | **11.15 Ki** | 24 |

The wall column is a wash at 6.25 M and −8.1 % at 2.3 M, which is the expected reading:
**this machine cannot see the production conversion cost.** ADR-0014 recorded the same
negative result (5900X, p = 0.38, n = 7 interleaved) and explained it — the 675 ns per
entry is a property of 8–10 tasks scattering concurrently against one shared L3, not of
the algorithm. What the local run does establish is the mechanism and the deterministic
side: the conversion fires exactly once per sink on the adaptive arm and never on the
row-bound arm, and the bucket allocators' Go-heap churn goes from 448 MiB to 11 KiB per
sink (the local counterpart of SF100's `allocIntSubEntries` −82 %).

Neutrality on shapes the rule does not touch, same machine, `-count 10`, with a
base-vs-base control run to establish the noise floor (`benchstat base=… new=…`):

| benchmark | before₂ | after | Δ |
|---|---|---|---|
| `HashAggregate` | 290.6 µ ± 4 % | 286.8 µ ± 2 % | ~ (p=0.63) |
| `HashAggregateHighCardinality` | 289.1 µ ± 4 % | 294.3 µ ± 1 % | ~ (p=0.11) |
| `HashAggregatePackedNearUnique/two-int64` | 2.498 m ± 2 % | 2.504 m ± 2 % | ~ (p=0.85) |
| `HashAggregatePackedNearUnique/four-int32` | 2.518 m ± 4 % | 2.636 m ± 3 % | +4.7 % |
| `HashAggregateSumAvgSameColumn` | 1.142 m ± 5 % | 1.113 m ± 2 % | −2.6 % |
| geomean | 903.9 µ | 908.8 µ | **+0.55 %** |

The two "significant" entries are inside the machine's own drift: the **base-vs-base**
control (two runs of the *unmodified* tree) shows −17.7 % on `HighCardinality` and
+28.3 % on `SumAvgSameColumn` between samples. `B/op` and `allocs/op` are byte-identical
on every one of them. `AggIntCardinalitySweep` and `AggIntCappedEpochs` (all arms,
`-benchtime 1x -count 5`) are unchanged, including `conv/op`.

The struct cost of the change is one `int64` and one `uint8` at the **end** of
`HashAggregate` — the reason field is a byte enum rather than a string, because the tail
offsets of that struct are measurably load-bearing for the consume loop (the comment
above `routeOnce` records the last time that bit someone).
