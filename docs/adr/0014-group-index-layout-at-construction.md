# ADR-0014: Group-index layout is decided at sink construction, not by runtime conversion

Status: Accepted (born-flat rule landed 2026-08-22, `dcc95a8`; SF100-validated
the same day, run `20260822-032608`)

## Context

The two-level (bucketed) group index shipped 2026-08-17 (`e93edd8`, G6) as an
**adaptive runtime bet**: every `HashAggregate` starts flat and converts — a
full rehash of everything live — once it crosses a threshold. `1a39e1e` moved
the conversion point to the flat table's load-factor crossing so the rehash
would displace a doubling rather than add to one. Neither commit asked whether
a given sink should convert **at all**.

Same-window three-arm SF100 (2026-08-22 00:57–01:40 UTC; base `23abd8e` / cand
`1441ca4` / cand with `WADJET_TWO_LEVEL_HT=0`, same binary;
`docs/benchmarks/sf100-window-analysis-2026-08-22.md`):

- **Q18 steady wall 18.8 / 24.7 / 10.9 s** — a 2–4× loss, and switching the
  index off restores the 2026-08-16 record of 11.0 s.
- The whole delta is one stage: the `exchange-repartition`'s
  `worker.cappedPartialAgg`, which finalizes and **rebuilds** its aggregate
  every 128 MB of state. Mean task on the identical 50 000 000-row input
  **2.25 s flat / 6.96 s old gate / 10.13 s load-factor gate**, at 5 / 8 / 9
  epochs.
- The conversion costs **~670–675 ns per live entry in production**, derived
  independently from two arms — (6.96−2.25)/7/1.00 M and (10.13−2.25)/8/1.47 M —
  agreeing to 0.5 %. The design comment budgeted **25–30 ns**; production is
  22–27× that, because 8–10 tasks run the scatter concurrently against one
  shared 32 MB L3, making it DRAM/TLB-bound rather than a cache-resident rehash.
  It reproduces in the negative: on a 5900X the same shape shows **no** wall
  difference between converting and not (p = 0.38, n = 7 interleaved processes).
  *The local machine cannot see this cost* — which is why the calibration missed
  it.
- The bucketed layout also **inflated the epoch accounting**: 8 flushes against
  the flat form's 5 on the same input, tracked operator peak 13 → 285 → 423 MB.
  More epochs means the conversion is paid 7–9 times per task instead of once.

The second fact is structural, not numeric: **a sink torn down every epoch has
nothing to amortize a conversion against.** No placement of the conversion point
repairs that — the old gate converts early and pays it 8 times, the new gate
converts later and pays 1.5× as much 9 times, and both lose to never converting.

## Decision

**The layout is a property of the sink's configuration, decided once before the
first row, not a bet re-evaluated at runtime.**

- `HashAggregate.SetEpochByteCap(C)` declares that the owner finalizes and
  rebuilds this aggregate whenever `StateBytes` crosses `C` — a **bounded**
  sink. Called before `Init`.
- `resolveIndices` computes the group ceiling `Gmax = C / s`, where `s` is a
  **lower** bound on per-group state (`perGroupStateBytes`: index entry at the
  70 % load factor + key SoA + one flat-accumulator element per aggregate). A
  lower bound on `s` gives an *upper* bound on `Gmax`, so the layout is pinned
  flat only when even the most optimistic group count stays below `G*`.
- Below `G* = twoLevelBoundedMinGroups = 4 M` the index is **born flat and every
  conversion gate returns false for its whole life**, merge paths included.
- **Unbounded sinks — final and standalone aggregates, everything ClickBench
  runs — take the unchanged adaptive path**, NDV-hint direct builds included
  (`boundedIndexStaysFlat` returns immediately when no cap was declared).

`G*` is calibrated from two measurements and nothing else. Q18's partial
aggregate has C = 128 MB, s ≈ 46 B ⇒ `Gmax ≈ 2.9 M`, and the index-off arm
measures 12 497 812 out_rows ÷ 5 flushes = **2.50 M groups per epoch** — the
same number, at which bucketing is a 3–4× loss. Every measured *win* is
unbounded: ClickBench's high-cardinality GROUP BYs (~6 M groups per partitioned
sink on Q33) and `BenchmarkAggIntCardinalitySweep`'s 16 M near-unique arm
(−11 %). At 4 M groups that sweep measures the bucketed arm **+25 %** — a loss —
so 4 M is a floor on where bucketing could pay *even with* an unbounded tail,
and a bounded sink has none. `twoLevelConvertAt` (1 M) is deliberately not the
comparand: it is a live-count crossover for a table that will keep growing.

**Kill switches:** `WADJET_TWO_LEVEL_BORN_FLAT=0` restores runtime conversion for
bounded sinks; `WADJET_TWO_LEVEL_HT=0` removes the bucketed path entirely. Both
are in `internal/optswitch`, so the invariance oracle sweeps them.

## Consequences

- **Measured (window 2, 2026-08-22 03:10–04:10 UTC, four arms, same binary for
  B/C/D):** Q18 **19.3 → 12.7 s**, partial-aggregate stage span **11.98 →
  3.82 s** — identical to the index-off arm's 4.12 s — flushes **8 → 5**,
  conversions per task **8 → 0**, `cappedPartialAgg.consume` CPU −74 %,
  `allocIntSubEntries` churn −82 %. That is **58 % of the whole suite gain**
  (steady mean 173.5 → 162.2 s). ClickBench flat (cold 161.5 / hot 84.6 vs the
  release's 162.3 / 85.2): its aggregates are unbounded.
- The price of being born flat is +5 CPU-s per suite run of flat-table probing —
  a tenth of what it buys.
- **The window-1 flush asymmetry is closed for bounded sinks.** Every partial-agg
  task now logs `born_flat=true conversions=0 group_ceiling=2 917 776 flushes=5`,
  so the unlocalized 5→8 inflation was the bucketed layout charging the same
  128 MB budget. It can still exist on genuinely bucketed sinks — which have no
  epoch cap for it to shrink — and stays open there.
- **The counters are readable now** (`d13eff7`): `born_flat` / `group_ceiling` /
  `conversions` / `cap_mb` on `shuffle partial agg`, and
  `two_level_conversions` / `_direct_builds` / `_born_flat` on `task completed`.
  Every claim above had to be reconstructed from CPU-profile edge weights because
  those counters existed since 08-17 and were read nowhere.
- A future sink declaring a much larger `C` can legitimately reach `G*` and will
  be born bucketed. The rule survives that; a threshold would not.
- Q18's *unbounded* `final_aggregate-7` is untouched and remains the residual
  (4.14 s index-off vs 5.2–6.5 s bucketed). Closing it is blocked on the owed
  ClickBench A/B below.

## Alternatives rejected

- **Disable the index globally.** It was the only window-1 arm faster on the
  steady mean (170.7 vs 177.4 / 178.4 s) and −8.2 % on suite task-seconds, so it
  led until the ClickBench arm ran. **Honest caveat: that ClickBench bisect is
  confounded, so the flip is rejected on design grounds, not on it.** The
  "index OFF" arm (run `20260822-014659`) slowed 27 of 43 queries by a run-wide
  ~25 %, including scalar aggregates, a filtered `COUNT(*)`, queries with no
  aggregate at all, and string-keyed GROUP BYs the code never converts, while the
  clearest packed high-card shape moved +2.3 %. That is instance/run drift, not a
  switch that gates three lines. **The ClickBench two-level win is UNPROVEN in
  either direction; a clean same-instance interleaved A/B is still owed.** This
  decision does not depend on it — bounded sinks are flat because a per-epoch
  rebuild cannot amortize a rehash, and unbounded sinks are left as they were.
- **Raise `twoLevelConvertAt`.** A threshold tweak that moves Q18's epochs under
  the bar by luck; the next shape with 3 M groups per epoch reproduces the whole
  regression.
- **Size the conversion's destination at doubled capacity** so the bucketed table
  is born at 35 % load and no bucket regrows. `BenchmarkAggIntCappedEpochs`:
  flat-capacity 2 058 ms vs doubled 2 204 ms (**+8.2 %**) — doubling only moves
  the per-bucket doublings into the conversion's own scatter, which then works
  over twice the bytes and is DRAM-bound. The in-loop `growSub` storm that makes
  flat-capacity look bad on SF100 (76 % of `growSub` samples came from inside the
  conversion) is *legitimate* work on an unbounded sink, and moot once bounded
  sinks never convert.
- **Revert to the old count gate.** Kept. The only corpus that still converts
  after this ADR is the unbounded one, where the load-factor gate measures
  positive (ClickBench hot 83.9 s vs the release's 85.2 s); reverting trades that
  for nothing. The gate's entire SF100 penalty lived in the capped-epoch case.
- **Drop the shared off-heap arena.** Kept on its own evidence: −18 % Go-heap
  churn from the bucket allocators, one `MADV_HUGEPAGE` mapping instead of 256
  4 KiB-paged ones, no measurable GC cost. Its accounting problem was **real RSS,
  not an over-charge**, and was fixed at the source: a departing bucket returns
  its pages (`memory.DiscardSlice`, MADV_DONTNEED on whole pages) and
  `MemoryUsage` charges the mapping minus what went back.

## Related

- ADR-0006 (never-OOM; the 2026-08-17 off-heap group-state amendment), ADR-0011
  (same-window pairs; mechanism metrics decide), ADR-0002
- `docs/benchmarks/high-card-aggregation-gap-2026-08-17.md` §G6 (the `Gmax = C/s`
  and `G* = 4 M` derivation, and the arena accounting finding)
- `docs/benchmarks/sf100-window-analysis-2026-08-22.md` §8 + its
  ClickBench-confound addendum; `…-window2-analysis-2026-08-22.md` §1
- `internal/engine/exec/two_level_hash.go`, `internal/engine/exec/aggregate.go`,
  `internal/worker/shuffle_partial_agg.go`
