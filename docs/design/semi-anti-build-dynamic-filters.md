# Semi/anti-build dynamic filters (probe-sourced build reduction)

Status: Landed + SF100-validated for the multi-build class (2026-08-04
arms: dd1fbef mechanism, 0350268 merge fix + eligibility).
Kill switch: `WADJET_SEMIANTI_BUILD_FILTER=0`.

## SF100 validation results (2026-08-04, three arms vs control ed7fe7e)

- Mechanism (arm dd1fbef): Q21's rp-11 shipped 98.5M rows instead of
  600M (6.1×); semi+anti build cpu 212 → 76 cpu-s (−64%); every join
  and aggregate output byte-identical; 44/44 vsigs identical across
  the pair (Q19 last-digit ULP flicker excepted, known).
- First arm regressed wall: the coordinator's 24×8MB SERIAL partial
  fetch put ~25s on the critical path. Fixed by WDF2 (s2-compressed
  artifacts, ~5×) + bounded-parallel merge — gap now sub-second
  (join-4 ends 23:32:49.0, merged .494, shuffle starts .5).
- Fixed arm (0350268): Q21 cold 50.0 → 34.7s (−31%), steady −14%
  (window-gapped comparison; same-window pair still owed).
- Q04 cold regressed +19s and Q22 steady +11s: the row-level bloom
  probe taxes EVERY scanned build row (~0.35µs against a multi-MB
  cache-hostile bitset; Q04's 380M-row scan paid +137 cpu-s) while a
  SINGLE cheap key-only build saves less than that. Hence the cost
  eligibility below.

## Cost eligibility (v1.1)

Engage only when ≥2 logical semi/anti builds consume the filtered
exchange (primary consumers + chained semi/anti joins riding the same
build dep). Multi-build sharing is what amortizes the per-row probe
tax — Q21 (semi l2 + anti l3 on one raw exchange) qualifies; Q04/Q22
(one build each) do not and revert to baseline. Note: SF1's
default-broadcast plan chains Q21's legs into one stage behind a
replicate, so runtime engagement at SF1 requires the shuffle-regime
shape (`--broadcast-bytes=-1`); the planner unit test pins marking on
the SF100-like shape.

Future refinements that could re-admit single-build shapes: blocked
(cache-line) bloom layout to cut probe cost ~2×, source-cardinality-
sized blooms (7M keys need 10 Mbit, not 64), and consumer-side (build-
read-time) filtering that skips the shuffle-sender tax entirely.

## Problem

Q21 at SF100 spends ~35s of its 42s cold wall on one structure: the raw
lineitem exchange (rp-11, 600M rows / 9.9GB — the l2 EXISTS leg, with
the l3 NOT-EXISTS leg riding as a `__subsume` computed flag) is consumed
as the BUILD side of a semi join (join-12) and an anti join (join-16)
whose probe is only 7.3M rows. Each join hash-builds 600M rows per pass
(112 + 98 cpu-s) to answer a 7M-row probe. Q04 (EXISTS lineitem semi
build vs a 3.8%-selective orders probe) and Q22 (NOT EXISTS orders anti
build vs a filtered customer probe) are the same class.

Trino closes this with dynamic filtering: the probe side's key set,
collected at runtime, prunes the build before it is shuffled/built.
Wadjet's existing dynamic-filter pass (`applyDynamicFilters`,
default-off since the 2026-06-16 wash) cannot express this:

1. It only wires leaf-scan → leaf-scan blooms; `findLeafScanStage`
   bails on join-shaped probe/build sides.
2. Its emit op is *prepended* to the fragment's unary chain, so it
   accumulates PRE-filter rows — a bloom over a filtered scan carries
   every key in the table (one reason June's blooms rejected ~0%).
3. Its consume path applies blooms only as row-group pruning
   (min/max + `CanBloomPruneRowGroup`). Join keys like `l_orderkey`
   are uniform across row groups: nothing prunes.

## Key correctness insight

For a semi or anti join J, filtering J's BUILD input by any superset of
the key values present in J's PROBE input is semantics-preserving:

- **Semi**: a build row whose key matches no probe row can never emit a
  probe row. Dropping it changes nothing.
- **Anti**: a build row whose key matches no probe row can never block a
  probe row. Dropping it changes nothing. (This is the direction that
  is usually unsafe for build filtering — it is safe here precisely
  because the filter source is the probe input itself.)
- **Bloom false positives** only KEEP extra build rows — safe for both.
- **NULL keys** never match in equi-joins; the emit op skips NULL keys
  and the consume op passes NULL-key rows through (conservative, and
  they are dropped by the join's own NULL handling).

The filter source is J's immediate probe dependency stage (its output
IS the probe input — the set is exact, not an ancestor superset), so
no column-provenance walk through intermediate joins is needed. The
probe key is resolved by name against actual batches at runtime; a
missing column degrades to no-emit → consume degrades to unfiltered
(existing `attached < requested` path).

Sourcing from the probe-chain ROOT scan was considered and rejected:
for Q21 the root (l1, σ receipt>commit) covers ~98% of orderkeys — the
25× reduction only exists AFTER the supplier⋈nation cut, i.e. in
join-4's output (7.3M rows ≈ 3.4% of orderkeys).

## Design

New physical-planner pass `markSemiAntiBuildFilters` (runs after
`fuseScanShuffle`/subsume, before dispatch; independent of the legacy
`DynamicFiltersEnabled` flag, which stays default-off):

For each hash-join stage J with `JoinType ∈ {semi, anti}`:

- **Build walk**: `RightDepStage` → exchange-repartition/replicate
  chain → pass-through leaf scan B (no FilterExprs; subsume
  ComputedCols on the exchange are fine). J's build key must be a
  column of B (single-column integer key, v1).
- **Probe source** S = J's immediate `LeftDepStage` stage. Eligibility:
  S estimated output is meaningfully smaller than B's raw rows
  (evidence of reduction: S is a join/filtered stage, not a raw scan of
  the same table). J's probe key must resolve against S's output.
- **Emit**: append a `DynamicFilterEmit` to S's OUTPUT stream
  (sink-side emit — new placement; see below). Partial per task,
  merged by the coordinator into `StageOutput.BuildStats` (existing
  machinery, extended to compute stages).
- **Consume**: `ConsumeDynamicFilters` on B + stat-dep edge B ← S
  (existing pass-through-scan forwarding: B's StageOutput carries the
  specs into the fused shuffle's tasks via `runShuffleSide`).
- Multiple semi/anti joins sharing one build exchange (Q21's join-12 +
  join-16 both read rp-11) dedupe to one emit/consume per (S, key).

Runtime changes:

1. **Sink-side emit**: `DynamicFilterEmits` may ride the LAST OpSpec of
   a fragment (the sink/sender); the executor appends the accumulator
   after all operators so it observes the stage's OUTPUT rows.
   Scan-side (source OpSpec) emits move AFTER the stage's OpFilter ops
   — post-filter, pre-projection — fixing weakness (2) for the legacy
   pass too.
2. **Row-level consume**: when a task's `DynamicFilters` specs are
   materialized, in addition to row-group pruning, insert a row-level
   bloom operator (reusing `exec.BloomFilterOp`'s selection-vector
   kernel and its adaptive self-disable: <5% rejection over the first
   32 batches → bypass). Applies to scan-filter fragments and fused
   scan+shuffle tasks. The adaptive disable is the runtime backstop
   against the June-wash class (0%-rejection blooms costing hash time).
3. **Compute-stage partial merge**: `dispatchComputeStage` gains the
   same partial-merge → BuildStats block `dispatchScanFilterStage` has.

Bloom sizing: keys are unknown at plan time (S's output NDV). Bits =
clamp(10 × est(S rows), 1 Mbit, 64 Mbit). An undersized bloom raises
FPR — weaker, never wrong.

Known degradation edge (correct, unoptimized): a source task whose
InputFiles slice is empty short-circuits before the emit ops exist, so
no partial uploads and the completeness check withholds the entire
filter. Follow-up: upload an explicit empty partial on that path so
legitimately-empty tasks don't disable the filter for everyone.

## Serialization cost (why this is not free)

The stat-dep edge makes B's shuffle wait for S. For Q21 cold, rp-11
currently overlaps the scans (done ~t+16s); post-change it starts at
join-4 completion (~t+21s with slice 2, ~t+35s without). The win
requires (a) the filtered shuffle to ship ~3% of rows (cheaper hash +
write + downstream builds collapse ~20s → ~3s), and (b) the probe
chain to be fast. Slice 2 (cascaded dimension blooms:
nation → supplier scan → l1 scan, using the same post-filter emit +
row-level consume primitives) cuts scan-0's output 379M → 15M and
join-4 from 77 cpu-s to ~8, moving join-4 completion to ~t+21s.
Estimates from the 2026-08-04 wlog stage timeline: Q21 cold 41 → ~33s,
steady 58 → ~low-20s (steady is join-cpu-bound, the bigger win).
Q04/Q22 have no equivalent serialization exposure (their probe sources
are small filtered scans that finish early anyway).

## What this does NOT touch

- The legacy `applyDynamicFilters` scan→scan pass stays default-off;
  the June 2026 wash verdict stands for pure-FK blooms. This pass has
  its own eligibility (semi/anti build + reduction evidence) and its
  own kill switch.
- Q18's partial-agg exchange: consumers are a grouped aggregate and an
  inner join — not semi/anti — ineligible by construction. Verified by
  the planner eligibility test.
- No new exchange types, no streaming filter push (the filter arrives
  before the build shuffle STARTS, riding the stat-dep edge like the
  existing pass).

## Tests

- Planner: Q21-shape marks (one emit on the probe-dep stage, consume on
  the build scan, stat-dep edge, join-12+join-16 dedupe); Q18 shape
  does not mark; raw-probe-source does not mark; kill switch → zero
  marks; counter `SemiAntiBuildFiltersPlanned` nonzero when marked.
- Worker: sink-side emit placement (bloom contains output keys only);
  post-filter scan emit; row-level bloom op (rejection, NULL keys,
  adaptive disable, selection-vector input).
- Distributed e2e (MemStore): semi + anti values identical with pass
  on/off on a fixture where the bloom demonstrably drops build rows.
- TPC-H SF0.01 22/22; `tpch-harness --mode=local` A/B rows + vsig
  (coordinator/shuffle change → required before EC2); SF100 pair per
  ADR-0011.
