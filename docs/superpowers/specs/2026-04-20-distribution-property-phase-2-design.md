# Distribution-Property Pass — Phase 2: Exchange Insertion + Retire the Switch

**Date:** 2026-04-20
**Status:** Design — ready for implementation planning
**Predecessor:** `2026-04-20-distribution-property-phase-1.md` (on `feat/distribution-property-phase-1`, pending merge to main)
**Research:** `docs/superpowers/research/2026-04-19-distribution-model-trino-spark.md`
**Parked prior art:** `feat/broadcast-hazard-mitigation` @ `67ba055` (reference only for Phase 2; logic belongs to Phase 3)

---

## Summary

Phase 2 replaces Wadjet's coordinator-level routing switch
(`shuffle-distributed` / `probe-split + has_merge` / `single-worker`) with
a property-based Exchange insertion pass modeled on Spark's
`EnsureRequirements` and Trino's `AddExchanges`. Every edge between stages
gets inspected by a single mechanical rule — `actual.Satisfies(required)`
— and an Exchange stage is inserted iff the rule fails. All four existing
routing heuristics (`CanProbeSplit`, `PickShuffleCandidate`,
`PickAggregateShuffleCandidate`, `MergeInfo`) are deleted.

Phase 2 is a **behavior-preserving structural refactor**. No new runtime
optimisations. No new cost decisions. Same execution plan for every TPC-H
query as today, re-derived from distribution properties instead of
declared by a switch case.

The cost-model work that motivated the research (Q17 pre-compute, Q21
multi-build shuffle) moves to Phase 3, which lands cleanly on the new
structure.

## Goals

1. Delete the four-mode routing switch in `internal/coordinator/coordinator.go:472-605`.
2. Delete `CanProbeSplit`, `PickShuffleCandidate`, `PickAggregateShuffleCandidate`, `MergeInfo`, `ExtractMergeInfo`.
3. Introduce three first-class Exchange stage types (`StageExchangeRepartition`, `StageExchangeReplicate`, `StageExchangeGather`).
4. Add a single `EnsureDistribution` planner pass that inserts Exchanges based on Phase 1's `RequiredChildDistribution` and `OutputDistribution` helpers.
5. Flip `BehaviorPreservingMode` to `false` in the production planner path; `AssertExchangeConsistency` becomes a strict gate.
6. Preserve TPC-H SF1 (±5%) and SF10 (parity on one EC2 run) performance.

## Non-Goals (explicitly deferred)

- **Cost-based distribution choice** (broadcast vs shuffle per join driven by build size, NDV, etc.). Phase 3.
- **Multi-build shuffle activation** for Q21. Phase 3. The `orchestrateMultiBuildShuffle` code ported from the parked branch is committed dormant — not wired into Phase 2 dispatch.
- **Derived-aggregate pre-compute** (today's `preComputeDerivedAggregate`). Deleted in Phase 2; re-introduced as a logical-plan rewrite rule in Phase 2.5 if SF10 Q17 regresses beyond the parity gate. Accepted risk at design time.
- **AQE-style runtime re-planning.** Phase 4.
- **SF100 validation.** Out of scope (Q03/Q09 have unrelated open failures).

## Prerequisites

Phase 1 merged to main. That puts on main:
- `Distribution.Satisfies(RequiredDistribution) bool`
- `RequiredChildDistribution(stage, side)` and `OutputDistribution(stage, workerCount)` helpers
- `assignStageDistributions` (populates `Stage.Distribution` for every stage)
- `AssertExchangeConsistency` with `BehaviorPreservingMode` package var (default `true`)
- 22/22 TPC-H SF0.01 `TestTPCHDistributionConsistency` acceptance gate

Phase 2 branches from main after the Phase 1 PR merges.

---

## Architecture

### Before (today on main)

```
PlanDistributed → []Stage (with mode hints stashed on stages)
Coordinator.run:
    switch {
    case shuffleApplicable && mergeInfo != nil: ...    // shuffle-distributed
    case canProbeSplit       && mergeInfo != nil: ...  // probe-split + (optional) pre-computed agg
    default:                                            // single-worker
    }
```

Four ad-hoc decision helpers (`CanProbeSplit`, `PickShuffleCandidate`,
`PickAggregateShuffleCandidate`, the fused-join branch) each inspect stage
shape to produce a hint. The coordinator then hand-rolls the resulting
stage graph. Adding a mode requires revisiting every other mode.

### After Phase 1 (on `feat/distribution-property-phase-1`, pending merge)

```
PlanDistributed
  └─ assignStageDistributions       // populates Stage.Distribution
  └─ AssertExchangeConsistency      // no-op in BehaviorPreservingMode
Coordinator.run: unchanged — still the four-mode switch
```

Properties exist; nothing requires them. The algebra is tested; the
coordinator ignores it.

### After Phase 2 (this spec)

```
PlanDistributed
  ├─ assignStageDistributions        // unchanged
  ├─ EnsureDistribution              // NEW: inserts StageExchange{...}
  └─ AssertExchangeConsistency       // strict (BehaviorPreservingMode=false)
Coordinator.run: stage-DAG executor; dispatch per StageType
  ├─ StageExchangeRepartition → orchestrateRepartition
  ├─ StageExchangeReplicate   → orchestrateReplicate
  ├─ StageExchangeGather      → orchestrateGather
  └─ StagePipeline            → dispatchPipeline (unchanged)
```

`EnsureDistribution` is the sole site making the "insert an exchange"
decision. The coordinator no longer inspects stage shape.

---

## Components

### New types (`internal/planner/physical/`)

```go
// stage.go — three new StageType constants
const (
    StageExchangeRepartition StageType = "exchange-repartition" // many→many, hash-partitioned
    StageExchangeReplicate   StageType = "exchange-replicate"   // one→many, broadcast
    StageExchangeGather      StageType = "exchange-gather"      // many→one, final merge
)

// exchange.go (NEW) — payload carried by Exchange stages
type ExchangeStage struct {
    Keys     []ColumnRef  // Repartition only: hash keys
    Count    int          // Repartition only: target partition count
    Ordering []OrderKey   // Gather only (optional, for sort-merge gather)
}
```

`StageShuffle` (existing) is **renamed** to `StageExchangeRepartition` at
the source level. Same runtime behavior.

The `ExchangeStage` struct outline is ported from
`feat/broadcast-hazard-mitigation`'s prototype — the three-variant payload
shape is validated there.

### `EnsureDistribution` pass (`internal/planner/physical/ensure_distribution.go`, NEW)

```go
// EnsureDistribution walks the stage DAG and inserts Exchange stages
// wherever a child's OutputDistribution does not satisfy its parent's
// RequiredChildDistribution. Runs after assignStageDistributions.
func EnsureDistribution(stages []*Stage, workerCount int) ([]*Stage, error)
```

Algorithm (single bottom-up pass, ~100 LOC):

1. For each stage `S` in topological order:
   - For each input slot `i`:
     - `required := RequiredChildDistribution(S, i)` *(Phase 1 helper)*
     - `child := stages[S.Inputs[i]]`
     - `actual := child.Distribution`
     - If `actual.Satisfies(required)`: continue.
     - Otherwise: synthesize the appropriate `StageExchange*` stage
       matching `required`, splice it between `child` and `S`, update
       `S.Inputs[i]`, and set the new stage's `Distribution` equal to
       `required`.
2. If the root stage's `Distribution` is not `Singleton`: insert a
   `StageExchangeGather` above it.
3. Run `AssertExchangeConsistency` in strict mode
   (`BehaviorPreservingMode = false`). Any violation is a planner bug.

**Plan-DAG sharing:** if the same child feeds two parents with identical
required distributions, one Exchange is inserted and both parents'
`Inputs[i]` reference it (matches Trino/Spark).

**Exchange variant mapping:**

| Required kind | Exchange type inserted |
|---|---|
| `RequiredBroadcast` | `StageExchangeReplicate` |
| `RequiredHashPartitionedOn(keys)` | `StageExchangeRepartition(keys, N)` |
| `RequiredClusteredOn(keys)` | `StageExchangeRepartition(keys, N)` |
| `RequiredSingleton` | `StageExchangeGather` |
| `RequiredAny` | no insertion |

### Coordinator changes (`internal/coordinator/coordinator.go`)

Delete the four-mode switch (lines 472–605). Replace with ~30 LOC:

```go
for _, s := range plan.Stages {  // topo-sorted from planner
    switch s.Type {
    case physical.StageExchangeRepartition: orchestrateRepartition(ctx, s, ...)
    case physical.StageExchangeReplicate:   orchestrateReplicate(ctx, s, ...)
    case physical.StageExchangeGather:      orchestrateGather(ctx, s, ...)
    case physical.StagePipeline:            dispatchPipeline(ctx, s, ...)
    }
}
```

### Orchestrator backends

- `orchestrateShuffleStages` → **renamed** `orchestrateRepartition`. Logic unchanged.
- `preScanBuildTables` → **reshaped** as `orchestrateReplicate`. Takes an
  `*Stage` instead of side-channel hints; emits the same NATS broadcast.
- `orchestrateGather` — **new, ~50 LOC**. Wraps the existing coordinator-side
  merge that today runs implicitly after the switch. No new runtime logic
  — just lifted into a named backend for symmetry.
- `preComputeDerivedAggregate` — **deleted**. Its optimisation returns in
  Phase 2.5 (or Phase 3) as a logical-plan rewrite creating a
  `MaterializedAggregate` node; out of scope here.
- `orchestrateMultiBuildShuffle` (ported, unwired) — committed to
  `internal/coordinator/orchestrator_multibuild.go` with a unit test to
  keep it compiling. Not called from Phase 2 dispatch; Phase 3 wires it up.

### Deletions

| File / symbol | Lines (approx) |
|---|---|
| `coordinator.go:472-605` (four-mode switch + shape mutation) | -130 |
| `plan.go:1044-1085` (`CanProbeSplit`) | -40 |
| `plan.go:1099-1290` (`PickShuffleCandidate`) | -190 |
| `aggregate_shuffle.go` (`PickAggregateShuffleCandidate` routing fn) | -160 |
| `MergeInfo`, `ExtractMergeInfo`, `has_merge` threading | -80 |
| `preComputeDerivedAggregate` | -100 |

Net LOC: roughly **-600 / +350**. The refactor shrinks planner+coordinator.

---

## Data Flow

### Q01 — single-worker aggregation

`Scan → Filter → HashAggregate → Sort`. All stages produce `Singleton`;
no join.

- `assignStageDistributions`: every stage `Distribution = Singleton`.
- `EnsureDistribution`: every edge requires `Any` or `Singleton`;
  `Singleton` satisfies both. No Exchanges inserted.
- Root `Distribution = Singleton`; no Gather needed.
- Coordinator: one `StagePipeline`, one worker.

**Identical execution to today.**

### Q05 — probe-split (default for large-scan joins)

`lineitem (largest) × orders × customer × nation × region → HashAggregate → Sort`.

- Scan of `lineitem`: `Distribution = HashPartitioned(file_id, N)`
  (probe-split becomes a declared partitioning on the scan's output).
- HashJoin build sides (`orders`, `customer`, `nation`, `region`): each
  declares `Required = Broadcast`.
  - Build scans produce `Singleton`.
  - `Singleton.Satisfies(Broadcast) = false` →
    `EnsureDistribution` inserts `StageExchangeReplicate` between each
    build scan and its join.
- HashJoin probe side: when the build is `Broadcast`,
  `RequiredChildDistribution(join, probe) = RequiredAny`
  (probe-side partitioning is free). No Exchange inserted on the probe.
- Final HashAggregate: `Distribution = HashPartitioned(file_id, N)`.
  Query root requires `Singleton` →
  `EnsureDistribution` inserts `StageExchangeGather` above the aggregate.
- Coordinator: N pipeline replicas, M `orchestrateReplicate` backends,
  one `orchestrateGather`.

**Identical execution to today's probe-split; declared as a DAG instead of
implied by a switch case.**

### Q17 — derived aggregate (the one place Phase 2 diverges)

`lineitem × (SELECT AVG(quantity) FROM lineitem GROUP BY partkey) → filter → aggregate`.

- Today: `PickAggregateShuffleCandidate` detects the derived agg,
  pre-computes once on the coordinator, substitutes as a build-cache,
  broadcasts. SF10 ~27s.
- Phase 2: the derived-agg subplan appears twice in the DAG. Without
  `preComputeDerivedAggregate` it computes twice.
- Correctness preserved (22/22). Perf may regress on SF10 due to duplicated work.
- **Decision at SF10 gate:**
  - If Q17 within ±5%: accept; schedule rewrite for Phase 3.
  - If Q17 regresses >5%: Phase 2.5 = port the pre-compute as a
    logical-plan rewrite rule (+~3 days) before merging Phase 2.

### Q21 — multi-build (no change at SF10)

SF10 broadcast fits for every build; no hazard fires. All builds
replicate; total time ~1m54s. Before and after Phase 2: identical.
(SF100 architectural benefit is Phase 3 territory.)

### Four-mode switch → DAG mapping

| Old mode | New DAG shape |
|---|---|
| `single-worker` | All `Singleton`, no Exchanges. |
| `probe-split + has_merge` | Partitioned source scan + Replicate on builds + Gather at root. |
| `shuffle-distributed` | Repartition on one side of each join whose probe isn't co-located with the build; Gather at root. |
| `probe-split + pre-computed agg` | Same as probe-split; pre-compute deferred (see Q17). |

---

## Error Handling

### Planner-time (fail query planning, return to client)

- `EnsureDistribution` encounters a satisfaction gap with no applicable
  Exchange variant — shouldn't happen given the variant mapping covers
  every `RequiredKind`. Returns
  `fmt.Errorf("ensure distribution: no exchange variant satisfies %v from %v", required, actual)`.
- `AssertExchangeConsistency` fails in strict mode after
  `EnsureDistribution` — always a planner bug. Panic in tests, wrapped
  error in prod. Error includes the offending
  `(parent stage, input slot, actual, required)` tuple.
- Cycle detection — DAG input is assumed acyclic (inherited from Phase 1
  walk); cycle = planner corruption; panic.

### Execution-time (runtime, same behavior as today)

- Worker failure during `StageExchangeRepartition` — inherits
  `orchestrateShuffleStages` error handling.
- Worker failure during `StageExchangeReplicate` — inherits
  `preScanBuildTables` handling.
- `StageExchangeGather` errors — coordinator-local; merge failures surface
  as today's `runPipelineOnly` merge errors.
- Memory pressure at any exchange — existing spill paths apply to
  underlying operators. Exchange stages are thin dispatch wrappers, not
  new memory consumers.

### Behavior-preservation guardrails

- `BehaviorPreservingMode` stays `false` in Phase 2 production. Every
  production query runs strict consistency assertions.
- Feature flag `PhysicalPlanner.UseEnsureDistribution` (default `true`
  after merge) allows fallback to the legacy switch. **Exists only
  through Phase 2 merge + SF10 confirmation; deleted immediately after.**
  No long-term dual path.
- `AssertExchangeConsistency`: panic-on-violation in tests; wrapped error
  in prod (no server panic).

### Out of scope

- No retry of Exchange insertion on failure — always a planner bug.
- No runtime stats feedback (AQE — Phase 4).
- No graceful degradation on partial worker failure during shuffle —
  today's fail-the-query behavior preserved.

---

## Testing

### Unit tests (`internal/planner/physical/`)

- `TestEnsureDistribution_NoOp` — already-consistent DAG: no inserts.
- `TestEnsureDistribution_InsertsReplicate` — Singleton→Broadcast requires Replicate.
- `TestEnsureDistribution_InsertsRepartition` — mismatched hash keys require Repartition.
- `TestEnsureDistribution_InsertsGather` — non-Singleton root requires Gather.
- `TestEnsureDistribution_MultiConsumerReuse` — single child with two identical-requirement parents: one Exchange, shared.
- `TestEnsureDistribution_Idempotent` — running twice produces the same DAG.
- `TestEnsureDistribution_StrictModeFailsOnGap` — synthetic unsatisfiable edge returns error with offending tuple.

### Acceptance gate (extends Phase 1's `TestTPCHDistributionConsistency`)

- `TestTPCH_EnsureDistribution_Parity` — Q01–Q22 through `EnsureDistribution`, strict mode. Per query:
  1. `AssertExchangeConsistency(stages) == nil`.
  2. Every Exchange stage's `Distribution` matches its payload
     (Repartition → HashPartitioned; Replicate → Broadcast;
     Gather → Singleton).
  3. **Snapshot:** per-query expected `(stageType, distribution)`
     sequence captured in a golden file; diff on regression. Makes the
     coordinator-switch→DAG translation reviewable.

### Coordinator tests (`internal/coordinator/`)

- `TestCoordinator_DispatchesPerStageType` — one of each Exchange type lands in the right backend (stub dispatcher, no NATS).
- `TestCoordinator_Q05_ProbeSplit_Parity` — Q05 under Phase 2 against `objstore.NewMemStore()` + in-process workers; row-identical to legacy-switch output.
- `TestCoordinator_Q17_PreComputeDeferral_Correctness` — Q17 correct rows (perf regression noted in comment, not asserted).
- **Parity harness:** `PARITY=1 go test` mode runs every TPC-H query through both legacy switch and new dispatch; asserts bit-identical result batches. Kept only through merge; deleted with the feature flag.

### Regression coverage

- One regression test per deleted function: capture a representative scenario as a unit test on `EnsureDistribution` before deleting the function; verify same DAG shape emerges. Prevents "deleted the switch, lost a corner case we only exercised there."
- Q21 multi-build scenario: planner test asserting `EnsureDistribution` inserts two `StageExchangeReplicate`s at SF10 sizes. Canary for Phase 3.

### Benchmarks

- Local: `TPCH_SCALE=1 go test ./benchmarks/tpch/` before & after on same hardware; ±5% gate.
- **SF10 EC2 deploy:** main baseline + Phase 2 branch, one run each,
  workers = c7g.4xlarge×3, coordinator = c7g.2xlarge, on-demand,
  `profile=citc`, destroy as part of run
  (per `feedback_deploy_preflight.md`, `feedback_baseline_first.md`).
  Required to sign off. Estimated cost: ~$2.

### Out of scope

- SF100 (unrelated open failures; not a Phase 2 signal).
- Micro-benchmarks of `EnsureDistribution` itself (~100 LOC, runs once per query, not hot-path).

### Commit discipline

- One behaviour per commit; tests in the same commit as the behaviour they cover. Mirrors Phase 1's rhythm.
- Each deletion commit runs the full coordinator test suite. Red after deletion = incomplete `EnsureDistribution` coverage — add tests before landing the deletion.

---

## Risks

1. **Q17 SF10 regression** (see §Data Flow). Mitigation: SF10 gate
   decides whether Phase 2.5 (pre-compute rewrite) is required before merge.
2. **Hidden shape-inspection logic** in the coordinator beyond the
   documented switch. Mitigation: parity harness running both paths
   catches divergence; regression-per-deletion discipline forces
   each "this used to work because…" case into a named test before
   deletion.
3. **Plan-DAG sharing bugs** (inserting two Exchanges where one should
   suffice, or vice versa). Mitigation: `TestEnsureDistribution_MultiConsumerReuse`
   + idempotence test; snapshot tests surface accidental extra inserts.
4. **Feature flag lingering.** Mitigation: spec explicitly requires
   deletion of `UseEnsureDistribution` flag and parity harness
   immediately after SF10 sign-off. Plan's final task is "delete the
   dual path."

## Rollout

1. **Prerequisite:** Phase 1 PR merged to main.
2. Branch `feat/distribution-property-phase-2` off main.
3. Implement in the order below, each a separate commit/PR-worthy slice:
   1. Add `ExchangeStage` type + three `StageType` constants; rename `StageShuffle → StageExchangeRepartition`.
   2. Port `orchestrateMultiBuildShuffle` dormant (file + unit test).
   3. Add `EnsureDistribution` pass (not yet called from planner).
   4. Unit tests for `EnsureDistribution` variant mapping + multi-consumer + idempotence.
   5. Wire `EnsureDistribution` into `PlanDistributed` behind `UseEnsureDistribution` flag (default `false`); flip `BehaviorPreservingMode` to `false` only when the flag is `true`.
   6. Parity harness in `internal/coordinator/` tests.
   7. Rename `orchestrateShuffleStages → orchestrateRepartition`; reshape `preScanBuildTables → orchestrateReplicate`; add `orchestrateGather`.
   8. New coordinator dispatch (behind the flag).
   9. TPC-H SF0.01 green under `UseEnsureDistribution=true`.
   10. TPC-H SF1 local ±5% green under the flag.
   11. Flip `UseEnsureDistribution` default to `true`; legacy switch still present.
   12. SF10 EC2 baseline + Phase 2 parity run; sign-off.
   13. Delete legacy switch, `CanProbeSplit`, `PickShuffleCandidate`, `PickAggregateShuffleCandidate`, `MergeInfo`, `ExtractMergeInfo`, `preComputeDerivedAggregate`.
   14. Delete `UseEnsureDistribution` flag and parity harness.
   15. Final `go test ./...` + SF0.01 + SF1 gate; merge.

## References

- Research: `docs/superpowers/research/2026-04-19-distribution-model-trino-spark.md`.
- Phase 1 spec: `docs/superpowers/specs/2026-04-20-distribution-property-phase-1.md` (on `feat/distribution-property-phase-1`).
- Parked prior art: `feat/broadcast-hazard-mitigation` @ `67ba055` — reference for `ExchangeStage` shape and `orchestrateMultiBuildShuffle`.
- Deploy discipline: `feedback_deploy_preflight.md`, `feedback_baseline_first.md`, `feedback_ec2_teardown_discipline.md`.
