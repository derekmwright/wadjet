# Broadcast-Hazard Mitigation Pass — unifying PR #40 + PR #43 into a per-join planner pass

**Status:** Draft
**Date:** 2026-04-19
**Author:** Derek Wright (with Claude)
**Subsumes:** `2026-04-18-shuffle-based-build-partitioning-design.md` (PR #40), `2026-04-18-shuffle-distributed-aggregate.md` (PR #43)
**Architectural invariant established:** *No query in the optimized plan broadcast-duplicates a join build whose per-worker bytes exceed the configured budget.*

## Problem

The planner today reasons about broadcast-duplication hazards through **two parallel primitives**, each with the same Phase-1 limitations:

| Primitive | Detects | Cap | Threshold |
|---|---|---|---|
| `physical.PickShuffleCandidate` (PR #40) | Joins whose build is a base-table scan above `shuffleBuildThreshold` | One candidate per query | `var shuffleBuildThreshold = 4 GB` |
| `physical.PickAggregateShuffleCandidate` (PR #43) | Joins whose build is `aggregate(GROUP BY K, scan(T))` above `aggregateShuffleThreshold` | One candidate per query | `var aggregateShuffleThreshold = 1 GB` |

Both primitives express the same underlying concept — *"this join's build will broadcast-duplicate to every worker; rewrite the plan to avoid it"* — but each is shape-specific, single-candidate, and gated by its own constant. The duplicate-primitive smell is the architectural symptom. Q21's poor performance at SF10 is the loudest visible consequence: it has **two** raw-scan builds (decorrelated EXISTS and NOT EXISTS over `lineitem`), each just below the 4 GB threshold, neither of which the existing primitives handle. A future query with three large builds would expose the same gap again. Lowering thresholds, adding a third primitive for semi/anti shapes, or capping at "Phase 2 multi-candidate" all preserve the wrong abstraction.

The right abstraction: **broadcast-hazard mitigation is a per-join planner pass, not a per-query coordinator branch.** Every join's build cost is evaluated against a memory budget; every hazard gets a remedy chosen by build shape; the choice between remedies is a cost comparison, not a hard-coded rule. The two existing primitives become specific remedies the new pass selects between; the constants become a single budget derived from cluster config.

## Goals

1. Replace `PickShuffleCandidate` and `PickAggregateShuffleCandidate` with a single primitive that walks every join in the plan and returns every broadcast-duplication hazard.
2. Establish and enforce the architectural invariant in planner-layer tests, independent of any specific TPC-H query.
3. Make the threshold a budget derived from per-worker memory, not a hand-tuned constant per shape.
4. Preserve the executable behavior of PR #40 and PR #43 unchanged — they become remedies the new pass selects, not new code paths.
5. Leave a clear extension point for additional remedies (distinct-key pre-compute for key-only semi/anti builds; runtime-adaptive partition counts; etc.) without growing the primitive count.

## Non-Goals

- New remedy types beyond the two PR #40 / PR #43 already provide. The distinct-key pre-compute remedy that would further reduce Q21's broadcast volume is a *future* slot the framework exposes; this spec does not implement it.
- Cost-based join-order changes. The pass operates after logical optimization; build/probe assignment is taken as given.
- Skew handling beyond the 4N partition count PR #40 ships with.
- Replacing or modifying the executor's hash-join, semi/anti probe, or aggregate paths. The pass only rewrites the *plan*; execution code is untouched.
- Standalone (single-process) mode. Standalone has no broadcast hazard because there's only one worker; the pass is a no-op when worker count is 1.
- The build-cache primitive (`internal/coordinator/build_cache.go`, using `LargeBuildScans`). Build cache addresses *I/O* hazard (every worker re-reads the same Parquet) by materializing the scan once to S3 and sharing it; the new pass addresses *memory* hazard (every worker holds the same hash table). Both can apply to the same join. Unifying the two is a worthwhile follow-up but is not in this spec — neither pass currently changes the other's behavior.

## Design

### Approach summary

| Decision | Choice | Rationale |
|---|---|---|
| Primitive shape | Single function `IdentifyBroadcastHazards(stages, budget) []BroadcastHazard` returning all hazards | Plan-wide visibility instead of per-call iteration; eliminates the "what if there are two" failure mode by construction |
| Hazard granularity | Per-join, not per-query | Multi-build queries (Q21, Q07, Q09) are expressed as N hazards naturally; no special multi-candidate code |
| Remedy selection | Per-hazard cost comparison among applicable remedies for that build's shape | Future remedies extend the choice set without rewriting the dispatcher |
| Threshold model | One `broadcastBudget int64` derived from `GOMEMLIMIT × bcastBudgetFraction / workerCount` (config) | Replaces two magic-byte constants with a derived value tied to the actual constraint (per-worker memory) |
| Execution wiring | Each remedy maps 1:1 to an existing orchestrator helper (`orchestrateShuffleStages`, `preComputeDerivedAggregate`) | Zero change to the orchestrators; the new pass picks which to invoke and the coordinator iterates |
| Migration | Old primitives deleted in the same change that introduces the new one | No "Phase 1 / Phase 2" or feature-flag bridge — the architectural simplification is the *point* of the change |

### The primitive

```go
// In internal/planner/physical/broadcast_hazard.go (new file).

// BroadcastHazard records a join whose build side, if left as a broadcast,
// would exceed the per-worker memory budget. The pass returns one of these
// for every such join in the plan.
type BroadcastHazard struct {
    JoinStageID    string         // the join whose build poses the hazard
    BuildBytes     int64          // estimated per-worker bytes if broadcast (full build size)
    BuildShape     BuildShape     // RawScan | AggregateOverScan | Unsupported
    Remedy         Remedy         // chosen remedy (see RemedyKind)
    // Remedy-specific payloads (one populated, others zero):
    ShuffleCand    ShuffleCandidate          // when Remedy.Kind == RemedyShuffleScan
    AggregateCand  AggregateShuffleCandidate // when Remedy.Kind == RemedyPreComputeAggregate
}

type BuildShape int
const (
    BuildShapeRawScan           BuildShape = iota // build is a single base-table scan (possibly with filters)
    BuildShapeAggregateOverScan                    // build is aggregate(GROUP BY K, scan(T))
    BuildShapeUnsupported                          // shape doesn't match any current remedy
)

type Remedy struct {
    Kind     RemedyKind
    EstCost  int64 // per-worker bytes after remedy applies; for cost comparison if multiple remedies are applicable to a shape
}

type RemedyKind int
const (
    RemedyNone                  RemedyKind = iota // hazard recognized but no remedy available; broadcast remains
    RemedyShuffleScan                              // PR #40 mechanism: partition the build's scan by join keys
    RemedyPreComputeAggregate                      // PR #43 mechanism: pre-compute aggregate centrally, broadcast cache paths
    // Future: RemedyPreComputeDistinctKeys, RemedyShuffleAggregate, etc.
)

// IdentifyBroadcastHazards walks every hash_join / broadcast_join stage in the
// plan and returns one BroadcastHazard per join whose build's per-worker bytes
// exceed budget. Returns the empty slice when no hazards are present.
//
// Hazards are returned in plan order (deterministic for downstream orchestration).
//
// The function is total: every join is either classified into a known
// BuildShape with a chosen Remedy, or returned with BuildShape=Unsupported and
// Remedy.Kind=RemedyNone so the caller can log/observe it. Unsupported hazards
// are never silently dropped — telemetry is the spec's main feedback loop for
// finding new remedy needs on production workloads.
func IdentifyBroadcastHazards(stages []Stage, budget int64) []BroadcastHazard
```

### Per-join classification

For each join stage:

1. **Compute build bytes.** Walk the build sub-DAG to its root scan(s); sum `EstimatedBytes`. (For aggregate-over-scan builds, this is the *input scan* bytes — matches PR #43's gating.) If `buildBytes ≤ budget`, the join is not a hazard; skip.

2. **Classify build shape.** Apply existing helpers (`followToAggregate`, `followToScan` — already in `aggregate_shuffle.go`) plus a simple "is the build a single scan with optional filters" check.

3. **Select remedy.** For each applicable remedy, compute estimated per-worker bytes after remedy:
   - `RemedyShuffleScan`: `buildBytes / workerCount` (partitioned).
   - `RemedyPreComputeAggregate`: `aggregate output bytes` (estimated from NDV statistics if available; conservatively `buildBytes / 100` as a first-pass heuristic when stats are missing — telemetry will inform tuning).
   Pick the lowest-cost applicable remedy. If none apply, set `RemedyNone`.

4. **Validate join-key alignment.** If the chosen remedy needs the join keys to be partitionable by the remedy's partition keys (shuffle: build scan's columns must include join keys; aggregate: GROUP BY keys must cover join keys), confirm it. On failure, demote to next-best remedy or `RemedyNone`. Reuse `keysCovered` and the existing column-anchored fallback logic.

5. **Emit hazard.** Append `BroadcastHazard{...}` to the result.

### Coordinator integration

In `internal/coordinator/coordinator.go`, the existing two-branch routing block — the back-to-back `PickShuffleCandidate` + `PickAggregateShuffleCandidateDiag` calls plus the suppression logic between them — is replaced by a single block:

```go
hazards := physical.IdentifyBroadcastHazards(physStages, c.broadcastBudget())
for _, h := range hazards {
    c.logger.Info("broadcast hazard identified",
        "query", queryID, "join", h.JoinStageID,
        "build_bytes", h.BuildBytes, "shape", h.BuildShape,
        "remedy", h.Remedy.Kind, "remedy_cost", h.Remedy.EstCost)
}
revisedStages, preExecArtifacts, err := c.applyBroadcastRemedies(ctx, queryID, physStages, hazards)
if err != nil {
    return nil, fmt.Errorf("apply broadcast remedies: %w", err)
}
physStages = revisedStages
preComputedAggregates = preExecArtifacts.Aggregates
shuffleTasks = preExecArtifacts.ShuffleTasks
```

`applyBroadcastRemedies` is a thin coordinator method that iterates hazards and dispatches:

- `RemedyShuffleScan` → `c.orchestrateShuffleStages(...)` (existing function, unchanged), populating a `ShuffleTasks` map keyed by the resulting probe stage ID.
- `RemedyPreComputeAggregate` → `c.preComputeDerivedAggregate(...)` (existing function, unchanged), appending to `Aggregates`.
- `RemedyNone` → no plan transformation; the join continues to broadcast, the hazard remains in telemetry.

The shuffle and pre-compute helpers themselves remain identical to today. The architectural simplification is purely at the planner/coordinator boundary: one identification pass, one dispatch loop, no per-shape branching.

When multiple hazards reference the same upstream scan (e.g. a scan that feeds two joins), the dispatch loop deduplicates with a concrete rule: hazards are processed in plan order; for each hazard, before dispatching, check whether an already-applied remedy upstream of this join already reduces its build to ≤ budget. If so, skip. If not, dispatch this hazard's remedy. Two hazards needing *different* shuffle keys on the same scan is the only conflicting case: the loop dispatches the first and demotes the second to `RemedyNone` with a telemetry line. None of the 22 TPC-H queries exhibit this conflict; if a future query does, the invariant test fails and points at the join that needs a richer remedy (e.g. shuffle-by-multiple-keys or a re-shuffle stage).

### Budget model

`broadcastBudget()` returns `bcastBudgetFraction × goMemLimit / max(1, workerCount)`, with:

- `bcastBudgetFraction` defaulting to `0.25` (a single broadcast hazard should not consume more than a quarter of per-worker heap).
- `goMemLimit` read from runtime; fallback to a `BroadcastBudgetBytes` config value when GOMEMLIMIT is unset (e.g. local dev).

This ties the threshold to the actual constraint (per-worker memory pressure) instead of a magic byte count. Tests can override the fraction or set the budget directly via the same `var` pattern PR #40 / PR #43 use today. The constants `shuffleBuildThreshold` and `aggregateShuffleThreshold` are deleted in this change; their existing test overrides migrate to setting `broadcastBudget` directly.

### Q21 walkthrough (correctness check, not goal)

After the pass, Q21's plan at SF10 with workerCount=4 should produce:

- Hazard A: join between l1 (probe) and l2 (semi-build), `buildBytes ≈ 3.7 GB`, `BuildShape=RawScan`, `Remedy=RemedyShuffleScan` partitioning l2 by `l_orderkey`.
- Hazard B: join between l1 (probe) and l3 (anti-build), `buildBytes ≈ filtered_lineitem_bytes`, `BuildShape=RawScan`, `Remedy=RemedyShuffleScan` partitioning l3 by `l_orderkey`.

Both hazards' shuffle keys align (`l_orderkey`), so the coordinator dispatches one shuffle layout per hazard with a shared key. Per-worker build state for each is `buildBytes / 4`.

Q21 is *not* a goal of the spec — it's a witness that the architectural fix produces the right plan transformation. The goal is the invariant: no broadcast hazard above budget remains in any query's plan. Q21's wall-time delta is downstream evidence, not the success criterion.

## Architectural invariant

After this change, the following property holds for every distributed query at planning time:

> For every `hash_join` or `broadcast_join` stage `J` in the optimized physical plan, either (a) `J.buildBytes ≤ broadcastBudget`, or (b) the plan contains a remedy stage upstream of `J` whose effect is to reduce per-worker build bytes for `J` to ≤ `broadcastBudget`.

This invariant is asserted by a planner-layer test (`TestBroadcastHazardInvariant_AllTPCHQueries`) that runs every TPC-H query through the planner with a deliberately small budget, identifies all hazards, applies the pass, and re-checks: no remaining hazards above budget. If a future query introduces a build shape no remedy handles, the test fails on `BuildShapeUnsupported` — the failure points exactly at the missing remedy.

## Files

| File | Change |
|---|---|
| `internal/planner/physical/broadcast_hazard.go` | **New.** `BroadcastHazard`, `BuildShape`, `Remedy`, `IdentifyBroadcastHazards` and helpers. |
| `internal/planner/physical/broadcast_hazard_test.go` | **New.** Unit tests for shape classification, remedy selection, key-alignment validation. |
| `internal/planner/physical/plan.go` | **Delete** `PickShuffleCandidate`. Keep `LargeBuildScans` (still used by `internal/coordinator/build_cache.go` for the orthogonal build-cache I/O remedy — not in this spec's scope). Move shared scan-walk helpers into `broadcast_hazard.go` if reused there. |
| `internal/planner/physical/aggregate_shuffle.go` | **Delete** `PickAggregateShuffleCandidate`, `PickAggregateShuffleCandidateDiag`, `AggregateShuffleRejectReason`, `AggregateShuffleDiag`. Keep `BuildAggregateShuffleSQL`, `AggregateShuffleCandidate`, `followToAggregate`, `followToScan`, `keysCovered` — these become helpers for the new pass and the existing orchestrator. |
| `internal/coordinator/coordinator.go` | Replace the two-branch routing block with `IdentifyBroadcastHazards` + `applyBroadcastRemedies` loop. |
| `internal/coordinator/broadcast_remedies.go` | **New.** `applyBroadcastRemedies` method dispatching hazards to existing orchestrators. |
| `internal/coordinator/shuffle_orchestrator.go` | **Delete** `var shuffleBuildThreshold`. Existing `orchestrateShuffleStages` unchanged. |
| `internal/coordinator/aggregate_shuffle.go` | **Delete** `var aggregateShuffleThreshold`. Existing `preComputeDerivedAggregate` unchanged. |
| `internal/coordinator/budget.go` | **New.** `broadcastBudget()`, `var bcastBudgetFraction = 0.25`. |
| `internal/coordinator/distributed_tpch_test.go` | Migrate threshold overrides to `broadcastBudget` overrides. Add `TestBroadcastHazardInvariant_AllTPCHQueries`. |
| `internal/planner/physical/shuffle_insertion_test.go`, `aggregate_shuffle_test.go` | Delete tests bound to old primitives; tests for shape classification and remedy selection move to `broadcast_hazard_test.go`. Tests for the underlying mechanism (orchestrator behavior, shuffle SQL generation) stay. |

## Testing strategy

**Planner-layer (the invariant).**

- `TestBroadcastHazardInvariant_AllTPCHQueries` — for every Q1-Q22 at SF1 catalog stats with `workerCount=4` and a deliberately low budget (e.g. 100 MB to force hazards on most join-heavy queries): identify hazards, apply, re-identify, assert zero hazards above budget remain. Any `BuildShapeUnsupported` causes failure with a clear message.
- `TestIdentifyBroadcastHazards_MultiBuildQueries` — synthetic stages mirroring Q21 (two raw-scan builds), Q09 (multiple builds), Q17 (aggregate build + raw probe). Asserts the right hazards are returned, in the right order, with the right remedy per hazard.
- `TestIdentifyBroadcastHazards_KeyAlignmentFallback` — synthetic stages where shuffle keys don't align; assert the pass falls back to the next-best remedy (or `RemedyNone`) instead of producing an incorrect plan.
- `TestRemedyCostComparison` — synthetic stages where both `RemedyShuffleScan` and `RemedyPreComputeAggregate` are applicable to the same hazard; assert the lower-cost one is chosen.

**Coordinator-layer (the dispatch loop).**

- Existing `TestDistributedTPCHForcedShuffle_*` tests migrate to set `broadcastBudget = 1`. They should pass unchanged in result content because the underlying orchestrators are not modified.
- New `TestApplyBroadcastRemedies_NoHazards` — when `IdentifyBroadcastHazards` returns empty, the coordinator's plan and stages are unchanged.
- New `TestApplyBroadcastRemedies_TwoShufflesOneAggregate` — hazards mixing remedy types are dispatched correctly.

**Correctness gate.**

- All 22 TPC-H queries pass at SF0.01 with `broadcastBudget = 1` (forces every join through the remedy path). Result row checksums match the SF0.01 baselines stored in `benchmarks/tpch/baseline-sf100.json` and equivalents.

**No new EC2 deploys are required to land this change.** The architectural invariant is verified at the planner layer; the orchestrator code paths it dispatches to are already deploy-validated by PR #40 and PR #43.

## Migration plan

This is a single-PR change. There is no feature flag and no parallel old/new code path. The PR contains:

1. New `broadcast_hazard.go` with the primitive and remedy types.
2. New `applyBroadcastRemedies` and `broadcastBudget` in coordinator.
3. Deletion of `PickShuffleCandidate`, `PickAggregateShuffleCandidate*`, `shuffleBuildThreshold`, `aggregateShuffleThreshold`.
4. Coordinator routing block rewritten to the single-loop form.
5. Test migration (existing tests setting old thresholds → set new budget).
6. New invariant test at the planner layer.

All-in-one is appropriate because the new primitive *is* the replacement; shipping them side-by-side would re-introduce the duplicate-primitive smell the spec exists to eliminate.

## Risks and open questions

1. **Cost estimation accuracy for `RemedyPreComputeAggregate`.** The conservative `buildBytes / 100` heuristic when stats are missing may pick the wrong remedy on some shapes. Mitigation: telemetry on the chosen remedy + actual post-execution build bytes; revisit with stats-driven estimation once we have data. Wrong choice still produces a *correct* plan, just a less efficient one.

2. **Multi-hazard remedy interaction.** Two hazards on the same scan (e.g. a scan feeding two joins) could pick conflicting remedies. The dispatch loop's documented dedup behavior handles the common cases (TPC-H), but a synthetic adversarial query could expose a gap. The invariant test on all 22 queries gives a baseline; new shapes appearing in production would surface as `BuildShapeUnsupported` or invariant violations in CI.

3. **Budget vs. existing per-task memory budget.** The `broadcastBudget` and the executor's per-task memory budget interact: budget should be ≤ per-task budget × bcastBudgetFraction. The spec defaults to 0.25, leaving 0.75 for non-build state. Operationally, both numbers should derive from the same root config (per-worker memory + worker count). The first version of this spec uses GOMEMLIMIT directly; refining to a single config root is a follow-up.

4. **Standalone mode no-op.** `workerCount = 1` makes the budget per-worker = full GOMEMLIMIT; no hazards trigger. This is correct (broadcast is free in single-worker), but the test suite must explicitly cover the path to prevent a regression where a future change accidentally reads `workerCount` before it's populated and triggers spurious shuffles.

5. **Future remedy interaction.** When `RemedyPreComputeDistinctKeys` is added (a likely follow-up for key-only semi/anti builds), it must slot into the existing per-hazard cost comparison without changing the dispatch loop. The spec deliberately leaves the cost-comparison API open-ended (any number of applicable remedies) to make this trivial.

## Out of scope (future remedies, separate specs)

- **`RemedyPreComputeDistinctKeys`.** For key-only semi/anti joins where DISTINCT join-key cardinality ≪ row count, pre-compute `SELECT DISTINCT k FROM T [WHERE filter]` centrally and broadcast the small key set instead of partitioning the full scan. Would further reduce Q21's per-worker build bytes; complements rather than replaces shuffle.
- **Stats-driven remedy cost estimation.** Replace the conservative output-bytes heuristic with NDV-driven estimates from the column-stats catalog landed earlier.
- **Runtime-adaptive partition counts.** Currently 4N; could derive from observed key skew at planning time using sketch statistics.
- **Logical-plan EXISTS-with-inequality decorrelation.** A separate optimizer rewrite that transforms certain EXISTS shapes into aggregate forms before this pass runs. Independent change at the logical layer; would let `RemedyPreComputeAggregate` cover queries the rewrite touches.
