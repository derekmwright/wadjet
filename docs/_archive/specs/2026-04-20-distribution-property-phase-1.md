> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Distribution-Property Pass — Phase 1: wire up the existing scaffolding

**Status:** Draft
**Date:** 2026-04-20
**Author:** Derek Wright (with Claude)
**Branch:** `feat/distribution-property-phase-1`
**Companion research brief:** `docs/_archive/research/2026-04-19-distribution-model-trino-spark.md`
**Origin context:** `feat/broadcast-hazard-mitigation` (parked at `67ba055`) — five SF10 iterations confirmed the whack-a-mole pattern is an architectural mismatch, not a bug.
**Architectural invariant established:** *Every stage in a distributed plan declares the actual partitioning of its output, and every consuming stage's required input distribution is checkable against the producer's actual distribution via a single `Satisfies` predicate. Phase 1 establishes this property + assertion infrastructure with zero behavior change.*

## Problem

Wadjet's coordinator currently picks one of three distributed routing modes per query via a hand-written switch in `internal/coordinator/coordinator.go:541-605`:

```go
switch {
case shuffleApplicable && mergeInfo != nil:   // shuffle-distributed
case canProbeSplit && mergeInfo != nil:       // probe-split + optional pre-computed-aggregates
default:                                       // single worker
}
```

Each mode is decided by a separate detection helper (`PickShuffleCandidate`, `CanProbeSplit`, `PickAggregateShuffleCandidate`) that re-inspects stage shape independently. The detectors interact through a shared `[]Stage` graph: PR #43 had to add `if len(preComputedAggregates) > 0 { shuffleApplicable = false }` (`coordinator.go:537-539`) precisely because two detectors disagreed. Across five EC2 SF10 iterations on the parked `feat/broadcast-hazard-mitigation` branch (`project_q21_broadcast_hazard_iteration_2026-04-19.md`), every fix to one detector broke another. The whack-a-mole was the symptom of an architectural mismatch — the research brief identifies that **Trino and Spark express the same set of routing decisions as a single property algebra (`Partitioning.satisfies(Distribution)`) over operator inputs and outputs**, and the four-mode coordinator switch is the wrong substrate for the same decisions.

The research brief's load-bearing primitive (sections 1-2): *child produces Partitioning P; parent requires Distribution D; insert exchange iff `!P.satisfies(D)`*. The brief also flags that Wadjet already has a 58-line beachhead — `internal/planner/physical/distribution.go` (commits `d5c2bf3` + `cc92345`) — with `DistKind`, `Distribution`, `Equals`, `SatisfiesJoinKeys`, plus a `Stage.Distribution Distribution` field on `Stage` (`plan.go:115-120`). The scaffolding is **never written to** anywhere in production code (verified by `grep -n "Distribution:" internal/planner/physical/`). Per `feedback_context_loss_architectural_shortcuts.md`, the next session shipped a heuristic shortcut and the abstraction was abandoned.

Phase 1 wires up the existing scaffolding behind a single mechanical predicate (`Distribution.Satisfies(RequiredDistribution)`) and a single assertion that every stage's actual distribution is consistent with what its consumer requires. Phase 1 makes no execution changes — the heuristic switch keeps making the same decisions. Phase 1 only proves that the *new model can describe the existing plans* — and adds a planner-level safety net so future work cannot silently desync the property algebra from reality.

## Goals

1. **Promote `Distribution` from a detected-but-unused property to a populated, queried property** on every stage emitted by `physical.PlanDistributed`.
2. **Add `RequiredDistribution`** as a derived view on consumer stages — what each stage requires of each input — computed from existing stage fields (`JoinLeftKeys`, `JoinRightKeys`, `GroupByCols`, `ShuffleKeys`). Not stored; recomputed.
3. **Add `Satisfies(req RequiredDistribution) bool`** on `Distribution`, generalising `SatisfiesJoinKeys` to cover all required-distribution kinds (Singleton, Any, ClusteredOn, HashPartitionedOn, Broadcast).
4. **Add a planner-layer "exchange consistency" assertion** that runs after `PlanDistributed`: for every stage, for every dependency, assert `dep.Distribution.Satisfies(stage.RequiredChildDistribution(slot))`. **The assertion must hold for every TPC-H query at SF0.01 today.** If it fires, the population rules in this spec are wrong.
5. **Every TPC-H query at SF0.01 produces identical row checksums** before and after Phase 1 lands. Verified by existing `TestTPCHQueries`.
6. **Every TPC-H query at SF1 produces identical wall-time within ±5%** before and after. Verified by `TestTPCHQueriesLarge`. The only added work is one O(stages) walk during planning.

## Non-Goals (the "Phase 1 does NOT do" list)

Phase 1 is **purely additive**. Out of scope:

- **No coordinator changes.** The four-mode switch at `coordinator.go:541-605` keeps making the same decisions. Phase 2 retires it.
- **No modifications to `CanProbeSplit`, `PickShuffleCandidate`, `PickAggregateShuffleCandidate`.** Phase 2 deletes them.
- **No changes to `build_cache.go`, `shuffle_orchestrator.go`, `aggregate_shuffle.go`.** Phase 2 re-targets the dispatch backends.
- **No reference to `IdentifyBroadcastHazards` or code from `feat/broadcast-hazard-mitigation`.** That branch is parked; its multi-build orchestrator is a Phase 2 reference, not a Phase 1 input.
- **No new exchange stages.** Stages are read-only in Phase 1's pass; only the `Distribution` field is written.
- **No cost-based decisions, no stats infrastructure.** Phase 3.
- **No `Exchange` stage type.** Phase 2 unifies today's `shuffle` / build-cache / coordinator-merge under `Exchange{Type: Repartition|Replicate|Gather}`.
- **No worker changes.** The worker does not see `Stage.Distribution`; it remains a planner-internal property.
- **No changes to the stages emitted by `PlanDistributed`.** `walkStages` (`plan.go:1713`), `emitMergeAggregateTree`, `emitMergeSortTree`, `fuseJoinStages` are unchanged. The only new output is the `Distribution` field per stage.
- **No standalone-mode change.** Standalone plans don't emit stages; the pass is gated on `len(stages) > 0`.
- **No adaptive runtime re-planning.** Phase 4.

## Design

### The property algebra (Phase 1 nucleus)

Extend `internal/planner/physical/distribution.go`:

```go
// Existing struct; no field changes.
type Distribution struct {
    Kind  DistKind
    Keys  []string
    Count int
}

// NEW: enumerates the partitioning a consumer needs from each input.
// Mirrors Spark's Distribution trait subclasses (research brief §1).
type RequiredKind int

const (
    RequiredAny               RequiredKind = iota // no constraint
    RequiredSingleton                              // exactly one partition (final result, coordinator merge)
    RequiredBroadcast                              // every worker has every row
    RequiredClusteredOn                            // co-partitioned on Keys, any partition count
    RequiredHashPartitionedOn                      // hash-partitioned on Keys with exactly Count partitions
)

type RequiredDistribution struct {
    Kind  RequiredKind
    Keys  []string
    Count int
}

// NEW: the single mechanical predicate. Mirrors Spark's
// Partitioning.satisfies(Distribution).
//
//   RequiredAny:                always true.
//   RequiredSingleton:          only DistSingleton.
//   RequiredBroadcast:          only DistBroadcast.
//   RequiredClusteredOn(K):     DistBroadcast yes; DistSingleton yes;
//                               DistHashPartitioned iff Keys==K.
//   RequiredHashPartitionedOn(K, N): only DistHashPartitioned with Keys==K
//                               and Count==N.
func (d Distribution) Satisfies(req RequiredDistribution) bool

// SatisfiesJoinKeys preserved as a thin wrapper:
//   d.Satisfies(RequiredDistribution{Kind: RequiredClusteredOn, Keys: joinKeys})
// Existing callers and tests continue compiling unchanged.
```

The Phase 1 design splits Spark's single `Partitioning.satisfies(Distribution)` across two Go types because Go lacks the trait inheritance Spark uses; the predicate `actual.Satisfies(required)` carries identical semantics.

### `RequiredChildDistribution` — what each stage needs from each input

```go
// NEW: derives a per-input requirement from existing stage fields.
// `slot` indexes into Stage.Dependencies. Never stored; recomputed.
func RequiredChildDistribution(stage Stage, slot int) RequiredDistribution
```

Rules per `Stage.Type`, derived from how `walkStages` already implicitly constructs the plan:

| Stage.Type | Requirement per input slot |
|---|---|
| `scan`, `dual` | No inputs. |
| `shuffle` | `RequiredAny` (shuffle accepts any input and re-partitions). |
| `hash_join` | Probe slot (`LeftDepStage`): `RequiredClusteredOn(JoinLeftKeys)`. Build slot (`RightDepStage`): `RequiredClusteredOn(JoinRightKeys)`. |
| `broadcast_join` | Probe: `RequiredAny`. Build: `RequiredAny` (Phase 1 does not strengthen this — see Risk #4). |
| `aggregate` | `RequiredClusteredOn(GroupByCols)` when `WorkerCount > 1`; `RequiredAny` otherwise. |
| `final_aggregate`, `merge_aggregate` | `RequiredAny` for partial-merge groups (`MergeGroupCount > 0` — see Risk #1); `RequiredAny` for the lone final reducer (today's plans gather to a single task implicitly). |
| `sort`, `merge_sort` | `RequiredAny`. |
| `window` | `RequiredClusteredOn(PartitionBy)` if any `WindowCol` partitions; `RequiredAny` otherwise. |
| `pipeline` | All slots `RequiredAny`. |
| `table_func` | Per-function; default `RequiredAny`. |

Unknown stage types return `RequiredAny` — no constraint asserted, so the consistency check passes by default. New stage types added later must add their rule here.

### `OutputDistribution` — what each stage produces

```go
// NEW: computes the per-stage output distribution given resolved dependency
// distributions. Pure function over stage fields + dep map.
func OutputDistribution(stage Stage, deps map[string]Distribution) Distribution
```

Rules derived directly from how today's planner emits stages:

| Stage.Type | Output Distribution |
|---|---|
| `scan` | `DistSingleton` (Phase 1 conservatively labels even multi-task probe-split scans as `Singleton` because the per-worker file-list is opaque to the property algebra — see Risk #2). |
| `shuffle` | `DistHashPartitioned{Keys: ShuffleKeys, Count: NumPartitions}`. |
| `hash_join`, `broadcast_join` | Inherits the probe input's distribution (the join doesn't re-partition the joined output). |
| `aggregate` (partial, single-task) | `DistSingleton`. |
| `final_aggregate` (lone reducer) | `DistSingleton`. |
| `final_aggregate` (merge group, `MergeGroupCount > 0`) | `DistHashPartitioned{Keys: GroupByCols, Count: MergeGroupCount}`. See Risk #1. |
| `sort`, `merge_sort` (lone) | `DistSingleton`. |
| `merge_sort` (merge group) | `DistHashPartitioned{Keys: SortKeys-derived, Count: MergeGroupCount}`. |
| `window`, `pipeline`, `dual`, `table_func` | `DistSingleton`. |

### The wire-up: `assignStageDistributions` + the assertion

```go
// NEW: walks stages in dependency order and populates Stage.Distribution
// per the OutputDistribution rules above.
func assignStageDistributions(stages []Stage, workerCount int)

// NEW: walks every (producer, consumer, slot) edge and asserts
// producer.Distribution.Satisfies(RequiredChildDistribution(consumer, slot)).
// Returns the first violation, or nil.
//
// Phase 1: must pass for every TPC-H query at SF0.01. Phase 2 promotes this
// to the satisfaction check that drives Exchange insertion.
func AssertExchangeConsistency(stages []Stage) error
```

Both are called once at the end of `PlanDistributed` (after `enforceQueryLimits`). A package-level `BehaviorPreservingMode bool` (default `true`) controls assertion hardness — in `BehaviorPreservingMode`, a non-nil error is logged at WARN; tests flip the var to assert hard. Phase 2 deletes the var.

The walk-order issue: today's `[]Stage` slice from `walkStages` is topological *except* for the late `fuseJoinStages` rewire. `assignStageDistributions` resolves dependencies by ID lookup at run time, after all rewiring is complete, so fusion is invisible to it.

### Architectural invariant established by Phase 1

> For every stage `S` in the output of `PlanDistributed(...)`, `S.Distribution` is a populated `Distribution` value reflecting the actual per-worker partitioning of `S`'s output. For every dependency edge `(producer, consumer, slot)`, `producer.Distribution.Satisfies(RequiredChildDistribution(consumer, slot))` holds.

This is the load-bearing property the entire phased redesign rests on. Phase 2 (Exchange insertion) trusts the invariant when deciding where to insert exchanges. Phase 3 (cost model) trusts it when picking among satisfying alternatives. If a future change to `PlanDistributed` (new stage type, new fusion pass) breaks the invariant, the planner-layer test fires immediately — long before EC2 gets involved.

## Files

| File | Change |
|---|---|
| `internal/planner/physical/distribution.go` | **Extend.** Add `RequiredKind` enum, `RequiredDistribution` struct, `Satisfies(req RequiredDistribution) bool` method on `Distribution`, `RequiredChildDistribution(stage Stage, slot int) RequiredDistribution`, `OutputDistribution(stage Stage, deps map[string]Distribution) Distribution`, `assignStageDistributions(stages []Stage, workerCount int)`, `AssertExchangeConsistency(stages []Stage) error`, `BehaviorPreservingMode bool`. Keep `DistKind`, `Distribution`, `Equals`, `SatisfiesJoinKeys` unchanged (the latter delegates to `Satisfies`). Net additions ~250 lines. |
| `internal/planner/physical/distribution_test.go` | **Extend.** Table-driven `TestDistributionSatisfies` covering every `(DistKind × RequiredKind)` cell. Per-stage-type tests for `RequiredChildDistribution` and `OutputDistribution`. Sanity test for `assignStageDistributions` over a synthetic 3-join plan. ~150 lines. |
| `internal/planner/physical/plan.go` | **Three-line change in `PlanDistributed`.** After `enforceQueryLimits`, call `assignStageDistributions(stages, p.WorkerCount)` then `if err := AssertExchangeConsistency(stages); err != nil { /* log; do not fail in BehaviorPreservingMode */ }`. No other changes to `walkStages` or its helpers. |
| `internal/planner/physical/plan_tpch_test.go` | **Add `TestTPCHDistributionConsistency`.** For every Q1-Q22, run `PlanDistributed` with `WorkerCount=4`, assert every stage's `Distribution` is populated, then assert `AssertExchangeConsistency(stages) == nil`. The acceptance gate. |
| `internal/coordinator/coordinator.go` | **No changes.** The four-mode switch is untouched. The coordinator does not read `Stage.Distribution` in Phase 1. |

Total churn: ~400 added lines, ~3 modified. No file deletions, no coordinator changes, no worker changes.

## Testing strategy

**Unit tests on the property algebra.** Table-driven `TestDistributionSatisfies` exhaustively covers `(DistKind × RequiredKind)`:

| Distribution | Required | Satisfies? |
|---|---|---|
| Singleton | Any, Singleton, ClusteredOn(K) | true |
| Singleton | Broadcast, HashPartitionedOn(K, N) | false |
| Broadcast | Any, Broadcast, ClusteredOn(K) | true |
| Broadcast | Singleton, HashPartitionedOn(K, N) | false |
| HashPartitioned(K, N) | Any, ClusteredOn(K), HashPartitionedOn(K, N) | true |
| HashPartitioned(K, N) | ClusteredOn(K'), HashPartitionedOn(K', N), HashPartitionedOn(K, N') | false |

Plus per-stage-type tests on `RequiredChildDistribution` and `OutputDistribution` (one synthetic stage per type).

**Planner-layer consistency test.** `TestTPCHDistributionConsistency` iterates `tpchPlanQueryMap`, runs each through `physical.PlanDistributed` with `WorkerCount=4`, and asserts: every stage's `Distribution` is populated; `AssertExchangeConsistency(stages) == nil`. **This is the Phase 1 acceptance gate.** Failure on any of Q1-Q22 means either the rules in this spec are wrong or `PlanDistributed` emits an inconsistent plan.

**Correctness gate (no-behavior-change verification).**

- `go test -v -run TestTPCHQueries ./benchmarks/tpch/` (SF0.01, ~5s) — every query produces identical row checksums to baseline. **Load-bearing test for Goal #5.**
- `TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge -timeout 30m ./benchmarks/tpch/` (SF1, ~66s) — within ±5% of pre-Phase-1 wall-time. The only new work is one O(stages) walk plus one O(edges) assertion, both microsecond-scale.

**No EC2 deploys are required for Phase 1.** The change is planner-internal; SF0.01 + SF1 local runs are sufficient.

## Migration plan

Single PR. No public feature flag. The internal `BehaviorPreservingMode` bool gates assertion hardness — defaulting to `true` keeps Phase 1 strictly additive.

PR contents:
1. Extended `distribution.go` (new types, `Satisfies`, `RequiredChildDistribution`, `OutputDistribution`, `assignStageDistributions`, `AssertExchangeConsistency`, `BehaviorPreservingMode`).
2. Extended `distribution_test.go`.
3. Three-line wire-up in `plan.go`'s `PlanDistributed`.
4. New `TestTPCHDistributionConsistency` in `plan_tpch_test.go`.
5. Pass `go test ./...`, the SF0.01 correctness gate, and SF1 wall-time within ±5%.

The PR is small (~400 added lines, ~3 modified) and reviewable in one sitting.

## Risks and open questions

1. **Merge-group output distribution may need refinement.** Today's `emitMergeAggregateTree` (`plan.go:5460-5505`) emits per-merge-group `final_aggregate` stages with `MergeGroup`/`MergeGroupCount` fields, but the worker doesn't actually receive a hash-partitioned input — the coordinator routes results by `MergeGroup` index. Phase 1's rule labels these as `DistHashPartitioned{GroupByCols, MergeGroupCount}` for symmetry with Trino/Spark. **Resolution:** if `TestTPCHDistributionConsistency` fires on a merge-grouped query, the rule moves to `DistSingleton` per group with a comment documenting index-based routing.

2. **Probe-split per-worker scan distribution is opaque.** `scan` stages with `Tasks > 1` are partitioned at the file level, not by row key. Phase 1 conservatively labels these `DistSingleton`. The risk is `TestTPCHDistributionConsistency` passing for trivially wrong reasons (`DistSingleton` satisfies `RequiredAny`). **Mitigation:** the test logs the per-stage distribution; PR reviewer confirms the labels make sense for at least Q01, Q03, Q05, Q17, Q21. Phase 2 adds `DistRoundRobin` (or `DistFilePartitioned`) to model probe-split correctly.

3. **`fuseJoinStages` rewires `LeftDepStage` / `RightDepStage` after `walkStages`.** `assignStageDistributions` resolves dependencies by ID lookup at run time, so fusion is invisible to it. Verified by the consistency test on Q05 (which has fused joins).

4. **Broadcast-build distribution is not strengthened in Phase 1.** Today's planner has no broadcast stage between scan and `broadcast_join` (the executor handles broadcast in-process). Phase 1 leaves the broadcast-join build slot at `RequiredAny`, so the assertion does not check broadcast-build distributions. Phase 2 inserts an explicit `Exchange{Type: Replicate}` between scan and `broadcast_join`, at which point the requirement strengthens to `RequiredBroadcast`.

5. **`pipeline-0` collapse is opaque.** When the coordinator collapses the entire query to a `pipeline-0` stage (probe-split or single-worker mode), `Stage.Distribution` is `DistSingleton` and `RequiredChildDistribution` is `RequiredAny` — the algebra reduces to a no-op. **This is fine for Phase 1.** Phase 2 retires the collapse and emits individual stages back through the coordinator dispatch loop. Note that Phase 1's `assignStageDistributions` runs inside `PlanDistributed`, *before* the coordinator's switch collapses to `pipeline-0` — so the consistency assertion sees the full pre-collapse plan.

6. **The "DO NOT EXTEND THE HEURISTIC" connection.** Per `feedback_context_loss_architectural_shortcuts.md`: the prior architectural-shortcut incident left `distribution.go` orphaned because the next session shipped a heuristic instead. **If a future session sees the heuristic switch in `coordinator.go:541-605` and tries to extend it (add a fourth case, tweak `PickShuffleCandidate`, etc.) instead of completing Phase 2, that is a repeat of the prior mistake.** The switch remaining in place after Phase 1 is *expected and temporary*; do not extend it.

## Out of scope (mapped to research-brief Phase boundaries)

- **Phase 2 — Exchange insertion + retire the heuristic switch.** Add `Exchange` stage type with `Type ∈ {Repartition, Replicate, Gather}`. Implement `EnsureDistribution` rule that walks the stage graph and inserts `Exchange` stages where `producer.Distribution.Satisfies(RequiredChildDistribution(consumer, slot))` would fail. Replace `coordinator.go:541-605`'s switch with a stage-DAG dispatcher that maps `Exchange{Type: Repartition}` → `orchestrateShuffleStages`, `Exchange{Type: Replicate}` → `preScanBuildTables`, `Exchange{Type: Gather}` → coordinator merge. Delete `CanProbeSplit`, `PickShuffleCandidate`, `PickAggregateShuffleCandidate`, `MergeInfo`. Multi-build queries (Q21) emerge naturally as multiple `Exchange{Type: Repartition}` stages with shared partitioning schemes — no special multi-candidate handling. Effort estimate (research brief §6): ~3 weeks.
- **Phase 3 — Cost model + stats integration.** Wire `Stage.EstimatedBytes` / `EstimatedRows` into a cost-aware `EnsureDistribution` that picks among equally-satisfying alternatives (broadcast vs. shuffle) per join. Replicate Trino's `useParentPreferredPartitioning` heuristic. The threshold variables `shuffleBuildThreshold`, `aggregateShuffleThreshold` get a principled home as inputs to the cost function. ~2 weeks.
- **Phase 4 — AQE-style adaptive re-planning.** Split the stage DAG into query stages at `Exchange` boundaries. After each `Exchange` materialises, collect partition-size stats, re-run `EnsureDistribution` on the unexecuted remainder. Highest-ROI item but largest lift. ~4-6 weeks.

## Connection to prior work

This spec is the *foundation* the parked `feat/broadcast-hazard-mitigation` branch (`67ba055`) needed. The branch's `IdentifyBroadcastHazards`, multi-build shuffle orchestrator, and shape-budget cost model are reusable in Phase 2/3 — but only on top of the property algebra Phase 1 establishes. Attempting to land that branch's logic *without* the algebra is what produced the five-iteration whack-a-mole at SF10.

When a future session revives the broadcast-hazard work, the order is: Phase 1 (this spec) → Phase 2 (Exchange insertion + EnsureDistribution rule, retire the switch) → port hazard detection into Phase 3's cost model. The branch's code is a reference, not a starting point.
