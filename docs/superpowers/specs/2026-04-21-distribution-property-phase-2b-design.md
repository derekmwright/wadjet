# Phase 2b: dispatchStageDAG execution integration

**Date:** 2026-04-21
**Phase:** 2b (follow-up to Phase 2a, which landed planner-side infrastructure in PR #45)
**Scope:** runtime integration of the distribution-property refactor — delete the legacy four-mode switch in `coordinator.go:541-605` and route all queries through a lowering pass that consumes Exchange stages produced by `EnsureDistribution`.

## Context

Phase 2a (PR #45) landed 17 commits on `feat/distribution-property-phase-2`:
- Planner-side `EnsureDistribution` pass that inserts `StageExchange{Repartition,Replicate,Gather}` stages.
- Passive coordinator shims: `orchestrateRepartition`, `orchestrateReplicate`, `orchestrateGather`.
- Test-seam dispatcher: `dispatchStageDAG` + `hasExchangeStages` (case bodies empty).
- SF0.01 22/22 parity gate under `UseEnsureDistribution=true` (planner-side only).
- 22 golden snapshots of Exchange-annotated DAGs.

Nothing executes Exchange stages today. The `UseEnsureDistribution` flag has zero runtime effect — the legacy switch at `coordinator.go:541-605` handles every query regardless of the flag, and silently discards the Exchange stages the planner emits.

Phase 2b makes the flag real by replacing the switch with a lowering pass, then deleting the flag because the new path is the only path.

## Goals

1. Delete the four-mode switch (`coordinator.go:541-605`).
2. Delete the pickers' role as dispatchers: `PickShuffleCandidate`, `CanProbeSplit`, `PickAggregateShuffleCandidate`, and the coordinator's `ExtractMergeInfo` call become unused.
3. Make `EnsureDistribution` the single source of truth for distribution decisions.
4. Preserve the runtime shape (single `pipeline-0` stage + side-channel metadata) that the existing pipeline-executor downstream of line 609 consumes — no executor change.
5. Ship behind no flag. Rollback is `git revert`, not a runtime toggle.

## Non-goals (deferred)

- Teaching the pipeline-executor to walk multi-stage DAGs natively (Phase 3).
- Lifting `preComputeDerivedAggregate` (Q17 fast path) out of its inline branch at `coordinator.go:497-532`. Stays exactly where it is under C-minimal scope.
- Moving `MergeInfo` extraction out of the coordinator. `ExtractMergeInfo(logicalPlan)` remains; it feeds the Gather lowering as a coordinator-side side-channel.
- New distribution variants (broadcast-build-partitioning, aggregate shuffle). Phase 3+.

## Decisions

### Target end-state: C-hybrid

Structured lowering pass. Pickers die as *dispatchers*; pipeline-executor stays single-stage. Phase 3 handles the executor rewrite.

### Boundary: B3 — fat structural Exchange stages, coordinator handles runtime

Exchange stages carry everything knowable at plan time (shuffle keys, build/probe roles, byte estimates). Worker-count partitioning and file enumeration stay coordinator-side. Pickers are deleted, not demoted.

Rationale: keeping pickers as "helpers" preserves the exact coupling Phase 2 was designed to delete — two sources of distribution truth (planner rule + coordinator helper) that will drift as new variants land. B3 makes the plan authoritative.

### Structure: L1 — replace switch, no flag

`executeDistributed` calls `lowerExchangeDAG` unconditionally in place of the switch. The `UseEnsureDistribution` flag is deleted at the end of Phase 2b. No coexistence period; no two-path hedge.

### Stage fields: F1b — use `ExchangeStage` payload struct

The `ExchangeStage` struct already exists at `internal/planner/physical/exchange.go:27-31` with `Keys`, `Count`, `Ordering`. Phase 2b extends it and migrates today's flat `Stage.ShuffleKeys`/`Stage.NumPartitions` under it.

```go
type ExchangeStage struct {
    Keys       []string       // Repartition only
    Count      int            // Repartition only
    Ordering   []SortKeySpec  // Gather only (existing)
    BuildAlias string         // Repartition, Replicate (new)
    ProbeAlias string         // Repartition, Replicate (new)
    BuildBytes int64          // Repartition (new; for logging/threshold checks)
}
```

`Stage` gains `Exchange *ExchangeStage`. Non-Exchange stages leave it nil. `EnsureDistribution` populates the struct at insertion time.

### Testing: T2 — full gate before merge

Baseline-first EC2 A/B. Phase 2b PR is not merged until all gates pass.

## Architecture

### Lowering pass

Signature:

```go
func (c *Coordinator) lowerExchangeDAG(
    ctx context.Context,
    queryID, sql string,
    stages []physical.Stage,
    logicalPlan *logical.Plan,
    workerCount int,
) (loweredStages []physical.Stage,
   shuffleTasks map[string][]distributed.Task,
   mergeInfo *logical.MergeInfo,
   err error)
```

Lives in `internal/coordinator/dag_dispatch.go` (renamed to `dag_lower.go` during Phase 2b for accurate naming; `dispatchStageDAG` becomes `lowerExchangeDAG`). Returns the same tuple the legacy switch produces today; the existing pipeline-executor consumes it unchanged.

### Per-variant lowering

- **`StageExchangeRepartition`** → reads `Exchange.BuildAlias/ProbeAlias/Keys/Count/BuildBytes`, calls `orchestrateRepartition(ctx, queryID, candidate, stages, workerCount)` to produce a layout, then `buildShufflePipelineTasks(...)` to synthesize `distributed.Task` instances. Result merges into `shuffleTasks["pipeline-0"]`. The synthesized `candidate` is a `ShuffleCandidate` struct built from the stage's `Exchange` fields — `PickShuffleCandidate` is not called.
- **`StageExchangeReplicate`** → reads `Exchange.BuildAlias`, calls `orchestrateReplicate(ctx, stage, queryID, sql, stages, probeAlias)` which wraps `preScanBuildTables`. Produces a `BuildCachePreScans` entry attached to the consumer pipeline stage. `CanProbeSplit` is not called; `probeAlias` comes from the stage field.
- **`StageExchangeGather`** → calls `logical.ExtractMergeInfo(logicalPlan)` (unchanged coordinator helper) and returns the result as the `mergeInfo` out-parameter. The pipeline-executor's post-execution merge consumes it via the same code path as today.

After walking all Exchange stages, the lowering collapses `stages` into a single `pipeline-0` stage carrying:
- `ProbeSplitAlias`, `ProbeSplitFiles` (from Replicate handling)
- `BuildCachePreScans` (from Replicate handling)
- `PreComputedAggregates` (passed through from the inline Q17 branch, which runs *before* the lowering call)
- `Tasks: workerCount` (or `1` for single-worker fallback when no Exchange stages are present)

Single-worker fallback: when `hasExchangeStages(stages) == false`, the lowering produces a single-tasks `pipeline-0` stage with no side channels. This matches the legacy default branch at `coordinator.go:601-608`.

### `executeDistributed` shape after Phase 2b

```
1. Logical plan → physical stages
2. (unchanged) preComputeDerivedAggregate inline (Q17 branch)
3. lowerExchangeDAG(...) → (loweredStages, shuffleTasks, mergeInfo, err)
4. ExpandFederatedScans
5. executePipeline(loweredStages, shuffleTasks, mergeInfo, preComputedAggregates)
```

Step 2 stays inline for C-minimal. Steps 1, 4, 5 unchanged.

### Planner-side changes

`EnsureDistribution` populates `Stage.Exchange` fields at insertion time:
- When inserting a `StageExchangeRepartition`, set `BuildAlias`/`ProbeAlias`/`BuildBytes` from the join's operand metadata — the same info today's `PickShuffleCandidate` derives post-hoc.
- When inserting a `StageExchangeReplicate`, set `BuildAlias` from the replicated table and `ProbeAlias` from the sibling scan feeding the consumer join.

Existing `Stage.ShuffleKeys`/`Stage.NumPartitions` fields migrate under `Stage.Exchange.Keys`/`.Count`. This is a mechanical rename touching Phase 2a code (`distribution.go:228-232`, `aggregate_shuffle.go`, `plan_tpch_test.go`, `ensure_distribution_test.go`).

### Deletions at end of Phase 2b

- `coordinator.go:541-605` (the switch).
- `UseEnsureDistribution` flag and its wiring.
- Dispatcher roles of `PickShuffleCandidate`, `CanProbeSplit`, `PickAggregateShuffleCandidate`. (The aggregate-shuffle picker is still consulted inline for Q17 at step 2; that call stays. The other two are fully unused after the switch deletion and are deleted.)
- `dispatchStageDAG`'s test-seam hooks (superseded by unit tests on `lowerExchangeDAG`'s per-variant output).

`ExtractMergeInfo` is retained — the Gather lowering calls it.

## Test plan (T2 gate)

Before merging Phase 2b:

- [ ] `go test ./internal/planner/...` green (covers `EnsureDistribution` field population + `Stage.Exchange` migration)
- [ ] `go test ./internal/coordinator/...` green (covers `lowerExchangeDAG` per-variant unit tests)
- [ ] `go test -v -run TestTPCHQueries ./benchmarks/tpch/` — 22/22 at SF0.01
- [ ] PARITY=1 harness: row-equality green on all 22 queries at SF0.01 (activates the stub from commit `9399371`)
- [ ] `TPCH_SCALE=1 go test -v -run TestTPCHQueriesLarge ./benchmarks/tpch/` — 22/22, no query more than 2× slower than `main`
- [ ] Goldens updated for the 22 Exchange-annotated DAGs to reflect new `Exchange.BuildAlias/ProbeAlias/BuildBytes` fields
- [ ] SF10 EC2 A/B: deploy `main` as baseline, then Phase 2b branch; no query more than 10% slower than baseline (see `feedback_baseline_first.md`, `feedback_deploy_preflight.md`)

No merge without SF10 A/B — Phase 2b touches the exact runtime path the broadcast-hazard branch taught us not to ship on local-only validation.

## Risks and mitigations

- **Field-population divergence.** `EnsureDistribution` now has to derive `BuildAlias/ProbeAlias/BuildBytes` from the logical plan at insertion time instead of the coordinator deriving them from `physStages` at dispatch time. If the derivation disagrees with today's picker output, queries reroute silently.
  - *Mitigation:* unit test that compares `EnsureDistribution`-populated fields against `PickShuffleCandidate`/`CanProbeSplit` output on all 22 TPC-H plans, as a one-shot equivalence check before deleting the pickers.
- **Goldens need re-snapshotting.** Migrating `ShuffleKeys`/`NumPartitions` from flat Stage fields to `Exchange.Keys/Count` changes the golden snapshot format.
  - *Mitigation:* regenerate goldens as part of the first commit that does the field migration; reviewer diffs the renames against Phase 2a's 22 snapshots.
- **Q17 inline branch and Repartition lowering interact.** The Q17 pre-compute runs before `lowerExchangeDAG` and produces `preComputedAggregates`. Today the shuffle (Repartition) branch is suppressed when `preComputedAggregates` is non-empty (`coordinator.go:541-543`) — the pre-computed derived aggregate isn't a true broadcast build and can't be re-partitioned by the outer join's keys. Under Phase 2b this suppression must be preserved: the Repartition handler in `lowerExchangeDAG` must short-circuit to the probe-split path (Replicate + cached build) when `len(preComputedAggregates) > 0`.
  - *Mitigation:* explicit test case for the Q17 shape at SF0.01 + SF1 in the PARITY harness.
- **Single-worker fallback.** When the planner inserts zero Exchange stages (small query, single worker), `lowerExchangeDAG` must produce the same `{Tasks:1}` pipeline-0 the default switch branch produces. Easy to regress to `workerCount` tasks.
  - *Mitigation:* dedicated unit test for the zero-Exchange-stage case.

## Commits (anticipated)

Ordered for reviewability:

1. Migrate `ShuffleKeys`/`NumPartitions` onto `Stage.Exchange` struct (mechanical rename, goldens update).
2. Add `BuildAlias`/`ProbeAlias`/`BuildBytes` to `ExchangeStage`; populate in `EnsureDistribution`.
3. Equivalence test: `EnsureDistribution` fields vs. picker output across 22 TPC-H plans.
4. Flesh out `lowerExchangeDAG` (rename from `dispatchStageDAG`), each variant in its own commit if large.
5. Wire `lowerExchangeDAG` into `executeDistributed` in place of the switch; delete switch.
6. Delete `UseEnsureDistribution` flag, dispatcher roles of pickers, test-seam hooks.
7. Activate PARITY=1 row-equality in the harness.

## References

- `memory/project_distribution_phase_2_integration_gap_2026-04-21.md` — why Phase 2a stopped where it did and what the plan's Task 19 under-scoped.
- `memory/feedback_push_back_on_short_term_tradeoffs.md` — guidance that produced B3 over B2-revised.
- `memory/feedback_context_loss_architectural_shortcuts.md` — rule that produced Phase 2a/2b split.
- `memory/feedback_baseline_first.md`, `memory/feedback_deploy_preflight.md` — EC2 discipline informing T2.
- `docs/superpowers/specs/2026-04-20-distribution-property-phase-2-design.md` — Phase 2 original spec (Phase 2b is the runtime-integration half).
- PR #45 — Phase 2a draft.
