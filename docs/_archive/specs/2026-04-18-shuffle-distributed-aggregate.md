> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Shuffle-Distributed Aggregate — extending shuffle-build-partitioning to derived aggregate relations

**Status:** Draft
**Date:** 2026-04-18
**Author:** Derek Wright (with Claude)
**Builds on:** `2026-04-18-shuffle-based-build-partitioning-design.md` (shipped in PR #40)
**Motivating queries:** Q17 (correlated scalar subquery → decorrelated inner aggregate), future high-cardinality GROUP BY

## Problem

PR #40 added shuffle-based build partitioning for queries whose **build-side base table** exceeds a threshold — the hash join's build is partitioned by join key across workers instead of broadcast. This fixes Q03-style base-table broadcast duplication.

Q17 exhibits the **same pathology through a different path**: its decorrelated plan has a **derived aggregate** on the build side of a LEFT JOIN:

```
Project → Aggregate(SUM(l_extendedprice))
           → Filter(l_quantity < 0.2 * __scalar_0)
              → LEFT JOIN(p_partkey = l_partkey)
                  ├── INNER JOIN(p_partkey = l_partkey)     -- outer probe
                  │     ├── Scan(lineitem)
                  │     └── Filter(p_brand, p_container) → Scan(part)
                  └── Aggregate(GROUP BY l_partkey, AVG(l_quantity))   -- INNER BUILD
                        └── Scan(lineitem)                              -- full table
```

The inner aggregate runs on every probe-split worker, each re-scanning full lineitem and re-computing the same 5M-group hash. Local SF1-sample repro (`TestDistributedTPCHQ17SF100Sample`): per-worker heap grows 5→17 GB in 10s, OOM.

Heap profile confirms the same broadcast-duplication signature as Q03, just sourced from a derived aggregate instead of a base scan. PR #40's shuffle mechanism doesn't fire because its detection is keyed on scan stages, not aggregate subplans.

## Goals

1. Eliminate broadcast-duplication memory pressure for queries whose **build side is a derived aggregate**.
2. Cover Q17 directly, and lay the path for all decorrelated-scalar-subquery patterns (Q20 has a similar shape).
3. Reuse PR #40's `Distribution` property + shuffle orchestration + probe-side co-located join infrastructure. Do not introduce a parallel "hoist-and-broadcast" mechanism — unifying behind shuffle avoids fragmented codepaths.
4. Compose with PR #40: if a query has BOTH a large base-table build AND a derived-aggregate build, both get shuffled.

## Non-Goals

- Replacing the aggregate execution path for standalone mode. Single-worker aggregation is unchanged.
- Hoisting aggregates that are NOT on the build side of a join (e.g., scalar final aggregates). Those already run once.
- Aggregates whose input is a join subplan (not a raw scan). Phase 2 — needs multi-stage chained shuffles.
- Shuffle skew handling beyond the 4N partition count (matches PR #40 scope).

## Design

### Approach Summary

| Decision | Choice | Rationale |
|---|---|---|
| Trigger | Same `shuffleBuildThreshold` (4 GB) applied to the aggregate's **input scan estimated bytes**, not output bytes | Aggregate's *output* is usually small (~MB), but its *input* scan is the broadcast-duplication cost. Gating on input matches PR #40's semantics. |
| Execution | Partial-aggregate-then-shuffle-merge, not centralized-compute-then-shuffle | Parallelizes the aggregate itself. Each worker scans its slice of the input and emits partial partial-aggregate rows hashed by GROUP BY key. A merge stage combines partials per partition into final groups. |
| Join-key alignment | Require GROUP BY keys ⊇ the build-side equi-join keys | Guaranteed for decorrelated scalar subqueries (correlation column becomes the GROUP BY key). When this doesn't hold, fall back to broadcast. |
| Partition count | 4N (matches PR #40) | Same skew headroom, same file-count bound |
| Re-use | `TaskTypeShuffle` tasks, `ShuffleLayout` output, probe-side co-located join path | One code path for "partition-by-key and meet on the other side", whether the source is a scan or an aggregate |

### Architectural Extension

PR #40 introduced a `Distribution` property carried by physical nodes. A shuffle node inserts when inputs don't satisfy a join's required distribution. This extension pushes the `Distribution` property *through* aggregate nodes:

```
Aggregate(GROUP BY keys K, aggs A).Distribution =
    if input.Distribution = HashPartitioned(K) then HashPartitioned(K)   -- already aligned, merge locally
    else                                            HashPartitioned(K) after inserting a Shuffle on K
```

That is: an aggregate's output distribution is `HashPartitioned` on its GROUP BY keys, naturally. To produce a globally-correct aggregate, each worker must see all rows for its partition's keys — which means shuffling the input by GROUP BY key before the per-worker final aggregate.

For Q17: the inner aggregate's GROUP BY is `l_partkey`. The outer join needs `HashPartitioned(l_partkey)` on both sides. The probe (part × lineitem inner join) must also shuffle on `l_partkey`. All three align; the distributed plan becomes:

```
Stage A: Shuffle lineitem by l_partkey → partitioned shards
Stage B: Partial-aggregate each shard (GROUP BY l_partkey) → partitioned aggregate result
Stage C: Shuffle (part × lineitem filter-join result) by l_partkey → partitioned probe shards
Stage D: Per-partition co-located LEFT JOIN between B's output and C's output
Stage E: Merge all per-partition outer-aggregate results at coordinator
```

Stages A+B and C run in parallel (no dependency between them). Stage D dispatches after both complete. Stage E is the existing coordinator merge.

### Key Reuse from PR #40

| PR #40 infra | Used for Option 2 |
|---|---|
| `ShuffleLayout` type | Same, extended alias key (see below) |
| `runShuffleSide`, `orchestrateShuffleStages` | Drive stages A, B, C with a new task variant for B |
| `TaskTypeShuffle` | Same. Input may be a scan (Stage A) or a partitioned stream of partial aggregates (Stage B doesn't re-shuffle; it's a merge) |
| Probe-side co-located join | Exactly the same — worker reads its partition shards from S3 and runs the join locally |
| `shuffleBuildThreshold` | Same knob, broader detection |

### New Components

1. **Aggregate-as-build detection in physical planner.** Walk stages, identify joins whose build side is `Aggregate(GROUP BY K, Scan(T))` with scan `T` ≥ `shuffleBuildThreshold`. Tag with a `ShuffleAggregateCandidate` record (cousin of `ShuffleCandidate`).

2. **New `TaskTypePartialAggregate`.** Reads a file slice of the input scan, runs the aggregate on that slice, hash-partitions the partial-aggregate rows by GROUP BY keys, emits `.wshf` files like the scan-shuffle path. Memory-bounded: partial aggregate's group count is ≤ input scan's distinct keys; for Q17 SF100 that's 20M groups per task × 1/N workers = ~7M groups ~ 800MB max, acceptable.

3. **New `TaskTypeMergeAggregate`.** Reads all partial-aggregate shards for its assigned partition, merges partial-aggregate rows into final groups (COUNT→SUM, SUM→SUM, MIN→MIN, MAX→MAX, AVG→ (SUM of sums) / (SUM of counts)), emits one `.wshf` file per partition.

   The coordinator's existing `reAggregatePartials` logic already does this merge for probe-split partial results; the worker-side variant handles the same arithmetic on shuffled partitions instead of at the coordinator.

4. **Extended `ShuffleLayout`.** Add `BuildAggOutputFiles[][]string` so probe tasks can stream the merged aggregate output per partition. Keyed by synthetic alias (e.g., `__agg_<stageID>`) so the worker's planner can match subplan to shard.

5. **Worker's planner hook.** When running a probe task whose logical plan contains an aggregate subtree matching `Task.PreComputedAggregate.Signature`, replace the aggregate's source with a streaming reader of that partition's aggregate shards. Use a stable signature (GROUP BY cols + AGG function + input scan alias + input predicates) to match.

### Routing Decision

Extend the existing routing cascade:

```
if any large aggregate-build present:
    route to shuffle-distributed (aggregate variant)
elif largest base-table build > shuffleBuildThreshold:
    route to shuffle-distributed (base-table variant)
elif probe-split applicable:
    route to probe-split (existing path, possibly with build cache)
else:
    route to single-worker pipeline
```

When BOTH a large base-table build AND a large aggregate build exist, both shuffle. The coordinator orchestrates all shuffles in parallel and meets them at the probe tasks.

### Correctness Invariants

- The partial aggregate's emitted partial-state format must round-trip through `.wshf` and merge correctly. COUNT requires counts, not row counts. SUM stays SUM. AVG splits into SUM + COUNT. MIN/MAX are idempotent.
- The shuffle's hash function MUST be identical across all tasks (same columns, same hash, same mod). Reuse PR #40's exact function.
- When an input row has NULL on a GROUP BY key: standard SQL groups NULLs together. Ensure hash(NULL) is deterministic.
- LEFT JOIN semantics: the outer side's unmatched rows get NULL in the aggregate output column. Make sure probe-side handles a partition where the build shard is empty (not all partitions get rows for every join key).

### Memory Accounting

- Partial-aggregate task: bounded by per-task memory budget (already enforced via SpillManager). The per-operator HashAggregate tracker fix (shipped earlier this session via the Q12 work) ensures spill decisions are based on actual group state, not input throughput.
- Merge-aggregate task: bounded by per-partition group count ≈ (total distinct keys) / 4N. For Q17 at SF100 with 20M keys and 3 workers × 4 partitions = 12: ~1.7M keys per partition. Comfortably bounded.
- Probe-side co-located join: same as PR #40 — partition build shard size is `agg_output_size / 4N`, tiny for Q17 (~80MB / 12 = ~7MB).

## Files Touched (Phase 1, Estimated)

| File | Change |
|---|---|
| `internal/planner/physical/plan.go` | Aggregate detection pass; `ShuffleAggregateCandidate` type; distribution-property propagation through aggregate nodes |
| `internal/planner/physical/distribution.go` | (NEW from PR #40 if not yet split out) distribution matching logic extended for aggregate outputs |
| `internal/coordinator/shuffle_orchestrator.go` | Add `runPartialAggregateStage` + `runMergeAggregateStage`; extend `ShuffleLayout` with aggregate output files |
| `internal/coordinator/coordinator.go` | Routing branch; wire merged aggregate shards into probe tasks |
| `internal/distributed/messages.go` | `TaskTypePartialAggregate`, `TaskTypeMergeAggregate` task types; `Task.PreComputedAggregates` field |
| `internal/worker/executor.go` | Handle the two new task types |
| `internal/worker/agg_shuffle.go` | NEW: partial-aggregate-then-hash-partition sink |
| `internal/worker/agg_merge.go` | NEW: merge partial-aggregate shards → final groups |
| `internal/coordinator/distributed_tpch_test.go` | Make `TestDistributedTPCHQ17SF100Sample` pass under the new path |

## Validation Plan

- **Gate 0 — Local in-process Q17 repro.** `TestDistributedTPCHQ17SF100Sample` currently OOMs (~17GB heap per 2-worker process). After the change, must complete with per-worker heap bounded near the per-task budget (~2GB).
- **Gate 1 — Unit tests.** Aggregate-as-build detection; partial-aggregate serialization round-trip; merge-aggregate arithmetic correctness for every supported aggregate function (COUNT, SUM, MIN, MAX, AVG, variance/stddev if supported).
- **Gate 2 — SF0.01 correctness.** All 22 TPC-H queries pass. Q17 specifically returns `1` row with the correct `avg_yearly` value. `TestDistributedTPCHBuildCache` suite still passes.
- **Gate 3 — SF10 EC2 run.** Q17 wall time recovers toward Graviton baseline (~2s historical). Full 22 queries within 1.5× of post-PR-#41 baseline — no regressions on Q03/Q05/Q07/Q12 which we just stabilized.
- **Gate 4 — SF100 sample local repro.** Q17 completes locally with bounded heap, confirming the broadcast-duplication is gone. EC2 SF100 validation is a follow-up once the in-memory numbers look right.

## Phased Rollout

**Phase 1 (this spec) — Q17 shape: aggregate on scan feeding a join.**
This is the common decorrelated-scalar-subquery pattern. Ship this first; it closes Q17 and Q20.

**Phase 2 — Aggregate on a join subplan feeding a join.**
Needs multi-stage chained shuffles. Groundwork sits in PR #40's `Distribution` property; implementation is a follow-up spec.

**Phase 3 — High-cardinality skew handling.**
Observed-key-driven re-partitioning for aggregates where one partition gets 10× the work of others. Tracked separately.

## Risks

- **Plan-matching fragility.** Worker's planner must recognize the same aggregate subplan the coordinator tagged. If the worker's optimizer rewrites the plan shape (e.g., pushes filters), the signature match fails and we fall back to in-pipeline execution. Mitigation: stamp a stable ID on the logical aggregate node at coordinator planning time, carry through task; worker's planner passes respect the ID.
- **Two-stage S3 round-trip cost.** Partial-aggregate + merge adds one S3 write+read pass vs. in-memory aggregate. Acceptable trade for avoiding OOM at scale; measure in SF10 gate. If SF10 regression is material, investigate inlining the merge into probe tasks (worker reads all partial shards for its partition directly, no dedicated merge stage).
- **Interaction with the existing build cache.** Base-table build cache must not fire on the aggregate's input scan if we're shuffling that input. Routing precedence: shuffle > cache > broadcast.
- **Correctness for AVG after partial aggregation.** The partial output must carry SUM+COUNT; a naive AVG merge (averaging the partial AVGs) is wrong when partition sizes differ. Mitigation: explicit partial-AVG → final-AVG code path; dedicated unit test.

## Open Questions

- Should the partial aggregate and shuffle happen in a single task (read scan, aggregate, hash-partition, emit) or two tasks (scan-shuffle, then aggregate-on-partitioned-shards)? Fusing reduces I/O. Default to fused; split if profiling shows shuffled raw rows can be smaller than partial aggregate rows (unlikely for low-cardinality GROUP BYs).
- Where to stamp the aggregate node's stable ID — logical plan at coordinator or physical plan? Probably logical, so worker's logical-plan match is direct.
- Should the worker's merge-aggregate task be a distinct task type, or can it ride on the existing `TaskTypePipeline` with a specially-constructed SQL like `SELECT ... FROM <shard files> GROUP BY ...`? The latter reuses more infrastructure.
