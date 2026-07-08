# Cardinality-Aware Per-Task Memory Budgets

**Status:** Draft
**Date:** 2026-04-18
**Author:** Derek Wright (with Claude)
**Related:** `2026-04-18-spill-trigger-redesign.md` (Phase 5 made concrete), `project_sf10_regression_2026-04-18` (root-cause bisect to `de4d262`)

## Problem

Every task on a worker today gets the same per-task memory budget, computed once at worker startup:

```
budget_per_task = (envelope - cache) / (multiplier × max_concurrent)
```

This is scale-invariant. The same budget applies to a 100-row aggregation as to a 60-million-row hash join. Two consequences observed today:

1. **`de4d262` (Mar 30)** raised `multiplier` 3 → 5 to shrink per-task budgets so SF100 wouldn't OOM. SF100 now survives, but every SF10 task is now over-throttled — spill triggers fire on queries whose working sets would have fit comfortably under the prior budget. Q03 SF10: 5s → 33s, all due to unnecessary spill.
2. **The `SpillUrgency` differentiation** in the spill-trigger redesign branch correctly raises the threshold for expensive-spill operators, but doesn't help when the budget itself is too small. Every operator class trips on a half-sized budget.

The spill-redesign branch closes ~28s of the SF10 Q03 regression by softening the heap-pressure check; the remaining ~28s is the per-task budget being too small for SF10's working sets.

## Goals

1. **SF10 task budgets sized for SF10 work, SF100 task budgets sized for SF100 work** — both running on the same binary, same machines, same routing logic.
2. **Restore SF10 historical baseline** (Q03 ~5s, total ~60s for 22 queries).
3. **Preserve SF100 OOM protection** — large tasks still get bounded budgets that match what SF100 needs.
4. **Don't add new tunables** — the planner's existing `EstimatedBytes` per stage is the source of truth.
5. **Graceful fallback** — when `EstimatedBytes` is missing or zero, the current static-budget formula remains the safe default.

## Non-Goals

- Dynamic budget rebalancing between in-flight tasks. If a task uses less than its allotment, the surplus stays unused for the task's lifetime. (Future work.)
- Multi-worker budget cooperation (e.g., letting one worker run a "big" task while neighbors run "small" ones to balance). Worker-local for now.
- Query-level admission control (rejecting queries whose sum-of-task-budgets exceeds cluster capacity). Per-worker admission control is in scope; cluster-level is not.
- Replacing the existing `cmd/wadjet` envelope auto-tune. That logic still computes the *envelope* (per-worker memory available); the change is how the envelope is divided across in-flight tasks.

## Design

### Architecture

The coordinator already knows each stage's `EstimatedBytes`. The worker already reports `MemoryTotal` (envelope) in heartbeats. The change: the coordinator stamps a `BudgetBytes` field on each task before dispatch, and the worker honors that budget instead of falling back to its static per-task default.

```
Coordinator                                          Worker
-----------                                          ------
PlanStages → EstimatedBytes per stage                MemoryTotal in heartbeat
   ↓
budget_for(stage) = clamp(stage.EstimatedBytes × SafetyFactor, MinBudget, MaxBudget)
   ↓
Task{..., BudgetBytes: budget_for(stage)} → → →     receive task
                                                     create per-task Tracker(BudgetBytes)
                                                     execute with that budget
```

### Budget formula

```
budget_for(stage) = clamp(
    stage.EstimatedBytes × SafetyFactor,
    MinBudget,
    MaxBudget,
)
```

Where:
- **`SafetyFactor = 4`** — accounts for hash table overhead (~2× rows-of-build), intermediate batches in flight (~1×), agg state (~0.5×), and a margin for cardinality estimates that under-count (~0.5×). Empirically derived but rounded for simplicity. Tunable via const, not flag.
- **`MinBudget = 256 MB`** — even trivial tasks (single small scan, scalar agg) get this much. Below it, spill-trigger overhead exceeds query cost.
- **`MaxBudget = workerEnvelope / 2`** — no single task can take more than half the worker, so at least one other concurrent task can coexist. Computed per-task from the destination worker's reported envelope.

### Multi-task admission control (per-worker)

Coordinator's task scheduler maintains a per-worker accounting of in-flight task budgets. When dispatching a new task, it checks:

```
worker.in_flight_budget_sum + task.BudgetBytes ≤ worker.MemoryTotal × AdmissionMargin
```

Where `AdmissionMargin = 0.85` (reserves 15% for unaccounted memory + GC headroom).

If the new task wouldn't fit, the scheduler:
1. Picks a different idle worker that does have room, OR
2. Defers the task until a current task completes on this worker.

This replaces the implicit `max_concurrent` admission control (which assumed every task was the same size). Today's `max_concurrent` flag still bounds the absolute concurrent task count; admission control bounds the byte budget on top of that.

### Fallback path

When `stage.EstimatedBytes == 0` (planner didn't annotate, or stage type doesn't have a meaningful estimate — e.g., shuffle, merge), the coordinator stamps `BudgetBytes = 0`. The worker treats `BudgetBytes == 0` as "use the legacy per-task budget formula" (the current behavior). This keeps unannotated paths working.

This also means the rollout is safe: a new coordinator talking to an old worker → worker ignores the field. An old coordinator talking to a new worker → worker uses legacy budget. Both directions degrade gracefully.

### Worker-side changes

The worker already creates a per-task `memory.Tracker` when it starts a task. The change: instead of using `e.memoryBudget` (static), it uses `task.BudgetBytes` if set, else `e.memoryBudget`. One-line conditional in the executor.

The per-task `SpillManager` and the `SpillUrgency` thresholds (from the spill-trigger redesign) all use `tracker.Budget()` directly, so they automatically respect the new per-task value.

## Why this is the right architecture

- **The signal exists.** The planner already computes `EstimatedBytes`. We're not adding a new estimator; we're connecting an existing one to a downstream consumer.
- **It's the only way to serve both ends of the workload.** No static formula can satisfy "SF10 needs 4 GB per task" and "SF100 needs 1.4 GB per task" simultaneously. The right answer is "size the budget to the work."
- **It keeps SF100 OOM protection.** SF100 tasks still get bounded budgets — the bound is just driven by what they need, not by a global divisor.
- **It removes the `multiplier` tuning knob.** `de4d262` raised it 3 → 5 for SF100 safety; with cardinality-aware budgets, the multiplier becomes irrelevant for tasks where `EstimatedBytes > 0`. The legacy fallback still uses it for safety on unannotated tasks.
- **It composes with `SpillUrgency`.** The spill-redesign branch's cost-class differentiation operates on `tracker.Budget()`. When the budget is right-sized per task, both `SpillCheap` (60%) and `SpillExpensive` (90%) thresholds give the right behavior at every scale.

## Design choices considered

### Where the budget is decided
- **A. Coordinator** (chosen): coordinator has the full plan + worker telemetry, can make admission-control decisions, can adjust per-deploy without worker code change.
- **B. Worker**: worker would need to receive `EstimatedBytes` per task and apply formula locally. More duplication and harder to admit-control across tasks.
- **C. Both**: coordinator hints, worker can override. Adds confusion about who's authoritative.

A wins because admission control is a coordinator concern.

### Budget formula shape
- **A. Linear with clamps** (chosen): `clamp(EstimatedBytes × 4, 256MB, envelope/2)`. Simple, easy to reason about, easy to tune.
- **B. Logarithmic**: budget grows with log of estimated bytes. Smoother but harder to reason about.
- **C. Per-operator-type formula**: hash join gets 3× build bytes, agg gets 2× group state, etc. More accurate but planner overhead.

A wins on simplicity. C is future work if A's safety factor proves wrong at scale.

### Handling EstimatedBytes inaccuracy
- **A. Safety factor + clamps** (chosen): a 4× safety factor absorbs ±2× cardinality estimate errors. The MinBudget clamp protects when estimate is unrealistically low.
- **B. Per-stage feedback**: track actual peak memory after task completes, refine future estimates. More accurate but stateful and complex.

A is enough for Phase 5; B is future work.

### Admission control granularity
- **A. Per-worker** (chosen): each worker tracks its own in-flight budget sum.
- **B. Per-cluster**: global cluster budget pool. Cleaner but adds coordinator state.
- **C. None**: trust the planner to not over-allocate. Brittle.

A is the minimum needed to honor per-task budgets safely.

## Phasing

| Phase | What | Risk |
|---|---|---|
| 5a | Add `BudgetBytes int64` to `distributed.Task`. Worker uses `task.BudgetBytes` when nonzero. | Low — additive, with fallback. |
| 5b | Coordinator computes `BudgetBytes` per task from stage `EstimatedBytes`. | Low — wired into existing `createTasksForStage` enrichment path. |
| 5c | Per-worker admission control: scheduler tracks in-flight budgets, defers/redirects when full. | Medium — new state in scheduler, requires worker heartbeat to surface envelope. |

**Ship 5a + 5b together.** They form a complete unit: budgets are sized correctly per task. Without 5c, a worker can be over-subscribed if multiple big tasks land on it concurrently — but that's the same risk we have today (max_concurrent caps it crudely), and 5c is a clean follow-on once 5a/5b prove out.

5c follows in a separate PR.

## Validation gates

### Gate 0 — Unit tests

`internal/coordinator/budgets_test.go`: test the budget formula with a table of (estimated_bytes, worker_envelope) → expected budget. Cover: zero estimate (returns 0 → fallback), tiny estimate (returns MinBudget), giant estimate (returns MaxBudget), normal range (returns 4× estimate).

### Gate 1 — SF0.01 correctness gate

Existing distributed TPC-H test suite passes. No correctness change.

### Gate 2 — SF10 EC2

Same cluster as the regression bisect runs. Target: total 22-query wall-clock under 90s (vs historical 60s + ~30s headroom for the spill-redesign overhead). Q03 under 10s.

### Gate 3 — SF100 EC2

Q03/Q05/Q07 complete without OOM on the standard SF100 cluster. Wall-clock not catastrophically worse than the SF100 numbers from before `de4d262`. Most importantly: confirm no worker reaped within 30 min.

## Out of scope (future work)

- **Phase 5c** (admission control) ships separately.
- **Per-operator-type budget formulas** — current 4× safety factor is a uniform approximation.
- **Feedback-driven estimation** — refining future budgets from past task peak-heap reports.
- **Cluster-level admission control** — rejecting queries whose sum-of-task-budgets exceeds cluster capacity.

## Open questions

1. **Does `EstimatedBytes` exist on every stage type?** Confirmed for scan, hash_join, broadcast_join, hash_aggregate. Need to verify for sort, window, and the new shuffle stage. Each unannotated type falls back to legacy budget — safe but suboptimal. Plan task: enumerate stages and confirm coverage; if shuffle needs annotation, add it as part of 5b.

2. **What happens when the coordinator under-estimates and a task OOMs anyway?** With `SafetyFactor = 4`, even a 2× under-estimate gets 2× headroom. A 4× under-estimate hits OOM. Mitigation: workers reject tasks they can't fit (admission control 5c) or runtime expands budget on heap-pressure WARN (Phase 6).

3. **Should `SafetyFactor` be different per operator class?** Hash joins need ~3× build bytes (hash overhead); aggs need ~2× group state. Single uniform 4× is a compromise. Defer to feedback-driven estimation if 4× proves wrong.
