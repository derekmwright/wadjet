> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Spill Trigger Redesign — Honest Tracker, Differentiated Triggers

**Status:** Draft
**Date:** 2026-04-18
**Author:** Derek Wright (with Claude)
**Related:** SF10 regression investigation (`project_sf10_regression_2026-04-18`), commit `f2f0722` (heap-pressure spill trigger), PR #38 (heapPressureRatio 0.7→0.5)

## Problem

The current spill trigger is a static-threshold heuristic that cannot serve both ends of Wadjet's workload spectrum at the same time:

- **SF100 on 32 GB nodes**: must spill aggressively to survive kernel OOM. Live memory was 22× larger than the per-tracker view (`f2f0722` commit msg). The tracker undercounts; an external pressure signal is needed.
- **SF10 on the same nodes**: doesn't need to spill at all. With aggressive triggers, every join spills probe-side batches to disk and wall-clock collapses (Q03 5s → 1m13s).

We've tried both knob settings and confirmed neither works:

| Setting | Q03 SF10 | Q07 SF10 | SF100 OOM |
|---|---|---|---|
| `heapPressureRatio = 0.7` (Apr 8 baseline) | (regression already present — investigation pending) | 3m21s under our experiment | OOM at 31 GB |
| `heapPressureRatio = 0.5` (PR #38 today) | 1m13s | (likely faster than 0.7) | better but still OOMing |

Single-knob revert is not the fix. We need an architectural redesign that protects low-memory environments without paying the SF10 wall-clock tax.

## Goals

1. **Restore SF10 baseline performance** (Q03 ~5s, full 22 queries ~2m02s) — the workload Wadjet's primary users see.
2. **Preserve SF100 OOM protection** — 32 GB nodes must survive the build-cache + probe-pipeline + join-state pattern that previously hit 31 GB anon-rss.
3. **Eliminate the static-threshold tradeoff** — the next operator added shouldn't make a third constant we have to tune.
4. **Keep the change durable** — design for the next 6+ months of operator additions, not just current state.

## Non-Goals

- Auto-tuning the existing `heapPressureRatio`. We're removing the need for it as a primary signal.
- Adding new memory-management primitives in `internal/engine/memory/`. The fix is correct accounting + differentiated triggers, not new infrastructure.
- Reducing per-task memory budget calculations (the `(envelope - cache) / (4 * maxConcurrent)` formula). That heuristic is independently correct.

## Root-Cause Analysis

### The 22× tracker undercount (per `f2f0722` commit)

The `Tracker.Used()` value reports tracked allocations only. At SF100, the actual live process memory was ~22× larger because these allocation paths bypass the tracker entirely:

1. **Parquet decompression buffers** — `buildRGUnits` loads each file into a heap `[]byte` upfront. Untracked.
2. **Scan source channel batches** — in-flight batches between scanner and operator. Untracked.
3. **Probe-pipeline gather buffers** — accumulators in pipeline ops between scan and join. Untracked.
4. **Probe-side join state** — hash join's probe-side batch buffering. Untracked (only build-side state was tracked).
5. **Intermediate batches between operators** — the `Source → Operator → Sink` chain holds batches in flight. Untracked.

`f2f0722`'s response was correct in principle (add a *separate* signal that catches what the tracker misses) but wrong in execution: it added a process-wide heap-pressure check that fires *globally* whenever heap is high, even when the *operator about to spill* isn't contributing to the pressure. Probe-side bridges spill batches to disk because the build-cache loaded a big file — that's the wrong actor responding to the wrong signal.

### Why "spill at 60% of tracker budget" hurts

`f2f0722` also lowered `Used > Budget` to `Used > Budget*0.6`. This compounds the undercount problem: when the tracker's view is incomplete, lowering its threshold makes it MORE prone to false positives (spilling early when there's no real pressure) without actually catching the bypass paths. The 60% threshold was chosen to give "headroom" against the untracked allocations, but the right answer is to fix the undercounting.

### Why differentiating spill cost matters

Spill is not a single uniform operation. Two distinct cost classes:

- **Cheap spill**: build-side hash table partitions, hash-aggregate hash table partitions. Bounded memory, sequential writes, recoverable on demand. Triggering this slightly early costs little.
- **Expensive spill**: probe-side bridge batches (`deferredJoinBridge`, `reverseBloomBridge`). Streaming a large table to disk just to read it back. Costs proportional to probe table size — 60-100 GB at SF100. Triggering this unnecessarily *destroys* wall-clock.

The current code triggers BOTH at the same threshold. SF10 Q03's slowdown is dominated by probe-side bridges spilling unnecessarily. Differentiating restores SF10 performance with no risk to SF100 protection.

## Design

### Phase 1 — Tracker honesty (the actual fix)

Make `Tracker.Used()` reflect reality. Audit each bypass path identified by the `f2f0722` commit message and add tracker calls. Concretely:

| Allocation site | File | Track on | Untrack on |
|---|---|---|---|
| Parquet file `[]byte` from `buildRGUnits` | `internal/storage/parquet/` | file load | file release / scan close |
| Scan source channel batches | `internal/engine/scan/` | enqueue | dequeue |
| Probe-pipeline gather buffers | `internal/engine/exec/pipeline.go` (or similar) | accumulator append | accumulator drain |
| Probe-side hash join state | `internal/engine/exec/hash.go` | probe batch arrival | probe batch consumed |
| Inter-operator batches in flight | `internal/engine/exec/source.go` (or wherever Source/Sink chain) | `Next()` return | `Consume()` ack |

Each addition is mechanical: an `e.tracker.TrackBatch(b)` on entry, `e.tracker.UntrackBatch(b)` on release. Most call sites are <5 lines of change each.

After Phase 1, `Tracker.Used()` should track within ~10% of `runtime.MemStats.HeapAlloc` for steady-state operators. The 22× gap collapses to ~1.1×.

**Validation:** the existing SF100 sample test (`TestDistributedTPCHBuildCacheSF100Sample`) with `WADJET_HEAP_PROFILE=1` should report tracker / heap ratio close to 1.0 through the entire query, not 0.04 (1.4 GB / 31 GB).

### Phase 2 — Differentiated spill triggers

Replace the single `ShouldSpill()` with cost-class-aware triggers:

```go
// SpillUrgency describes how much pressure is needed before this operator
// should spill. Operators self-classify based on the cost of their spill path.
type SpillUrgency int

const (
    SpillCheap     SpillUrgency = iota // build-side hash, hash-agg hash; recoverable
    SpillExpensive                     // probe-side bridge; streams big data
    SpillCritical                      // last resort, only when actual OOM imminent
)

// ShouldSpillFor returns true when this operator class should spill.
// Cheap operators trigger at 60% of tracker budget.
// Expensive operators trigger at 90% of tracker budget.
// Critical fires only on the heap-pressure circuit breaker.
func (sm *SpillManager) ShouldSpillFor(urgency SpillUrgency) bool { ... }
```

Call-site changes:
- `HashJoin.Build` calls `ShouldSpillFor(SpillCheap)` — no behavior change vs today
- `HashAggregate` calls `ShouldSpillFor(SpillCheap)` — no behavior change
- `deferredJoinBridge` and `reverseBloomBridge` call `ShouldSpillFor(SpillExpensive)` — only spill at 90% instead of 60%

This single change eliminates the SF10 Q03 regression: the bridges only spill when the tracker is seriously full, not on speculative pressure.

### Phase 3 — Demote heap-pressure check to circuit breaker

After Phase 1 fixes the tracker undercount, the tracker IS the reliable signal. The heap-pressure check becomes a backstop:

- **Threshold**: 95% of GOMEMLIMIT (was 50% in PR #38, 70% in `f2f0722`). At 95% we are genuinely close to OOM.
- **Behavior**: log loudly when it fires (`level=WARN msg="heap-pressure spill triggered — likely tracker accounting gap"`). This becomes a leading indicator of unaccounted allocations to fix.
- **Effect**: in normal operation, this should NEVER fire. If it does, that's a bug in tracker accounting we need to find.

### Phase 4 — Dynamic concurrency throttling (optional, follow-up)

If post-Phase-1-2-3 we still see memory pressure, the right response is to reduce concurrent task count rather than spill more. `f2f0722` started this by auto-tuning `max_concurrent` at startup; Phase 4 makes it dynamic:

- Worker exposes a `currentMaxConcurrent` value, initialized from startup auto-tune
- On sustained heap pressure (95%+ over 30s), decrement by 1
- On sustained heap headroom (40%- for 5min), increment by 1 (up to the original limit)

This is a graceful-degradation strategy. Out of scope for this spec; tracked as future work.

### Phase 5 — Cardinality-aware budgets (optional, future)

The per-task budget today is `(envelope - cache) / (4 * maxConcurrent)`, identical for all tasks. Phase 5: planner annotates each stage with `EstimatedBytes`; executor sizes the per-task budget proportionally. Big joins get bigger budgets; trivial scans get smaller. Tracked as future work.

## Phasing & Risk

| Phase | Risk | Validates by |
|---|---|---|
| 1 (tracker honesty) | Low — pure additive accounting | SF100 sample test heap profile shows tracker/heap ratio ~1.0 |
| 2 (differentiated triggers) | Low — narrow code change in bridges | SF10 Q03 returns to ~5s; SF100 OOM protection unchanged |
| 3 (demote circuit breaker) | Medium — relies on Phase 1's accuracy | SF100 sample never trips the 95% breaker |
| 4 (dynamic concurrency) | Medium — runtime behavior change | Future spec |
| 5 (cardinality budgets) | High — planner change | Future spec |

**Phase 1 + 2 + 3 ship as one PR.** They're tightly coupled: Phase 2's differentiation only works if Phase 1 makes the tracker reliable, and Phase 3 only safely demotes the circuit breaker if Phases 1+2 have validated accuracy.

Estimated implementation: 2-3 days for the tracker audit (Phase 1 is most of the work), <0.5 day each for Phases 2 and 3.

## Validation Plan

### Gate 0 — Tracker accuracy unit test

New test in `internal/engine/memory/`: drive a synthetic source → operator chain with known batch sizes; assert `Tracker.Used()` matches the sum of in-flight bytes within 10%.

### Gate 1 — SF100 sample test (heap profile)

`TestDistributedTPCHBuildCacheSF100Sample` with `WADJET_HEAP_PROFILE=1`. Compare tracker.Used vs MemStats.HeapAlloc throughout the query lifecycle. Ratio should be ≥0.9 at peak (was ~0.04 before Phase 1).

### Gate 2 — SF10 EC2 distributed (the regression we're fixing)

Same cluster config (c7g.2xlarge coord + 3× c7g.4xlarge workers). Target: total wall-clock ≤2m30s for 22 queries (matches historical baseline 2m02s + ~25% margin for environmental variance).

### Gate 3 — SF100 EC2 distributed (the failure mode we must preserve)

SF100 cluster (same as `feedback_benchmark_consistency.md` SF100 config). Target: Q03/Q05/Q07 complete without OOM. Wall-clock not regressed beyond the SF100 numbers from before f2f0722 (i.e., similar or better than the SF100 results captured before the tracker undercount became known).

## What This Doesn't Solve

- **The shuffle-build-partitioning work** (separate spec). Shuffle is a memory-architecture fix for the case where even with perfect tracker accounting, broadcast-build hash tables are too large per worker. Spill discipline complements but doesn't replace shuffle.
- **The SF100 OOM root cause if it's NOT just tracker accounting.** If, after Phase 1+2+3, SF100 still OOMs, the next investigation is whether actual peak memory exceeds GOMEMLIMIT regardless of accounting (i.e., we genuinely need MORE spilling, not just better-targeted spilling). Likely candidates: `buildRGUnits` full-file load can be replaced with mmap+range-read (already partially done per `ed8f9e4`).

## Open Questions

1. **Should `tracker.TrackBatch` be debit-only or also throttle on backpressure?** Today `Track` is non-blocking. If a hot loop allocates faster than spill can release, we still hit OOM. Phase 4 (dynamic concurrency) addresses this, but a simpler in-place fix could be: `Track` blocks if `Used > Budget * 0.95` and waits up to N ms for spill to catch up. Defer this decision to Phase 1 implementation; we may not need it once tracker is honest.

2. **Should the circuit-breaker Phase 3 check include RSS, not just HeapAlloc?** RSS includes off-heap allocations (mmap, shared libs, NATS buffers). HeapAlloc misses these. For "are we about to OOM" the right signal is RSS approaching cgroup memory limit. Defer to Phase 3 implementation; if Phase 1's tracker is accurate enough, the 95% HeapAlloc threshold may be sufficient as a backstop.

3. **What happens during the gap between this PR and shuffle-build-partitioning?** Both PRs are independent. The shuffle PR is also clean to merge but doesn't help SF100 unless the build threshold is hit (4 GB; SF100 orders is ~8 GB so it would). Order: this PR first (fixes SF10 regression + improves SF100 reliability via accurate tracking), then shuffle PR (architecturally orthogonal, unlocks Q03/Q05/Q07 SF100). Either order works; this one is recommended because the SF10 regression is user-visible today.
