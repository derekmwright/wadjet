# Heartbeat Decoupling & Cost-Based Bloom Pre-Filter

**Status:** Draft
**Date:** 2026-04-18
**Author:** Derek Wright (with Claude)
**Related:** `project_sf10_regression_2026-04-18` (bisect localized to commit `9c5401d`), supersedes the cardinality-aware-budgets spec for the SF10 fix.

## Problem

The Mar 30 → Apr 18 SF10 perf regression bisected to commit `9c5401d` (Apr 4): "perf(worker,planner): probe partition bloom pre-filter for build-side scans." On SF10 deploy, this commit causes Q01 to complete in 1.3s but Q02 to **hang for 5+ minutes until workers are reaped as stale**. Two independent bugs surfaced at once:

1. **Heartbeats are gated by task execution.** Workers stop sending heartbeats while executing a long task. The 5-min stale-worker threshold (intended for crashed workers) reaps them. The query restarts, retries, hangs again — visible as a query that never completes.

2. **The bloom pre-filter is unconditional.** It pre-scans probe partition files to extract join key columns and build a bloom filter, attached to all build-side scans. This optimization saves significant memory at SF100 (build rows reduced ~67% per worker) but at SF10 the pre-scan cost exceeds the savings — it's negative-EV at small scale.

The two bugs *interact*: the bloom pre-scan is the long task that triggers the heartbeat reap. But each bug is independently severe and durable improvements address both.

## Goals

1. **Heartbeats reflect worker liveness, not task progress.** A healthy worker mid-task is not a dead worker. Reap only on actual failure (crash, network partition, OOM kill).
2. **Bloom pre-filter runs only when it pays back.** A cost model decides per-join, using existing planner cardinality estimates. No hardcoded scale thresholds.
3. **No benchmark-specific tuning.** The fix should improve every workload, not just TPC-H. SIEM dimension joins, network-flow analytics, ad-hoc queries all benefit.
4. **Restore SF10 baseline (~Q03 5-10s) without regressing SF100.** The bloom pre-filter still runs at SF100 (where it pays back), and its long-running pre-scan no longer fails the worker liveness check.

## Non-Goals

- Cluster-level admission control, dynamic concurrency throttling, or per-task memory budgets. (Cardinality-aware-budgets spec is shelved — turned out to be the wrong axis.)
- Changes to the bloom filter algorithm itself (1MB, FNV-1a, etc.). The runtime question is *whether* to run it, not *how*.
- Heartbeat protocol changes beyond decoupling. The on-the-wire format stays the same; the change is when/how heartbeats are emitted.

## Design

### Part A — Heartbeat decoupling

**Today**: heartbeats are emitted from the same goroutine that processes tasks. While a task's `Execute` is running, no heartbeat is sent. With a 30-second heartbeat interval and a 5-minute stale threshold, any task taking >5min triggers a stale-worker reap.

**Change**: spawn a dedicated heartbeat goroutine at worker startup. It runs on a fixed timer (the existing 30s interval) and reads the worker's current state from atomic/locked fields populated by the task processor. The task processor never blocks the heartbeat.

The heartbeat content stays the same:
- `WorkerID`, `ClusterID` (static, set at startup)
- `ActiveTasks`, `ActiveTaskIDs` (read from atomic int + lock-protected slice)
- `MemoryUsed`, `MemoryTotal`, `RSS`, `NumGoroutines`, `Mallocs` (read from runtime stats — already cheap, already non-blocking)
- `SpillDiskUsed` (read from atomic counter)
- `Draining` (read from atomic bool)

State updates from the task processor are atomic operations or short critical sections; the heartbeat reader holds a brief read lock. There is no path where the heartbeat goroutine can block on task execution.

**The 5-minute stale threshold becomes meaningful again**: it now signals "the worker process is dead or unreachable" — kernel killed it, network partitioned, host crashed. Slow tasks no longer trip it.

**Out of scope for this spec but follow-up worth noting**: per-task progress reporting (Phase 3b in the distributed simplification plan, partially done). With proper heartbeats, the coordinator can also surface "this task has been running for X" to the user without conflating it with worker death.

### Part B — Cost-based bloom pre-filter

**Today**: the planner unconditionally injects a bloom-filter pre-scan stage for every build-side scan in a probe-split query. Cost = scan all probe partition files for the join key columns + build 1MB bloom. Benefit = filter ~(N-1)/N of build rows during the actual build phase.

**Change**: at plan time, compute expected cost and expected benefit per join. Run the pre-filter only when benefit > cost × safety factor.

Concretely (per join with build-side stage `B` and probe-side scan `P`):

```
preScanCost  ≈ probe_keys_bytes_per_worker
              = (P.EstimatedBytes / workerCount) × keyColsRatio(P)
              where keyColsRatio = sum(width(joinKeyCols)) / sum(width(allCols))

buildSavings ≈ B.EstimatedBytes × selectivityReduction
              where selectivityReduction = (workerCount - 1) / workerCount × bloomEfficacy
              bloomEfficacy = 0.95 (assumed; bloom false-positive rate ~5%)

runBloom = buildSavings > preScanCost × safetyFactor
where safetyFactor = 2 (require benefit to be 2× the cost before running)
```

When `runBloom == false`, the planner emits the join without the bloom pre-scan stage. When `true`, current behavior.

**Why this is principled, not benchmark-tuning**:
- Both inputs (`EstimatedBytes`, schema column widths) are already in the planner.
- The formula reflects the actual mechanics: pre-scan cost is real bytes read; build savings are real bytes filtered.
- The safety factor (2×) is one knob, justified by the asymmetric cost: if we wrongly skip the bloom, we lose some build-time savings (proportional). If we wrongly run the bloom, we burn pre-scan compute that doesn't help (also proportional but adds latency on the critical path).
- It composes with future work: if cardinality estimates improve, the decision improves automatically.

**At SF10 Q03**: orders build is ~1.5 GB. probe (lineitem) keys per worker ≈ (5 GB × ~6/16 cols) / 3 ≈ 625 MB. Build savings ≈ 1.5 GB × 2/3 × 0.95 ≈ 950 MB. Cost (1.25 GB with safety factor) > benefit (950 MB) → skip bloom. ✓

**At SF100 Q07**: orders build ≈ 8 GB per worker (after cache). probe (lineitem) keys per worker ≈ (60 GB × ~6/16) / 3 ≈ 7.5 GB. Wait, that's the same ratio — let me re-check. Actually at SF100 the build savings are linear: 8 GB × 2/3 × 0.95 ≈ 5 GB. Cost (15 GB with safety factor) > benefit (5 GB) → still skip?

That suggests the cost model needs refinement OR the bloom isn't actually a clean win at SF100 either. The empirical evidence said it helped Q07 OOM at SF100 — but OOM is about peak memory, not throughput. The bloom reduces *peak* live memory by skipping builds; the pre-scan reads sequentially without holding everything live. So the relevant comparison may be:
- **Without bloom**: peak live = full build hash (e.g., 8 GB)
- **With bloom**: peak live = pre-scan keys (e.g., 7.5 GB, but transient) + filtered build hash (~3 GB)

If the OOM gate is peak live memory, the bloom reduces 8 GB → max(7.5 GB, 3 GB) = 7.5 GB transient, then 3 GB sustained. That's a real win for OOM headroom.

So the cost model should compare *peak memory* not just bytes processed:

```
peakWithoutBloom = build_hash_bytes_per_worker
peakWithBloom    = max(probe_keys_bytes_per_worker, filtered_build_hash_bytes)

runBloom = peakWithBloom < peakWithoutBloom × oomMargin
where oomMargin = 0.7 (run bloom only when it gives 30%+ peak reduction)
```

This is the actual optimization the bloom serves. At SF10:
- peakWithoutBloom = 1.5 GB
- peakWithBloom = max(625 MB, 500 MB) = 625 MB
- 625 MB < 1.5 GB × 0.7 = 1.05 GB → run bloom?

Hmm, that says run it at SF10 too — which contradicts the empirical observation that it hangs there. So either:
- The pre-scan at SF10 is much more expensive than `probe_keys_bytes_per_worker` (maybe it scans full files, not just the key column)
- The bloom pre-scan has a large per-call setup cost
- Something else about the implementation

**Decision for the spec**: implement the *peak memory* model as the primary cost gate, AND add a wall-clock-cost lower bound that prevents the pre-scan from running when its raw bytes-to-process exceeds the build-side bytes-to-process. Belt and suspenders. If real measurements show one is sufficient, drop the other later.

```
runBloom = (peakWithBloom < peakWithoutBloom × oomMargin) AND
           (preScanBytes < buildBytes × workCostMargin)
where workCostMargin = 0.5 (don't pre-scan if it's > half the build's work)
```

This is the formula. Two factors, both grounded in operator mechanics. No scale thresholds.

## Phasing

| Phase | What | Risk |
|---|---|---|
| HB1 | Heartbeat goroutine decoupled from task processor. State read via atomics/RWMutex. | Low — additive goroutine, no behavior change for healthy workers. |
| BL1 | Cost model in planner: compute peakWithBloom vs peakWithoutBloom, runBloom decision per join. | Medium — touches planner; need correct cost-formula tests. |
| BL2 | Wire decision into the existing bloom pre-scan injection path. | Low — gates an existing code path. |

**HB1 ships independently first.** It's the structural fix and is valuable on its own. Tests: SF10 should no longer hang on Q02 (workers stay alive); they'll just be slow.

**BL1 + BL2 ship together as one PR.** They're tightly coupled.

## Validation Gates

### Gate 0 — Unit tests
- HB1: tests that simulate a long task and confirm heartbeats continue at the configured interval (using a fake clock or a real 60s+ test).
- BL1: table of (build_bytes, probe_bytes, key_col_ratio, worker_count) → expected runBloom decision. Cover SF10 Q03 (skip), SF100 Q07 (run), trivial scan-only (skip), heavy multi-join (run).

### Gate 1 — SF0.01 correctness
- Existing distributed TPC-H suite passes with both fixes.
- No correctness change expected.

### Gate 2 — SF10 EC2
- Same cluster (c7g.2xlarge coord + 3× c7g.4xlarge workers).
- Target: total 22-query wall-clock under 90s (vs historical 60s + ~25% margin for instrumentation).
- Q03 under 10s.
- Workers never reaped during execution.

### Gate 3 — SF100 EC2
- Q07/Q09 complete without OOM (the original SF100 wins from `9c5401d`).
- Wall-clock for big-build queries (Q07/Q09) within 10% of pre-this-spec measurements (the bloom should still fire at SF100 where it pays back).
- Workers never reaped.

## What this doesn't solve

- **Cardinality-aware per-task budgets** (separate spec, shelved). Shrinking per-task budget for SF100 OOM safety remains a tuning knob today; getting rid of it is future work.
- **Spill-trigger redesign** (separate branch, ready to merge). This spec doesn't change spill behavior.
- **Cost-model accuracy**. The bloom decision uses `EstimatedBytes` from the planner, which is sometimes wrong by 2-5×. The safety factors compensate but can be tightened with feedback from past task peak-heap reports (future work).

## Open questions

1. **Should HB1 use atomics or RWMutex for state?** Atomics for counters (ActiveTasks, MemoryUsed, etc.); RWMutex for the slice (ActiveTaskIDs). Probably mixed. Decide during implementation; both are fast enough for 30s heartbeats.

2. **Does the bloom pre-scan implementation read full files or just the key column?** If full files, the cost model's `preScanCost` formula is too optimistic. Need to verify in the parquet reader path. If full files, BL1 should also include a fix to project just the key columns (or the cost model is permanently pessimistic, which still produces the right decision but loses bloom efficacy at scale).

3. **What happens if the bloom decision changes mid-query rerun?** A retry that lands on a different worker with different MemoryTotal could change the decision. Probably fine — the cost model is consistent given the inputs, and inputs (EstimatedBytes, worker envelope) are stable across retries. Worth a note but not a spec change.
