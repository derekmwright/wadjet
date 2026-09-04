> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Shuffle-Based Build Partitioning

**Status:** Draft
**Date:** 2026-04-18
**Author:** Derek Wright (with Claude)
**Related:** PR #38 (spill fragmentation fixes), `project_sf100_q03_broadcast_duplication_2026-04-18`

## Problem

At SF100, Q03 OOMs all three workers ~26 min into execution. Heap profiles confirm the binding constraint is **broadcast duplication of the `orders` hash table**: each worker independently builds the full ~7-8 GB `orders` hash, and at 2 concurrent tasks per worker the per-worker live memory exceeds the 23 GB `GOMEMLIMIT` on c7gd.4xlarge. PR #38's spill batching reduced cumulative allocation by 23% but did not address the broadcast itself; the OOM moved from 14 min to 26 min, not eliminated.

The same pattern affects Q05, Q07, Q09 — any TPC-H query that broadcasts a build table larger than per-worker memory headroom allows.

The existing `BuildCache` (pre-scan large build tables once into shared S3 `.wshf` files) reduces source I/O but does not reduce per-worker memory: each worker still loads the full cached table into a hash table.

## Goals

1. Eliminate broadcast-duplication memory pressure for SF100-scale queries with large build tables.
2. Preserve the broadcast/cache fast path for the common SIEM/network-analytics workload (small dimension tables joined to large fact tables), which is what Wadjet is optimized for.
3. Validate Q03/Q05/Q07 SF100 pass after this change.
4. Lay the architectural groundwork for Q09 (multi-large-build) without shipping it in this spec.

## Non-Goals

- Replacing or retiring the existing `BuildCache` for medium-size tables.
- Multi-stage chained shuffle (Q09). Architecture supports it; implementation is Phase 2 / follow-up.
- Reorganizing the planner around shuffle-by-default. Broadcast remains the default for tables under threshold.
- Optimizing the shuffle transport for latency (S3-mediated is the chosen tradeoff).

## Workload Framing

Wadjet's primary workload is SIEM and network analytics: high-cardinality event/flow streams joined against small dimension tables (assets, users, threat intel). For that workload, broadcast-build is the right default — small builds fit in worker RAM, and a shuffle round-trip would only add latency.

TPC-H queries like Q03 are not representative of the primary workload, but passing them at SF100 establishes credibility at scale. The design here intentionally adds shuffle as an *opt-in path triggered only when broadcast would fail*, rather than as a new default.

## Design

### Approach Summary

| Decision | Choice | Rationale |
|---|---|---|
| Trigger | Single threshold on largest build's `EstimatedBytes` (4 GB to start) | Reuses the same shape as `buildCacheThreshold`; one comparison; easy to tune. Memory-headroom-relative trigger documented as future work. |
| Shuffle stage location | Coordinator-orchestrated pre-shuffle stages (synchronous, S3-mediated) | Reuses `shuffleStreamSink` and `cachedFileStreamSource` patterns; durable shards in S3; no new transport semantics. |
| Both sides shuffled | Yes — textbook distributed hash join | Maps cleanly onto preserved `TaskTypeShuffle` path; memory scales 1/N on both sides. |
| Partition count | 4N (where N = active worker count) | Modest insurance against key skew in real-world joins (e.g., hot `src_ip`); bounded file count. |
| Multi-join scope | Architecture supports chained shuffles via a `Distribution` property; Phase 1 implementation handles a single shuffle stage per query. | Q03/Q05/Q07 are single-large-build queries. Q09 deferred to Phase 2 with the same infrastructure. |
| Validation | Local memory-pressure repro harness → SF0.01 in-process tests → SF10 EC2 gate → SF100 final validation | Avoids burning EC2 dollars on changes that don't actually reduce peak memory. |

### Architecture: Distribution Property

Every node in the physical plan carries a `Distribution` property describing how its output rows are partitioned across workers:

```go
type Distribution struct {
    Kind  DistKind   // Singleton | Broadcast | HashPartitioned
    Keys  []string   // for HashPartitioned: the column names rows are partitioned on
    Count int        // number of partitions (4N for shuffles)
}
```

Joins demand that both inputs match the join key:
- If both inputs are `HashPartitioned` on the join keys → join locally on each worker
- If one input is small and `Broadcast` → join locally (existing path)
- If neither holds → planner inserts a `Shuffle` node that re-partitions on the join keys

In Phase 1, the planner inserts exactly one shuffle per side of one join — specifically the join whose build input has the largest `EstimatedBytes` above `shuffleBuildThreshold`. The shuffle keys are that join's equality keys. All other joins in the same query continue to use broadcast.

In Phase 2, the planner walks the join tree and inserts shuffles wherever distribution doesn't match — multi-stage chained shuffles fall out of this naturally.

### Execution: Shuffle Stage

A `TaskTypeShuffle` task does:

1. Read assigned source files (build table) or assigned upstream output (intermediate).
2. For each row, compute `hash(partition_keys) % (4 × N_workers)` to get target partition `p`.
3. Append the row to a per-partition output buffer.
4. When buffer fills, flush to a local `.wshf` spill file partitioned by `p`.
5. On finalize, upload one `.wshf` per partition to S3 under `s3://.../shuffle/<query>/<stage>/partition=<p>/<task>.wshf`.

Memory bound: 4N output buffers × batch-flush-size. With N=3 workers and 64 KB flush threshold, that's ~768 KB per shuffle task — bounded regardless of source table size.

### Execution: Probe-Side Co-located Join

After the shuffle stage completes, the coordinator dispatches probe tasks. Each worker is assigned a contiguous slice of the 4N partitions (e.g., partitions 0-3 for worker 0 in a 3-worker cluster, with 4N=12 partitions = 4 each).

For each assigned partition `p`, the worker:
1. Loads the build shard (concatenation of all `.wshf` files at `partition=p/`).
2. Builds an in-memory hash table on the join key.
3. Streams the probe shard for partition `p` (concatenation of all probe-side `.wshf` files at the corresponding S3 prefix), performs the hash join, emits results.
4. Releases the hash table before moving to the next partition.

Per-worker peak memory: `(build_table_size / 4N) × 4 (assigned partitions)` if held concurrently, or `build_table_size / 4N` if processed sequentially. We process **sequentially per partition** to maintain the memory-scales-1/N property.

### Routing Decision

Inserted in the existing `coordinator.ExecuteSQL` routing block, after `CanProbeSplit`:

```
if largest_build.EstimatedBytes > shuffleBuildThreshold (4 GB):
    route to shuffle-distributed path
else if probe-split applicable:
    route to probe-split (existing path, possibly with build cache)
else:
    route to single-worker pipeline
```

The threshold is `var shuffleBuildThreshold = 4 << 30` so tests can lower it to exercise the path on small data.

### Failure Handling

Standard coordinator semantics apply unchanged:
- Worker death during shuffle stage → query fails (same as today's broadcast).
- S3 write failure → task fails → coordinator marks query failed.
- No partial-shuffle retry in this spec (architecturally compatible if added later).

### Memory Accounting

The shuffle task's per-partition buffers are charged against the existing per-task memory budget. With 4N=12 partitions and a 64 KB flush threshold, this is negligible (~768 KB) and does not require budget changes.

The probe-side hash table on each shard is charged against the per-task budget as today. Because shard size is `build_size / 4N`, even a 12 GB original build table becomes a ~1 GB hash table per shard with N=3 — comfortably within the 4-8 GB per-task budget.

## Files Touched (Phase 1, Estimated)

| File | Change |
|---|---|
| `internal/planner/physical/distribution.go` | NEW: `Distribution` type, equality/compatibility helpers |
| `internal/planner/physical/plan.go` | Annotate nodes with `Distribution`; add `Shuffle` node type; insertion pass for single shuffle |
| `internal/coordinator/coordinator.go` | New routing branch for shuffle-distributed; orchestration of shuffle stage → probe stage |
| `internal/coordinator/shuffle_orchestrator.go` | NEW: dispatches shuffle tasks, waits for completion, plans probe partition assignment |
| `internal/worker/executor.go` | Handle `TaskTypeShuffle` task with hash-partitioning sink |
| `internal/worker/shuffle_sink.go` | Extend with `partitionedShuffleSink` variant (per-partition flush) |
| `internal/worker/stream_source.go` | New `partitionShardSource` that reads all `.wshf` files for an assigned partition |
| `internal/engine/exec/hash.go` (or equivalent) | Reuse the existing partition-key hash function — no change expected |
| `benchmarks/local/broadcast_pressure_test.go` | NEW: local memory-pressure repro under tight `GOMEMLIMIT` |
| `internal/coordinator/distributed_tpch_test.go` | New SF0.01 case forcing shuffle path via lowered threshold |

## Validation Plan

**Gate 0 — Local memory-pressure repro (NEW).** Write a Go test that, in a single process under `GOMEMLIMIT=8GB`, loads the SF100-sample `orders` table into a hash twice concurrently to reproduce the broadcast-duplication signature. Confirm it OOMs without the change. With the change (using lowered `shuffleBuildThreshold`), confirm peak heap stays bounded.

**Gate 1 — Unit tests.** `Distribution` property arithmetic, shuffle insertion logic, partitioned sink correctness (round-trip through hash and back), partition assignment.

**Gate 2 — SF0.01 correctness.** All 22 TPC-H queries pass with `shuffleBuildThreshold` lowered to force shuffle on multiple queries. Verify result correctness against the existing baseline.

**Gate 3 — SF10 EC2 distributed run.** Full 22 queries on the standard 3-worker c7g.4xlarge cluster. Confirm:
- No correctness regressions
- Shuffle-routed queries (Q03, Q05, Q07) complete
- Wall-clock for shuffle queries within ~2x of probe-split baseline (extra S3 round-trip is acceptable)

**Gate 4 — SF100 final validation.** Q03 isolated run first (the gate that's been failing). Then full 22 if Q03 passes. Heap profile to confirm peak per-worker memory stays bounded.

## Phase 2 — Out of Scope (Documented for Continuity)

- **Chained shuffle stages** for queries with multiple large builds on different keys (Q09).
- **Memory-headroom-relative trigger** (option B from brainstorming Q4): compute expected per-worker live build bytes and compare against `GOMEMLIMIT × headroom_fraction`. Replaces or augments the static 4 GB threshold.
- **Re-shuffle insertion** when a join's required distribution doesn't match its input's distribution (the general-case planner pass).
- **Inline NATS-based shuffle** (option B from brainstorming Q2): worker-to-worker shuffle without S3, if profiling shows S3 round-trip is on the critical path.
- **Skew handling beyond 4N partitions:** dynamic re-partitioning of hot keys.

## Risks

- **Wall-clock regression on SF100.** Shuffle adds an extra S3 write+read of both build and probe tables. Probe is the big one (~100 GB at SF100). We accept this trade for memory safety; if profiling shows it dominates, revisit transport (Phase 2 inline NATS).
- **Routing thrash.** A static 4 GB threshold may misroute borderline queries. Mitigation: easy to tune; unit tests cover both sides of the threshold.
- **Skew on real-world keys.** 4N partitions reduce but do not eliminate hot-key impact. Mitigation: documented; future skew-handling work tracked separately.
- **Phase 1 interaction with `BuildCache`.** Both paths exist; routing must pick exactly one. Cleanly handled by the threshold: builds above 4 GB → shuffle (no cache); builds 2-4 GB → cache; builds < 2 GB → broadcast.

## Open Questions

None blocking. Phase 2 questions (chained shuffle planner pass, memory-headroom trigger, skew handling) are deferred by design.
