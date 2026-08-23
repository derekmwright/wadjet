# Sort-merge join for big-vs-big shuffled joins

Status: DESIGN — grounded against main @ `9dbf16e` (2026-07-07), awaiting review.
Scope: new `exec.SortMergeJoin` operator + planner/coordinator/worker wiring,
flag-gated and dormant by default (the memory-accounting-overhaul rollout
pattern: land dark, activate deploy-gated).

## 1. Motivation

The join-task memory model is what pins worker concurrency. A shuffled hash
join materializes one side's partition as a hash table (grace partition-on-
arrival, `join_partition_arrival.go:48`), reserving against the shared pool
(`worker.go:214-219`, budget = MemoryBudget × MaxConcurrent). Resident builds
are what historically forced `max_concurrent` down to 3 and what the Q17/Q18
"mc=4 failure mode" (`join.go:711`) was about; today's cooperative-spill
machinery makes it *safe*, but a big build still consumes pool budget that
would otherwise admit more tasks (`waitForPoolHeadroom`, `worker.go:594,642`),
and under pressure the grace path burns re-spill churn (evict → probe-side
disk routing → per-partition reload/rebuild).

A sort-merge join streams both sides in join-key order: resident memory is
O(sort-run buffer + one batch per merge cursor), independent of either side's
size. That converts the worst joins from "resident-build vs pool budget"
into "sequential disk bandwidth", which is the memory-constrained-scaling
shape this project prioritizes. It also mechanically removes grace-probe
respill (each side is written once as sorted runs and read once).

The honest trade: hash join streams its probe side with ZERO materialization.
SMJ materializes BOTH sides into sorted runs when they don't fit. So SMJ is
not a general hash-join replacement — it wins precisely when the build side
is large enough that grace eviction/respill and pool starvation dominate,
i.e. the both-sides-large class (SF100 lineitem⋈orders, Q18; the forced-
shuffle join family in `distributed_tpch_test.go:1417-1596`).

## 2. Grounded facts the design stands on (verified 2026-07-07)

- **Co-partitioning is already guaranteed.** Both sides of a shuffled join
  hash-partition with the identical inlined FNV-1a over the same key columns
  and the same `numParts` (`partitioned_shuffle_sink.go:698-843`), and one
  join task receives partition *p* of BOTH sides
  (`orchestrate_repartition.go:365-400`, `stage_output.go:90-128`). SMJ
  needs no new exchange machinery.
- **Partition files are unsorted and readers are order-agnostic.** `.wshf`
  chunks carry no ordering and may interleave across concurrent consumers
  (`shuffle_format.go:159-187` writes them, `internal/wshf/decode.go`
  reads them order-agnostically; `partitioned_shuffle_sink.go:74-78`); the
  streaming-exchange fetch path never inspects order
  (`stream_source.go:168-194`, `peer_exchange.go:145-225`).
- **The sorted-run facility is shared and SF100-validated.** Sort's external
  path produces sorted columnar runs (`sortBatchesToRun`,
  `sort_external.go:184-213`) over the shared spill codec
  (`spillBatchWriter/Reader`, `join_spill.go:242-432`, nested types
  included), merges them streaming with bounded fan-in
  (`runMerger`/`runCursor`/`preMergeRuns`, `sort_external.go:220-427`,
  `maxMergeFanIn=64`), and Window already consumes the same machinery
  (`window_external.go:348`), including a re-sort-by-new-keys precedent
  (`resortRunsByKeys`, `window_external.go:280`).
- **Typed comparators are standalone.** `kernel.SortCompareKernel`
  (`kernel/types.go:167`) compares a row of one vector against a row of
  another and is resolved per type; `mergeHeap.Less`
  (`sort_external.go:311-326`) shows the multi-key composition pattern. A
  two-stream join comparison is the same composition over two cursors.
- **No interesting-order machinery exists.** Nothing tracks sortedness
  between stages (`Distribution{Kind,Keys,Count}` has no order field;
  `Stage.SortKeys` only gates serial execution). Order is operator-internal
  today.
- **The seams are single points.** Local: `buildJoin`
  (`plan.go:3857`, the one place `exec.NewHashJoin` is constructed) serves
  embedded, local fast path, and standalone alike. Distributed: walkStages'
  join case (`plan.go:3306`) already branches broadcast vs hash_join; op
  types live in `messages.go:254-283`; the worker maps op→operator in
  `buildFragmentUnary` (`executor_fragment.go:1556`).
- **Memory contract template = Sort.** Lazy `RegisterAccounted` on first
  consume, `TrackBatch`/`PublishOwned` on buffering, self-spill at
  `ShouldSpillFor(SpillCheap)` past `minSortRunBytes`, `EstimateRelief`/
  `SpillSome` for cooperative relief, `OpClosed` + unregister at finalize
  (`sort.go:81-141,381-427`).

## 3. Design

### 3.1 Where the sort happens: at the join, not at shuffle-write (v1)

The sort is operator-internal to the join task. Each side's partition input
streams into a run-generation phase (Sort's machinery verbatim: buffer under
tracker, self-spill sorted runs, in-memory remainder as the last cursor),
then a two-cursor merge joins the two sorted streams.

Rejected for v1 — sorting at shuffle-write:

- Sorting each producer's whole per-partition contribution before writing
  breaks the shuffle sink's memory bound (64 KB/partition accumulators,
  `flushPartitionBytes`) — it would buffer entire partition contributions.
- Sorting individual 64 KB chunks keeps the bound but yields thousands of
  micro-runs per partition (1 GB partition ⇒ ~16k runs vs `maxMergeFanIn`
  64), forcing multi-level pre-merge at the join task anyway — the read-side
  work survives, the format acquires order semantics, and the interleaving-
  tolerant sink + fetch paths (§2) would all need re-auditing.
- There is no order-property plumbing to let the planner *know* an exchange
  output is sorted (§2). Building that is real work with one consumer.

Sort-at-write over sorted exchange becomes attractive only with order
properties in the distribution system; that is explicitly future work.

### 3.2 The operator: `exec.SortMergeJoin`

A pipeline breaker on BOTH sides (inherent to sort-based joins over unsorted
input; hash join breaks on one). Shape:

- **Inputs.** Build-side `Source` (as HashJoin.Build takes today) plus the
  probe side arriving via `Consume` — both funnel into per-side
  `smjSideState`, each of which is Sort's buffering/spill core parameterized
  by the side's join-key sort keys (ascending, nulls last).
- **Sortedness.** Keys sort ascending; rows with a NULL in any join key are
  excluded from the merge at run-generation time (SQL equi-join semantics:
  NULL matches nothing; this also mirrors the hash paths where null keys
  produce no index entry). For LEFT variants (post-v1) null-key probe rows
  bypass the merge straight to the unmatched output.
- **Merge.** Two cursors (`runMerger` per side — the existing N-way merger
  already collapses each side's runs into one sorted stream; SMJ composes
  two of them). Advance-lesser-side loop using one resolved
  `SortCompareKernel` per key. On key equality, the classic duplicate-group
  algorithm: materialize the RIGHT side's current key group into a small
  buffer, emit the cross product against each LEFT row of the same key,
  advance. The group buffer is tracker-Reserved; group size is bounded by
  per-key duplication (TPC-H: ≤7 lineitems per orderkey), and a
  pathologically hot key fails loudly via Reserve rather than OOMing —
  documented bound, spillable group buffering is future work if real data
  demands it.
- **Output.** Same gather/output-column semantics as HashJoinProbe
  (OutputColumns filter honored); emits `DefaultBatchSize` batches.
- **Join types v1: Inner only.** The big-vs-big class is inner joins. LEFT/
  SEMI/ANTI are mechanical extensions of the merge loop (emit-unmatched /
  emit-on-first-match / emit-on-no-match) staged after v1 correctness soaks.
  No JoinFilter in v1 (falls back to hash join).

### 3.3 Memory contract (never-OOM)

One `AccountedOperator` registration owning both side states, mirroring
Sort's checklist exactly (§2 last bullet): buffered batches
`TrackBatch`+`PublishOwned`; self-spill a side's buffer to a sorted run at
`SpillCheap` past the run floor; `SpillSome` sheds the larger buffered side
first (both sides are spillable pre-merge — strictly better than HashJoin,
whose arena/index can never shed). Merge phase holds one batch per live
cursor per side (fan-in ≤ 64/side via `preMergeRuns`) plus the duplicate
group buffer. Morsel interaction: SMJ is not `Cloneable` in v1 → the
breaker path runs it k=1 automatically; no new parallel-consume surface.

### 3.4 Planner gate

A join takes SMJ only when ALL hold (else the existing hash paths, entirely
unchanged):

- equi-join, `JoinType == Inner`, no `JoinFilter`;
- not a broadcast candidate (`isBroadcastCandidate` false — small builds
  keep the strictly-better broadcast/hash path);
- BOTH sides' estimated bytes exceed `SortMergeJoinThreshold`. The build
  side estimate exists today (`isBroadcastCandidate`'s selectivity-scaled
  walk, `plan.go:3689-3725`); v1 mirrors that walk on `node.Children[0]`
  for the probe side (CBO `RelStatsOf` scaling included). Threshold default
  proposed: the worker pool budget signal the coordinator already computes
  for broadcast (`broadcastThresholdFromCluster`, `workers.go:334`) scaled
  up — concretely `4×` the per-worker memory budget, i.e. "this build
  cannot sit resident without dominating the pool". Single knob
  (`--sort-merge-join-bytes`), `0 = disabled` = the shipped default.

Decision sites: local `buildJoin` (`plan.go:3857`) grows an SMJ branch
before `NewHashJoin`; distributed walkStages' join case (`plan.go:3306`)
emits `StageSortMergeJoin` instead of `StageHashJoin` — the exchange-
repartition children are IDENTICAL (co-partitioning already suffices), so
the shuffle plumbing, streaming exchange, and task retry story carry over
untouched.

### 3.5 Distributed wiring

- `messages.go`: `OpSortMergeJoin` op type; OpSpec reuses the join fields
  (LeftKeys/RightKeys/BuildAlias/BuildFiles/OutputColumns) — no new fields
  expected in v1.
- Coordinator: `buildSortMergeJoinFragment` alongside `buildJoinFragment`
  (`execute_stage_dag.go:2284`); dispatch gate alongside the join branches
  (`:2014,:2205`). Task `EstimatedBytes` for admission should reflect SMJ's
  smaller resident footprint — v1 keeps the existing estimate (conservative:
  over-admission is the risk to avoid; relaxing admission is the PAYOFF
  step and comes only after SF100 soak).
- Worker: `buildFragmentUnary` maps the op type; classified as a breaker
  (`isFragmentBreakerOp`, `executor_fragment.go:1503-1511`) so it runs on
  the validated breaker path.

### 3.6 What this explicitly does not do (v1)

- No order-property/interesting-order plumbing (future: sorted exchange,
  sort elimination when upstream ORDER BY exists).
- No LEFT/RIGHT/FULL/SEMI/ANTI, no JoinFilter (hash paths remain).
- No admission-estimate relaxation, no `max_concurrent` change — those are
  the *reward* once SF100 evidence is in, not part of the operator PR.
- No morsel-parallel SMJ consume.

## 4. Phases and gates

1. **Operator PR**: `exec.SortMergeJoin` + two-stream merge; unit tests
   (dup groups both sides incl. cross-batch groups, null keys, empty sides,
   one-side-empty, forced-spill runs on both sides, nested-type payload
   columns, -race), micro-bench vs HashJoin build+probe at in-memory and
   spilled scales. Accounting truth-tests mirroring Sort's.
2. **Local planner PR**: `buildJoin` gate + flag; TPC-H SF0.01 22/22 with
   the flag FORCED on (threshold=1) — every inner equi-join in the suite
   routed through SMJ and row-identical; then flag-off default confirms
   dormancy (byte-identical plans).
3. **Distributed PR**: stage/op wiring; `tpch-harness --mode=local` both
   flag states (forced-on = the correctness gate; the harness's forced-
   shuffle recipes already exist); -race harness arm.
4. **Activation evidence** (deploy-gated, user-approved per deploy): SF10
   same-window A/B flag-on vs off; then SF100 same-window pair watching
   Q18/Q17/Q05 walls, pool headroom waits, spill volumes, zero reaps.
   Only after that: the admission/concurrency payoff experiments.

Hard rules carried: architectural fix not benchmark chasing; no per-query
special cases; harness-local before EC2; every deploy individually
preflighted; no revert on flat wall if CPU/memory wins are real.

## 5. Open questions for review

1. Threshold shape: fixed bytes knob vs fraction of pool budget (proposal:
   knob with pool-scaled default, matching the broadcast pattern).
2. Right-side duplicate-group buffer: accept the loud-Reserve bound for v1
   (proposed) or require spillable group buffering upfront?
3. Is Inner-only v1 acceptable given Q18-class is the target (proposed
   yes)?
