# 1:1 stage-chain fusion (join→join, join→partial-aggregate)

Status: shipped with the implementation (kill switch `WADJET_STAGE_FUSION=0`;
the join→agg absorb has its own sub-switch `WADJET_STAGE_FUSION_AGG=0`).

SF100 kill-switch A/B of the join→join step (2026-07-25, PR #269): suite
scratch 261.8 → 212.2 GB (−19%), join-class −59%, Q18 scratch −56% and
wall −38.8% cold / −22.4% steady, rows 44/44 both arms.

## Problem

In the native DAG, every stage's output materializes to scratch/S3 and is
read back by the next stage. For 1:1 same-distribution chains — consumer
task *i* reads exactly producer task *i*'s output — locality placement
(ADR-0008, `--locality-placement`) already co-locates the endpoints, so the
bytes stay on one worker, but each link still pays a full encode → NVMe
write → page-cache ride → decode round trip, plus the eager-durability S3
upload of bytes nothing else ever reads.

Measured on main bdef5ce (results/20260725-123737, SF100 steady): Q18
scratch is 72.2 GB, of which **join-class outputs are 48.9 GB** — the
join-8 → join-10 class chain (`distribution_count=24`, dep 1:1). memmove
(materialization copies + encode/decode) is 14.1% of worker CPU; s2 eager
upload encode ~9%, much of it insurance bytes on exactly these links.

## Rewrite

A late planner pass, `fuseStageChains` (`fuse_stage_chains.go`), absorbs a
1:1 downstream join into its upstream hash_join so the pair dispatches as
ONE fragment — producer operators feed consumer operators batch-by-batch
in-process; the intermediate file never exists.

Eligibility (v1, all required):

- **P** (producer): `hash_join`, `Distribution{HashPartitioned, N}`, no
  `Exchange`, no `SortKeys`/`Limit`/`GroupByCols`, no probe-split/
  build-cache/merge-tree fields.
- **C** (consumer): `hash_join` or `broadcast_join`; P's **only** consumer;
  `C.LeftDepStage == P.ID` (P feeds C's probe — never its build);
  `C.RightDepStage != P.ID` (self-join both-sides excluded);
  `Distribution{HashPartitioned, N}` with the same N (this is the 1:1);
  same field restrictions as P.

Rewrite (P survives, C is dropped):

- `P.ChainedJoins += [C.FusedJoins…, C.primary]` — a new `ChainedJoinSpec`
  list executed **after** P's primary probe (unlike `FusedJoins`, which run
  before). C's primary spec sets `Partitioned = (C.Type == hash_join)`:
  its build input is hash-partitioned 1:1 and each task reads its own
  partition slice; broadcast builds ride whole (and stay shared per-worker
  via the broadcast-join cache, so per-worker memory matches the unfused
  plan). `C.FilterExprs` become the chained spec's `FilterExprs` (an
  `OpFilter` right after that probe).
- `P.Dependencies += C's build deps`; every downstream reference to `C.ID`
  (Dependencies / LeftDepStage / RightDepStage) rewires to `P.ID`.
- `P.Distribution = C.Distribution` (identical kind/count by gate; keys
  kept exact so downstream labels and `Satisfies()` are unchanged),
  `P.EstimatedBytes += C.EstimatedBytes`, `P.EstimatedRows = C.EstimatedRows`.
  `P.Columns` stays P's own projection (the chain still needs those
  columns mid-stream); C's output projection rides the chained spec as its
  probe op's `OutputColumns`, so the fused output schema is exactly C's.
- The pass iterates to fixpoint, so join→join→join chains collapse into
  one fragment (each link re-checked against the gates).

Runs immediately after `rewireAggOverRawExchange` + the elide passes in
`PlanDistributed`, when distributions are final; before
`fuseScanAggregateShuffle` and dynamic filters, so both see the fused graph.

## Dispatch and execution

`dispatchComputeStage` translates `ChainedJoins` to wire ops appended after
the primary probe in `buildJoinFragment`:

```
[OpShuffleSource(P probe part i), P.FusedJoins…, P.primary(build part i),
 OpFilter?(P residual), {chained probe, OpFilter?}…, OpSort?(folded), sink]
```

- Partitioned chained builds slice via `partitionFilesForWorker` (task i ↔
  partition i, same as the primary build); broadcast chained builds flatten
  the replicated output, exactly like `FusedJoins` today.
- The worker fragment runner needs **no changes**: `buildUnaryChain`
  already composes multiple `OpHashJoinProbe`/`OpBroadcastProbe` ops in
  spec order, each loading its own build (broadcast ones through the
  shared per-worker cache), and all probe ops are `Cloneable`, so morsel
  parallelism is preserved (the per-consumer clone chain gets longer; the
  width gate is unchanged).
- Output: the fused stage writes what C wrote — same partition count, same
  per-partition rows/bytes accounting, so downstream skew detection and
  consumers see an identical interface. **No file-count amplification**
  (task count is unchanged), which is what killed the old
  fuseScanShuffle/fuseJoinShuffle passes.

## Interactions

- **Worker admission / memory (hazard: fused build residency).** The fused
  task holds P's partition build and C's build simultaneously.
  `estimateComputeTaskBytes` sums over the union of deps (partitioned ÷ N,
  replicated whole), so admission sees the sum; hash-join spill degrades
  gracefully past budget. Broadcast chained builds don't increase
  per-worker residency vs the unfused plan (shared cache, same N readers).
- **Skew split.** Still applies to the fused stage (it stays `hash_join`
  with partitioned probe/build accounting) **unless** a chained spec is
  `Partitioned` — a skew sub-task's index no longer equals its partition
  index, which would mis-slice the chained build, so the gate refuses
  those stages (logged). Broadcast-only chains (the SF100 Q18 shape)
  remain skew-eligible.
- **Eager consumer dispatch.** `eagerEligibleJoinConsumer` requires
  `len(Dependencies) == 2+len(FusedJoins)`; fused stages carry extra
  chained-build deps and therefore keep the completion barrier by
  construction. Extending eager feeds to fused stages is a possible
  follow-up.
- **Streaming exchange / durability.** The elided link has no file: no
  adoption, no peer serving, no eager-upload insurance bytes. Consumers of
  the fused output read it exactly as they read C's output before.
- **Retries** re-run both joins in one task — idempotent same-key
  overwrites, unchanged failure classification.
- **Locality placement** hints now point at the chain's upstream inputs
  (P's probe/build partitions), which is where the bytes actually are.
  Peer-location annotation walks chained build files via `Operators[]`.

## Kill switch and tests

`WADJET_STAGE_FUSION=0` (exported `physical.StageFusion` atomic.Bool, same
pattern as `AggOverExchange`; tests pin either arm). Plan-dump regression
tests cover fused vs unfused shapes for the Q18/Q05/Q10 chain patterns and
the interplay with `AggOverExchange`; a multi-worker e2e diff runs the
affected TPC-H shapes through both arms and compares rows.

## Step 2: join→partial-aggregate absorb

After the join→join fixpoint, a producer whose sole consumer is a PARTIAL
aggregate (`aggregate` stage, single dep, no GroupByAll / filters / sorts
/ exchange / scan-side fused-agg fields) absorbs it as the chain terminal:
`ChainedAggGroupBy`/`ChainedAggSpecs` on the fused stage, dispatched as a
raw-mode `OpHashAggregate` breaker after the chained joins (the fragment
runner's multi-breaker path). AVG decomposes to (SUM, COUNT) at dispatch
exactly as the dropped stage's own dispatch did, so the downstream final's
merge and avg-fold are unchanged.

Partial aggregation is partition-agnostic, so this absorb needs **no 1:1
or same-distribution gate** — and the fused stage **keeps its own
Distribution and task count** (the dropped stage's RoundRobin label only
described its fan-out; adopting it would collapse the join's 1:1
task/partition dispatch). The final simply merges N per-task partials
instead of the old fan-out count. A stage that absorbed an aggregate is
chain-terminal (producer/consumer gates exclude it).

This is the shape Q09/Q10 have at SF100 (their join→join links are real
re-key repartitions, but `aggregate` consumes the last join 1:1), and it
deletes the fused Q18 stage's remaining output materialization.

## Out of scope

- Producer types other than `hash_join` (broadcast_join producers with
  hash-partitioned output didn't appear in any TPC-H chain scout;
  broadcast_join→agg Singleton links are small).
- Skew-splitting stages with partitioned chained builds (needs
  partition-indexed slicing for sub-tasks).
