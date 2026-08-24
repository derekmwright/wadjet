# Native-DAG Distributed Execution — Internals Map

> **Audience:** engineers and agents navigating the distributed query path. This
> is a *machinery map* with `file:line` anchors, not a tutorial. Line numbers
> drift — treat them as starting points and confirm against current source.
> Last verified: 2026-06-29 (main, post-#161).

## TL;DR

A SQL query becomes a **DAG of `Stage`s** that workers execute as **fragments**
(pipelines of `OpSpec` operators), with intermediate results materialized to the
object store between stages and a terminal **gather** streaming the result back
to the coordinator.

```
SQL → logical.Node → PlanDistributed → []Stage → executeStageDAG → worker fragments → gather
```

There are **two coordinator entry paths** — know which one you're in:

| Path | Entry | Planner | Merge/dedup | Used by |
|---|---|---|---|---|
| **Native-DAG** (primary) | `Coordinator.ExecuteSQL` (`coordinator/coordinator.go:520`) | `PlanDistributed` → multi-stage DAG | inside the DAG (aggregate/sort/exchange stages); **no `MergeInfo` post-pass** | pgwire, gRPC, HTTP server, alerts, benches — **all production reads** |
| **Probe-split pipeline** (legacy/secondary) | `Coordinator.SubmitSQL` (`coordinator/coordinator.go:~2160`) | collapses to one `pipeline-0` stage | `mergeProbePartials`/`deduplicatePartials` using `logical.MergeInfo` (`coordinator/coordinator.go:1154,1223`) | async submit path |

⚠️ **These paths handle DISTINCT differently.** The probe-split path dedups via
`MergeInfo.HasDistinct`; the native-DAG path does **not** — it relies on the
logical rewrite having turned every user DISTINCT into an aggregate, plus a
post-gather `dedupGatherResult` for the root-path shapes the rewrite declines
(see §Where dedup / aggregate / distinct happen).

## Small-query local fast path (routing ahead of the DAG)

Before planning a DAG, `ExecuteSQL` routes small queries onto a
**coordinator-local single-process pipeline** (`coordinator/local_fastpath.go`
`tryLocalFastPath`): when `EstimatePlanScanBytes`
(`planner/physical/scan_estimate.go` — catalog bytes after partition-filter
pruning, summed over every scan in the optimized logical plan) stays under
`Config.LocalFastPathBytes`, the query executes in-process via
`physical.Planner.Plan` (the `wadjet.DB` engine) and streams columnar batches
straight into `SQLResult`. No task dispatch, no per-stage object-store
materialization — the DAG's fixed costs are O(stages) and independent of data
size, so small queries pay a latency floor the local pipeline doesn't have
(measured: DAG 2–12× slower at zero store latency, top-N worst; the full
tiny-scale TPC-H suite runs row-identical through either path).

Facts that matter when touching this:
- Both paths consume the **identical optimized logical plan** (post
  RLS-injection, post `logical.Optimize`) — result and policy parity is by
  construction. A local PLAN error falls back to the DAG (which covers
  shapes the local pipeline declines); an EXECUTION error falls back only
  when it is unrelated to the query's meaning — see `classifyLocalFailure`
  and `FastPathStrict` in coordinator/local_fastpath.go (#308).
- Unestimable plans (unknown table = table functions, residual subquery
  expressions in Raw predicate/projection text) route to the DAG.
- Concurrency-capped (`localSem`); overflow routes to the DAG, never queues.
- **Adaptive bail-out:** scan input is bounded by the estimate but join
  output is not — the collect sink carries a result budget (8× the routing
  threshold; `CollectSink.MaxBytes` → `exec.ErrCollectBudget`), and an
  over-budget local run aborts and re-dispatches as a DAG query (reads are
  idempotent; the DAG gather spills oversized results to scratch). This
  makes the threshold a latency knob, not a correctness-of-judgment knob.
- `Config.LocalFastPathBytes <= 0` = disabled (the zero value, so library
  and test usage is DAG-pure by default); `wadjet serve` enables it by
  default via `--local-fastpath-bytes` (64 MiB).
- The tpch-harness spawns coordinators with `--local-fastpath-bytes=0` so
  the DAG gate keeps meaning; re-enable via
  `--serve-args=--local-fastpath-bytes=N` (later flag wins) to run the same
  suite through the fast path.
- Differential gate: `coordinator/local_fastpath_test.go` runs every shape
  through both paths and diffs rows. **It found #169 on its first run.**

## ABAC on the coordinator paths

With an auth provider wired (`Coordinator.SetAuthProvider`, done by
`wadjet serve` whenever auth is configured), `ExecuteSQL` enforces access
policies itself via `auth.EnforcePlanPolicies` — the same helper the
embedded engine calls — table denial, row-filter injection, and column
deny/mask, applied to the logical plan after scan annotation and before
`Optimize`, so both the local fast path and the DAG consume the enforced
plan. This is what lets pgwire route **authenticated** connections through
the coordinator (`canBypassDB` accepts when `coord.EnforcesABAC()`);
previously every authed connection fell back to the legacy single-process
`db.Query` path — no distribution, no fast path.

Distributed subtlety: `InjectColumnPolicies` wraps the scan in a
**security-barrier Project** (`Node.SecurityBarrier`) — masked columns as
literals, denied columns absent. Ordinary Projects are walkStages
passthroughs, but a dropped barrier leaks raw values, so `walkStages`
absorbs it into the scan stage (`absorbSecurityBarrier` →
`Stage.SecurityProjectExprs`) across any pushed-down Filters in between;
scan fragments apply it as the first `OpProject` — before SELECT-list
projections and before fused partial aggregates — so restricted values
never leave the worker. Parity gate:
`coordinator/abac_distributed_test.go` compares fast path and DAG against
the embedded engine per policy shape.

## Planning pipeline (logical → stages)

`PlanDistributed` (`planner/physical/plan.go:1579`):
1. `AnnotateScanColumns` — scan column metadata for shuffle-key assignment.
2. `generateStages` (`plan.go:2456`) → `walkStages` (`plan.go:2919`) builds the raw stage list, then `fuseJoinStages` / CTE flattening.
3. `assignStageDistributions` + `EnsureDistribution` (`plan.go:1595,1612`) — the **distribution-property system**: each `Stage` gets a `Distribution` (Singleton / HashPartitioned{keys,count} / …); `EnsureDistribution` inserts `exchange-*` stages where a stage's input requirement isn't met by its child's output. This is *the* mechanism that introduces shuffles.
4. Collapse/fuse passes: `collapseMergeTreesForNativeDAG`, `fuseSortIntoPredecessor`, `fuseScanAggregateShuffle` (`plan.go:1601-1659`).
5. `applyDynamicFilters`, `AssertExchangeConsistency`, attach SELECT aliases to the terminal `exchange-gather`.

### `walkStages` per-node behavior (`plan.go:2919`)

| Logical node | Emits | Notes |
|---|---|---|
| `NodeScan` | `scan` stage (tasks = file/row-group split) | `buildScan` is the single-process analog |
| `NodeJoin` | `hash_join`/`broadcast_join` (+ `exchange-repartition` shuffles for non-broadcast) | keys parsed at plan time (`plan.go:3145+`); RIGHT/FULL never broadcast — see §Outer joins |
| `NodeAggregate` | `aggregate` (partial) → `final_aggregate` | two-phase (`plan.go:3034-3103`); the distribution pass adds the exchange |
| `NodeSort` | `sort` → merge-sort tree | `plan.go:3105`. A key naming a term the SELECT list drops is a `__sortkey_N` that no stage emits — see §Synthetic sort keys. |
| `NodeWindow` | `window` | `plan.go:3384`. One task per PARTITION BY partition when the input already arrives clustered on those keys; Singleton otherwise — see §Window. |
| `NodeFilter` | pushes predicates onto child stage | `plan.go:3323` |
| `NodeUnion` | `union` (+ a `GroupByAll` `final_aggregate` when not ALL) | `set_op_stages.go`. One task per arm: task *i* reads arm *i*'s whole output and projects it onto the result column names and types, so the stage's files ARE the concatenation. |
| `NodeIntersect` / `NodeExcept` | `union` (with per-arm tag columns) + a grouped counting `final_aggregate` | `set_op_stages.go` (#346). The distribution pass inserts an `exchange-repartition` on the full result row between them — see §Set operations. |
| **`NodeDistinct`** | **nothing — passthrough** | `default` case `plan.go:~3415`; walks children only. No USER DISTINCT reaches here — `logical.rewriteDistinctAsGroupBy` (optimizer) turns every `Distinct(Project)` in the tree, at any depth, into an aggregate-free `NodeAggregate` first, so it rides the aggregate stages (#466 widened this from the root path only). What still passes through: planner-inserted `BuildSideDedup` Distincts (semi/anti build dedup, decorrelated semijoin key source), which carry no user-visible semantics, and root-path fallback shapes the coordinator dedups after the gather. A user Distinct anywhere else is REFUSED by `refuseUnstageableDistinct` (`physical/distinct_refusal.go`) rather than dropped, and the coordinator answers it on the local single-process pipeline. |
| **`NodeProject`** | **nothing — passthrough** | same `default` case; aliases recovered at gather |

## Synthetic sort keys: the column a Project would have computed

`ORDER BY b` over `SELECT a` names a column the SELECT-list Project drops, so
`logical.resolveOrderBy` MATERIALIZES the term as a hidden projection called
`__sortkey_N` and points the Sort at it. The single-process pipeline runs that
Project as a real operator below the Sort and `hiddenSortTrimOp` drops the
column again before the client sees it.

**On the DAG an ordinary Project emits no stage**, so the name exists nowhere
unless a pass writes it onto the fragment producing the sort's input.
`attachScanSelectProjections` does that — but only for the OUTERMOST SELECT
list, the one feeding the terminal gather. Every other sort (an ORDER BY
inside a derived table or CTE, whose consumer is an aggregate or a join)
reached a fragment emitting no such column and the task failed with
`sort: key column "__sortkey_0" does not exist in the input schema` on a query
the fast path answered (#424).

`resolveHiddenSortKeys` (`planner/physical/hidden_sort_key.go`) settles them,
and runs **last** in `PlanDistributed` — after `attachScanSelectProjections`,
because the repair depends on what that pass did:

- key already emitted → nothing to do;
- a plain column reference → the KEY is renamed to its source column, which
  the producer already ships under its own name (the DAG's convention);
- a computed term → projected INTO the producing fragment under the hidden
  name, the `Stage.ProjectExprs` → `OpProject` machinery (#169, #383).

Running late is load-bearing in the other direction too: a key renamed BEFORE
`attachScanSelectProjections` falls outside that pass's sort-key coverage
check on `SELECT a, LENGTH(b) AS l … ORDER BY c`, and the whole projection —
including the computed select item — would be declined. The definition rides
`SortKeySpec.SourceExpr`/`SourceColumn`/`SourceType` rather than the stage
because every pass that MOVES an ordering (`fuseSortIntoPredecessor`'s fold
onto a join or aggregate, `emitMergeSortTree`, the gather's
`Exchange.Ordering`) copies the key slice wholesale.

This is the follow-on to #390: that guard keeps a sort with a dependent as its
own stage rather than folding it into a predecessor dispatch may re-fan-out,
and that stage is exactly the one whose input had no key to sort on.

## Stage → worker fragment conversion

Every stage dispatches as a **fragment**: a list of `distributed.OpSpec`
operators (`distributed/messages.go:260`). The worker requires `Operators` to be
non-empty (`worker/executor_stage.go:22`).

Conversion lives in `coordinator/execute_stage_dag.go`:
- `buildAggregateFragment` (`:2373`) → `OpShuffleSource` + `OpHashAggregate{GroupByCols, Aggregates, MergeMode, InputRowBound}`. **`MergeMode = stage.Type=="final_aggregate"||"merge_aggregate"`** (`:2400`) — merge mode rewrites `InputCol→OutputCol` and `COUNT→SUM`. `InputRowBound` is `aggregateInputRowBound`: the exact Σ`PartitionRows` over the partitions bound to this task (0 = unknown), which decides the worker aggregate's group-index layout — see `docs/design/unbounded-final-aggregate-layout.md`.
- `buildSortFragment` (`:2514`), shuffle dispatch `dispatchShuffleStage` (`:772`).
- Terminal stage gets an `OpGatherSink` when `gatherReplySubject != ""`.

### A base table reaches the worker THREE ways, and all three must declare its types

A parquet file cannot express nine of this engine's types (IPv4, IPv6, MAC,
UUID, BYTES, PORT, PROTOCOL, DURATION have no logical annotation to write;
CIDR is written as plain UTF8). Files written from v0.18.0 on carry the
declared schema in their own footer (`parquet.DeclaredSchemaKey`); files
written before it do not, and a reader that types from the FILE answers
`167772165` where the catalog says `10.0.0.5` (#396, then #423 for the
migration boundary). The catalog's answer therefore rides the plan —
`physical.Stage.ScanSchema`, filled by `annotateScanSchemas` at the end of
`PlanDistributed`, at PLAN time so every task of a query sees one catalog
revision.

**The trap when adding a base-table read path**: a plain unfiltered scan
dispatches NO TASKS. `executeStage` passes its parquet keys through as a
`StageOutput{Kind: OutputSinglePart, ScanTable, ScanColumns, ScanSchema}`, so
the table is read by the CONSUMER stage, not by a scan fragment. The three
carriers, and where each is read:

| Carrier | Set by | Read by |
|---|---|---|
| `OpSpec.ColumnTypes` | the two `OpScan` builders (`dispatchScanStage`, `buildScanAggregateFragment`) from `stage.ScanSchema`; `applySourceColumnTypes` for a pass-through input, from `stageInputScanSchemas` | `worker.buildFragmentSource` |
| `OpSpec.BuildColumnTypes` | `applySourceColumnTypes`, keyed by `BuildAlias` | `worker.applyBuildSchema`, both join build sources |
| `Task.ColumnTypes` | `runShuffleSide` from the synthetic source stage | `executeShuffleTask`'s implicit parquet scan |

All three land on `cachedFileStreamSource.SetDeclaredSchema` and are applied
per file in `finishParquetState` via `parquet.Reader.SchemaAs`, which IS
`retypeFromCatalog` — the row reader's own admission, so the two read paths
cannot disagree about what a declaration may replace. Where the footer key
exists the two agree and the substitution is a no-op; where they disagree the
catalog wins if the file's bytes can carry its type and the open FAILS by
name if they cannot.

`applySourceProjection` (what to read) and `applySourceColumnTypes` (what it
is) share `stageInputDeps` for the alias→dependency mapping, so they cannot
disagree about which alias is which input. **A new dispatcher that reads base
parquet has to pick one of the three carriers**, or its columns silently
revert to the file's own types — which is the #396 symptom, one path further
out. Gate: `coordinator.TestTypeMatrixTwoPathWithoutDeclaredSchemaFooter`,
which runs the type-matrix corpus over footer-stripped files
(`parquet.StripDeclaredSchema`).

Worker side: `buildFragmentUnary` (`worker/executor_fragment.go:952`) and
`buildFragmentHashAggregate` (`:788`). **Gap:** the hash-aggregate fragment
requires `len(GroupByCols)>0 || len(Aggregates)>0` and has **no `GroupByAll`**
— the single-process `buildDistinct` (`plan.go:5583`) uses
`HashAggregate.GroupByAll=true`, which has no distributed equivalent yet.

### Where a fragment's inputs are visible to the annotator

`coordinator.annotateTaskPeerLocations` (`coordinator/peer_locations.go`) is
the only thing that turns a completed producer task into a consumer's
peer-location hint, and it can only hint file lists it walks. It walks
`Task.{Files,InputFiles,BuildFiles,Inputs,PreScannedInputs,ScanFileFilter}`,
`FusedJoins[].BuildFiles`, and — since 2026-08-22 — `Operators[].BuildFiles`
**and `Operators[].InputFiles`**.

That last one matters because the two are not redundant. Every dispatcher
except one mirrors its fragment's inputs into `Task.Inputs` as well as into
`OpShuffleSource.InputFiles`; `dispatchFinalAggregateFanout`
(`execute_stage_dag.go`, the `final_aggregate-N-merge-K` tasks and the final
merge over their `-interm-` outputs) does not. Before the annotator walked
op source inputs, that whole task class was dispatched with a fetch token
and **no hint at all**: no Tier-1.5 peer read was ever attempted, the first
S3 `Get` hit the producer's still-in-flight background upload, and the task
sat in `awaitDurableObject`. SF100 window 2 measured that as 12–14.5 s of
critical path per suite run, on 4-row tasks, with `peer_fallthroughs = 0`
across every arm (nothing ever fell through because nothing was ever
tried) — `docs/benchmarks/sf100-window2-analysis-2026-08-22.md` §7.1.

Kill switch `WADJET_INTERM_PEER_HINTS=0` restores the builds-only hint set.
Hints are advisory on every path — a stale or absent one costs one failed
fetch and the read falls through to the same durable copy — so the only
thing this can move besides read tier is placement, via
`pickLocalityWorkerFrom` (`--locality-placement`), which co-locates a task
only when *all* of its hints name one worker.

**The rule for new dispatchers:** put a fragment's input keys somewhere the
annotator walks. `Operators[].InputFiles` now qualifies, so a fragment-only
dispatcher is fine; a dispatcher that invents a third home for input keys
has to teach the annotator about it or its consumers lose the peer tier
silently.

### The coordinator is a consumer too

Two coordinator-side reads pull stage outputs directly, with no task and
therefore no annotator involved: `substituteScalarDependencies` →
`readScalarFromStageOutput` → `fetchStageOutputData` (scalar-subquery
producers) and `readFinalResults` → `fetchResultData` (probe-split merge
partials, oversized final-stage files). Both now go through
`fetchResultDataTiered` (`coordinator/stage_read.go`): **NATS KV → the
producing worker's local copy → S3**, with `fetchStageOutputData`'s bounded
re-poll still behind the durable tier.

The peer tier resolves the producer from the same registry the annotator
uses (`peerFileRegistry.Lookup` → `WorkerRegistry.PeerAddr`) and presents
the query's already-minted fetch token via
`peerFileRegistry.ExistingTokenFor` — which never mints, because a token
the workers never saw buys nothing but a `PermissionDenied`. Kill switch
`WADJET_COORD_PEER_READS=0`; per-read tier and wall land on the
`scalar substitution` log line as `tier=` / `wait_ms=`, and
`Coordinator.StageReadTierCounts()` aggregates them. Full argument
(including why scalar producers still upload eagerly in every durability
mode) in `docs/design/coordinator-stage-reads.md`.

Before this, the coordinator's only read path was S3, and SF100 window 4 §7
measured the cost: 1.5–2.1 s of **whole-cluster idle per steady suite run**
across the three substitution sites (Q11, Q15, Q22), the cluster blocked on
an 80-byte object whose producer had it on local NVMe. Same shape as the
`Operators[].InputFiles` gap above, one consumer further out.

## Outer joins: the rows an empty side still owes

An outer join's defining rows are the ones its data does NOT produce — the
preserved side padded with NULLs — so both halves of that shape are decided by
plan-time declarations and by which side each task owns, not by what arrives.

**Declared side schemas.** `Stage.JoinProbeSchema` / `Stage.JoinBuildSchema`
(`physical.declaredJoinSchema`, derived from the scans' `ScanColumns` +
`ScanColTypes` narrowed to `NeededColumns` + keys) ride to the worker as
`OpSpec.ProbeSchema` / `OpSpec.BuildSchema` and land on
`exec.HashJoin.ProbeSchemaHint` / `BuildSchemaHint`. They are read **only**
when that side delivers no batch at all: a real batch's schema always wins.
Without them `buildSchema` stayed nil and the joined schema carried only the
preserved side — values still read NULL through the projection's missing-name
fallback, but `COUNT(col)` degenerated to `COUNT(*)` and `IS NULL` matched
nothing (#348, same declare-on-the-wire shape as #329's `AggSpec.OutputType`).

**Empty-side short circuits.** Two exist and both had to learn the join type:

- `buildFragmentJoinProbe` (`worker/executor_fragment.go`) replaces the whole
  op chain with a drop-everything filter when `BuildFiles` is empty. Correct
  for inner/semi/right/cross; for LEFT/FULL/ANTI it deleted every preserved
  row (`preservesProbeSide`), so those build an empty hash table instead.
- `executeFragment`'s empty-`InputFiles` return does the same to the PROBE
  side. A RIGHT/FULL join whose probe partition is empty still owes all of its
  build rows — the ordinary shape of one shuffle partition — so it falls
  through on `emptyFragmentSource{}` (`preservesBuildSide`).

**Unmatched build rows.** `HashJoinProbe.FlushUnmatchedRows` emits them once
per join, guarded on the shared `HashJoin` because every clone drains
`FlushableOperator`. Both drivers go through it: `physical.joinFlushSource`
(single process) and the worker's `drainFlushableOps`. The worker had no
equivalent at all before #352, so a distributed RIGHT/FULL join answered with
its matched rows only.

**Why RIGHT/FULL never broadcast.** Any layout that REPLICATES the build side
across tasks is unsound for them: each task holds the whole build and sees one
slice of the probe, so each emits the same unmatched rows (25 → 75 on three
workers). `walkStages` therefore refuses broadcast for those join types,
`fuseStageChains` refuses them as chained consumers, and
`planSkewSplitTasks` already refused to split them. The hash-shuffle layout is
sound: a partition's build and probe rows land on the same task.

**Keyless joins.** `FROM a, b WHERE a.x < b.y` plans as `jt == "cross"` with no
keys. `buildFragmentJoinProbe` demands keys for every other join type and used
to demand them here too, failing the task; the cross-join probe path handles
it, with the inequality riding as the stage's post-filter.

## Shuffle internals

- Stage type `StageExchangeRepartition = "exchange-repartition"` (`planner/physical/exchange.go:19`), `ExchangeStage{Keys, Count}` (`:33`). Sender op `OpExchangeSender{ShuffleKeys, NumPartitions}` (`distributed/messages.go:134,287`).
- `partitionedShuffleSink.Consume` (`worker/partitioned_shuffle_sink.go:103`) resolves `keyIdxs` from key names on the first batch, then `hashRowsIntoPartitions` (`:611`) computes `fnv(col1||col2||…) % numParts`.
- **Type coverage of the hash:** Int32/Port/Protocol/Date, Int64/Timestamp/IPv4/MAC/Duration, Float32, Float64, String/Bytes/IPv6/CIDR/UUID. **NOT covered** (hashed as a constant via the `default` arm): Bool, Decimal, Vector, Array, Row, Map. The planner only ever picks scalar key types for joins, so this is fine there.
- **Collision-safety property:** because the hash is a deterministic function of the row bytes, *identical rows always hash identically* → same partition. Uncovered types therefore cause only **skew**, never incorrect partitioning — which is why an all-columns hash (for a future sharded DISTINCT) is correct even on tables with nested/decimal columns: the final per-partition dedup compares actual values.

## Where dedup / aggregate / distinct happen

- **Aggregate (with functions):** `aggregate` (partial, per scan task) → `final_aggregate` (merge). The distribution pass inserts a shuffle/gather so the final merges all partials. ✅ correct distributed.
- **Sort:** `sort` → merge-sort tree, merged at the gather. ✅
- **Bare GROUP BY (no agg fn):** same stages as aggregates — the fused scan runs the partial dedup and hash-partitions on the group keys; the `final_aggregate` fans out one task per disjoint partition. The dispatch gate that routes a fused scan into `dispatchScanAggregateStage` accepts `FusedAggGroupBy`-only stages (`execute_stage_dag.go`, was `FusedAggSpecs`-only — issue #166). ✅ sharded.
- **DISTINCT, anywhere in the plan:** rewritten at logical-optimize time to an aggregate-free GROUP BY over the projection below it (`logical.rewriteDistinctAsGroupBy`, `planner/logical/distinct_rewrite.go`) — bare columns AND scalar expressions (derived group-bys are evaluated by the worker's `buildAggInputProjection`). Rides the sharded path above; the coordinator does no dedup (`MergeInfo.HasDistinct` is false post-rewrite). ✅ sharded.

  The rewrite walks the WHOLE tree. It used to walk only the root path, and a DISTINCT inside a derived table feeding an aggregate then reached nobody: `walkStages` emits no stage for it, and `ExtractMergeInfo` returns at the first `NodeAggregate` it meets, so the coordinator never saw it either. `SELECT COUNT(*) FROM (SELECT DISTINCT c FROM t) u` answered with the raw count on the DAG and the deduplicated one single-process — silent, deterministic, and unnoticed because the two shapes on either side of it (root `SELECT DISTINCT`, and a derived DISTINCT feeding a plain projection, which `ExtractMergeInfo` does see past one Project) were both correct (#466).

### DISTINCT fallback shapes: what actually happens to each

A DISTINCT is executed by being turned into a GROUP BY. Three outcomes, and no shape is silently dropped:

| Shape | What happens |
|---|---|
| `SELECT DISTINCT a, b + c AS x …` — every projection is a usable group key | **Rewritten in place**, wherever the Distinct sits. Sharded. |
| `SELECT DISTINCT *` / `SELECT DISTINCT t.*` — no `NodeProject` exists at all (a bare-star select list produces none), so the plan is `Distinct → Scan` or `Distinct → Filter → Scan` or `Distinct → Join(Scan, Scan)` | **Rewritten in place** by `rewriteStarDistinct`: the group keys are the relation's own columns, read off `Node.ScanColumns` (the catalog annotation `ExpandStarProjections` uses). Descends only through column-preserving nodes — a Filter, and a join that emits BOTH sides — and declines a semi/anti join, a nested aggregate/projection, an unannotated scan, or a name that appears in two scans (one group key cannot stand for two columns). Sharded. |
| `SELECT DISTINCT a, SUM(b) …` (an aggregate projection has no group key) or a projection carrying a subquery | **Not rewritten.** On the ROOT path `ExecuteSQL` applies `dedupGatherResult` over the projected gather output (`MergeInfo.HasDistinct`) — correct, but single-node at the coordinator. Anywhere else nothing on the DAG would apply it, so `PlanDistributed` returns `ErrDistinctDistributed` (`planner/physical/distinct_refusal.go`) and the coordinator **routes the query to its local single-process pipeline** (`Coordinator.runDistinctLocal`, `coordinator/refused_local.go`) — the #359 pattern, counter `DistinctLocalRoutes()`. |

The refusal is not the answer: it is the handoff. Refusing beat dropping the DISTINCT (#466, the #308 position — a loud failure over a silently different answer), but the query still HAS an answer and one engine in the coordinator process computes it, so an error would be a worse outcome than either. What the refusal buys is that nothing reaches `walkStages` with a semantics-carrying Distinct in it.

Declaring the star's group keys is also what stops the column pruner from eating the dedup. A `NodeDistinct` names no columns, so `computeRequiredColumns` narrowed a star DISTINCT's scan to whatever else the query mentioned: `SELECT COUNT(*) FROM (SELECT DISTINCT * FROM lineitem) u` deduplicated on ONE column (the distinct `l_orderkey` count, 14979 instead of 60175), and over a join it pruned to zero columns and tripped the schemaless-batch guard (#277). Group keys are required columns (#479).
- **`ExtractMergeInfo` walks a Project CHAIN** above the aggregate, not a single node, composing renames innermost-outward. A derived table's SELECT list and the outer query's are separate `NodeProject`s that nothing merges, so `SELECT c FROM (SELECT COUNT(*) AS c FROM t) u` stacks two; stopping at the first made the aggregate invisible and a probe-split merge concatenated the workers' partial groups instead of re-aggregating them.

## Set operations

`planner/physical/set_op_stages.go`. Until #346 `walkStages` walked both arms
of a set operation and emitted nothing else, on the comment *"each side runs
independently; merge results at the end"* — and nothing merged. The terminal
gather attached to whichever arm was emitted last, so a union answered with
ONE arm's raw, unprojected scan: half the rows, and that table's full column
list.

**`UNION ALL` → `StageUnion`** (`exchange.go`), Dependencies = the arms in SQL
order, `Tasks = len(UnionArms)`. Task *i* reads arm *i*'s output **whole** (not
a partition slice) and its fragment is
`[OpShuffleSource, OpProject(arm→result columns), OpFilter?, sink]`
(`coordinator/execute_stage_dag.go buildUnionFragment`). Three things make the
concatenation well-formed:

- **Names.** SQL takes the result columns from the first arm; every arm is
  projected onto them. Without the projection a pass-through parquet scan arm
  reaches the consumer carrying every column of its table.
- **Types.** The arms' outputs are separate `.wshf` files read as one stream, so
  a column declared FLOAT64 by one arm and INT32 by another is a decoding
  error, not a union — it panicked the gather task writing the second arm's
  chunk. `reconcileSetOpArmTypes` widens numerics (INT32 → INT64 → FLOAT64) with
  a `CAST` on the narrower arms and refuses anything else.
- **Output shape.** `dispatchComputeStage` collapses the per-task files into one
  `OutputSinglePart` list (the `probeSplit` / `rrAggGroups` branch) — a
  consumer reading `Files[0]` alone would get one arm.

`Distribution` is `DistRoundRobin` (N outputs, no key clustering); the task
count comes from `UnionArms`, not from the label. `UnionArm.DepStage` must stay
index-aligned with `Dependencies[i]` — `ValidateNativeDAGShape` asserts it, and
`rewireEdges` (shared-subplan dedup) rewrites both together.

**`UNION` (distinct)** appends a `final_aggregate` with `GroupByAll` above the
union. Singleton: one task holds the whole distinct set. That is a scalability
bound, not a correctness one, and the same bound the coordinator's DISTINCT
fallback carries. Sharding it means a hash exchange on all output columns
feeding N per-partition dedups — sound because identical rows hash identically
(see §Shuffle internals, collision-safety).

**`INTERSECT` / `EXCEPT` (and their `ALL` forms)** lower as **grouped
counting** (#346, second half — until then they were refused at plan time):

```
arm 0 stages ─┐
              ├─ union (per-arm projections + TAG columns: arm 0 rows carry
arm 1 stages ─┘         (1,0), arm 1 rows (0,1) — physical.SetOpLeftCountCol
              │         / SetOpRightCountCol, literal Int64 projections)
              ├─ exchange-repartition on the FULL result row   ← inserted by
              │   EnsureDistribution: the union is DistRoundRobin and a
              │   grouped final requires ClusteredOn(GroupByCols)
              └─ final_aggregate {GroupByCols: result row, SUM(tagL),
                  SUM(tagR), RawInputAggregate, SetOp: intersect|except,
                  SetOpAll}
```

Each distinct result row therefore reaches exactly one task carrying
(countA, countB) — its multiplicity in each arm — and the fragment appends an
**`OpSetOpEmit`** (`exec.SetOpEmit`) right after the counting
`OpHashAggregate` that applies the operation's count rule and drops the tags:
INTERSECT 1 copy iff both counts > 0; INTERSECT ALL min(a,b); EXCEPT 1 copy
iff a > 0 && b == 0; EXCEPT ALL max(0, a−b). Distinct forms emit via a
selection vector (zero copy); ALL forms materialize (row replication has no
selection representation). It runs before the stage's post-filter and any
fused sort, both of which name the operation's OUTPUT columns.

Everything else is reuse, which is what makes the shape sound end to end:
co-partitioning is the ordinary exchange (identical rows hash identically —
NULL cells hash a deterministic marker byte, see §Shuffle internals), NULL
membership equality is HashAggregate's NULL-groups-equal rule (#338), spill
is the aggregate's own, and the counting stage is **sharded** — it mirrors
the exchange's partitioning, one task per partition, not a Singleton
(`RawInputAggregate` keeps the dispatcher off the merge-mode spec rewrite and
off `dispatchFinalAggregateFanout`). A sort/LIMIT folded in by
`fuseSortIntoPredecessor` collapses it to Singleton via the same rules every
grouped final follows: correct, serial.

**Refused, loudly** (planning error, never a partial answer):

- An arm whose SELECT list is an **aggregate** (`SELECT COUNT(*) … UNION ALL …`):
  the arm's aggregate stage names its own output, so the union's projection
  would have to guess that name.
- Arms whose column **types** do not widen into one another, or whose **arity**
  differs.

These answer correctly on the single-process path, so they are a two-path
divergence of the "errors on one arm" kind — which is the honest shape.
Returning one arm is the wrong answer that looks like a right one.

Coverage: `physical/set_op_stages_test.go` (plan shape + every refusal),
`exec/set_op_emit_test.go` (the count rules),
`benchmarks/tpch/two_path_invariance_test.go` (`Union*`, `Intersect*`,
`Except*` — row count AND column list asserted absolutely on both arms;
duplicates-within-arm, NULL membership, empty arms, type widening, stacked
ORDER BY/LIMIT/WHERE), `duckdb_compare_test.go` + `pagination_test.go`
(values and sequence against DuckDB).

## Correlated subqueries (refused → routed local)

A subquery whose correlation SURVIVES decorrelation (a non-equi correlation —
equality correlations become joins in the logical optimizer, TPC-H
Q17/Q20/Q22) must re-execute once per outer row. The DAG has no distributed
lowering for that: a worker fragment compiles filters and projections with no
`SubqueryRunner` (`worker/filter_compile.go`), and per-row execution on
workers would need catalog access, outer-scope shipping on the wire, and would
multiply the single-process slow path across the cluster without distributing
anything. Before #359, a correlated EXISTS failed the task loudly while a
correlated SCALAR was **mis-deferred** by `resolveFilterSubqueries` to a
producer stage whose dangling outer reference evaluated NULL — the query
answered **0, silently**, distributed-only.

Mechanism (same refuse-loudly shape as set operations):

- `Planner.refuseCorrelatedSubqueries` (`physical/correlated_refusal.go`) —
  pre-pass over the optimized logical plan, run at the top of
  `PlanDistributed`. Scope per expression is derived exactly as the
  single-process pipeline derives it when it DECIDES correlation
  (`collectTableAliases`/`collectOuterColumns` + the #334 inner-column
  resolver), so the two paths classify identically by construction. Covers
  filter predicates and SELECT-list projections; scalar, EXISTS/NOT EXISTS,
  IN, ANY/ALL; nesting via `FindCorrelatedRefsWithScope`'s recursion.
- Typed error `physical.ErrCorrelatedSubqueryDistributed`; the coordinator
  matches it after `PlanDistributed` and answers on the coordinator-local
  single-process pipeline (`Coordinator.runCorrelatedLocal`,
  `coordinator/correlated_local.go`) — a ROUTE, not a fallback: no DAG plan
  exists, every local failure (including the result budget) is the query's
  outcome. No byte-threshold gate (these plans are unestimable); memory and
  result budgets still bound it. Counter: `CorrelatedLocalRoutes()`. The
  guards live in `Coordinator.runRefusedLocal` (`coordinator/refused_local.go`),
  shared with the #466 DISTINCT route.
- Backstop at the deferral seam: `resolveSubqueryAST` refuses to defer or
  eagerly execute a subquery that is not self-contained
  (`plansql.DanglingTableRefs`), parking `correlatedErr` the way `setOpErr`
  is parked — this is what makes the silent 0 structurally impossible even
  for an expression the pre-pass never visited (e.g. inside a scalar
  producer's own re-walk).

Coverage: `physical/correlated_refusal_test.go` (refusal + the shapes that
must KEEP planning: uncorrelated deferral, #334 inner-scope binding, equality
decorrelation), `coordinator/correlated_subquery_e2e_test.go` (five shapes
end-to-end on a DAG-forced cluster, route counters, over-budget fails loudly),
`benchmarks/tpch/two_path_invariance_test.go` (`Correlated*` entries with
`localRoute: true` — the runner asserts the route engages for exactly those
entries and no others), `duckdb_compare_test.go` (`Correlated*` against DuckDB
ground truth, both arms gated).

The real distributed algorithm for this shape is a dependent join / general
non-equi decorrelation — a separate feature, exactly as #346 grew
INTERSECT/EXCEPT stages after their refusal landed.

## Window

`walkStages` has always emitted a `window` stage, and until #349 nothing
converted it into a fragment: there was no `OpWindow`, no
`buildWindowFragment`, and `dispatchPipelineStage`'s migrations covered joins,
aggregates and sorts only. The task shipped with `Operators == nil` and the
worker rejected it (`executeStage: empty Operators … StageType="window"`), so
**every** window query failed on the DAG, `ROW_NUMBER()` included.
`distributed.Task.WindowCols` was dead wiring — assigned nowhere outside a
round-trip test — and has been deleted; the spec rides
`OpSpec.WindowCols` like every other fragment operator's configuration.

**Fragment shape** (`coordinator/execute_stage_dag.go buildWindowFragment`):

```
[OpShuffleSource, OpWindow, OpFilter?(predicate above the window), <sink>]
```

There is deliberately **no `OpSort`** ahead of the window. A window's ORDER BY
defines its FRAME, not the stream: `exec.Window` groups its input by
(PARTITION BY, ORDER BY) and sorts each group itself, which is also why one
stage can carry two OVER clauses that order differently — a single pre-sort
could not serve both.

**Distribution** (`physical/distribution.go`, `windowPartitionKeys`):

- A window over `PARTITION BY k` is computable one partition at a time, so
  the stage requires `ClusteredOn(k)` and — when its input actually arrives
  `DistHashPartitioned` on exactly `k` — **mirrors that distribution**: one
  task per partition, each running the whole operator over its slice. This is
  the shape that scales, and it engages on real plans: a window over a
  shuffled hash join partitioned on the join key inherits the join's
  partitioning, and a window partitioned on some other column gets an
  `exchange-repartition` spliced in by `EnsureDistribution`
  (`coordinator/window_distributed_test.go` runs one over a 3-worker cluster
  and asserts each partition is numbered 1..n).
- Everything else is **Singleton**: no `PARTITION BY` (a global window's
  universe is every row), or window columns that disagree on their partition
  keys (one exchange cannot cluster both, and clustering on the first spec's
  keys would compute the second over a fragment of its partition — a wrong
  answer, not a slow one). A Singleton stage's single task reads *every*
  partition of its input (`partitionFilesForWorker` with `workerCount == 1`),
  so it is a scalability bound only. **A window over a leaf scan is always
  this case** — a scan is Singleton, which already satisfies clustered-on —
  so today's common shape is one task holding all the rows.

**Spec resolution.** `physical.WindowColSpec` is filled by `windowExecColumn`,
the same helper `buildWindow` compiles the single-process operator from: the
column out of the argument list, the offset/default/N that share it, the
frame, and `OutputType`. The worker has no catalog and no logical plan, so
anything left unresolved would be a second implementation there — and a
window value function's output type IS its input column's type (#345), which
nothing downstream of the operator corrects. A nil `OutputType` means "not
declared" (older coordinator, or a type the planner declined) and the worker
keeps the conservative float64, which `exec.Window.retypeValueColumns` still
fixes for the five value functions. It is a **pointer**, unlike
`AggSpec.OutputType`, because `parquet.TypeID`'s zero value is BOOL — an int
field cannot tell `LAG(bool_col)` from a spec that declares nothing.

**Not covered:** frames are carried end to end but `exec.Window` never reads
`WindowColumn.Frame` at all (#350) — an operator defect both paths share.

## Query cancellation (and `statement_timeout`)

Client cancellation and the query timeout are one mechanism: a cancelled
statement context, carried from the wire down to the running stage tasks.

1. **Key material.** pgwire gives each session a random `(pid, secret)` pair in
   BackendKeyData and registers it in `Server.sessions`
   (`server/pgwire/cancel.go`). A CancelRequest (protocol version `80877102`)
   arrives on its own connection — the session's own connection is blocked
   reading results — and `cancelSession` verifies the secret with
   `crypto/subtle` before cancelling. The cancel connection gets no reply and
   is closed, as PostgreSQL does.
2. **Statement context.** `queryContext()` → `beginStatement`
   (`server/pgwire/server.go`, `cancel.go`) builds `WithCancelCause` (plus
   `WithTimeoutCause` when `statement_timeout` / `--query-timeout` applies) and
   registers it as `pgConn.stmt`; the deferred CancelFunc unregisters it, so a
   cancel between statements or after completion is a no-op. The cause tells
   the two conditions apart and both report SQLSTATE **57014**
   (`query_canceled`).
3. **Coordinator.** That context is what `ExecuteSQL` / `executeStageDAG` run
   under — every stage wait, dispatch and gather selects on it, so the DAG
   unwinds in milliseconds — and the deferred `cleanupQuery` broadcasts
   `wadjet.cancel.<root>` (`coordinator.go`).
4. **Workers.** The broadcast root id lands in `w.cancelled`; queued tasks Term
   at pickup and running tasks abort on a 500 ms poll. The match is
   `taskCancelled(task.QueryID, rootQueryID)` (`worker/worker.go`), which tests
   BOTH the task's stage-scoped QueryID (`st-<stage>-<root>`) and the root
   recovered from its `queries/<root>/…` scratch paths
   (`distributed.TaskRootQueryID`). Testing only the former never matched a DAG
   stage task, so cancellation and timeout used to free the client while the
   cluster kept executing. Regression tests:
   `worker/cancel_root_query_test.go` (the id match) and
   `coordinator/cancel_distributed_test.go` (end-to-end: a parked worker scan
   must return while its gate is still shut).

Seams this does **not** close:

- **JetStream dispatch drops the deadline.** `scheduler.PublishTasks` computes
  `deadlineNano` from `ctx.Deadline()`, but only the gRPC data-plane path
  carries it into `worker.taskContext`; on the NATS path the task runs with no
  deadline of its own. Timeout still stops the work through the cancel
  broadcast above — the deadline is not a second, independent bound there.
- **`exec.Pipeline.runParallel` swallows cancellation**
  (`engine/exec/pipeline.go`): its workers exit on `workerCtx.Err()` without
  setting `firstErr`, so a cancelled parallel pipeline can return a *partial*
  result with a nil error. pgwire therefore re-checks the statement context
  after a "successful" execution and reports 57014 instead of sending a
  silently truncated result.
- **`Coordinator.CancelQuery` cannot stop a synchronous `ExecuteSQL`** — it
  marks the tracker and broadcasts, but no cancel func is registered for the
  DAG path (`resultSubs` is populated only by the async `SubmitSQL` path).

## Known Issues

### DISTINCT over a non-decorrelatable scalar-subquery projection (fallback, distributed)

The coordinator-dedup fallback dedups the *gather output*, and the gather
cannot compute expression projections — so a `SELECT DISTINCT (<subquery>) …`
whose subquery survives decorrelation dedups over raw upstream columns
(over-distinguishes). Decorrelated subqueries become column refs and take the
sharded rewrite, so this is a narrow residual shape.

### History

**Fixed 2026-07 (#169):** a bare expression SELECT over a scan returned raw
scan columns distributed — no compute stage existed and the gather can only
rename/drop. Now `attachScanSelectProjections` (plan.go) sets
`Stage.ProjectExprs` on a leaf scan feeding the gather directly, the scan
dispatches through the fragment path, and an `OpProject` op computes the
SELECT list worker-side (plan-time type inference via `inferProjectionType`
rides along — the output column doesn't exist in the input schema, so the
worker can't resolve its type). Found by the fast-path differential test.

**Fixed 2026-07:** distributed `SELECT DISTINCT` and aggregate-free `GROUP BY`
returned every input row (no cross-task dedup) — #163 (first-cut coordinator
dedup, PR #165) then #166 + the sharded rewrite (partial-dedup at scan →
hash-partition exchange → per-partition final tasks). Regression coverage:
`coordinator/distinct_distributed_test.go` (multi-worker e2e, 9 query shapes).

**Fixed 2026-08 (#466):** the same class one level down. The 2026-07 rewrite
only covered the ROOT DISTINCT, so a DISTINCT inside a derived table feeding
an aggregate still returned every input row on the DAG — `SELECT COUNT(*) FROM
(SELECT DISTINCT c FROM t) u` answered 100 where PostgreSQL and the
single-process path answer 25. The rewrite now walks the whole tree, marked
`BuildSideDedup` Distincts are excluded by an explicit flag rather than by
position, a star DISTINCT (which has no `NodeProject` to read) takes its group
keys from the scan's catalog annotation, and anything the rewrite still
declines off the root path is refused and routed to the coordinator-local
pipeline instead of dropped. Regression coverage:
`planner/logical/distinct_rewrite_test.go` (rewrite scope, star keys, marker),
`planner/physical/derived_distinct_test.go` (the emitted dedup stage and the
refusal), the `DerivedDistinct*` / `DerivedStarDistinct*` /
`GroupByOverDerivedDistinct` entries in the two-path invariance corpus, and the
matching PostgreSQL-oracle entries that settle what the answers ARE.

Two things the review of that fix turned up and this document now states above:
a star DISTINCT used to be REFUSED where the pre-#466 DAG answered correctly,
and the group-key test was gated behind a text match for the word `select`
that fired on string literals. Three defects it found that are NOT this
family are tracked separately: #478 (a derived-table LIMIT lands on no stage),
#479 (the pruned-column-set dedup, fixed here), and #480 (two loud DAG
failures on a derived table feeding a join, both with explicit-`GROUP BY`
twins).

## How to inspect this machinery (recipes)

**Dump the stages a query plans to** — fastest way to see what the DAG looks
like. Pattern (see `planner/physical/dynamic_filter_test.go` `sqlToStages` /
`plan_tpch_test.go:setupTPCHCatalog`):

```go
cat, ctx := setupTPCHCatalog(t)
for _, s := range sqlToStages(t, cat, ctx, "SELECT DISTINCT l_orderkey FROM lineitem", 3) {
    fmt.Printf("id=%s type=%s tasks=%d dist=%v groupBy=%v aggs=%d deps=%v\n",
        s.ID, s.Type, s.Tasks, s.Distribution, s.GroupByCols, len(s.AggSpecs), s.Dependencies)
}
```

**End-to-end multi-worker run** (real NATS + 3 in-process workers + coordinator)
— the pattern in `coordinator/multi_worker_test.go:TestShuffleCorrectness`:
embedded NATS → MemStore + NATS-KV catalog → write parquet + `cat.AddFiles` →
`worker.New(...).Start` ×N → `New(coordinator.Config{...})` → inject fake
heartbeats → `coord.ExecuteSQL(ctx, sql)`. Use this to verify row-count
correctness of a distributed change before EC2.

## Related

- `docs/architecture.md` §Distributed Execution (high level), `docs/distributed.md` (deployment).
- Memory: `project-distributed-distinct-design-2026-06-29` (the sharded-distinct fix design + this investigation).
