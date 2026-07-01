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
`MergeInfo.HasDistinct`; the native-DAG path does **not** (see Known Issues).

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
  construction. Any local plan/execute error falls back to the DAG.
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
| `NodeJoin` | `hash_join`/`broadcast_join` (+ `exchange-repartition` shuffles for non-broadcast) | keys parsed at plan time (`plan.go:3145+`) |
| `NodeAggregate` | `aggregate` (partial) → `final_aggregate` | two-phase (`plan.go:3034-3103`); the distribution pass adds the exchange |
| `NodeSort` | `sort` → merge-sort tree | `plan.go:3105` |
| `NodeWindow` | `window` | `plan.go:3384` |
| `NodeFilter` | pushes predicates onto child stage | `plan.go:3323` |
| **`NodeDistinct`** | **nothing — passthrough** | `default` case `plan.go:~3415`; walks children only. Top-level DISTINCT never reaches here — `logical.rewriteDistinctAsGroupBy` (optimizer) turns it into an aggregate-free `NodeAggregate` first, so it rides the aggregate stages. Remaining NodeDistincts (semi/anti build-side dedup, fallback shapes) stay passthrough. |
| **`NodeProject`** | **nothing — passthrough** | same `default` case; aliases recovered at gather |

## Stage → worker fragment conversion

Every stage dispatches as a **fragment**: a list of `distributed.OpSpec`
operators (`distributed/messages.go:260`). The worker requires `Operators` to be
non-empty (`worker/executor_stage.go:22`).

Conversion lives in `coordinator/execute_stage_dag.go`:
- `buildAggregateFragment` (`:2373`) → `OpShuffleSource` + `OpHashAggregate{GroupByCols, Aggregates, MergeMode}`. **`MergeMode = stage.Type=="final_aggregate"||"merge_aggregate"`** (`:2400`) — merge mode rewrites `InputCol→OutputCol` and `COUNT→SUM`.
- `buildSortFragment` (`:2514`), shuffle dispatch `dispatchShuffleStage` (`:772`).
- Terminal stage gets an `OpGatherSink` when `gatherReplySubject != ""`.

Worker side: `buildFragmentUnary` (`worker/executor_fragment.go:952`) and
`buildFragmentHashAggregate` (`:788`). **Gap:** the hash-aggregate fragment
requires `len(GroupByCols)>0 || len(Aggregates)>0` and has **no `GroupByAll`**
— the single-process `buildDistinct` (`plan.go:5583`) uses
`HashAggregate.GroupByAll=true`, which has no distributed equivalent yet.

## Shuffle internals

- Stage type `StageExchangeRepartition = "exchange-repartition"` (`planner/physical/exchange.go:19`), `ExchangeStage{Keys, Count}` (`:33`). Sender op `OpExchangeSender{ShuffleKeys, NumPartitions}` (`distributed/messages.go:134,287`).
- `partitionedShuffleSink.Consume` (`worker/partitioned_shuffle_sink.go:103`) resolves `keyIdxs` from key names on the first batch, then `hashRowsIntoPartitions` (`:611`) computes `fnv(col1||col2||…) % numParts`.
- **Type coverage of the hash:** Int32/Port/Protocol/Date, Int64/Timestamp/IPv4/MAC/Duration, Float32, Float64, String/Bytes/IPv6/CIDR/UUID. **NOT covered** (hashed as a constant via the `default` arm): Bool, Decimal, Vector, Array, Row, Map. The planner only ever picks scalar key types for joins, so this is fine there.
- **Collision-safety property:** because the hash is a deterministic function of the row bytes, *identical rows always hash identically* → same partition. Uncovered types therefore cause only **skew**, never incorrect partitioning — which is why an all-columns hash (for a future sharded DISTINCT) is correct even on tables with nested/decimal columns: the final per-partition dedup compares actual values.

## Where dedup / aggregate / distinct happen

- **Aggregate (with functions):** `aggregate` (partial, per scan task) → `final_aggregate` (merge). The distribution pass inserts a shuffle/gather so the final merges all partials. ✅ correct distributed.
- **Sort:** `sort` → merge-sort tree, merged at the gather. ✅
- **Bare GROUP BY (no agg fn):** same stages as aggregates — the fused scan runs the partial dedup and hash-partitions on the group keys; the `final_aggregate` fans out one task per disjoint partition. The dispatch gate that routes a fused scan into `dispatchScanAggregateStage` accepts `FusedAggGroupBy`-only stages (`execute_stage_dag.go`, was `FusedAggSpecs`-only — issue #166). ✅ sharded.
- **Top-level DISTINCT:** rewritten at logical-optimize time to an aggregate-free GROUP BY over the SELECT list (`logical.rewriteDistinctAsGroupBy`, `planner/logical/distinct_rewrite.go`) — bare columns AND scalar expressions (derived group-bys are evaluated by the worker's `buildAggInputProjection`). Rides the sharded path above; the coordinator does no dedup (`MergeInfo.HasDistinct` is false post-rewrite). ✅ sharded.
- **DISTINCT fallback shapes** (`SELECT DISTINCT *`, DISTINCT with aggregate projections, subquery expressions): not rewritten; `ExecuteSQL` applies `dedupGatherResult` over the projected gather output (`MergeInfo.HasDistinct`). Correct but single-node at the coordinator.

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
