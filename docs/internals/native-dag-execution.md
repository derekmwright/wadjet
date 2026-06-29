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
| **`NodeDistinct`** | **nothing — passthrough** | `default` case `plan.go:~3415`; walks children only |
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
- **DISTINCT / bare GROUP BY (no agg fn):** `NodeDistinct` is passthrough and a no-aggregate `NodeAggregate` produces no merge — so the native-DAG emits `scan → gather` with **no dedup operator**. ❌ See Known Issues.

## Known Issues

### Distributed DISTINCT and aggregate-free GROUP BY are not deduplicated (native-DAG)

`SELECT DISTINCT …` and `SELECT col … GROUP BY col` (no aggregate function)
return **every input row** in distributed mode — no cross-task dedup. Confirmed
2026-06-29 by stage dump (`scan → exchange-gather`, no dedup stage) and an
end-to-end multi-worker run (`SELECT DISTINCT l_returnflag` → 60000 rows vs 3).
The probe-split path dedups via `MergeInfo.HasDistinct`; the native-DAG path
(every production entry point) does not. Masked because: standalone/single-
process uses `buildDistinct` (works), and no test/TPC-H query exercises bare
distinct or no-agg group-by distributed. Likely a native-DAG-unification
regression. **Fix:** make `NodeDistinct` (and no-agg `NodeAggregate`) emit the
two-phase dedup stages that the working aggregate path uses
(`GroupByAll` partial → exchange(all-cols) → `GroupByAll` final). Tracked in the
distributed-distinct workstream.

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
