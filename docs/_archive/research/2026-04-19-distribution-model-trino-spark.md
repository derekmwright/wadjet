> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Distribution Model Research: Trino, Spark, DuckDB

**Date:** 2026-04-19
**Goal:** Inform a redesign of Wadjet's distributed routing layer. The
current coordinator switch (`shuffle-distributed` / `probe-split + has_merge`
/ `single-worker`) creates cascading interactions whenever a mode or gate
is added. Hypothesis: this is an architectural mismatch with how mature
engines work — they treat distribution as a property of every operator,
with explicit Exchange insertion driven by satisfaction checks, not as a
top-level routing decision. This brief synthesises Trino, Spark, and DuckDB
from primary sources and proposes what Wadjet should adopt.

---

## Section 1 — Distribution as operator property (the core abstraction)

### Trino

Every plan node carries derivable partitioning properties about the data it
produces. The optimiser propagates them upward and inserts an `ExchangeNode`
whenever a parent operator's required partitioning is not satisfied by the
child's actual partitioning.

`io.trino.sql.planner.plan.ExchangeNode` (`core/trino-main/src/main/java/io/trino/sql/planner/plan/ExchangeNode.java`) is the unit of redistribution. Its constructor:

```java
public ExchangeNode(
    PlanNodeId id,
    Type type,                       // GATHER | REPARTITION | REPLICATE
    Scope scope,                     // LOCAL | REMOTE
    PartitioningScheme partitioningScheme,
    List<PlanNode> sources,
    List<List<Symbol>> inputs,
    Optional<OrderingScheme> orderingScheme)
```

`Type` is `GATHER` (many→one), `REPARTITION` (many→many by hash/round-robin),
or `REPLICATE` (one→many broadcast). `Scope` is `LOCAL` (intra-process between
pipelines on a worker) or `REMOTE` (network). Same node type for both; only
Scope changes.

The detail of *how* data is repartitioned lives in `PartitioningScheme`
(`PartitioningScheme.java`): `partitioning` (a `Partitioning` containing a
`PartitioningHandle` plus argument expressions), `outputLayout`,
`replicateNullsAndAny`, `bucketCount`, `bucketToPartition`, `partitionCount`.

`PartitioningHandle` (`PartitioningHandle.java`) wraps a connector handle and
a system handle. `SystemPartitioningHandle.SystemPartitioning` enumerates the
canonical kinds: `SINGLE`, `FIXED` (fixed bucket count, hash or round-robin),
`SOURCE` (whatever the connector produces), `COORDINATOR_ONLY`, `ARBITRARY`.

The decisive observation: partitioning kind is *not* an enum on a routing
decision. It is a property attached to every plan node, derived bottom-up,
required top-down. The optimiser reconciles the two by inserting
`ExchangeNode`s.

### Spark Catalyst

Spark uses the same abstraction with cleaner names. From
`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala`:

```scala
sealed trait Distribution {
  def requiredNumPartitions: Option[Int]
  def createPartitioning(numPartitions: Int): Partitioning
}
```

`Distribution` describes what an operator **requires**. Subclasses:
`UnspecifiedDistribution`, `AllTuples` (single partition),
`ClusteredDistribution(clustering, requireAllClusterKeys)`,
`OrderedDistribution`, `BroadcastDistribution(mode)`,
`StatefulOpClusteredDistribution`.

```scala
trait Partitioning {
  val numPartitions: Int
  final def satisfies(required: Distribution): Boolean
  protected def satisfies0(required: Distribution): Boolean
}
```

`Partitioning` describes what an operator **produces**. Subclasses:
`UnknownPartitioning`, `RoundRobinPartitioning`,
`HashPartitioning(expressions, numPartitions)`, `KeyedPartitioning`,
`RangePartitioning`, `BroadcastPartitioning(mode)`, `PartitioningCollection`
(joins whose output satisfies multiple distributions).

The pivotal method is `Partitioning.satisfies(Distribution)`:

```scala
final def satisfies(required: Distribution): Boolean = {
  required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)
}
```

Each `Partitioning` overrides `satisfies0` for its match logic (e.g.
`HashPartitioning(a, b, 200)` satisfies `ClusteredDistribution(a, b)` but
not `ClusteredDistribution(c)`).

The load-bearing primitive across both systems: **child produces
Partitioning P; parent requires Distribution D; insert exchange iff
`!P.satisfies(D)`**.

### DuckDB

Single-process, no inter-node exchange. Per the 2022 aggregate-hashtable
post, parallelism uses radix-partitioned hash tables: each thread builds
local partitioned tables on `radix(hash(group))`, then a parallel pass
merges per-partition. Sidesteps the exchange operator — partitioning is a
runtime property of the in-thread hash table, not a plan node. Useful
contrast: when you do not need to model network boundaries, you do not need
an Exchange operator. Wadjet does.

### What Wadjet should adopt

Across Trino and Spark, one consistent pattern with three pieces:

1. **Required distribution** — a property a parent demands from each child
   input ("co-partitioned on join keys", "broadcast", "single partition").
2. **Actual partitioning** — a property each operator declares about its
   output.
3. **Satisfaction + Exchange insertion** — a single mechanical pass that
   asks `actual.satisfies(required)` and inserts an `Exchange` (Spark's
   `ShuffleExchangeExec` / `BroadcastExchangeExec`; Trino's `ExchangeNode`
   typed `REPARTITION` / `REPLICATE` / `GATHER`) when it does not.

This eliminates the routing switch by construction. Whether to broadcast,
shuffle, or pre-aggregate becomes *which required distribution does the join
declare*, not *which top-level mode does the coordinator pick*.

---

## Section 2 — Exchange insertion mechanics

### Trino's `AddExchanges`

`io.trino.sql.planner.optimizations.AddExchanges` is a `PlanOptimizer` whose
visitors return `PlanWithProperties` (each plan node paired with the actual
properties it produces). The walk derives properties bottom-up via
`PropertyDerivations.deriveProperties()` and compares against parent
preferences via `computePreference()`.

The join visitor (`visitJoin()`):

- If the probe side is naturally partitioned on the join symbols, plan a
  partitioned join; add a `partitionedExchange()` only on the build side
  (or neither, if `isCompatibleTablePartitioningWith()` is true).
- Otherwise fall back to a replicated (broadcast) join.
- `useParentPreferredPartitioning()` is the cost-aware tiebreaker — when
  the parent's preferred partitioning has too few distinct values to
  provide parallelism, fall back to partitioning by all grouping keys.

For multi-consumer outputs Trino reuses the same `ExchangeNode` — being a
node in the plan DAG, downstream operators sharing a distribution
requirement read from the same exchange's output.

### Spark's `EnsureRequirements`

`sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala`
runs after physical planning. Its docstring:

> "Ensures that the Partitioning of input data meets the Distribution
> requirements for each operator by inserting ShuffleExchangeExec Operators
> where required. Also ensure that the input partition ordering
> requirements are met."

`ensureDistributionAndOrdering` walks the plan, examines each operator's
`requiredChildDistribution` / `requiredChildOrdering`, and for each child
checks whether `child.outputPartitioning.satisfies(required)`. If not:

```scala
ShuffleExchangeExec(
  distribution.createPartitioning(numPartitions),
  child,
  shuffleOrigin)
```

For broadcast distributions: `BroadcastExchangeExec(mode, child)`. A
separate pass (`reorderJoinKeys`) recursively reorders join keys to align
with existing child partitioning, avoiding shuffles when reordering is
sufficient.

### Multi-consumer handling and driver

Both systems rely on plan-DAG structure: once an `Exchange` is inserted,
every operator above it consumes the single output; the optimizer does not
re-shuffle the same data twice. Spark AQE additionally reuses *executed*
shuffle stages via `ReuseExchange`.

| | Trino | Spark |
|---|---|---|
| When | Mid-optimisation, top-down property propagation | Post-physical-planning, single bottom-up pass |
| Decision input | `ActualProperties` + cost stats + `taskCountEstimator` | `Partitioning.satisfies(Distribution)` + (later) AQE runtime stats |
| Cost-aware? | Yes (broadcast threshold, NDV-based partitioning preference) | Static rule + post-shuffle AQE re-plan |

Same shape: **child says what it produces; parent says what it needs;
exchange fills the gap.**

---

## Section 3 — Cost model

### Trino (static CBO)

Trino's CBO consumes `TableStatistics` (row count, per-column NDV, null
fraction, avg row size, data size) supplied by connectors. Relevant
decisions:

- **Broadcast vs partitioned join** — driven by session property
  `join_distribution_type` (`AUTOMATIC` consults stats; build must fit
  under `join_max_broadcast_table_size`).
- **Partitioning preference inheritance** —
  `useParentPreferredPartitioning()` uses
  `taskCountEstimator.estimateHashedTaskCount()` against
  `PREFER_PARENT_PARTITIONING_MIN_PARTITIONS_PER_DRIVER_MULTIPLIER = 128`
  to decide if adopting parent partitioning would starve parallelism.
- **Bucket count** — configured task concurrency, not stats.

Missing stats → broadcast joins below the size limit, partitioned otherwise.
No skew handling, no re-shuffle.

### Spark (static + AQE)

Static CBO uses `Statistics(sizeInBytes, rowCount, attributeStats)` from
`ANALYZE TABLE`. The static broadcast decision uses
`spark.sql.autoBroadcastJoinThreshold` (default 10 MB) against the build's
estimated size.

**AQE** is the breakthrough. AQE splits the physical plan into query stages
at materialisation points (shuffle and broadcast boundaries). After each
stage materialises, the framework retrieves runtime stats and re-runs the
optimizer + physical planner on the unexecuted remainder. Three features:

1. **Coalesce shuffle partitions** to a target size
   (`spark.sql.adaptive.advisoryPartitionSizeInBytes`, default 64 MB).
   Triggered after every shuffle. Stats: per-partition output size.
2. **Switch sort-merge join to broadcast hash join** if a join side's
   actual shuffle output is below
   `spark.sql.adaptive.autoBroadcastJoinThreshold`.
3. **Skew join optimisation** — split partitions where
   `size > median × 5.0 AND size > 256 MB`.

The asymmetry between Trino and Spark is not the cost model — both have one
— but *when the decision locks in*. Trino commits at planning time (fast
startup, brittle under bad stats); Spark commits at stage boundaries (extra
round-trips, robust under bad stats).

---

## Section 4 — Wadjet's specific gap

### What Wadjet has today

`internal/planner/physical/distribution.go` already contains the right
nucleus: `DistKind` (`DistSingleton`, `DistBroadcast`, `DistHashPartitioned`),
a `Distribution` struct with `Kind` / `Keys` / `Count`, and methods
`Equals` and `SatisfiesJoinKeys`. `Stage.Distribution Distribution` is
already a field on `Stage` (`internal/planner/physical/plan.go:120`).
Comments document that shuffle stages set `DistHashPartitioned`, broadcast
pre-scans set `DistBroadcast`. This is a beachhead.

What is missing: nothing requires it. The coordinator never asks "does this
stage's distribution satisfy the next stage's requirement?" — it does the
satisfaction check itself by inspecting stage shape, in the switch in
`internal/coordinator/coordinator.go:541-605`:

```go
switch {
case shuffleApplicable && mergeInfo != nil: // shuffle-distributed
case canProbeSplit && mergeInfo != nil:     // probe-split + optional pre-computed-aggregates
default:                                    // single worker
}
```

This switch *implements* exchange insertion in ad-hoc form. Each case
mutates `physStages` to a hardcoded shape (e.g. a single `pipeline-0` stage
with embedded `BuildCachePreScans`) and delegates to a hand-rolled
orchestrator (`orchestrateShuffleStages`, `preScanBuildTables`,
`preComputeDerivedAggregate`). The four decision helpers — `CanProbeSplit`,
`PickShuffleCandidate`, `PickAggregateShuffleCandidate`, the fused-join
logic — each independently inspect stage shape and return a routing hint.
There is no single satisfaction view.

### What Wadjet needs to add

**Types (Phase 1).** Promote `Distribution` to a *required* / *produced*
pair, mirroring Spark. Add `RequiredDistribution` (what a parent demands)
distinct from `Distribution` (what a stage produces). Add an Exchange stage
type with `Type` ∈ {`Gather`, `Repartition`, `Replicate`} — generalise the
existing `shuffle` Stage type and let the Type field discriminate;
`ShuffleKeys` becomes the Repartition variant's payload. Add
`RequiredChildDistribution() []RequiredDistribution` to existing stages —
what a hash join requires from its build (broadcast OR hash-partitioned on
build keys), what an aggregate requires from its child (hash-partitioned on
group-by keys for partial-then-merge, single for finalised), etc.

**Optimizer rule (Phase 2).** A new `EnsureDistribution` pass over the
stage DAG, after physical planning. For each stage, for each dependency, if
`dep.Distribution` does not satisfy the stage's required input distribution
for that slot, insert an Exchange between them with the required
partitioning. This pass *replaces* `CanProbeSplit`, `PickShuffleCandidate`,
`PickAggregateShuffleCandidate`, and the coordinator switch. Probe-split
becomes "partition the largest scan's output" expressed as
`scan.Distribution = HashPartitioned(file_id)` and the join's build
required as `Broadcast` (or `HashPartitioned(build_keys)`). The
pre-computed aggregate optimisation becomes a logical rewrite rule — "if a
derived aggregate subtree appears under multiple probe-split tasks,
materialise it once" — independent of the exchange decision.

**Executor (Phase 2 follow-up).** The coordinator becomes a stage-DAG
executor walking stages in dependency order. For each Exchange stage, it
dispatches the relevant shuffle / broadcast / gather (existing
`orchestrateShuffleStages` and `preScanBuildTables` are the
implementations). The switch goes away entirely.

### What Wadjet should delete

- The `switch { case shuffleApplicable: ... }` block in the coordinator.
- `CanProbeSplit`, `PickShuffleCandidate`,
  `PickAggregateShuffleCandidate` as routing functions. Their *detection*
  logic survives as inputs to the optimizer; their *routing* logic does
  not.
- `MergeInfo` / `ExtractMergeInfo` as a logical-plan side-channel. "Do we
  need a merge" becomes "is the final stage's distribution Singleton? if
  not, insert a Gather" — falls out of `EnsureDistribution` for free.
- The `has_merge` boolean threading through coordinator code. After the
  redesign, just a stage DAG terminating in a Gather.

---

## Section 5 — Migration sketch

**Q21 multi-build shuffle.** Q21 has multiple joins where each build, in
isolation, deserves shuffle. Today the coordinator picks one shuffle
candidate via `PickShuffleCandidate` and broadcasts the rest. Under the new
model, each join independently declares its required input distribution;
`EnsureDistribution` inserts Exchanges per join. If the same table appears
as a build under two joins, plan-DAG sharing ensures one shuffle, not two.

**Q17 pre-computed aggregate.** Today `PickAggregateShuffleCandidate` runs
*after* `PickShuffleCandidate` and *suppresses* it (`shuffleApplicable = false`
at coordinator.go:537-539). The cascade problem in microcosm. Under the new
model: the aggregate subplan's output `Distribution` is computed bottom-up
(`HashPartitioned(group_by_keys, N)` after partial; `Singleton` after
merge); the downstream join declares its required build distribution; if
the aggregate already satisfies the requirement (group-by keys ⊇ join
keys, partition counts match) no exchange is inserted — it is co-located.
Otherwise an exchange repartitions. The "compute once vs per-task" decision
becomes a separate logical rewrite creating a `MaterializedAggregate` when
its output would be consumed by multiple probe-split tasks.

**TPC-H routing preserved.** A query that today routes to `single-worker`
produces a stage DAG whose outputs are all `Singleton` — `EnsureDistribution`
inserts no Exchanges. A `probe-split` query has its largest scan produce
`HashPartitioned(file_id, N)`, requiring a Gather above the final aggregate
— same execution, derived rather than declared. A `shuffle-distributed`
query declares its build's output as `HashPartitioned(build_keys, N)` and
its probe likewise, requiring no exchange between them — same execution,
no centralised pick.

---

## Section 6 — Implementation effort estimate

**Phase 1 — Add types and properties (no behaviour change). ~2 weeks.**
Extend `Distribution` / `RequiredDistribution` with broadcast and gather
semantics, partition counts, and `Satisfies(RequiredDistribution) bool`
(Spark's `satisfies0`). Annotate each `Stage.Type` with
`OutputDistribution(stage)` and `RequiredChildDistribution(stage)` helpers.
Unit tests on satisfaction semantics. No optimizer or executor changes —
get the property algebra right in isolation.

**Phase 2 — Insert exchanges, retire the switch. ~3 weeks.** Implement
`EnsureDistribution` over the stage DAG. Existing shuffle stages re-typed
as `Exchange{Type: Repartition}`; build-cache pre-scans as
`Exchange{Type: Replicate}`; the implicit final gather as
`Exchange{Type: Gather}`. Replace the coordinator switch with a stage-DAG
executor dispatching per stage type. The orchestrator implementations
(`orchestrateShuffleStages`, `preScanBuildTables`,
`preComputeDerivedAggregate`) become per-Exchange-type backends. Delete
`CanProbeSplit`, `PickShuffleCandidate`, `PickAggregateShuffleCandidate`,
`MergeInfo`. Run TPC-H at SF1 and SF10 for parity.

**Phase 3 — Cost model + stats integration. ~2 weeks.** Wire
`Stage.EstimatedBytes` / `EstimatedRows` (already populated) into a
cost-aware `EnsureDistribution` that picks broadcast vs hash partitioned
per join based on build size and partition count. Replicate Trino's
`useParentPreferredPartitioning` heuristic so a child with low NDV does
not adopt parent partitioning that would starve parallelism. This is where
`shuffleBuildThreshold` and `aggregateShuffleThreshold` get a principled
home.

**Phase 4 — AQE-style adaptive re-planning. Later. ~4-6 weeks.** Split the
stage DAG into query stages at Exchange boundaries. After each Exchange
materialises, collect partition-size stats, re-run `EnsureDistribution` on
the unexecuted remainder, dispatch the next query stage. Highest-ROI item
but largest lift — defer until Phase 3 stabilises.

---

## Recommendation

Adopt Spark's `Distribution` / `Partitioning` / `satisfies` triad as the
load-bearing primitive, with Trino's `ExchangeNode` (`Type` × `Scope`) as
the in-plan operator:

1. Distribution as a property of every stage's output, not a coordinator
   routing decision.
2. Each operator's input requirement as a `RequiredDistribution`, not a
   switch case.
3. Exchange insertion as a single mechanical pass:
   `actual.satisfies(required)` ? skip : insert.
4. Preserve current execution behaviour by translating the four-mode switch
   into the equivalent `Distribution` / `RequiredDistribution` annotations
   — no runtime change in Phase 2, just a structural refactor.
5. Cost-based decisions in Phase 3 once the structure is in place.
6. Defer AQE-style runtime re-planning to Phase 4.

**Why this fixes the cascade.** Today, adding a fourth mode requires
revisiting every other mode — each mode is a global routing decision and
every interaction is hand-coded. Under the property-based model, each
operator declares only what it produces and what it needs. Adding a new
partitioning kind (e.g. range for sort) is one new `Partitioning` type plus
a `satisfies0` override; the exchange insertion pass is untouched. A new
execution strategy means changing only that operator's
`requiredChildDistribution`; the coordinator is untouched. The cascade is
structurally impossible because no single node knows about all modes.

---

## File references

**Wadjet (current):** `internal/planner/physical/distribution.go` (existing
nucleus, 58 lines); `internal/planner/physical/plan.go:35-121` (`Stage` with
`Distribution` field); `plan.go:1044-1085` (`CanProbeSplit`);
`plan.go:1099-1290` (`PickShuffleCandidate`);
`internal/planner/physical/aggregate_shuffle.go`
(`PickAggregateShuffleCandidate` + rejection taxonomy);
`internal/coordinator/coordinator.go:472-605` (the four-mode switch this
brief proposes to delete); `internal/coordinator/shuffle_orchestrator.go`
(390 lines, the existing repartition backend);
`internal/coordinator/aggregate_shuffle.go` (202 lines, pre-computed
aggregate backend).

**Trino (master, `core/trino-main/src/main/java/`):**
`io/trino/sql/planner/plan/ExchangeNode.java`;
`io/trino/sql/planner/PartitioningScheme.java`;
`io/trino/sql/planner/PartitioningHandle.java`;
`io/trino/sql/planner/SystemPartitioningHandle.java`;
`io/trino/sql/planner/optimizations/AddExchanges.java`.

**Spark (master, `sql/`):**
`catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala`
(canonical `Distribution`/`Partitioning`/`satisfies`);
`core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala`;
`core/src/main/scala/org/apache/spark/sql/execution/adaptive/` (AQE).

**External:** Trino concepts (https://trino.io/docs/current/overview/concepts.html);
Spark AQE tuning (https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution);
Databricks AQE deep dive (https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html);
DuckDB partitioned aggregate hash table (https://duckdb.org/2022/03/07/aggregate-hashtable.html).
