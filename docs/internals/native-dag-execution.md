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
| `NodeLimit` | a bound on the sort stage below it (unless a lower LIMIT already owns that sort — #525), a per-task `RowLimit` on the scans, and — for every LIMIT the coordinator's post-gather pass cannot see — a `limit` stage | `plan.go`, `needsLimitStage`. See §Where a LIMIT is applied. |
| `NodeUnion` | `union` (+ a `GroupByAll` `final_aggregate` when not ALL) | `set_op_stages.go`. One task per arm: task *i* reads arm *i*'s whole output and projects it onto the result column names and types, so the stage's files ARE the concatenation. |
| `NodeIntersect` / `NodeExcept` | `union` (with per-arm tag columns) + a grouped counting `final_aggregate` | `set_op_stages.go` (#346). The distribution pass inserts an `exchange-repartition` on the full result row between them — see §Set operations. |
| **`NodeDistinct`** | **nothing — passthrough** | `default` case `plan.go:~3415`; walks children only. No USER DISTINCT reaches here — `logical.rewriteDistinctAsGroupBy` (optimizer) turns every `Distinct(Project)` in the tree, at any depth, into an aggregate-free `NodeAggregate` first, so it rides the aggregate stages (#466 widened this from the root path only). What still passes through: planner-inserted `BuildSideDedup` Distincts (semi/anti build dedup, decorrelated semijoin key source), which carry no user-visible semantics, and root-path fallback shapes the coordinator dedups after the gather. A user Distinct anywhere else is REFUSED by `refuseUnstageableDistinct` (`physical/distinct_refusal.go`) rather than dropped, and the coordinator answers it on the local single-process pipeline. |
| **`NodeProject`** | **nothing — passthrough**, unless a consumer needs it materialized | same `default` case; aliases recovered at gather, and every other consumer resolves them back to source names — see §Derived-table aliases and §Where a Filter and a Project land |

## Where a Filter and a Project land (the #656 class)

`walkStages` lowers a Filter by appending its predicate to the stage it just
emitted, and a Project by emitting nothing at all. Both shortcuts hold only
while the stage underneath **runs what it is handed**, and for five producers
it did not:

| producer | what happened | shape |
|---|---|---|
| `merge_sort` over a `sort` | `collapseRedundantFinalMergeSort` DELETED the carrier, predicate and all | a–d |
| `sort` | `buildSortFragment` was the one fragment builder with no `OpFilter` slot | a–d |
| deduped `cte-alias` | `flattenCTEAliases` deleted the carrier; its target is SHARED with the other reference and must not be filtered for it | e |
| aggregate output alias | the predicate was re-spelled into the group key's INPUT expression (`gk>3` → `(g+1)>3`), which the aggregate's OUTPUT — a column literally named `g + 1` — cannot evaluate | f |
| `window` | `attachScanSelectProjections` accepted only a scan or a join, so the SELECT list above a window was never computed | g |
| a SHARED CTE body's terminal | the FIRST reference walked emits the real producer, so its WHERE landed on the stage every OTHER reference reads | A1/B3 |
| a COLLAPSING producer (aggregate, union, fused scan-agg) | its output is a NEW column set, so the SELECT list can neither fuse into it nor be evaluated below it — nothing computed it | F2 |
| an absorbed aggregate rename | the aggregate stopped emitting the group key's old spelling while the gather, the sort keys and the shuffle keys still named it | F1 |
| an ORDER BY under an outer AGGREGATE | the outer `COUNT(*)` needs no columns, so the projection that computes the sort key is pruned and the sort keys on a name nothing emits (loud, at dispatch) | F2 |
| a computed GROUP BY key read above its aggregate | the stage emits the key under its expression TEXT; a projection or a union arm that rebuilds the arithmetic reads a column the aggregate does not emit and answers NULL for every row | N1 |
| a shared CTE body that AGGREGATES on a computed key | the body's own projection on the shared terminal was read as one consumer's and the query refused | N1 |
| a SELF-JOIN ordered on ALIASES of qualified columns | the ordering fuses into the join, and a projection inserted between it and the gather hides it — the right rows in the wrong SEQUENCE, which no multiset gate can see | R5 |
| a set operation over an aggregate on a computed key | the aggregate arm could not be TYPED, so the union reconciled to FLOAT64 and cast the other arm; the arms then wrote different bytes under one declaration and the sort above indexed an empty column (a recovered panic) | R4 |
| `sort` / `limit` | nothing ever populated `ProjectExprs` on one, so a SELECT list above an `ORDER BY … LIMIT` was never applied and the client got the producer's raw column | B2/D |

Every one answered WITHOUT the predicate or the projection, silently, because
no operator ever saw a name it could not resolve.

Three things close it (`planner/physical/filter_carrier.go`):

- **`stageEvaluatesFilter` / `stageAppliesProjection`** are the planner-side
  mirrors of the coordinator's fragment builders — per stage type, does the
  fragment emit an `OpFilter` for `FilterExprs` and an `OpProject` for
  `ProjectExprs`, and does the projection run ABOVE the filter slot. They are
  deliberately NOT `projectableProducer`, which answers a different question
  (can a computed sort key be materialized INTO this fragment, BELOW its
  ordering).
- **`filterCarrierIndex`** gives a predicate a stage that will run it: the
  last emitted stage when it qualifies, and otherwise a **`project` stage**
  (`StageProject`, Singleton, one dep, fragment
  `[OpShuffleSource, OpProject?, OpFilter?, sink]`) inserted above it. The
  filter runs ABOVE the projection there — that is why the stage is separate.
- **`resolveFilterAliasSpelling`** settles which of a predicate's two
  spellings the carrier can evaluate. `Stage.FilterAliases` carries the
  query's own spelling beside the resolved one, because whether the producing
  fragment emits the ALIAS or the SOURCE column is decided later, by
  `attachScanSelectProjections` — the same deferral
  `resolveDerivedAliasSortKeys` makes for a sort key.

Fragment builders that now carry what the planner attaches:
`buildSortFragment` (filter + projection, both ABOVE `OpSort` — so a WHERE
above an `ORDER BY … LIMIT` sees the LIMIT's rows), `buildWindowFragment` and
`buildLimitFragment` and `buildAggregateFragment` (projection),
`buildProjectFragment` (both).

A stage that is the terminal of a CTE body referenced MORE THAN ONCE is never
a carrier at all: every reference reads its output, so the reference carrying
the predicate gets a `StageProject` of its own. `Stage.ConsumerScoped` records
a carrier whose filter belongs to one consumer, and
`assertNoConsumerScopedFilterOnSharedStage` refuses a plan where such a stage
has two.

`ValidateNativeDAGShape` refuses any plan that still attaches either field to
a stage that ignores it — a loud refusal in place of a silently different
answer — and it runs ALL SIX checks below on every distributed plan, not
only in tests: they cost microseconds on a plan of tens of stages, and a check
that runs only over a corpus closes the class for the corpus and leaves it
open everywhere else.

The reachability check refuses at PLAN time instead, so the coordinator can
route the query local and ANSWER it (`ErrUnreachableGatherOutput`,
`runUnreachableOutputLocal`) rather than hand the client the producer's raw
columns.

Two gates cover them: `TestStageDAGCarriesEveryFilterAndProjection`
(`filter_carrier_test.go`) over TPC-H and the shape corpus, and
`TestStageShapePlacementSweep` (`shape_sweep_test.go`) over the CROSS of every
producer class with every consumer shape — the breadth gate, whose assertion
is that every shape ends in an ANSWER: planned, or refused and routed local.
The six parts are:

1. **placement by type** — `ValidateNativeDAGShape` itself;
2. **placement by SCHEMA** — every expression on a carrier resolves against
   that carrier's INPUT (`carrier_schema.go`). A stage type that reads the
   field and an expression it cannot resolve give the SAME silent answer;
   joins are excluded, because their input is the qualified union of two
   sides;
3. **conservation** — every predicate AND every projection output that stage
   emission attached is still readable off some stage afterwards;
4. **output reachability** — every `OutputRename.From` on the gather names a
   column some stage emits. This is the only part that sees a projection that
   was NEVER attached, which is what B2 was;
5. **sort-key resolvability** — the same name question asked of the one field
   part 2 does not cover. `resolveHiddenSortKeys` below settles a SYNTHETIC
   key; an ORDER BY on a real SELECT-list ALIAS inside a derived table whose
   consumer is an AGGREGATE is the sibling it does not reach — the outer
   `COUNT(*)` needs no columns, `attachScanSelectProjections` sees an
   aggregate at the root and declines, and the sort keys on a name nothing
   emits. Loud at dispatch, on a query the fast path answers. Refused at plan
   time now, so the coordinator routes it local; materializing the key on the
   DAG is the open residual (#716), and #717 tracks the locally-routed class
   as a whole;
6. **set-operation arm agreement** — every arm of a union declares the same
   type for each output column, AND an arm that COPIES a column declares what
   its producer says that column is. The worker DirectCopies a bare reference
   with the declaration unread, so two arms can agree on paper and still write
   different bytes; the consumer then reads one arm's data at the other arm's
   type, which is a runtime index panic rather than a wrong value.

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

## Derived-table aliases: eight resolvers, one convention

Because a Project emits no stage, **a derived table's rename happens nowhere
on the DAG**: every stream carries SOURCE column names, and each consumer
resolves the alias back through the logical plan.

| consumer | resolver | file |
|---|---|---|
| join key / shuffle partition key | `resolveShuffleKey` | `plan.go` |
| a join key's TYPE (both sides + the partition hash) | `resolveJoinKeyTypes` → `joinSideColTypes` → `emittedColTypes` / `setOpDeclaredOutputSchema` | `join_key_types.go` |
| which SIDE of a join a key belongs to | `subtreeNaming.ownsKey` → `assignJoinKeySides` | `subtree_naming.go` |
| aggregate argument, GROUP BY key | `resolveAggInputName` / `aggStageGroupKey` | `plan.go` |
| a column reference INSIDE an aggregate argument expression | `respellAggInputExpr` | `window_alias_respell.go` |
| ORDER BY term over an AGGREGATE producer | `resolveSortKeyColumn` | `plan.go` |
| ORDER BY term over a SCAN/JOIN/WINDOW producer | `annotateDerivedAliasSortKey` → `resolveDerivedAliasSortKeys` | `hidden_sort_key.go` |
| a UNION/INTERSECT/EXCEPT arm's projection | `setOpArmProjection` | `set_op_stages.go` |
| the gather's result schema | `resolveOutputRenameSource` | `output_rename_resolve.go` |
| a WHERE above the Project | `logical.ResolveFilterThroughProjects` | `logical/filter_project_pushdown.go` |
| WHICH GROUP KEY a SELECT item / HAVING term / sort key IS | `plansql.ExprIdentity` → `groupKeyOutputs` / `groupKeyByIdentity` | `group_key_identity.go` |

The last one is the answer to a question every resolver above an aggregate
has to ask first, and until #720/#723/#725 each of them answered it on its
own by comparing rendered TEXT — one case-insensitively, one stripping only
outer parentheses, one neither. A GROUP BY key now has ONE identity
(`plansql.ExprIdentity`, which erases parentheses, identifier case and
whitespace and nothing else) and ONE published name (`plansql.GroupKeyName`),
and BOTH engines publish a derived key under that name: the single-process
pre-aggregate projection no longer uses a synthetic `__gb_expr_N`, so the two
aggregate output schemas are identical and one logical rewrite — the HAVING
respelling — is evaluable on both. See ADR-0026.

The filter is the eighth and was added last (#653), because for a DERIVED
table it is normally not needed: `pushdownPredicates` swaps Filter-Project and
SUBSTITUTES the alias away before the physical planner ever sees it
(`splitFilterForProjectPush`, #384), so the predicate that reaches walkStages
already names source columns. It DECLINES that swap for a Project tagged with
a `CTEName` — the single-process planner replays one cached CTE result for
every reference, so a predicate pushed inside would apply to all of them — and
`walkStages`' `NodeFilter` case then appended the predicate's TEXT to the
producing stage verbatim. `WITH c AS (SELECT c_i64 AS v FROM t) SELECT … WHERE
c.v > 0` therefore reached a scan fragment whose schema carries `c_i64`, the
row evaluator answered nil for `v`, and a WHERE that admits only TRUE dropped
every row — zero rows on the DAG, correct single-process, for every type. The
resolver re-spells the predicate in place (it never MOVES it, so the
materialization fence is untouched), chaining through nested CTEs and
descending a join one arm at a time.

Two rules it needed on top of the bare-name map, each a wrong answer before
they existed:

- **The QUALIFIER decides which arm.** Applying each arm's map to the whole
  predicate rewrote a reference qualified to the OTHER arm with this arm's
  definition — `d.k > 3 OR c.gg > 100` over `SELECT id AS k, g AS gg` became
  `id > 3 or g > 100`, a *silent wrong* answer where the unfixed version was a
  visible zero. Each arm carries its scope names (`logical.nodeScopeNames`:
  scan alias/table, derived alias, `CTEName`/`CTERefAlias`) and only rewrites
  references those names claim.
- **A dotted reference resolves in ADR-0022 §1's order**, which is
  `expr.ResolveColumnRef`'s, which is what resolves the name at RUN time: the
  spelling as written, then the BARE column after dropping the qualifier, and
  only then the qualifier as a ROW container with the name as its field. So
  `rw.b` over `SELECT c_row AS rw, id AS b` is `id`, and over
  `SELECT c_row AS rw, id` it is the field — the QUALIFIER substituted, the
  field kept (`c_row.b`). Resolving in a different order describes a different
  column: skipping the bare-column step made the DAG answer the field where
  the single-process engine answered the column, and moved the derived-table
  spelling on both paths, so one query answered two ways by spelling.
  PostgreSQL 17 rejects the unparenthesised form outright (42P01, "missing
  FROM-clause entry"), so answering at all is the documented superset and the
  ADR fixes which answer.

And it STOPS at a Sort or a LIMIT: unlike a Project those DO emit stages,
carrying the names above them, so re-spelling past one would name a column the
stage the filter lands on does not have.

AMBIGUITY is not this pass's to report: `physical.validate` rejects a bare
name two relations in scope both carry before any of this runs ("column
reference %q is ambiguous", 42702, on both paths). A duplicate check inside
the resolver was written and removed — no SQL could reach it.

**The backstop under it.** A resolver that misses is silent, because the row
evaluator answers nil for a name it cannot resolve and a WHERE admits only
TRUE. The vectorized `KernelFilter` has refused that since #147; the row
evaluator now does too, on the one input whose schema is authoritative — a
SCAN's (`OpSpec.ScanSchemaFilter` → `worker.compileFilterExprs` →
`expr.CheckFilterColumns`, SQLSTATE 42703). It is a schema test, never a value
test, so a legitimately-NULL column resolves and passes. It is deliberately
NOT applied above a join: a hash-join partition whose build side is EMPTY
emits its probe rows with only the join keys declared for the missing side, so
a build column that is genuinely NULL for every row of that partition is
absent from the batch's schema — TPC-H Q20's `ps_availqty > 0.5 * __scalar_0`
over a decorrelated LEFT join is exactly that, resolvable on the partitions
with matches and schema-less on the ones without.

**A CTE is a named scope, and it records it differently.** A derived table's
alias is stamped onto every Scan below it (`setSubtreeAlias` →
`Node.DerivedAliases`); a CTE's name sits on the SUBTREE ROOT
(`Node.CTEName`, plus `Node.CTERefAlias` for `FROM c AS x`), and
`subtreeNamesRelation` reads both. Stamping the CTE name onto its scans
instead was tried and reverted: `Node.OuterTableID` would then answer `c` for
every scan in the body, so two relations comma-joined INSIDE the CTE share one
identity for predicate attribution and a predicate spanning them is pushed
onto one of them (#281's q18 CTE spelling). Before the scope was readable at
all, `c.gk` over `WITH c AS (SELECT g AS gk …)` resolved to nothing in
`resolveShuffleKey`, and the broadcast join's probe matched no row — the
silent half of #653.

Three rules they share (`planner/physical/derived_alias.go`,
#467/#468/#480/#489/#490):

- **Renames chain.** `SELECT k AS j FROM (SELECT s_nationkey AS k FROM
  supplier) x` has to walk `j` → `k` → `s_nationkey`; stopping one level
  short leaves a key that matches nothing.
- **A qualified reference may drop its qualifier only inside the scope that
  owns it.** `x.k`, `u.k`, `y.j` name the derived table's OUTPUT column, and
  `derivedScopeBareName` drops the qualifier when the subtree being searched
  is in the scope it names — `BuildFromTable`'s `setSubtreeAlias` records the
  derived alias on every Scan below it. The guard is not decoration:
  `SUM(t.c)` over `t JOIN (SELECT d AS c FROM u) v` must keep naming t's own
  column, and an unconditional strip resolves it to `d`.
- **The scope is recorded ALONGSIDE the scan's own alias, never over it**
  (`logical.Node.DerivedAliases`, #489). The two answer different questions —
  which relation the scan IS, and which derived table's scope it is IN — and
  writing the first into the second erased it: `(SELECT n1.n_name AS a,
  n2.n_name AS b FROM nation n1 JOIN nation n2 ON …) u` planned as two scans
  both called `u`, after which nothing could say which `n_name` was which.
  For the same reason a plain rename resolves to the QUALIFIER-PRESERVING
  spelling (`Projection.Expr`, not `Projection.Column`) wherever a self-join
  can put the same bare name on both arms.

Which resolvers are join-recursing is **not** uniform, and the difference is
where the gaps have been: `resolveShuffleKey`, `resolveAggInputName` and
`resolveOutputRenameSource` descend one arm at a time, so their scoping is
exact; the sort resolvers do not recurse into a join at all, which is why
`ORDER BY x.col` at the query ROOT (where `x` is a base-table alias and a
SELECT alias shadows `col`) is settled in the logical builder instead —
`sortKeyCarried`, #488 — rather than here.

A resolver that answers for a NAME does not answer for a name INSIDE an
expression, and that is the ninth entry above. `resolveAggInputName` resolved
an aggregate argument that IS an alias; an argument that is an EXPRESSION over
one was shipped as TEXT and compiled at the worker against the scan's columns,
so `SUM(CASE WHEN s = 'x' THEN twice ELSE 0 END)` over
`(SELECT s, id * 2 AS twice FROM t)` read `twice` off a batch that has no such
column and summed only the ELSE branch — TPC-H Q08's shape, silently 0, and
where the derived alias SHADOWS a base column, silently a different number
(#702). `respellAggInputExpr` applies the same resolution per reference: a
rename becomes its source column, a computed alias becomes its defining
expression PARENTHESIZED (spliced bare, `id * 2` inside `x * 3`
re-associates). It rewrites the STAGE SPEC's text only — the single-process
pipeline runs that Project as a real operator, so its alias is real there.

Both this and `respellDerivedAliasRefs` walk the expression through
`rewriteColRefs` (`colref_rewrite.go`), which covers every AST node kind and
REPORTS when it meets one it does not. Three respell sites had grown their own
walk, each covering the kinds its own defect needed, and a walk that does not
descend into a node kind is not a no-op — it leaves the references inside it
naming nothing. `assertAggregateInputsResolve` is the backstop: an
`AggSpec.InputExpr` whose references the stage's input cannot supply is a
PLAN-TIME refusal naming the column, not a wrong number.

One cause, five failure modes before the fixes: the sort loud (`sort: key
column "k" does not exist in the input schema`), the aggregate loud (`hash
aggregate: GROUP BY key "u.k" is not a column of its input`), the shuffle
loud (`partitioned shuffle: key "y.b" not in schema`), the union arm loud
(`column "k" does not exist in the input schema`), and a **broadcast join
SILENT** — its probe matched nothing and the query returned 0 rows where
PostgreSQL returns 24.

The join-SIDE consumer is the one whose failure is silent on the small end
and loud on the large end. `assignJoinKeySides` decides which key is the
probe's and which the build's by column OWNERSHIP, and a derived table's
output column belongs to no scan's column set — so ownership was
unanswerable for BOTH sides, the pair kept its positional order, and each key
was resolved against the arm that does not own it. Two sibling derived tables
hide it (their keys are symmetric); three do not.

The sort consumer is split across two passes rather than resolved in place
because `attachScanSelectProjections` may still MATERIALIZE the alias on the
producing fragment (the outermost SELECT list, #316). `SortKeySpec.AliasSource`
records the source column at stage-emission time and
`resolveDerivedAliasSortKeys` decides after that pass has run: where the
fragment's `ProjectExprs` computes a column under the key's own name the key
is already right, and otherwise it is pointed at the source. The test is
whether the projection MATERIALIZES the name, not whether the name exists —
with a SHADOWING alias (`SELECT s_acctbal AS s_suppkey … ORDER BY s_suppkey`,
which PostgreSQL binds to the alias) the producer emits a column spelled like
the key that is exactly the wrong one, and nothing errors.

## Stage → worker fragment conversion

Every stage dispatches as a **fragment**: a list of `distributed.OpSpec`
operators (`distributed/messages.go:260`). The worker requires `Operators` to be
non-empty (`worker/executor_stage.go:22`).

Conversion lives in `coordinator/execute_stage_dag.go`:
- `buildAggregateFragment` (`:2373`) → `OpShuffleSource` + `OpHashAggregate{GroupByCols, GroupByResolve, Aggregates, MergeMode, InputRowBound}`. **`MergeMode = stage.Type=="final_aggregate"||"merge_aggregate"`** (`:2400`) — merge mode rewrites `InputCol→OutputCol` and `COUNT→SUM`. `InputRowBound` is `aggregateInputRowBound`: the exact Σ`PartitionRows` over the partitions bound to this task (0 = unknown), which decides the worker aggregate's group-index layout — see `docs/design/unbounded-final-aggregate-layout.md`.
- `buildSortFragment` (`:2514`), shuffle dispatch `dispatchShuffleStage` (`:772`).
- Terminal stage gets an `OpGatherSink` when `gatherReplySubject != ""`.

### A GROUP BY key crosses the boundary under TWO names

`Stage.GroupByCols` is what the aggregate **publishes** each key as — the name
every consumer above the stage reads, and the same text the single-process
planner hands `exec.HashAggregate`. `Stage.GroupByResolve` is index-aligned
with it and is what the fragment that **computes** the key resolves it BY,
against the columns its own input carries: a column of that input
(`Computed=false`), or an expression the fragment materializes into a hidden
`__gb_expr_N` slot (`Computed=true`). Both ride `distributed.OpSpec`, and the
worker's `buildFragmentHashAggregate` sets `exec.HashAggregate.GroupByCols`
from the first and `GroupByOutNames` from the second — the same pair the
single-process builder sets (ADR-0026 §2).

Which stages carry a resolution list is the whole of #794:

| stage | computes its keys? | carries `GroupByResolve` |
|---|---|---|
| `scan` with `FusedAggGroupBy` | yes — from the table's raw rows | yes |
| `aggregate` (the partial) | yes — from raw upstream rows | yes |
| `hash_join`/`broadcast_join` with `ChainedAggGroupBy` | yes — from the join's rows | yes |
| `final_aggregate` with `RawInputAggregate` | yes — the exchange below partitions raw rows into disjoint groups | yes |
| `final_aggregate` / `merge_aggregate` (merge mode) | **no** — the input is a partial's OUTPUT | **no** |
| `exchange-repartition` with `PartialAggGroupBy` | no — the payload IS the partial's output | n/a |
| the `-interm` phase of `dispatchFinalAggregateFanout` | no — same | n/a |

The last three are the merge boundary, and it needs no agreement to keep: a
key reaching them is already a column of the stream under its published name,
so there is only one name to have.

The resolution spelling of a key that names a derived table's COMPUTED alias
is not decidable where `walkStages` emits the stage — whether any fragment
publishes the alias is what `attachScanSelectProjections` and
`absorbWindowArmProjection` decide, later. `resolveStageGroupKeys`
(`planner/physical/group_key_resolution.go`) settles it at the end of
`PlanDistributed`, against `stageStreamColumns` — a model of what a fragment
SHIPS that mirrors `joinOutputSchemaWithMapping` line for line, including the
duplicate-name qualification that makes a join stream carry `w` and `y.w` at
once, and a chained link's own `Columns` as that link's output filter (#795).

Every column in that model carries its ORIGIN ARM (the build subtree's alias,
"" for the probe side), and every rule asks it: a key names ONE arm of the
join, and a column of the same name on another arm is a different value. The
DEFINITION is re-spelled into the arm's own stream spellings (`a * 3` becomes
`z.a * 3`) before the fragment sees it, because an ordinary lookup would give
the probe's copy whichever arm the key meant.

The resolution is decided against what the arms can SUPPLY, and
`ensureJoinCarriesEvaluatedColumns` then reads the resolutions and widens the
join's OutputFilter and its input manifests to carry them —
`groupKeyResolutionRefs`. A key whose ARM carries the value nowhere is refused
with the arm named and the stream's column list in the message, and the
coordinator answers it on its local pipeline.

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

**And the worker REFUSES a base-table read that arrives without one** —
`applyDeclaredScanSchema` (`worker/executor_fragment.go`), on all three
carriers. Absence used to mean "no opinion" everywhere, and
`parquet.Reader.SchemaAs(nil)` short-circuits to the FILE's own schema, so
the catalog never entered the comparison: a stale or foreign file — the shape
a chunk-name collision (#494) produces on its own — returned the string
`'hello'` for a column the catalog calls BIGINT, while the single-process
reader refused it by name. The distinction the guard needs is one
`classifyInputFiles` already computed and threw away
(`worker/source_select.go`): a `.wshf` input legitimately carries no
declaration, a `.parquet` one cannot (#503).

The carriers the refusal found empty, and now filled:

| Dispatcher | Was | Now |
|---|---|---|
| `dispatchGatherStage` | nothing — a plain `SELECT … FROM t` dispatches no scan task, so the GATHER task does the base-table read | `Task.ColumnTypes` + the ordered fragment's `OpSpec.ColumnTypes` |
| `dispatchReplicateStage` (pass-through branch AND the materialization-failure fallback) | dropped `ScanTable`/`ScanColumns`/`ScanSchema` off the `StageOutput`, so a broadcast join's build read parquet blind | both go through `replicatePassThrough`, which forwards all four scan annotations |
| `materializeReplicate` | an `OpScan` with neither `Columns` nor `ColumnTypes`; its `allWSHF` bypass means every list that reaches it IS base parquet | both, from the upstream's annotations |
| `dispatchFinalAggregateFanout` | built its own tasks and never called `applySourceProjection`/`applySourceColumnTypes` | calls both on the intermediates (the final merge reads WSHF and needs neither) |
| `stageInputDeps` | join stages emitted probe+build only, and every other stage only `Dependencies[0]` | fused- and chained-join build aliases, and every dependency on the default arm |

The fallback is the one worth stating twice: it exists so a materialization
failure costs the query its consolidation and not its answer, and it runs
only over an upstream the bypass declined — which by construction is
multi-file BASE parquet, exactly what the guard refuses. Handing that over
bare made the guard fail the query. Both branches build their output with the
same function for that reason (`coordinator/execute_stage_dag.go`,
`replicatePassThrough`); gate:
`coordinator.TestReplicateFallbackKeepsTheQueryAnswerable`.

The refusal is **fatal, not retryable**. It is a verdict on the PLAN, which
every retry carries unchanged, so the worker types it
(`declaredSchemaRefusal`) and sets `ResultNotification.PlanRefused` from the
error's TYPE — never from its text, on #511's rule. `taskRetrier.Observe`
treats that bit the way it treats `Panicked`: terminal on the first failure.
Before this it burned all three attempts and told the client nothing the
first had not (`stage join-2 … failed after 3 attempts`).

**Kill switch: `WADJET_DECLARED_SCHEMA_STRICT=0`** restores the pre-#503
behavior — the file's own types win — on the `WADJET_FASTPATH_STRICT` (#308)
precedent. The refusal turns reads that USED to answer into hard failures, so
a deployment that hits a plumbing gap nobody has found yet needs a way back
to last release's behavior rather than an outage. It is a way out, not a
supported mode: what it restores is a read that can answer `167772165` for
`10.0.0.5`, and every use logs at Warn. Registered in `internal/optswitch`, so
the optimization-invariance oracle runs the whole corpus with it off as well
as on — a healthy fixture declares every base-table read, so the two
configurations must agree.

A gather task that fails now says so on the wire, too. The coordinator waits
on the gather STREAM and nothing else for that stage — it never reads the
task's result notification — and the worker publishes its terminal marker
even on error (withholding it turned every failure into a timeout hang). A
marker with no `Err` on it therefore reported SUCCESS with zero rows: the
worker refused correctly and the client got an empty result set.
`executeGatherStage` records its error on the sink before the deferred
`Finalize`, and `gatherReceiver` already turns that into
`gather worker error: …`.

Worker side: `buildFragmentUnary` (`worker/executor_fragment.go:952`) and
`buildFragmentHashAggregate` (`:788`). **Gap:** the hash-aggregate fragment
requires `len(GroupByCols)>0 || len(Aggregates)>0` and has **no `GroupByAll`**
— the single-process `buildDistinct` (`plan.go:5583`) uses
`HashAggregate.GroupByAll=true`, which has no distributed equivalent yet.

### …and the other thing a base-table read has to be told: which rows are deleted

A DELETE does not rewrite parquet. It records the FILE-ABSOLUTE row indices
it removed in the manifest (`catalog.DeleteMarker`), and every scan of the
marked file skips them until compaction folds them in. The single-process
scanner reads the manifest at scan Init and tracks the offset per row group
(`physical.rgUnit.rgRowOffset`); the DAG's workers read a file list the
coordinator hands them and, before #491, were told nothing — so **every DAG
scan of a table with deletes returned the deleted rows**, silently, while the
same query answered correctly on the fast path.

The fix deliberately does NOT add a fourth carrier beside the three above,
because a delete marker belongs to the **file**, not to the alias reading it:

| Where | What |
|---|---|
| `physical.Stage.ScanDeletes` | file → deleted row indices, read from the SAME manifest object that produced `ScanFiles` (`walkStages` `NodeScan`). Replayed onto the final stage list by `annotateScanDeletes` (`physical/scan_delete_markers.go`) from the planner's snapshot — **never a second catalog read**: markers grow until a compaction replaces the file they name, so a marker set read AFTER the file list can be missing the markers of a file the list still holds. |
| `coordinator.collectStageDeletes` → `withQueryDeleteMarkers(ctx, …)` | the query's union, parked on the dispatch context at the top of `executeStageDAG`. **Not** at the top of `SubmitSQL`, deliberately: `physStages` there is overwritten with a synthetic single "pipeline" stage before any stamp built from it would be read, and that emptiness is correct — every `TaskTypePipeline` task re-plans its `SQLText` on the worker against a live catalog (`executePipeline` → `planner.Plan`), whose scanner reads `manifest.DeleteMarkers` itself at scan Init, the same as the single-process engine. `coordinator.TestDistributedScanHonorsDeleteMarkersOnThePipelinePath` covers both shapes that path dispatches (plain and probe-split). |
| `Scheduler.PublishTasks` → `stampTaskDeleteMarkers` | the ONE stamp. Walks every file list a task can carry — `Files`, `InputFiles`, `BuildFiles`, `Inputs`, `PreScannedInputs`, `ScanFileFilter`, `FusedJoins[].BuildFiles`, `Operators[].{InputFiles,BuildFiles}`, `PreComputedAggregates[].CacheFiles` — the same set `annotateTaskPeerLocations` walks (`coordinator.TestTaskFieldCarrierCoverage` guards both against a new carrier going unclassified), and emits `Task.DeleteMarkers`. Every dispatcher and every retry passes through here, so a new dispatcher gets it for free. |
| worker `taskDeleteSets` → `cachedFileStreamSource.SetDeleteMarkers` | decoded per task, handed to every source the task builds; a key naming a file this source never opens simply never matches |

`collectStageDeletes` unions its stages' snapshots FIRST-WINS on a file that
appears in more than one (`delete_markers.go:63`) — harmless for a self-join,
where every scan of the file comes from the SAME plan-time manifest read, but
was a genuine gap for a concurrent DELETE landing between two *separate*
scan-node manifest reads within one statement: the older marker set could win
for a file the newer read would have marked further. Closed by #502's
per-statement manifest pinning: `physical.ManifestSnapshot`
(`internal/planner/physical/manifest_snapshot.go`), attached to a statement's
context by the coordinator (`physical.WithManifestSnapshot`,
`ExecuteSQL`/`SubmitSQL`) and consulted by every `physical.Planner` built for
it (`NewPlannerForContext`) in place of a bare `catalog.GetManifest` call.
Every scan node of one table in one statement now reads from the SAME
`*catalog.PartitionManifest` object regardless of how many times, or through
how many `logical.Optimize` passes, that table is scanned — closing this
race and the analogous staleness window for `ScanSchema` together, since
both are read off the one pinned manifest. `physical.
TestManifestSnapshotClosesTheDeleteMarkerRace` covers the race directly;
`TestManifestSnapshotPinsReadsAcrossAStatement` and
`TestManifestSnapshotIgnoresAConcurrentWriteMidStatement` cover the read-count
and staleness properties. The floor the pin reaches is two catalog reads per
table per statement, not one — `GetManifest` and `AggregateColumnStats` are
separate `Catalog` operations pinned separately, and the latter reads the
manifest a second time internally to key its own revision-validated cache
(`ManifestSnapshot`'s doc comment has the full accounting) — down from the
#483 review's 9 for a single-table SELECT, and no longer scaling with scan
node or optimizer-pass count.

Wire form: `distributed.DeleteSpec{File, Runs}`, where `Runs` is
`scan.EncodeDeleteRuns` — varint (gap, length) pairs over the coalesced runs.
Measured (`coordinator.TestTaskSpecSizeWithDeleteMarkers`, per file, against
the same markers as one `catalog.DeleteMarker` in the manifest):

| shape | wire | manifest JSON |
|---|---|---|
| 8 sparse deletes | 123 B | 172 B |
| one 500k contiguous run | 87 B | 3.39 MB |
| 1% scattered (10k rows) | 26.7 KB | 69.0 KB |

A one-file scan task therefore carries 260 B – 27 KB, and the encoding is
smaller than the manifest's own in every shape — so **the catalog, not the
8 MB NATS payload cap, is the binding constraint** and no S3 side channel is
needed. The residual is one task reading very many heavily-marked files at
once (an unfiltered pass-through gather over a whole table); that fails
loudly at `nc.Publish` with `ErrMaxPayload`, never silently.

Applying them is offset arithmetic, and the frame is the FILE:
`RowGroupIter.RowOffset()` / `DecodeAheadIter.RowOffset()` report where the
batch just delivered begins, computed from the whole file's row-group prefix
sum — so a pruned row group, a shard starting at group *k*, and the
decode-ahead iterator's out-of-order decode all stay correct.
`scan.ApplyDeleteMarkers` intersects the existing selection rather than
overwriting it, and a fully-deleted batch is DROPPED rather than shipped with
an empty selection. The row-reader fallback (Array/Map schemas) decodes the
file in one shot and uses a running count instead.
`parquetGroupIter` declares `RowOffset` so a new iterator cannot join this
path silently answering 0.

Gates: `coordinator.TestDistributedScanHonorsDeleteMarkers` (the two-path
corpus — scan/COUNT/GROUP BY/JOIN/DISTINCT before and after a DELETE),
`TestDistributedScanHonorsUpdateDeleteMarkers` (UPDATE = delete+insert; the
failure mode is DOUBLE counting),
`TestDistributedScanAgreesAcrossCompactionOfDeletedRows`, the DELETE arm of
`TestDistributedSelectAfterWriteSeesTheWrite`, and
`worker.TestStreamSourceDeleteMarkers*` for the offset arithmetic (both
iterators, sharding, per-file keying, the row-reader fallback, the
projection-intersection path).

**ADR-0010's wholesale-deploy rule applies**: a worker predating
`Task.DeleteMarkers` unmarshals it away and returns the deleted rows, so a
rolling deploy makes *some* of a stage's tasks answer wrong.

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
`FlushableOperator`. Both drivers reach it through `NextFlush`:
`physical.joinFlushSource` (single process) and the worker's
`drainFlushableOps`. The worker had no equivalent at all before #352, so a
distributed RIGHT/FULL join answered with its matched rows only.

`NextFlush`, not `FlushUnmatchedRows` directly, is the entry point, and the
difference is the whole of #550. A grace build that EVICTED a partition nils
its `h.buildBatches` slots while leaving the arena entries that point at them
in place — an argument written about the in-memory PROBE path, where partition
routing diverts the probe row before any hash lookup. The build-side flushes
are not that path: they walk the arena directly, so they used to dereference
the nil slot and fail the query. They skip a non-resident entry now
(`HashJoin.residentBuildBatch`), and the rows are not lost with it: `NextFlush`
replays each spilled partition from disk first, and the temp join over it emits
that partition's own build-side rows — `FlushUnmatched` for RIGHT/FULL,
`FlushAntiMatched` for RIGHT ANTI, `FlushMatched` for RIGHT SEMI. The replay
reads the partition's COMPLETE contents (the batches evicted plus every row
that arrived for it afterwards, which was never indexed and has no arena entry
at all), so emitting them from the resident flush as well would double them.

Two things that path depends on and that a reader should not assume:
`buildTempJoinFromBatches` resolves the replayed build key with
`columnIndexFallback`, because the key is spelled the way the PLAN wrote it
(`ON b.bk = p.pk`) while the spilled batch carries bare source names; and
`joinFlushSource` must DRAIN `NextFlush` rather than call the resident flush,
because the probe sits in its `innerOps` and `exec.Pipeline.flushSpilledOps`
never sees it. `exec.JoinPartitionsEvicted` counts evictions so a gate can
prove it reached this path instead of skipping.

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

- Stage type `StageExchangeRepartition = "exchange-repartition"` (`planner/physical/exchange.go:19`), `ExchangeStage{Keys, KeyTypes, Count}`. Sender op `OpExchangeSender{ShuffleKeys, ShuffleKeyTypes, NumPartitions}` (`distributed/messages.go`).
- `partitionedShuffleSink.Consume` (`worker/partitioned_shuffle_sink.go:103`) resolves `keyIdxs` from key names on the first batch, then `hashRowsIntoPartitions` (`:611`) computes `fnv(col1||col2||…) % numParts`.
- **Type coverage of the hash:** Int32/Port/Protocol/Date, Int64/Timestamp/IPv4/MAC/Duration, Float32 and Float64 (canonical bits, #459), String/Bytes/IPv6/UUID, CIDR (`kernel.CidrOrderKey`, #492/#520), Bool, Decimal (`batch.AppendDecimalKey`, scale-normalized so a DECIMAL(9,2) and a DECIMAL(18,4) holding one quantity co-partition, #474) and Vector. **NOT covered** (hashed as a constant via the `default` arm): Array, Row, Map. The planner never picks a container column as a partition key, so this is fine there.
- **A key pair the planner cannot type is caught at RUN time, not refused at plan time.** `joinSideColTypes` reads the shared declared-type layer, so an aggregate / window / set-operation / DISTINCT / CAST side resolves; a side it still cannot type answers `exec.KeyTypeUnresolved`, and `exec.checkProbeKeyTypes` raises when the two sides' actual key ENCODINGS then disagree (the integer fast path over a non-integer probe, or a numeric-ladder pair with different encodings and nothing resolving them). Refusing at plan time on "cannot type" would refuse joins over table functions and unannotated scans that are perfectly well-typed at run time; the runtime check cannot produce that false positive because it sees both vectors.
- **The hash runs at the key pair's RESOLVED type, not the column's** (#615). Both sides of a shuffle join repartition on their OWN key column, so a cross-width pair (`a.i = b.d`) hashed at each column's own width sends equal values to different partitions and the join downstream matches none of them — no error, just fewer rows. `ExchangeStage.KeyTypes` carries `physical.resolveJoinKeyTypes`' answer for the pair; a key whose resolved type differs from its column's is mixed as `exec.AppendWidenedKeyValue`'s canonical bytes, the SAME producer the join's own key uses (ADR-0023 item 5). Nil, or `exec.KeyTypeUnresolved` in a slot, means "the column's own type" — every same-type shuffle, unchanged. `Distribution.KeyTypes` carries it into the property algebra so `Satisfies` does not call two differently-hashed exchanges interchangeable.
- **Collision-safety property:** because the hash is a deterministic function of the row bytes, *identical rows always hash identically* → same partition. Uncovered types therefore cause only **skew**, never incorrect partitioning — which is why an all-columns hash (for a future sharded DISTINCT) is correct even on tables with nested/decimal columns: the final per-partition dedup compares actual values.

## Where dedup / aggregate / distinct happen

- **Aggregate (with functions):** `aggregate` (partial, per scan task) → `final_aggregate` (merge). The distribution pass inserts a shuffle/gather so the final merges all partials. ✅ correct distributed.

  **An UNGROUPED partial that consumed no rows still writes a file.** SQL's identity row (SUM/MIN/MAX/AVG → NULL, COUNT → 0) is owed by every ungrouped aggregate, so a selective filter that matches nothing in one task's files produces a one-row `.wshf` there rather than nothing — unlike a GROUPED partial, which emits no rows and so writes no file at all (`writeStageOutput` returns early on `totalRows == 0`). That row is the one output with no input vector to type itself from, which makes it the place a parameterized declaration goes missing: it shipped `DECIMAL(0,0)` where its siblings shipped `DECIMAL(38,s)`, and the merge read their unscaled Int128s at scale 0 — `SUM(a) WHERE id < 5` answering `3824.00` for `38.24` (#685). `exec.AggColumn.OutputPrecision/OutputScale` (planner → `distributed.AggSpec` → `buildHashAggregate`) is what it declares now; ADR-0010 carries the rule and the reader-side guard behind it. AVG is split into `__avg_sum#X` + `__avg_count#X` by `decomposeAvg` before this, so the sum leg's declaration is the INPUT's scale and has to be carried too (`AggSpec.InputScale`) — `batch.AvgScale` saturates at 38 and cannot be inverted.
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

### `COUNT(DISTINCT)` on the DAG, and what a partial-dedup exchange would need (#294)

A DISTINCT aggregate has no bounded partial form (#291), so the DAG routes
every aggregate carrying one through the one-level shape:
`RawInputAggregate: true, Tasks: 1` (`planner/physical/plan.go`, the
`hasDistinctAgg` arm), and `execute_stage_dag.go` then REFUSES to fan it
out. One task reads the whole input. Measured intra-node, before any
cluster factor: grouped `COUNT(DISTINCT)` 291.0 ms at one worker against
67.5 ms at eight (4.31x); ungrouped 189.9 → 49.3 (3.85x).

One family escapes, through the logical rewrite in
`planner/logical/count_distinct_rewrite.go` (gated by the `optswitch`
`two-level-distinct` / `WADJET_TWO_LEVEL_DISTINCT`): a two-level fold that
dedups at level 1 by adding the distinct column to the GROUP BY and counts
at level 2. It applies only when ALL of eight rules hold — one aggregate
node with no grouping sets; **exactly one** DISTINCT aggregate; it is
`COUNT` over a non-empty input column; its input expression, if present, is
a bare column reference; every other aggregate is count/sum/min/max/avg; if
grouped, the distinct column AND every group key are integer-typed; if
grouped, no more than one aggregate in total (Q10 regressed +54% otherwise,
pending NDV bounds); and no group key equals the distinct column. SF1
single-process A/B through the toggle: an eligible global
`COUNT(DISTINCT)` 99.2 ms ON against 155.6 ms OFF (1.57x); the ineligible
shapes do not move with the toggle, which is the control that validates the
census.

Everything else — a grouped `COUNT(DISTINCT)` with a second aggregate, a
STRING group key, a STRING distinct column under an integer group key
(a shape the filing does not name), multi-distinct, `COUNT(DISTINCT expr)`,
`SUM(DISTINCT …)` global or grouped — takes the `Tasks: 1` route.

**The remaining half is a LEAD, not a follow-up.** A distributed
partial-dedup exchange needs three things that do not exist, and this is
recorded so the next attempt starts here:

1. **A dedup-capable exchange sink.** `StageExchangeRepartition` carries
   `Exchange.Keys` / `Count` / `ComputedCols` and has no dedup-at-write
   flag and no per-partition set state. Dropping duplicate `(K, x)` pairs
   at write time is a stateful sink the exchange writer does not have.
2. **A wire encoding for a distinct set.** ADR-0010's `.wshf` header has no
   spelling for one, and `AggSpec` has no partial/merge form for
   `Distinct` — only `count_distinct` as a `Func` string. Every merge-form
   aggregate on the wire today has a BOUNDED partial; a distinct set does
   not.
3. **A cost model.** Rule 7 above is explicitly blocked on NDV-based
   pair-cardinality bounds that do not exist, and the same bound is what
   decides whether a dedup exchange pays for itself. ANALYZE's sketches are
   the raw material; nothing consumes them at plan time for this.

Extending the rewrite to `SUM(DISTINCT x)` is structurally a one-line
change at the level-2 fold table (`sum` over `x` instead of `count`) —
`innerGroupBy := append(n.GroupBy, x)` is already "dedup at the input's
exact type" and AVG already decomposes. It is blocked by rule 3, which
excludes every non-COUNT aggregate before that point, and by rule 2 for the
mixed `SUM(DISTINCT a), COUNT(*)` shape, which needs PostgreSQL's
join-of-aggregates or a per-aggregate hash — a structure this file does not
have.

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
`[OpShuffleSource, OpProject(arm→result columns), OpDecimalCoerce?, OpFilter?, sink]`
(`coordinator/execute_stage_dag.go buildUnionFragment`). Three things make the
concatenation well-formed:

- **Names.** SQL takes the result columns from the first arm; every arm is
  projected onto them. Without the projection a pass-through parquet scan arm
  reaches the consumer carrying every column of its table.
- **Types.** The arms' outputs are separate `.wshf` files read as one stream, so
  a column declared FLOAT64 by one arm and INT32 by another is a decoding
  error, not a union — it panicked the gather task writing the second arm's
  chunk. `reconcileSetOpArmTypes` widens numerics along
  INT32 → INT64 → DECIMAL → FLOAT64 (PostgreSQL's own resolution, ADR-0012
  item 12) and refuses anything else. A CAST rewrites the narrower arm's
  projection where the target is INT64 or FLOAT64.
- **Scales.** A DECIMAL needs more than a matching `TypeID`, and this is where
  a `TypeID`-only reconciliation silently lied: `DECIMAL(9,2)` and
  `DECIMAL(18,4)` ARE the same TypeID, so nothing was rewritten, each arm's
  file kept its own scale in its WSHF header, and the reader of both files took
  the FIRST one's — the wider arm's unscaled Int128 read at the narrower arm's
  scale, every value 100× too large (#533). `setOpDecimalTarget` computes the
  arms' common `(p,s)`, and each arm carries a `UnionArm.DecimalCoercions`
  entry that becomes an **`OpDecimalCoerce`** (`exec.DecimalCoerce`) right
  after the projection: the unscaled carrier is MULTIPLIED, never
  reinterpreted, and a value with no Int128 at the output scale fails the task
  rather than wrapping. An integer arm (`numeric UNION ALL bigint` is numeric
  in PostgreSQL) is coerced by the same operator, from scale 0. A CAST cannot
  serve here — the cast evaluator's DECIMAL destination produces a float64.
  Where an arm cannot be resolved at plan time the query is **REFUSED, naming
  the column** — it used to leave every arm as written, which is a wrong answer
  rather than a failed task, because `shuffleWriter.writeChunk`'s scale check
  only sees a SINGLE writer handed two scales: in a union stage each arm writes
  its own consistent file and the reinterpretation happens in the downstream
  stage that reads several of them, upstream of any writer (ADR-0010, ADR-0012
  item 12). TWO conditions reach it: an arm typed DECIMAL whose `(p,s)` nothing
  resolved, and an arm with NO resolved type beside a DECIMAL sibling. The
  single-process path answers both, so this is a divergence in which answer
  EXISTS, not in what the answer is.
- **What the arm walk resolves.** `setOpArmDecls`
  (`internal/planner/physical/set_op_arm_decls.go`) is the set operation's own
  view of an arm's columns, and it differs from `inputColDecls` in the two ways
  the arm needs. A **JOIN** keeps a PER-SIDE answer: `inputColTypes` /
  `inputColDecimal` merge the two sides and DELETE any name they disagree
  about, which is right for a TypeID and throws away precisely the fact a set
  operation reconciles, so each side's columns are also keyed under its own
  relation names and the QUALIFIED spelling the projection carries (`a.dx`)
  answers about the right side (#551). A **PROJECT** is descended INTO rather
  than stopped at, so a DERIVED-TABLE arm resolves through the names its
  subplan emits (#554); `setOpArmComputedSource` rewrites a reference that
  forwards a derived table's COMPUTED column into the expression that builds
  it, because no column of that name reaches the union stage. A **nested set
  operation** behind such a Project reads its own reconciled result types
  (`setOpNodeDecls`), the same answer `setOpArmProjection` takes when the
  nested operation IS the arm. A numeric **LITERAL** arm takes its spelling's
  `(p,s)` and has its expression rewritten to that spelling's plain TEXT,
  because the evaluator folds a literal into a float64 (`litDeclType`, #665).
  A **ROW FIELD PATH** resolves through the field's declaration (ADR-0022) and
  carries its type on the spec, because nothing downstream resolves a field
  path by name. A **DERIVED TABLE or CTE on one side of a join** keys its
  emitted names under its SCOPE name (`armScopeNames`), which is what tells
  the two sides apart when neither contributes a qualified one. The walk
  claims NOTHING it cannot resolve — `declaredProjectionDecl`'s STRING
  fallback is right for advisory wire metadata and wrong here, where a
  confident wrong type casts the arm's values.

  What does NOT reach the refusal: a **computed DECIMAL expression**. `d + d`
  and `COALESCE(d, d)` declare FLOAT64 (ADR-0024 item 3's arithmetic rule has
  not landed) so the pair resolves FLOAT64 and answers float-rounded, and
  `CAST(d AS DECIMAL(p,s))` declares STRING and meets the LADDER's refusal
  instead. Both are #555, and the rung is not what has to change.
- **Nested arms.** `a UNION ALL b UNION ALL c` parses LEFT-DEEP, so the outer
  union's first arm is itself a union. Its reconciled output type comes from
  `setOpNodeResultTypes`, which recurses. Before that it was reported as
  "unknown", the enclosing operation declined to reconcile ANY arm of the
  chain, and the three files disagreed: for a DECIMAL that is #533 one level
  up, and for the INT/FLOAT ladder it dropped a whole arm's rows.
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

## SELECT-list subqueries (refused → routed local)

An UNCORRELATED subquery has a distributed lowering in a PREDICATE and none in
a PROJECTION. `resolveFilterSubqueries` replaces it with a `:scalar_N`
placeholder, `emitScalarProducerStages` emits a producer stage for it,
`Stage.ScalarDependencies` records the edge and
`substituteScalarDependencies` splices the value into the filter text before
dispatch. Nothing does any of that for a SELECT-list item:
`attachScanSelectProjections` attaches the list verbatim and the worker's
compiler has no `SubqueryRunner`, so every task failed three times with
`compile projection "(SELECT MAX(v) FROM c)": subqueries require a
SubqueryRunner` — for a query PostgreSQL and the single-process pipeline both
answer (#659). Loud, but a legal query with no answer.

`refuseScalarSubqueryProjections` (`physical/scalar_projection_refusal.go`)
refuses it before stage generation with
`ErrScalarSubqueryProjectionDistributed`, and the coordinator routes it to the
local pipeline (`runScalarProjectionLocal`, counter
`ScalarProjectionLocalRoutes()`) — the same route the correlated, DISTINCT and
IN-set refusals take. It is not CTE-specific: the same failure reproduced over
a base table and a dimension, so a CTE-narrow refusal would have left the
commoner spelling failing. A subquery in a WHERE or a HAVING is untouched and
still runs on the DAG.

## IN-subqueries the semi-join rewrite declines (materialized → set literal)

`WHERE x IN (SELECT …)` has ONE distributed lowering:
`logical.tryDecorrelateInSubquery` turns it into a semi/anti join. Three guards
DECLINE that rewrite — a subquery carrying `LIMIT`/`OFFSET` (#482), an
ungrouped aggregate item, and a computed item (#516) — and a declined IN stays
a subquery PREDICATE. Until #524 the DAG had nothing to execute one with:
`Planner.resolveSubqueryAST` handled a scalar `SubqueryNode` and fell through
`default:` for `InExpr`, so the filter shipped verbatim and the fragment failed
with *"IN subquery requires a SubqueryRunner"* while the single-process path
answered correctly.

Mechanism (`physical/in_subquery_set.go`, ADR-0021 §2):

- `resolveSubqueryAST`'s `InExpr` arm calls `Planner.materializeInSubquery`,
  which executes the UNCORRELATED subquery once on the coordinator and rewrites
  the predicate to the literal list the expression layer already evaluates —
  three-valued over a NULL in the list (#370), the same rule #507 gave the
  semi-join lowering. The subquery runs AS WRITTEN, so `LIMIT`, `OFFSET` and
  `ORDER BY` mean what they say.
- An EMPTY set renders as the constant it is (`1 = 0` for IN, `1 = 1` for NOT
  IN): true for every row including a NULL-keyed one, because an empty set has
  nothing to be UNKNOWN about (#481's `LIMIT 0` is a bound, not an absence).
- Two bounds refuse rather than guess: a set past `WADJET_IN_SET_MAX` rows
  (default 10,000 — a plan-TEXT budget, since the expression is serialized into
  every task; `=0` disables materialization) and a value with no literal
  spelling that survives the round trip through the filter's text.
- Typed error `physical.ErrInSubqueryDistributed`, parked like `correlatedErr`
  and returned by `PlanDistributed`; the coordinator routes it to
  `Coordinator.runInSubqueryLocal` — the same `runRefusedLocal` guards as the
  #359 and #466 routes. Counter: `InSubqueryLocalRoutes()`.
- A subquery that is NOT self-contained is the correlated refusal's shape, not
  this one, and `materializeInSubquery` hands it there via
  `plansql.DanglingTableRefs`.

Coverage: `benchmarks/tpch/two_path_invariance_test.go` (seven declined shapes
including a string set, each with PostgreSQL's absolute answer in `assertA`;
the runner asserts this refusal fires for NO corpus entry, because one that
takes the route is a set the planner should have inlined),
`benchmarks/tpch/in_subquery_dag_test.go` (the refusal forced by shrinking the
bound: the query must still ANSWER, and answer the same thing),
`physical/in_subquery_set_test.go` (literal round trip per kind — the quote is
the one that turns a set into a different set — refusal for what it cannot
spell, and the empty-set constants).

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
[OpShuffleSource, OpWindow, OpFilter?(predicate above the window),
 OpProject?(SELECT list above the window), <sink>]
```

The `OpProject` is #656 shape g: a Project above a window emitted no stage and
`attachScanSelectProjections` accepted only a scan or a join, so
`SELECT id, UPPER(s) FROM (… ROW_NUMBER() OVER … ) x` came back as the
window's raw input plus the window column. It runs ABOVE the operator and
narrows the output, which is what distinguishes it from `WindowKeyExprs` —
those are evaluated BEFORE the operator partitions, and APPEND to the batch.

`WindowKeyExprs` now also carries a window function's **argument** expression,
and its DECLARED type is what makes the value exact.
`resolveWindowKeys` materialized only the PARTITION BY / ORDER BY terms, so
`WindowColumn.InputCol` named a column the batch did not carry and
`SUM(d * 2) OVER ()` answered NULL on every row, on BOTH paths, for every
input type (#672). It is materialized as `__winkey_N` exactly as a computed
partition key is; `*` and a literal are excluded (COUNT(*) counts rows), and a
bare or qualified column keeps `exec.Window`'s name lookup. The argument list
splits on a TOP-LEVEL comma (`logical.firstWindowArg`) — the old
`SplitN(…, ",", 2)` cut `COALESCE(c, 0) * 2` into `COALESCE(c`.

The materialized column's **declaration** is inferred from the expression
RESPELLED through any derived-table or CTE rename between the window and its
producer (`respellDerivedAliasRefs`), and from the source declarations reached
the same way (`windowKeyInputTypes` / `windowKeyInputDecimal` fall through to
`sourceColTypesThroughRenames` / `sourceColDeclsThroughRenames`). Only the
TYPE comes from the respelled form: the single-process pipeline runs the
Project below the window as a real operator, so its output really is called
`v`, while the DAG respells the TEXT as well because that Project emits no
stage. `inputColTypes` answers nothing for a Project, so before this a key
over a derived alias had NO declarations at all and fell to the float rule —
and with exact DECIMAL arithmetic (#555) the evaluator then hands an exact
value to a FLOAT64 vector: `cannot store string into FLOAT64 vector`, on both
paths, for `SUM(v * 2) OVER ()` over `SELECT d_4 AS v`.

With both halves in place `SUM(d * 2) OVER ()` answers PostgreSQL's exact
numeric and agrees digit-for-digit with `SUM(d * 2) … GROUP BY` — pinned as
`WindowSumDecimalExpression*` in the PostgreSQL oracle corpus. What is still
float is `COALESCE(d, 0) * 2`, because COALESCE's own return-type resolution
picks the integer literal's type over the DECIMAL column's; `COALESCE(d, 0)`
alone fails outright on both paths. That is the function's declaration, not
the window's — the windowed and grouped spellings agree exactly — and it is
pinned in `stage_filter_carrier_two_path_test.go`.

A window function's **output** lives in a slot of its own,
`__win_N` — the same synthetic the nested-window rewrite has used since #610,
now taken by the BARE spelling too (`logical/builder.go`, `bareWinOutput`).
`exec.Window` APPENDS its result to the input batch, so a window that wrote
under the user's ALIAS handed the SELECT-list projection two columns of that
name; the projection resolves by name and took the input one. `SELECT id,
SUM(a) OVER () AS s FROM decpair` came back with `decpair.s`, the TEXT column,
on BOTH paths and in silence, and `AS a` came back with the window's own
argument column (#694). A window with no alias at all named nothing: the
projection asked for `""`, which the single-process path answered NULL and the
DAG dropped from the result. The projection now reads the slot and publishes it
under `windowOutputName(col)` — the alias, or the window call's own text when
there is none — and `selectOutputNames` makes the same choice so an ORDER BY
over an unaliased window resolves against the name the projection really emits.

Nothing else changes: `__win_N` is the gather's `OutputRename.From`, the
carrier-schema check reads it off `Stage.WindowCols`, and the wire declares the
output from the window rather than from whatever column the alias shadowed
(`WindowSumAliasShadows*` in the wire corpus, where PostgreSQL's `\gdesc` says
`numeric` and the shadowed reading would have said `numeric(38,10)` or
`integer`).

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
- A **materialized** key (`__winkey_N`, below) is likewise Singleton: it
  exists only inside the window fragment, after that fragment's own
  projection has run, so no upstream stage emits a column by that name and
  no exchange can hash-partition on it.

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

**Key resolution** (`physical/window_keys.go`, `resolveWindowKeys`). A window
reads its PARTITION BY / ORDER BY keys and its argument **by name** off the
input batch, and until #585 a name the batch did not carry was SKIPPED — so a
window whose only key dropped out ran over ONE partition spanning the input
and answered a different query in silence. Three spellings did that, and each
takes a different repair, chosen once here for both execution paths:

- a **qualified** reference (`PARTITION BY p.g`) is BOUND to the input column
  — `cleanExpr` drops the qualifier, and where a join really emits `p.g` the
  exact spelling wins. This has to happen at plan time and not only in the
  operator, because the key name is also the DAG's clustering key;
- an **expression** (`PARTITION BY id % 3`) is MATERIALIZED as a synthetic
  `__winkey_N` column. Named synthetically rather than by the expression's
  text because that text is already a key elsewhere — `exec.Project` types a
  projection by looking its source spelling up in the input batch. (A GROUP
  BY key goes the other way now: it is published under its CANONICAL text on
  both engines so the two aggregate output schemas match, and `__gb_expr_N`
  survives only for a LITERAL key, which no consumer resolves by expression.
  See ADR-0026.);
- a **ROW field path** (`PARTITION BY rw.f`, `SUM(rw.f) OVER ()`) is
  materialized too, at the FIELD's declared type (#603, #568's rule): it
  parses as a qualified reference, so dropping the qualifier keys on a column
  named `f` that exists nowhere.

Anything else reaches `exec.Window.bindKeyNames`, which resolves it with
`columnIndexFallback` (the qualified↔bare rule every other operator uses) and
**refuses** what it cannot resolve. Degrading to one partition is not an
outcome either half can produce.

The materialized keys ride the wire as `Stage.WindowKeyExprs` →
`OpSpec.WindowKeyExprs`, and the worker compiles them into a pass-through
projection prepended to the window's consume phase
(`worker.buildWindowKeyProjection`, `physical.NewComputedColumnsOp`). It
APPENDS rather than narrowing, because a window emits every input column plus
its own — which is also why it is not `exec.Project`. Nothing above the window
reads `__winkey_N`, which is what keeps it clear of #558: the gather projects
to the visible SELECT list and a consumer stage reads the window's outputs.

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
family are tracked separately: #478 (a derived-table LIMIT landed on no
stage — fixed, see §Where a LIMIT is applied),
#479 (the pruned-column-set dedup, fixed here), and #480 (two loud DAG
failures on a derived table feeding a join, both with explicit-`GROUP BY`
twins).

## The query panic boundary (#511, ADR-0019)

Every goroutine a query spawns converts ANY panic into that query's error
instead of ending the process: `exec.RecoverQueryPanic` /
`exec.CatchQueryPanic` in `internal/engine/exec/panic_boundary.go`. A
`FatalEvalPanic` still becomes its own precise error; anything else becomes a
`*exec.QueryPanic` (SQLSTATE XX000), logged at error level with the query id
and a truncated stack, and counted by `exec.QueryPanicsRecovered` — which is
what the two process-killer gates read.

**A boundary owes every obligation the dying goroutine held**, not just the
unit in flight (ADR-0019 §2a). That obligation is a property of the site, so
it is written down here. Adding a goroutine to the query path means adding a
row.

| Site | Boundary name | Obligation it discharges |
|---|---|---|
| `exec/pipeline.go` `Pipeline.Run` | `pipeline` | none — returns the error |
| `exec/pipeline.go` `ChainDriver.Push` | `operator chain` | none — returns the error (per batch; the defer pre-dates the boundary) |
| `exec/pipeline.go` runParallel worker | `pipeline worker` | first-error slot + `cancel()` |
| `exec/pipeline.go` partition-queue closer | `partition queue closer` | **closes every remaining partition queue** (workers drain on them) |
| `exec/aggregate_parallel_emit.go` `produce` | `aggregate emit` | drain error slot; unit `Close` + `wg.Done` stay on their own defers |
| `exec/aggregate_parallel_emit.go` closer | `aggregate emit closer` | **closes `d.out`** (`next()` blocks on it) |
| `exec/join.go` key-build worker | `hash join key build worker` | **releases `sourceMu`** (siblings block on it) + `cancelBuild()` |
| `exec/join_spill.go` build prefetch ×2 | `join spill build prefetch` | sends the partition's error down the prefetch channel |
| `scan/scanner.go` file prefetch ×2 | `scan file prefetch`, `scan reader-at prefetch` | sends this file's result **exactly once** (`sent` guard) |
| `physical/plan.go` `buildJoin` | `hash join build` | sets `buildErr` before the build barrier opens |
| `physical/plan.go` scan/rg workers, ch closer | `scan worker`, `scan row-group worker`, `scan batch-channel closer` | error onto `errCh` + `cancel()` |
| `physical/util.go` footer readers | `scan footer reader` | records **`fatalScanErr`** — a tolerated per-file failure would silently drop that file's rows |
| `physical/sort_merge_join.go` build | `sort-merge join build` | sets `buildErr` before the barrier |
| `physical/metadata_minmax.go` workers | `min/max metadata worker` | `declined` → the query falls back to a real scan |
| `coordinator/coordinator.go` `ExecuteSQL` | `coordinator query` | none — returns the error |
| `coordinator/coordinator.go` gather / inline / re-agg / result-file | `gather result fetch`, `inline result decode`, `partial re-aggregation shard`, `result file decode` | fills that slot's error; the result-file one **must surface** or rows go missing |
| `coordinator/dynamic_filter.go` | `dynamic filter artifact decode` | leaves the slot nil → the filter is withheld |
| `worker/worker.go` task executor | `worker task <id>` | failure `ResultNotification`; **stack stays in the log, not on the wire** |
| `worker/executor_fragment.go` source pump | `fragment source pump` | errgroup return value |
| `worker/executor.go` upload / parquet / shuffle | `shuffle partition upload`, `parquet file decode`, `shuffle file decode` | fills that index's result slot |
| `worker/scan_prefetch.go` prefetch workers | `scan file prefetch` | in-flight index **plus every index still queued** (nothing else drains `jobs`) |
| `worker/shuffle_decode_ahead.go` scanner | `shuffle decode-ahead scan` | `d.fail` **before** the scanner's own `close(d.delivery)` |
| `worker/shuffle_decode_ahead.go` decode workers | `shuffle chunk decode` | releases the slot's CPU token, sets its error, **closes its `done`** |
| `worker/cached_store.go` `readFully` | (plain defer) | **releases the worker-lifetime ledger charge** and closes the reader |
| `pgwire/server.go` per message | `pgwire message` | ErrorResponse; ReadyForQuery **only for 'Q'** — extended-query messages enter `skipUntilSync` and let Sync send it |
| `pgwire/server.go` per connection | `pgwire connection` | drops this connection only |
| `wadjet/wadjet.go`, `wadjet/dml.go` | `embedded query`, `embedded statement` | none — returns the error |

Across the process boundary the panic survives only as text, so
`exec.IsQueryPanicMessage` (matching `queryPanicPrefix`) is what
`coordinator/task_retry.go` uses to (a) mark the task terminal rather than
retrying a deterministic failure and (b) re-attach XX000 via
`stageTaskFailure`.

Gates: `TestTypeMatrixNoProcessKillers` and
`TestQueryPanicBoundaryHoldsForEveryShape` (both in `wadjet/`) fail on a dead
child AND on an unpinned recovered panic; the obligation regressions live in
`exec/join_build_panic_test.go`, `worker/panic_obligations_test.go` and
`pgwire/panic_protocol_test.go`.

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

## Where a LIMIT is applied

Three things can bound a stream on the DAG. Deciding which one owns a given
`NodeLimit` is `needsLimitStage` (`planner/physical/plan.go`):

| Applier | Reaches | How |
|---|---|---|
| the coordinator's post-gather pass | the PLAN ROOT's LIMIT and no other | `ExecuteSQL`'s `mi.Limit`/`mi.Offset` block, from `logical.ExtractMergeInfo` — which reads `plan` itself, not a path |
| a `sort` / `merge_sort` stage's top-N | a LIMIT with an ORDER BY below it, **no OFFSET**, and **no lower LIMIT already holding that sort** | `walkStages` scans backwards for an unclaimed sort and writes `Stage.Limit = limit+offset`; the SKIP is the coordinator's job in the shape that was written for |
| **`limit` stage** (`StageLimit`) | everything else | one Singleton task, `[OpShuffleSource, OpLimit, sink]`, applying OFFSET then LIMIT once over its whole input |

**Disjoint is a property of the ownership RULE, not of the shapes.** Two
LIMITs in one query can both want the same sort stage, and until #525 the
outer one took it: `(SELECT n FROM nation ORDER BY n LIMIT 3) i LIMIT 5`
overwrote the inner's 3 with 5 and then suppressed the outer's own stage
because it had just found a sort, so the query answered 5 where PostgreSQL
answers 3. Both halves were wrong and each hid the other. `walkStages`'
backwards scan now stops at a sort that already carries `HasLimit` (nothing
else writes it during the walk) and at any `StageLimit` it passes (a lower
LIMIT is applied above that sort, so this bound has to compose ON TOP), and
reports `sorted = false` for both — which sends the outer LIMIT to a stage of
its own, the composition the all-bare nesting already produced. Restricting
the scan to this LIMIT's own stages would not have helped: the inner sort is
inside the outer's child walk. Gates:
`physical.TestNestedLimitDoesNotStealTheInnerSort` (plan shape),
`coordinator.TestDerivedLimitBoundsDistributedResult` (`nested_limit_*`), and
the `NestedLimit*` families in the two-path and PostgreSQL corpora.

A LIMIT the first two miss bounded **nothing** before #478. `SELECT COUNT(*)
FROM (SELECT DISTINCT k FROM t LIMIT 2) u` counted every distinct k, its plain
and explicit-`GROUP BY` twins did the same, and an `ORDER BY … LIMIT 3 OFFSET
5` one level down answered 8 — the sort's `limit+offset` truncation with
nothing to skip it. All silent, all deterministic, all a function of how much
data sat behind the query.

**Singleton is the correctness property, not a scalability compromise.** A
per-task bound is not a global one: k tasks each keeping n rows is not the
first n rows of their union. `RequiredChildDistribution` stays `RequiredAny`
because a Singleton stage's single task already reads every partition of its
input (`partitionFilesForWorker` with `workerCount == 1`) — the same rule the
Singleton window stage runs on — so no gather exchange is spliced in to move
the same bytes to the same task. `ValidateNativeDAGShape` asserts the stage has
exactly one dependency, carries a bound, and says ONE TASK in both fields that
can say it: `Distribution.Kind == DistSingleton` (what the dispatcher reads to
pick `numTasks`, and what a fusion pass could overwrite — `fuse_stage_chains`
and `fuse_join_shuffle` both copy a neighbour's distribution wholesale) and
`Tasks == 1`. Neither alone is an assertion: `DistSingleton` is `DistKind`'s
zero value, so that half is silent about a stage whose distribution was never
assigned, while `Tasks`' zero value is 0 and catches exactly that.
`physical.TestValidateNativeDAGShapeLimit` exercises every rejection.

The per-task `Stage.RowLimit` pushdown is unchanged and still an optimization
underneath this: each producing task stops pulling at limit+offset, and the
stage above re-limits the union. That composition is exactly what the root
path already did (`RowLimit` + the coordinator's trim), and it is sound
because the global prefix takes at most limit+offset rows in total, so no task
can be truncated below what the prefix could need from it.

## Limit sentinels (#481)

A real `LIMIT 0` is a bound, not an absence. The convention after #481: `exec.Sort.Limit` and the shared sort helpers use `-1` (`logical.NoLimit`) for "unbounded"; `physical.Stage`, `logical.MergeInfo`, and `distributed.OpSpec` carry companion bools (`HasLimit`, `HasSortLimit`) because their zero-value literals are ubiquitous. An unbounded stage leaves both fields at their zero value — the shared-subplan fingerprint hashes `Limit` unconditionally. Never reintroduce `0 == no limit` on any carrier; the wire's `HasSortLimit` also tolerates a pre-#481 coordinator via the `SortLimit > 0` disjunct in the worker (ADR-0010 mandates wholesale deploys regardless).
