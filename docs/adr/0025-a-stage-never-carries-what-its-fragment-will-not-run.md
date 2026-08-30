# ADR-0025: A stage never carries a predicate or a projection its fragment will not run

Status: Accepted (2026-08-29, #656)

## Context

`walkStages` lowers a logical Filter by appending its predicate text to
the stage it has just emitted, and a logical Project by emitting nothing
at all — the DAG's long-standing convention, because the gather's
`OutputRenames` recovers the SELECT list and every consumer resolves a
derived alias back to its source column (docs/internals §Derived-table
aliases: eight resolvers, one convention).

Both shortcuts are unchecked. Nothing asserted that the stage underneath
would RUN what it was handed, and for five producers it did not:

- `buildSortFragment` was the one fragment builder with no `OpFilter`
  slot, so a WHERE above an `ORDER BY` was never evaluated;
- `collapseRedundantFinalMergeSort` and `flattenCTEAliases` DELETE the
  stage a predicate had just been attached to;
- a deduped `cte-alias`'s target is SHARED with the other reference of
  the same CTE, so it must not be filtered on that reference's behalf;
- an aggregate's output carries a computed group key under the TEXT of
  its GROUP BY expression, which the re-spelled predicate reads as
  arithmetic over a column the output does not have;
- `attachScanSelectProjections` accepted only a scan or a join, so a
  SELECT list above a window was never computed.

Every one of these answered the query WITHOUT the predicate or the
projection. Silently — no operator ever sees a name it cannot resolve,
so there is no loud failure to notice. Seven shapes were catalogued
(#656 a–g) before the mechanism was named, and each had been filed
separately.

## Decision

**A stage carries `FilterExprs` or `ProjectExprs` only if the fragment
built for it evaluates them, and the planner proves that rather than
assuming it.**

Three parts:

1. **The planner mirrors the fragment builders.**
   `stageRunsFilterExprs` / `stageAppliesProjection`
   (`planner/physical/filter_carrier.go`) answer, per stage type,
   whether the coordinator emits an `OpFilter` / `OpProject` for the
   field. They are read off `execute_stage_dag.go` and are deliberately
   separate from `projectableProducer`, which answers a different
   question — whether a computed sort key can be materialized INSIDE a
   fragment, below its ordering.

2. **When nothing can carry it, emit a stage that can.**
   `StageProject` is a Singleton one-dependency stage whose fragment is
   `[OpShuffleSource, OpProject?, OpFilter?, sink]`, with the filter
   ABOVE the projection. `filterCarrierIndex` inserts one exactly when
   the last emitted stage would not run the predicate. The alternative —
   refusing the shape at plan time and routing it to the local engine —
   is reserved for constructs the DAG has no lowering for at all
   (correlated subqueries, unstageable DISTINCT, unmaterializable IN
   sets, and now SELECT-list subqueries, #659); a filter is not one of
   those.

3. **A mismatch is a plan-time refusal, not a wrong answer.**
   `ValidateNativeDAGShape` rejects any plan that populates either field
   on a stage that ignores it. This is the #349 precedent: fail where
   the shape is visible. All FIVE checks below run on every distributed
   plan, not only in tests — they cost microseconds on a plan of tens of
   stages, and a check that runs only over a corpus closes the class for
   the corpus and leaves it open everywhere else. A 155-shape sweep found
   twelve live flags outside the corpus the day they were promoted,
   three of them silent wrong answers.

   The reachability check refuses at PLAN time rather than at dispatch,
   so the coordinator can route the query to its local engine and ANSWER
   it (`ErrUnreachableGatherOutput`), the way it already does for a
   correlated subquery and a SELECT-list subquery. A shape whose SELECT
   list the DAG cannot compute gets the right answer instead of the
   producer's raw columns.

4. **A SHARED producer carries nothing consumer-specific.** A CTE
   referenced more than once is planned ONCE — that dedup is what keeps
   Q15's two chains from drifting — so its terminal stage belongs to
   every reference equally. A WHERE above one reference is not a
   property of the CTE, in EITHER direction: the first reference walked
   emits the body's real producer, so its WHERE landed on the stage every
   other reference reads. `filterCarrierIndex` refuses such a stage as a
   carrier (the reference gets a `StageProject` of its own), and
   `assertNoConsumerScopedFilterOnSharedStage` then refuses ANY stage
   with more than one consumer that carries a filter or a projection —
   derived from the PLAN, not from a marker, because a guard that trusts
   `Stage.ConsumerScoped` passes any plan built another way. The one
   exception is a scan's own pushed-down predicate, which is part of the
   relation every consumer reads; there the marker still selects, because
   only stage emission knows whether the filter was attached inside the
   CTE body or above a reference.

Four things a per-stage check cannot see run alongside it, on every
plan, and are gated over TPC-H plus the shape corpus
(`TestStageDAGCarriesEveryFilterAndProjection`) and over the CROSS of
every producer class with every consumer shape
(`TestStageShapePlacementSweep` — the breadth gate, whose assertion is
that every shape ends in an ANSWER: planned, or refused and routed
local):

- **conservation** — every predicate and every projection output stage
  emission attached is still readable off some stage after every
  rewriting pass (a pass that DELETES a carrier);
- **name resolvability** — every expression on a carrier resolves against
  that carrier's INPUT SCHEMA, not merely against its stage type. The two
  differ exactly where the class is silent: `expr.ColRef.Eval` answers nil
  for a name it cannot resolve, so the predicate is UNKNOWN on every row.
  Join stages are excluded and named as excluded, because their input is
  the qualified union of two sides and asserting over it produces false
  refusals;
- **output reachability** — every `OutputRename.From` on the gather is a
  column some stage really emits. This is the only one that sees a
  projection that was NEVER ATTACHED: nothing was deleted and nothing is
  misplaced, the SELECT list simply became nobody's job, and the client
  gets the producer's raw columns;
- **sort-key resolvability** — the same name question asked of the one
  field the schema check does not cover. A sort key is not silent (`sort:
  key column "d" does not exist in the input schema` is a loud dispatch
  failure), but it is a loud failure for a query PostgreSQL ANSWERS, and it
  is reached by exactly this route: the projection that would have computed
  the key was never attached. `SELECT COUNT(*) FROM (SELECT k * 2 AS d FROM
  … ORDER BY d) x` is the shape — the outer aggregate needs no columns, so
  the projection is pruned and the sort keys on a name nothing emits.
  Refusing at plan time routes it local, which answers it. Materializing
  the key on the DAG instead is the open residual; the refusal is what
  makes the shape ANSWER in the meantime.

## Alternatives rejected

- **Keep attaching to `len(*stages)-1` and fix each shape.** Seven
  issues had already been filed this way. The placement is the defect;
  the shapes are how it surfaces.
- **Give every logical Project its own stage.** Correct and much
  simpler, and it would add an S3 round-trip per Project to every TPC-H
  query. The projection fuses into the producing fragment wherever that
  fragment can evaluate it (sort, window, limit, aggregate all gained an
  `OpProject` slot); a separate stage is the exception.
- **Rename an aggregate's outputs to the SELECT list's aliases.** Tried,
  and it broke Q15 and two derived-alias joins: every DAG resolver maps
  an alias BACK to the source column, so emitting the alias instead
  makes the join key name a column that is no longer there. The
  projection therefore carries a name only where the stage has none a
  consumer can use — a computed group key, or an expression over the
  aggregate's outputs.
- **Fuse the outer SELECT list into every producer.** A producer that
  COLLAPSES its input — an aggregate, a union, a fused scan-aggregate —
  has an output that is a NEW column set, so the SELECT list can neither
  fuse into it nor be evaluated below it. Those get a `StageProject`
  inserted directly above them, positioned so a sort between the two can
  key on what it computes. When the sort above needs BOTH a column the
  projection drops and one only the projection provides, neither side
  works and the pass declines: the reachability refusal then routes the
  query local, which answers it.
- **Let any projection-capable stage carry the outer SELECT list.**
  Tried, and it broke `SELECT DISTINCT COALESCE(x, 0)`: the list is
  written against the producer's OUTPUT, which for a scan, join, window,
  sort, limit or project IS its input's columns, and for an AGGREGATE is
  the group keys and aggregate outputs. Re-evaluating `COALESCE(x, 0)`
  over a stream that no longer carries `x` answers nothing. The
  aggregate's route is `absorbAggregateOutputProjection`, which spells
  against the output names.

## Consequences

- `StageProject` is Singleton with `Tasks: 1`, so it SERIALIZES its
  input: one task reads every partition. That is correct for a per-row
  operator and cheap for the shapes it is emitted for (a filter above a
  CTE reference, above an aggregate's SELECT list), all of which sit
  above an already-collapsed producer. It would not be acceptable above
  a wide partitioned scan, and the planner never puts it there — but a
  future pass that does must give the stage a partitioned variant first.

- The class is closed structurally: a new stage type, a new fragment
  builder, or a pass that deletes a stage now fails one of the two
  gates rather than silently dropping a WHERE.
- `Stage.FilterAliases` makes a predicate carry BOTH spellings — the
  query's own and the one re-spelled into source columns — because which
  is evaluable depends on a pass (`attachScanSelectProjections`) that
  runs later. `resolveFilterAliasSpelling` picks, mirroring what
  `resolveDerivedAliasSortKeys` already does for a sort key.
- TPC-H stage shapes are unchanged: `TestTPCH_EnsureDistribution_Snapshot`
  needed no update, because no TPC-H query puts a WHERE above a CTE's
  ORDER BY or a SELECT list above a window.
