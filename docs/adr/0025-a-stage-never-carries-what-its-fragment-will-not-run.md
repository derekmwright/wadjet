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
   the shape is visible.

The conservation half — a pass that DELETES a carrying stage — is not
expressible as a per-stage check, so it is a gate:
`TestStageDAGCarriesEveryFilterAndProjection` asserts that every
predicate stage emission attached is still readable off some stage after
every rewriting pass, over TPC-H and the shape corpus.

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

## Consequences

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
