# Decorrelated subquery own from plan

Source: internal/planner/logical/decorrelated_inner_plan.go — decorrelatedInnerPlan, moved 2026-09-11 (#1026)

The BUILD side of a decorrelated subquery is the subquery's OWN PLAN
------------------------------------------------------------------------

decorrelateExists, decorrelateInSubqueries and decorrelateScalarSubqueries
lower a correlated subquery into a semi / anti / LEFT join whose build side
is what the subquery reads. Each of them used to assemble that side out of
`NewScan(info.Tables[0].Name, …)` plus one Scan per explicit JOIN, and that
is a model of a FROM clause with three holes in it:

  - a DERIVED TABLE has no name a Scan can hold. The parser keeps a
    FROM-subquery as a table whose NAME is its own SQL text, so the build
    side became a scan of a table the catalog has never heard of. That scan
    does not fail — it yields zero batches, so `IN` answered nothing and
    `NOT IN` answered every row, in silence (#571).
  - a CTE REFERENCE has the same exposure spelled as a bare identifier
    (#535, #581).
  - a COMMA-JOINED inner drops every relation past the first outright.

The answer up to now was to DECLINE all three (innerRelationsAreScannable),
which is right and slow: the subquery stays a per-row predicate and the
re-run reads the whole inner relation once per outer row — measured at
2N+1 reads for N outer rows against a flat 3 for the spelling that lowers
(#852, `coordinator.TestCorrelatedRerunReadsTheInnerOncePerOuterRow`).

The answer here is to BUILD it: `buildFromClause` is the builder's own FROM
assembly, so a derived table, a CTE reference, a comma list and an explicit
JOIN all plan exactly as they do at the top level. Two things then have to
follow, and they are the whole of the delicacy ADR-0021 §1 is about:

 1. NAMES. The build side carries the names its ROOT emits, and a derived
    table's or a CTE's root is a Project whose columns answer to the SCOPE
    the enclosing query gave it — `d.k`, not `d5_inner.k`. emittedColumns
    learns that scope here (scopeOwnerOf), so repairDecorrelatedSpelling can
    resolve a key spelled `d.k` against a subtree that emits `k`.
 2. A COMMA inner's equalities are JOIN CONDITIONS, not filters. Built as
    written they are condition-less cross joins with the equalities left in
    the subquery's WHERE, where innerOnlyPredicate declines them for naming
    two relations at once. liftWhereEquiPredsIntoJoins is the pass that
    already fixes that shape one level up, and running it here — on an
    ANNOTATED subtree, so it can attribute an unqualified column to its
    relation — is what lets a comma-joined correlated inner lower at all
    (#616). Whatever it cannot lift is DECLINED rather than left above the
    join, because a qualified residual there names a column the join emits
    bare, which is a wrong answer and not an error.
