# Semi anti inner join pushdown

Source: internal/planner/logical/semi_pushdown.go — pushSemiAntiBelowInnerJoins, moved 2026-09-11 (#1026)

pushSemiAntiBelowInnerJoins pushes semi/anti joins (decorrelated IN /
NOT IN subqueries) below inner joins when the probe-side keys resolve
entirely within one side of the inner join:

	semi(A ⋈ B, sub)  →  semi(A, sub) ⋈ B     (keys all from A)

A semi/anti join is a pure filter on its probe input — it never adds
columns or duplicates rows — so the rotation preserves semantics
exactly while filtering BEFORE the inner join multiplies work. The
motivating shape is SF100 Q18: `o_orderkey IN (HAVING subquery)` was
applied ABOVE customer⋈orders⋈lineitem, so the plan joined all 150M
orders×customer rows (join-4: 15.8s, 8.8 GB materialized to S3) and
only then filtered to the 6,398 qualifying orders. Pushed, the
semijoin filters orders first and the customer/lineitem joins run on
6,398 rows.

Runs after pushdownPredicates (filter chains are already settled; the
rule descends through remaining Filters — semi commutes with
conjunctive filters, same argument as reduceDecorrelatedScalarAggs)
and before reorderJoins, which then treats the semi-filtered relation
as a leaf of the inner-join chain it reorders.

Conservative guards: the inner join must be a plain inner join without
CTE refs (column attribution relies on scan info), every probe key
must resolve unambiguously to exactly one side, and any extra JoinCond
on the semi must not reference the other side's columns.
