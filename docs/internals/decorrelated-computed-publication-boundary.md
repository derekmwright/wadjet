# Decorrelated computed publication boundary

Source: internal/planner/logical/decorrelated_inner_plan.go — decorrelatedInnerPlan, moved 2026-09-11 (#1026)

A derived table or a CTE reference that COMPUTES one of the columns it
publishes declines too, and this is the #516 rule reaching one level
down rather than a new one.

innerSemiJoinKey already refuses a COMPUTED select item as a semi-join
key, because the key would name nothing the build side emits. A derived
table HIDES that: from the subquery's side `SELECT b.m FROM (SELECT
n + 1 AS m FROM t) b` is a plain column reference, and the computation
is a level down where the guard never looks. The single-process arm
evaluates it; the stage DAG carries `m` as if it were a scan column,
finds none, and the semi join builds EMPTY:

	SELECT COUNT(*) FROM mk_outer a WHERE a.n IN (
	  SELECT b.m FROM (SELECT n + 1 AS m FROM mk_inner) b)
	-- PostgreSQL 17 and single-process: 32.  Stage DAG: 0.

The same body with `n AS m` — a RENAME rather than a computation —
answers 40 on both arms, which is what says the trigger is the
EXPRESSION and not the published name.

Declining on ANY computed published column rather than only the one the
key names is deliberate: the three call sites spell their key three
different ways and none of them has resolved it yet when this runs, so
a rule that needed the key would have to be written three times and
would be checked against the un-repaired spelling. The cost is a
derived inner that computes a column the query never keys on, which
stays a per-row predicate — right, and slow.
