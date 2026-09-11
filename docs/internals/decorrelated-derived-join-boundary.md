# Decorrelated derived join boundary

Source: internal/planner/logical/decorrelated_inner_plan.go — decorrelatedInnerPlan, moved 2026-09-11 (#1026)

A derived table or a CTE reference JOINED to another relation declines.

The build side then carries TWO renamings: the join's own (probe bare,
build qualified where the bare name collides, decided by reorderJoins)
and the derived arm's Project, whose published name — `k` for
`SELECT c.n AS k` — is a name no scan below it produces. The logical
model tracks both, and the single-process arm answers correctly; the
stage DAG's carried-column derivation does not, and answers a DIFFERENT
number rather than failing:

	SELECT COUNT(*) FROM nation a WHERE a.n_nationkey IN (
	  SELECT s.k FROM (SELECT c.n_nationkey AS k, c.n_regionkey AS rk
	                     FROM nation c) s
	  JOIN nation b ON b.n_regionkey = s.rk WHERE s.k < 3)
	-- PostgreSQL 17 and the single-process arm: 3.  Stage DAG: 10.

Declining leaves it a subquery predicate, which both arms answer. The
spelling that puts the derived arm on the PROBE happens to agree today,
and that is the reason to decline BOTH rather than the shape that was
caught: which arm the estimator puts where is `reorderJoins`' decision
from row counts, so a cut drawn there would move under the fixture.
This is the same boundary ADR-0021 §1 draws for the key SPELLING, one
layer out; closing it is the stage model's carried columns, not this
rewrite's (report deferral).
