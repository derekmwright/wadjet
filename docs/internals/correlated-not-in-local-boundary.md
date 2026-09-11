# Correlated not in local boundary

Source: internal/planner/logical/optimizer.go — tryDecorrelateInSubquery, moved 2026-09-11 (#1026)

A CORRELATED NOT IN is not lowered to this join at all (#538, #578).

NOT IN is three-valued: TRUE only when x differs from every y in ITS OWN
correlation group, FALSE when it equals one, and UNKNOWN — so WHERE
drops the row — when x is NULL and the group is non-empty, or when the
group holds a NULL y that x did not otherwise match. An anti join
answers the TWO-valued question "did nothing match", which is its NOT
EXISTS twin: measured against live PostgreSQL 17 over the multikey
fixture, three shapes answered 13 for 9, 6 and 9 — exactly what the
corresponding NOT EXISTS answers, on all four arms and in silence.

Node.NullAwareAnti above cannot express the correlated form and its own
comment has said so since #507: the flag reads ONE fact off the WHOLE
build side and empties the output when it is true, so setting it here
would drop every row the moment ANY group held a NULL. The fact this
predicate needs is per correlation GROUP.

The identity that WOULD express it with joins this engine already has:

	x NOT IN (SELECT y FROM t WHERE corr)
	  ≡  NOT EXISTS (SELECT 1 FROM t WHERE corr AND y = x)
	     AND NOT EXISTS (SELECT 1 FROM t WHERE corr AND (y IS NULL OR x IS NULL))

— an ordinary equi-key anti join beside a second one whose residual is
`(y IS NULL OR x IS NULL)`. Both hash-partition like any other join, so
neither needs #539's replicated build. It was built and measured, and it
does not work TODAY for a reason that is not in this package: a
semi/anti join's residual is compiled by physical.BuildSemiAntiFilter,
which reads the filter as TEXT — split on " and ", then find one of six
comparison operators — so an OR and an IS NULL compile to NOTHING and
are dropped in SILENCE, and physical.extractFilterBuildColumns narrows
the stored build by the same text split and would delete the very column
the residual reads. Measured with the two-join form in place: 0 rows for
PostgreSQL's 9. That is #562's defect class one layer down.

So the honest lowering is none: leave the IN a subquery predicate, where
expr.CorrelatedInSubquery.EvalBoolNull already carries the exact rule per
outer row (a NULL probe is UNKNOWN, a miss against a set containing a
NULL is UNKNOWN, an empty set is TRUE). A slower right answer beats a
wrong one, and the stage DAG routes such a plan to the coordinator-local
pipeline — which the census asserts with CorrelatedLocalRoutes beside the
rows, so the cost is recorded and not merely described.
