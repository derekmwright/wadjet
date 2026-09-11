# Boolean subquery hoisting boundary

Source: internal/planner/physical/subquery_resolution.go — resolveBooleanExists, moved 2026-09-11 (#1026)

resolveBooleanExists resolves the EXISTS leaves under a boolean connective
and leaves every other leaf exactly as it found it.

A boolean connective SHORT-CIRCUITS, and hoisting is unconditional
evaluation: it turns a subquery the query may never reach into one the query
always runs, so any way that subquery can FAIL becomes the query's answer.
PostgreSQL 17 measured, and the single-process path agrees with it because
it evaluates per row and lazily:

	… WHERE d.id < 100 OR d.id > (SELECT id FROM t WHERE id < 5)   -- 9 rows
	… WHERE d.id < 0   OR d.id > (SELECT id FROM t WHERE id < 5)   -- 21000

The subquery returns five rows either way. The first answers because the
left arm is true for every row and the right one is never needed; the second
raises because it IS needed. Hoisting made the first 21000 as well
(round-1 review P2) — a query PostgreSQL answers, refused.

An EXISTS is the leaf where hoisting is sound: it reads no outer row, it is
TRUE or FALSE rather than a value, and it cannot raise the cardinality
violation that is the failure at issue. A SCALAR subquery in a
short-circuitable position keeps whatever the path did before — which on the
DAG is a loud task failure, pinned per arm beside PostgreSQL's answer in
coordinator.TestArcI1AnUnqualifiedNameBindsTheInnerRelation, because
answering it needs the DAG to evaluate a subquery lazily and that is not a
scope repair.
