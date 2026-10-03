# Boolean subquery hoisting boundary

Source: internal/coordinator/dagplan/subquery_resolution.go — resolveBooleanExists, moved 2026-09-11 (#1026)

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
violation. Any OTHER failure of its run — a sample's 2202H, a 22003 its
argument cannot hold, a 22012 — is not the statement's answer either: since
arc TB (#1411) a failure of class 22 or 21 stands where
the boolean would have, as `plansql.DeferredErrorNode` (spelled
`__deferred_error('<sqlstate>', '<sentence>')` in the stage text, compiled to
`expr.DeferredError`), and raises when a row evaluates it. The connectives
evaluate per row and short-circuit, so `CASE WHEN id > 5 THEN EXISTS (…)
ELSE true END` raises only if a row has id > 5, as PostgreSQL's InitPlan
runs only on its first reference. A conjunct that reads no row outside its
subqueries is evaluated once at plan time (`gateDeferredFailure`), which is
PostgreSQL's one-time filter: `WHERE EXISTS (…101)` over an empty table is
2202H there too. A statement-level refusal (42501, 42P01, 0A000) is still
parked as the answer wherever the subquery sits.

A SCALAR subquery in a short-circuitable position keeps whatever the path
did before — which on the DAG is a loud task failure, pinned per arm beside
PostgreSQL's answer in coordinator.TestArcI1AnUnqualifiedNameBindsTheInnerRelation
and coordinator.TestArcTBDeferredSubqueryFailureOnEveryArm (an open distributed defect). The
deferred failure would make resolving one sound when the coordinator runs it
at plan time, but a one-row-provable scalar is deferred to a PRODUCER stage,
which runs at dispatch, where its failure still ends the statement.
