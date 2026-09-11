# Derived table outer reference boundary

Source: internal/planner/physical/validate_outer_level.go — refuseOuterLevelReference, moved 2026-09-11 (#1026)

refuseOuterLevelReference classifies a qualified reference this scope cannot
resolve, when the scope is a plain derived table's body (#614).

A reference to a relation of an ENCLOSING QUERY LEVEL is legal SQL and
PostgreSQL answers it — LATERAL governs references to SIBLINGS in the same
FROM list, not to outer levels. Both halves measured on postgres:17-alpine:

	SELECT a.n, (SELECT COUNT(*) FROM (SELECT s FROM i WHERE i.n = a.n) d)
	FROM o a                                    -- five rows, one per a
	SELECT COUNT(*) FROM o a, (SELECT s FROM i WHERE n = a.n) d
	  -- ERROR: invalid reference to FROM-clause entry for table "a"

This engine does not support the first. The correlation analysis reads the
enclosing block's own terms, and a derived table's body is a separate query
block whose references it never sees, so the reference binds INSIDE the body
instead — measured, with the binder's scope opened to let it through:
`EXISTS (SELECT 1 FROM (SELECT id, g FROM t b WHERE b.g = a.g) d)` answered
5000 of 5000 rows where 4616 match, and the scalar-subquery spelling answered
one constant for every outer row. A silent wrong answer, on every arm.

So refusing is the right disposition, and it is the one that was already
happening. What was wrong is the CLASS: 42P01 `missing FROM-clause entry`
says the query is malformed, and ADR-0021 §4 said the same in prose. It is
not malformed, and a user told that it is cannot act on it. 0A000 with the
two workarounds is what this engine owes them.

The SIBLING spelling keeps 42P01, which is PostgreSQL's own disposition for
it — `outerDiag` holds only ENCLOSING levels, never this block's own FROM.
