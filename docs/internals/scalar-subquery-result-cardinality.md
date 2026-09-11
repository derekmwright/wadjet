# Scalar subquery result cardinality

Source: internal/engine/expr/correlated.go — func ScalarSubqueryValue(sql string, rows []map[string]any) (any, error) {, moved 2026-09-11 (#1026)

ScalarSubqueryValue reduces a scalar subquery's RESULT to the one value a
scalar subquery is, and is the single place in the engine that decides what
"one" means.

PostgreSQL's rule, and now this engine's (ADR-0021 §5):

	NO rows      the value is SQL NULL. An absent row is not an error.
	ONE row      that row's value.
	MORE         SQLSTATE 21000, `more than one row returned by a subquery
	             used as an expression`. Never the first row.

Every evaluator used to take `rows[0]` and say nothing. That is a WRONG
ANSWER wearing a plausible one — `WHERE n < (SELECT n FROM src)` over a
two-row `src` answered against whichever row the runner happened to return
first — and on the DML door it was worse than wrong: `DELETE FROM t WHERE
n < (SELECT n FROM src)` emptied the table where PostgreSQL raises and
deletes nothing.

The MULTI-COLUMN case is decided here too, and it is PostgreSQL's rule:
42601, `subquery must return only one column`. It used to be left alone —
the loop below picked one out of a Go map, whose iteration order is
randomized per range statement, so `SELECT (SELECT id, c_i64 FROM t LIMIT
1)` answered a different column on different runs of the same query. An
arbitrary column is not a smaller answer than a refusal, it is a wrong one,
and PostgreSQL refuses this shape at analysis time (measured, 17.5).

A ONE-column subquery whose PIPELINE emits more than one column is a
different thing and is not this refusal: it was the hidden ORDER BY key
(#875), and it is trimmed where the pipeline is built
(physical.buildSubqueryPipelineFor), so what reaches here is the SELECT
list.
