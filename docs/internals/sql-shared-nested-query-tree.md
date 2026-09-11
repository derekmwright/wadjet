# Sql shared nested query tree

Source: internal/planner/sql/sub_block.go — func (t *TableRef) SubSelect() (*SelectInfo, error) {, moved 2026-09-11 (#1026)

A nested query block is parsed ONCE, and every layer that reasons about that
block reasons about the SAME tree.

A derived table and a CTE arrive at the planner as SQL TEXT inside their
enclosing statement, and two layers read that text: the BINDER
(physical.validateBlock), which decides the questions a parser cannot
because they need a schema, and the LOGICAL BUILDER, which plans the block.
While each parsed the text for itself, a decision the binder RECORDED by
rewriting the block's terms reached only the top-level statement — the
binder had mutated a tree the builder threw away.

PostgreSQL's GROUP BY precedence is exactly such a decision: a bare name
there binds an INPUT COLUMN before a SELECT alias, the parser substitutes
the alias unconditionally because it has no schema, and
RevertGroupByAliasesShadowedByInput undoes the substitution at the layer
that knows the FROM sources (#739). Inside a derived table the undo was
discarded with the binder's copy of the block, so

	SELECT x.g FROM (SELECT g*0 AS g, COUNT(*) AS n FROM t GROUP BY g) x

grouped by the OUTPUT alias — ONE row where PostgreSQL 17 answers six, on
every arm and in silence (#851). The same held for a CTE body.

The memo is on the REFERENCE, so it propagates to any nesting depth without
anything having to carry a path: the builder plans the very SelectInfo the
binder validated, whose own Tables and CTEs carry their own memos.

Because the cache lives in the struct, a caller that wants the shared tree
must hold the reference by POINTER — a copy caches into itself and the
original never sees it. Both readers take one.
