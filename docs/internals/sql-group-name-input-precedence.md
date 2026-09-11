# Sql group name input precedence

Source: internal/planner/sql/parser.go — func RevertGroupByAliasesShadowedByInput(info *SelectInfo, provides func(bare string) bool) {, moved 2026-09-11 (#1026)

RevertGroupByAliasesShadowedByInput applies PostgreSQL's precedence for a
bare GROUP BY name: an INPUT COLUMN wins over a SELECT alias.

The parser substitutes such a name with the alias's defining expression
unconditionally, and its own doc comment claimed the opposite ("a table
column with the same name keeps precedence over the alias") — protocol item
9's exact failure mode, a record describing intended behaviour as present
behaviour. There is no precedence check in the parser and there cannot be
one: it has no schema and no scope. So the substitution is provisional and
this undoes it, called from the layer that knows what the FROM sources
provide.

provides reports whether one of this block's own sources carries the bare
name. It must answer only where it is CERTAIN: an unenumerable source (a
table function, a SELECT *, a table absent from the catalog) has to answer
false, which keeps the substitution and the pre-#739 answer.

The wrong-answer shape this closes: `SELECT h AS g, COUNT(*) FROM gcov
GROUP BY g, h` grouped by (h, h) and answered 2 rows where PostgreSQL — which
groups by (g, h) — answers 6. Both engines answered, and they answered
different numbers.
