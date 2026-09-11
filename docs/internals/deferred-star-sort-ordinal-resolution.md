# Deferred star sort ordinal resolution

Source: internal/planner/logical/ordinal_sort_keys.go — ResolveOrdinalSortKeys, moved 2026-09-11 (#1026)

`SELECT * ... ORDER BY 1` — a select-list POSITION over a list whose length
is not known until the star expands.

The ordinal is resolved in the parser (`resolvePositionalRefs`) for every
list whose items are countable there, which is every list with no `*` before
the position. A star is not countable there: it stands for however many
columns its source has, and the source is a catalog question. So the parser
leaves such an ordinal alone, `resolveOrderBy` carries it on the sort key as
`OrderExpr.Position`, and this pass resolves it where the answer exists —
immediately after `ExpandStarProjections`, against the projection list the
star produced.

Before this, the shape was REFUSED on every arm ("`SELECT *` is expanded too
late for the planner to count its positions here"), while PostgreSQL answers
it. `SELECT * FROM t ORDER BY 1` is what psql, DataGrip, Superset and every
"preview this table" button emit (#810).

The refusal it replaces was not wrong about the risk — materializing the
numeric CONSTANT as the sort key would sort by a value that is the same in
every row, which is a silent no-op and exactly what the ORDER BY pass exists
to end. That is why an ordinal this pass cannot resolve stays unresolved and
is refused loudly by the physical planner rather than quietly materialized.
