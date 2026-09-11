# Row field join key materialization

Source: internal/planner/logical/join_predicates.go — isBareColRef, moved 2026-09-11 (#1026)

isBareColRef reports whether expr is a plain column reference, qualified or
not — the only operand physical.parseJoinKeys can turn into a key name.

A ROW FIELD PATH is NOT one, and that is the whole of #769's join-key face.
`c_row.b` LOOKS like a qualified column here, so it stayed in JoinCond as a
key pair, and the executor — which matches on column NAMES — resolved
`c_row.b` to nothing: `ON c_row.b = d.b` answered ~10,000 rows of
`id, NULL` (every probe row against every build row, the silent cross
product #351 is written about) on the single, spilled and broadcast arms,
and `partitioned shuffle: key "c_row.b" not in schema` on the shuffled one,
where PostgreSQL answers ONE row. The instrument that localises it is
`ON c_row.b + 0 = d.b`, an EXPRESSION operand containing the same path: it
was already right on all four arms, because the arithmetic made this
function decline and the residual route materialized the path.

Declining the bare path sends it down that same route, which is the one
ADR-0022 rule 1 prescribes anyway — a field path is materialized like a
computed expression, never passed on as a name.

rowFields is `subtreeRowFields`, so this shares the pushdown's annotation
dependency: where nothing below is annotated the map is empty, no reference
reads as a field path, and the pre-#769 routing stands. That is the same
deliberate conservative answer, and
`TestRowFieldPathPushdownFollowsTheAnnotation` is the fixture for it.
