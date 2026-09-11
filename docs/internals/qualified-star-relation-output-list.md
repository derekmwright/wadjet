# Qualified star relation output list

Source: internal/planner/logical/star_expansion.go — relationOutputColumns, moved 2026-09-11 (#1026)

relationOutputColumns is what the relation called alias PUBLISHES, in order,
or nil when this pass cannot enumerate it — in which case the star stays
unexpanded and the query stays LOUD, which is what it was before this
expansion existed. Never a guess.

The OUTPUT list, never a scan beneath a projection. A derived table, a CTE
and a VALUES list all sit above their own scans, and expanding from the scan
published columns the relation does not have:
`SELECT d.*, x.id FROM (SELECT id, customer FROM lat_ord) d` came back with
`total` as well, and a `d(a, b)` column-alias list was ignored entirely —
loud → silently wrong.

A block that names itself (`DerivedAlias`, `CTEName`) is therefore answered
ONLY from its own projection, and where that projection was elided — the
planner drops one whose shape matches its input — there is no list here to
read and the answer is nil. A base-table scan is the one relation whose
output IS its catalog schema.

A decorrelated LATERAL is enumerated by nobody here: `setSubtreeAlias` puts
its alias on its SCAN too, its own output is a projection this pass does not
resolve, and that projection carries the correlation slot the join is about
to drop — so `s.*` beside another item stays unexpanded and LOUD.
