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

A decorrelated LATERAL is enumerated HERE as well (arc O2). `setSubtreeAlias`
puts its alias on its SCAN, which is where the name is looked for; the scan's
own columns are never the answer, because what the relation publishes is the
body's SELECT list. That list carries the correlation slot the lowering
injected, and the slot is identified by `Node.HiddenJoinCols` on the join above
— the same identity the join's own drop uses (ADR-0026 §3c) — rather than
guessed at, so what is left is exactly the columns the query wrote. Until arc
O2 there was no such identity here and `s.*` stayed unexpanded and LOUD, which
ADR-0012 recorded as a divergence; the divergence is deleted with the refusal.

The block's projection is reached through its own `ORDER BY`, `LIMIT` and
`DISTINCT`, because none of those changes a column or its position. A list with
TWO columns of one resolution name is not enumerable here at all and answers
nil: the expansion emits one reference per column, and two references spelled
alike both bind the first.
