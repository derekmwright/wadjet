# Lateral correlation key publication

Source: internal/planner/logical/builder.go — buildLateralSubquery, moved 2026-09-11 (#1026)

The key must be SELECTED — and, for an aggregated subquery, grouped.
The rewrite above promotes the correlated equality into the join
condition, so the join keys on the inner column — and a column the
subquery's select list does not publish is not there to key on. It
used to be there by accident: buildProject elided every projection
over an aggregate, so the aggregate's raw output (keys first, then
aggregates) reached the join and the key leaked through. Once that
elision became conditional on the shapes matching (c55492d1, #591)
the projection was kept, the key was genuinely gone, and
exec.HashJoin resolved its build key to index -1 — which it treats
as an unresolvable-but-matchable null key, so every build row
serialized the same degenerate key, nothing equalled the probe's
real value, and the query answered zero rows (a LEFT JOIN LATERAL
answered every aggregate NULL, which is worse).

That reasoning never depended on the aggregate, but the gate did:
it read `hasAgg && …`, so a NON-aggregated LATERAL whose projection
narrows away the correlated column got no injection and hit the
identical degenerate key. `JOIN LATERAL (SELECT amount FROM item
WHERE order_id = o.id)` answered ZERO rows and its LEFT twin
answered every amount NULL, on the single-process path, where
PostgreSQL 17 answers four rows and five (#767 part 2). It was
invisible because every LATERAL test in the tree writes `SELECT *`,
which publishes the key by definition — and lateralSelectsColumn
still declines to inject there and where the list names the key
under its own name, so the controls are unchanged.

It declines in one case where it should not, and that is a stated
boundary rather than an oversight: it matches the key's name
against a select item's ALIAS as well as its source column, so
`SELECT amount AS order_id` looks like it publishes `order_id` and
gets no injection — zero rows for PostgreSQL's four, here and at
this arc's base. Matching the published COLUMN instead would inject
a second `order_id` beside the aliased one, and `li.order_id` would
then read the KEY where PostgreSQL reads the amount: a plausible
wrong number for an obvious zero, which protocol item 8 refuses.
The key has to be published under a name nothing can collide with —
a hidden slot — which is #785's territory (ADR-0026 §3a). Pinned as
`boundary_inner_alias_shadowing_the_key_answers_nothing`.

The GROUP BY half stays gated on hasAgg: a subquery with no
aggregate has nothing to group.
ONE allocator for this lateral, from the shared reserved-slot API:
a slot is safe only when nothing else answers to it, and a per-key
namer is what let two slots of one family land in one column
(ADR-0026 2a). Seeded with every name the subquery itself binds.
