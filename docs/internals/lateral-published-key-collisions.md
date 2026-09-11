# Lateral published key collisions

Source: internal/planner/logical/builder.go — buildLateralSubquery, moved 2026-09-11 (#1026)

THE JOIN KEYS ON THE NAME THE SUBQUERY PUBLISHES (#767).

The key may be selected under an ALIAS — `SELECT t.g AS gg,
COUNT(*) FROM t WHERE t.g = d.k GROUP BY t.g`. lateralSelects-
Column sees the SOURCE and declines the injection, correctly:
the value IS published. But the promoted equality still names
`t.g`, which the subquery's output does not carry, so
exec.HashJoin resolved the build key to index -1 — the
degenerate all-rows-equal key — and the join answered ZERO
rows where PostgreSQL 17 answers seven. Silent, on the
single-process path only: both DAG arms answered correctly,
which is what made it a two-path divergence nothing gated.

Recording the published name here and rewriting the equality
below is the whole repair. It is deliberately NOT the mirror
case: an item whose ALIAS matches the key's name while its
SOURCE is something else (`SELECT amount AS order_id`)
publishes a different value under that name, and pointing the
join at it would answer a plausible wrong number for an
obvious zero. That one stays pinned and needs a hidden slot
(ADR-0026 3a).
A COLLISION IS DECIDED FIRST, because it decides whether the
list's own name for the key can be keyed on at all.

`SELECT order_id AS oid, MAX(amount) AS order_id … GROUP BY
order_id` publishes the key as `oid` and aliases its MAX to the
key's own name. Stamping the aggregate's key as `__key_0` while
the join kept keying on `oid` worked on the single-process path
— the projection is there to rename — and answered ZERO ROWS on
both DAG arms: a Project emits no stage, so the build stream is
the AGGREGATE's `[__key_0, order_id]` and `oid` is not in it.
exec.HashJoin resolved the build key to -1, the degenerate
all-rows-equal key.

So a colliding shape takes the FULL mint: the slot is injected
as an output item and the join keys on the SLOT, which is the
one name that survives every path — the aggregate publishes it,
the projection carries it, the shuffle can spell it, and the
join drops it again on the way out.
