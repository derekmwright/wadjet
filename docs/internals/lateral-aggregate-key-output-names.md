# Lateral aggregate key output names

Source: internal/planner/logical/builder.go — buildLateralSubquery, moved 2026-09-11 (#1026)

THE AGGREGATE'S KEY OUTPUT IS NOT THE USER'S TO NAME (#956).

The two branches below leave the key where the SELECT list put
it, which is right for the JOIN — it keys on what the lateral
publishes — and says nothing about what the AGGREGATE one
operator lower publishes the key as. That name is the source
column's stripped text, and it collides with an aggregate the
list aliased the same way:

  SELECT t.g AS gk, MAX(t.id) AS g … WHERE t.g = d.k GROUP BY t.g
    the aggregate emits [g(key), g(max)], the projection
    resolves `g` by name, ColumnIndex answers with the FIRST
    match, and `s.g` read the KEY — `0,0,0…` where PostgreSQL 17
    answers `0,0,4998…`, on the single-process path AND on both
    DAG arms.

So the slot is minted here too, and stamped onto the aggregate
as the key's PUBLISHED name. Nothing is INJECTED: the list
already carries the key, the join already keys on the name the
list publishes, and an extra output column would be a second
leak to fix. Only the aggregate's own name for the key moves.

ONLY where the names really collide. Renaming the aggregate's
key output when nothing answers to that name has a cost of its
own: `SELECT t.g, COUNT(*) AS c` has its projection ELIDED over
the aggregate (the shapes match), so what the lateral emits IS
the aggregate's output, and moving the key to a slot while the
join still keys on `s.g` left the shuffle with a key that is
not in its schema. A rename that breaks no collision buys
nothing, so it is not made.

And only where the list publishes the key under ANOTHER name.
Under its OWN name (`SELECT t.g, MAX(t.id) AS g`) the lateral
publishes two columns called `g`, PostgreSQL refuses the outer
`s.g` as ambiguous (42702), and this engine answers the key —
a superset. Moving the key to a slot THERE made both DAG arms
answer NO ROWS: a superset traded for an empty result, which is
worse than the divergence it closes. That spelling is left
where it is and recorded in the census.
