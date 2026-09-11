# Lateral key slot reference scope

Source: internal/planner/logical/builder.go — respellKeyRefsToSlot, moved 2026-09-11 (#1026)

respellKeyRefsToSlot points the subquery's own references to the correlation
key at the SLOT the aggregate publishes it under.

Every site keeps its own text and its own position; only what it READS
changes, from the source column to the slot. It matters on the DAG and not
on the single-process path, which is what made it invisible for a round: a
Project emits no stage, so what a fragment above the aggregate sees is the
AGGREGATE's output — the key under `__key_N` and the aggregates under their
own names. A site still reading the SOURCE column then bound whatever
answered to that name in the stream, and in the colliding shape that is the
AGGREGATE:

	SELECT order_id AS oid, MAX(amount) AS order_id … WHERE order_id = o.id
	  → aggregate emits [__key_0, order_id(max)]
	  → `oid` read `order_id` = the MAX. `Alice,100,100` for PostgreSQL's
	    `Alice,1,100`, on both DAG arms.

IT WALKS THE BLOCK, not the select list. Round 2 rewrote select items only,
and a HAVING over the key — `GROUP BY order_id HAVING order_id > 1` — was
left reading a column the aggregate no longer publishes: `filter column
"order_id" does not exist in the input schema` on the single-process arm and
`SELECT list no stage computes` on both DAG arms, where the base answers
PostgreSQL's row. HAVING and the subquery's own ORDER BY read what the
aggregate PUBLISHES and take the slot; the WHERE and the GROUP BY are
resolved against its INPUT and keep the source column. `plansql.RewriteExpr`
reaches a reference nested in a CASE, a cast or a function call, and stops
at an aggregate call for the same reason the WHERE is left alone.

A WINDOW spec is deliberately not walked: `refuseDecorrelatedWindow` reads
PARTITION BY as the query WROTE it to decide whether the decorrelation
preserves the frame, and a slot there would make the allowed spelling
(`PARTITION BY <the correlation key>`) look like an unrelated partition and
be refused.

The planted reference carries ColRef.Slot, which is the provenance every
pass that has to tell a planner-planted slot reference from a user's column
of that name reads (ADR-0025 rule 1).
