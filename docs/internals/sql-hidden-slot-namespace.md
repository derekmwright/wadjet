# Sql hidden slot namespace

Source: internal/planner/sql/reserved_slots.go — type SlotFamily string, moved 2026-09-11 (#1026)
Superseded: Reserved-name refusal applies to user-minted names, not reading stored columns. Stored collisions are handled by renumbering planner slots through a seeded SlotAllocator; a query merely spelling a stored reserved-looking name is not blanket-refused.

Hidden slots, and why the namespace they live in is RESERVED.

The planner materializes its own values into hidden slots: a window
function's output (`__win_N`), a materialized window key (`__winkey_N`), an
ORDER BY term the SELECT list does not carry (`__sortkey_N`), a computed
GROUP BY key (`__gb_expr_N`), an aggregate's derived argument
(`__agg_expr_N`), a scalar subquery's answer (`__scalar_N`), AVG's
decomposed pair, STDDEV's and CORR's partial-state tuples, and several more.
Every consumer of a slot reads it BY NAME off a batch.

The invariant those consumers need is that the slot names something the
planner put there. It does not hold on its own, because the names are
ordinary identifiers a user can write:

	SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __win_0 FROM t) x
	-- PostgreSQL 52.99 on every row; wadjet answered t.b, silently, on every
	-- execution path, because the window's slot and the user's column were
	-- the same name and the projection resolved by name.

That is #694 re-created under the slot's own name, and it is not specific to
windows: `s AS __winkey_0` made a window over an expression answer NULL, and
a computed GROUP BY key published under its own text collides with an input
column of that name the same way. Naming a slot after something the grammar
cannot produce would close it structurally, but the SQL grammar can produce
ANY string as a delimited identifier, so there is no such name.

The namespace is therefore RESERVED, and reserving it means refusing to
answer a query that spells one. That is a divergence from PostgreSQL — which
has no reserved column namespace and answers these queries — and it is the
right trade: the alternative is not answering them either, it is answering
them WRONGLY. The refusal names the collision, so a user who genuinely has
such a column knows to alias it.

This file is deliberately self-contained: the prefix table, the name
constructor, the family test and the refusal, with no dependency on anything
else the planner has grown. A pass that mints a new slot family adds one row
to reservedSlotPrefixes and mints its names through SlotName, and both the
reservation and the refusal follow.
