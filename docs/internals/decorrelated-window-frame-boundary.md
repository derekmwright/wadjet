# Decorrelated window frame boundary

Source: internal/planner/logical/builder.go — refuseDecorrelatedWindow, moved 2026-09-11 (#1026)

refuseDecorrelatedWindow refuses a LATERAL whose WINDOW FRAME the
decorrelation would silently change.

Decorrelation moves the correlated predicate OUT of the subquery and into
the join condition, so the subquery runs over the WHOLE inner relation and
the join selects rows afterwards. For a filter that is exact. For a WINDOW
it is not: a window is computed over the rows the subquery sees, and after
the move it sees every row.

	SELECT o.customer, s.w FROM lat_ord o JOIN LATERAL
	  (SELECT SUM(amount) OVER () AS w FROM lat_item WHERE order_id = o.id) s ON true
	-- PostgreSQL 17: 150,150,200,200 — the sum PER ORDER
	-- decorrelated:  350,350,350,350 — the sum over the whole table

350 is not a near miss, it is a different question's answer, and it was
given on every arm in silence. A window whose PARTITION BY carries the
correlation key is the one case the move preserves — each output row still
reads exactly its own correlated group — so that one is allowed and
everything else is refused. Answering it would need the window evaluated
per outer row, which this lowering does not express (0A000, the class for
"valid SQL this engine does not implement").
