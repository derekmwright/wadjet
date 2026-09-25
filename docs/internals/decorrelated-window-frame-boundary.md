# Decorrelated window frame boundary

Source: internal/planner/logical/builder.go — lateralWindowsPerOuterRow
(was refuseDecorrelatedWindow until arc JP round 5, 2026-09-25; moved 2026-09-11, #1026)

Decorrelation moves the correlated predicate OUT of the subquery and into
the join condition, so the subquery runs over the WHOLE inner relation and
the join selects rows afterwards. For a filter that is exact. For a WINDOW
it is not: a window is computed over the rows the subquery sees, and after
the move it sees every row.

	SELECT o.customer, s.w FROM lat_ord o JOIN LATERAL
	  (SELECT SUM(amount) OVER () AS w FROM lat_item WHERE order_id = o.id) s ON true
	-- PostgreSQL 17: 150,150,200,200 — the sum PER ORDER
	-- decorrelated:  350,350,350,350 — the sum over the whole table

350 is not a near miss, it is a different question's answer.

THE RULE (round 5). When every correlated part is a key — `<inner
expression> = <outer expression>` — the rows ONE outer row sees are exactly
the inner rows whose key equals one value. A window partitioned by the keys
(before its own PARTITION BY) therefore reads exactly those rows, so every
window of the body is given the keys it does not already carry: a bare
SELECT-list window (`WindowSpec.PartitionBy`), a window nested in a SELECT
expression, in QUALIFY, HAVING or ORDER BY (`WindowFuncNode.PartitionBy`).
It is the per-key partition arc LT's bound mints
(lateral-per-outer-row-bound.md), applied to the body's own windows. Over an
aggregated body the window reads the aggregate's output, so a key the
injection minted into a slot is partitioned by that slot, and any other key
by its source column (respelled onto the group key's output).

REFUSED (0A000):
- a non-key correlated part (`AND i.v < o.total`): it is evaluated as a
  filter over the join, after the window numbered rows it then removes — no
  partition makes that right (a PARTITION BY the key was allowed before
  round 5 and answered wrong on five arms);
- an UNGROUPED aggregate body: its row for an outer row with no matches is
  the join's default pad, and the window's value over that row (`rank()` is
  1) is not known there.

A window inside a NESTED subquery of the body belongs to that subquery's own
block, which the decorrelation does not move, and is left alone.

Until round 5 only the SELECT list's bare windows were asked, and those were
refused unless partitioned by the key: `QUALIFY row_number() OVER (ORDER BY
i.v DESC) = 1` answered zero rows on every arm (LEFT: every row NULL), and
`row_number() OVER (…) + 0` the whole relation's numbers. Measured over the
w/* census (15 body positions × 4 correlation forms × JOIN/LEFT ×
unbounded/LIMIT 2, five arms against PostgreSQL 17.11): every answering cell
is PostgreSQL's; gate coordinator.TestArcJP5LateralBodyIsRightPerOuterRow.

On the stage DAG a null-extending join over an arm that PUBLISHES a window's
output wrote its pad file one column narrower than the arm's stream;
dagplan.refuseCollidingLateral routes such an arm single-process
(publishesWindow), as it does a grouped arm.
