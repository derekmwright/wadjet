# Lateral ungrouped empty input

Source: internal/planner/logical/lateral_empty_input.go — lateralEmptyInput, moved 2026-09-11 (#1026)

What an EMPTY inner input means for a LATERAL subquery — #767 part 1.

PostgreSQL evaluates a LATERAL subquery ONCE PER OUTER ROW. An UNGROUPED
aggregate over an empty input still yields exactly one row, so an outer row
the lateral matches nothing for SURVIVES, with `COUNT` reading 0 and every
other aggregate reading NULL.

buildLateralSubquery decorrelates by promoting the correlated equality into
the join condition and injecting the correlated inner column into the
subquery's GROUP BY, which turns "one row per outer row" into "one row per
GROUP THAT EXISTS". An outer row with no matching inner rows then has no
group, so an INNER join DROPS it:

	SELECT o.customer, s.item_count, s.total_amount
	FROM lat_ord o JOIN LATERAL (
	  SELECT COUNT(*) AS item_count, SUM(amount) AS total_amount
	  FROM lat_item WHERE order_id = o.id) s ON true

PostgreSQL 17 answers THREE rows over the fixture, the third being the order
with no items at `item_count = 0, total_amount = NULL`. This engine answered
two, in silence; written `LEFT JOIN LATERAL` it answered three and gave that
row `item_count = NULL`, which is a different wrong answer to the same
question.

Two things restore it, and both are decided here rather than at the join:

  - the join is a LEFT join whatever the query wrote, because the lateral
    side produces a row for every outer row and only the DECORRELATION made
    that conditional;
  - `COUNT` reads 0 on the padded rows. NULL is right for every other
    aggregate (`SUM` of nothing IS NULL in PostgreSQL) and the LEFT pad
    already gives it; COUNT is the one whose empty-input value is not NULL,
    so its references are wrapped in `COALESCE(…, 0)`.

A subquery the QUERY grouped is untouched: `GROUP BY x` over an empty input
yields NO row in PostgreSQL either, so an outer row with no match is
correctly dropped by an inner join.
