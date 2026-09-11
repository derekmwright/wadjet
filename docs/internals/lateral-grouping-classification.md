# Lateral grouping classification

Source: internal/planner/logical/builder.go — buildLateralSubquery, moved 2026-09-11 (#1026)

For aggregated LATERAL subqueries, add the correlated inner column
to GROUP BY so the aggregate applies per-group rather than globally.
e.g., SELECT COUNT(*) FROM t WHERE t.id = o.id
    → SELECT t.id, COUNT(*) FROM t GROUP BY t.id
A BLOCK THAT GROUPS IS AN AGGREGATE, whether or not its SELECT list
calls an aggregate function (#1008).

BuildFromSelect builds the Aggregate node on `hasAgg ||
len(info.GroupBy) > 0`, and every decision below asks the same
question of the same block — so reading the SELECT list alone made
this lowering and the builder disagree about the very next node.
`SELECT i.product AS p FROM item i WHERE i.order_id = o.id GROUP BY
i.product` then had its correlation key MINTED into the select list
(`i.order_id AS __key_0`) but NOT added to the GROUP BY, so the
aggregate published one key column — the product, a STRING — under
the slot the join keys on. On the single-process arms the join matched
nothing and the query answered ZERO ROWS where PostgreSQL 17 answers
eight (and, being a star over two joins, no columns either); on both
DAG arms it was the loud `join key "s.__key_0" is STRING on the probe
side and the build side took the integer key path` (#615). The LEFT
spelling padded every outer row instead: three all-NULL rows for
PostgreSQL's nine.

`hasAgg` still names what it always did — the list calls an aggregate
— because lateralEmptyInputOf's contract is about an UNGROUPED
aggregate's one row over an empty input, which a GROUP BY does not
have (it re-checks GroupBy itself, so either name gives the same
answer there).
