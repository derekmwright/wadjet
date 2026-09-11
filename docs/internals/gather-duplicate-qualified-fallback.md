# Gather duplicate qualified fallback

Source: internal/coordinator/dag_merge.go — classScopedMatch, moved 2026-09-11 (#1026)

The same qualified→bare fallback resolveRenameSource applies, and
for the same reason: a SELECT item written through a derived
table's alias arrives spelled `u.g` while the stream carries the
bare `g` twice. Counting only the exact spelling found NO duplicate
and handed the item the first `g`, which is the KEY (#785 round 2).

ONLY when the exact spelling matched NOTHING. The rescan used to run
whenever the exact spelling matched fewer than TWO columns, so an
item whose QUALIFIED name resolved uniquely was thrown away in
favour of the first column of its BARE name — and where the producer
publishes a name twice, two different items then bound one column.
`SELECT a.order_id, a.amount, b.amount FROM lat_item a JOIN lat_item
b ON b.order_id = a.order_id GROUP BY a.order_id, a.amount,
b.amount` reaches this over the stream `[order_id amount amount
a.order_id a.amount b.amount]`: `a.amount` and `b.amount` each
matched ONE column exactly and both were re-bound to the first bare
`amount`, so the third output carried the second's value on both DAG
arms while the single-process arms and PostgreSQL 17 answered eight
distinct rows. Exact first, then the fallback, is
`exec.ColumnIndexFallback`'s own order and the rule the rest of the
engine binds by.
