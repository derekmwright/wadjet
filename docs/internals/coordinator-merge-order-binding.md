# Coordinator merge order binding

Source: internal/coordinator/merge_sort_keys.go — mergeOrderSQLState, moved 2026-09-11 (#1026)

THE COORDINATOR'S MERGE APPLIES THE QUERY'S ORDERING OR SAYS IT CANNOT
(#1002).

Two merges in this package re-apply a top-level ORDER BY over rows the DAG
has already produced: `mergeProbePartials`, after a probe-split re-aggregate
or dedup, and `dedupGatherResult`, after the post-gather DISTINCT that
`walkStages` emits no stage for (#163). Both used to bind a key by looking
its written spelling up in `mergeColIdx`, an EXACT map, and to `continue`
past a key that missed.

A dropped key is a silent wrong ORDER. `SELECT DISTINCT * FROM lat_item a
JOIN lat_item b ON b.order_id = a.order_id ORDER BY a.order_id, a.amount,
b.amount` is a TOTAL order — ADR-0013 lists no nondeterminism class that
covers one — and the join publishes `[id order_id product amount b.id
b.order_id b.product b.amount]`: the probe's columns bare and every
DUPLICATE build column qualified by its owning alias. Neither `a.order_id`
nor `a.amount` is a key of that map, so both were dropped and both DAG arms
answered the rows sorted by `b.amount` alone — the LEADING key not applied
at all — while PostgreSQL 17 and the two single-process arms answered the
written sequence.

The binding is `exec.ColumnIndexFallback`, the engine's ONE resolver: the
exact spelling, then the bare name for a qualified reference, then a single
`.bare` suffix match, declining an ambiguity rather than guessing. It is
what the single-process Sort binds through (`physical.sortKeyLocalColumn`,
#989) and what the DAG's own sort stage binds through, so the three paths
resolve one key one way. `SlotPos` wins where the planner recorded one: a
name stops being an address the moment two output columns carry it (#557),
and that is the address the SELECT list gives.

A key that still does not resolve is an ERROR. The alternative is what this
function replaces — rows in an order the client did not ask for and cannot
detect — and `reAggregatePartials` already refuses an unresolvable GROUP BY
name one screen up for the same reason.
mergeOrderSQLState is the class both merge refusals carry: PostgreSQL
ANSWERS these statements, and what wadjet is saying is that ITS OWN merge
cannot apply the ordering — `0A000`, feature not supported, which is the code
the rest of this family uses for a wadjet-side bound (#811 family C, ADR-0012).
A refusal that reaches a client with no SQLSTATE is one the client cannot act
on.
