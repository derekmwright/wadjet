# Materialized select list prefix

Source: internal/planner/physical/sort_plan.go — producerPublishesSelectList, moved 2026-09-11 (#1026)

```go
// producerPublishesSelectList reports whether the stage that produces this
// sort's input publishes the SELECT list as the ordered prefix of its own
// output.
//
// It is the measurement the bound above otherwise has to guess at, and
// declining to measure it is a silent wrong ORDER (#1003). Two output columns
// may legally carry one name — `SELECT DISTINCT a.order_id AS amount,
// b.amount … ORDER BY 1, 2 DESC` publishes `amount` twice — and once the
// position is dropped the key is resolved by that name, which
// `ColumnIndexFallback` answers with the FIRST match. BOTH keys then bound
// column one, so the two DAG arms returned the rows sorted by the leading key
// alone where PostgreSQL 17 and the single-process arms apply both. A total
// order is not one of ADR-0013's nondeterminism classes.
//
// What makes the position usable here is not the producer's KIND but what it
// PUBLISHES (ADR-0026 §8, K3's rule): the `final_aggregate` stage under that
// query materializes `[a.order_id→amount, b.amount→amount, a.order_id,
// b.amount]`, so the select list IS positions 1 and 2 of the stream. Where the
// projection is NOT materialized — `SELECT clt1.c2, clt2.c1 FROM clt1, clt2
// ORDER BY 2`, whose join stage carries no ProjectExprs — the check fails and
// the key resolves by name exactly as before.
//
// The whole visible list is compared, name AND source expression, so a
// producer that publishes the same names in another order, or narrows the
// list, does not qualify.
```
