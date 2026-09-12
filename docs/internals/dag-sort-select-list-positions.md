# Dag sort select list positions

Source: internal/planner/physical/sort_plan.go — sortKeySlotPosStage, moved 2026-09-11 (#1026)

```go
// sortKeySlotPosStage is sortKeySlotPos for the DAG, which needs a stricter
// proof and gets one.
//
// The position addresses the SELECT LIST, so it may only be used where the
// operator's input IS the select list. On the single-process path a Project
// operator sits directly below the Sort and it is. On the DAG **no stage is
// emitted for a Project**, so the sort stage reads the materialized output of
// the PRODUCING stage — which is the select list only when that producer is a
// single relation, narrowed to exactly those columns by the scan-output
// pruning. Put a JOIN or a set operation under it and the stage emits both
// arms' whole schemas: `SELECT clt1.c2, clt2.c1 FROM clt1, clt2 ORDER BY 2`
// then sorted by column ONE on both DAG arms, right values in the wrong
// sequence (round-0 B4 — the author's own self-flag).
//
// The producer's final column list is NOT available here: Stage.OutputColumns
// is filled by pruneScanOutputColumns AFTER walkStages returns, so an exact
// check against it cannot be made at this point. The subtree's SHAPE can be,
// and it is the claim this bound rests on — asserted from both sides in
// benchmarks/tpch/duplicate_name_dag_test.go and
// internal/coordinator/collide_two_path_test.go: one relation uses the
// position, a join declines it and resolves by name, and both answer
// PostgreSQL's order.
```

## Amendment, 2026-09-12 (#1003, #1014)

The shape bound above is no longer the whole rule.

**#1003 — the position is MEASURED where the producer materialized the select
list.** `producerPublishesSelectList` compares the sort's whole visible SELECT
list, source expression and name, against the ordered prefix of the producing
stage's `ProjectExprs`. A `final_aggregate` under `SELECT DISTINCT a.order_id
AS amount, b.amount … ORDER BY 1, 2 DESC` publishes exactly that list, so the
ordinal is usable there even though the subtree joins two relations. See
`materialized-select-list-prefix.md`.

**#1014 — a WRITTEN term binds a slot too, but only under that measurement.**
An ordinal and a written qualified term address the same list, and both
engines now resolve a written term's position through one function,
`sortKeyWrittenSlotPos` (extracted from `sortKeyLocalSlotPos`). The proof is
not shared: on the DAG a written term takes a position ONLY when
`producerPublishesSelectList` holds, never under the subtree-shape bound an
ordinal may also use, because a written term is resolvable on far more queries
than an ordinal is and a position handed out where this layer has not looked
at the producer is the defect rather than the fix.

Without it, `ORDER BY 1, b.amount DESC` over an output list that publishes
`amount` twice bound BOTH keys to column one on the DAG arms —
`1,50 | 1,100 | …` for PostgreSQL 17.11's `1,100 | 1,50 | …`, and at 5000 rows
under a LIMIT a wrong ROW SET as well as a wrong sequence. Gate:
`coordinator.TestC3AWrittenSortKeyBindsItsOwnColumn`, five arms, with the bare
cross join as the control the measurement must still decline.
