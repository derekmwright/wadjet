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
