# Group key publication and resolution

Source: internal/planner/physical/group_key_carrier.go — GroupKeyResolution, moved 2026-09-11 (#1026)

```go
// A GROUP BY key travels the stage DAG under TWO names.
//
// The PUBLISHED name is what the aggregate emits the value as, and what every
// consumer above it reads: `Stage.GroupByCols`, `plansql.GroupKeyName`, the
// same text the single-process planner hands `exec.HashAggregate`.
//
// The RESOLUTION spelling is what the fragment that COMPUTES the key resolves
// it by, against the columns its own input carries. It is one of three things,
// and the third is why a second field is needed at all:
//
//   - a bare column of that input — every ordinary `GROUP BY c`, and every
//     key an aggregate DIRECTLY BELOW already published (`SELECT DISTINCT
//     g + 1 … GROUP BY g + 1` lowers to two aggregates keyed alike, and the
//     outer one reads a column, not arithmetic);
//   - an expression over columns that input carries, which the fragment
//     materializes into a hidden `__gb_expr_N` slot;
//   - a column a JOIN's stream spells differently from the query — `w` where
//     the query wrote `x.w`, or `y.w` where the join qualified a duplicate.
//
// `Stage.GroupByCols` was one field doing both jobs, and the worker recovered
// the second by PARSING the first (`worker.derivedGroupKeys`). Every unfixed
// member of #736's family was that: a key whose two names differ answers one
// NULL group over the whole table, silently, on both DAG arms, where the
// single-process path answers PostgreSQL's rows (ADR-0026 §2, §4a).
```
