# Real in list plan time refusal

Source: internal/planner/physical/real_list_refusal.go — refuseUnrepresentableRealInList, moved 2026-09-11 (#1026)

```go
// The plan-time refusal for a `real IN (...)` list holding a literal that
// cannot be a real (#631 follow-up).
//
// PostgreSQL builds the array before it reads a row: `real IN (1e40, 3.1)`
// casts `{1e40,3.1}` to real[] during parse analysis, 1e40 does not fit, and
// the query fails with 22003 — whether or not any row would have been
// examined, and whether or not the predicate is even reachable:
//
//	WHERE r_val IS NULL AND r_val IN (1e40, 3.1)  -> ERROR 22003
//	WHERE r_key < 0     AND r_val IN (1e40, 3.1)  -> ERROR 22003
//
// Both evaluation paths raised this from inside the ROW LOOP instead, which
// makes an error that PostgreSQL guarantees depend on the data: the kernel
// resolves on the first BATCH, so an empty scan never raised, and the row
// evaluator's binding raises on the first non-NULL row, so a predicate that
// only ever meets NULLs never raised either. Both shapes above answered 0 rows
// on at least one path.
//
// Refusing here fixes both at once, and at the layer that can: the planner
// holds the catalog's declared types (AnnotateScanColumns leaves them on the
// scan nodes, and inputColDecls walks them up to the filter), which is exactly
// what decides whether the list is a real[] cast at all. It runs from Plan and
// PlanDistributed, so the single-process engine, the small-query fast path and
// the stage DAG all refuse identically, before any task is dispatched.
//
// The row-loop raises are KEPT as backstops. They cover the shapes this pass
// cannot see — a predicate whose column resolves through a projection alias the
// planner cannot type, a filter compiled from a fragment by a worker running
// an older coordinator's plan — and a second refusal of a query already
// refused costs nothing.
```
