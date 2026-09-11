# Unlowered scalar projection refusal

Source: internal/planner/physical/scalar_projection_refusal.go — ErrScalarSubqueryProjectionDistributed, moved 2026-09-11 (#1026)

```go
// ErrScalarSubqueryProjectionDistributed marks a plan the stage DAG refuses
// because a SELECT-LIST item contains a subquery.
//
// The DAG lowers a scalar subquery in a PREDICATE: walkStages replaces it
// with a `:scalar_N` placeholder, emits a producer stage for it, records the
// edge in Stage.ScalarDependencies, and the coordinator substitutes the
// producer's value into the filter text before dispatch
// (resolveFilterSubqueries → emitScalarProducerStages →
// substituteScalarDependencies). There is no such machinery for a
// PROJECTION: attachScanSelectProjections attaches the SELECT list verbatim,
// and the worker's expression compiler has no SubqueryRunner, so every task
// failed three times with
//
//	compile projection "(SELECT MAX(v) FROM c)": subqueries require a SubqueryRunner
//
// for a query PostgreSQL answers and the single-process pipeline answers
// (#659). Loud, but the query HAS an answer and one engine in this process
// can compute it — so the planner refuses BEFORE stage generation and the
// coordinator routes it onto its local pipeline, exactly as it does for a
// correlated subquery (#359), an unstageable DISTINCT (#466) and an
// unmaterializable IN set (#524).
//
// The refusal is not CTE-specific: the same failure reproduces for a subquery
// over a base table or a dimension. What it does NOT cover is a subquery in a
// WHERE or a HAVING, which the deferral machinery above really does lower —
// those keep running on the DAG.
```
