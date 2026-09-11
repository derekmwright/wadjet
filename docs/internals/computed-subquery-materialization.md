# Computed subquery materialization

Source: internal/planner/physical/join_input_projection.go — absorbComputedSubqueryProjection, moved 2026-09-11 (#1026)

```go
// absorbComputedSubqueryProjection materializes a subquery's COMPUTED
// projection columns into the scan stage that produces the subtree's rows
// (#383).
//
// walkStages treats an ordinary Project as a passthrough — it emits no stage
// — so a subquery's computed column never exists anywhere on the DAG. For a
// RENAME the resolve-through helpers compensate per consumer
// (resolveShuffleKey, resolveAggInputName, resolveSortKeyColumn, the gather's
// OutputRenames), but a computed value has no source column to resolve TO:
// `SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region` under a
// join dispatched a scan reading [r_regionkey, rk2], the parquet reader
// dropped the phantom rk2 (worse: its all-or-nothing projection guard fell
// back to full width), and everything downstream that read rk2 — an outer
// join's ON residual (#358), a projected output, a sort key — saw NULL or a
// missing column, silently.
//
// The aggregate consumer already materializes derived inputs on its own
// (#355: resolveAggInputName hands the worker an InputExpr to project before
// aggregating), which is the resolve-through shape. This helper is the
// materialize-at-source shape for the consumers that have no such hook: the
// computed column is projected INTO the producing scan fragment
// (Stage.ProjectExprs → OpProject, the #169 machinery), so the build/probe
// files a join reads — and the rows a sort keys over — really carry it.
//
// Deliberately additive: bare and renamed columns pass through under their
// SOURCE names (the DAG's naming convention, which every resolver
// compensates for), and only computed aliases are appended. Nothing is
// renamed and nothing existing is dropped, so plans without a computed
// subquery projection are byte-identical — and the #355 aggregate path keeps
// finding the source columns its InputExpr references.
//
// Scope: the subquery must be a Project over a scan-rooted chain
// (Project → Filter* → Scan) whose subtree emitted exactly one scan stage.
// Anything else — aggregates, nested joins, set operations, CTE-deduped
// aliases, nested Projects — bails and keeps today's behavior.
// requireEnclosing restricts the pass to a computing Project that sits UNDER
// at least one other Project — a genuine subquery. The sort hook passes true:
// a sort's child Project can be the query's OUTPUT projection
// (`SELECT NULLIF(x, 1) AS k FROM t ORDER BY k`), and that shape belongs to
// attachScanSelectProjections, which projects exactly the SELECT list under
// its final names for the gather. A join input is never the output
// projection, so the join hook passes false.
```
