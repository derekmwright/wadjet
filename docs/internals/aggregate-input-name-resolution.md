# Aggregate input name resolution

Source: internal/planner/physical/group_key_binding.go — resolveAggInputName, moved 2026-09-11 (#1026)

```go
// resolveAggInputName maps a name an aggregate stage READS — an aggregate
// argument, or a GROUP BY key — back to what the stage below it actually
// emits, following the SELECT-list renames of any Project in between.
//
// walkStages treats an ordinary Project as a passthrough: it emits no stage,
// so a subquery's rename never happens on the DAG. `SELECT MAX(n) FROM
// (SELECT o_custkey AS n FROM orders)` therefore dispatched a scan reading
// o_custkey and an aggregate asking for `n`, and exec.HashAggregate answers a
// column it cannot resolve with NULL — 1499 on the single-process path and on
// DuckDB, NULL on the DAG (#355). A renamed GROUP BY key is the louder half of
// the same defect: an unresolvable key serializes as a NULL key, so every row
// collapses into one NULL group.
//
// This is the aggregate's version of what resolveShuffleKey does for join keys
// and resolveSortKeyColumn for ORDER BY terms — the same root cause, patched
// per consumer because the passthrough is what all three share.
//
// Three outcomes:
//
//	name unchanged, alias false — not a rename; the name is whatever the
//	  child already emits, which is the overwhelmingly common case.
//	name rewritten, alias true — the Project renamed a plain column; the
//	  aggregate reads the source column instead.
//	expr non-nil, alias true — the Project computed an EXPRESSION under this
//	  name (`SELECT o_custkey * 2 AS n`). There is no column to read; the
//	  caller attaches it as the aggregate's derived InputExpr, which the
//	  worker projects before aggregating. exprInput is then the node that
//	  Project reads, which is what the expression's column references are
//	  written against — the caller types the expression there, because the
//	  Project's OWN output does not carry them and a polymorphic declaration
//	  (COALESCE, NULLIF, GREATEST, LEAST) falls back to Float64 without them
//	  and drops every string (#333).
//
// It stops at an Aggregate: that node's outputs are its own GroupBy and
// OutputCol names, which the parent reads directly, and descending past it
// would resolve a name against the wrong schema.
```
