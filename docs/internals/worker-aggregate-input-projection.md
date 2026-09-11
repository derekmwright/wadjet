# Worker aggregate input projection

Source: internal/worker/filter_compile.go — func buildAggInputProjection(, moved 2026-09-11 (#1026)
Superseded: Derived GROUP BY keys can also require a Project even when no aggregate carries InputExpr; the nil condition is no derived work.

buildAggInputProjection returns a Project operator that materializes
each aggregate's derived input expression into a named column that
HashAggregate can look up by AggSpec.InputCol. Pass-through columns
(GROUP BY keys, filter references, bare-column aggregate inputs) are
included via DirectCopy so the output batch contains everything the
downstream aggregate or filter needs.

Returns (nil, nil) when no aggregate has a derived InputExpr — the
caller skips inserting a Project in that case.

The referenced-columns list is the union of all bare columns each
derived expression reads; callers extend the source projection hint
with these so parquet readers don't prune them.
groupByTypes is the plan-time type of each derived key, keyed by its
exact GroupByCols text (OpSpec.GroupByTypes), and groupByDecimal carries
the (p,s) of its DECIMAL entries. Together they override the
schema-blind ProjectionOutputType inference below, which has no catalog
and typed COALESCE(l_extendedprice, 0) Int64 from the literal alone —
truncating every float group key on write (#379). Absent entries (bare
keys, older coordinators) keep the inference.
keys, when non-nil, is the planner's own two-name answer (OpSpec.
GroupByResolve) and REPLACES the text-parsing recovery below: it says which
keys this fragment materializes, which slot each one lands in, and what the
aggregate publishes them as. nil is an older coordinator.
