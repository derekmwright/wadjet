# Worker derived group key compatibility

Source: internal/worker/filter_compile.go — func derivedGroupKeys(groupBy []string, aggs []distributed.AggSpec, filterCols []string,, moved 2026-09-11 (#1026)
Superseded: Stored columns may use the reserved-looking __gb_expr_N spelling, so allocation is seeded with bound names; the namespace alone is not a collision proof.

derivedGroupKeys splits a fragment's GROUP BY key list into the keys this
fragment must COMPUTE and the column each key is RESOLVED by.

It is the COMPATIBILITY path since ADR-0026's two-name carrier landed: a
spec that carries OpSpec.GroupByResolve says both names outright and nothing
is derived from text. See fragmentGroupKeyPlan.

A key is derived when parsing it yields anything but a bare column
reference, and also when it IS a bare reference the planner marked derived:
a ROW FIELD PATH (`c_row.b`) parses to a ColRef and names no column any
stage emits, so HashAggregate could not look it up and the key serialized
as NULL. groupByTypes is the planner's answer — derivedGroupKeyTypes
records an entry for exactly the keys that must be computed here, and a
bare column has none (#568).

slots is parallel to groupBy: a bare key resolves by its own name, and a
derived key by a hidden `__gb_expr_N` that no query can spell (the planner's
reserved namespace). Naming the computed column after the key's own text
instead put it in the user's namespace, where it shadowed — or was shadowed
by — an input column of the same spelling, differently on each engine
(ADR-0026).
