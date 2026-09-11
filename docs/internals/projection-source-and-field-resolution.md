# Projection source and field resolution

Source: internal/engine/exec/project.go — projSourceName / resolvePlainColumn, moved 2026-09-11 (#1026)

resolvePlainColumn resolves a projected plain-column reference against the
input schema and reports whether it is servable at all.

idx >= 0 is a bulk-copy source. idx == -1 with ok=true means the name is
reachable only through per-row evaluation (a ROW field like
"attrs.score"). ok=false means the name resolves to NOTHING, which the
caller turns into an error rather than an all-NULL column (issue #147).

The plain-column lookup is columnIndexFallback — the same bidirectional
qualified↔bare resolution every other operator uses. A join emits the
self-joined table copy that lands on the PROBE side under its bare name
while qualifying the build side ("n_name" + "n2.n_name"), so a downstream
reference to "n1.n_name" only resolves after the qualifier strip. Without
it the projection fell to the per-row ColumnRef path, which misses the
same way and silently emits NULLs (#314: Q07's supp_nation). The output
TYPE for such a rename already resolved through the same fallback in the
schema pass above; this makes the value path agree with it.

The ROW-parent check runs BEFORE the fallback so "attrs.score" keeps
extracting the ROW field even when an unrelated bare "score" column is
also in scope.
projSourceName is the input spelling a projection reads its value from,
resolved in the same order the schema pass resolves its source index:
the bulk-copy name, then the rename's source, then the output's own name
for a projection that is not computed.
