# Outer join residual evaluation

Source: internal/planner/physical/join_residual.go — BuildJoinResidualFilter, moved 2026-09-11 (#1026)

```go
// BuildJoinResidualFilter compiles an outer join's ON-clause residual — every
// conjunct that is not an equi-join key pair — into a predicate over the
// COMBINED row: the probe row plus one candidate build row (#358).
//
// An outer join's ON runs BEFORE the NULL-padding, so this residual cannot be
// a filter above the join (that deletes the preserved rows) and cannot be
// pushed into a preserved side's scan (that deletes the rows the join owes
// unmatched). The executor evaluates it per key-matched candidate; see
// exec.HashJoin.Residual for the unmatched semantics it feeds.
//
// BuildSemiAntiFilter is not reusable here: it only expresses
// `probeCol OP buildCol` and ignores NULLs, while a residual must take
// literals (`r.r_regionkey < 3`), arithmetic (`n.x = r.y + 3`) and SQL
// three-valued logic (a residual evaluating to NULL rejects the candidate,
// but NOT of it must not accept). This is a small AST interpreter instead:
// per-row and boxed, which is acceptable for a capability the planner
// previously refused outright — no existing plan shape gains this code path.
//
// Column resolution against the two sides is by name, decided lazily on the
// first evaluated pair and cached: a qualified name is looked up in the probe
// then the build schema (self-join chains carry qualified columns); a
// qualifier equal to buildAlias forces the build side; otherwise the bare
// name resolves probe-first. Every one of those lookups is
// ResolveColumnIndex, not ColumnIndex: the names here are REFERENCES off the
// ON clause, so they arrive folded from the lexer (#731), while the batch
// carries the catalog's own spelling — `RegionName`, not `regionname`, for a
// parquet-registered table. A byte-exact probe misses every CamelCase column,
// and because an unresolvable column makes the residual UNKNOWN the failure
// mode is not a loud one: the join rejects every candidate and NULL-pads each
// preserved row, so a LEFT JOIN whose ON carries a residual answers all-NULL
// on the null-supplying side. The rule the resolver applies (fold only a
// reference that is itself folded; a delimited name stays byte-exact) is in
// internal/engine/batch/schema.go.
//
// The camel-case invariance battery does not yet distinguish this site — its
// residual conjuncts name `tier` and `counterid`, which the fixture spells
// folded, so the byte-exact probe happened to answer. Measured with every
// other fix in place and this one reverted, it owns 0 of the battery's cells;
// the change is the hazard closed, not a cell recovered. An unresolvable
// column still makes every evaluation UNKNOWN (candidate rejected) and logs
// once — the planner ships JoinFilter columns through NeededColumns, so a
// miss here is a plan bug, not user error.
//
// Returns nil when the expression contains a shape the interpreter does not
// support; the caller must then refuse the plan loudly rather than drop the
// conjunct.
```
