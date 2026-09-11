# Aggregate argument declaration scope

Source: internal/planner/physical/aggregate_declared_output.go — aggInputColumnType / aggInputColumnDecimal, moved 2026-09-11 (#1026)

```go
// aggInputColumnType and aggInputColumnDecimal answer "what does this
// aggregate's bare column argument declare" for the two halves of a
// declaration, and they answer it against the aggregate's OWN INPUT before
// falling back to the scans below it.
//
// scanColumnType/scanColumnDecimal search every Scan beneath a node for a
// column of that NAME, which cannot see a RENAME: over
// `SUM(v) FROM (SELECT dw AS v FROM decw) x` the scan carries `dw` and
// nothing carries `v`, so the aggregate's output declared FLOAT64 while the
// accumulator held an exact Int128 — and `SUM(v * 2)`, which is that
// declaration times two in the projection ABOVE the aggregate, came back
// -1.7283950641728393e+19 where the same query spelled over the base column
// answers -17283950641728394664.17283948 (#728). Two spellings of one
// question, two numbers, on the single-process path.
//
// emittedColDecls is the walk that CROSSES a rename or derived-table Project
// — the same walk buildAggregate already uses for a COMPUTED argument
// (aggInputDecls) and declaredOutputSchema uses for the SELECT list — so the
// aggregate's input, its output and the projection above it now read one map.
// It is consulted FIRST rather than as a fallback because where the two
// disagree the emitted walk is the right one: a Project is free to bind a
// name to a different column than the scan of that name below it
// (`SELECT dw AS other, k AS dw`), and the scan walk would answer for the
// column the query is NOT reading.
```
