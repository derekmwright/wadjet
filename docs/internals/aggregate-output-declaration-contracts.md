# Aggregate output declaration contracts

Source: internal/planner/physical/aggregate_declared_output.go — aggOhlcvOutputFields / aggSpecOutputType, moved 2026-09-11 (#1026)

```go
// aggSpecOutputType declares the output type of one aggregate over the
// subtree rooted at the Aggregate node that owns it.
//
// COUNT is input-independent in this engine, so aggOutputType alone is exact
// for it, and it is the same declaration the single-process pipeline compiles
// into exec.AggColumn.OutputType. SUM and AVG are input-independent for every
// type but DECIMAL, over which they answer in DECIMAL (#455).
//
// MIN/MAX are the exception, and MIN_BY/MAX_BY with them: their output IS
// their (first) input's type, which exec.HashAggregate resolves from the
// vector it observes at Consume. To declare the same thing at plan time the
// input column has to resolve to a catalog type, so this walks the
// aggregate's inputs for it and returns 0 — undeclared — when it cannot: a
// derived-expression argument, a column no scan below carries, or two scans
// carrying it at different types.
// ok=false is the undeclared answer; callers fall back to the
// function-name derivation. It is returned as a second value rather than
// as a zero TypeID because TypeBool IS zero: MIN_BY over a BOOL column
// declares BOOL, and a caller reading that as "undeclared" is how a
// declaration goes missing on exactly one path (#354, #371).
// aggOhlcvOutputFields declares a bar's ROW fields from the PRICE and VOLUME
// column declarations, through exec.OhlcvOutputFields — the same function the
// operator asks at Consume, so the declaration and the value are one decision.
//
// ok=false when either input is not a bare column this subtree can type (a
// computed argument, a name no scan below carries, two scans disagreeing).
// The operator then re-derives the list from the vectors it reads, which is
// exactly what aggSpecOutputType's "unresolved" answer does for MIN/MAX.
```
