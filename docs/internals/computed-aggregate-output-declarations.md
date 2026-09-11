# Computed aggregate output declarations

Source: internal/planner/physical/aggregate_declared_output.go — aggOutputFromInputDecl, moved 2026-09-11 (#1026)

```go
// aggOutputFromInputDecl is aggSpecOutputType/aggSpecOutputDecimal's rule for a
// COMPUTED argument: the aggregate's declared output, derived from the
// declaration its INPUT PROJECTION is built from.
//
// It exists because an aggregate over a computed argument had no declared
// output at all — aggSpecOutputType declines a non-bare ColRef and falls to
// aggOutputType's float64 — while the partials that saw a row emitted whatever
// the projected vector actually was. On the stage DAG those are the same file
// set: a partial whose filter matched nothing writes the identity row under the
// float64 default, its siblings write DECIMAL, and one stage's files then
// describe two different relations. Before #685's reader guard that was a
// silent 10^scale on SUM(a * (1 - b)) — the TPC-H revenue shape — and after it,
// a refused read. Neither is an answer.
//
// The derivation is BY CONSTRUCTION rather than by inference, which is what
// makes it total: the worker builds the pre-aggregate projection from
// AggSpec.InputType/InputPrecision/InputScale (worker.buildAggInputProjection),
// so the vector every non-empty partial observes IS this declaration. Reading
// the output off the same triple means the identity row and its siblings agree
// whatever the triple says — including when it is the float64 fallback for an
// expression nothing could type.
//
// ok=false only for a function whose output does not follow its input at all;
// those keep aggOutputType's answer, which is already input-independent.
// wideInt says the computed integer argument provably carries an int8-domain
// operand (aggInputIsWideInteger). It is what lets this function apply
// PostgreSQL's by-WIDTH SUM rule to an expression whose declared TypeID this
// engine has already widened to INT64.
```
