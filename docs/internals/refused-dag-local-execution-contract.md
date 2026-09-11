# Refused dag local execution contract

Source: internal/coordinator/refused_local.go — runRefusedLocal, moved 2026-09-11 (#1026)

runRefusedLocal executes a query the stage DAG REFUSED — it produced no
plan at all — on the coordinator-local single-process pipeline.

This is a ROUTE, not a fallback: there is no second engine to retry on, so
every failure here is the query's outcome and is reported to the client.
That is the whole point of a typed refusal. Before #359 a per-row
correlated scalar answered 0 on the DAG; before #466 a DISTINCT the DAG
had no stage for was dropped and the raw pre-dedup rows came back. Both
are wrong answers, and a wrong answer is strictly worse than an error —
but an error is also worse than the right answer, which the local pipeline
can produce for both classes.

Unlike tryLocalFastPath there is no byte-threshold gate: a refused plan is
often unestimable and the alternative is refusing outright. Capacity is
still bounded — the pipeline runs under the same memory budget (pipeline
breakers spill past it) and the collect sink's result budget, both derived
from the fast-path threshold or its default when the fast path is
disabled. Past those bounds the query FAILS with the budget error rather
than degrading the coordinator; a deployment that needs larger results
raises --local-fastpath-bytes.

what names the construct in operator-facing messages; counter is the
per-class route counter a distributed suite asserts engagement on.
