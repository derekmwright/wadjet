# ADR-0019: A panic fails the query, not the server — and the gate is what keeps that honest

Status: Accepted (2026-08-24)

## Context

Expression evaluation has no error return: `Expr.Eval` yields a value and
nothing else. The one condition that cannot answer with a value and must not
answer with NULL — an invalid cast, a division by zero, a correlated
subquery whose outer column is not in the batch (#347) — raises a panic
carrying a `FatalEvalPanic`, and the pipeline drivers convert it back into an
error. `recoverFatalEval` implements exactly that: convert the designed class,
**re-panic everything else**.

The re-panic was deliberate. A blanket catch would turn every engine bug into
a quiet error return, and ADR-0012's philosophy is that a wrong answer must be
loud. The reasoning was sound about visibility and wrong about cost.

An unrecovered panic on *any* goroutine terminates the whole Go program. So
"re-panic the rest" did not mean "the bug stays visible"; it meant **one
connection's index-out-of-range ends every other connection's query too**. The
SQLancer soak (wadjet#289) found three independent instances in under two
minutes of a 20-way run:

- #509 `CONCAT(text_col, int_col)` — no join, no error condition, ordinary
  single-table SQL, whole server down. Two working recovery points saw the
  panic, correctly recognised it was not a `FatalEvalPanic`, and correctly
  re-panicked it into a process exit.
- #510 a nil dereference in `joinFlushSource.Close`, same shape.
- #508 the hash-join build goroutine, which had no recovery at all, so even
  the *designed* class killed the process there.
- #512 the recovery machinery panicking inside itself: two workers storing
  differently-typed errors into one `atomic.Value`.

Patching those four does not close the class. The next reachable panic has the
same blast radius by construction.

## Decision

### 1. Every goroutine a query spawns converts any panic into that query's error

`exec.RecoverQueryPanic(ctx, where, r)` is the single conversion:

- a `FatalEvalPanic` keeps its own precise error and SQLSTATE — the designed
  contract, unchanged and still the first thing checked;
- anything else becomes a `*exec.QueryPanic`: SQLSTATE **XX000**, the panic
  value in the message, a truncated stack and the query id logged at error
  level.

`exec.CatchQueryPanic` is its deferred form for a goroutine that reports
somewhere other than a return value. The boundary sits at goroutine entry
points and driver edges — pipeline workers, join build, morsel parallelism,
spill and scan prefetchers, the aggregate emit drain, coordinator gather and
merge, the worker task executor, the pgwire message loop, the embedded API.

**The cost is one deferred call per goroutine.** Nothing per batch, nothing
per row. The two per-batch defers involved (`Pipeline.Run`, `ChainDriver.Push`)
already existed; only the function they call changed.

### 2. Nothing above the boundary re-panics

A boundary that re-raises is not a boundary. `recoverFatalEval` still exists
for the narrow conversion, but no goroutine entry point is allowed to let its
result escape as a panic — that is exactly what turned #509 and #510 from
query failures into outages.

### 3. Recovered panics are counted, and the count is gated

This is the half that makes the rest safe. Recovery would otherwise make new
panics **invisible**: the server no longer dies, so the process-killer gate
sees no dead child and passes. `exec.QueryPanicsRecovered()` counts every
conversion of the undesigned class, and two gates read it:

| Gate | Corpus | Fails on |
|---|---|---|
| `TestTypeMatrixNoProcessKillers` (`wadjet/type_matrix_crash_test.go`) | the 22-type matrix | a child that dies, OR an entry with a nonzero recovered-panic delta |
| `TestQueryPanicBoundaryHoldsForEveryShape` (`wadjet/panic_boundary_gate_test.go`) | query SHAPES from the soak | the same two, plus a shape whose declared client error stops appearing |

Both drive child processes and attribute a death to the entry that was
running, so a fatal that recovery cannot catch (a Go runtime fatal such as a
concurrent map write) still fails the gate. Both use pin maps (`tmPanicPins`,
`pbPins`) on ADR-0013's terms: an undeclared panic fails, and a pin that stops
firing fails too, because deleting it is the proof the fix landed.

The shape gate declares an expected client error where one is correct
(`CAST('x' AS INT)` must fail as 22P02), so a build that "survives" by
refusing everything fails the gate rather than passing it.

## Consequences

- A query that panics is still a defect and still fails CI. It just no longer
  bills every other connection for the diagnosis.
- The client gets a reportable internal error instead of `server closed the
  connection unexpectedly`, which is what PostgreSQL does with the same
  statement.
- Two tests that asserted the old policy now assert the new one, with the
  reversal recorded in both:
  `TestPipelineReportsOrdinaryPanicsAsInternalErrors` (exec) and
  `TestScanWorkerPanicReportsRealBugsAsInternalErrors` (physical).
- Adding a goroutine to the query path without a boundary is now a reviewable
  omission with a named helper to point at.
- **Reopening this requires new evidence.** "A blanket recover hides bugs" is
  the argument this ADR answers: it hides nothing that the counter and the two
  gates do not surface, and the alternative was measured in dead servers.
