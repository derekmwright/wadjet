# ADR-0019: A panic fails the query, not the server — and the gate is what keeps that honest

Status: Accepted (2026-08-24; §2 obligation principle added the same day after an adversarial review found four boundaries that recovered without discharging one)

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

A boundary must also not PANIC ITSELF. A recovery that runs after the dying
goroutine's own `defer close(ch)` and then sends on that channel raises an
unrecoverable panic from inside the deferred recovery; ordering the boundary
before those defers (registered last, so it runs first) is part of the
contract, not a detail.

### 2a. The obligation principle

**A boundary must discharge every obligation the dying goroutine owed, not
just the unit of work in flight.** This is the rule the first implementation
got wrong four separate ways, and every one of them replaced a crash with
something worse — a hang, a silent truncation, or a slow starve, none of which
any timeout or gate in the engine would have attributed back to the panic. A
crashed server at least restarts and says why.

The obligations a query goroutine can hold, and what a boundary owes for each:

| Obligation | What the boundary must do | What happened when it did not |
|---|---|---|
| A held **lock** | release it | `buildParallelKeyOnly` calls `source.Next` under `sourceMu`; recovering inside that critical section left 23 sibling morsel workers blocked on `sourceMu.Lock` forever and `wg.Wait` never returned. main crashed in 0.04s; the boundary hung, holding the memory budget and the client connection |
| **Queued work** it was the pool's share of | fail the rest, or hand it back | the scan prefetcher's `jobs` channel is pre-filled and closed; a worker that resolved only its in-flight index and exited abandoned the remainder, and once every worker had died that way `take()` blocked on slots nobody owned |
| An **unclosed channel** a consumer waits on | close it (or deliver a terminal error first) | the decode-ahead scanner, the aggregate emit closer and the partition-queue closer each own a `close`; recovering without it hangs the consumer |
| A **reservation, token or ledger charge** | release it | `CachedStore.readFully` `ForceReserve`s on a worker-LIFETIME tracker; a panic past the release left 67 MB charged forever, so every later query on that worker ran with a smaller budget |
| A **protocol reply** the caller is blocked on | send exactly the ones actually owed | pgwire: `handleQuery` owes its own ReadyForQuery, the extended-query handlers owe none. Sending one unconditionally emitted a spurious Z that the client consumed as the NEXT query's answer — zero rows, no error |

The obligation is a property of the SITE, so it is recorded per site in
`docs/internals/native-dag-execution.md` §The query panic boundary rather than
left to be rediscovered.

Two rules fall out of the table and are worth stating separately:

- **Cancel is not a substitute for delivery.** The prefetcher's boundary
  deliberately does not cancel: `p.cancel` reaches only the prefetcher's child
  context while `take()` waits on the caller's, so cancelling would make
  healthy siblings abandon indices they had already received and reintroduce
  the hang from the other side. It drains and fails instead.
- **Do it once.** A boundary that delivers a result the body already delivered
  desynchronises the consumer just as badly as one that delivers none, so
  every site tracks what it has actually done (`sent`, `inFlight`, `closed`,
  `returned`) rather than assuming.

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
  omission with a named helper to point at — and the review question is not
  "does it recover" but "what does it owe".
- A recovered panic is **not retryable**. It is deterministic, so the stage
  retrier marks it terminal on the first failure instead of spending the
  budget re-running it (and re-running whatever it damaged).
- The panic message crosses the distributed wire as a bare string, losing its
  type and its SQLSTATE, so `queryPanicPrefix` is load-bearing: the retry
  decision and the XX000 the client is handed both key off it
  (`exec.IsQueryPanicMessage`).
- The **stack stays in the log**. It is logged at error level with the query
  id where an operator looks for it, and never travels in a result — a task
  error string reaches the SQL client, and 8 KB of Go frames in a psql ERROR
  line helps nobody.
- **Reopening this requires new evidence.** "A blanket recover hides bugs" is
  the argument this ADR answers: it hides nothing that the counter and the two
  gates do not surface, and the alternative was measured in dead servers.
