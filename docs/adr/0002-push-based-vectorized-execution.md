# ADR-0002: Push-based vectorized execution with selection vectors

Status: Accepted (long-standing; recorded 2026-07-25)

## Context

The engine executes analytics over columnar batches. The classic
alternatives: tuple-at-a-time Volcano iterators (simple, slow), fully
compiled queries (fast, huge complexity budget), or vectorized batches
with either pull or push control flow.

## Decision

- **Batches of 2048 rows** (`batch.DefaultBatchSize`), columnar layout,
  typed vectors; do not change the batch size without benchmarks.
- **Push-based pipelines**: `Source → [UnaryOperator...] → Sink`;
  pipeline breakers (Aggregate, Sort, Window) are Sink+Source pairs.
- **Selection vectors over copying**: filters mark `batch.Sel` indices;
  rows are not materialized until an operator genuinely needs a gather
  (see `docs/design/late-materialization.md` for how far that defers).
- **Typed kernels**: resolve types once per batch/column and dispatch to
  a typed function; no per-row type switches in hot paths.
- **No SIMD intrinsics**: the Go compiler's autovectorization is the
  ceiling we accept; intrinsics would fork the kernel set per
  architecture for gains the profile has not justified.
- **Batch pooling** (`BatchPool`) for zero-alloc reuse.

## Consequences

- Operators are simple to write and compose; performance work
  concentrates in kernels and IO, where the profiles say it belongs.
- Blocking operators must implement spill paths (ADR-0006) since a push
  model cannot backpressure through a breaker.
- The 2048-row batch is a contract: memory accounting, shuffle chunking
  (ADR-0010), and morsel scheduling are all sized against it.
