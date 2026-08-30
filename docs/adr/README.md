# Architecture Decision Records

Short, immutable records of the architectural decisions this codebase has
committed to: the decision, the alternatives it beat, and what we accept
in consequence. Deep design detail lives in the linked `docs/design/`
memos — an ADR is the index entry that says *what we chose and why it
stuck*, so nobody relitigates a settled question without new evidence.

Conventions:

- Numbered, never renumbered. Superseding a decision means a new ADR that
  names the old one; the old record's status flips to `Superseded by
  ADR-NNNN`.
- Status is one of `Accepted`, `Superseded`, `Deprecated`.
- Every claim of measured behavior cites the run/date it came from.
  Refuted premises and honest failure findings belong in the record —
  they are why the decision is trustworthy.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-push-based-vectorized-execution.md) | Push-based vectorized execution with selection vectors | Accepted |
| [0003](0003-recursive-descent-parser.md) | Hand-written recursive-descent SQL parser | Accepted |
| [0004](0004-stage-dag-with-streaming-exchange.md) | Stage-DAG execution: durable materialization with a streaming-exchange overlay | Accepted |
| [0005](0005-split-control-and-data-plane.md) | NATS control plane, gRPC data plane | Accepted |
| [0006](0006-never-oom-memory-model.md) | Never-OOM memory: shared pool, ownership ledger, spill-everywhere | Accepted |
| [0007](0007-shuffle-durability-policy.md) | Shuffle durability is a policy spectrum; eager stays the default | Accepted |
| [0008](0008-task-placement-policy.md) | Task placement: eager reservation → cache affinity → input locality → memory binpack → round-robin, under a same-batch anti-clump cap | Accepted |
| [0009](0009-worker-scratch-lifecycle.md) | Worker scratch lifecycle: adopt-into-cache, paced asynchronous purge | Accepted |
| [0010](0010-shuffle-wire-formats.md) | WSHF/WSHC shuffle formats and where compression happens | Accepted |
| [0011](0011-performance-measurement-methodology.md) | Performance measurement methodology at SF100 | Accepted |
| [0012](0012-sql-semantics-authority.md) | PostgreSQL is the SQL semantics authority; DuckDB is the performance goal and an oracle | Accepted |
| [0013](0013-correctness-gates-and-their-boundaries.md) | The correctness gates, and what they deliberately do not gate (amended 2026-08-23: type-matrix gates + a per-issue, not per-pin, ratchet) | Accepted |
| [0014](0014-group-index-layout-at-construction.md) | Group-index layout is decided at sink construction, not by runtime conversion | Accepted |
| [0015](0015-decode-ahead-is-an-admission-class.md) | Decode-ahead is a CPU-token admission class, not a `TryAcquire` client behind the consumer FIFO | Accepted |
| [0016](0016-detach-is-the-ownership-claim.md) | `Detach` is the ownership claim; producers may reuse vector backing while a batch is unclaimed (amended 2026-08-22: scan output needs release + claim) | Accepted |
| [0017](0017-stage-sinks-copy-outside-the-lock.md) | Stage sinks copy outside the lock; the lock covers handoff only | Accepted |
| [0018](0018-parquet-file-numbers-are-input.md) | A parquet file's own numbers are input, not fact | Accepted |
| [0019](0019-query-scoped-panic-boundary.md) | A panic fails the query, not the server — and the gate is what keeps that honest | Accepted |
| [0020](0020-drop-table-reclaim-is-opt-in.md) | DROP TABLE's physical reclaim is guarded and opt-in | Accepted |
| [0021](0021-subquery-name-resolution-and-set-materialization.md) | A decorrelated subquery's names are resolved from the plan, and the sets it cannot join are materialized | Accepted |
| [0022](0022-a-row-field-path-is-not-a-column-reference.md) | A ROW field path is not a column reference: it is resolved from its parent's declaration and materialized like a computed expression | Accepted |
| [0023](0023-group-key-and-group-value-are-two-encodings.md) | A group's KEY and its VALUE are two encodings; never decode one out of the other | Accepted |
| [0024](0024-decimal-is-finite-fixed-point-with-postgres-result-types.md) | DECIMAL is a finite 128-bit fixed-point type that follows PostgreSQL's result-TYPE rules | Accepted |
| [0025](0025-a-stage-never-carries-what-its-fragment-will-not-run.md) | A stage never carries a predicate or a projection its fragment will not run | Accepted |
| [0026](0026-a-group-key-has-one-identity-and-one-name.md) | A GROUP BY key has one identity and one published name | Accepted |
