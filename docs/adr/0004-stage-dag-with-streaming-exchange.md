# ADR-0004: Stage-DAG execution — durable materialization with a streaming-exchange overlay

Status: Accepted (streaming exchange default-on 2026-07-02; ledger-validated 2026-07-22)

## Context

Distributed queries need an exchange model. Trino frames the spectrum:
streaming exchange (fast, a lost worker kills the query) vs
fault-tolerant execution with spooled exchanges (durable, slower). We
also have a small-query regime where any per-stage round trip dominates.

## Decision

Three-layer design:

1. **Structure**: every distributed query runs as a stage DAG whose
   outputs are addressed as durable S3 objects (`queries/<id>/...`) —
   the FTE-shaped skeleton. Task retries are idempotent overwrites of
   the same keys.
2. **Overlay**: with `--streaming-exchange` (default on), consumers
   actually read stage outputs from the producing worker's local disk —
   same-worker mmap, then peer gRPC, then NATS KV for small payloads —
   with S3 as the last-resort tier. Producer death before a durable copy
   exists degrades to a one-shot whole-query re-execution with streaming
   disabled.
3. **Escape hatch**: queries under `--local-fastpath-bytes` (64 MiB
   post-pruning) skip the DAG entirely and run in-process.

## Consequences

- Measured ground truth (SF100 suite pair, shuffle-io ledger,
  2026-07-23): shuffle reads are served 100% by the local/peer/KV tiers
  — **zero S3 read-back** in healthy operation. The S3 layer is purely
  the fault-tolerance and addressing substrate.
- That finding killed two once-plausible directions: a consumer-side
  NVMe shuffle-read cache (nothing is re-read), and treating S3 GET cost
  as a shuffle optimization target. It also enabled ADR-0007.
- Fault tolerance is coarse (whole-query re-run, not per-task) whenever
  the durable copy hasn't landed; classifyFatalResult detects exactly
  that case. Accepted: re-runs are rare and bounded to one.

References: `docs/design/streaming-exchange.md`,
`docs/design/exchange-streaming-consumption.md`,
`docs/internals/native-dag-execution.md`.
