# ADR-0005: NATS control plane, gRPC data plane

Status: Accepted (landed 2026-05, b2fbb0e; recorded 2026-07-25)

## Context

Early builds pushed everything — task dispatch, results, gather
payloads, heartbeats — through NATS. Payload-size caps (the 8 MB gather
incident), head-of-line blocking between control messages and bulk data,
and lock contention under SF100 load made one bus for both planes
untenable.

## Decision

Split by traffic class:

- **NATS (control)**: heartbeats, query cancel/complete broadcasts,
  upload-complete and upload-release signals, JetStream task queues in
  NATS-only mode, KV for catalog metadata and small stage outputs.
- **gRPC (data)**: with `--data-plane=grpc`, task dispatch, results,
  gather payloads, and TaskProgress ride per-worker bidi streams;
  worker↔worker shuffle fetches ride the dedicated peer-exchange gRPC
  listener. HTTP/2 flow control is the backpressure story.
- Federation (leaf-node edge clusters) stays a NATS-topology concern.

## Consequences

- Bulk bytes can never starve control signals; payload caps stopped
  being correctness hazards.
- Two transports to operate. Accepted: the SG/ports story is small and
  the NATS-only path remains as a degraded mode.
- Targeted gRPC dispatch has no work stealing — a saturated worker's
  queue stays its own. That property is why placement policy (ADR-0008)
  carries anti-clump obligations.
