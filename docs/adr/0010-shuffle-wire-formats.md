# ADR-0010: WSHF/WSHC shuffle formats and where compression happens

Status: Accepted (compress-on-serve landed PR #265, 2026-07-24)

## Context

Exchange data needs one on-disk/on-wire format family that every tier —
local mmap, peer stream, KV payload, S3 object — can serve and every
consumer can sniff and decode, including mid-stream.

## Decision

- **WSHF**: the raw chunked columnar shuffle format; what sinks write to
  disk and local consumers mmap.
- **WSHC**: `"WSHC"` magic + an s2 stream of the WSHF bytes; the one
  compressed envelope, decodable streaming. Consumers sniff four bytes
  and handle either, from any tier.
- **Compression points** (s2 everywhere, ~20% on TPC-H shuffle data,
  with a ≥10% savings heuristic where staging allows):
  - S3 uploads: compress at upload (skipped entirely under lazy
    durability, ADR-0007).
  - Peer streams: `--peer-wire-compression` compresses on serve —
    the file stays raw on disk (local mmaps pay no decode) and the wire
    carries WSHC. Default off pending its SF100 A/B; the consumer's
    `peer_bytes` ledger counts wire bytes, so the metric is built in.
  - On-disk stays raw WSHF: disk is NVMe-cheap, local reads dominate on
    1:1 chains (ADR-0008), and write-path compression would sit on the
    shuffle finalize critical path.
- Retries overwrite identical keys (same task ID → same key), which is
  what makes every cache/tier safe without validation metadata.

## Consequences

- One format sniff covers every read path; new tiers get compression
  support for free.
- CPU-for-bytes trades are per-edge policy flags, not format forks.
- s2 was measured wall-neutral for background encode
  (`docs/design/s2-shuffle-encode.md` refutation memo, 2026-07-19) —
  the win case is wire bytes, never encode overlap.

References: `docs/design/peer-wire-compression.md`,
`docs/design/exchange-streaming-consumption.md`.
