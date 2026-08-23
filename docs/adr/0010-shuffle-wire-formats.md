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
- **The partition ASSIGNMENT is part of the exchange contract, not just the
  byte layout.** Every producer of a repartition stage must map a key to the
  same partition number, because the consumer of partition *p* reads only the
  files named *p* and assumes every row with such a key is in them. A binary
  that hashes a key differently from its peers does not fail: it writes the
  row to a partition nobody looks in for it, and the join or aggregate comes
  back short. `hashRowsIntoPartitions`
  (`internal/worker/partitioned_shuffle_sink.go`) is that function, and it
  changed in the #397 wave — BOOL, DECIMAL and VECTOR keys hash their bytes
  where they previously mixed a constant `0x00`. **So a cluster's coordinator
  and workers are deployed WHOLESALE, never rolling**: a stage whose tasks run
  a mix of binaries across a change to this function answers wrong, silently.
  The same rule covers any future change to the hash, the partition count
  derivation, or the WSHF field order.

References: `docs/design/peer-wire-compression.md`,
`docs/design/exchange-streaming-consumption.md`.
