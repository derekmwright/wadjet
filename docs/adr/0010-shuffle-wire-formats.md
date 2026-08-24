# ADR-0010: WSHF/WSHC/WSHZ shuffle wire formats and where compression happens

Status: Accepted (compress-on-serve landed PR #265, 2026-07-24; WSHZ zstd
envelope landed `6d3082c`, 2026-08-14; the single read side landed as
`internal/wshf`, #422, 2026-08-23)

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
- **WSHZ**: `"WSHZ"` magic + a zstd stream of the WSHF bytes. The
  S3-upload-only envelope (`docs/design/exchange-zstd-wire.md`):
  `WADJET_EXCHANGE_ZSTD=1` (default off) trades s2's cheaper decode for
  zstd's better ratio on stage/shuffle PUTs, where ADR-0007 already
  measured the decode as near-never-paid. Peer wire stays s2-or-raw
  (`--peer-wire-compression`; decode sits on the probe critical path
  there) and on-disk stays raw WSHF — WSHZ never appears in either
  place. Sniffed by the same four-byte dispatch as WSHF/WSHC.
- **Compression points** (s2 everywhere, ~20% on TPC-H shuffle data,
  with a ≥10% savings heuristic where staging allows):
  - S3 uploads: compress at upload (skipped entirely under lazy
    durability, ADR-0007); WSHZ is an alternative codec at this same
    point, gated by the flag above.
  - Peer streams: `--peer-wire-compression` compresses on serve —
    the file stays raw on disk (local mmaps pay no decode) and the wire
    carries WSHC. Default off pending its SF100 A/B; the consumer's
    `peer_bytes` ledger counts wire bytes, so the metric is built in.
  - On-disk stays raw WSHF: disk is NVMe-cheap, local reads dominate on
    1:1 chains (ADR-0008), and write-path compression would sit on the
    shuffle finalize critical path.
- Retries overwrite identical keys (same task ID → same key), which is
  what makes every cache/tier safe without validation metadata.
- **`internal/wshf` owns the read side — all of it.** The three magics,
  the envelope `Codec`/`CodecForMagic`, and the one bounds-checked decoder
  (`Cursor`, `ReadColumn`, `ChunkReader`, `DecodeBatches`, `Decompress`)
  live there, imported by both the coordinator and the worker. Before
  #422 the coordinator and the worker each hand-walked the payload with
  their own unchecked copy, and the copies had already drifted (the
  coordinator understood only the WSHC envelope, not WSHZ). `wshf.Decompress`
  unwraps WSHC and WSHZ alike, so every consumer can sniff-and-decode any
  of the three magics through the same call. The WRITER stays in
  `internal/worker` (`shuffle_format.go` — it needs the engine's batch
  gather and view resolution), so the format has exactly one writer
  package and one reader package, never a reader per consumer.

## Consequences

- One format sniff covers every read path; new tiers get compression
  support for free.
- CPU-for-bytes trades are per-edge policy flags, not format forks.
- s2 was measured wall-neutral for background encode
  (`docs/design/s2-shuffle-encode.md` refutation memo, 2026-07-19) —
  the win case is wire bytes, never encode overlap.
- **One reader, fuzzed.** `internal/wshf`'s decode path is the only place
  a WSHF payload is interpreted, and `FuzzWSHFDecode`
  (`internal/worker/shuffle_format_fuzz_test.go`) drives it with every
  truncation of real multi-type, container and compressed payloads. A
  truncated or hostile payload is an error returned up the call stack,
  never a panic — `Cursor` bounds-checks every read instead of using a
  length field straight off the wire as a slice index. Both the
  coordinator's inline/result reads and the worker's shuffle reads
  (file, stream, pread) share this one decoder, so they cannot diverge
  on what a payload means, and a defect fixed once is fixed on every
  tier at once.
- **The partition ASSIGNMENT is part of the exchange contract, not just the
  byte layout.** Every producer of a repartition stage must map a key to the
  same partition number, because the consumer of partition *p* reads only the
  files named *p* and assumes every row with such a key is in them. A binary
  that hashes a key differently from its peers does not fail: it writes the
  row to a partition nobody looks in for it, and the join or aggregate comes
  back short. `hashRowsIntoPartitions`
  (`internal/worker/partitioned_shuffle_sink.go`) is that function, and it
  changed in the #397 wave — BOOL, DECIMAL and VECTOR keys hash their bytes
  where they previously mixed a constant `0x00` — and again in the #459/#474
  wave (2026-08-24): the FLOAT32/FLOAT64 and VECTOR arms now hash the
  CANONICAL bits (`kernel.KeyFloat32Bits`/`KeyFloat64Bits` — NaN payloads and
  -0.0 folded, matching PostgreSQL's float order, ADR-0012 item 8) instead of
  the raw IEEE754 bits, and the DECIMAL arm hashes the canonical unscaled
  digits at the column's scale instead of a float64 cast, so a cross-scale
  DECIMAL join co-partitions and a `{-0.0, 0.0}`/NaN GROUP BY does not split
  across the shuffle. **So a cluster's coordinator and workers are deployed
  WHOLESALE, never rolling**: a stage whose tasks run a mix of binaries
  across a change to this function answers wrong, silently. The same rule
  covers any future change to the hash, the partition count derivation, or
  the WSHF field order.

References: `docs/design/peer-wire-compression.md`,
`docs/design/exchange-streaming-consumption.md`.
