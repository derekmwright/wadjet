# Peer-wire compression (`--peer-wire-compression`)

Status: implemented 2026-07-24. Default off pending SF100 validation.

## Motivation

The #261 ledger: 351.9 GB of SF100 exchange reads per suite pair arrive
over worker↔worker peer gRPC streams, and those streams carry **raw WSHF
file bytes** — compression has only ever existed on the S3 upload path
(`CompressShuffleFile`, s2, ~20% savings on this data), which the
shuffle-durability knob can now elide entirely. Meanwhile the consumer's
peer/streaming decode path already accepts WSHC (magic-sniffed in
`openShuffleFromPeer`), so the wire format for compressed peer streams
has existed all along — nothing produced it.

## Mechanism

`PeerServerConfig.CompressWire`: `FetchShuffle` sniffs the resolved
file's first four bytes.

- Raw WSHF → the stream carries a standard **WSHC envelope**: the magic,
  then the s2 stream of the raw payload, chunked into the usual 256 KiB
  frames (`chunkStreamWriter` adapts `stream.Send` to `io.Writer`).
- Anything else (already-WSHC, short, foreign) passes through
  byte-identical.

No client, consumer, or format changes: the consumer's existing WSHC
streaming decode handles the envelope, and its peer-tier ledger counts
the (now smaller) wire bytes — so the #261 `peer_bytes` metric measures
the win directly.

Cost: s2 encode on the producer, ~1 core-GB/s per stream, bounded by the
existing 16-stream serve cap. Under lazy durability the same CPU that
used to compress uploads is idle; under eager it is additive.

## Validation

- Unit: WSHC round-trip through a real server/client pair, pass-through
  for already-WSHC and short payloads, existing rejection paths.
- tpch-harness local (gRPC data plane, flag on): a true end-to-end —
  consumers decode compressed peer streams for every exchange; rows must
  match the reference.
- SF100 A/B (flag on vs off): `peer_bytes` should drop ~20% at equal
  rows; wall judged per the window-noise rule (mechanism metric first).
  Worth stacking with locality placement (fewer peer streams to compress)
  once that flips.
