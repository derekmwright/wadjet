# ADR-0010: WSHF/WSHC/WSHZ shuffle wire formats and where compression happens

Status: Accepted (compress-on-serve landed PR #265, 2026-07-24; WSHZ zstd
envelope landed `6d3082c`, 2026-08-14; the single read side landed as
`internal/wshf`, #422, 2026-08-23; the wholesale-deploy rule extended to
answer-changing task-spec fields with `Task.DeleteMarkers`, #491, 2026-08-24)

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
- **A DECIMAL's SCALE is part of the file, not part of the chunk, and the
  writer enforces that.** The header declares the schema ONCE and every chunk
  after the first is read under it, so for a DECIMAL the header holds half the
  value: the chunk carries the unscaled integer and the header carries the
  scale. A task that concatenates two producers' batches at different scales
  therefore writes one file that means something other than what it was given
  — 127501 written at scale 4 and read at scale 2 is 1275.01 where the value
  is 12.7501. That was reachable from any cross-scale set operation until
  #533, silently, because the union stage's arms were reconciled by `TypeID`
  and two DECIMALs share one. `shuffleWriter.writeChunk` now refuses a chunk
  whose DECIMAL vector disagrees with the header.

  **That check covers the SINGLE-WRITER shape and only that shape**, and the
  distinction matters because the first draft of this bullet claimed it
  covered the whole class. It fires where ONE writer is handed batches at two
  scales — a gather task assembling both arms before writing. It does NOT fire
  where each arm is its own task writing its own internally-consistent file
  and the DOWNSTREAM stage reads several of them and takes the first header's
  scale: there is no writer at the point of reinterpretation. That is the
  ordinary union-stage shape, and the residual that survived in
  `reconcileSetOpArmTypes` — a join arm whose `(p,s)` the type walk dropped —
  was #551. It is closed two ways: the arm walk resolves a join's sides
  separately, under their qualified and derived-scope names
  (`physical.setOpArmDecls`), and an arm that STILL cannot be resolved — no
  `(p,s)`, or no type at all beside a DECIMAL sibling — is now REFUSED at plan
  time naming the column, rather than left as written for this reader to
  misread.

  The planner is where this is FIXED — `physical.reconcileSetOpArmTypes`
  coerces every arm to the set operation's output `(p,s)` before its rows enter
  the stream (ADR-0012 item 12). The writer check is a narrow second line, not
  a safety net for the general case, and reasoning that treats it as one will
  be wrong in exactly the direction that hurts.

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
- **And it covers a TASK-SPEC field whose absence changes the answer.** The
  wholesale rule was written for the exchange format, but the failure it
  guards against — a stage whose tasks run a mix of binaries answering
  differently, silently — is a property of the DECLARATION, not of the byte
  layout. `Task.DeleteMarkers` (#491, 2026-08-24) is the second entry in
  that wave list: it carries the merge-on-read DELETE state for the
  base-table files a task reads, and a worker that predates the field
  unmarshals it away and returns the deleted rows. On a rolling deploy that
  is *some* of a stage's tasks returning them — a partial, non-reproducible
  wrong answer, which is worse than all of them doing it. Same rule, same
  reason: coordinator and workers deploy together.

  The general test for whether an additive task field falls under this rule
  is whether a worker IGNORING it still answers correctly. `HasSortLimit`
  and `ColumnTypes` degrade conservatively (an old worker answers the way it
  always did); `DeleteMarkers` does not, because the field's whole purpose is
  to REMOVE rows.

References: `docs/design/peer-wire-compression.md`,
`docs/design/exchange-streaming-consumption.md`.
