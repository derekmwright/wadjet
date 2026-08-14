# Exchange zstd-on-wire (barrier-overlap arc, step 3)

Status: KICKOFF — premise measured 2026-08-14, implementation pending.
Companion benchmark: `internal/worker/shuffle_codec_bench_test.go`
(`BenchmarkWSHCCompressionCodecs` / `BenchmarkWSHCDecompressionCodecs`).

## 1. Premise

Outbound ENA throttle is byte-driven and join-window-concentrated
(step-0 attribution: 68% of out-exc in join-stage windows; upload
pacing at 150 MB/s already cut out-exc −30% at EQUAL bytes). The
byte-economy half is untouched: S3 stage/shuffle uploads carry WSHC =
s2, chosen when encode CPU was scarce. CPU is now ~80% idle and
ADR-0007 measured stage-output PUTs as ~zero-read-back insurance, so
a slower, stronger codec's decode cost is almost never paid and its
encode cost lands in idle background CPU (compression-source is
unpaced; s2 already "runs ahead" of the paced PUT-body reader).

## 2. Measured (2026-08-14, real TPC-H lineitem distributions, SF0.01
## proxy payload ~8.6 MB WSHF, x86 dev box, single-thread)

| codec        | ratio | encode MB/s | decode MB/s |
|--------------|-------|-------------|-------------|
| s2 (today)   | 0.487 | 2565        | 1346        |
| zstd-fastest | 0.323 | 299         | 787         |
| zstd-default | 0.324 | 262         | —           |
| zstd-better  | 0.313 | 188         | —           |

- **zstd-fastest is the only sensible level**: default buys 0.1pp,
  better buys 1pp for 1.6× more CPU. Fastest cuts wire bytes ~34%
  RELATIVE to s2 (0.323/0.487 = 0.66) at 8.6× encode CPU.
- **Caveat carried to the A/B**: ADR-0010 quotes s2 at ~20% savings
  on real SF100 exchange bytes; this proxy shows 51%. Absolute
  savings will differ on real payloads (wider post-projection
  schemas, more float entropy); the RELATIVE s2→zstd gap is the
  robust part. The EC2 pair judges bytes, not this table.
- Graviton absolute speeds will be ~2× lower; ordering holds. Worker
  PUT rate averages 60–90 MB/s (paced ceiling 150), so one core of
  zstd-fastest encode (~150+ MB/s on c7gd) keeps up; zstd's encoder
  concurrency can add a second core if drain-backlog appears.

## 3. Scope decision: S3 uploads ONLY

- Peer wire stays s2 (`--peer-wire-compression`, SF100-validated
  default-on): peer streams ARE consumed, on the probe critical
  path, where zstd's 1.7× slower decode is a real tax.
- On-disk stays raw WSHF (ADR-0010 unchanged).
- Envelope: new magic `WSHZ` = zstd stream of WSHF bytes, sniffed
  everywhere WSHC is. Consumers keep decoding WSHC (peer wire, old
  objects); WSHZ appears only on S3-uploaded objects when the flag
  is on. Mixed-version note: old binaries cannot decode WSHZ — bench
  clusters are single-version, flag default OFF regardless.

## 4. Implementation map (all in internal/worker unless noted)

- `shuffle_format.go`: `zstdMagic = "WSHZ"`; codec parameter (or
  env-selected package var) in `CompressShuffleData` /
  `compressShuffleStream`; zstd branches in `DecompressShuffleData`,
  `streamDecompressShuffle`, `isShuffleFormat`; pooled zstd
  encoder/decoder alongside the s2 pools (klauspost zstd decoders
  are goroutine-bound — pool them like the s2 readers, do NOT share).
- `shuffle_stream_reader.go`: three-way magic branch (WSHF/WSHC/WSHZ),
  pooled zstd reader parallel to `s2r`.
- `stream_source.go`: the `wshc bool` threaded through
  `openShuffleInMemory` / `openShuffleStreaming` / staged paths
  becomes a codec enum (3 sniff sites + `DecompressShuffleData` at
  ~1616).
- `scan_prefetch.go` transcode + `executor.go` ~1780 decode: same
  enum.
- `peer_exchange.go` payload validation: accept WSHZ magic as valid
  shuffle format (a consumer may re-serve an S3-restaged file).
- Knob: `WADJET_EXCHANGE_ZSTD=1` (default off) worker-side, tf var
  `exchange_zstd` exported in worker user_data; encode level pinned
  fastest, no level knob (see §2).
- Upload QoS/pacing interaction: none — compression still happens
  at/before the governed reader exactly as s2 does today.

## 5. Validation plan

- Round-trip unit tests mirroring `TestShuffleCompressedRoundTrip`
  for WSHZ (+ streamed file variant, + sniff detection, + mixed
  WSHC/WSHZ objects in one query).
- Race on touched packages; full worker+coordinator suites.
- Harness `--mode=local` BOTH flag states (coordinator/shuffle-path
  rule) with engagement proof (grep worker logs for WSHZ uploads).
- EC2 A/B (needs Derek's approval): control flag-off vs treatment
  flag-on, adjacent arms, runs=2, judged on rows+vsig; ENA out-exc
  totals at equal logical bytes (expect −25–35% wire bytes on stage
  PUTs if §2's relative ratio holds); watch drain-backlog (encode
  keeping up) and pace_wait deltas; R3/R4 steady walls primary
  (multi-suite runs are cheap post-stall-fix).
