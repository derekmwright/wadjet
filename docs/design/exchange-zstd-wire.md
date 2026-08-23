# Exchange zstd-on-wire (barrier-overlap arc, step 3)

Status: VALIDATED 2026-08-14 (§6) — SF100 A/B green; pinned
`exchange_zstd=1` in sf100-distributed.tfvars. Engine default stays
env-off (`WADJET_EXCHANGE_ZSTD` unset/0 = s2). Engagement marker:
`wshz_files`/`wshz_bytes` in the "shuffle io stats" line. Harness
local gate green both flag states (treatment: wshz_files=32/48,
upload_failed=0, all queries green vs baseline).
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

- `shuffle_format.go`: `zstdMagic = wshf.MagicWSHZ` (the magic itself
  now lives in `internal/wshf/wshf.go`, sniffed via `wshf.CodecForMagic`
  rather than a package-local `isShuffleFormat`); codec parameter (or
  env-selected package var) in `CompressShuffleData` /
  `compressShuffleStream`; zstd branches in `DecompressShuffleData`,
  `streamDecompressShuffle`. (The coordinator's own decode — inline
  results and stage-output reads — goes through the shared
  `wshf.Decompress`/`wshf.DecodeBatches` instead of a worker-side
  helper; see ADR-0010 and #422.) Pooled zstd encoder/decoder alongside
  the s2 pools (klauspost zstd decoders are goroutine-bound — pool them
  like the s2 readers, do NOT share).
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

## 6. SF100 A/B verdict (2026-08-14, bin 6d3082c, adjacent arms,
## runs=4 each): VALIDATED — pinned exchange_zstd=1 in
## sf100-distributed.tfvars

Control (flag off, results/20260814-100950) vs treatment (flag on,
results/20260814-110321); both destroyed, EC2 zero; artifacts
~/wadjet-artifacts/20260814-zstdpair/.

- **CORRECTNESS: perfect.** Rows 88/88 identical both arms; vsig 84/84
  identical except the known-benign Q19 last digit. Engagement real:
  wshz_files ≈2500/worker (7,515 total), 18.3 GB WSHZ wire bytes.
- **BYTE ECONOMY**: S3 upload bytes 92.1 GB (closure-run clean s2
  reference, same config/runs) → 74.7 GB = **−19%** (cross-window
  reference; the compressed-subset envelope measures −34% in the §2
  bench). Same-window NIC tx −13.9% (427.9→368.5 GB) — confounded
  upward by control's re-dispatch re-uploads. ENA out-exc totals NOT
  comparable at these walls (treatment ran 3× faster; same-bytes-
  shorter-window inflates event counts — the known chronic-throttle
  pattern). A paced-rate ENA read at equal walls remains unread;
  judge it opportunistically on future arms (all future arms carry
  the flag via tfvars).
- **WALLS: treatment R1 255.7 / R2 217.0 / R3 201.8 / R4 198.4 —
  the best steady suites recorded on this config** (closure baseline
  212.1/214.2), cross-window caveat. Control walls are unjudgeable
  (see below).
- **★ STALL ASYMMETRY (same window, same binary, only the flag
  differs)**: control suffered ~13 frozen-spin firings incl. 5
  SIGABRTs (all 3 workers, 88/88 still correct through re-dispatch);
  treatment fired ZERO. Control SIGABRT dumps show the mechanism
  class: GC mark cycle wedged with 31 goroutines in "GC assist wait",
  port handler starved ~5s (NOT specimen-8's "no GC workers" read —
  go1.26 dumps show them plainly). Hypothesis for the frozen-spin
  arc: s2.Writer's default-concurrency encode goroutines + block
  churn (writeFull/Reset frames present in dumps) drive allocation
  pressure that zstd's pooled concurrency-1 encoder avoids. NOT
  proven — the clean-closure-vs-control gap on the SAME s2 codec
  (zero vs 13 firings, near-identical binary, night vs day window)
  is an open residual. Evidence archived: 7 stack captures + SIGABRT
  dumps in control/wlogs/.
- Reap-grace note: its first live exposure (5 worker deaths) —
  recovery clean, no lost-input query failures, rows perfect.

DECISION: exchange_zstd=1 pinned in sf100-distributed.tfvars (engine
default stays env-off; 0 reproduces s2 baselines). Envelope decode
support for WSHC and WSHZ ships everywhere regardless of the flag.
