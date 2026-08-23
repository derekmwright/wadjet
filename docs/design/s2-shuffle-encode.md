# s2 shuffle-write encode (investigation memo, 2026-07-19)

Status: **investigated, recommendation = no codec/format change**. The
kickoff premise ("s2 encodeBlockGo 8.33% + load32 1.37% ≈ 9.7% worker
CPU = #1 addressable class") is refuted by call-path attribution: since
streaming-exchange Phase B, all shuffle-write compression runs in the
**background upload manager, off the query critical path**. Cutting the
work would free idle-core CPU (cluster utilization ~3.1/16) and convert
to approximately zero wall. Byte measurements below also kill the two
secondary motivations (S3 cost, upload bandwidth). Evidence, options
considered, and the answers to the kickoff design questions follow.

Profile basis: floor-validation run, bin 5d21461, main bbddda1
(results/20260719-105312; worker profiles from all 3 workers).
Byte basis: SF1 local harness run of this date (method §3).

## 1. Why the profile misled: encode is background work

Two facts combine to make s2 encode look like foreground shuffle-write
cost when it is not:

1. **All native-DAG stage/shuffle tasks upload asynchronously.** The
   coordinator sets `AsyncUpload=true` on every `TaskTypeStage` /
   `TaskTypeShuffle` task (`coordinator/peer_locations.go:207`), unless
   the query is in ErrInputLost re-execution. On the worker, the task
   reports completion at sink Finalize; each partition's **raw WSHF**
   file is adopted into the LocalStageCache, and the S3 PUT — including
   the s2 compression that precedes it — is handed to the background
   `uploadManager` (`worker/executor.go:1128-1169`,
   `worker/executor_async_upload.go`). Nothing on the query path waits
   for it: background uploads gate only `Worker.Drain`/`Flush`
   (`worker/upload_manager.go`), and the coordinator merely flips
   per-key durability bits on `UploadComplete`
   (`coordinator/peer_locations.go:99-135`).

2. **s2's internal concurrency detaches encode samples from their
   caller.** `s2.NewWriter` defaults to `concurrency = GOMAXPROCS`;
   block encoding runs in spawned goroutines rooted at
   `s2.(*Writer).writeFull.func1`, so `encodeBlockGo` appears in the
   profile with no application ancestor. Peeking the submission side
   shows the truth: on **all three workers**, 100% of
   `CompressShuffleFile` samples sit under
   `uploadManager.runJob → uploadOnce` (51.5s / 51.1s / 47.6s of
   ~9.5ks samples each); the legacy synchronous path
   (`executor.go:1224`) contributed zero. The ~790s/worker of
   `encodeBlockGo` is the async continuation of those submissions.

Consumers do not read the compressed artifact in steady state: the
tiered open is LocalStageCache mmap (raw, same worker) → peer gRPC
fetch (raw, 256 KiB frames; `dataplane/peer_server.go`,
`worker/peer_exchange.go:136`) → S3 (WSHC/WSHF, streaming reader;
fallback only — the streaming-consumption arc measured fallbacks=0 at
SF100). The durable S3 copy is written ~always and read ~never within
a healthy run. s2 **decode** is correspondingly cheap in the profile
(~1.2%, 2026-07-13 re-rank).

Precedent: this is the eager-dispatch lesson again
(docs/design/eager-consumer-dispatch.md — barrier block −24.9%, wall
flat). CPU-class size in a 3.1/16-utilized cluster says nothing about
wall unless the work is on a stage's serial leg. Shuffle-write encode
was on the serial leg before Phase B; it is not anymore.

## 2. Who reads .wshf / WSHC (kickoff question 1)

| Reader | Path | Format seen |
|---|---|---|
| Same-worker consumer | LocalStageCache mmap (`stream_source.go` tier 0) | raw WSHF (async-upload tasks adopt the raw file) |
| Peer worker | gRPC FetchShuffle → spill/stream (`scan_prefetch.go:260`, `peer_exchange.go:162`) | raw WSHF (serves the adopted cache file) |
| Any worker, fallback | S3 GET → `streamingShuffleReader` / mmap-transcode (`shuffle_stream_reader.go`, `stream_source.go:1110-1150`) | WSHC or WSHF (magic-dispatched) |
| Coordinator | gather/data-plane + small-payload KV tier (`executor.go:1551`, `coordinator/shuffle_reader.go`) | WSHC via `CompressShuffleData` (negligible in profile) |

So compression today buys: smaller S3 PUTs (background) and smaller
S3 GETs on the rare fallback. It costs: ~8.3% of worker CPU samples
(background), plus an extra NVMe read+write cycle per partition file
(raw file re-read → compressed temp written → temp uploaded and
removed).

## 3. Measured shuffle byte volumes (kickoff question 3)

Method: SF1 local harness (`tpch-harness --mode=local
--scale-factor=1`, fresh main build, PASS), with a sampler copying
every `queries/<id>/…` FileStore object out before the coordinator's
end-of-query purge (FileStore Put is temp+rename atomic, so every
visible file is complete). Copies were then magic-classified and WSHC
bodies stream-decoded to recover raw sizes. 1404 objects captured,
1 truncated copy discarded.

Full 22-query suite + micros, totals:

| | files | stored | raw (decompressed) |
|---|---|---|---|
| WSHC (compression paid ≥10%) | 916 | 2.21 GB | 4.40 GB |
| WSHF (compression below threshold, raw uploaded) | 489 | 6.31 GB | 6.31 GB |
| **Total** | **1404** | **8.52 GB** | **10.71 GB** |

Key numbers:

- **Aggregate ratio raw/stored = 1.26x.** Dropping compression
  entirely would raise scratch upload bytes by only +26%.
- **59% of raw bytes are effectively incompressible** (fail the ≥10%
  s2 threshold — dense numeric columns). For these,
  `CompressShuffleFile` runs the full encode and discards the result:
  **more than half the background encode CPU produces nothing** even
  on its own terms.
- Where compression pays, ratio ≈ 2.0x (string-heavy payloads);
  per-query dir ratios ranged 1.0-1.85.

SF100 extrapolation (order of magnitude; same schema and value
distributions, plans differ some): ~0.85 TB stored / ~1.1 TB raw of
S3 scratch per full suite run. Per-worker background upload ≈ 1.5
Gbps average over a 25.7m run today, ≈ 1.9 Gbps uncompressed — both
comfortably inside c7g.4xlarge bandwidth, and off the critical path
either way.

## 4. AWS cost (kickoff question: "bigger S3 scratch objects")

S3 scratch cost is **request-count- and duration-dominated, not
byte-dominated**, and compression changes neither:

- PUT requests: file count is set by partitions × tasks (plan shape),
  not bytes — ~1.4-3k PUTs per suite run ≈ $0.01-0.015. Unchanged by
  codec choice.
- Storage: ~0.85 TB for ~30 minutes ≈ $0.01 (purged by run-benchmark
  before shutdown). Uncompressed: ~$0.013.
- Data transfer: EC2↔S3 same-region is free. GETs: fallback-only.

Total scratch S3 ≈ **$0.02-0.03 per SF100 run vs ~$0.90 of EC2**; the
compression decision moves ~$0.005/run. The real AWS-cost lever
remains wall time (EC2 hours). Bigger scratch objects are a
non-issue as long as the purge discipline holds (run-benchmark.sh
`s3 rm` before shutdown; the 2026-06 1.4 TB leak was missing purge,
not object size).

## 5. Options from the kickoff, with verdicts

- **(a) Skip/lighten compression for NVMe-local same-worker
  partitions** — already the architecture: local + peer consumers read
  the raw adopted file; only the S3 durability copy is compressed.
  Nothing to do.
- **(b) Size-threshold skip for tiny partitions** — exists
  (`CompressShuffleFile` skips <64 B; the ≥10% heuristic handles the
  rest). Non-lever.
- **(c) s2 encode mode** — already the fastest mode (pooled default
  `s2.NewWriter`, not `EncodeBetter`; `shuffle_codec_pool.go`).
  Going *slower/denser* to shrink the 1.26x would spend more
  background CPU to save pennies. Rejected.
- **(d) Parallel encode across partitions** — already exists twice
  over: 8-way upload concurrency (sync path) / per-job goroutines
  under an 8-slot semaphore (async path), × s2's internal
  GOMAXPROCS block concurrency. Non-lever.
- **(e) No-compress + rely on NVMe/network headroom** — the measured
  case is *weak in both directions*: saves ~0.26 cores/worker of
  background CPU (wall-neutral at 3.1/16 util) against +26% background
  upload bytes and +26% fallback GET bytes. No wall win, no cost win,
  plus a semantics knob to maintain. Rejected.

## 6. Wire-format versioning (kickoff question 2)

- WSHF carries **no version byte**: magic `WSHF` + `NumChunks`
  (patched at Finalize via seek-back,
  `partitioned_shuffle_sink.go:716-726`) + schema + chunks (layout
  doc `internal/wshf/wshf.go:8-22`, written by
  `shuffle_format.go:102-187`, parsed by `wshf.ParseHeader`,
  `internal/wshf/decode.go:14-66`). WSHC is magic `WSHC` + an s2 stream
  of the complete inner WSHF file (WSHZ, added later, is the zstd
  variant of the same envelope). Format identity = the 4-byte magic;
  readers dispatch on it and error loudly on anything else.
- Scratch is intra-query ephemeral (written and read within one run,
  purged at query end), so cross-version exposure exists only when a
  single query spans mixed-version workers (rolling upgrade). Any
  future incompatible change should bump the magic (e.g. `WSH2`) so
  old readers fail the query cleanly instead of misdecoding; no
  in-band version negotiation is warranted for scratch files.
- Note for any future "write WSHC natively during production" idea:
  the seek-back `NumChunks` patch is the one whole-file field and is
  impossible inside an s2 stream; it would need a read-to-EOF
  sentinel or trailer — a format-semantics change requiring the magic
  bump above. Not needed now.

## 7. Parked micro-lever (record only, do not schedule)

Early-bail sampling in `CompressShuffleFile`: compress the first
block(s), extrapolate, and skip the remaining encode when projected
savings <10%. Would eliminate ~59% of background encode CPU at zero
byte cost. It is wall-neutral today for the same reason the whole
class is; it becomes worth doing only if cluster utilization rises to
the point where background CPU contends with the critical path.
Revisit trigger: worker utilization sustained above ~10/16 on suite
runs.

## 8. Where the wall lever actually is

With shuffle-write encode reclassified as background, the floor-run
profile's wall-relevant classes are unchanged from the post-cache
re-rank: **exchange arrival waits** (block class — consumers waiting
on upstream *production*, not on encode/upload), zstd scan decode
(real work, already parallelized), and the memmove composite. The
structural read of arrival waits is the stage barrier itself:
downstream stages start only when the upstream stage completes, while
utilization sits at 3.1/16. The candidates are task-granularity
consumer start (revisiting the eager-dispatch idea above the
now-existing peer/streaming-read machinery rather than below it) or
splitting the serial legs of the worst stages. That investigation is
out of scope here and should get its own evidence pass before any
design.
