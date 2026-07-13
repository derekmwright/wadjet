# Exchange streaming consumption (streaming-exchange Phase D)

> **Status:** DESIGN FOR REVIEW — no code. **Date:** 2026-07-13.
> **Verified against:** main @ `5f3707e`. Anchors drift.
> **Context:** next lever after the base-table NVMe cache arc
> (docs/design/base-table-nvme-cache.md §10). The post-cache profile
> (results/20260713-160213) shows base-table fetch ELIMINATED from the
> block profile and the new #1 critical-path block class is consumer
> fragments starving on exchange input arrival, while utilization sits
> at ~2.8/16 cores.

## 1. Goal and evidence

Remove the consumer-side file-granular staging serialization from the
exchange read path: today a consumer cannot decode the first row of a
shuffle input until the ENTIRE file has been fetched, written to NVMe,
fsync'd, and mmap'd — and cannot start fetching input N+1 until input N
is fully consumed. Both gates are artifacts of the read plumbing, not of
the format or the fault-tolerance contract.

Evidence (post-cache profiling run, block profiles Σ2 suites/worker):

- #1 named critical-path block: `runFragmentLinearParallel` waits
  (~2.5h func5 + ~1.1h func1 per worker) — morsel consumers parked on
  the dispenser channel because the *source* is stalled, and the source
  stall is whole-file staging (`openShuffleFile`'s `io.Copy` + `Sync` +
  mmap) plus arrival waits.
- Upload legs are off the critical path (background pool; `uploadOnce`
  → `Put` ~1.35h/worker parked in the pool, by design).
- CPU is not the binder (2.8/16 cores): memmove 17% flat, zstd decode
  18.5% cum, s2 encode 8.6%.
- The eager-dispatch arc verdict (PRs #218/#219) still holds shape:
  consumers drain arrived input fast, then wait. Eager moved the wait
  from the stage barrier to the manifest feed; this design attacks the
  remaining per-file serialization so that waiting is spent overlapped
  with useful decode instead of after staging completes.

What this does NOT do: raise producer throughput. If the pair shows
consumer waits collapse but wall stays flat, the binder is producer
production rate and the profile re-rank will say which producer cost
(shuffle-write memmove/s2, zstd decode) to attack next — same
honest-bounds framing as the last two arcs.

## 2. What the code does today (verified)

The write side is already incremental; every consumer-visible gate is
file- or task-terminal:

- Producers flush self-contained WSHF chunks to disk every ~64 KB per
  partition DURING Consume (`partitioned_shuffle_sink.go:298-308`,
  `flushPartitionLocked :607`); `unpartitionedStageSink` double-buffers
  16 MB flushes outside the lock (`unpartitioned_stage_sink.go:229-258`).
- The WSHF format is chunk-framed and self-describing
  (`shuffle_format.go:17-35`); `writeChunk` (`:123`) needs no footer.
  The ONE whole-file field is the header `NumChunks`, written as 0 and
  patched at Finalize (`partitioned_shuffle_sink.go:690-697`). For a
  COMPLETED file the header is already correct. The s2 layer is
  streaming-framed (`CompressShuffleFile :772`,
  `streamDecompressShuffle :832`).
- Peer transport already streams: `PeerServer.FetchShuffle` serves the
  file in 256 KiB frames (`dataplane/peer_server.go:183,:214-231`),
  `peerFetchReader` adapts it to `io.Reader` (`peer_client.go:89`).
- **The consumer then throws the streaming away**: `openShuffleFromPeer`
  (`peer_exchange.go:167`) hands the reader to `openShuffleFile`
  (`stream_source.go:643`), which `io.Copy`s the whole body to a spill
  temp, `Sync`s, mmaps, and only then constructs `shuffleChunkReader`
  (`:698-729`). Decode of chunk 1 waits for byte N. The S3 read path is
  identical.
- `shuffleChunkReader` decodes chunk-at-a-time but over a random-access
  `[]byte` (`shuffle_format.go:432,:493`), not an `io.Reader`.
- Files are consumed strictly one at a time: `cachedFileStreamSource`
  iterates `s.files` serially (`openNextFile`, `stream_source.go:304`);
  the scan prefetcher deliberately excludes shuffle keys
  (`scan_prefetch.go:60-63` — written when blind-GETting them from S3
  raced async upload; a PEER-tier prefetch has no such race for
  manifest-announced completed files).
- Arrival announcements are per producer TASK at terminal success
  (`ProducerTaskManifest`, `messages.go:525`; published from
  `taskRetrier.Observe` only on Success, `task_retry.go:126-141`).
- Producer chunk order is NON-deterministic across attempts: concurrent
  Consume calls interleave chunks within a partition file
  (`partitioned_shuffle_sink.go:76-78`). The eager fencing contract
  handles retries by poisoning the whole consumer task
  (`manifest_stream_source.go:128-133`) — attempt-scoped resume is not
  possible without serializing the sink.
- Skew-split needs full terminal PartitionBytes on both deps
  (`skew_split.go:124-130`); the eager join path already falls back to
  the barrier when a split might fire (memo #218 §6, C2 slice 1).

## 3. Design

Three slices, strictly ordered by fault-tolerance delta. D1 and D2 have
ZERO delta — they change only how bytes of already-announced, completed,
attempt-fixed files reach the decoder. D3 is the mid-task extension and
is explicitly gated on the D1+D2 profile.

### D1 — streaming chunk decode (kill the staging hop)

A `streamingShuffleReader`: an `io.Reader`-based variant of
`shuffleChunkReader` that reads the header (including the — correct,
file-is-complete — `NumChunks`), then decodes chunk-by-chunk directly
from the peer gRPC stream (or S3 body), validating the promised count at
EOF exactly as the mmap reader does today (`shuffle_format.go:503`'s
truncation guard moves to end-of-stream). Wire it in
`openShuffleFromPeer` and the S3 WSHF branch of `openNextFile`; the
decoded batches already copy out of the buffer
(`readColumnData :591`), so no aliasing changes.

- No format change. No disk write, no fsync, no mmap, no
  `mmapRegistry` traffic on this path; heap holds one chunk frame at a
  time (strictly less than today's full-file page-cache footprint).
- Tier changes: LocalStageCache tier-0 (same-worker mmap-in-place) and
  the KV in-memory tier keep their current shape — they are already
  zero-copy local. Only peer and S3 reads become streaming.
- WSHC: `streamDecompressShuffle` already decodes s2 incrementally;
  compose the s2 stream reader under the chunk decoder.
- Failure mid-stream (peer dies at chunk k): identical contract to
  today's failure mid-`io.Copy` — the source falls through to the
  durable S3 copy and re-reads FROM THE TOP. Chunks are not
  checkpointed; a re-read re-decodes. Partial-consumption state is
  confined to the source's current-file cursor, which is rebuilt on
  fallback — no operator sees a torn file because the fallback replays
  the file completely and the source only exposes whole batches.
  (Pipeline breakers consume batches idempotently within a task; a
  double-delivered batch is impossible because the source discards the
  partial file's progress and restarts its own iteration of that file.)

**The one real design constraint in D1 — no double delivery:** a source
that has already yielded
batches 1..j from file F cannot re-yield them after falling back. The
streaming reader therefore tracks the chunk index it has delivered; on
fallback it opens the durable copy with the mmap reader and SKIPS the
first j chunks before resuming. Chunk boundaries are deterministic
within one written file (the file's bytes are immutable once finalized),
so skip-by-index over the same completed file is exact. This is the
streaming-read analog of the prefetcher's "best-effort, durable copy is
authoritative" rule.

### D2 — concurrent input fetch window (kill the serial file walk)

A shuffle-input prefetcher on `cachedFileStreamSource`, mirroring the
parquet `filePrefetcher`'s shape (index-ordered delivery, byte-window
bound, deadlock-free bypass — `scan_prefetch.go:225`) but sourcing from
the PEER tier (and S3 fallback) instead of blind S3 GETs:

- Eligibility: shuffle keys whose location is already resolved — a peer
  hint from `Task.InputLocations`/manifests (`peer_exchange.go:52,:77`)
  or a durable copy (coordinator durability bit at dispatch). This is
  what removes the fe52a79-class objection recorded in
  `scan_prefetch.go:60-63`: we never race the producer's async upload
  because we only prefetch files the manifest says exist, from the peer
  that owns them.
- With D1, "prefetch" for the NEXT files means opening their streams
  and buffering ahead within the byte window while the CURRENT file's
  chunks feed the dispenser; small files (the common case — one file
  per producer task × partition) simply land whole.
- Window sizing: reuse the parquet prefetcher's constants as a starting
  point (4 concurrent, 256 MB window); the A/B decides if shuffle wants
  different numbers — no new tuning surface in v1.
- `manifestStreamSource` composes for free: it feeds resolved file sets
  into an inner `cachedFileStreamSource` (`manifest_stream_source.go:190`),
  which gains the same prefetcher.

### D3 — mid-task chunk announcement (gated, probably not needed)

The full intra-task lever: producers announce sealed chunk ranges
during Consume (flush hooks at `partitioned_shuffle_sink.go:607` /
`unpartitioned_stage_sink.go:238`), consumers stream them before the
producer task finishes. Deliberately OUT of the initial scope:

- It multiplies manifest volume, needs a mid-task publish channel
  (today's publisher fires only from `Observe` on terminal success),
  needs partially-written files to be readable (sentinel framing or
  side-channel counts — a format change), and inherits the
  non-deterministic-attempt-order fencing cost: any producer retry
  poisons every consumer that ingested its chunks
  (`manifest_stream_source.go:128`), turning one producer failure into
  a consumer retry storm.
- Go/no-go criterion: run the D1+D2 SF100 pair with block profiling. If
  fragment-input waits are still the top class AND they decompose as
  waiting-for-manifest (production tail) rather than
  waiting-for-bytes-of-announced-files, D3 is the residual lever and
  gets its own memo. If waits collapse or move to producer-side CPU,
  D3 is dead and the next arc is shuffle-write CPU (bulk bytes
  scatter: memmove 17% flat / SetFrom+appendBatchRowsBulk ~13%).

## 4. Fault-tolerance contract (D1+D2)

Unchanged, provably, by the same argument as streaming-exchange Phase A
(docs/design/streaming-exchange.md §3A): D1/D2 only change the transport
of completed, manifest-announced or dispatch-frozen files whose attempt
identity is fixed and whose durable copy either exists or is
`awaitDurableObject`-covered exactly as today. Every failure falls
through to the tier below with a full re-read (D1's skip-by-index is an
optimization of the re-read, validated by chunk-count equality). Eager
fencing (StaleInputAttempt), retry classification (missingInputError →
liveness+durability), skew accounting (terminal PartitionBytes), and
StageOutput freezing are untouched.

## 5. Flags and rollout

- `--streaming-shuffle-read` (bool, default false until the SF100 pair;
  kill switch thereafter — the base-table-cache arc convention).
  Gates BOTH D1 and D2: they ship together as one read-path change; an
  intermediate D1-only default serves no operational purpose, though
  the implementation lands them as separate reviewable slices.
- tpch-bench env `WADJET_STREAMING_SHUFFLE_READ` (explicit opt-in
  parse), terraform var `streaming_shuffle_read` default false.
- Markers (§8 pattern): per-worker counters
  `ShuffleStreamReads/ShuffleStreamFallbacks/ShuffleStreamSkipResumes`
  + prefetch window stats; DEBUG log on every fallback with the tier
  it fell to.

## 6. Slices and gates

- **S1**: `streamingShuffleReader` + unit tests (round-trip vs the mmap
  reader on identical files incl. WSHC, truncation mid-chunk, count
  mismatch, skip-by-index resume equivalence, fuzz the frame parser
  against the existing writer).
- **S2**: wire into peer + S3 read paths behind the flag; fallback
  skip-resume; counters. Race tests; harness local both flag states
  (default + DAG-forced) rows+checksums identical.
- **S3**: shuffle-input prefetcher (peer-tier eligibility) + tests
  (window accounting reuse, hint-miss → no prefetch, prefetch-then-
  fallback interplay).
- **S4**: SF100 same-window pair, both suites per arm
  (`benchmark_runs=2`), block profiling ON in both arms (the samplers
  are cheap and the decisive markers are block-profile classes, not
  wall). Decisive markers in order: (1) fragment-input wait class
  (expect the staging component eliminated), (2) utilization vs the
  2.8/16 reference, (3) `ShuffleStreamReads` vs fallbacks (expect
  ~100% streaming on healthy runs), (4) wall LAST. Pull worker logs
  BEFORE teardown (base-table-cache arc lesson).

## 7. Open questions for review

1. **Scope check (technical, decide-and-execute unless objected):
   D1+D2 as one arc, D3 explicitly deferred behind the profile
   go/no-go.** The alternative reading of the evidence — jump straight
   to D3 because eager showed waits move — is rejected in this memo on
   cost/risk grounds: D3's fencing amplification and format change are
   only worth buying once we know announced-file bytes aren't the
   binder.
2. **Naming/framing (PM, trivial):** this is streaming-exchange
   "Phase D" in spirit; the doc is standalone rather than an edit to
   streaming-exchange.md. OK?
3. **If the pair shows wall flat but waits collapse** (the eager-arc
   outcome shape): the follow-on is producer-side (shuffle-write bulk
   scatter or D3 per the §3 criterion) — flag stays default-off until
   a lever moves wall, same no-forcing clause as eager. Confirm that
   convention still holds.
