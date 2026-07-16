# Scan decode pipelining (parallel row-group decode-ahead)

Status: PROPOSED. Follow-up predicted by
`docs/design/scan-decompress-parallelism.md` §5 ("scan decode becomes
CPU-parallel instead of slot-serial" fixed the *decoder*; the *producer*
stayed serial) and by the morsel cap comment itself
(`executor_fragment.go:461-463`: "past ~8 consumers the single producer
(source decode) is the bottleneck").

Evidence: SF100 streaming-ON profiles `results/arm-b-treatment`
(2026-07-14 pair, main-equivalent bin `5820244`, 2 suites/worker).

## 1. The structure

On the distributed worker scan path, **row-group decode is fully
serial per fragment**:

- `cachedFileStreamSource.Next` yields one decoded row group per call
  from a lazy iterator (`stream_source.go:218-225`); files within a
  scan leg advance one at a time (`openNextFile`,
  `stream_source.go:264`, `:336-337`).
- `scan.RowGroupIter` advances `cur` one row group at a time and calls
  `ReadRowGroupNative` per group (`rowgroup_iter.go:41`, `:160`).
- Morsel consumers do NOT decode. One producer goroutine pulls from the
  source and feeds k ≤ 8 consumers zero-copy views of already-decoded
  batches (`morsel_dispenser.go:40-49`, `executor_fragment.go:334-341`,
  `:507-520`). The cap exists *because* the serial producer saturates
  first.

The only decode parallelism today is **within a single row group**:
the per-column errgroup in `ReadRowGroupNative`
(`columnar_native.go:130-135`, limit = min(#projected columns,
GOMAXPROCS)) and zstd's internal ≤ 16 states (`decompress.go:53-70`,
PR #216). Narrow projections (Q06 reads 4 lineitem columns) leave most
of that width unused, and page decode of row group N+1 never overlaps
the operator chain over row group N beyond what the single dispenser
producer pipeline gives.

## 2. Evidence from the 2026-07-14 streaming-ON pair

All three workers agree within a point (per worker, 2 suites,
~8.9 ks CPU samples on ~3.1 ks wall = **2.9 of 16 cores**):

| Class | Cum CPU | Reading |
|---|---|---|
| parquet zstd page decode (`decompressZstd`) | **~20%** | top real work; sits on the serial producer |
| `runtime.memmove` (flat) | ~17% | split: shuffle-write append ~6%, join gathers ~3.5%, zstd inherent ~4% |
| s2 shuffle encode | ~8.7% | shuffle-write class (candidate B, §7) |
| mallocgc | ~5.4% | |
| fragment-input waits (old #1 block class) | **collapsed** | streaming shuffle read did its job |

The exchange-arrival wait class is gone (PR #225/#226), base-table
fetch is gone (base-table cache arc), the zstd decoder is
CPU-parallel (PR #216) — and utilization still sits at 2.9/16 with
decode as the largest real-work class. The remaining serialization is
structural: **one goroutine's decode throughput bounds every scan
leg**, exactly as the morsel cap comment predicted. NVMe cache hits
make bytes arrive instantly and then re-decode them on one core per
fragment.

## 3. Design

Apply the validated prefetcher shape (filePrefetcher / D2
shuffle-input prefetch: **window + ordered delivery + bypass**) one
level up — from "bytes staged ahead of consumption" to "row groups
decoded ahead of consumption":

- A `decodeAheadIter` wraps the current per-file `RowGroupIter`
  inside `cachedFileStreamSource`: k decode workers pull row-group
  indices (file-ordered), each runs the existing
  `ReadRowGroupNative` for its group, results deliver **in source
  order** by sequence number. The consumer-facing `Next` contract is
  byte-identical to today — in-order delivery means no downstream
  semantics change at all, independent of sink order-tolerance.
- **Cross-file continuation**: when the tail of file F is in flight,
  workers continue into file F+1 (already NVMe-staged by
  scanPrefetch; opening its reader early is metadata-cheap). Without
  this, files with few row groups cap the win. The mmap-release
  discipline in `stream_source.go:225-233` moves from "release when
  iterator exhausts" to "release when the last in-flight group of
  that file has delivered" (refcount per file, broadcastJoinCache
  pattern).
- **Byte-bounded window**: today "only one row group's worth of
  memory is live at a time per source" (`stream_source.go:216-217`).
  Decode-ahead relaxes this to W bytes of decoded batches in flight,
  estimated from row-group metadata (TotalByteSize), default
  256 MiB per fragment source (same order as the scanPrefetch byte
  window; the morsel dispenser's 512 MiB budget already bounds the
  next stage downstream). Window full ⇒ workers block; consumer
  drains ⇒ they resume. Estimation error is bounded by one row group
  per worker.
- **CPU-token integration**: decode workers beyond the first draw
  from the same `cpuTokens` budget as morsel consumers
  (`morselFragmentWorkers` shape: non-blocking TryAcquire, degrade
  toward serial under exhaustion, k=1 ⇒ exactly today's behavior).
  This keeps fragments×workers from oversubscribing 16 vCPUs — scan
  decode and morsel consumption compete for the same physical cores
  and must share one budget.
- Scope v1 = the worker DAG scan path (`cachedFileStreamSource`).
  The single-process/fastpath scanner (`scanSourceInner`,
  planner/physical/util.go) has a different shape and its own arc if
  the profile ever ranks it.

## 4. Alternatives considered

- **Transcode cached base tables to a cheaper codec** (snappy or
  uncompressed on BaseTableCache insert). Rejected for v1: it shrinks
  the serial section instead of removing the serialization (utilization
  stays producer-bound); it only helps cache *hits* (cold S3 and
  non-cached deployments keep full-cost decode); the cache tee is
  synchronous on the read path (`base_table_cache.go:512-542`) so it
  needs a background transcode worker; and it puts the parquet
  *writer* on the critical data path with the full
  parquet-safety-critical burden (the writer can only emit
  uncompressed/snappy/zstd/gzip today, `file_writer.go:1340-1369`,
  and the existing reader→writer round trip is the row-based
  compaction path, `compactor.go:317-369`, unproven for this use).
  Decode pipelining composes with it if decode CPU ever ranks #1
  again *after* the serialization is gone.
- **Out-of-order delivery to the dispenser**: sinks tolerate it on the
  DAG path (morsel consumers already interleave,
  `docs/design/morsel-execution.md` §sink-concurrency), but it buys
  little over an ordered window of the same byte size and forfeits the
  "no semantics change anywhere" property (SMJ, if ever revived, needs
  order). Deferred; the sequence-number plumbing makes it a flag-flip
  later if profiles justify it.
- **Raise the per-column errgroup width / row-group size tuning**:
  helps only wide projections / does nothing for the overlap
  structure. Narrow-projection scan legs (the TPC-H scan band) stay
  serial. Rejected as the primary lever.
- **Decode inside morsel consumers** (consumers pull row-group indices
  instead of decoded batches): erases the producer/consumer split the
  morsel arc is built on (clone/merge machinery assumes decoded input
  views), and couples decode width to op-chain width — they have
  different optimal k. Rejected.

## 5. Flags and rollout

- `--scan-decode-ahead` (bool, default **false** until the SF100 pair;
  kill switch thereafter — the streaming-shuffle-read arc convention).
  Gates the decodeAheadIter + cross-file continuation together.
- `--scan-decode-ahead-bytes` (window, default 256 MiB) — sizing knob,
  not expected to need tuning (no threshold-tweak campaigns; if the
  default is wrong the design is wrong).
- Terraform var `scan_decode_ahead` mirroring the flag for the A/B.
- Markers from day one (§8 convention): per-worker counters
  `ScanDecodeAheadGroups` / `WindowFullStalls` / `TokenDegrades` +
  per-fragment chosen k; DEBUG line per fragment on degrade-to-serial.

## 6. Slices and gates

- S1: decodeAheadIter (in-file window, ordered delivery, byte budget)
  behind the flag; unit tests = ordered-delivery contract, window
  accounting, error propagation (decode error on group N surfaces at
  the consumer exactly where serial would), shard interaction
  (`SetShard` ranges, `columnar_native.go:63-79`).
- S2: cross-file continuation + mmap refcount release; tests for file
  lifetime (batch from file F alive while F+1 decodes), release-order
  regression (the 2026-05-22 mmap-lifetime comment in
  `stream_source.go:225-233` becomes a test).
- S3: cpuTokens integration + degrade; `-race` on the whole feature
  (this is made of data races waiting to happen — morsel gate list
  applies verbatim).
- Gates in order: full unit + worker `-race`; SF0.01 22/22; `tpch-harness
  --mode=local` both flag states, rows + checksums identical, plus
  DAG-forced arm (`--local-fastpath-bytes=0`); SF100 same-window pair
  (needs deploy approval), benchmark_runs=2 both arms, block+CPU
  profiling both arms. Decisive markers in order: **utilization vs
  2.9/16 ref** → decode-ahead engagement counters → scan-band wall
  (Q01/Q06/Q12/Q14/Q15/Q19) → suite wall LAST. Success = utilization
  and scan band move together; wall flat + utilization up = re-rank,
  not failure (`feedback_no_revert_on_serial_clog`).

## 7. Honest bounds and what this does NOT fix

- The 20% decode share is CPU, not wall; the wall claim is structural
  (scan legs stop being bounded by one core's decode throughput) and
  only the SF100 pair prices it. Queries whose scan legs are already
  short (exchange-dominated Q17/Q18) should move little.
- The **shuffle-write class (~16% combined: row-wise
  `appendBatchRowsBulk`/`SetFrom` ~6%, s2 encode ~8.7%, bufio ~1%) is
  untouched** — that is candidate B (bulk bytes scatter: run-detection
  over ascending per-partition rows + the existing `BulkCopy`
  primitive, `vector.go:144-152`; the unpartitioned no-Sel path is
  already a dense 0..n range and a single BulkCopy per column,
  `unpartitioned_stage_sink.go:212-215`). B is a separate, smaller,
  micro-bench-gated arc (`feedback_no_ab_on_architectural_perf`
  shape); its null-bitmap edge is the c0c58ea class and needs
  null-aware run detection.
- Join-gather memmove (~3.5%) and mallocgc (~5.4%) are untouched.
- Memory: +W bytes decoded-ahead per active scan fragment is real heap
  the ledger does not charge today (same accounting posture as the
  morsel dispenser's 512 MiB). The window default keeps
  fragments×window inside the envelope on c7gd.4xlarge; a
  memory-pressure collapse hook (drop to k=1, drain window) is listed
  in S3 and must be in place before default-ON is proposed.
