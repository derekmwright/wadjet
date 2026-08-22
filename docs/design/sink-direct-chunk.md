# Stage-sink direct-chunk path

**Status:** shipped (2026-08-12). Kill switch: `WADJET_SINK_DIRECT_CHUNK=0`.

## Problem

The 2026-08-12 SF100 clean re-profile (`docs/benchmarks/lever-ranking-2026-08-12.md`,
block profiles in `~/wadjet-artifacts/20260812-reprofile/run/profiles/`) pinned
**64.3% of all worker mutex block time (2,690 s across one suite)** on
`partitionedShuffleSink.appendAndMaybeFlush`, with
`unpartitionedStageSink.Consume` contributing another 11.2% in the same shape.
This contention sits exactly in the stage-barrier phase behind the remaining
R2/R1 gap and the sub-trap pause windows.

The sink already had per-partition locks (the 2026-07-07 refactor away from a
sink-wide mutex), so the blocking was on the partition locks themselves.
98%+ of it arrived via the large-consume burst path (`Consume.func2`):
morsel-parallel fragments (k ≤ 8 consumers, morsel views up to 64K rows —
`morselMaxViewRows`) each fan out per-partition goroutines, and every such
goroutine held its partition's lock for a multi-thousand-row accumulator copy
plus, at the flush threshold, a chunk encode. Concurrent consumers serialized
per partition on both.

## Fix

A consume slice whose estimated bytes already exceed its sink's flush
threshold would only pass through the accumulator to be flushed immediately.
The direct-chunk path skips the accumulator for those slices and encodes one
complete `.wshf` chunk **straight from the source batch** (`writeChunk`
already takes a selection), **outside the lock**:

- **Exclusion.** Each partition stream has two domains: `mu` guards the
  accumulator and any writer use while held; a `flushing` flag (set/cleared
  under `mu`, waiters on `flushCond`) marks a direct writer streaming into
  `writer`/`bufFile` outside `mu`. Accumulator flushes and lazy writer init
  wait for `!flushing`. This is the same shape the unpartitioned sink's
  double-buffered flush already used; the partitioned sink now carries it
  per partition, and the unpartitioned sink reuses its existing flag.
- **Lock hold time** for direct slices drops to counter updates.
- **One copy instead of two:** source→wire, rather than
  source→accumulator→wire. (Also removes the accumulator's growslice
  traffic for those rows — the micro-bench shows −21% allocated bytes and
  −25% allocs on the 8-consumer 64K-row shape.)
- **Gate:** `len(rows) × approxRowBytes(b) ≥ flushBytes` per partition slice
  (unpartitioned: `n ≥ flushRows` or estimated bytes ≥ `flushBytesT`, flat
  schemas only, matching the coalesce gate). Sub-threshold slices keep the
  locked accumulator path, so chunk sizes never fragment below today's
  floor (the 2026-07-03 SF10 lesson: chunk-per-consume fragmented outputs
  ~100× and cost +15%).

The scatter row lists changed from `[]int` to `[]uint32` so a partition slice
feeds `writeChunk` as a selection without conversion (and
`appendBatchRowsBulk` reads the same lists; the unpartitioned sink now passes
`b.Sel` through directly).

## What did NOT change

- **WSHF format:** byte-identical framing; chunks from the direct path and
  the accumulator path interleave within a partition file, which the format
  explicitly permits (self-contained chunk sequence; no chunk-order
  semantics). Readers untouched.
- **Flush thresholds, file layout, upload path, Finalize header patching.**
- **Memory:** no new buffers — the direct encode streams into the existing
  256 KB `bufio.Writer`. (An encode-to-heap-buffer variant was rejected: it
  would have re-introduced pooled multi-MB buffers with ledger interplay.)
- **View safety:** unchanged contract. `Consume` still pre-flattens own-null
  views and pre-warms `HasNulls` memoization single-threaded before fan-out;
  `writeChunk` serializes remaining views through composed selections
  without mutation.

## Verification

- Parity tests (direct vs. locked arm produce identical per-partition
  contents and counts), concurrent mixed-size race tests interleaving both
  paths on the same partitions, view-batch direct-path test, unpartitioned
  bypass test: `internal/worker/sink_concurrency_test.go`.
- Gates: `go test -race ./internal/worker/`, TPC-H SF0.01 22/22,
  `tpch-harness --mode=local --slice=small`, SF100 same-window A/B pair with
  block-profile confirmation (baseline arm `WADJET_SINK_DIRECT_CHUNK=0`).

## Validation verdict (2026-08-12, KEEPER default-on)

Two SF100 same-window pairs, bin 30587ff, fixed shapes, runs=2, baseline
arm = kill switch off. Evidence: `~/wadjet-artifacts/20260812-sinkdirect/`.

**Profiled pair** (block rate 20000; ctl results/20260812-163738, trt
results/20260812-165832 — the decisive pair, no anomalies in either arm):

- Walls: total **−15.7%** (683.6 s → 576.1 s), R1 −10.0%, R2 −20.6%;
  R2/R1 drift 1.166 → **1.029**.
- Rows 44/44 exact both arms; vsigs identical except Q19's documented
  last-digit float merge-order wobble.
- **Mutex confirm:** `appendAndMaybeFlush` block 2,860 s (3 workers,
  reproducing the morning ranking's 64% share) → **231 s (−92%)**; the
  direct path's own `Mutex.Lock` totals ~18 s.

**Clean pair** (ctl results/20260812-155149, trt results/20260812-161603):
total −14.9%; ctl suffered a one-off 4m02s Q21-R2 barrier collapse
(242.5 s → trt 29.8 s) with otherwise anomalously fast R2s — the window
lottery that motivated the profiled confirm pair. Worker pressure_stall_ms
totals: ctl 35.6 s vs trt 2.8 s (12× lower under the fix).

Open residuals / follow-up levers, not blockers:

- The per-partition stream still serializes concurrent direct writers via
  `flushCond` (~2.4 ks summed cond-wait inside `writeDirectChunk` across
  3 workers, replacing 2.9 ks of mutex block on half the copied bytes).
  Next-level lever if it ever ranks: per-writer partition files (readers
  already accept multiple files per partition prefix) or offset-reserved
  `pwrite`.
- `unpartitionedStageSink.Consume` block only moved 601 s → 575 s: its
  sub-threshold coalescing appends still copy under one mutex. Candidate:
  striped accumulators, only if it ranks in a future profile.
  **It ranked** — see "Producer-local stage-sink slabs" below.


## Producer-local stage-sink slabs (2026-08-21)

The residual above ranked. The first SF100 block/mutex profiles put
`unpartitionedStageSink.Consume` at **32.6% of all worker mutex delay**,
95.8% of it in the real `sync.Mutex.Unlock` handoff, **39–44 s of mutex
delay per worker per suite run** — because `appendBatchRowsBulk` ran INSIDE
the sink lock. Total serialized time is then bounded below by total copy
time, so amortizing lock ACQUISITIONS (the `partitionedShuffleSink`
consumer-local fix, e50fd1b, which drains a local slab into the shared
per-partition accumulator) cannot fix it here: the copy itself has to leave
the critical section.

**Design** (`internal/worker/stage_sink_accum.go`, kill switch
`WADJET_STAGE_SINK_ACCUM=0`, registered in `internal/optswitch` as
`stage-sink-accum`):

- A `Consume` checks a `stageSlab` out of a LIFO freelist and appends the
  batch's active rows into it with **no sink lock held**. Slabs carry rows
  between consumes; the freelist is explicit and registered (`slabAll`), not
  a `sync.Pool`, because a GC-dropped entry would silently lose rows.
- Because the unpartitioned sink has exactly ONE output stream (not
  `numParts` accumulators), a filled slab is written as **its own chunk**
  rather than copied a second time into a shared accumulator. The lock is
  held only to take stream ownership (the existing `flushing` flag) and to
  bump counters; encode + write stay outside it, as before.
- **Chunk sizing is unchanged**: a slab flushes at the sink's own
  `flushRows`/`flushBytesT`. LIFO checkout keeps a serial producer on one
  slab, so serial output is byte-identical to the pre-change accumulator
  (`TestUnpartitionedStageSink_SlabSerialParity` compares whole files).
- **Memory**: the natural bound becomes (concurrent consumers × slab), so
  the sink charges every append to a `bufferedBytes` counter at accumulate
  time and flushes early once the total passes
  `stageSlabBudgetFactor (4) × flushBytesT` — worst case ~64 MB per sink
  (2× the old two-accumulator peak); chunks shrink only when high consumer
  parallelism meets wide rows, which is exactly where the old bound would
  have been blown instead.
- **Finalize drains every registered slab** before the stream footer
  (`drainAllSlabs`); `TestUnpartitionedStageSink_SlabFinalizeDrain` is the
  row-loss gate (8 producers, random batch sizes, key-multiset comparison).
- Durability/upload order is untouched: this sink never uploads inside
  `Consume` — `uploadUnpartitionedSpill` runs after `Finalize`, so
  `--shuffle-durability` behaviour is unchanged.
- One shared read escapes the lock: `appendBatchRowsBulk` calls
  `HasNulls()` on a view column's BASE, which for a post-join view is the
  join's shared build batch, and `Bitmap.HasNulls` memoizes into a plain
  field. Those bases are warmed under the sink lock (a handful of cached
  loads) before the out-of-lock append.

**Local measurement** (`BenchmarkUnpartitionedSinkConsume_Producers`,
16 producers × 2048-row batches, tmpfs spill, 5900X): sink mutex delay
**964 ms → 57 ms (−94%)**; total profiled mutex delay 966 ms → 358 ms, the
remainder being runtime allocator locks from slab growth in a short run.
Wall time is indistinguishable locally — an INTERLEAVED A/B at 8 producers
(6 alternating pairs, same binary, `WADJET_STAGE_SINK_ACCUM=0` as control)
gives control 44.4–69.8 µs/op vs treatment 50.7–62.2 µs/op, arms fully
overlapping — because this box's sink is bound by the serialized write
syscall, not the lock: the CPU profile puts 69% (control) of in-`Consume`
samples in `syscall.write`, and throughput is independent of producer count
in BOTH arms. (Non-interleaved runs on this box drift by up to 50% between
samples of the SAME binary; only the interleaved pairs are usable.) A machine whose page-cache
writes are faster than its row gather is the regime the change targets, so
**the SF100 A/B (`WADJET_STAGE_SINK_ACCUM=0` as the control arm) is the
verdict**, not the local wall clock.

### Audit of the other sinks in `internal/worker`

| Sink | Shape | Action |
|---|---|---|
| `unpartitionedStageSink` | row copy inside the sink mutex | **changed** (above) |
| `partitionedShuffleSink` | per-partition locks + consumer-local slabs since e50fd1b (64% → 0.8% of worker mutex block) | unchanged — slab-as-chunk would need `numParts × consumers` chunk-sized buffers |
| `shuffleStreamSink` (build-cache pre-scan) | chunk-per-consume ENCODE under one mutex, no accumulator | unchanged — nothing to localize; coalescing it changes chunk granularity for the build-cache reader and it did not rank in the profile |
| `gatherReplySink` | no accumulator, publishes per consume; serialized by `fragmentGatherSink.mu` | unchanged — coordinator-reply volumes |
| `fragmentExchangeSink` / `fragmentUnpartitionedSink` | adapters; lock only for lazy construction | unchanged |
| `exec.CollectSink` / `exec.BatchSink` | hold their mutex across `FlattenForConsumer` + Sel copy — same "real work inside the lock" shape | unchanged, noted: not stage sinks and absent from the profile; a candidate if the fast-path/gather collector ever ranks |
