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
