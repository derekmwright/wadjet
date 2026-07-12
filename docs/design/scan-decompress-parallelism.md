# Scan-side zstd decompression parallelism

Status: DRAFT — awaiting review. No code yet.
Evidence: SF100 profiling run `results/20260711-225605` @ `9723c54`
(= main `7a1a28e` + PR #215 block/mutex profiling), 22/22 row-identical
to baseline `20260709-175846`, suite 49m42s.

## 1. The defect

`internal/storage/parquet/decompress.go:47`:

```go
zstdDecoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
```

One process-global `zstd.Decoder` with **one** internal decoder state,
shared by every goroutine in the worker. `DecodeAll` acquires a state from
the decoder's internal channel; with concurrency 1, every concurrent
parquet page decompression in a 16-vCPU worker queues on a single slot.

The line is original to the first parquet-reader commit (`1b36fd3`) and
was never revisited. It could not appear in any profile until now:
nothing in the tree ever called `runtime.SetBlockProfileRate`, so block
profiles were structurally empty until PR #215.

## 2. Evidence from the 2026-07-11 profiling run

Per worker (all three agree):

| Signal | Value |
|---|---|
| Suite wall | 2,985 s |
| CPU consumed | 4,142–4,439 s ≈ **1.4 of 16 cores** (91% idle) |
| Blocked in `decompressZstd → DecodeAll → chanrecv1` | **15,300–16,700 goroutine-seconds** (≈5.6 goroutines queued on average, suite-long; bursty — deep during scan phases, empty between) |
| zstd decode CPU (`sequenceDecs.decodeSync`) | ~654 s flat = #2 CPU consumer (~15%) |
| Next-largest work-path blockers | filePrefetcher ~5.6 ks, shuffle drain ~5.4 ks, `CircuitStore.Put` ~3.2 ks |
| Mutex contention (whole suite) | 364 s total — negligible; locks are not the problem |

Interpretation: during scan phases the decoder slot is saturated and
column-reader goroutines (`ReadRowGroupNative.func2` fan-out) queue
behind it. Head-of-line blocking delays row-group completion, which
starves downstream pipeline stages, which is a large part of why worker
CPU sits at 1.4/16 cores — the same utilization observed in the 2026-05
block profile that predated morsel/streaming-exchange/late-mat. Those
three features fixed real things; this bottleneck sat underneath all of
them, invisible.

Why the gap table implicates the scan path: the gap vs Trino is
broad-based, not tail-only. Q03/Q04/Q05/Q10/Q13 — plain scan→join→agg
shapes with no exotic plan — run 14–25× behind the cold-S3 Trino
reference. A plan-quality or semi-join-specific lever cannot explain a
25× deficit on Q04.

Scope note: only externally-produced **zstd** parquet is affected.
Wadjet's own writer emits snappy (`writer.go:70`, per-call codec, no
shared state). The SF100 bucket is Polars-produced zstd; zstd is the
modern default for Spark/Trino/Polars lakes, so real-world reads hit
this path. This is also why no SF10 arc ever surfaced it (SF10 bucket is
Wadjet-generated snappy) — and why **SF10 cannot validate the fix**.

## 3. Design

Replace the single shared decoder with per-CPU decode concurrency:

```go
zstdDecoder, err = zstd.NewReader(nil,
    zstd.WithDecoderConcurrency(min(runtime.GOMAXPROCS(0), 16)),
    zstd.WithDecoderLowmem(true))
```

- `DecodeAll` is the only call path (no streaming reads); klauspost pools
  N decoder states internally and allocates them lazily. Concurrency N
  means N pages decompress in parallel; callers beyond N queue exactly as
  today.
- Output buffers stay on the existing `getZstdBuf`/`putZstdBuf` pool —
  unchanged, already sized by `uncompressedSize`.
- `WithDecoderLowmem(true)` keeps per-state history allocation small for
  the DecodeAll path (states hold FSE tables + literal buffers, low
  single-digit MiB each with lowmem; parquet pages are ≤1 MiB
  uncompressed in practice). Worst case 16 states ≈ tens of MiB per
  worker process — inside the envelope, transient, and of the same
  character as today's single state ×N. No ledger charge: mirrors the
  validated pool/alloc-primitive shape (`feedback_no_ab_on_architectural_perf`).
- Cap at 16 so a future larger host doesn't multiply states unbounded.

Alternatives considered:
- **Per-call `zstd.NewReader`** — one state alloc + FSE table build per
  page; GC churn on the hottest path. Rejected.
- **Pool of N concurrency-1 decoders** — semantically identical to
  concurrency-N with more code and a second pool to size. Rejected.
- **Raise concurrency on the ingest/write side too** — out of scope;
  writer is snappy and write path is not scan-critical.

## 4. Gates (parquet package = safety-critical)

1. Existing round-trip + decoder unit suite green (no format-semantics
   change — same `DecodeAll` API, same buffers).
2. New regression test: N-goroutine concurrent `Decompress(CodecZstd)`
   round-trip under `-race` (also guards the pool interaction).
3. Micro-bench: `BenchmarkDecompressZstdParallel` before/after —
   expect ~linear scaling to min(cores,16); flat single-threaded cost.
4. TPC-H SF0.01 correctness gate (all 22) — mandatory for any parquet
   change.
5. `tpch-harness --mode=local` (S3 source mode reads the snappy SF10
   sample — exercises the codec dispatch, not the zstd queue).
6. **SF100 same-window pair** (needs deploy approval): main control +
   fix arm. SF10 is not a valid gate here (snappy data). Success
   criteria: 22/22 row-identical; scan-heavy band (Q01, Q03, Q04, Q06,
   Q10, Q13) improves beyond the ±15% single-pair noise envelope, or the
   fix is re-examined — no threshold tuning, no partial keep.

## 5. Honest bounds and what this does NOT fix

- The queueing number (16.7 ks/worker) is goroutine-time, not wall; the
  wall gain cannot be derived from it precisely. The claim this design
  makes is structural: scan decode becomes CPU-parallel instead of
  slot-serial. The SF100 pair measures the rest.
- Exchange-bound legs keep their cost: s2 shuffle encode (~9% CPU),
  string rematerialization (`BytesColumn.SetFrom` = 27% of memmove),
  stage output to S3 (`CircuitStore.Put` blocking) are untouched. Q18/Q21
  keep a large exchange component regardless of scan speed.
- After this lands, the bottleneck re-rank (dyn-filter gate redesign vs
  physical exchange costing vs streaming-exchange extension) must be
  re-litigated against a **post-fix** profile — today's profile is
  dominated by a defect that distorts every downstream estimate.
