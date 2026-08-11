# Pread-staged scan reads: removing the mmap fault class from decode

**Status**: shipped (default on; kill switch `WADJET_SCAN_PREAD=0`)
**Date**: 2026-08-11
**Prior**: docs/benchmarks/cache-on-reprofile-2026-08-09.md (lever menu §"End
state"), docs/design/rowgroup-readahead.md, docs/design/rowgroup-touch-ahead.md

## Problem: R2 steady-drift is STW-pause degradation, not I/O

The 2026-08-11 drift diagnosis (four SF100 runs) established the run-2
slowdown mechanism end to end:

- Run-2 decode ns/byte inflates 2–4× (8–11 healthy → 20–46) on a
  per-window-varying subset of workers; the number of inflamed workers
  equals the drift magnitude.
- It is **not** I/O: inflamed windows decode 9.4 GB/min against
  2.1 GB/min of NVMe reads — the source pages are RAM-resident. majflt
  rates do not correlate. It is not network (rx near idle in R2), and
  not the touch ceiling (populate covers 100 % of touched bytes,
  0 drops).
- The smoking correlate is **GC stop-the-world pause length**: STW
  per cycle degrades progressively over the run life — 0.3–2.7 ms/cycle
  early-R1 → 10–38 ms/cycle by R2, on exactly the workers whose decode
  inflames. At 3–7 GC/s that is 10–30 % of wall inside STW plus the
  assist tax riding along. (The 08-09 GC-dose A/B ruled out cycle
  *count*; the live mechanism is per-cycle stretch.)

Why pauses stretch: run-2 decodes over **mmap'd NVMe-cache files**. A
goroutine that takes a page fault inside a decode span — or serializes
on `mmap_lock` during cache map churn (minflt runs 4–8 M/min in these
windows) — cannot reach a GC safepoint, so every STW in the process
waits on it. Run-1 is immune because the S3-streamed bytes are decoded
off freshly written, resident pages. Touch-ahead populate
(`MADV_POPULATE_READ`, 2c7f7a9) cut the worst of the *fault-throughput*
ceiling (steady −35 %) but cannot fix this: populate covers the touch
path, while decode and gathers still fault and still take `mmap_lock`.

## Fix: decode from pread-staged pooled buffers

The cache-on reprofile's documented end state (lever 3): cache-hit scans
decode the same way the S3 path effectively does — from heap bytes, with
no mmap under decode at all. A goroutine blocked in `pread(2)` parks in
a syscall at a GC-safe point; the fault class, and its STW interaction,
disappears structurally rather than being paced around.

### Mechanism

1. **`parquet.OpenFileReaderAt` / `parquet.NewReaderAt`** — a staged
   FileReader mode: footer read eagerly (as before), no whole-file
   buffer ever held. Each `ColumnPages` call returns a staged
   `ColumnPageReader` that reads exactly its column chunk's byte range
   `[chunk start, start + TotalCompressedSize)` from the fd with one
   `ReadAt` on first use, into a **pooled buffer** (size-classed
   sync.Pools, 64 KiB–32 MiB powers of two; larger chunks allocate
   fresh). `Close` returns the buffer. Offsets inside the reader are
   chunk-relative; the decode path above it is byte-identical.
2. **Worker local-tier opens go staged**: base-table cache hits,
   prefetched downloads, the decode-ahead pre-open continuation, and
   the S3 stream-to-spill path all open `NewReaderAt` over the local
   file's fd instead of mmapping it. The fd lives on
   `pendingParquet.file`/source and closes only after the iterator
   quiesces (same ordering munmap obeyed). The in-memory fallback
   (tests, no spill dir) keeps `NewReaderFromBytes`.
3. **I/O-ahead is preserved, faults are not**: the decode-ahead
   `Advise` seam now issues `posix_fadvise(FADV_WILLNEED)` on the fd
   (plus `FADV_SEQUENTIAL` at open) — same async kernel readahead the
   madvise path had, so the staging pread copies from page cache
   instead of blocking on NVMe under a held CPU token. The touch-ahead
   worker is not wired on this path: with no mapping there is nothing
   to fault in. The mmap machinery (toucher, populate, WILLNEED
   madvise, relief registry, drop-behind) remains intact behind the
   kill switch and for WSHF shuffle mmaps, which are unchanged.

### Lifetime contract (safety-critical)

Uncompressed (`CodecNone`) pages alias the chunk buffer; zstd pages
alias the pooled decompress buffer. The already-documented contract —
values are **copied** into their destination Vector before
`PageData.Release`, and nothing derived from a page outlives the
column read — now also carries the chunk buffer's reuse. Every
`ColumnPages` consumer (`readColumnNative`, `readColumnToAny`,
`readLeafColumn`) closes the page reader only after all copies.
Guarded by `TestReadRowGroupNative_StagedNoAliasing` (scan): decode an
uncompressed file staged, force pool reuse with different bytes of the
same size class, verify the first batches are bit-intact.

A failed staging read surfaces as an error from
`NextPage`/`NextDictionary` — never a silently empty column
(`TestStagedReader_ReadErrorSurfaces`); a silent empty would decode as
all-NULL vectors.

### Memory shape

Peak staged bytes ≈ decode workers (≤4/source) × projected chunks of
one row group in flight — the same order as the compressed bytes the
decode window already admits, and far under the window's uncompressed
`est` charge, so no new ledger seam. Pooling keeps the steady-state
allocation rate flat (`pread_allocs` marker near-zero after warmup):
an unpooled path would push the whole scan byte volume through the
heap and convert the GC pause problem into a GC frequency problem.

## Engagement markers

`drop-behind stats` wlog lines carry `pread_chunks`, `pread_bytes`,
`pread_allocs` (`parquet.PreadStats`). Expectations on an engaged run:
`pread_bytes` ≈ decode_bytes, `touch_bytes`/`touch_populate_bytes`
frozen (no scan mmaps to touch), `readahead_advise_bytes` still
climbing (fadvise path), majflt collapsed in scan-heavy windows.

## Kill switch

`WADJET_SCAN_PREAD=0` restores the mmap + WILLNEED + touch-populate
path byte-for-byte (`TestScanPread_ParityWithMmapPath` pins both modes
to identical results across cold and cache-hit tiers).

## Validation

- Unit: staged/slice parity across all five codecs, multi-row-group,
  bytes- and file-backed (`TestStagedReader_ParityAllCodecs`);
  decode-ahead parity over a staged reader
  (`TestDecodeAheadIter_StagedParity`); pool reuse aliasing guard;
  error surfacing; fd lifecycle.
- TPC-H SF0.01 correctness gate (all 22 queries) — required for any
  parquet-touching change.
- tpch-harness `--mode=local` before any EC2 run (standing local gate).
- SF10-capped zero-EC2 testbed: the steady regime reproduces there
  (+23 % ns/byte, 14× token stalls per the 08-08 recipe); expect the
  staged arm to hold ns/byte at the page-cache-hot floor.
- SF100 same-window pair (baseline first): gate on R2/R1 drift ratio
  vs the 1.24× post-populate floor, decode ns/byte per worker,
  gc_pause_delta_ms per cycle, rows 44/44.
