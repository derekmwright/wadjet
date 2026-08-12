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

## SF10-capped zero-EC2 verdict (2026-08-10 night, bin ca856eb)

Shape: EDGE_CAP_MB=3072 EDGE_CPUS=4, --workers=1, runs=2,
--base-table-cache-bytes=6 GiB (recipe's 8 GiB × 3 processes does not
fit one shared local disk — 6 GiB × 2 does; same both arms).

- Engagement (worker wlogs): pread_chunks 95.7k, pread_bytes 29.6 GB ==
  readahead_advise_bytes (full scan volume staged through the fadvise
  seam); touch_bytes = touch_populate_bytes = 0 (no scan mmaps exist);
  pread_allocs 1.3k = 1.4 % pool miss rate.
- Correctness: row counts identical across modes and runs, no zero-row
  queries; 10-significant-digit value signatures bit-identical
  cross-mode on the same dataset (q01/q14/q19 probes + full-suite row
  parity). The harness's full-precision row_checksum flips run-to-run
  WITHIN each arm (mmap arm included) on aggregate-heavy queries —
  pre-existing sub-ulp parallel-merge-order noise, not a decode
  artifact; use value_sig, not row_checksum, for cross-arm verdicts.
- Walls: same-dataset cold pass over the 10 scan-heavy queries 92.3 s
  (pread) vs 95.0 s (mmap) — in-band. Full-suite steady R2 172.4 s vs
  169.7 s (+1.6 %, arms crossed different dataset states; no
  regression signal). The capped box does not fire the intermittent
  STW storm, so this gate validates no-regression + engagement; the
  drift verdict itself belongs to the SF100 same-window pair.
- Testbed note: every harness launch strands the previous launch's
  compaction outputs (compacted_*.parquet) in the shared data dir —
  35 GB of orphans across four launches filled the disk twice before
  diagnosis. Sweep them between launches until the harness cleans up
  after itself.

## Refinement (2026-08-11, follows the pair verdict below): hot/cold backing split

The pair priced full-pread's cold cost: R1 decodes just-written spill
temps whose pages are cache-resident — mmap read them zero-copy, pread
paid an alloc+memcpy per chunk (+15.9 % cold, 42 % of SF100 chunks
over the 32 MiB pool ceiling). The refinement matches backing to
thermal state:

- **Just-written temps** (S3 stream-to-spill, prefetch downloads):
  zero-copy mmap, as before the lever. Pages are resident by
  construction; minor faults on them were never implicated in the
  drift.
- **Base-table cache hits** (including the decode-ahead pre-open cache
  tier): pread-staged pooled buffers — the potentially-cold NVMe reads
  whose fault class drives the STW interaction.
- Chunk pool ceiling 32 → 128 MiB (SF100 lineitem chunks run tens of
  MB; transient footprint unchanged — oversized chunks allocated fresh
  anyway).

`WADJET_SCAN_PREAD=0` still forces mmap everywhere. The worker parity
test pins the tier split: a cold (stream) pass must not move the pread
counters; a cache-hit pass must.

## SF100 confirm pair (2026-08-11 day, bin cb706cd) — refinement KEEPER

Pair: ctl 20260811-1450xx (`scan_pread=0`) / trt (refined split), same
shape as the night pair. Rows identical across all four runs × both
arms (Q21 = Q02 = 100), EC2 zero after.

- **Cold reclaimed and reversed: trt R1 322.5 s vs ctl 360.2 s
  (−10.5 %).** The full-pread arm's +15.9 % cold cost is gone. A key
  reframe from the markers: R1's scan volume converges onto CACHE HITS
  almost immediately (the miss tee populates fast, and the prefetcher
  skips resident files), so even in R1 the pread tier dominates
  (47.4 GB staged vs only ~1.5 GB touched via the residual mmap
  paths) — the cold win comes from pread of page-hot cache files
  beating the mmap+touch machinery, not from the stream-temp mmap
  revert alone.
- **Steady mechanism stays dead**: trt R2 decode 9.7–12.3 ns/B, STW
  ≤ 5.1 ms/cycle late-run, every worker, while ctl drew a mild 1.234×
  drift window. Excluding the incident query below, trt R2 = 280.2 s
  vs ctl 418.9 s (−33.1 %).
- **Headline ratio is incident-confounded, not mechanism**: trt R2
  TOTAL 522.2 s (1.62× vs its low R1) is entirely one query — Q21-R2
  242.0 s vs ctl 25.6 s (+216 s). CORRECTED anatomy (wlog timeline,
  same day): the stall-watchdog FROZEN-SPIN fired at 15:23:38 during
  an EARLIER query; its SIGQUIT drained + restarted the worker in ~7 s
  and the suite ran at full cadence for 3.5 more minutes — the restart
  itself was absorbed invisibly. Q21 (15:27:10) then dispatched and
  ran normally for 14 s, after which THREE st-join-12 tasks on a
  DIFFERENT worker sat input-starved for ~227 s ("task progress idle;
  stopping AckWait extension" at idle=2m) and completed
  simultaneously at 15:31:11 — a classic dispatch-stall-arc
  input-wait, caught live with full wlogs. Probable chain: the
  restarted worker's drain cancelled 1,316 queued uploads and reset
  its peer-exchange state, so a repartition-11 partition was neither
  peer-fetchable (stale hint/token) nor yet durable (upload QoS
  backlog: ~10,000 s cumulative upload yield on that worker); the
  consumers waited out AckWait until redelivery / the late durable
  copy landed. Every other R2 query nets −139 s in trt's favor.
- **Handed to the dispatch-stall arc** (now the top open item): (1)
  the trap works but destroys its own evidence — wadjet swallows
  SIGQUIT as drain, so capture /debug/pprof/goroutine?debug=2 over
  localhost BEFORE signalling (or SIGABRT + GOTRACEBACK); 2/2 firings
  on pread arms vs 0/2 ctl is an open correlation question. (2) The
  Q21 stall class: post-drain shuffle-partition resolution takes
  minutes because failure falls through peer → durable-wait →
  AckWait-expiry redelivery instead of an eager re-resolve; the
  coordinator log (not grabbed by the pair script) is the missing
  witness — grab it next firing.

## SF100 same-window pair VERDICT (2026-08-11, bin 1ab474e) — DRIFT KILLED

Pair: ctl 20260811-113603 (`-var=scan_pread=0`, mmap + touch-populate)
/ trt 20260811-115448 (pread on), same binary, benchmark_runs=2,
sf100-distributed profile.

- **Drift: ctl 1.756× → trt 1.029×.** Walls ctl 312.4 / 548.6 s, trt
  362.0 / 372.5 s. Steady wall **−32.1 %**; whole pair −14.7 %. The ctl
  arm drew a strong drift window and reproduced the full documented
  anatomy: 2/3 workers R2-inflamed (decode 23.8 / 29.3 ns/B vs 9–13
  healthy) and exactly those two degrading to 15.8 / 12.4 ms STW per
  GC cycle (healthy stays ~4). The trt arm on the same window: ns/B
  9–13 both runs on every worker, pauses ≤ 7.5 ms/cyc late-run — the
  mechanism is gone, not paced around.
- Engagement exact both arms: trt pread_bytes 118.6 GB ==
  readahead_advise_bytes with touch_bytes = 0; ctl pread_chunks = 0
  with touch_populate covering 112.7 GB (kill switch verified live).
- Rows: all four runs × both arms identical per query (Q02 = 100), no
  zero-row queries.
- **Cold regression (open residual): ctl R1 312.4 → trt R1 362.0 s
  (+15.9 %).** Mechanism, partially quantified: R1 decodes just-written
  spill files whose pages are cache-hot — the mmap path read them
  zero-copy, the pread path pays a buffer allocation + memcpy per
  chunk, and at SF100 chunk sizes 42 % of stagings (99 k of 237 k)
  overflow the 32 MiB pool ceiling and allocate fresh. R1 decode ns/B
  moves ~+11 % (9.3–10.8 → 9.4–12.7), which does not account for the
  full +49.6 s — remainder unattributed. Refinement that follows the
  mechanism: keep zero-copy mmap for just-written temps (stream-to-
  spill, prefetch — pages guaranteed resident, never implicated in the
  drift) and stage via pread only the base-table-cache hits, where the
  cold-fault class actually lives; plus larger pool classes for
  SF100-sized chunks.
- Incident, feeds the dispatch-stall arc: the stall-watchdog fired for
  the FIRST time (trt worker i-075b, R2, FROZEN-SPIN unresp_ms=5157
  cpu_jiffies=151 → SIGQUIT). The frozen-spin class therefore SURVIVES
  the scan-pread lever — consistent with it living in the WSHF shuffle
  mmap walk, which this lever does not touch. Design flaw exposed:
  wadjet handles SIGQUIT as graceful drain, so the trap produced a
  drain + restart instead of the goroutine dump it was built to
  capture (the FTE retry re-ran the lost tasks; rows unaffected, trt
  R2 wall slightly inflated — the drift kill is conservative).

## Hot-tier retry (WADJET_SCAN_PREAD_HOT, 2026-08-12)

The just-written mmap exemption above is withdrawn behind a new flag,
default on. Two pieces of evidence moved it:

1. The +15.9%-cold pricing that motivated the exemption predates the
   128 MiB pool classes (cb706cd); the alloc component (42% overflow at
   32 MiB) is largely absorbed now.
2. The 2026-08-12 SF100 pair for the WSHF read-staging lever
   (shuffle-pread-reads.md §Validation) fired the frozen-spin trap
   twice with WSHF fully converted and parquet ~96% pread by bytes,
   and the SIGABRT dump put the non-preempting M inside a
   ReadRowGroupNative per-column decode worker — the just-written
   parquet mmaps (S3-staged temps + prefetched downloads) are the
   prime surviving fault class.

With WADJET_SCAN_PREAD_HOT=1 (default) every local parquet open stages
via pread; =0 restores the hot/cold split exactly (`-var=scan_pread_hot`
on the benchmark deploy). Inert when WADJET_SCAN_PREAD=0. Engagement:
the cold pass now moves parquet.PreadStats — the parity test pins
per-flag-state behavior (TestScanPread_ParityWithMmapPath).

### SF100 pair verdict (2026-08-12 morning window, bin ebd006f)

ctl results/20260812-114313 (`-var=scan_pread_hot=0`), trt
results/20260812-120237 (default on); evidence
~/wadjet-artifacts/20260812-preadhot.

- **Gate PASSED: trap firings ctl 1 → trt 0.** Control fired at
  11:51:52 (Q10-R2, unresp 5.0s, worker death + clean a805b37
  recovery, Q10-R2 174s) — the disease was live in this window — and
  the pread-everywhere arm ran both suites without a single firing:
  the first firing-free SF100 run on any pread-era binary (previously
  6/6 arms fired).
- **Cold cost REVERSED**: trt R1 280.5s vs ctl 299.0s (−6.2%). The
  +15.9% that motivated the exemption is fully gone with the 128 MiB
  pool classes.
- R2: trt 354.0s (1.26× its R1, no deaths) vs ctl 481.5s (1.61×;
  ex-firing 307.5s ≈ 1.07× — mild morning window). Pair total
  −18.7% (634.5 vs 780.5).
- Rows: 44/44 both arms, no per-query mismatch, no zero-row. Q19-R2
  vsig differs in the 10th digit (rel ~2e-10) — float SUM merge
  order, rows exact.
- Engagement: trt parquet pread 104–115 GB/worker (everything
  stages); ctl surviving workers ~104–112 GB (cache-hit tier) — the
  arms differ exactly in the just-written slice, and only ctl fired.
- Open residuals: (1) trt Q17-R2 55.6s vs 23.0 R1 — no retries, no
  firings; sub-trap pause tax (20 intervals ≥1 s gc_pause_delta per
  30 s on trt vs 27 on ctl) and/or probe-split share lottery — the
  drift-arc 1.6×-share thread; (2) sub-trap STW stretch class remains
  at reduced severity — revisit only if it escalates or the clean
  re-profile pair surfaces it.
- Verdict: **KEEPER, default stays on.** The frozen-spin arc's
  firing gate is met in shipped default config; the trap-threshold
  question (4 s → 8–10 s) stays open pending soak.
