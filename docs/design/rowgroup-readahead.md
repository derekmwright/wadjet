# Row-group I/O-ahead (MADV_WILLNEED)

## Problem

After the single-pass drop-behind fix (single-pass-drop-behind.md)
killed the refault-sensor tax, steady-state SF100 runs remained ~1.5×
their own cold run (results/20260808-210616: cold 423s / steady 685s).
The dominant stall bucket moved to **token stalls: 575-627s per worker
per suite** (pressure stalls 0-5.4s).

Mechanism: steady-regime scans read NVMe-cached parquet through a
whole-file mmap (`buildParquetFromLocal`). The decode-ahead iterator's
workers (`scan.DecodeAheadIter`) hold a CPU token "only for the decode
itself" — but in the saturated page-cache regime the decode faults
synchronously on nearly every page it touches. Fault latency extends
token holds; other decode workers and morsel consumers park behind the
shrunken effective capacity, and the wait books as token stalls.
`MADV_SEQUENTIAL` alone cannot fix this: column projection jumps
between column-chunk offsets, defeating linear kernel readahead. Cold
runs don't pay it because their tee-written bytes are still
page-cache-hot and the S3 streaming path is pipelined.

## Mechanism

The decode-ahead iterator already knows the future: row groups are
assigned in file order, and parquet metadata gives every projected
column chunk's exact byte range (dictionary-or-data page offset +
`TotalCompressedSize` — the same page_reader.go convention).

- `scan.DecodeAheadOpts.Advise func(off, n int64)` — a seam the
  iterator feeds with projected-chunk ranges. As group N is assigned
  to a decode worker, the advise cursor extends to group
  N + workers + 1: the advice leads decode by roughly one full
  assignment wave — long enough for readahead to land the pages,
  short enough that a churning LRU hasn't re-evicted them. Ranges are
  claimed under the window lock, issued after release (madvise is a
  syscall). Decode workers are joined by Close, so no advise call can
  outlive the munmap that follows.
- `worker.willNeedAdviser(mmapData)` — the closure wired in
  `buildParquetState` whenever the file bytes are a real mmap
  (local-tier opens: base-table cache hits, prefetched downloads,
  owned temps). It clamps to the mapping, page-aligns downward, and
  issues `madvise(MADV_WILLNEED)`: asynchronous kernel readahead into
  the page cache. On hot pages it is a no-op; on the S3-streamed path
  there is no mmap and the hook is not wired.

Serial `RowGroupIter` (decode-ahead disabled) is out of scope — no
token pool, and the kill switch story stays one-dimensional.

## Why not …

- **Buffered pread decode path**: converts the whole mmap zero-copy
  read stack to buffered I/O — large refactor, allocs on the hot
  path, and it re-reads what readahead can deliver for one advisory
  syscall per chunk.
- **mlock/residency pinning**: fights the memory ledger for the same
  RAM and needs a budget policy; only worth revisiting if WILLNEED
  proves insufficient (pages re-evicted between advise and decode).
- **Larger advise distance**: advising the whole file up front is
  just readahead-vs-LRU roulette in the saturated regime; one wave
  ahead tracks the decode rate by construction.

## Observability & kill switch

- `readahead_advise_bytes` on the wlog "drop-behind stats" marker —
  engagement proof (0 when disabled or never mmap-backed).
- `WADJET_ROWGROUP_READAHEAD=0` kills the wiring (worker-side);
  forwarded into edge containers by cap-wrapper.sh.

## Validation shape

SF100 same-window pair vs main. Verdict: run-2 token_stall_ms
materially below 575-627s/worker, run-2 wall toward ≤ 1.2× run-1,
run-1 unchanged (advise is no-op on hot pages), rows + value sigs
identical, readahead_advise_bytes > 0 on every worker.

## SF100 verdict (pair 20260808-223206 ctl daece0d-bins / 225512 trt 65cdf61)

KEEPER — default stays ON. Same-window pair, runs=2 each, wlogs both
arms, all EC2 destroyed.

- **Steady −16.7%**: run 2 667→555s; steady/cold ratio 1.71×→1.47×.
  The movers are broad and all one-directional — Q14 −58%, Q22 −43%,
  Q19 −38%, Q20 −35%, Q17 −28% — the signature of an I/O-shape
  change, not single-query variance.
- **Cold unchanged**: run 1 389→378s (−2.8%, in-band) — the no-op-on-
  hot-pages design held.
- **Engagement**: readahead_advise_bytes 104-126 GB per worker (both
  runs' projected chunks, advised once per decode pass).
- Cumulative token_stall_ms: 2131s→1857s summed across workers
  (−13%). Wall moved more than the stall ledger — consistent with
  faults ALSO stretching decode/consume spans that never park (stall
  counters only see waiters).
- Correctness: rows identical 44/44 both runs; single vsig delta is
  the known Q19 1-ULP float-order flicker (each arm internally
  consistent).
- Side observation: the cluster first-touch S3 total (single-flight
  metric, identical code both arms) read 24.9 GB ctl / 32.0 GB trt —
  ±20% window wobble from same-worker miss races; `peer_fallthroughs`
  stayed 0/0/0 in both arms, so the single-flight mechanism verdict
  (scan-affinity.md) is unaffected.

`WADJET_ROWGROUP_READAHEAD=0` reproduces the ctl arm. Residual: run 2
still 1.47× run 1 — remaining candidates are the never-parked decode
spans above and the raw NVMe bandwidth floor.

**Residual resolved 2026-08-09**: rowgroup-touch-ahead.md — the
bandwidth floor was refuted (device ~1% utilized during steady decode)
and the inline-fault stretch confirmed (+91% decode ns/byte run 2,
measured by the new decode-span instrument); forced page-in behind the
WILLNEED took the steady ratio 1.483 → 1.242 (steady −17.6%).
