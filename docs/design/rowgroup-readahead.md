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
