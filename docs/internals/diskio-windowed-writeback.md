# Diskio windowed writeback

Source: internal/engine/diskio/diskio.go — package diskio, moved 2026-09-11 (#1026)
Superseded: Asynchronous writeback does not guarantee completion within one window, and DONTNEED cannot guarantee spill bytes never compete for cache. The implementation advises the preceding window and performs the blocking whole-file wait/drop at Spill Finish.

Package diskio bounds the page-cache impact of the worker's large
sequential file writes (operator spill files, local cache downloads,
stage-output files).

Mechanism: windowed asynchronous writeback (PostgreSQL's
checkpoint_flush_after, RocksDB's bytes_per_sync — their DEFAULT,
non-strict shapes). As each window of windowBytes completes,
asynchronous writeback is started for it
(sync_file_range(SYNC_FILE_RANGE_WRITE)), so dirty pages reach the
device within one window of being written instead of waiting on the
~30s kernel flusher. Steady-state dirty footprint per writer ≈
write-rate × device-latency. There is deliberately no per-window
blocking wait — the strict variant (RocksDB's off-by-default
strict_bytes_per_sync) serialized multi-GB cache downloads behind
device writeback at SF100 and regressed exchange-heavy queries 50-110%
(run 20260610-203304); see Flusher.wrote.

Why dirty pages specifically: they cannot be reclaimed until written
back, so a multi-GB write flood (cache downloads + spill) forces kernel
reclaim to evict whatever IS cheaply reclaimable — the clean mmap'd
cache-file pages concurrent tasks are still walking. That eviction is
the major-fault mechanism behind the PR #112 regression
(project_mmap_selfrelief_postmortem_2026-06-10); bounding the write
side attacks the cause without any reader coordination.

Class picks the fate of written-back (now clean) windows:

  - Spill: read back at most once, much later — each clean window is
    FADV_DONTNEED'd immediately, so spill bytes never compete with the
    mmap'd cache pages at all. Read-back streams from NVMe.
  - KeepResident: mmap-walked or uploaded right after the write — the
    windows stay resident (only the dirty bound applies), so the
    imminent readers take minor faults, not majors.

The write mechanism is gated on --bounded-dirty-writes (default true
since the SF100 suite-neutral validation; =false restores
kernel-writeback-only), with Spill-class writers additionally kept
active by the drop-behind default below. When fully disabled, NewWriter
returns the file itself and a nil Flusher, so the only cost is one
atomic load at writer construction.
