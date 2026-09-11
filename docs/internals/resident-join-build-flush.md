# Resident join build flush

Source: internal/engine/exec/join.go — HashJoin.residentBuildBatch, moved 2026-09-11 (#1026)

residentBuildBatch returns the in-memory build batch an arena entry points
at, or nil when that entry's rows are no longer resident.

Two ways an entry stops being resident, and both are answers rather than
errors here:

  - Its PARTITION WAS EVICTED. spillOneInMemoryPartition writes the
    partition's batches to disk and nils their h.buildBatches slots, leaving
    the arena entries that point at them in place — its correctness argument
    covers the in-memory PROBE path (partition routing diverts a probe row
    for a spilled partition to disk before any hash lookup), and the
    build-side flushes are not that path. They walk the arena directly, so
    they used to dereference the nil slot and take the whole query down with
    a nil pointer panic on any spilling RIGHT/FULL/RIGHT-ANTI join (#550).
    Those rows are NOT lost by skipping them: NextFlush replays every
    spilled partition from disk through a temp join whose own flush emits
    them, and that replay reads the partition's COMPLETE contents — the
    batches evicted here plus every row that arrived for the partition
    afterwards, which was never indexed and has no arena entry at all.
    Emitting them here as well would double them.
  - The index outruns the slice, which nothing is expected to do; it was
    already tolerated by two of the three callers and is kept.
