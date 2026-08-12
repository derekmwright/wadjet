# Read-staged WSHF shuffle consumption (WADJET_SHUFFLE_PREAD)

Status: shipped (2026-08-12). Companion to
[scan-pread-reads.md](scan-pread-reads.md), which removed the same fault
class from the parquet scan side (ca856eb + cb706cd).

## Problem

The frozen-spin worker freezes (trap SIGABRTs, ~2 min recovery each, 5/5
all-time firings on pread-era binaries) were root-caused on 2026-08-11/12
to a GC/mmap interaction in the **shuffle consumption** path:

1. All workers reach a stage barrier within ~1 s of each other and start
   staging + walking multi-GB WSHF exchange inputs simultaneously.
2. The synchronized writeback storm (originally amplified by
   per-partition fsync at finalize — removed in 1ed4006) congests the
   NVMe device exactly while decode goroutines take page faults on the
   mmap'd WSHF files.
3. A thread stuck in a kernel page fault cannot reach a GC safepoint, so
   GC mark-termination STW stretches from microseconds to seconds —
   process-wide freeze; the stall trap kills the worker.

Evidence: five goroutine dumps across two nights
(~/wadjet-artifacts/20260811-reprofile, 20260812-validation); the
2026-08-12 validation run showed the fsync removal alone was necessary
but insufficient — the same GC mark-termination signature persisted with
no in-Go culprit attached, i.e. a kernel page-fault holder on the
remaining WSHF shuffle mmaps.

A goroutine blocked in a **read syscall** parks at a GC-safe point (the
scheduler hands its P away); one faulting on an mmap'd page inside a
decode span blocks the whole process's GC. Same mechanism as the R2
steady-drift diagnosis that motivated the scan-side lever.

## Change

All local .wshf opens in `cachedFileStreamSource` decode via sequential
`read()` into the existing `streamingShuffleReader` (heap scratch, reused
across chunks) instead of `mmap` + `shuffleChunkReader`:

| Tier | Before | After |
|---|---|---|
| S3/peer staged to NVMe (`openShuffleFile`) | write temp → mmap → walk | write temp → rewind fd → read()-stream |
| Tier-0 LocalStageCache hit (`openShuffleFromLocalFile`) | mmap cache file | read()-stream cache file (never unlinked) |
| Prefetched download (`openPrefetchedShuffle`) | mmap temp | read()-stream temp |
| NATS KV / no spill dir (`openShuffleInMemory`) | heap slice | unchanged (already heap) |
| `--streaming-shuffle-read` transport decode | unchanged (already heap) | unchanged |

Mechanics (`internal/worker/shuffle_pread.go`):

- The fd goes in at offset 0; `streamingShuffleReader` wraps it in a
  256 KiB bufio layer and decodes chunk-by-chunk with the exact same
  `readColumnData` path as the mmap reader, so the two modes cannot
  diverge on payload interpretation (the streaming reader also carries
  the stricter stream-side sanity bounds).
- `FADV_SEQUENTIAL` on the fd doubles kernel readahead (the walk is
  strictly front-to-back).
- Drop-behind: owned single-pass temps wrap the fd in
  `diskio.NewDropBehindReader` (FADV_DONTNEED one window behind the
  cursor, whole file on EOF) — the fd analog of the mmap walk's
  `dropBehindWalk`, same `WADJET_DROP_BEHIND` kill switch, same
  read-drop counters. Cache-owned files never drop (other consumers or
  a peer fetch may re-read them).
- The reader owns the fd (Close releases it); `releaseCurrentFile`'s
  existing streamReader/localPath handling covers release and unlink
  ordering unchanged.

### Just-written temps convert too

Deliberate divergence from the scan-side refinement (cb706cd kept mmap
for just-written parquet temps after pread priced at +15.9% cold):

1. That regression came from pread staging of large **random-access**
   column chunks — 42% overflowed the 32 MiB pool class and allocated
   fresh. The WSHF walk is strictly sequential and single-pass with one
   reused scratch buffer; there is no per-chunk allocation class to
   regress.
2. The freeze evidence sits **exactly on just-written staged temps**
   inside stage barriers — exempting them would exempt the trigger.

## Engagement markers

- `file_pread_files` / `file_pread_bytes` on the worker's 60 s
  "streaming shuffle read stats" wlog line. On a staged run expect
  file_pread_bytes ≈ what the local/s3/peer tiers serve (KV and
  streaming-transport opens don't count — they were never mmap'd).
- Read-side drop-behind bytes fold into the existing diskio drop
  counters.

## Kill switch

`WADJET_SHUFFLE_PREAD=0` (worker env) / `-var=shuffle_pread=0`
(deploy/benchmark/terraform) restores the mmap + `shuffleChunkReader`
path byte-for-byte; it remains the off arm for same-binary A/Bs.

## Validation

- Unit + race: worker package green, including new
  `TestShufflePread_StagedParityAcrossModes` (WSHF + WSHC through the
  staged path, both modes, identical results + temp-unlink contract),
  `TestShufflePread_LocalStageCacheNotUnlinked` (tier-0 ownership +
  ledger size), `TestShufflePread_DropBehindEngages` (fd drop-behind
  counters advance on a >2-window walk).
- TPC-H SF0.01: 22/22.
- tpch-harness --mode=local --slice=small: PASS vs baseline.
- SF100 pair (pending): gate is **trap firings → 0** and R2 ex-firing ≈
  R1; off arm `-var=shuffle_pread=0`. Do not change the trap threshold
  in the same window.
