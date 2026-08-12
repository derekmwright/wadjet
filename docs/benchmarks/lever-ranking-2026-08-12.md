# SF100 lever ranking — 2026-08-12 re-profile

The clean profile-weighted lever ranking the 08-11 handoff asked for,
now possible because the frozen-spin firings are gone in shipped
config (WSHF read-staging 9df82ca + hot-tier parquet pread ebd006f).

## Run provenance

- Bin ebd006f (== main code), default lever config, runs=2,
  `-var=block_profile_rate=20000`, results/20260812-132257, evidence
  `~/wadjet-artifacts/20260812-reprofile/run/`.
- **Zero trap firings** (2/2 firing-free SF100 runs in shipped config
  today), 44/44 rows, no zero-row.
- Walls R1 357.6s / R2 570.5s — WITH profilers; the same-day clean
  reference (no profilers, same window class) is 280.5/354.0
  (results/20260812-120237). Block sampling taxes contended paths, so
  R2/R1 inflates to 1.60× here vs 1.26× clean — the inflation itself
  points at contention, consistent with the block findings below.
  R2 drifters under the profiler: Q04–Q07 (2.6–2.7×), Q11 (2.1×).
- The 08-11 "R1 422–429s vs 322.5 ref" question is resolved as
  contamination/window: today's clean R1s were 280.5–299.0.

## CPU ranking (worker-*-cpu.prof merged: 8,214 CPU-s over 3 workers)

| Rank | Subsystem | Weight | Anchor symbols |
|---|---|---|---|
| 1 | zstd parquet decompression | 24.2% cum (1,983s) | `zstd.(*sequenceDecs).decodeSync` 13.8% flat + internal memmove 366s + `bitReader.fillFast` |
| 2 | memmove complex | 15.4% flat (1,264s) | 29% zstd-internal, **28% `batch.(*BytesColumn).SetFrom`** (358s — row-wise bytes gather), 11% `runtime.growslice` (undersizing), 5% `scan.copyNativeDataDirect` |
| 3 | join/agg hash ops | ~12% | `intHashTable.Get/PutNoGrow/GetOrInsertNoGrow`, `inlineIntProbe`, `gatherBuildVector` 653s cum, `fillEmptyEntries` |
| 4 | parquet decode kernels | ~7% | `DecodeBitPacked`, `resolveNativeDictionary`, `DecodePlainByteArray` |
| 5 | s2 shuffle/peer compression | 4.2% cum (348s) | `s2.encodeBlockGo` |
| 6 | pread syscalls | 3.5% (290s) | `Syscall6` — the accepted price of the two pread levers |

## Contention ranking (worker-*-block.prof, ex-idle)

| Rank | Site | Share of mutex block | Note |
|---|---|---|---|
| 1 | **`partitionedShuffleSink.appendAndMaybeFlush`** | **64.3%** (2,690s blocked) | one mutex serializes every producer goroutine feeding a shuffle sink — the barrier-phase serializer |
| 2 | `unpartitionedStageSink.Consume` | 11.2% | same shape, unpartitioned sink |
| 3 | `memory.HeapBackpressureActive` + `heapPressureExceeded` | 13.2% | pressure gauge read under mutex on hot paths |
| 4 | `DecodeAheadIter` window waits | ~16% of Cond.Wait | pacing, mostly by design |

(selectgo/chan tops are parked idle goroutines — NATS `waitForMsgs` is
84% of Cond.Wait — not levers.)

## Recommended lever order

1. **Shard the shuffle-sink append lock** (per-partition buffers or
   lock striping in `partitionedShuffleSink`; same treatment for the
   unpartitioned sink). 75% of real contention, sits exactly in the
   barrier phase that drives the sub-trap pause windows and the
   remaining R2/R1 gap. Architecture-level, kill-switchable.
2. **Bulk bytes gather** — replace row-wise `BytesColumn.SetFrom` in
   the gather/materialize paths with the existing `BulkSet`/arena
   shape (+ pre-size the growslice sites). ~400–500 CPU-s.
3. **Heap-pressure gauge → atomic snapshot** (small, clean; removes
   13% of mutex block from hot paths).
4. **zstd bill** — biggest single subsystem but needs design, options:
   decoded-rowgroup cache for cache-hot re-reads (R2 re-decompresses
   the same bytes today), write-side level/codec experiment for base
   tables. Interacts with the memory ledger — design first.
5. Join hash-path micro-work (prefetch, probe layout) — only after
   the above; it's diffuse.

Open residuals carried: sub-trap GC-pause intervals (present both
arms of the morning pair), Q17-R2 2.4× outlier (morning pair, absent
in this profiled run — intermittent), Q19 vsig last-digit float
merge-order wobble (both directions observed, rows exact).
