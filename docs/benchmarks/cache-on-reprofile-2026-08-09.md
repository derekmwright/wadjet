# Cache-on re-profile: the steady residual is fault-throughput-bound scan admission

**Date**: 2026-08-09 (late night arm, results/20260809-223131)
**Binary**: b4828f4 (engine-identical to main 3698d78), block_profile_rate=20000
**Config**: sf100-distributed profile with its new base-table-cache-on default
(150 GB NVMe, commit 7530412), benchmark_runs=2, ENA polling via the
`ena-poll` journald sampler (3698d78) — first arm with throttle context
attached automatically.

Continues docs/benchmarks/network-bound-diagnosis-2026-08-09.md, which
diagnosed the *cache-off* config. This arm re-derives the residual on the
shipped cache-on default, as that document required.

## Result

Rows 44/44 identical to baseline. Walls R1 377.5 s / R2 530.3 s (+40 %
run-2 drift; reference pair without profiling was 323.5/449.8 = +39 %).
The drift is fully reproduced under instrumentation.

## Network exonerated for the cache-on drift

Per-worker `ena-poll` series (30 s cadence, in wlogs):

- R1: NIC hot — initial S3 cache-fill bursts throttle inbound at 1–3 k
  events/s, then outbound shuffle-upload bursts throttle at up to ~2.4 k/s.
  Mean per worker ~90 MB/s rx / ~125 MB/s tx.
- R2: NIC mostly idle — rx collapses to ~13 MB/s (cache serving), tx
  roughly halves, throttle events sparse. The slow run does far *less*
  network I/O than the fast run.

Whole-pair per worker: ~46 GB rx / ~85 GB tx. With the cache on, the
dominant NIC direction is now **outbound** (S3 shuffle uploads ~44 GB PUT
per worker per pair + peer serves), and `bw_out_allowance_exceeded` >>
`bw_in`. Outbound-byte reduction is where any future network work should
aim, but network does not explain the run-2 drift.

## The run-2 in-host signature (wlog markers, run-split)

Per worker, R1 → R2:

| Signal | R1 | R2 |
|---|---|---|
| decode span time (Σ, per worker) | 571–596 s | 1166–1304 s (~2×, equal bytes) |
| decode ns/byte | ~10 | 17.5–27 |
| token_stall (Σ) | 155–222 s | 350–772 s (~3×) |
| decode bytes/worker | 55.8/56.2/59.9 GB (even) | 51.6/74.3/42.8 GB (skewed — cache-affinity concentration) |
| host busy | ~26 % | ~15–20 % (mostly idle) |
| PSI cpu/mem/io | ~0 | ~0 |
| majflt | 100–142 k | 46–141 k (faulting regime, NOT zero) |
| GC STW total (equal cycle counts) | 9.5/15.0/14.9 s | 24.5/23.7/39.2 s (2–2.6×; per-cycle 3–5 ms → 8–16 ms) |

Effective R2 scan throughput: 43–74 GB over ~530 s = **81–140 MB/s per
worker** from local NVMe capable of multi-GB/s. R1's S3-streaming scan
path outran R2's local-cache path.

Touch-ahead engagement: R2 touched ~72 GB ≈ every decoded byte — on
32 GB hosts nothing of the 150 GB cache set survives page cache between
queries, so every steady-state query re-faults its full input.

## Root cause: serial page-touch ceiling on the cache-hit read path

`internal/worker/rowgroup_touch.go`: the toucher is **one goroutine per
scan mmap** whose loop faults **one 4 KiB page at a time** (`sink +=
t.data[off]`). At NVMe fault latencies that is an ~80–160 MB/s ceiling
per active mmap — matching the measured R2 scan throughput. When the
toucher falls behind, decode workers fault inline **under held CPU
tokens** (decode_ahead.go documents exactly this hazard), which:

1. stretches decode spans ~2× (fault wait inside the span),
2. starves other decode workers of tokens (the 3× token_stall),
3. inflates GC STW pauses (goroutines blocked in a page fault cannot
   reach a safepoint; pause length 8–16 ms ≈ fault tail), and
4. leaves CPU, NIC, and NVMe all idle — the fractal inflation across
   stage types seen in every steady-window diagnosis.

One mechanism explains the whole R2 signature. The upload-QoS backlog
(142 goroutines queued in `uploadManager.acquireSlot` at R2 start) and
the GC pause inflation are downstream symptoms, not causes: upload drain
rates are similar across runs, and the 08-09 GC-dose A/B (4.15× fewer
cycles, walls flat) already ruled out cycle count.

Secondary finding: scan-affinity routing concentrates R2 decode bytes
unevenly (74.3 vs 42.8 GB worst/best worker); per-worker token stalls
track that skew, and stage barriers pace the cluster at the slowest
worker. Worth revisiting once the fault ceiling is lifted.

## Lever menu (architectural, in order of reach)

1. **Batch page-in in the toucher**: replace the per-page byte-walk with
   `MADV_POPULATE_READ` per advise range (AL2023 kernel ≥ 6.1), falling
   back to `preadv` into a scratch buffer where unsupported — bulk
   population at device streaming speed, no per-page fault round-trips.
2. **Parallel touchers**: a small pool per mmap (fault concurrency ≈
   NVMe queue depth), if (1) alone doesn't reach device bandwidth.
3. **End state**: cache-hit scans decode from `pread` buffers exactly
   like the S3 streaming path (no mmap faults under tokens at all) —
   removes the fault class entirely, including its STW interaction.

Validation path: SF10-capped zero-EC2 testbed first (the 08-08 recipe;
the same regime — +23 % ns/byte, 14× token stalls — reproduces locally),
then one SF100 pair. Gate: R2/R1 ratio and absolute steady wall vs the
323.5/449.8 reference; rows 44/44.
