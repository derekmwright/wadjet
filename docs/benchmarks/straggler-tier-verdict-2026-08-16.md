# Straggler tier named: un-overlapped prefetch-take wait (2026-08-16)

**Verdict: the run-level slow mode is the file prefetcher's download
wait, paid synchronously inside src.Next at first probe-file open.**
One window with the acquisition counters (bin `4892d76`,
`results/20260816-004847`, 4 runs, walls 201.8/172.7/161.4/158.6s —
R4 ties the record cluster) caught the mode in R1 and R2 and named
the tier on every affected task.

## The counter evidence

join-6 (q08 family), same window:

| shape | src_ms | acquisition line |
|---|---|---|
| clean (R2–R4, 2 of 3 in R2) | 2.4–3.9s | `acq_basecache_files=1 acq_basecache_ms=0–1` |
| degraded (R1 ×3, R2 ×1) | 8.1–9.7s | `acq_prefetch_files=2–3 acq_prefetch_ms=5786–7227` |

The 5.8–7.2s prefetch-take wait IS the src inflation, and the src
inflation IS the q08 slow mode (elapsed 20–21s vs 13.5–14s clean —
the same ~6.5s). Window-wide, prefetch waits >2s sum to ~300s:
~203s in the cold first minute (scan-0 at 16–17s/task, scan-1 at
6.5–7s), ~60s spread over R2's joins (join-2, join-6, join-19), and
ZERO after 00:54 — R3/R4 resolved every open from local cache in
0–1ms, which is exactly why they are the record-cluster runs. The
mode's per-worker randomness is per-worker cache state; the earlier
windows' q09-family stragglers (not caught this window) are the same
acquisition-wait class pending their own catch.

## The mechanism

`openNextFile` starts the download-ahead prefetcher lazily AT THE
FIRST FILE OPEN ("start the pipeline on first open"). For a broadcast
join task, that is AFTER dispatch and after the build-side broadcast
cache load — so the first probe file's download proceeds with nothing
to hide behind, and the consumer fleet idles at eff 3–8 while
src.Next blocks in prefetch.take. The file list is known at dispatch;
the wait is architectural only in the sense that nobody starts the
transfer until the moment the bytes are needed.

## The lever (next change)

Start the prefetcher at source Init (or task start) instead of first
open, so probe-file downloads overlap the build load and any earlier
phase work. Bounded expectations, stated before measuring: overlap
can hide at most the build-load + pre-probe span (a few seconds on
these joins — likely most of the 5.8–7.2s join waits); the cold
scan-0 waits (16–17s) are bandwidth-bound first-touch downloads that
overlap cannot fully hide — that is the cold-run floor, and it is a
different (smaller) claim than "slow mode eliminated". Implementation
must check Init ordering across all fragment runner paths and the
prefetcher's ctx lifetime; not a late-night edit.

## Correctness

88/88 rows; vsigs spot-identical to prior windows; zero reaps. Suite
walls in the record cluster on R3/R4 — the counters cost nothing
measurable.

## Window log

Preflight clean (zero orphans), on-demand, sha-pinned 4892d76.
Counters verified emitting at T+5min via SSM (a cold join-2 already
showed src 12.7s / prefetch 8.6s live). Completion monitor fired
correctly; destroyed before analysis; EC2 zero.
