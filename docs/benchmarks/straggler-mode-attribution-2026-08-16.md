# Run-level straggler mode: attributed to src-side input acquisition; everything else exonerated (2026-08-16)

Log-first attribution over seven same-config SF100 windows from
08-15/16 (204851, 222607, 225843, 231122, 232354, 233347, 000900 —
all reader modes, all donation states, workers 4 and 8). No deploys;
all evidence from wlogs + coordinator logs already on disk.

## The structure of "slow mode"

The join-6 work per run is two task families, cleanly separated by
output rows in the `fragment task phases` lines:

| family | rows/task | clean shape | degraded shape |
|---|---|---|---|
| q08 join-6 | ~1.33–1.37M | 13.2–14.6s, eff 12, src 2.4–3.5s, ops ~140s, sink ~27s | 16.6–22.8s, eff ~8, **src 5.7–11.8s**, ops ~140s, sink ~27s |
| q09 join-6 | ~10.8–11.1M | 7.2–8.9s, eff 6.5, src 2.8–4.3s, ops ~25s, sink ~26s | 11.8–19.5s, eff 3–4.7, **src 7.2–14.7s**, ops ~26s, sink ~28s |

**In every degraded task across all seven windows, ops_ms and sink_ms
are unchanged to within noise; the ENTIRE degradation is src_ms** —
the morsel producer's `src.Next` time — inflating by +5 to +12
seconds. "q08 slow mode" and "the q09 straggler" are the same
mechanism at two severities: consumers starve (dry 9–11 widths)
because input parents arrive late, and the fragment's own compute
never changes. Affected tasks per run: 0 (clean runs), 1–2 (typical
slow runs), up to all 6 (cold runs).

## Exonerated, with the evidence

- **CPU contention / co-running work**: host counters during a
  straggler minute show ~6.6 of 16 cores busy — idle capacity while
  the fragment runs eff 4.2.
- **Token pool**: width_wait_ms is IDENTICAL between clean and
  degraded tasks of the same family (~9–15s cumulative); donation
  counters normal; the pool is not the throttle.
- **Memory/pressure**: pressure_stall 0 everywhere; peaks normal.
- **Upstream stage tail / eager dispatch**: coordinator timeline
  shows join-6 dispatched ~31s AFTER its probe-producing stage
  completed; the files existed.
- **S3 fallback**: s3_bytes ≈ 0 in every degraded minute — reads are
  local + peer only.
- **Decode/staging engine**: all seven windows' variants (walk,
  index, index+readahead, workers 4/8) show the same mode at the
  same rate — the in-process pipeline is not the variable.
- **Skew**: task input sizes within a stage differ by <3%; within
  one dispatch instant (e.g. st-join-6-b46a0fba, 20:50:57.698Z,
  204851 R1) one worker completed clean at 7.5s while two siblings
  with the same-size inputs straggled at 16–17s — the condition is
  per-worker-per-moment, not per-task-shape.

## What remains (the residual suspect set)

src.Next covers tiered file acquisition (local staged / tier-0 cache
/ peer fetch / prefetcher) plus decode-ahead delivery waits. With
decode-ahead exonerated, the suspects are the ACQUISITION tiers:
peer-fetch service latency (the serving worker's outbound path also
carries eager S3 uploads and peer serves — upload_yield deltas run
2000–3800 s/min in degraded minutes), prefetcher depth/ordering
misses on the ~⅔-remote probe file set, and staging-pipeline waits.
Per-minute tier byte counters are demand-confounded and cannot
separate these; there is no per-open latency marker at Info level.

## Next step (shipped next commit): src-side acquisition counters

`cachedFileStreamSource` gains per-task tallies folded into the
`fragment task phases` line: files by tier (local/cache/peer/s3),
`open_wait_ms` (tiered open through first byte, per tier),
`prefetch_hits/misses`, and `stream_wait_ms` (mid-file waits). One
slow window with those counters converts "src is slow" into "tier X's
open/stream on worker Y stalled Z ms", after which the fix follows
the same pattern as every arc this week: pace, prioritize, or overlap
the guilty tier.

Judgment protocol once instrumented: any window that catches ≥1
straggler run suffices — counters, not walls, carry the verdict
(mechanism counters need no paired arms).
