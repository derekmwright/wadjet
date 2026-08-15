# q08 width plateau: attributed — the single producer paces the fragment (2026-08-15)

**Verdict:** join-6's effective-width plateau is **dispenser-paced by its
single-threaded producer**, not token-paced and not sink-paced. The CPU
token pool and the width gate are exonerated with numbers.

Evidence (window `results/20260815-135022`, bin 1d138a2, 2 runs; counters
from `feat(worker) 1d138a2` — consumer_dry_wait_ms + process_ms on the
morsel done-lines, promoted Debug→Info):

| join-6 task | elapsed | process_ms → eff. width (k=15) | dry-wait (widths) | token wait (widths) |
|---|---|---|---|---|
| A | 19.1s | 165.1s → **8.7** | 93.1s (4.9) | 13.7s (0.7) |
| B | 14.7s | 54.8s → **3.7** | 156.2s (10.6) | 9.2s (0.6) |

The buckets close (8.7+4.9+0.7 ≈ 15; 3.7+10.6+0.6 ≈ 15): every missing
core is a consumer parked on an EMPTY morsel channel with its slot
yielded. And `dispenser_producer_wait_ms = 0` on both tasks — the
producer is never blocked by the in-flight budget; it simply cannot
produce faster. One goroutine runs `src.Next` (probe-input decode) for
the whole fragment: ~772 parent batches over 19s ≈ 40 parents/s is the
ceiling 15 consumers starve behind.

## What this kills

- "Width plateaus because tokens/dispenser-admission pace it" — the
  token half is dead (≤0.7 widths); the admission half is dead
  (producer_wait=0).
- Any further sink work for q08 (see q08-sink-attribution memo — ~2s
  ceiling; with dry-wait at 5–10 widths the sink is third-order).

## The fix direction (next arc)

Parallelize the fragment's PRODUCER side for probe-heavy fragments:
multiple source readers feeding the dispenser (the scan path already has
multi-group decode-ahead; the fused scan→join fragment shape runs its
source single-threaded into the morsel channel). Options to evaluate
against docs/design/morsel-execution.md and scan-decode-pipelining §5:
N producer goroutines over disjoint file/row-group slices, or routing
this shape through the decode-ahead scanner's multi-group window.
Task A/B asymmetry (165s vs 55s process for identical morsel counts)
says warm-cache runs starve HARDER (decode is a bigger relative
bottleneck when probing is fast) — the lever grows as everything else
gets faster.

Estimated value: q08 join-6 at full width ≈ elapsed × eff/15 → ~11s and
~4s for A/B-shaped tasks (5–8s off q08); q09's same-shape join should
gain similarly.

Window log: 2 runs, zero reaps, rows exact (Q08 2r, vsig match), EC2
zero after teardown.
