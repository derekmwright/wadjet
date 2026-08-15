# Shuffle decode-ahead pressure exemption: stall class eliminated, pacer re-attributed to token priority (2026-08-15)

**Verdict: KEEP b4b3caf (exemption stays default).** It achieved its
mechanical goal exactly — `pressure_stall_ms` 92s/worker → **0** — at
zero cost (88/88 rows+vsig identical to window 154200, zero reaps,
suite flat). But the warm join-6 dry-wait it was expected to recover
did NOT come back: the stall mass moved to the CPU-token pool, which is
now the attributed pacer.

Window `results/20260815-163743`, bin `b4b3caf`, standard 4-run config,
same-day back-to-back with 154200 (as close to same-window as
single-arm allows). Torn down on the DONE marker, EC2 zero.

## Walls

| run | suite | q08 | q09 |
|---|---|---|---|
| 1 (cold) | 3m36.5s | 26.0s | 18.9s |
| 2 | 2m57.6s | 24.8s | 19.4s |
| 3 | 2m56.1s | 25.5s | 18.9s |
| 4 | 2m44.7s | **17.5s** | 17.3s |

Suite R4 164.7s ≈ 154200's 164.0s. q08 is **bimodal across both
windows** (~25s vs ~17.5s runs; 154200 flipped fast at R3, this window
at R4) — an ~8s run-to-run mode swing that now dominates q08's
variance and is NOT explained by join-6 width (fast runs occur with
warm-task dry-wait unchanged). Cold-lottery-shaped; separate
investigation if pursued.

## Counters (w0 final sample, vs 154200)

| marker | 154200 (coupled) | 163743 (exempt) |
|---|---|---|
| pressure_stall_ms | 91.7s | **0** |
| token_stall_ms | 24.5s | **43.5s** |
| window_full_ms | 5.7s | 5.2s |
| stage_ms / decode_ms | 28.3s / 22.4s | 29.5s / 23.5s |

join-6 shapes: mid-shape unchanged-good (eff ~12, dry ~2); warm-shape
7.3–8.2s at eff 6.3–7.3, dry 6.6–7.4 widths (154200: 8.0–8.4s,
eff 6.1–6.6, dry 7.1–7.6) — marginal, within shape noise.

## The re-attribution

With the refault channel gone, WSHF decode admission beyond the cursor
is denied almost exclusively by `cpuTokens.TryAcquire` — and that is
structural, not incidental: **queued width-gate waiters take strict
priority over TryAcquire** (morsel memo §4.2.1: "they hold admitted
morsels; feeding them beats widening decode"). On a producer-starved
fragment that rule is exactly inverted — consumers park as FIFO token
waiters because the dispenser is dry, and their queued presence starves
the very decode that would fill it. The system oscillates: consumer
demand blocks decode width; starved decode dries consumers. The same
tension exists on the parquet path (token_stalls 18–28k in §9.3) but
binds hardest here, where warm probe fragments saturate the pool with
mc=4 × k=15 consumer targets against capacity 14.

**Next lever (needs design, not a patch):** token policy for
producer-starved fragments — when a fragment's dispenser is EMPTY, its
producer's decode admission should rank at least equal to that
fragment's own queued consumers (a starved consumer's token does
nothing until decode runs). Candidate shapes: scanner joins the FIFO
via enqueueWaiter with a went-empty escape to the cursor exemption;
or the width gate donates a yielded slot directly to its fragment's
producer before returning it to the pool. Deadlock analysis required
(the §4.2.1 no-wedge argument must be re-established for either).

## Window log

Deploy ~16:30Z, suite 16:37–16:50Z, destroyed on DONE, EC2 zero
verified. 88/88 OK, vsigs byte-identical to 154200, zero reaps, zero
watchdog firings. `WADJET_SHUFFLE_DA_REFAULT=1` remains the same-binary
arm to restore the coupled behavior if edge deployments ever need it
suite-wide.
