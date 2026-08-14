# SF100 pair: shared-subplan dedup (2026-08-14 evening) — storm-contaminated, three fixes out

Arms: ctl `aa47b15` (pre-dedup, engine ≡ the validated 1388bf8) →
`results/20260814-195058`; trt `ba4551c` (dedup) →
`results/20260814-204612`. Both profile-first config, runs=4, 3×
c7gd.4xlarge + c7g.2xlarge, deployed back-to-back. Local copies:
scratchpad `ctl-results/` + `trt-results/` (this session); wlogs in the
S3 prefixes.

## Verdict

**Walls unjudgeable for the headline; correctness and plan-shape
verdicts are in; three code fixes shipped off the run's evidence.**
Both arms were hit by frozen-spin stall storms (watchdog kills:
~20 firings ctl, comparable trt; 37 durable-copy stalls + 3 whole-query
re-executions ctl, 18 terminal failures + 1 re-execution + one 30m hang
trt). Run totals (22-query suite, seconds):

| run | ctl | trt |
|---|---|---|
| R1 (cold) | 376.9 | 333.0 |
| R2 | 388.3 | 406.2 |
| R3 | 597.1 | 419.5 + **1800 (Q22 timeout)** |
| R4 | 476.3 | 347.3 |

Same-day reference on this binary lineage: 187–210s steady
(results/20260814-121720, zero firings). The inter-run inflation is
storm-driven; the storm trigger itself is the open residual below.

## What the pair DID establish

- **Rows/vsigs: every completed query correct on both arms, 44/44
  shapes** (Q02=100, Q11=92,698; Q22 completions vsig-identical). The
  only FAIL was trt Q22-R3's 30m deadline — a hang, not wrong rows,
  root-caused below.
- **Dedup engaged in production** (coordinator planner logs, every
  run): Q17 semi→inner (`keep=join-2 drop=join-5`), and — unplanned —
  **Q02** exact-match (`keep=join-2 drop=join-11`): its
  MIN(ps_supplycost) scalar subquery clones the main
  partsupp⋈supplier⋈nation⋈region leg. **Q02 min-steady 21.8→6.2s
  (−71%)** — large enough to survive the storm noise; treat as
  provisional until a clean pair, but the mechanism (whole clone
  pipeline dropped) is structural.
- **Q11's dedup SKIPPED at SF100** — the collision gate pooled extras
  globally and tripped on ps_partkey, which sits on the join's BUILD
  side where no rebinding is possible. Fixed (6e01351, per-join
  sidedness); verified against the production catalog snapshot via
  plan-repro: the full 8-stage clone leg now drops.
- **Q17 wall inconclusive** (14.5→16.7 min-steady, storm swings
  ±50% on peers). The semi leg + its 600M decode are gone from the
  plan; whether the multi-consumer read of join-2's output gives it
  back is the clean-pair question. Chain-fusion loss on the shared
  join is the named suspect if it does.

## Q22-R3: the 30-minute hang (fixed, bbdb985)

Chain, from coordinator log: worker-a22577f3 wedges (frozen spin)
~21:09:35 → reap-grace holds 3m7s (25 non-durable outputs) → reaped
21:13:12, 6 tasks queued for re-dispatch → **21:13:20 two re-dispatched
tasks published back to the reaped worker** (`placement=affine` — the
wedged process's TCP connection stays ESTABLISHED, so data-plane
connectivity still listed it) → the re-dispatched tasks are never
mentioned by any heartbeat, and reapStuckOnce had REMOVED them from
liveness tracking, so the stuck sweep never re-fired → silence until
the query deadline. Two fixes shipped: placement candidates are
liveness-filtered, and re-dispatched stuck tasks keep a reset liveness
clock. Any worker death was a potential 30m query loss in gRPC mode
(no JetStream redelivery backstop); this supersedes the "600s ack-wait
re-dispatch hole" item — that NATS-era framing understated it.

## OPEN RESIDUAL: frozen-spin storm trigger

Both arms, ~40 specimen dumps
(`results/20260814-{195058,204612}/wlogs/stall-*`). Config drift ruled
OUT this time — verified live: gRPC data plane (coordinator startup
line), WSHZ engaged (`wshz_files=777` in shuffle-io stats), caches +
locality flags on the worker cmdline. **The zstd-arm-zero-firings
correlation from the 08-14 afternoon 3-arm is dead** — storms fire with
zstd on. Signature: `FROZEN-SPIN unresp_ms≈5000 cpu_jiffies≈230`,
pprof fully wedged, SIGABRT dump shows **no runnable/GC-assist
goroutines while process CPU accrues** → the wedge is below the Go
scheduler (runtime/STW-level; specimen-8 protocol: per-thread jiffies +
kernel stacks are in the threads/stacks files). Why the 12:17 morning
run (same lineage, same config) had zero firings and the evening runs
stormed is unexplained — that is the hunt, with specimens in hand.
Amplification (kill → output loss → re-execution → 15s×3 stalls) is
now bounded by the bbdb985 fixes; the trigger remains.

## Next

1. Clean pair re-run for the record: ctl aa47b15 vs main (≥bbdb985,
   now carrying q11-unlock + corpse fixes). Watch: q02/q11/q17,
   firing counts per arm, and whether R3-class inflation recurs.
2. Frozen-spin root cause from the specimen corpus (open lead
   continues from the zstd-wire arc, s2-correlation retired).
3. q17 clean-pair verdict decides whether shared-join chain-fusion
   loss needs a chained-agg-aware dedup variant.

## Clean-window re-pair (same night, 22:19 ctl / 22:54 trt — no kills, drag persists)

Arms: ctl aa47b15 → `results/20260814-221930`, trt b32e13b (dedup +
q11-unlock 6e01351 + corpse fixes bbdb985) → `results/20260814-225424`.
Runs=4 each, back-to-back, ZERO task failures / re-executions / hangs on
both arms (the bbdb985 fixes were not exercised — no kills occurred).
Dedup fired 3 queries × 4 runs in trt (q02 exact, q11 exact
post-6e01351, q17 semi≡inner).

**Correctness: 88/88 row sets identical across arms — the SF100 gate
for the dedup PASSES.**

Walls: intra-arm swings up to 5× on identical binary+config (ctl Q08
7.1↔36.1s across runs; trt Q06 2.0↔47.0s) with zero failures — the
per-query noise floor exceeds most expected effects. Judgeable signals
(every-run-dominant only):

- **Q17: every trt run ≤ every ctl run** — cold 24.3→8.5 (−65%), steady
  runs 31.9/17.9/16.8 → 14.0/17.6/14.1. The dropped 600M decode is
  visible through the noise. Best trt run (8.5s) beats Trino FTE (11.6).
- **Q12 and Q20: every trt run < every ctl run** (Q20 median 28→19).
  Neither dedups; candidate mechanism is the placement change riding
  bbdb985 (liveness-filtered rr rotation) — unverified, logged.
- Q02/Q11: inconclusive here (Q02's storm-pair −71% remains the better
  evidence for Q02; q11 4.3 best-of-arm vs ctl 5.0 — within noise).
- Q08/Q09/Q18 lean worse in trt but sit inside the 3-5× intra-arm
  swings — WATCH ITEMS for the next quiet-environment pair, not
  verdicts.

**OPEN RESIDUAL (sharpened): the no-kill drag.** Both arms' steady
totals 290-498s vs the same lineage's 187-210 at 12:17 — with zero
failures, zero re-executions, and only partial stall captures (14/18
wlog files, no SIGABRT). Whatever inflated tonight's cluster does NOT
require watchdog kills; the kill storms earlier were its worst
expression, not its definition. Signature for the hunt: multi-×
per-query intra-arm swings, broad-based, small queries included, both
arms equally. Specimens: tonight's four result prefixes + wlogs.

Standing verdict on the dedup: correctness proven at SF100 (this pair)
and e2e; q17 delivers consistently; no attributable regression. It
stays on main default-on. Record-book absolute numbers wait for a
quiet environment AND the drag root cause.
