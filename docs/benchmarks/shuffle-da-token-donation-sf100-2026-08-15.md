# Shuffle decode-ahead token donation: SF100 window — engaged at scale, record fast runs, deep-starvation gap remains (2026-08-15)

**Verdict: KEEP df2db8b (donation stays default).** Zero cost (88/88
rows + vsigs byte-identical to window 163743, zero reaps, zero
watchdog firings), engagement proven at scale (96.6k donated tokens),
and the two fast runs are the two best suites ever recorded on this
config (158.6s / 161.7s vs the 160.2s record). But the donation did
NOT collapse join-6's warm dry-wait — and the counters show exactly
why: the deep-starvation mode has no slot-holding consumer left to
donate. That gap, plus the now run-correlated q08+q09 bimodality, are
the open residuals.

Window `results/20260815-175927`, bin `df2db8b`, standard 4-run
config, deploy ~17:42Z, suite 17:59–18:12Z, torn down after
completion, EC2 zero verified. Single-arm window — cross-window wall
comparisons below are context, not A/B claims.

## Walls

| run | suite | q08 | q09 |
|---|---|---|---|
| 1 (cold) | 3m31.4s | 25.5s | 28.6s |
| 2 | **2m41.7s** | **17.5s** | 18.8s |
| 3 | 3m11.4s | 25.4s | 27.4s |
| 4 | **2m38.6s** | **16.5s** | **17.5s** |

R2 and R4 both beat the prior 160.2s record; q08 16.5s is the best
observed. Fast mode hit 2 of 4 runs (1 of 4 in each of 154200 and
163743). Per-query R4 vs 163743-R4 is flat-to-better across all 22
(Q05 −2.1s, Q18 −1.5s; worst movement Q15 +0.8s — noise band).

The slow-mode runs are no longer a q08-only story: q09 runs 27.4–28.6s
on exactly the runs q08 runs ~25s (R1, R3), where 163743 held q09 at
18.9–19.4s throughout. The mode is run-level and spans the pair.
Mechanism signature inside the slow runs: join-6 straggler tasks at
eff ≈ 3 widths / dry ≈ 10–11 widths (two in R1, one in R3, none in
R2/R4, whose mid-shapes run eff ~12 / dry ~1.7).

## Counters (4-run cumulative, final per-worker samples)

| marker | w0 | w1 | w2 |
|---|---|---|---|
| donated | 32,227 | 28,404 | 35,961 |
| token_stall_ms | 57,217 | 38,763 | 37,245 |
| pressure_stall_ms | 0 | 0 | 0 |
| window_full_ms | 1,418 | 3,852 | 5,004 |
| stage_ms / decode_ms | 36.8s / 25.8s | 30.6s / 22.9s | 31.7s / 23.6s |

Donations by stage (fragment-side `width_donations`, all workers):
join-4 **75,606**, join-13 11,308, join-10 7,849, **join-6 1,902**,
join-2 83. w0's cumulative donated was ~0 until ~18:01 (late R1), then
ramped (+15k over R2, the biggest fast run).

## The mechanism finding

Donation fires exactly where its precondition exists and nowhere else.
A consumer can only donate a token it HOLDS at the moment it goes
dispenser-dry. In the shallow-contention shape (join-4 class:
consumers actively cycling slots, scanner intermittently
token-stalled) that happens constantly — 75k donations, and the stage
runs clean. In join-6's deep-starvation oscillation the consumers are
parked INSIDE `widthGate.claim` as slot-less FIFO waiters — they hold
nothing, so `width_donations = 0` on every join-6 task in the window,
and the warm-shape dry-wait (~7 widths at eff ~6.5, elapsed ~7.5s) is
unchanged from both prior windows. token_stall_ms stayed roughly flat
vs 163743 (43.5s w0) — donations resolve individual stalls but the
deep mode never generates the donors that would break it.

**Residual lever (deep mode):** get producer admission a path to
capacity when NO consumer holds a slot — the §3-rejected
scanner-joins-FIFO shape, or a claim-path variant (a consumer parked
in claim whose own fragment's scanner is token-stalled is itself
holding a queue position it could cede). Second watch item: stage_ms
(30.6–36.8s/worker) now exceeds decode_ms — the serial stage walk is
approaching the pacer role the design memo's §4 predicted; the
follow-up there is extent skip-walk staging, not wider decode.

## Window log

Preflight clean, on-demand, sha-pinned bin df2db8b staged+verified.
Startup verified at T+3min (stage dispatch live). Suite completed
18:12Z; teardown lagged ~20min because the completion monitor
filtered results prefixes by upload-time stamps — **the results dir
stamp is the suite START time** (17:59:27), so the watch pattern never
matched. Gotcha recorded. Destroy complete, post-destroy
describe-instances empty (incl. stopped).
