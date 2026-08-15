# Extent index: SF100 window — floor eliminated as designed, walls inconclusive, trio attribution falsified (2026-08-15)

**Verdict: KEEP d5281d4 provisionally — mechanism goal met at zero
correctness cost — with a REQUIRED same-window A/B follow-up before
the wall question is called.** The index engaged massively and deleted
the serial stage floor (per-worker stage_ms 30–34s → 0.2–1.4s,
indexed_files ~15.1–15.3k/worker), rows and vsigs are byte-identical
to the prior window, zero reaps. But warm suite walls lean worse
(+2.5–6.3% vs 204851, inside the run-level bimodality's historic
swing), the warm join-6 trio did not move at all — falsifying the
stage-walk attribution for that shape — and the token counters expose
a real structural shift this change made: staging work that the walk
path ran on an UNGOVERNED scanner core now runs inside token-holding
decode workers, so the pool pays for it (token_stall +20%, donations
2×). Whether that costs wall is exactly what the built-in
`WADJET_SHUFFLE_INDEX_READ=0` arm exists to measure, same-window.

Window `results/20260815-222607`, bin `d5281d4`, standard 4-run
config, on-demand (terraform default held), suite 22:26–22:38Z,
destroyed before analysis, EC2 zero verified. Single-arm window —
cross-window wall deltas are context, not A/B claims.

## Walls

| run | suite | q08 | q09 |
|---|---|---|---|
| 1 (cold) | 3m30.6s | 25.7s | 24.5s |
| 2 | 3m03.1s | **26.5s** | 19.2s |
| 3 | 2m47.5s | 17.8s | 20.2s |
| 4 | 2m49.3s | 17.2s | **24.3s** |

vs 204851 (211.1/172.2/163.4/160.6): warm runs +6.3%/+2.5%/+5.4%.
q08's slow mode is back in one warm run (R2 26.5s, absent in all of
204851's warm runs) and q09 has none under 19.2s. Judged against the
run-level bimodality (fixed-binary windows have swung 158.6–191.4s
per run), this is a LEAN, not a finding; the memos' own rule — noise
±2–3%, same-window controls for wall claims — is why the A/B arm is
the required next step rather than a keep/kill call here.

## Counters (4-run cumulative, final per-worker samples)

| marker | w(05f4) | w(0858) | w(0ba5) |
|---|---|---|---|
| indexed_files | 15,321 | 15,102 | 15,301 |
| stage_ms | **212** | **1,371** | **1,339** |
| pread_ms | 35,045 | 21,255 | 35,745 |
| decode_ms | 22,615 | 20,580 | 22,777 |
| token_stall_ms | 51,575 | 46,606 | 64,258 |
| donated | 93,814 | 94,618 | 94,238 |
| window_full_ms | 6,392 | 5,063 | 1,689 |
| pressure_stall_ms | 0 | 0 | 0 |

Engagement is total: stage_ms collapsed 96–99% (the residual is the
few direct-stream/footer-less readers), staging reappears as parallel
worker-side pread_ms. Correctness: 88/88 rows and all vsigs
byte-identical to 204851 (including Q19's `…904`).

## The two findings

**1. The join-6 warm trio attribution is falsified — twice.** The
trio (three ~7.5–8.9s tasks per warm run) sits at eff 6.2–6.8 / dry
~7 — exactly its shape in the last three windows — with the stage
walk now gone. It was never stage-bound. And the saturation
hypothesis died in the same logs: the three tasks run ONE PER WORKER
(timestamps interleave across the three wlogs), and the span-weighted
concurrent morsel eff-sum on a trio worker is ~5.8 widths on 16
vCPUs — the box is not morsel-saturated. What remains: with staging
eliminated, the fragment's producer IS chunk decode, and decode is
hard-ceilinged at `shuffleDecodeAheadWorkersDefault = 4` workers per
reader regardless of pool headroom. Four decode widths feeding an
op-chain ~1.6× heavier per row lands at exactly eff ≈ 6.5. The
constant predates both the token pool governing admission and the
index removing the scanner bottleneck; the pool is the regulator now,
and the fixed ceiling is the suspected residual cap. Lever: raise the
ceiling (pool-governed width makes it a cap, not a commitment) —
priced SEPARATELY from the reader A/B so neither masks the other.
The eff≈3/dry≈10 straggler mode also persists (R2, R4 — matching
their elevated q08/q09).

**2. The pool now pays for staging.** Walk-mode staging ran on the
scanner goroutine WITHOUT a token — one effectively free core per
active reader, invisible to the admission economy. Index mode runs
pread+decode both under the chunk's token, so ~20–35s/worker of
former scanner work now competes with morsel consumers for the pool:
token_stall_ms up ~20%, donation volume doubled (the churn serves the
stalls faster, but they exist because the pool absorbed the staging
load). If the A/B shows this costs wall, the refinement is NOT
reverting to the walk — it is decoupling the phases: worker performs
the extent pread BEFORE taking/holding decode capacity (token span
covers decode only), restoring the walk path's effective economics
with the parallelism kept. That is a small, targeted change to
decodeIndexed and the admission token handoff.

## Required follow-up (in order)

1. **Same-window A/B**: default arm vs `WADJET_SHUFFLE_INDEX_READ=0`
   (files carry footers in both arms; reader path is the only delta).
   2 runs per arm, one deploy. Judgment: warm suite + q08/q09 walls,
   token_stall_ms, and whether the slow-mode incidence differs. Keep
   whichever reader default wins; the writer footer stays either way
   (0.005% of bytes, enables the choice per-binary).
2. If the index arm pays on the wall: token-span split (pread outside
   the token) and re-price.
3. Decode-worker ceiling lift (the trio's suspected residual cap, see
   finding 1) — own window, after the A/B settles the reader default.

## Window log

Preflight clean (zero orphans), on-demand verified on all four
instances (spot-default fix 783b432 held). Startup verified T+6min.
Mid-run engagement sampled live at T+9min via SSM (indexed_files
9,515, stage_ms 1.17s on w-0ba5) — first window with in-flight
engagement confirmation. Completion monitor raced the coordinator's
post-upload auto-stop and fired its stopped-coordinator alarm one
poll before seeing the results prefix; upload was already complete
(22:38Z). Teardown before analysis; post-destroy describe-instances
zero.
