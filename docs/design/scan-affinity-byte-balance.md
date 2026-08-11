# Byte-balanced scan-affinity fan-outs

Status: implemented 2026-08-11, DEFAULT ON. Kill switch:
`WADJET_AFFINITY_BYTE_BALANCE=0` (restores count-based affinity grouping;
`WADJET_SCAN_AFFINITY=0` still kills the whole placement half).

Extends docs/design/scan-affinity.md. Diagnosis evidence: 2026-08-10 SF100
pair wlogs + deterministic ownership recompute (zero EC2).

## The diagnosis: ownership bytes are a per-deploy lottery

Rendezvous hashing balances file COUNT in expectation, but the SF100
tables are coarse — lineitem is 63 files (~283 MB each), orders 17,
partsupp 9 — so each deploy's fresh worker IDs redraw per-worker BYTE
shares with huge variance. Recomputing the hash over the real file set
for the 2026-08-10 trap-watch pair:

| draw | lineitem | orders | partsupp | total |
|---|---|---|---|---|
| arm1 (results/20260810-210238) | 43/18/39 % (2.39×) | 37/19/44 % (2.36×) | 61/28/12 % (5.26×) | 43.9/19.5/36.7 % |
| arm2 (results/20260810-212706) | 36/34/30 % (1.20×) | 27/20/53 % (2.67×) | 41/25/35 % (1.62×) | 35.0/30.6/34.4 % |

Scan fan-outs group files by owner, so per-worker scan input bytes track
ownership, and every stage barrier paces the cluster at its hottest
worker. The wlog decode ledger confirms both directions:

- arm1 (skewed draw): R2 decode bytes 67.0/47.8/61.3 GB per worker —
  rank-identical to the ownership shares; the 19.5 %-owner decoded least
  on nearly every query. R2 suite wall 611 s.
- arm2 (even draw): R2 decode 58.8/57.5/54.9 GB — even. R2 wall 517 s on
  identical code, same window.

The 2026-08-09 re-profile arm's "cache-affinity concentration"
(74.3 vs 42.8 GB worst/best) was the same mechanism on a worse draw.
Consequence: the ownership lottery is a standing component of
cross-deploy steady-wall variance, and on a bad draw a permanent ~1.3×
pacing tax on every scan-bound stage.

## The fix

`affineFileSets` becomes byte-aware. The planner already walks the
catalog manifest to build `ScanFiles`; it now records the parallel
`ScanFileSizes` (catalog `SizeBytes`) on the stage, and the pass-through
leaf-scan `StageOutput` carries them to the shuffle dispatcher's
synthetic source stage. At fan-out:

1. Files group by rendezvous owner as before.
2. **Shed surplus**: any owner holding more than `(1+tol)×` the fair byte
   share (`tol = 0.10`) moves files — largest first, fewest moves — to
   each file's **rendezvous runner-up** (then third choice…), provided
   the recipient stays inside the band. Whole files only; a file no
   recipient can absorb stays put.
3. **Byte-proportional task shares**: each worker's group is sliced into
   tasks by its byte share (was: file-count share), so tasks are
   ~equal-byte and the scheduler's count-based same-batch anti-stacking
   cap remains aligned with byte fairness.

Why the runner-up, not the least-loaded worker: shedding is a pure
function of (files, sizes, worker set), and the runner-up is each file's
stable secondary home (it inherits the file on owner departure). The same
files therefore shed to the same recipient on every stage of every query,
so after one NIC-speed peer fetch the recipient's cache is warm and stays
warm — shed placement costs S3 nothing (owner read-through keeps first
touches single-flight cluster-wide) and steady-state nothing after the
first fetch.

Simulated on the real SF100 file set (whole-file granularity):

| draw | lineitem | orders | partsupp | shed |
|---|---|---|---|---|
| arm1 | 43/18/39 → 36/28/36 % | 2.36× → 1.08 | 5.26× → 1.10 | 2.5 GB, 11 files |
| arm2 | already 1.20× (no shed) | 2.67× → 1.00 | 1.62× → 1.10 | 1.2 GB, 4 files |

Sub-2×workers tables (customer, part, supplier, nation, region) never
take the affine path and are unaffected.

## Gates and degradation

- `affinityBalanceMinBytes` (64 MiB, the local-fastpath scale): stages
  below it are latency-bound; shedding would trade cache locality for
  nothing.
- Missing/misaligned sizes (older plans, synthetic stages without
  pass-through metadata): count-based behavior, bit-identical to before.
- Placement stays a preference: a shed task's hint degrades exactly like
  an affine hint (scheduler fallback → cache miss → peer tier → S3).

## Observability

Each shedding fan-out logs `scan-affinity byte-balance` on the
coordinator with `total_bytes`, `shed_files`, `shed_bytes`,
`max_share_before/after`. A/B analysis greps this plus the existing
per-worker wlog decode ledger (`scan decode-ahead stats` decode_bytes),
whose per-worker spread is the engagement criterion.

## Expected SF100 shape

- Engagement: `scan-affinity byte-balance` lines on lineitem/orders/
  partsupp fan-outs; per-worker R2 decode-byte spread max/min → ≤ ~1.15
  (from 1.40 on arm1, 1.74 on the re-profile arm).
- Peer wire: one-time shed transfer (~2.5 GB on an arm1-like draw) in
  run 1; `peer_hits` up modestly, `miss_bytes` unchanged (read-through
  keeps S3 single-flight).
- Walls: steady run compresses toward the even-draw reference (bad-draw
  arms were ~+18 % over even-draw same-window); cross-deploy steady
  variance shrinks. Cold run neutral-to-slightly-better (same barrier
  logic applies while populating).
- Rows 44/44, per-query counts identical — placement is never
  correctness.

## SF100 verdict (pair 20260810-232606 ctl / 234309 trt, bin 027c262)

KEEPER — default stays ON. Same-window pair, same binary, runs=2, wlogs
both arms, all EC2 destroyed. Cross-arm walls are draw-confounded by
construction (each deploy redraws worker IDs), so the pair was judged on
engagement, spread, and correctness:

- **Engagement exact.** Trt logged 30 shed events across every orders and
  partsupp fan-out (scans + exchange-repartition sources); the logged
  `max_share_before` values (1.36 orders, 1.53 partsupp) match the
  offline rendezvous recompute of that deploy's draw digit-for-digit,
  and `max_share_after` landed at 1.04/1.10 — inside the band. The same
  2 files shed with identical bytes on every recurrence of each stage
  shape (the determinism the runner-up rule promises). Trt's lineitem
  draw (1.09× fair) sat inside the band and correctly did NOT shed.
  Ctl logged zero events (kill-switch arm verified).
- **Spread compressed.** R2 per-worker decode bytes: ctl 51.0/64.4/49.3
  GB (1.31× max/min, tracking its 34/37/29 ownership draw) → trt
  50.2/52.6/58.7 GB (1.17×). Residual spread is consistent with the
  10 % tolerance plus non-affine reads.
- **Correctness.** Per-query row counts identical across arms and runs
  (44/44).
- **Walls neutral** — R1 338.9→350.7 s (+3.5 %, cold band), R2
  420.2→421.1 s (+0.2 %): the shed's peer traffic costs nothing
  measurable. Both arms drew MILD lotteries (totals 34/37/29 and
  32/36/32), so the bad-draw wall win (arm1-class, 43/18/39) was not
  exercisable this window; the sim over the real file set covers that
  case, and tonight's orders/partsupp sheds are the same mechanism at
  smaller byte scale.
- No stall-watchdog firings either arm (dispatch-stall trap stays armed).

The standing effect: worst-case draws are now clamped to ≤1.10× fair
share per stage, removing the largest known non-code component of
cross-deploy steady-wall variance.
