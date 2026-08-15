# Gap-closing diagnosis: q08 / q11 / q17 (2026-08-14)

Post-verdict-flip residuals from the 2026-08-14 same-window Trino pair
(steady W/T: q08 2.07×, q11 1.63×, q17 1.45×). All diagnosis local —
zero EC2. Evidence: `~/wadjet-artifacts/20260814-trino-compare/`
(wadjet run `results/20260814-121720`, bin b88159e), coordinator stage
timelines via `stage_timeline.py`/`qdetail.py`, worker fragment-phase +
decode-ahead lines (the 2026-07-20 instrumentation asks — they paid off
exactly as intended), and `cmd/plan-repro` against catalog snapshot
`20260726T105410Z`.

Note: the run's wlogs truncate before run 4, so run-4 (fast) tasks have
coordinator evidence only.

## q08 — SOLVED: morsel width cap starved the probe (fix shipped, 0af1ed5)

Walls 29.4 / 25.8 / 32.3 / 25.5 (Trino 14.0). join-6 — broadcast
probe-split, 3 tasks (one per 16-core worker), each probing ~200M
lineitem rows against a 134K-row part build — is 22.7–28.6s of every
run. Task spread is ≤2.3s: the July "one straggler 3× slow" signature is
gone post-stall-closure; the stage is now uniformly slow.

Per-task fragment phases (all 6 logged tasks, runs 1+3):
`ops_ms` 123.5–158.5s over 20–24s elapsed = **~6–7.5 effective cores of
16**. Decode-ahead: `decode_ms` ≈ 20s but `window_full_ms` 41.8–45.4s —
producers sat blocked on full windows while consumers paced the stage.
`src_ms` 0.07–0.8s warm (5–6.5s cold): nothing left to warm, which is
why steady ≈ cold (28.9 vs 29.4).

Mechanism: `morselFragmentParallelismCap = 8` (from the original morsel
commit 7a0d20d) bounded the fragment at 8 consumers on the
single-producer assumption; the multi-group decode-ahead scanner
invalidated it. In work-conserving mode the width gate already meters
active width against the CPU token pool per morsel.

**Fix (0af1ed5, merged):** yield mode bounds k by 1 + pool capacity
(15 on 16-core workers); static cap retained for legacy
`WADJET_MORSEL_YIELD=0`. Gates green (worker -race, SF0.01 22/22,
harness local PASS). Expectation: join-6 → ~11–13s, q08 → ~17–18s
(2.07× → ~1.25×); q09's same-shape join-6 and any single-heavy-fragment
worker should also gain. **SF100 pair owed** — this shape has never run
>8 wide at scale, so per ADR/measurement doctrine it needs the pair
(not covered by the no-A/B carve-out for validated shapes).

## q11 — duplicate scalar-subquery pipeline + serial tail

Walls 8.6 / 7.4 / 8.3 / 8.8 (Trino 5.2). Two independent components:

1. **The whole pipeline runs twice.** plan-repro: the scalar leg
   (scan-9…join-15) clones the main leg (scan-0…join-6) stage-for-stage
   — supplier scan, nation(GERMANY) scan, broadcast join (40,045-row
   build ×2), 80M-row partsupp scan, repartition (rp-5 2.6 GB + rp-14
   1.95 GB = 4.55 GB shuffled where 2.6 GB of union-column payload
   would do), 24-task hash join. The legs differ only in projected
   columns (scan-12 cols ⊂ scan-3 cols ∪ {ps_partkey}) and the final
   aggregate. Heavy phase ends at t+3.2s; it should be roughly half
   that CPU/bytes.
2. **Serial 5.1s tail on ~0.2s of work.** After join-6 (t+3.2s):
   fa-17-interm → fa-17 (the scalar total) → fa-8-interm → fa-8, four
   tiny stages (3-row / 92K-row outputs) with 1–1.4s
   dispatch/read-back gaps each. fa-8's *partial* phase waits for the
   scalar although only the final merge's HAVING (`value > :scalar_1`)
   needs it — the partials could overlap the scalar chain (~2s), and
   the per-boundary gap is generic small-stage dispatch latency.

## q17 — same disease as q11: the join is computed twice

Walls 20.5 / 20.0 / 20.8 / **13.1** (Trino 11.6; run 4 nearly parity).
join-2 (inner) and join-5 (semi) both = lineitem ⋈ part(Brand#23/MED
BOX), same key, identical 63-file lineitem scans (same 3 columns),
identical part scans, **identical 600,982 output rows** — semi ≡ inner
here because p_partkey is unique. Both probe-split legs decode the full
600M-row lineitem concurrently on the same workers (~28–31s decode CPU
each, per worker). Everything after the two joins is <3s.

Dedup nuance: sharing the *scans* would materialize a 600M-row
intermediate — a loss. Share the *join output* (600K rows, 20–86 MB):
rewire the semi leg's consumer (aggregate-6, groups by l_partkey ≡
p_partkey, needs l_quantity) onto join-2's output with union columns.
In-repo prior art: the #266 subsume machinery (Q21's l2/l3 legs riding
one exchange) and multi-consumer exchanges (Q18's rp-7 read twice).

Bimodality (20s vs 13s runs): in slow runs join-2 `src_ms` reaches
6.9–14.3s even steady (run 2: 4.5–7s) — read-side wait, unresolved;
run-4 wlogs missing. The dedup halves exposure to it regardless.

**RESIDUAL (named, needs local repro):** join-5's `sink_ms` is
55.2–56.4s cumulative — near-constant across tasks, workers, AND runs
(straggler task: 82.8s) — for ~200K rows / 28 MB per task (~275µs/row).
Data-independent constants mean structural blocking in `sink.consume`,
not write cost; `unpartitionedStageSink` cannot plausibly burn that on
this volume, so the fragment likely runs a different sink. Repro plan:
`tpch-harness --mode=local` q17 with block/mutex profile on the worker;
identify the sink type join-5's fragment actually gets.

## Unified lever ranking

1. ~~q08 morsel width~~ — shipped 0af1ed5; SF100 pair to confirm
   (expect q08 ~17–18s, q09 improvement, no regression elsewhere).
2. ~~Shared-subplan dedup (closes q11 AND q17)~~ — SHIPPED 2026-08-14
   (`dedupeSharedSubplans`, design
   `docs/design/shared-subplan-dedup.md`): fingerprint-based join-rooted
   subtree dedup; q17's semi rides the inner via a
   duplication-invariant-consumer gate (no uniqueness oracle needed —
   AVG grouped on the probe key is invariant under per-key duplication).
   Q11 15→10 stages, Q17 15→11. SF100 pair owed.
3. ~~q11 tail: unblock final-aggregate partials from scalar deps~~ —
   SHIPPED 2026-08-15 (`scalarsDeferrableToFinalMerge` +
   `scalarResolver` deferral): when a final_aggregate's placeholders
   appear only in FilterExprs (HAVING at the final merge), the
   coordinator defers the scalar await+substitution past the fanout's
   intermediate phase — partials overlap the scalar chain; the final
   task joins the substituted stage. Stages whose placeholders reach
   agg specs / join filters keep the upfront barrier. Generic
   small-stage dispatch gap (~1s per boundary) is a separate,
   suite-wide observation, still open.
   **SF100 verdict (window 20260815-121238, bin 6fa0237): wall-neutral
   — the 5.1s tail this item was diagnosed against had ALREADY
   collapsed to ~0.9s by ship time (dedup removed the duplicate leg;
   fa-17 now completes in ~10ms and fa-8 takes the single-task path,
   no fanout to overlap). q11 = 2.5-3.5s, dominated by the main leg.
   Change kept: architecturally correct, zero regression, and it
   engages whenever a fanout final-aggregate gates on a slow scalar
   chain (larger scales / other plans).**
4. q17 sink residual + slow-run read-wait — diagnose before touching.
