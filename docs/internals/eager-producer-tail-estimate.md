# Eager producer tail estimate

Source: internal/coordinator/eager_feed.go — projectedTailSeconds, moved 2026-09-11 (#1026)

projectedTailSeconds estimates the wall remaining until the producer's
last task completes, from the completions observed so far: mean
inter-arrival × tasks remaining. Called at consumer clearance time
(after decisionReady, so at least a full wave of completions exists).
Returns 0 when nothing remains or when fewer than two completions have
landed (single-task producers, threshold==1) — no measurable tail means
nothing worth overlapping, so the gate declines toward the barrier,
never toward a wrong clearance.

Calibration history (eager-consumer-dispatch.md §10, §15): the July C3
SF100 pair put the envelope at ~12s — every edge with producer spread
≥ ~12s converted under eager clearance (Q05 −29%, Q21, Q18, Q04, Q03),
every edge below ~10s paid a slot-occupancy tax. That world's long
tails were straggler-driven; after the 2026-08 producer speedups
(morsel-collapse fix dde1f02, decoded cache, scan levers) SF100 tails
compressed to 0–2.7s, making a 12s (or 3s) floor inert — the
2026-08-13 floor=3 arm declined 17/17 clearances, 11 of them with
tails ≤ 0.33s (nothing to overlap) and 6 in the 1.0–2.7s band. The
1.0 floor separates those two populations on the current config.
WADJET_EAGER_MIN_TAIL_SECONDS overrides it (0 = gate off, restoring
the ungated C3 behavior for A/B).
