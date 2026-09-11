# Memory cost sensitive spill thresholds

Source: internal/engine/memory/spill.go — func (sm *SpillManager) ShouldSpillFor(urgency SpillUrgency) bool {, moved 2026-09-11 (#1026)
Superseded: 40%/90% describes the static path; explicitly activated floating-budget mode uses cheapFrac/98%, and tracking-only mode returns false.

ShouldSpillFor returns true when an operator with the given spill cost
class should spill. SpillCheap operators trigger at 40% of the per-tracker
budget; SpillExpensive operators trigger at 90%. Either class also triggers
if the global heap-pressure circuit breaker fires.

The 40% SpillCheap threshold (was 60% pre-2026-05-19) is sized so that
3 concurrent fragment tasks at SF100 mc=3 — each holding ~5.7 GB peak
HashJoin/build=lineitem state on a 14.8 GB GOMEMLIMIT worker — stay
cumulatively under the process heap limit. With per-task share = budget
/ 3 ≈ 5 GB, a 60% threshold lets each task ramp to 3 GB before spilling,
and the 1.1× untracked overhead pushes 3 × 3 = 9 GB to ~10 GB heap. A
40% threshold caps the per-task pre-spill peak at 2 GB; cumulative
3 × 2 × 1.1 ≈ 6.6 GB heap. The architectural mechanism is documented
in project_q17_sf100_instrumented_2026-05-17.md and
project_bufio_fix_sf100_2026-05-18.md (worker-w0 hitting 19.7 GB
during shuffle-stage-7 from concurrent fragment-task heap stacking).

Threshold sweep on the local Q17 probe (TestHashJoin_Q17ShapeRepro):

	threshold   peak heap (vs tracker budget)
	30%         0.63×
	40% (now)   0.74×
	50%         0.92×
	60% (was)   1.10×
	70%         1.27×

Wall time was flat across thresholds — earlier spill is essentially
free on local NVMe. Spill bytes 67-80 MB across the sweep (lower
threshold → more bytes spilled, as expected).
