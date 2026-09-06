# SF100 baseline — v0.18.50 vs v0.18.12, 2026-09-06

Same-window A/B on the clean `wadjet-bench-sf100-use2` bucket. Distributed mode,
coordinator c7g.2xlarge + 3× c7gd.4xlarge workers, on-demand, us-east-2,
`benchmark_runs=4`. This run closes the 0.18.x correctness line: the arc landed
38 releases of correctness fixes (v0.18.12 → v0.18.50) across the planner,
executor, batch/vector layer, parquet reader/writer, compaction and the
distributed DAG, and this measures what they cost.

- **Arm A (control):** `8b693f30` (v0.18.12), the last measured SF100 baseline.
- **Arm B (candidate):** `e6c6bdad` (v0.18.50), read from the run's own binary
  revision (`vcs.revision=e6c6bdad`, `vcs.modified=false`), staged clean.

## Result

**Answers are identical.** Every one of the 22 queries returns the same row
count and the same value fingerprint on both arms; there are no failures and no
divergences. The row counts are the correct SF100 answers (Q02=100, Q11=92698,
Q16=27840, Q20=17971, Q22=7).

**Wall time is a tie cold, a small improvement warm.**

| | Arm A (v0.18.12) | Arm B (v0.18.50) | delta |
|---|---|---|---|
| cold (run 1) | 151.4 s | 152.1 s | +0.4% |
| warm mean (runs 2–4) | 121.0 s | 118.2 s | −2.4% |
| best warm | 119.8 s | 117.3 s | −2.1% |

The warm improvement is consistent: all three of Arm B's warm runs (118.6 / 117.3
/ 118.5 s) are below all three of Arm A's (122.1 / 119.8 / 121.2 s). It is within
the ±2–3% same-window noise band, so the honest reading is answer-identical and
performance-neutral-to-slightly-faster, not a headline speedup. Per-query warm
means are in the companion `-per-query.txt`; the per-query deltas are within
noise and are not KPIs (Q06 is sub-second and omitted from the table by the
parser).

## Provenance

Arm A results `results/20260906-095046/`; Arm B `results/20260906-100336/`. Both
clusters torn down immediately after their results were pulled. The SF100 bucket
was verified free of compaction residue before the run (see #921 — the SF10 local
gate bucket had been corrupted by background compaction, now fixed and disabled
for benchmark clusters).
