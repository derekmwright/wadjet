# ClickBench — v0.18.50, 2026-09-06

Single node c6a.4xlarge (16 vCPU / 32 GB / 500 GB gp2), amd64, `hits` 100 parts
/ 14.74 GB on S3 (us-east-2), 3 tries, 18 GiB memory budget, binary `e6c6bdad`
(`vcs.modified=false`). Results `results-c6a-20260906-v01850.json`; the run's
raw output is `results/clickbench/20260906-101838/` in the bench bucket.

## Correctness

**43 of 43 queries return, zero nulls.** This is the bar, and it is met.

## Performance vs the last window (cross-window)

Compared against `results-c6a-20260822-v0170.json` (v0.17.0-clawback, engine
`1441ca4`, 2026-08-22 — the perf-tuned release). This is a **cross-window**
comparison two weeks and a release line apart, on a dedicated single node.

| | v0.17.0 (2026-08-22) | v0.18.50 (2026-09-06) | delta |
|---|---|---|---|
| cold (Σ try 1) | 161.5 s | 179.7 s | +11.2% |
| hot (Σ min try 2/3) | 84.6 s | 107.1 s | +26.6% |

The 0.18.x line is the correctness line: it traded some ClickBench throughput
for correctness, and the regression is concentrated in two queries whose slowdown
traces to specific correctness fixes:

- **Q29** (`SUM(ResolutionWidth), SUM(ResolutionWidth+1), … +90`): 0.113 s →
  14.570 s. Integer `SUM` is now an exact `bigint`/`numeric` accumulator that
  errors on overflow rather than a `float64` that silently wraps (ADR-0024). A
  ~90-way exact-integer wide aggregate is far heavier than 90 float sums; this is
  the dominant single regression.
- **Q42** (`DATE_TRUNC('minute', EventTime)` grouped): 0.146 s → 1.086 s.
  `DATE_TRUNC` declares `TIMESTAMP` and boxes epoch-milliseconds since v0.18.44
  (so a client reads a timestamp OID, not text), which changed the group-key
  path.

The rest are 10–35% costs from the per-operation guards the correctness work
added; per-query detail in the companion `-per-query.txt`. Q28/Q29 of the
ClickHouse dialect now spell byte length with `OCTET_LENGTH` (wadjet `LENGTH`
counts characters since v0.18.34); the 2026-08-22 window predates that and used
`LENGTH`-counts-bytes, so those two are comparable.

## Lead

Performance recovery for the wide-integer-aggregate and DATE_TRUNC-group paths is
0.19+ work, tracked as a perf lead. Correctness is not in question: SF100 is
answer-identical to v0.18.12 and ClickBench is 43/43 with zero nulls.
