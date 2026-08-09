# Network-bound diagnosis and the base-table cache default (2026-08-09)

One day, one arc: the SF100 steady-regime residual (run 2 ≈ 1.2–1.5×
run 1) was chased through five refuted hypotheses to a two-part root
cause — EC2 network-allowance throttling on burst-networked workers,
amplified by the base-table NVMe cache having been silently disabled in
every benchmark deploy. Fixing the second (a profile default) produced
the largest single SF100 improvement on record.

## Diagnosis chain (all SF100, c7gd.4xlarge ×3, runs=2 per arm)

| Hypothesis | Instrument | Verdict |
|---|---|---|
| Hard faults / reclaim / device | `cpu psi stats` marker (29d63f2): majflt, PSI, iowait | REFUTED — majflt=0 both runs, PSI≈0, device ~idle |
| CPU saturation / scheduling | host busy, sched-wait, per-query CPU profiles | REFUTED — 12–15% busy, coordinator 0.4% |
| Lock/channel contention | block-profile run-boundary diff (`-base`) | REFUTED — no contended site; decode span waits are errgroup fan-out |
| GC latency tax | `-var=gogc=300 -var=task_gc=0` pair (d85e94a) | REFUTED — 4.15× fewer cycles, walls identical |
| Network allowance | SSM `ethtool -S` ENA counters | **CONFIRMED** — bw_in/out_allowance_exceeded climbing 300–900 events/s per worker, continuously, both runs |

Wadjet-vs-Trino on identical shapes: Trino walls 167.6/160.7s vs our
544.7/654.2s but **20/22 queries** (q09+q21 failed both runs, q15
returned 0 rows in run 2 — our 22/22 with identical rows held through
every arm). CONFIG CORRECTION (post-hoc audit of the deployed
properties): despite the harness's FTE labeling, terraform-trino wires
NO retry-policy, NO exchange manager, and NO spill — Trino ran default
PIPELINED/STREAMING mode (in-memory direct exchange, memory-or-die),
its fastest and most fragile configuration; the NVMe scratch mount and
S3 trino-exchange/* IAM grants are prepared-but-unused. The q09/q21
failures are deterministic query.max-memory=42GB cap kills (~14s/~11s
both runs) with no spill relief; q15's zero rows is double-evaluated
CTE + nondeterministic float summation missing an equality match.
Trino sustains
~400 MB/s/worker inbound pinned at the allowance ceiling (37k throttle
events/s); we averaged ~230 MB/s in burst/idle alternation (3k/s).
Byte economy AND pipe utilization both favored Trino — but see below:
our side of that comparison was running cache-disabled.

## The cache finding

Shuffle-io ledger decomposition of a suite pair (per worker): peer wire
17.4 GB (s2, post peer-wire-compression default flip b4828f4), local
18.5 GB, S3 shuffle reads ≈0, uploads 37.5 GB. Exchange ≈6% of measured
NIC rx. The other ~85% (~280 GB/worker/pair) was base-table S3
re-streaming: `base_table_cache_bytes` defaulted to 0 ("pending SF100
validation" that never ran) and the SF100 profile didn't set it, so
every run re-downloaded the dataset — including the ~15 queries per run
that re-scan lineitem.

## Validation pair (20260809-212321 ctl / 214810 trt, bin b4828f4)

| | ctl (cache off) | trt (150 GB) | delta |
|---|---|---|---|
| Run 1 wall | 519.9 s | 323.5 s | **−37.8%** |
| Run 2 wall | 697.7 s | 449.8 s | **−35.5%** |
| Cluster NIC rx | 908.4 GB | 136.5 GB | **−85%** |
| ENA bw_in throttle events | 4.92 M | 0.67 M | −86% |
| Rows | 44/44 | identical | ✓ |

KEEPER: `sf100-distributed.yaml` now sets
`benchmark.base_table_cache_bytes: 161061273600` (150 GB of the 237 GB
instance store); explicit `-var=base_table_cache_bytes=0` disables.

## Standing consequences

- Wall history is bimodal: runs before this flip that didn't set the
  var explicitly carry the ~300–400 GB/pair re-streaming tax. Compare
  new results only against cache-on baselines.
- The steady-regime residual literature (readahead/touch-ahead/
  drop-behind arcs, which ran cache-ON via explicit vars) needs
  re-profiling on the new default before further levers are ranked.
- The ENA allowance remains the arena: on cache-on runs the remaining
  inbound is exchange traffic, and cross-barrier transfer overlap (the
  Trino utilization gap) is the next candidate — now to be re-measured
  post-flip.
- ENA polling recipe (SSM `ethtool -S` + rx/tx statistics) should ride
  every future arm; wall deltas on this rig are uninterpretable without
  throttle context.

Same-day negative results, for the record: shuffle-durability `lazy`
tripled inbound throttle events (deferred-release re-reads) and
regressed run-1; the per-task forced `runtime.GC()` and GOGC=100 are
exonerated for walls (knobs remain for memory-bound work).
