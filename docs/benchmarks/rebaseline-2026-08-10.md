# Post-populate Trino re-baseline + the q22 dispatch-stall discovery

**Date**: 2026-08-10 (~02:00–02:30 UTC window)
**Arms**: Wadjet bin 78a3f3d (populate toucher + base-table cache
defaults, `benchmark_runs=2`) then Trino 470 (default pipelined config —
same mode caveat as docs/benchmarks/network-bound-diagnosis-2026-08-09.md;
their fastest, most fragile mode) on identical instance shapes,
same night, both destroyed after collection.

## Matched-set result (20 queries — Trino fails q09+q21 both runs)

| | Wadjet | Trino | ratio | pre-populate ref |
|---|---|---|---|---|
| cold (run 1) | 297.8 s | 164.2 s | **1.81×** | 1.65× (275.7/167.6) |
| steady (run 2) | 338.9 s | 156.2 s | **2.17×** | 2.36× (379.4/160.7) |

Correctness unchanged: Wadjet 22/22 rows both runs; Trino 20/22
(q09/q21 `query.max-memory` kills, no spill) plus the q15 run-2
zero-row CTE flake. Populate engagement 100 % (124.2 GB, 0 drops).

Steady improved ~0.2× despite this window's run-2 being a bad draw —
because the "bad draw" turned out to be one query (below). Excluding
q22's excess, matched-set steady is 295.8 s → **1.89×**, and run-2
drift on the other 19 queries is ~zero (297.8 → 295.8). The populate
toucher has effectively closed the *scan-side* steady drift; what
remains is event-shaped, not throughput-shaped.

## The q22 finding: a coordinator dispatch stall, not variance

q22 run 2: 5.6 s → 48.7 s reported (and the true span is worse — see
below). Coordinator timeline (query ad6030c5):

- 02:10:15.5 — stage-DAG dispatch (9 stages, `dispatch_concurrency=12
  source=cluster_capacity`)
- 02:10:17.4 — ALL scans + the orders repartition (148 M rows,
  9 tasks, 24 partitions) complete. Cluster then fully idle.
- 02:10:17 → 02:13:59 — **3 m 42 s of nothing**: `task_progress
  delta_delivered=0` every heartbeat, no reap/retry/redeliver/slot
  events, no worker activity.
- 02:13:59.2 — `dispatching compute stage join-4` (24 tasks) —
  completes in ~5 s; aggregate + final in ~5 s more.

The consumer stage's dispatch was gated for 3m42s with all inputs
ready, all capacity free, and total silence — then proceeded normally.
Shape: a lost dispatch wakeup rescued by some periodic sweep. This is
the coordinator-side cousin of the morsel-collapse lost-wakeup
suspicion in the 08-09 wedge fingerprint.

Corroboration that this is a standing event class, not a one-off:
q22 run-2 also blew up 3.8× in the same night's SF10 capped off-arm
(1.06 s → 4.0 s) and 2× in the on-arm. Smallest-input query, largest
late-suite repartition, always run 2 — whatever state the wakeup race
needs, end-of-suite run 2 has the most of it.

Measurement nuance: the harness reported 48.7 s for a query whose
dispatch-to-gather span was ~229 s — per-query walls under-count
coordinator-side stalls, and R2's sum-of-walls (571 s) disagrees with
the actual run span. Trust coordinator timestamps for stall forensics.

## Standing numbers

- Public-claim posture: cold ~1.8×, steady ~1.9–2.2× vs Trino's
  fastest (pipelined) mode, with 22/22 vs 20/22 correctness. The
  fair-fight (spill-enabled/FTE) Trino arm remains an open PM call.
- Next engineering target: the dispatch-stall class. Find what wakes
  `dispatching compute stage` when a shuffle side completes, and what
  the ~3–4 min rescue is; instrument dispatch-gate wait time per stage.
  Fixing it removes the dominant remaining run-2 event tax.
