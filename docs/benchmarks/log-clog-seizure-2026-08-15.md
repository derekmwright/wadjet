# The "GC-seizure" residual was a log-pipeline clog (2026-08-15)

**Verdict:** the last remaining stall mechanism from the 08-14→15 arc — filed
in the handoff as "GC-seizure residual, ~45–60s when mid-query" — is not a GC
mark-cost problem. It is a **log-pipeline saturation freeze**: a quadratic
`task completed` log payload outran the journald sink, and once the stream
backlog filled, synchronous log writes (including the runtime's gctrace line,
printed while the world is stopped) froze whole worker processes until the
sink drained. Frozen workers missed heartbeats, got reaped at 90s, restarted,
and their tasks re-dispatched — that is the entire seizure family.

ADR-0006's arena rejection **stands**: no pointer-density evidence was needed
because real GC cycles top out at 4.7s wall (worst observed, gc 546 on
worker-3f1df7e2), an order of magnitude short of the observed 60–180s
seizures.

## Corrected magnitudes (the handoff under-reported)

The handoff quoted "q17-R3 60.2s, q02-R3 58.5s" — the second number is itself
a victim of the minute-parse trap it warned about. Run 3 of
`results/20260815-011731` actually had **four** seizure-affected queries:

| Query (run 3) | Wall | Baseline (run 1) |
|---|---|---|
| Q01 | 2m4.8s | 25.6s |
| Q02 | 2m58.5s | 12.8s |
| Q16 | 2m21.6s | 6.5s |
| Q17 | 1m0.2s | 12.3s |

## Evidence chain (all from banked `results/20260815-011731`)

1. **Reaps, not slow execution.** Coordinator log: Q01-R3 dispatched
   01:25:34; workers 05bbdd4f and 12475c79 reaped stale (last_seen ~1m42s) at
   01:27:18/01:27:23, tasks re-dispatched to the lone survivor. 05bbdd4f
   reaped again 01:30:18 (Q02), f64b8a1a reaped 01:34:18 (Q16). Every run-3
   seizure maps to a reap.
2. **Workers frozen, not busy.** Stall thread-dumps on i-0de3bf27 taken 76s
   apart (01:26:51, 01:28:07) show every thread in S state with utime/stime
   **unchanged** — zero CPU across the window. Not a GC assist storm, not a
   spin: a full process freeze.
3. **App-timestamp gap.** The frozen worker emitted 442 journal lines with
   app-minute 01:24, 245 in 01:25, then **2** in 01:26 and **5** in 01:27 —
   silent exactly through the reap window, resuming 01:28.
4. **Journald ingestion lag sawtooth.** (receive_time − app_time) on all
   three workers climbs monotonically from suite start: 1–2s at 01:18 →
   75s at 01:23 → **278s** at 01:30, snapping to ~0 only when the worker
   process restarts, then climbing again in run 4. Production outran the
   sink from the first minutes; the freeze fired when the backlog filled.
5. **The sink ceiling.** Journal ingestion flat-tops at ~3.5 MB/min
   (~58 KB/s) per worker for minutes on end — a hard drain ceiling
   (journald stream path on the root EBS volume).
6. **The payload.** `msg="task completed"` lines are **52 MB of the 57 MB**
   worker journal (91%), 1,753 lines averaging ~30 KB. Line size grows
   0.8 KB → 48 KB over a run and resets only on worker restart.

## Root cause (two compounding defects)

**(a) Quadratic log payload.** `SpillManager.RegisterAccounted`'s unregister
closure appended every closed operator's final footprint to a per-instance
`departedFootprints` slice held for the life of the process-shared
SpillManager. `Inspect()` returned the whole list at every task end
(`executor.go` collectTaskStats), and `worker.go` logged all of it in the
`operator_peaks` attr of every `task completed` line. Total volume is
O(tasks × closed instances) — quadratic — and consists mostly of
`state=closed` 0MB entries.

**(b) STW log writes.** `GODEBUG=gctrace=1` (added 9031acd for the seizure
investigation) prints its per-cycle line **while the world is stopped**. With
the stream backlog full, that write blocks inside the STW window and the
entire process — heartbeat sender included — stays frozen until the sink
frees space. User-goroutine log calls block too (slog handler mutex →
convergent pile-up), but the gctrace STW write is what freezes even
goroutines that never log.

## Fix

- `internal/engine/memory`: departed footprints now coalesce **by operator
  Name** — max-peak instance kept, instance count recorded
  (`OperatorFootprint.Departed`, wire `OperatorPeak.Closed`, log suffix
  `;n=<count>`). `Inspect()` output is bounded by distinct operator names
  regardless of how many instances have ever closed. Regression test:
  `TestInspect_DepartedBoundedByDistinctNames` (1,200 closed instances, 3
  names → 3 entries, counts and max peaks preserved).
- `deploy/benchmark/terraform/main.tf`: `GODEBUG=gctrace=1` removed from
  worker units. Its diagnostic job is done; it is an STW-write hazard on any
  sink hiccup. Re-enable only for a dedicated GC investigation.

Gates run: full unit suite green, TPC-H SF0.01 22/22, multi-process local
harness (`tpch-harness --mode=local --slice=small`) 25/25.

## What this closes and what it doesn't

- Closes: the seizure/reap family (30m-hang class was already fixed by
  bbdb985; memcg throttle by bf4b895; this was the last firing mechanism).
- The 194.4s suite figure from this window already excludes the seizure run;
  a post-fix window should show run-to-run spread collapsing (no 2–3 minute
  outlier queries) rather than a headline-mean shift.
- **Watch item:** Q02-R3 and Q17-R3 also showed a ~50–60s coordinator-side
  gap between the previous query's completion and "routing to native DAG
  executor" (Q02's was 60.03s exactly, with zero coordinator log lines in
  between). It only appeared immediately after reap events; expected to
  vanish with the reaps. If a post-fix window still shows pre-dispatch gaps,
  instrument the parse→optimize→fast-path-estimate→plan segment — the
  fast-path `EstimatePlanScanBytes` bail path is silent.
- **Not chased:** why the journald drain ceiling is ~58 KB/s on these boxes
  (suspect EBS/journal fsync). At post-fix volumes (~0.3 MB/min/worker) the
  margin is >10×, so it stops mattering; do not tune journald as a fix.

## Validation (window `results/20260815-113452`, bin 92cc687, same config)

Same shape as the seizure window: 4-run SF100 distributed, on-demand
c7g.2xlarge + 3× c7gd.4xlarge, binaries pinned to the fix commit.

- **Zero worker reaps, zero stuck-task re-dispatches** across all 4 runs
  (seizure window: 4 reaps in run 3 alone).
- **No minute-scale outliers**: worst single query in the whole window is
  Q09 at 27.8s. Run totals 227.5 / 167.2 / 182.8 / 165.4s — runs 2 and 4
  are the **two best suites ever recorded** on this config (previous record
  187.2s); the run-to-run spread collapsed exactly as predicted.
- **Mechanism counters**: journald ingestion lag max **2–3s** flat across
  all three workers for the whole window (was a monotonic sawtooth to
  278s); `task completed` lines max **3.3 KB** (was 48 KB); worker journal
  totals ~6 MB (was 57–77 MB).
- **Watch item closed**: worst gather→next-dispatch gap is **0.4s** (was
  60.03s) — the coordinator pre-dispatch stall was a reap consequence, no
  further instrumentation needed.
- Correctness: 22/22 × 4 runs OK, Q02=100 rows, Q18=100 rows, no zero-row
  queries, no row drift across runs.

The seizure family is closed end-to-end: mechanism identified, fix shipped,
counters and walls both confirm.
