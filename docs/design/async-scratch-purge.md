# Async scratch purge (`--async-scratch-purge`)

Status: implemented 2026-07-24. Default on; `=false` is the inline-deletion
kill switch and A/B control arm.

## The bug (diagnosed from SF100 logs, 2026-07-24)

The suite's volatile small queries — Q22, Q14, Q11, swinging ±2–10s across
otherwise-identical arms all week — share one signature and one position:

- Each runs **immediately after a top-scratch query** (Q22 after Q21's
  33.6 GB, Q14 after Q13's 23.1 GB, Q11 after Q10's 19.5 GB).
- In slow runs, most of the query's sub-second tasks complete instantly
  and then one or more workers' remaining wave sits silent for 7–12s
  (Q22 join-4: 20/24 tasks < 5s, last task at +12s; dispatch spread
  uniform; occurs identically under eager/lazy durability and with
  locality placement off — mechanism-independent).

When a query terminates, the coordinator broadcasts `CompleteSubject` and
every worker's handler runs `LocalStageCache.CleanupQuery`: an **inline
unlink of the query's entire adopted scratch** (per-file `os.Remove` +
`RemoveAll`), gigabytes across thousands of files, precisely while the
next query's first tasks are starting on the same NVMe volume. The unlink
storm (journal traffic, extent freeing, page-cache invalidation) is the
prime suspect for the stalls.

## The fix

`CleanupQuery` keeps its synchronous contract for correctness-relevant
state — entries dropped, tombstone recorded, `Get` misses immediately,
late `Adopt` still declined — but detaches the on-disk work:

1. Rename the per-query directory into `<cache>/.trash/<qid>-<seq>`
   (instant; open fds and mmaps remain valid across rename and unlink,
   so a straggler reader is no worse off than under inline deletion).
2. A janitor goroutine unlinks the trashed tree **paced** — bursts of 64
   files with 5ms pauses — bounding IO contention with the running query.
3. Fallbacks stay inline: kill switch, rename failure, no on-disk dir,
   or a full janitor queue (blocking the broadcast handler would
   recreate the bug). Boot-time `RemoveAll(rootDir)` already reclaims
   anything a crash strands in `.trash`.

`broadcastJoinCache.CleanupQuery` releases in-memory builds only (no file
storm) and is untouched.

## Instrumentation (both modes)

- `"scratch purge inline"`: query, entries, bytes, `handler_ms` — in the
  control arm this measures the storm the broadcast handler absorbs, and
  its timestamps should overlap the straggler windows if the diagnosis is
  right.
- `"scratch purge deferred"` (`handler_us`) + `"scratch purge done"`
  (files, bytes, `purge_ms`) in the treatment arm.

## Validation

Unit (async purge semantics, tombstone, idempotence, no-adopt path),
worker `-race`, TPC-H SF0.01, tpch-harness local both arms. SF100
same-window pair: control `-var=async_scratch_purge=false` (instrumented
inline — confirms the correlation), then treatment (default). Success:
`handler_ms` storms in control align with straggler windows; treatment
collapses the Q22/Q14/Q11 tails; suite steady noise floor drops; rows
44/44. Local repro is NOT a gate — the effect needs real multi-GB scratch
churn ([[feedback_local_repro_lies]]).
