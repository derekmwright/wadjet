# ADR-0009: Worker scratch lifecycle — adopt-into-cache, paced asynchronous purge

Status: Accepted (async purge default-on since PR #264, 2026-07-24)

## Context

Stage outputs are written into task spill dirs, must outlive the task
(peers and uploads read them), and must die with the query. Two hazards
shaped the design: the 2026-06-10 straggler-adopt leak (a file adopted
after its query's cleanup is orphaned forever), and the 2026-07-24
observation that inline multi-GB unlink storms run on the query-complete
broadcast handler exactly when the next query's tasks start.

## Decision

- **Adopt**: producers rename finalized outputs into the
  `LocalStageCache` per-query tree; the cache owns them until query end.
  Tombstones refuse late adopts after cleanup (leak fix).
- **Purge**: `CleanupQuery` synchronously drops entries and tombstones
  (reads miss immediately), then **detaches** the on-disk tree into
  `.trash` and a janitor goroutine unlinks it paced (64-file bursts,
  5 ms pauses). Inline deletion survives as the kill switch
  (`--async-scratch-purge=false`) and the fallback for rename failure
  or a full janitor queue. Boot-time wipe reclaims `.trash` after
  crashes.
- Both modes emit a purge ledger (`scratch purge inline/deferred/done`
  with handler/purge millisecond costs and bytes).

## Consequences

- Broadcast handlers never do bulk IO; open fds/mmaps survive
  rename+unlink so straggler readers are unaffected.
- Honest status of the motivating hypothesis: the instrumented SF100
  pair (2026-07-24) showed inline purges cost ≤889 ms of unlink time
  and rolled **no** straggler event — the post-big-query straggler
  tails (7–12 s, observed 3× on 07-23/24) remain unexplained. The purge
  ledger now rides every run so the correlation accumulates for free;
  the async default stands on hygiene grounds regardless.

References: `docs/design/async-scratch-purge.md`.
