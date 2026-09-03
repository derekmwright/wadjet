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

## Amendment 2026-09-03: a scratch path names its OWNER, not just its task

A task ID identifies a task within a QUERY, so it does not identify a
directory on a HOST. The stage sinks built their scratch as
`<spillDir>/stage-<taskID>` — or, with no spill directory configured, as a
bare `/tmp/stage-<taskID>` — and every executor on the box derived the same
path. Both finalize paths end in `os.RemoveAll` on it, so whichever task
finished first deleted the other's partition files: `stat spill file: no such
file or directory`. Two co-located workers is the production shape; two `go
test` processes in `internal/worker`, which reuse fixed task IDs, is the CI
shape, and it is where it was found — four morsel tests fail in a combined run
and the package passes alone (#833).

**A per-task scratch path is `<scratch root>/<executor instance>/<query>/<kind>-<task>`.**
The instance segment is created once per `Executor` with `os.MkdirTemp`, which
is atomic and collision-free by construction; a pid is not, because pids are
reused and two executors can live in one process. It nests under whatever this
ADR's scratch root already is — the configured `--spill-dir` when there is one,
the per-process `wadjet-worker-<pid>` directory when there is not.

**Reclamation follows the root, and this took two passes to get right — the
first one moved the scratch out of the reach of the only sweeper that had ever
found it.** Before this amendment a hard-killed worker left
`<spillDir>/stage-<taskID>/`, which `sweepStaleBuildCacheFiles` reclaims by its
top-level `stage-`/`shuffle-` arms ("this is the only path that reclaims
those", its own comment). Putting the same files a level down, under a private
root, took them out of that match — and `sweepAbandonedScratchRoots` opened the
system temp dir and nothing else, so on the production deployment (an
operator-set `--spill-dir` on an NVMe volume) NOTHING reclaimed them. A worker
that is OOM-killed leaked its per-task scratch forever: the 98 GB failure this
ADR exists for, reintroduced by the fix for #833.

So the invariant is stated as a property of the pair, not of either half:

- **Every directory a worker can create a root in is a directory a sweeper
  scans.** `sweepAbandonedScratchRoots` walks the system temp dir AND
  `w.config.SpillDir`, and ownership is decided by pid liveness in both — both
  root names carry their owning pid, `scratchRootOwnerPID` reads it out of
  either, and a root whose owner is still running is never touched.
- **The MkdirTemp-failure fallback degrades to a shape that is still
  reclaimed.** When no private root can be made, a task's scratch is the flat
  pre-#833 `<base>/<kind>-<task>`, which `sweepStaleBuildCacheFiles` already
  matches — not `<base>/<query>/<kind>-<task>`, which no sweeper would see. A
  collision in that case is recoverable; an unreclaimable orphan is not.

`Worker.Stop` removes its executor's instance root on the clean path; the
per-task directories under it are still removed by the sink that made them.

Gates: `worker.TestTwoExecutorsRunningOneTaskIDKeepSeparateScratch` and
`TestTwoExecutorsRunOneTaskIDConcurrentlyAndBothAnswer` (the end-to-end arm,
ten iterations of one task ID on two executors at once) fail on revert;
`TestNoRuntimeScratchPathIsHardcodedUnderTmp` closes the /tmp-literal class by
scanning the query-executing package trees; and
`TestAnAbandonedScratchRootUnderAConfiguredSpillDirIsReclaimed` closes the
disk-leak class — the old shape reclaimed, the new shape reclaimed, and a LIVE
owner's root untouched, with a spill directory that is NOT under the temp dir.
That last condition is the one the suite lacked: every scratch-sweep test that
existed set `TMPDIR`, which is exactly why the regression above was invisible
to it. `TestScratchFallbackShapeStaysReclaimable` covers the fallback. Operator-facing
defaults in `internal/harness` and `cmd/` are out of that scope on purpose: a
default a human overrides on the command line is not a per-task directory two
processes race on.

References: `docs/design/async-scratch-purge.md`.
