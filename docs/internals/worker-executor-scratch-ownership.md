# Worker executor scratch ownership

Source: internal/worker/scratch_dir.go — const execScratchPrefix = "wadjet-exec-", moved 2026-09-11 (#1026)

Per-task scratch lives under THIS executor's own root, never at a path
derived from the task ID alone.

A task ID is unique to a QUERY, not to a machine. The stage sinks used to
build their scratch directory as `<spillDir>/stage-<taskID>` — or, with no
spill directory configured, as a bare `/tmp/stage-<taskID>` — so any two
executors that ran the same task ID on one host wrote into and, on
finalize, `os.RemoveAll`'d each other's directory. Two workers co-located on
a box is the production shape of that; two `go test` processes in one
package, which reuse fixed task IDs, is the CI shape, and it is where it was
found: four `internal/worker` morsel tests fail in a combined run while a
second test process is using `/tmp/stage-frag-morsel-agg/`, and the package
passes alone (#833).

So the path carries the three things that actually distinguish one
directory's owner from another's: the PROCESS-and-instance (this root), the
QUERY, and the TASK. The root is created once per Executor with
os.MkdirTemp, which is atomic and collision-free by construction — a PID is
not, because PIDs are reused and two Executors can live in one process.
Tests get their root from t.TempDir() by passing it as the spill directory,
exactly like every other spill artifact.

EVERY ROOT THIS FILE CAN CREATE IS A ROOT A SWEEPER REACHES, and that is a
property of the pair rather than of either half: `Worker.sweepAbandonedScratchRoots`
scans the system temp dir AND the configured spill directory, both by pid
liveness, and the MkdirTemp-failure fallback below degrades to the flat
pre-#833 shape that `sweepStaleBuildCacheFiles` already reclaims. The first
draft of this file scanned only the temp dir, which left an operator-set
`--spill-dir` — the production deployment — with per-task scratch nothing
reclaimed after a hard kill.

TestNoRuntimeScratchPathIsHardcodedUnderTmp keeps the /tmp-literal class
closed; TestAnAbandonedScratchRootUnderAConfiguredSpillDirIsReclaimed keeps
this one.
