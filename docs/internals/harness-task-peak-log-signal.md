# Harness task peak log signal

Source: internal/harness/spillcheck.go — func maxTrackerPeakMB(logsDir string) (int64, error) {, moved 2026-09-11 (#1026)
Superseded: ForceReserve can exceed the configured budget, and a logged tracker peak does not by itself prove eviction. The assertion that peak never exceeds budget and directly proves spill is stale.

maxTrackerPeakMB scans every *.log file directly under logsDir (coord.log,
worker-N.log — written by Cluster.spawn via cmd.Stdout/Stderr) and returns
the largest tracker_peak_mb value logged by any task.

Why this instead of the worker heartbeat's SpillDiskUsed: workers
heartbeat on a fixed 10s cadence (internal/worker/worker.go), while a
single local-mode query — even one whose build side is forced through the
spill path — routinely completes in well under a second. A per-query
heartbeat window can open and close between two ticks and see nothing, not
because nothing spilled but because the sample missed it; worse, the
spilling task's own spill directory is removed via `defer
os.RemoveAll(...)` (executor_fragment.go) the instant the task returns, so
even a "wait and re-check" strategy loses the race against a task that
finishes in milliseconds.

tracker_peak_mb has neither problem: it's written to the log file
synchronously when the task completes (collectTaskStats reads
tracker.Peak() before the deferred cleanup runs), so there's no sampling
window to miss. And because memory.Tracker.Reserve() rejects any request
that would push `used` over `budget` (internal/engine/memory/tracker.go),
a task's tracker_peak_mb can never exceed its configured budget — a task
that only ever needed a few MB peaks at a few MB, while a task whose build
side doesn't fit saturates at the ceiling. buildPartitioned
(internal/engine/exec/join_partition_arrival.go) only reaches that
ceiling by way of a failed Reserve() that falls through to
spillUntilCanReserve, i.e. an actual partition eviction to disk — so
observing the configured budget in tracker_peak_mb is direct evidence the
spill path ran, not just that a budget was configured.
