# Harness large slice spill signal

Source: internal/harness/harness.go — if cfg.Mode == ModeLocal && cfg.Slice == SliceLarge {, moved 2026-09-11 (#1026)
Superseded: The 40% gate is a tracker-pressure proxy, not proof of spill engagement; ForceReserve can raise tracked usage without eviction and floating-budget thresholds differ.

ExpectSpill assertion for large slice.

The primary signal is maxTrackerPeakMB: it reads the "task completed"
log lines already captured under runDir/logs/*.log and finds the
largest tracker_peak_mb any task logged. That value is written
synchronously when a task finishes — see maxTrackerPeakMB's doc
comment for why that sidesteps the heartbeat timing problem below.

The threshold is 40% of sliceCfg.MemoryBudget, not "saturated at the
ceiling": SpillManager.ShouldSpillFor (internal/engine/memory/
spill.go) proactively evicts a partition once a task's tracked usage
crosses 40% of budget (the "SpillCheap" threshold), so a task under
genuine sustained pressure sawtooths just above that line rather
than climbing to 100% — eviction keeps knocking it back down before
it gets there. Empirically, forcing this fixture's build side over
budget peaked its tracker at 62-100% of budget across several runs,
comfortably over 40%; an unpressured SF0.01 TPC-H query's tiny
tables shouldn't get within reach of even that.

collector.RunPeakSpillBytes is a fallback for the same assertion via
the worker heartbeat's SpillDiskUsed, in case a future change moves
the tracker_peak_mb logging or a task's spill genuinely outlives one
heartbeat tick. Workers heartbeat on a fixed 10s cadence
(internal/worker/worker.go) while a single local-mode query — even
one under real memory pressure — often completes in well under a
second, so this fallback alone is not reliable: a per-query
heartbeat window can open and close between two ticks and see
nothing, and the spilling task's own spill directory is removed via
`defer os.RemoveAll(...)` (executor_fragment.go) the instant the
task returns, so even "wait and re-check" can lose that race. Kept
as a fallback because it costs nothing when the log-based signal
already passed, and needs no per-query timing luck itself —
RunPeakSpillBytes tracks every heartbeat regardless of window
boundaries — for whatever residual chance it adds.
