# Worker abandoned scratch root sweep

Source: internal/worker/worker.go — func (w *Worker) sweepAbandonedScratchRoots() {, moved 2026-09-11 (#1026)

sweepAbandonedScratchRoots removes scratch roots left behind by workers that
are no longer running. A worker killed hard cannot run its own cleanup, so
without this the directories accumulate until the disk fills.

EVERY DIRECTORY A WORKER CAN CREATE A ROOT IN IS SCANNED. There are two: the
system temp dir, where the per-PROCESS root goes when no --spill-dir is
configured, and the configured spill directory itself, where an Executor's
per-INSTANCE root goes (scratch_dir.go, #833). Scanning only the first left
the production deployment — an operator-set --spill-dir on an NVMe volume —
with per-task scratch that NOTHING reclaimed after a hard kill:
sweepStaleBuildCacheFiles matches top-level `stage-`/`shuffle-` names and
the roots are a level below them, and this sweeper was not looking there at
all. That is the failure ADR-0009 exists for ("a 98 GB orphan filled a dev
box this way"), and it is a regression this arc introduced and this function
closes.

Ownership is decided by asking the operating system whether the pid in the
name is still alive, not by age: a directory whose owner is running may be
receiving writes RIGHT NOW, and deleting it would break that worker's
query. Every uncertain case — unparseable name, pid still alive, signal
refused — leaves the directory alone, so the failure mode is a directory
that outlives its owner rather than one deleted from under a live worker.
