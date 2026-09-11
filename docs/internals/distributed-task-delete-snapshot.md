# Distributed task delete snapshot

Source: internal/distributed/messages.go — DeleteMarkers []DeleteSpec `json:"delete_markers,omitempty"`, moved 2026-09-11 (#1026)

DeleteMarkers carries the merge-on-read DELETE state for every
base-table parquet file this task reads, under whichever carrier the
file arrives on — Files, Inputs, PreScannedInputs, ScanFileFilter,
BuildFiles, FusedJoins[].BuildFiles or Operators[].{InputFiles,
BuildFiles}. One task-level list rather than a field per carrier,
because the marker is a property of the FILE, not of the alias that
happens to read it: a self-join reading one file twice must skip the
same rows on both sides, and the coordinator cannot enumerate the
carriers a future dispatcher will invent. Stamped centrally in
Scheduler.PublishTasks from the plan's one manifest read, so every
task of a query — retries included — sees ONE catalog revision's
delete state (#491).

A DELETE records the file-absolute row indices it removed rather than
rewriting parquet; a scan that ignores them answers with the deleted
rows still in it. The single-process engine reads the manifest
itself; the DAG's worker has no business doing so (two tasks of one
stage could then read different revisions and a join would see a row
on one side and not the other), so the plan declares it — the same
argument as ColumnTypes and AggSpec.OutputType.

Empty for every task over a table with no deletes, which is the
common case and costs nothing on the wire.

ADR-0010 WHOLESALE-DEPLOY RULE: a worker that predates this field
unmarshals it away and answers with the deleted rows — silently, and
only for the fraction of a stage's tasks that landed on the old
binary. Coordinator and workers deploy together, never rolling, for
exactly the reason the partition-assignment function does.
