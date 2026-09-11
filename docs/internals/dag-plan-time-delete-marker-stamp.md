# Dag plan time delete marker stamp

Source: internal/coordinator/delete_markers.go — queryDeleteMarkersKey, moved 2026-09-11 (#1026)

Merge-on-read deletes on the stage DAG: one stamp, every carrier.

A DELETE marks file-absolute row indices in the manifest instead of
rewriting parquet, and every scan of the marked file has to skip them.
The single-process engine reads the manifest at scan Init; the DAG's
workers must be TOLD, because a worker reading the catalog itself would
let two tasks of one stage see different revisions — a join would then
find a row on one side and not the other.

Where the declaration is attached is the whole design question. #423's
declared-schema fix needed THREE carriers (OpSpec.ColumnTypes,
OpSpec.BuildColumnTypes, Task.ColumnTypes) because a type belongs to an
ALIAS, and every dispatcher that invents a new way to reach a base table
has to pick one — a standing trap the internals map documents. A delete
marker belongs to the FILE, not the alias, so it needs none of that:
stampTaskDeleteMarkers walks every file list a task can carry and emits
one task-level list. That is why it can live at the single choke point
every dispatcher and every retry already passes through
(Scheduler.PublishTasks), and why a future dispatcher gets it for free.

The map itself is the plan's, not a fresh catalog read: executeStageDAG
unions the stages' ScanDeletes (annotated at plan time from the same
manifest object that produced their file lists) and parks it on the
context for the dispatch subtree.
