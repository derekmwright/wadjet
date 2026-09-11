# Table less distributed handoff

Source: internal/planner/physical/table_less_refusal.go — ErrTableLessSelectDistributed, moved 2026-09-11 (#1026)

ErrTableLessSelectDistributed marks a plan the stage DAG refuses because it
reads from no table at all.

`SELECT CONCAT('a', NULL, 'b')`, `SELECT 1`, `SELECT current_schema()` —
every SELECT with no FROM — becomes a `logical.NodeDual`, and `walkStages`
emits a Stage of type "dual" with `Tasks: 1`, no dependencies and no
ScanFiles. Nothing downstream can run that: `buildTaskInputsForStage`'s
default arm requires a dependency and fails the dispatch with

	stage dual-0 has no dependencies and no ScanFiles

so the query FAILS on the DAG (#806). It is not rare — pgwire's synthetic
answers cover the introspection shapes a BI client sends, but anything past
that list reaches the engine, and every table-less SELECT a user writes does.

The refusal is a HANDOFF, not the query's outcome: `Coordinator.ExecuteSQL`
matches this error and answers on the coordinator-local single-process
pipeline, exactly as it does for six other constructs the DAG has no stage
for. The `dual` stage's own comment already says "runs locally on
coordinator"; this is what makes that true.

There is nothing to distribute here — a table-less SELECT is one row — so
routing costs nothing for the shape the issue is about. It DOES cost
something for a plan where a dual sits beside real scans, `SELECT 1 UNION
ALL SELECT c FROM big` being the shape: the whole query then runs in the
coordinator's process. That is a performance cliff, not a wrong answer, and
it is the same trade every other refusal here makes. Making the DAG execute
a dual stage — a single-row source operator, its fragment builder and a
wire tag — removes it; that is a feature, not this refusal.
