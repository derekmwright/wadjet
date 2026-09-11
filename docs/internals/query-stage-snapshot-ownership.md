# Query stage snapshot ownership

Source: internal/coordinator/query_tracker.go — snapshotStages, moved 2026-09-11 (#1026)

snapshotStages deep-copies a query's stage map into values that are
independent of the tracker's live *StageInfo pointers. Must be called
with qt.mu held (either lock).

A `copy := *q` (the previous behavior) copies the QueryInfo struct but
q.Stages is a map[string]*StageInfo — the copy's map holds the exact same
*StageInfo pointers as the original. RecordResult mutates those pointees
in place (stage.DoneTasks++, stage.Results = append(...), ...) while
holding qt.mu; a caller that reads fields off the "copy" does so after
Get/List already released the lock, so those reads race RecordResult's
writes with no synchronization at all (#514) — confirmed by -race on a
tight GetQueryStatus poll against a running query.

Results is deep-copied (append to a nil slice) rather than shared: the
field itself — not just its backing array — gets reassigned on every
RecordResult call (`stage.Results = append(...)`), so even a shared slice
header would be read concurrently with that reassignment.

SeenTasks is dropped (nil) rather than copied: it's RecordResult's
task-retry dedup bookkeeping, never read by any exported accessor, and a
shared map reference would still race — worse, map access itself is not
safe for even a concurrent read against a write (unlike slices/scalars),
so this one can't be fixed by taking a value copy of the field.

Dependencies is shared as-is: set once when the stage is constructed
(before Register), never mutated after — aliasing a read-only slice is
safe.
