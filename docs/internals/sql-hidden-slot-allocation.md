# Sql hidden slot allocation

Source: internal/planner/sql/reserved_slots.go — type SlotAllocator struct {, moved 2026-09-11 (#1026)

SlotAllocator hands out fresh hidden-slot names for ONE query scope.

It exists because `SlotName` is a pure namer and nothing ALLOCATED. Two
independent authors then wrote the same bug against it within a day: a slot
search that excluded the names already in scope but not the slots it had
itself already issued.

  - The window renamer, moving a slot past a stored `__win_0` column, took
    the first name not in the STORED set — `__win_1`, which the query's
    SECOND window already held. Both wrote `__win_1` and the by-name
    projection handed window #2 window #1's value. Silent, single-process
    path only, so a two-path divergence as well as a wrong number.
  - The group-key minting, materializing two computed GROUP BY keys, skipped
    names in scope but not slots issued to earlier keys of the same
    aggregate. Two keys landed in one column and twelve groups collapsed to
    three. Silent.

One shape, two authors, because the shared API let each of them write their
own search. This is the only way a slot may be obtained; `SlotName` remains
for rendering a known index and for tests.

Not safe for concurrent use: an allocator belongs to one query scope, which
is planned on one goroutine.
