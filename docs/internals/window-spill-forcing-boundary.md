# Window spill forcing boundary

Source: internal/engine/exec/window_force_spill.go — forceWindowSpillEvery, moved 2026-09-11 (#1026)

Deterministic window-spill forcing — TEST ONLY.

The Window's spill trigger is the Sort's, line for line:
SpillManager.ShouldSpillFor(SpillCheap), which is `tracker.Used() > 40% of
the budget` — a reading of the WHOLE query's memory, not of the window's. In
the type-matrix sweep's 36 window cells that reading is dominated by how many
row-group slabs the SCAN is holding at the instant the window checks, so the
family engaged 22 to 23 of 36 on a 24-core box and 0 of 36 on one core with
one scan worker. Same random variable, same reason, as the sort family's
(sort_force_spill.go carries the per-check measurement).

ForceWindowSpillEvery makes it deterministic: with N set, every Nth
Window.Consume that holds buffered batches writes a run through the real
writer, and the minSortRunBytes floor — a merge-economy heuristic sized
against real pressure — does not apply to it, because there is no pressure
for it to be economising against.

It is read from WADJET_TEST_FORCE_WINDOW_SPILL_EVERY once per process and
settable from Go. It is never set on any production path: the only cost when
unset is one relaxed atomic load per Consume, next to a batch of work.

# The same #864 warning as the sort's knob

Arming it around a whole QUERY bypasses ShouldSpillFor, and ShouldSpillFor is
where the production guard lives: a morsel clone gets a
memory.SpillManager.TrackingOnlyView whose ShouldSpillFor answers false, so a
clone never writes runs that the merge could orphan. The sort's knob armed at
the SQL layer drops 44% to 78% of the rows for exactly that reason (#864).
exec.Window is not in wireCloneSinkSpill's switch today, so it has no clone
arm to be wrong about — but the knob still belongs to exec-level gates that
drive ONE operator until #864 closes and the guard is understood end to end.
