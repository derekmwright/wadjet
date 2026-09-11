# Aggregate drain forcing

Source: internal/engine/exec/aggregate_force_drain.go — forceAggDrainEvery, moved 2026-09-11 (#1026)

Deterministic drain forcing — TEST ONLY.

Every spill defect in a HashAggregate is condition-triggered: it needs a
drain to land on a particular batch (the one that migrated the key path, the
one that carried a NULL key, the one whose group is wholly inside the drained
run). Under a real memory budget WHICH batch drains is decided by tracker
pressure, so a gate written against the budget alone reproduces the defect
some of the time and passes for the wrong reason the rest of it — five
replications of the 512 KiB arm of #782's DECIMAL twin produced the wrong
answer twice.

ForceAggDrainEvery makes the drain deterministic: with N set, every Nth
HashAggregate.Consume takes the same drain branch memory pressure would have
taken, and the drain-productivity gate (#325) is bypassed for those drains
because there is no pressure for it to measure. The aggregate still needs a
spill directory — a forced drain writes real run files through the real
writer, so what a gate observes is the production path, not a simulation.

It is read from WADJET_TEST_FORCE_AGG_DRAIN_EVERY once per process so an
end-to-end gate at the SQL layer can arm it, and settable from Go for exec
level gates (ForceAggDrainEvery / ResetForcedDrains). It is never set on any
production path: the only cost when unset is one relaxed atomic load per
Consume, next to a batch of work.
