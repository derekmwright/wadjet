# Join partition eviction forcing

Source: internal/engine/exec/join_force_spill.go — forceJoinEvictEvery, moved 2026-09-11 (#1026)

Deterministic join-partition eviction forcing — TEST ONLY.

A grace hash join evicts a build partition when
`SpillManager.ShouldSpillFor(SpillCheap)` is true, which reads the WHOLE
query's memory rather than the join's own. On a small fixture that reading
is dominated by what the scan happens to be holding when the build checks,
so whether a join spills at all is a coin toss: the same shape at the same
budget evicted a partition in one run of `go test` and not in the next, on
one machine, with no code change between them.

That is the condition ADR-0027 decision 6 already gave the aggregate
(`ForceAggDrainEvery`), the sort (`ForceSortSpillEvery`) and the window
(`ForceWindowSpillEvery`) a knob for, and the join was the one pipeline
breaker without one — so a gate for a defect that only exists AFTER an
eviction (the nested pipeline that never drained the evicted partitions,
#1010) had nothing to make its own trigger fire.

With N set, every Nth batch a partition-on-arrival build absorbs evicts one
in-memory partition through the real path — the same `spillOneInMemoryPartition`
pressure calls, writing real spill files — and the pressure check is bypassed
for those, because there is no pressure for it to be measuring. What a gate
observes is the production eviction, the production probe routing and the
production flush.

It is read from WADJET_TEST_FORCE_JOIN_EVICT_EVERY once per process so an
end-to-end gate at the SQL layer can arm it, and settable from Go. It is
never set on any production path: the only cost when unset is one relaxed
atomic load per arriving build batch, next to a partition scatter.
