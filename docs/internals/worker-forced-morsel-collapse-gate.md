# Worker forced morsel collapse gate

Source: internal/worker/morsel_force_collapse.go — var forceMorselCollapseEvery atomic.Int64, moved 2026-09-11 (#1026)

Deterministic morsel-collapse forcing — TEST ONLY.

The morsel-parallel breaker collapses to serial when the shared spill
manager says the fragment is under memory pressure
(`e.sharedSpill.ShouldSpillFor(memory.SpillCheap)` in runBreakerConsumeParallel).
That is a CONDITION, not a plan shape, and it is decided partly by a
process-wide Go-heap gauge whose verdict is cached for 100ms across the whole
process (memory.heapPressureExceeded) and partly by a race: the stop check
runs only after a morsel has been consumed, so a run whose four consumers
drain the whole fixture before the tracker crosses its 40% mark records ZERO
collapses however hard the machine is working.

A gate whose trigger is a condition cannot be relied on to fire (ADR-0027,
and the same lesson #788 taught four investigation rounds running).
TestExecuteFragment_MorselParallel_AggCollapseUnresolvedPrimary asserts what
happens to aggregate state ACROSS a collapse, and it opens by requiring at
least one collapse to have happened — a precondition that was failing under a
loaded parallel suite while nothing was wrong (#564).

With the knob armed, the Nth stop check of every morsel-parallel breaker
collapses regardless of pressure, and it takes exactly the branch pressure
would have taken: the same `collapsed` latch, the same serial continuation,
the same clone merge. A gate observes the production path, not a simulation.

It is read once from WADJET_TEST_FORCE_MORSEL_COLLAPSE_EVERY so an
end-to-end gate can arm it, and is settable from Go for worker-level gates.
It is never set on any production path: unset, the cost is one relaxed atomic
load per consumed morsel, beside a batch of work.
