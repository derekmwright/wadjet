# Worker breaker morsel parallelism

Source: internal/worker/executor_fragment.go — func (e *Executor) runBreakerConsumeParallel(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink exec.MergeableSink, k int, gate *widthGate) error {, moved 2026-09-11 (#1026)

runBreakerConsumeParallel is the morsel-parallel variant of the breaker
consume phase (source → first breaker): the single producer feeds a
bounded channel consumed by k goroutines, each running a Clone()d op
chain into its own CloneSink partial; partials merge into the primary at
the barrier (the exec.Pipeline.runParallel shape, with two additions the
never-OOM rules require — memo §4.3):

 1. Clones RESERVE. Each clone sink charges its accumulated state to a
    tracking-only view of the shared SpillManager, so admission and the
    primary's spill trigger see the k× partial footprint. Clones never
    spill — there is no concurrent spill format.
 2. Pressure COLLAPSES k. When the real SpillManager's ShouldSpillFor
    trips during parallel consume, the consumers stop, partials merge
    into the spill-armed primary (the merge transfers the memory
    accounting), and the remaining input drains serially through the
    ORIGINAL chain into the primary — whose own partial-drain spill
    machinery is exactly today's SF100-validated path. Parallelism is a
    fair-weather optimization; the pressure story is unchanged serial.

Sel is snapshotted before every breaker Consume: breakers retain batches
(Sort stores them) while upstream Filters reuse per-instance Sel scratch —
same rule as drainThroughBreaker. Finalize on the primary is called here,
matching the serial Pipeline.Run contract for the j==0 phase.
