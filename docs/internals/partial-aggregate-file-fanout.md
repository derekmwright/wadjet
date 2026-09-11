# Partial aggregate file fanout

Source: internal/coordinator/execute_stage_dag.go — aggSplitMinBytes, moved 2026-09-11 (#1026)

aggregatePartialSplit reports whether a partial "aggregate" stage
qualifies for round-robin fan-out, returning the dep ID and per-task
input file groups when it does.

The planner labels grouped partial aggregates DistRoundRobin when
workerCount > 1 (OutputDistribution, StageAggregate case) so the
property algebra forces a hash-shuffle ahead of the grouped final. The
dispatcher's task-count switch, however, had no DistRoundRobin arm — the
correctness-first default ran the partial as ONE task reading the entire
upstream (e.g. a 24-partition join output) while the rest of the cluster
idled. The 2026-07-19 arrival-waits evidence pass measured this as the
exclusive serial leg on Q10 (12.9s partial + downstream effects) and
Q13/Q02/Q03 at SF100. Partial aggregation is valid over any disjoint
cover of its input, so the fix is the same shape as the scan-fused
fan-out (dispatchScanAggregateStage): aggregate disjoint slices in
parallel, let the existing downstream machinery (exchange-repartition
from EnsureDistribution, or dispatchFinalAggregateFanout for Singleton
finals) merge the partials.

Slicing: an even split of the flattened upstream file list across at
most workerCount tasks — the same shape as probe-split. The first cut
of this fan-out used one task PER upstream partition (24-way at SF100)
with no size gate; the 2026-07-19 SF100 validation run showed why the
probe-split precedent caps at workerCount: small aggregates became
swarms of ~2ms tasks whose per-task dispatch/result overhead (~0.5-1s)
dwarfed the work, serialized through max_concurrent worker slots, and
queued the NEXT query's scan tasks behind the swarm (+18% suite task
count, steady pass +28% — slower than its own cold pass). Chunky
tasks or no split.

aggSplitMinBytes is the same "fanout only wins when each task performs
non-trivial work" reasoning as finalAggregateFanoutCandidate's
K > workerCount gate: below it, the single-task partial is already
cheap and the split is pure scheduling overhead. Bytes are the
worker-reported on-disk size of the upstream output; 0 (unknown,
legacy worker) declines the split — degrade to the pre-split shape,
never to a regression.

Guards: SortKeys/Limit/FilterExprs never appear on a partial today
(fuseSortIntoPredecessor folds into finals only; HAVING lands on the
final) — the guard keeps the split sound if that ever changes, since a
sort, limit, or post-filter applied per-slice would be wrong before the
merge has seen all partials. Eager provisional inputs are excluded the
same way probe-split/skew slicing is: their manifest-fed partition-range
convention does not match custom file groups.

WADJET_AGG_SPLIT_MIN_BYTES overrides the floor (small-scale harness runs
force the split path everywhere with =1 so the correctness gate actually
exercises it; SF1 upstreams are all under the production floor).
