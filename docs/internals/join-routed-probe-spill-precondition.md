# Join routed probe spill precondition

Source: internal/engine/exec/join_partition_arrival.go — HashJoin.probeRoutesByPartition, moved 2026-09-11 (#1026)

probeRoutesByPartition reports whether every probe row consults ONLY the
build rows of its own grace partition. It is the precondition
spillOneInMemoryPartition's contract rests on, and the build dispatch
(Build, join.go) now CHECKS it rather than assuming it, because a build that
partitions and evicts is only readable by a probe that routes.

A CROSS join is the join for which it is false, and it is false completely:
its probe has no key, so `computeBuildPartitionRows` sends every build row
to ONE partition (the empty key's), `HashJoinProbe.Execute` returns to
`nextCrossChunk` before the spilled-partition routing runs at all, and
nextCrossChunk then walks EVERY entry of h.buildBatches for every probe row.
One eviction therefore nils the slot it is about to read — a nil
dereference at `buildBatch.Len`, and, if the nil were skipped instead, a
silently missing row, which is worse.

This is the shape #832 arrives as. A join whose ON clause is an equality of
EXPRESSIONS rather than of bare columns has no equi-key for the planner to
give the operator, so it is planned as a cross join with the ON as a filter
above — `ON CONCAT('x', a.g) = CONCAT('x', b.g)`, `ON (a.c_str || 'x') =
(b.c_str || 'x')`, `ON UPPER(a.c_str) = UPPER(b.c_str)`, `ON a.id + 1 =
b.id + 1`, a CAST key, a key computed on one side only. All of them answer
on the single-process, DAG and DAG-shuffled arms and panic on the spilled
one, and the panic is not a race: a cross join whose build evicts ALWAYS
reads a nil slot.

A cross join therefore takes the flat build, which reserves per batch and
REFUSES when the budget cannot hold the build (ADR-0006: degrade or fail
loudly, never die and never answer differently). What it does not get is a
spill, because wadjet has no blockwise nested-loop join to spill INTO; that
is a distinct piece of work and it is recorded on #832 rather than faked by
a partitioner whose premise the operator does not meet.
