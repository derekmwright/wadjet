# Join build partition on arrival

Source: internal/engine/exec/join_partition_arrival.go — HashJoin.buildPartitioned, moved 2026-09-11 (#1026)

buildPartitioned is the partition-on-arrival build path. Instead of
accumulating every batch flat and reactively switching to partitioned-spill
on first pressure event, this path allocates spillState upfront, scatters
every arriving batch into its 64 hash partitions, and indexes per-partition
rows incrementally into the global hash table. When pool pressure rises,
spillOneInMemoryPartition picks the largest in-memory partition, writes its
batches to disk, and frees them — an O(partition_size) eviction instead of
the legacy path's O(total_size) "freeze, repartition everything, reset
hash-table, rebuild from in-memory partitions" sequence.

This matches the Grace Hash Join shape that Spark's UnsafeShuffleSorter and
Trino's HashBuilderOperator implement: build is partitioned-by-default, so
spill is just "evict one partition," not a global state reset.

Probe-side correctness: HashJoinProbe.Execute already routes spilled-partition
rows to disk before any hash lookup when spillState != nil, and within a
hash-bucket all chain entries share the same key (intHashTable.Get returns the
chain head for an exact key match) — so the chain for a probed key always
resolves to a single partition. If that partition is in-memory the whole
chain points to live batches; if it's spilled the partition routing has
already diverted the probe row to disk. The freed h.buildBatches[i] = nil
slots are therefore unreachable on the in-memory probe path.

That argument is about a KEY-ROUTED probe, and it was written as though
every probe were one. A CROSS join's is not — it reads every build batch for
every probe row and never reaches the routing at all — so it read the nil
slots, which is #832. probeRoutesByPartition above is the same sentence
turned into a precondition this path's caller checks.

Caller invariants:
  - h.MemTracker and h.Spill must both be set; otherwise the legacy path runs.
  - h.probeRoutesByPartition() must hold; otherwise the flat path runs.
  - SemiAntiKeyOnly takes its own no-storage build path before this fires.
  - The serial build path is the production caller; parallel-build (which
    merges per-worker locals) is currently key-only and not affected.
