# Grace join partition index ownership

Source: internal/engine/exec/join_index_parts.go — joinIndexPart / idxPart, moved 2026-09-11 (#1026)

The hash index is PER GRACE PARTITION, and that is what makes it reclaimable.

A grace build evicts one partition at a time: it writes that partition's
batches to disk and nils their `buildBatches` slots, which frees the COLUMN
data. Before this file the index was one global structure — one hash table,
one arena of build refs, one chain — so the entries belonging to an evicted
partition stayed resident and stayed charged. Measured on a 2,000-row build
at a 1 MiB budget with all 64 partitions evicted and no build column data
left in memory at all: `used = 106,320`, of which 98,304 (92%) was index —
65,536 of hash table, 28,672 of arena and chain, 4,096 of bloom (#823). At
20,000 rows the same shape held 696,320 gross index. The join's floor was
therefore proportional to TOTAL build rows however much of the build it had
spilled, which is the opposite of what a grace join promises.

# Why per-partition TABLES and not just per-partition arenas

Per-partition arenas alone are the contained change — a key's partition is a
function of the KEY, so a table can index `arena[spillPartition(key)]` with
the same int32 it already stores — and they reclaim the arena and chain,
which is 27%. They leave the hash TABLE, which is 62%. That is a fix bounded
by a model the same commit knows is incomplete, and it leaves #823's own
headline shape where it was, so it was refused (rule 11) and the whole fix
deferred to this arc.

With one table PER PARTITION the arenas follow for free, because a
partition's table only ever addresses its own arena: partition p's table
holds only keys k with spillPartition(k) == p, and p's arena holds only the
rows those keys chain through. Eviction frees the table, the arena, the
chain and the matched bitmap together with the columns. What must survive is
the BLOOM FILTER, and it is 4%. It is built at the END of the build over the
keys the index then holds, which is exactly the key set the IN-MEMORY probe
asks about — the partition router has already sent the rest to disk. It is
NOT a filter over the whole build side, and that is why a build that spilled
does not publish it upstream of the router (BloomPushdownOp, join.go).

# The floor is therefore DERIVED, not measured

After evicting every partition a join holds: the bloom filter, and the
`joinIndexPart` headers themselves (one struct per partition, whose slices
and table pointers are all nil). Both are computed from the structure sizes
rather than observed, which is what `TestEvictingEveryPartitionFreesTheIndex`
asserts. Nothing about that number depends on how many rows the build saw.

# The cost, and where it is NOT paid

The probe's inner loop gains a partition selection per row —
`spillPartition(key) & partMask`, one multiply, one shift, two ands — and 64
independently sized tables replace one. A build that cannot evict does not
pay for a reclaim it can never use: `partMask` is 0 for every non-partitioned
build (the flat path, the spilled-partition replay, the key-only builds), so
those keep ONE table, ONE arena and ONE chain, and the selection folds to
part 0. The two paths share one body rather than being written twice, because
a probe that indexed a different table from the one the build wrote is a
silently wrong answer and the way to make that impossible is to have one
routing function — `spillPartition`, the same one the build's scatter and the
probe's spilled-row routing already use.
