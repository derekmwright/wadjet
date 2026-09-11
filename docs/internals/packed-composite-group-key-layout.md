# Packed composite group key layout

Source: internal/engine/exec/packed_hash.go — packedKey, moved 2026-09-11 (#1026)

Packed composite group keys (G3 in
docs/benchmarks/high-card-aggregation-gap-2026-08-17.md).

The dual-int GROUP BY path stored the key across THREE per-group SoA
arrays (dualIntKeysA, dualIntKeysB, dualIntNextGroup) while the hash table
held only hash→chain-head. Verifying one candidate key therefore took
three dependent loads into three multi-GB arrays, a chain walk multiplied
that, and every insert probed twice (Get, then Put down the same chain).
ClickBench Q33 (GROUP BY WatchID, ClientIP at ~1:1 group:row) spends 62.8%
of its CPU in that loop.

When every group column is fixed-width int-class and the widths sum to
<= 16 bytes, the whole key fits in one 128-bit cell that lives INSIDE the
hash entry (ClickHouse's keys128 method): one probe, one compare against
data already in the loaded cache line, no chain, no side arrays. The
per-group key SoA survives as a single []packedKey (16 B/group in ONE
off-heap array, down from 20 B across three) because emission, merge, the
drain cursor and the spill run format all index state by group id.

Eligibility is decided once at Init (buildPackedLayout) and covers every
shape the dual-int path covered — two int columns are at most 8+8 bytes —
plus wider ones: (Int64, Int32), three or four narrow ints, etc.
