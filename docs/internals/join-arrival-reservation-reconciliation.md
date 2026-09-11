# Join arrival reservation reconciliation

Source: internal/engine/exec/join_partition_arrival.go — HashJoin.absorbArrivalBatch, moved 2026-09-11 (#1026)

absorbArrivalBatch charges one arrival batch to the shared pool, scatters it
into its grace partitions and indexes it. Caller holds h.mu.

Like the legacy path it Reserves and falls back to spilling on over-budget;
unlike the legacy path the spill is incremental - pick one partition and
evict it instead of repartitioning the whole flat state.

#598 is the third fallback, after Reserve and after eviction: a batch whose
own columns do not fit the pool is SPLIT and absorbed in pieces. Without it
the build's FIRST batch had nowhere to go - largestInMemoryPartition returns
-1 when nothing has been stored yet, so spillUntilCanReserve frees 0 and the
retry fails for exactly the reason the first attempt did, and the query died
with `used=0, requested=7813532` while the same rows delivered in smaller
batches built fine. The trigger is exactly hashBuildBytes(b) > what the pool
can give, which the parquet ROW GROUP decides: the scan hands the build one
batch per row group, so a fat row group is a fat arrival batch.

Splitting and not overcommitting is deliberate. The filing's other direction
- reserve past the budget for the first batch - would be another unceilinged
ForceReserve producer on a query tracker (ADR-0006's 2026-09-03 census
enumerates the ones that exist, two of them in this file's own operator), and
the overcommitted bytes would join the floor every DOWNSTREAM operator's
Reserve is measured against. Splitting adds no new overcommit.

# The reservation is RECONCILED to what the build kept

One arrival batch is charged ONCE, as hashBuildBytes(b). What happens to its
rows afterwards is one of two things: they are appended to an in-memory
partition (retained, and released when that partition is evicted) or written
straight to an already-spilled partition (retained by nobody). So the release
owed at the end of this call is `cost - retained`, and `retained` is the sum
of what partitionAndIndexBatch actually put into partMemory.

It used to be neither of those. The spilled branch released
hashBuildBytes(compactBatchForRows(b, rows)) PER PARTITION — a figure
computed from a freshly minted batch, which pays the per-column fixed
overhead (null-bitmap words, a bytes column's len+1 offsets, capacity
rounding) once per partition against an arrival batch that paid it once.
Measured on this arc's fixture: an arrival batch of 256 rows charged 24,932
bytes released 30,372 across its 63 partitions — 1.22x, over-releasing 5,440
bytes EVERY BATCH. The in-memory branch was wrong in the other direction: it
charges partMemory the tight per-row data bytes, which is less than the
arrival share, so a build that never spilled leaked ~1,000 bytes per batch
upward. Which way a build drifted therefore followed how many partitions had
spilled by the time each batch arrived, i.e. pressure and timing — the
moving floor of #789 — and at 100,000 build rows `used` reached MINUS 867,561
against a 1 MiB budget, a ledger that under-reports by 1.67 MB and admits the
next operator against room that does not exist.
