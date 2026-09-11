# Worker partitioned shuffle sink design

Source: internal/worker/partitioned_shuffle_sink.go — type partitionedShuffleSink struct {, moved 2026-09-11 (#1026)

partitionedShuffleSink is an exec.Sink that hash-partitions incoming batches
into N output .wshf files, one per partition. Each partition's writer flushes
its accumulated rows once a per-partition buffer threshold is reached, so
peak memory is bounded by (N partitions × 2 × flush threshold — accumulator
plus its ping-pong spare), independent of the total input size.

This is the build-side and probe-side output sink for the shuffle execution
path. The N output files are uploaded by the executor to S3 under a stable
per-partition prefix, and downstream join tasks read all files at their
assigned partition prefix via partitionShardSource.

Concurrency model: the lock is PER PARTITION, not sink-wide. Hashing and
row scatter are per-call private state (pooled scratch), so concurrent
Consume calls — morsel-parallel fragment consumers, or exec.Pipeline's
parallel workers — contend only when appending to the SAME partition. The
previous sink-wide mutex serialized the entire consume (hash + scatter +
append + flush) across k consumers, which is what kept join/probe
fragments +12-27% slower under morsel-auto at SF100 (2026-07-07
default-flip gate) — the sink was the fragment's dominant cost and only
one consumer could be inside it.

Large consume slices additionally take the DIRECT-CHUNK path (see
writeDirectChunk and docs/design/sink-direct-chunk.md): a per-partition
slice whose estimated bytes already exceed the flush threshold skips the
accumulator and encodes straight from the source batch into the partition
stream OUTSIDE pw.mu, guarded by pw.flushing. The 2026-08-12 SF100 block
profile showed the per-partition locks themselves as 64.3% of all worker
mutex block time — nearly all of it concurrent 64K-row morsel consumes
holding a lock for the accumulator copy plus encode. With the direct path
the lock covers only counter updates for those slices, and the row data is
copied once (source→wire) instead of twice (source→accumulator→wire).
WADJET_SINK_DIRECT_CHUNK=0 is the kill switch.
