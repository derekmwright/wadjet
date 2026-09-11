# Scan aggregate shuffle fusion

Source: internal/planner/physical/fuse_scan_aggregate_shuffle.go — fuseScanAggregateShuffle, moved 2026-09-11 (#1026)

```go
// fuseScanAggregateShuffle absorbs StageExchangeRepartition stages into
// their upstream scan-aggregate (StageScan with FusedAggGroupBy /
// FusedAggSpecs set) when the exchange's downstream consumer is a
// collapsing pipeline-breaker (final_aggregate / merge_aggregate).
//
// Today's flow:  scan-aggregate → exchange-repartition → final_aggregate
//
//	The scan-aggregate task emits a single unpartitioned WSHF; coord
//	dispatches a separate exchange-repartition task that reads it back
//	and writes hash-partitioned output; final_aggregate reads the
//	partitions.
//
// After fusion:  scan-aggregate(with shuffle metadata) → final_aggregate
//
//	Each scan-aggregate task hash-partitions its K aggregate rows
//	directly via the worker's executeFragment path:
//	[OpScan, OpFilter?, OpHashAggregate(partial), OpExchangeSender].
//	Saves one S3 PUT, one S3 GET, one NATS round-trip per fused pair.
//
// **Why this is safe (no fan-out amplification):** scan tasks run with
// task count = workerCount (or close to it via scanFanOutTaskCount).
// HashAggregate collapses the per-task input to K rows where K = group
// cardinality in the slice. Each task emits K rows hash-partitioned to
// numPartitions output files. Total intermediate file count is
// workerCount × numPartitions — same as the unfused path's
// exchange-repartition output count. Downstream final_aggregate reads
// partition K from workerCount upstream tasks = workerCount files per
// partition (same as unfused). No amplification because the scan task
// count doesn't grow with table size.
//
// Contrast with fuseScanShuffle (still disabled): plain scan tasks run
// with task count = scanFileCount, which can be >> workerCount at SF10
// (lineitem = 600 files vs workerCount=3). Fused output emits
// scanFileCount × numPartitions files; downstream consumers see
// scanFileCount/workerCount × amplification per partition assignment.
// Aggregate-fused scans don't hit this because aggregate is always
// dispatched at workerCount fan-out, regardless of file count.
//
// Contrast with fuseJoinShuffle (also disabled — see 2026-05-06
// project_broadcast_join_exchange_fusion_handoff): join tasks run at
// numPartitions parallelism (e.g. 32 at SF10), and join output retains
// input cardinality (no collapse). So fused join emits
// numPartitions × numPartitions files vs workerCount × numPartitions
// unfused — same amplification mechanism. Aggregates collapse, joins
// don't, so this fusion shape works where the join-tier didn't.
//
// Run AFTER assignStageDistributions so the exchange stages have their
// final Distribution.Keys/Count populated. Setting scan.Exchange directly
// here is durable — no later pass overwrites it.
//
// Safety conditions:
//  1. Exchange has exactly one dependency.
//  2. The dependency is a scan with FusedAggGroupBy or FusedAggSpecs set
//     (i.e. a scan-aggregate stage owned by dispatchScanAggregateStage).
//  3. The scan has exactly one dependent (the exchange we're absorbing).
//  4. The scan doesn't already carry an Exchange.
//  5. At least one stage depends on the exchange.
//  6. Every consumer of the exchange is a collapsing pipeline-breaker
//     (final_aggregate or merge_aggregate). Without this, fusing into a
//     non-collapsing consumer (hash_join, broadcast_join) re-introduces
//     the file-count amplification fuseJoinShuffle hit.
```
