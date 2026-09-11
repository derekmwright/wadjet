# Scan shuffle fusion

Source: internal/planner/physical/fuse_scan_shuffle.go — fuseScanShuffle, moved 2026-09-11 (#1026)

```go
// fuseScanShuffle absorbs StageExchangeRepartition stages into their upstream
// StageScan when safe, eliminating the round-trip of writing+re-reading an
// unpartitioned WSHF between scan and shuffle.
//
// Today's flow:  scan → exchange-repartition → consumer
//
//	scan emits unpartitioned WSHF; coord dispatches a separate shuffle task
//	that reads it and writes partitioned WSHF; consumer reads the partitions.
//
// After fusion: scan(with shuffle metadata) → consumer
//
//	scan task hash-partitions its filtered output directly
//	(runStageScanPartitionedStreaming + partitionedShuffleSink in the
//	worker) — saves one full write+read of the scan's output (2026-08-02
//	SF100 accounting: Q03 10.0 GB, Q21 6.8 GB, Q13 2.4 GB duplicated per
//	cold run on scan legs alone), one S3 PUT+GET cycle, and one NATS
//	round-trip per fused pair.
//
// Safety conditions:
//  1. Exchange has exactly one dependency (the scan).
//  2. Scan has exactly one dependent (the exchange) — no other stage reads
//     the scan's unpartitioned output.
//  3. Scan is a plain StageScan (not scan-aggregate, which has its own
//     fan-out semantics via dispatchScanAggregateStage; no prior Exchange),
//     and it is DISPATCHED-shape (pushed filters / projections / security
//     barrier / DF emits) — pass-through scans materialize nothing today,
//     so there is no round-trip to save and fusing would misroute them.
//  4. Every consumer of the exchange PARTITION-BINDS its input
//     (buildTaskInputsForStage → partitionFilesForWorker): hash_join,
//     sort_merge_join, or a grouped final_aggregate. Flattening consumers
//     — chained repartitions (flattenStageFiles), replicate/broadcast
//     caches (materializeReplicate), gathers, broadcast_join probe-split —
//     ingest scanTasks×numPartitions files where the unfused path handed
//     them scanTasks, the amplification behind the 2026-05 Q05 SF10
//     7m4s / Q07 +85% regressions that kept this pass disabled.
//  5. The exchange carries no computed-column machinery — the fragment
//     pipeline dispatchScanFilterStage emits (scan→filter→project→
//     column-prune→exchange-sender) has no ComputedCols evaluation ops.
//
// The historical file-count gate (skip when len(ScanFiles) > workerCount)
// is gone: both scan fan-out and shuffle fan-out are capacity-bound via
// scanFanOutTaskCount now, so for partition-binding consumers the fused
// layout produces the SAME per-partition file count as the unfused
// scan→shuffle two-step (T tasks × P partitions either way). The
// consolidation the legacy exchange provided only ever mattered to the
// flattening consumers condition 4 excludes.
//
// Worker requirements: dispatchScanFilterStage translates scan.Exchange
// into the fragment fuseShuffle path (OpExchangeSender terminal); the
// fused StageOutput carries per-partition rows/bytes (PartitionAccounting)
// so downstream skew-split stays live.
//
// Run AFTER pruneScanOutputColumns (which needs the exchange present to
// compute the scan's OutputColumns) and after every stage-rewiring pass,
// so the consumer set condition 4 inspects is final.
```
