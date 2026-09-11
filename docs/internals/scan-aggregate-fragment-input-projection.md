# Scan aggregate fragment input projection

Source: internal/coordinator/dag_fragments.go — buildScanAggregateFragment, moved 2026-09-11 (#1026)

buildScanAggregateFragment translates a fused scan + partial-aggregate
stage's task into a fragment Operators[] pipeline:

	[OpScan, OpFilter?(scan-pushed WHERE), OpHashAggregate(partial, BuildProject), terminalSink]

terminalSink is the caller-supplied sink — OpExchangeSender for the
fuseScanAggregateShuffle case (each task hash-partitions its K aggregate
rows by group key directly, skipping the standalone exchange-repartition
stage), OpUnpartitionedSink otherwise (one .wshf per task; downstream
Singleton final_aggregate reads them all). Output shape under the
unpartitioned terminal is byte-equivalent to the legacy
executeStageAggregate path.

BuildProject=true asks the worker's buildAggInputProjection to construct a
derived-input Project for AggSpecs whose InputCol references an expression
(e.g. SUM(l_extendedprice * (1 - l_discount))). The worker prepends the
project to the unary chain ahead of HashAggregate's consume phase.

ScanShardIndex/Count propagate single-file row-group sharding for fused
scan-aggregate over a single compacted parquet file (e.g. SF10 lineitem
at 5GB).
