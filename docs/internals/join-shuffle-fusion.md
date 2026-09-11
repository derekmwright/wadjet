# Join shuffle fusion

Source: internal/planner/physical/fuse_join_shuffle.go — fuseJoinShuffle, moved 2026-09-11 (#1026)

```go
// fuseJoinShuffle absorbs StageExchangeRepartition stages into their upstream
// hash_join / broadcast_join when safe, eliminating the round-trip of
// writing+re-reading an unpartitioned WSHF between the join and the shuffle.
//
// Today's flow:  join → exchange-repartition → consumer
//
//	join writes unpartitioned WSHF; coord dispatches a separate shuffle task
//	that reads it back and writes partitioned WSHF; the consumer reads the
//	partitions.
//
// After fusion:  join(with shuffle metadata) → consumer
//
//	The join task hash-partitions its probe-side output directly via the
//	worker's executeFragment path: [ShuffleSource probe, HashJoinProbe(s),
//	ExchangeSender]. Saves one S3 PUT, one S3 GET, one NATS round-trip per
//	fused pair.
//
// Run AFTER fuseScanShuffle so that any scan-fused exchanges have already
// been absorbed; the remaining repartition stages are the ones whose upstream
// is a join (or other compute stage). Setting join.Exchange directly here is
// durable — no later pass overwrites it.
//
// Safety conditions mirror fuseScanShuffle:
//  1. Exchange has exactly one dependency.
//  2. The dependency is a hash_join or broadcast_join.
//  3. The join has exactly one dependent (the exchange we're absorbing).
//  4. The join doesn't already carry an Exchange.
//  5. At least one stage depends on the exchange (otherwise no benefit; the
//     dropped exchange would orphan no consumers but the absorption is moot).
```
