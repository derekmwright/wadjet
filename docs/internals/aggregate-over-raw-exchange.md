# Aggregate over raw exchange

Source: internal/planner/physical/agg_over_exchange.go — rewireAggOverRawExchange, moved 2026-09-11 (#1026)

```go
// rewireAggOverRawExchange drops a fused scan-aggregate leg whose input is a
// full re-scan of a table some raw sibling exchange already shuffles on the
// aggregate's exact group keys, and feeds the aggregate's final directly
// from the sibling's partition files.
//
// The Q18 shape that motivates it: join-8's build side shuffles RAW lineitem
// (l_orderkey, l_quantity) by Hash(l_orderkey); the HAVING subquery scans
// lineitem AGAIN as a fused scan-agg (partial SUM(l_quantity) GROUP BY
// l_orderkey) whose partials a final_aggregate merges. Because the raw
// exchange hash-partitions on the group key, each of its partitions holds
// every row of the keys it covers — so a RAW (non-merge) aggregate over one
// partition produces exact groups with no cross-partition merge. The second
// full table scan (plus its partial-agg shuffle) buys nothing.
//
// Rewrite: the final_aggregate consumes the raw exchange's partitions, its
// AggSpecs switch from merge form to the fused scan's raw specs
// (Stage.RawInputAggregate makes the dispatcher build the fragment with
// MergeMode=false), and the fused scan + its exchange are dropped. The
// final's output distribution mirrors the raw exchange, which typically
// turns its downstream re-shuffle into an identity exchange that
// elideCoPartitionedExchanges removes (the caller re-runs it).
//
// HAVING is unaffected: it rides Stage.FilterExprs → PostFilterExprs and
// runs after the (now raw) aggregate exactly as it ran after the merge.
//
// Runs BEFORE fuseScanAggregateShuffle, so the fused scan-agg leg is still
// scan(FusedAgg*) → exchange-repartition → final_aggregate.
//
// Conservative eligibility (v1):
//   - F is a grouped final_aggregate: single dep, no SortKeys/Limit, no
//     merge-tree grouping, not GroupByAll.
//   - F's dep is an exchange-repartition B whose sole consumer is F and
//     whose sole dep is a scan-aggregate (FusedAggGroupBy set) of table T
//     with no filters and no security barrier.
//   - A is another exchange-repartition over a RAW scan of T (no filters,
//     no fused agg, no barrier), with Exchange.Keys exactly equal to F's
//     GroupByCols — exactness of per-partition groups needs the group keys
//     to determine the partition, and the exact-name match keeps
//     OutputDistribution's mirror rule and Satisfies() consistent.
//   - Aggregate functions are SUM/COUNT/MIN/MAX on bare columns (no AVG
//     synthetics, no derived input expressions), and every group key and
//     aggregate input is in A's scan payload.
```
