# Subsumed scan exchanges

Source: internal/planner/physical/exchange_subsume.go — dedupeSubsumedScanExchanges, moved 2026-09-11 (#1026)

```go
// dedupeSubsumedScanExchanges drops an exchange-repartition whose payload is
// a filtered subset of a sibling exchange over the same base table, rewiring
// its consumer to the subsuming exchange.
//
// The Q21 shape that motivates it: l2 scans lineitem RAW and shuffles
// (l_orderkey, l_suppkey) by l_orderkey for the EXISTS build; l3 scans the
// SAME table with σ(l_receiptdate > l_commitdate), same payload columns,
// same keys and partition count, for the NOT-EXISTS build. At SF100 the l3
// leg pays a full scan-filter materialization plus a 300 M-row shuffle for
// rows that are, exactly, a subset of what l2 already shipped.
//
// Because the filter's columns are deliberately NOT part of the shipped
// payload (shuffle projection), the residual cannot run on the subsuming
// exchange's rows as-is. Instead the raw exchange APPENDS the filter as a
// computed boolean column (~1 byte/row on the raw payload — vs a second
// full scan+shuffle), and the dropped exchange's consumer filters its build
// input on that flag at read time (Stage.BuildFilterExprs). Filtering build
// rows at read is semantically identical to filtering them at the dropped
// scan.
//
// Conservative eligibility (v1):
//   - A and B are exchange-repartition stages whose sole deps are SCAN
//     stages of the same table; A's scan is filter-free, B's carries
//     FilterExprs.
//   - Same Exchange.Keys (exact) and Count.
//   - B has exactly ONE consumer, which uses B as its build side
//     (RightDepStage) and carries no pre-existing BuildFilterExprs.
//   - Every payload column of B's scan is either in A's scan columns or
//     referenced only by the residual filter; and the consumer's build-side
//     references (join RightKeys + JoinFilter identifiers) are all present
//     in A's payload.
```
