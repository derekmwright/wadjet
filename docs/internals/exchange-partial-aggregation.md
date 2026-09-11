# Exchange partial aggregation

Source: internal/planner/physical/exchange_partial_agg.go — markExchangePartialAgg, moved 2026-09-11 (#1026)

```go
// markExchangePartialAgg marks exchange-repartition stages whose rows can
// be pre-combined in the shuffle sender (exchange partial aggregation —
// the reduce-before-ship mechanism; SF100 Q18's rp leg ships the full
// 600M-row lineitem as raw (l_orderkey, l_quantity) pairs into a grouped
// final_aggregate AND a join probe, ~9.75 GB read twice).
//
// The marked exchange ships name-preserving partials: each payload column
// is either kept as a partial group key or replaced by a self-mergeable
// partial aggregate (SUM/MIN/MAX) written under its own name. Merged rows
// are therefore indistinguishable from raw rows on every non-aggregated
// column, which makes the reduction invisible to any downstream grouping;
// the only consumers that could observe it are ones sensitive to row
// multiplicity or to per-row values of the aggregated columns. Eligibility
// enforces exactly that:
//
//  1. At least one consumer is a grouped final/merge_aggregate, and EVERY
//     aggregate spec any such consumer computes over the payload is a
//     bare-column SUM/MIN/MAX — one function per column across all
//     consumers (COUNT/AVG/DISTINCT/expressions are row-multiplicity- or
//     shape-sensitive and disqualify the exchange).
//  2. Every other consumer is a hash/sort-merge join whose E-side keys
//     avoid aggregated columns, whose filters and chained/fused join keys
//     never reference an aggregated column, and whose dependents are all
//     grouped aggregates that themselves only re-aggregate the covered
//     columns with the same functions (row-count changes pass through a
//     join, so a downstream COUNT would double-count-proof fail).
//  3. Exchange payload is exactly declared (no ComputedCols /
//     ExtraReadCols machinery), and partition keys stay group keys.
//
// Runs after every stage-rewiring pass (fusion, elision, agg-over-
// exchange) so the consumer set is final. Kill switch
// WADJET_EXCHANGE_PARTIAL_AGG=0.
```
