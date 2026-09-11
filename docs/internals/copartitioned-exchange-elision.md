# Copartitioned exchange elision

Source: internal/planner/physical/elide_copartitioned_exchange.go — elideCoPartitionedExchanges, moved 2026-09-11 (#1026)

```go
// elideCoPartitionedExchanges removes exchange-repartition stages whose
// input is ALREADY hash-partitioned exactly as the exchange would
// partition it — the identity re-shuffle (docs/design/exchange-reuse.md
// §2 B). walkStages emits the two input exchanges of every hash join
// unconditionally, and EnsureDistribution only ever ADDS exchanges, so a
// join whose probe side is itself a co-partitioned join output gets its
// rows re-hashed, re-written, and re-read for nothing: Q18's
// repartition-15 moved 40.13 GB / 600M rows at SF100 to reproduce the
// exact partition layout join-8 had already produced.
//
// Elision rule, conservative by construction:
//   - E is exchange-repartition, single dependency D, same ClusterID;
//   - D.Distribution is HashPartitioned with E's exact partition Count;
//   - per position, D's distribution key equals E's key BY NAME
//     (case-insensitive). Cross-name equivalence through equi-join pairs
//     (o_orderkey ≡ l_orderkey after join-8) is deliberately out of this
//     slice: AssertExchangeConsistency and Distribution.Satisfies compare
//     names, and both TPC-H identity shuffles (Q18 r-15, Q21 r-10/r-14)
//     are exact-name matches. Equivalence classes are lever B2 and land
//     together with the Satisfies() extension or not at all.
//
// Consumers of E are rewired to D (Dependencies, LeftDepStage,
// RightDepStage). Distribution labels stay valid: E's output distribution
// was by definition identical to D's. Partition alignment holds because a
// hash-join task p consumes input partition p and emits only rows whose
// keys hash to p — its output IS partition p.
//
// Kill switch: WADJET_EXCHANGE_ELIDE=0 (mirrors WADJET_SCAN_COL_SANITIZE).
```
