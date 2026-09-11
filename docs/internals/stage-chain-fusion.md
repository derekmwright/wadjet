# Stage chain fusion

Source: internal/planner/physical/fuse_stage_chains.go — fuseStageChains, moved 2026-09-11 (#1026)

```go
// fuseStageChains absorbs 1:1 same-distribution downstream joins into their
// upstream hash_join so the pair runs as one worker fragment, eliding the
// per-link materialization (docs/design/stage-chain-fusion.md).
//
// The link P → C fuses when consumer task i of C reads exactly producer
// task i of P's output — i.e. both are HashPartitioned with the same count
// and C consumes P as its probe. The absorbed join becomes a ChainedJoinSpec
// executed after P's primary probe; C's build dep becomes an auxiliary dep
// of P. P keeps its ID, type, keys, and primary build, so dispatch (1:1
// input slicing, skew gate, locality identity) is unchanged from its point
// of view; P adopts C's Distribution (identical kind/count by gate, keys
// kept exact) and estimated output so downstream labels stay valid.
//
// Runs after distributions are final (post EnsureDistribution + elide +
// subsume + agg-over-exchange passes). Iterates to fixpoint so
// join→join→join chains collapse into one fragment.
//
// Conservative eligibility (v1):
//   - P: hash_join, HashPartitioned{N}, no Exchange / SortKeys / Limit /
//     GroupByCols / probe-split / build-cache / merge-tree fields.
//     Existing FusedJoins and ChainedJoins are fine.
//   - C: hash_join or broadcast_join; P's only consumer; consumes P as
//     probe (LeftDepStage) and not also as build; HashPartitioned{N} with
//     P's N; same field restrictions as P. C's FusedJoins ride along as
//     chained broadcast probes (they ran before C's primary, so they stay
//     ordered before it in the chain).
//   - No dep aliasing: C's build deps must not already be deps of P,
//     EXCEPT C's primary build sharing P's primary build (hash-partitioned
//     only) — the semi/anti-over-one-exchange shape (Q21). The duplicate
//     dep entry satisfies the 2+fused+chained arithmetic, chained builds
//     resolve by upstream-ID lookup, and partition slices align because
//     both builds read the same exchange.
```
