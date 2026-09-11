# Shuffle candidate selection

Source: internal/planner/physical/stage_cost.go — PickShuffleCandidate, moved 2026-09-11 (#1026)

```go
// PickShuffleCandidate identifies the largest non-probe scan above
// thresholdBytes as the shuffle candidate — the table that would otherwise be
// broadcast-duplicated as the runtime build side — and returns the join stage
// that connects it to the probe.
//
// The approach deliberately does NOT read BuildTableAlias on the join stage
// because in probe-split mode the planner's logical build/probe assignment is
// inverted at runtime: the planner labels the largest scan as the build (e.g.
// "lineitem"), but probe-split partitions that scan across workers, making the
// second-largest scan (e.g. "orders") the actual broadcast hash table.
// Shuffling orders instead of broadcasting it is the correction.
//
// Algorithm:
//  1. probeAlias = largest scan (matches CanProbeSplit's heuristic).
//  2. candidate = largest non-probe scan above thresholdBytes.
//  3. Walk join stages to find one that directly references the candidate
//     scan (via LeftDepStage or RightDepStage) or via a FusedJoin entry
//     whose BuildTableAlias matches the candidate alias.
//  4. Extract build/probe keys from the matching join or fused-join entry.
//
// Phase 1: returns the single best candidate. Phase 2 (chained shuffles)
// will return all candidates.
```
