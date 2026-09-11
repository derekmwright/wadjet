# Shared subplan deduplication

Source: internal/planner/physical/shared_subplan_dedup.go — dedupeSharedSubplans, moved 2026-09-11 (#1026)

```go
// dedupeSharedSubplans drops stage subtrees that duplicate a sibling subtree,
// rewiring the duplicate's consumers onto the surviving root and letting the
// orphaned subtree fall out of the plan. Duplicates arise when subquery
// decorrelation clones a leg of the main query (Q11's scalar-subquery leg is
// a stage-for-stage copy of its main leg; Q17's decorrelated AVG leg re-joins
// lineitem⋈part that the main leg already computed).
//
// Equivalence is a structural fingerprint over the stage subtree that
// deliberately EXCLUDES projected columns (Columns/OutputColumns) and scan
// aliases — clones consumed by different outer queries always disagree on
// those (same rationale as cteSubtreeHash's RequiredColumns exclusion). What
// a leg projects is instead handled by the coverage check: the kept leg's
// scans must read a superset of the dropped leg's columns, so the kept output
// resolves every name the rewired consumers reference.
//
// Two match forms:
//
//  1. Exact: fingerprints equal. Any consumer may rewire.
//  2. Semi≡inner: a semi join whose fingerprint-with-JoinType-"inner" matches
//     an inner join (same children, keys, no JoinFilter). The inner output is
//     the semi output with build columns appended and probe rows repeated
//     once per matching build row. No build-uniqueness oracle exists, so
//     rewiring is gated on every consumer being an aggregate that is
//     provably invariant under that duplication: GroupByCols ⊇ the probe
//     join keys (the duplication factor is a function of the join key, hence
//     constant within each group) and every aggregate function is
//     duplication-invariant (avg/min/max). Q17's AVG(l_quantity) GROUP BY
//     l_partkey consumer is exactly this shape.
//
// Runs after distributions are final and BEFORE fuseStageChains: chain
// fusion absorbs consumers into the legs, making clones asymmetric; at this
// position the legs are still stage-for-stage symmetric. The coordinator
// side needs nothing new — stage outputs already support N consumers
// (Q18's rp-7 is read twice) and scratch cleanup is per-query.
```
