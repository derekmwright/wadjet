# Native dag merge tree collapse

Source: internal/planner/physical/native_dag_rewrite.go — collapseMergeTreesForNativeDAG, moved 2026-09-11 (#1026)

```go
// collapseMergeTreesForNativeDAG rewrites multi-level merge_aggregate /
// merge_sort fan-out trees back into single-stage form. The trees are
// emitted by emitMergeAggregateTree / emitMergeSortTree when the upstream
// task count exceeds mergeFanout (16) — valid for the single-pipeline
// executor where intermediate merges run in parallel as inner-pipeline
// operators, but catastrophic for native-DAG execution where each
// intermediate stage becomes an independent coordinator-worker round-trip
// that re-scans the same upstream data.
//
// Shape produced by emitMergeAggregateTree (upstream > 16):
//
//	intermediate-0   final_aggregate  dep=[leafIDs...] MergeGroup=0
//	intermediate-1   final_aggregate  dep=[leafIDs...] MergeGroup=1
//	...
//	intermediate-N   final_aggregate  dep=[leafIDs...] MergeGroup=N-1
//	final            final_aggregate  dep=[intermediate-0, ..., intermediate-N]
//
// Rewrite to:
//
//	final            final_aggregate  dep=[leafIDs...]    (no MergeGroup)
//
// The same pattern applies to merge_sort trees. Rewriting is safe because:
//   - Final stages already compute the full aggregate/sort from all upstream
//     rows; the tree only exists to parallelize intra-worker merging.
//   - Native-DAG dispatches workerCount tasks per stage, so parallelism
//     comes from task-level parallelism, not stage-level fan-out.
//   - executeStageAggregate + executeStageSort run as single-task fan-in
//     today (they consume all upstream inputs and emit merged output);
//     wiring them behind a N-partition dispatch is future work, but even
//     single-task is faster than the 83-stage tree.
```
