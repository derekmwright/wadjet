# Singleton sort fusion

Source: internal/planner/physical/native_dag_rewrite.go — projectionCoversSortKeys / fuseSortIntoPredecessor, moved 2026-09-11 (#1026)

```go
// fuseSortIntoPredecessor folds a Singleton sort stage into the compute
// stage that produces its sole input, so the predecessor applies sort
// in-process instead of writing intermediate output and letting a
// separate sort task pick it up. Same savings class as
// collapseRedundantFinalMergeSort but for the aggregate/join→sort edge:
// one fewer stage, one fewer JetStream round-trip, one fewer
// KV/S3 materialization per query.
//
// Predecessor eligibility:
//   - Singleton distribution (sort output stays Singleton regardless of
//     whether the predecessor was Hash-partitioned or Singleton; this
//     pass handles only the Singleton predecessor case to preserve
//     partition count semantics upstream).
//   - Doesn't already carry SortKeys (we'd clobber them).
//   - Is a compute stage type that the worker's Stage dispatcher knows
//     how to post-sort: hash_join, broadcast_join, aggregate,
//     final_aggregate. Other types (scan, merge_sort, window) are left
//     alone until the worker dispatcher supports post-sort there too.
//
// Sort eligibility:
//   - Type == "sort" and Singleton distribution.
//   - Exactly one dependency.
//
// Correctness: the sort stage carried SortKeys + Limit; both move onto
// the predecessor, and the fold is valid only while the predecessor still
// runs as ONE task. Nothing in the plan guarantees that — a Singleton
// broadcast_join is re-fanned-out at dispatch by broadcastJoinProbeSplit,
// which slices the probe files across workers — so each task would sort
// and limit its own slice and the outputs would be concatenated: the
// wrong top-N, not merely the wrong order (#390).
//
// What makes the fold safe in the shape it was written for is that the
// sort has NO DEPENDENTS at this point. EnsureDistribution has not run
// yet, so the terminal gather does not exist; a dependent-free sort is
// the one that will BECOME the gather's input, and dispatchGatherStage
// re-imposes the fused SortKeys/Limit as a merge-sort gather fragment
// (the #288 ordered-gather path). A sort that already has a dependent —
// an ORDER BY + LIMIT inside a derived table or CTE feeding a join or an
// aggregate — reaches a consumer that does no such thing: every other
// consumer of a stage's output (a downstream stage's inputs, an
// exchange-repartition or replicate source, a coordinator-read scalar)
// reads a flat concatenation of the producing tasks' files. So this pass
// refuses to fold into a predecessor whose sort someone else is reading,
// and the standalone single-task sort stage stays in the plan to do the
// global job.
//
// Downstream references to a dropped sort are rewritten to the
// predecessor.
// projectionCoversSortKeys reports whether every sort key names one of the
// projection's outputs. An OpProject narrows the batch to exactly its
// projections, so a key it does not emit cannot be sorted on downstream of it.
```
