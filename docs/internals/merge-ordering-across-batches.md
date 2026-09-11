# Merge ordering across batches

Source: internal/coordinator/coordinator.go — sortBatches, moved 2026-09-11 (#1026)

sortBatches performs a simple in-memory sort of batches by the given order keys.
Used for merging probe-split partial results (typically <100K rows).

It COALESCES first, and that is the whole of #480's round-1 review. A
selection vector reorders rows WITHIN one batch and cannot express an order
that crosses two, so this function used to return outright on a multi-batch
input — with the comment "reAggregatePartials produces a single batch",
true of the re-aggregating path and of no other. Every distributed result
whose ORDER BY is applied ONLY at this merge — no sort stage in the plan,
no aggregate and no DISTINCT to collapse the input — therefore came back in
whatever order the tasks happened to finish in, silently. The shape
measured to do that is a KEYLESS join whose probe is split across tasks —
which is how the arc that made those plans runnable exposed it, and it is
the shape the gates carry (`TestF1AKeylessJoinAsksForWhatItNeeds`,
`TestMergeOrdersAcrossBatches`). Which other plans reach this function with
more than one batch has not been enumerated; do not read the sentence above
as a claim about any particular join or scan, only about what this function
must do when it is handed more than one.

Returns the batch slice to use, which is a NEW single-batch slice whenever
it had to coalesce.
