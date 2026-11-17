# Whole input aggregates

Source: internal/coordinator/dagplan/agg_whole_input.go — aggNeedsWholeInput, moved 2026-09-11 (#1026)

```go
// aggNeedsWholeInput reports whether an aggregate's answer for one group
// cannot be assembled from per-task partial answers for that group.
//
// The distributed shape a plain aggregate takes is partial-then-merge: each
// task aggregates its slice and the final stage re-aggregates the per-task
// values. That is only sound when the function's own output is a valid
// input to itself — SUM of SUMs, MIN of MINs. For everything below it is
// not, and the failure is silent because the answer is still a plausible
// value of the right type:
//
//	median      a median of medians is not a median
//	percentile  same, at every fraction
//	mode        a mode of modes is the most common of K values, not of N
//	min_by      the per-task winners are the right shape, and which one
//	            comes out is decided by the tie-break between tasks
//	string_agg  the weakest case of the five, and measured rather than
//	            assumed: concatenating concatenations preserves both the
//	            values and the separator, and perturbs only their ORDER,
//	            which SQL does not define without an ORDER BY. It is gated
//	            with the rest because it is the same shape — an aggregate
//	            re-run over its own output — and because the moment
//	            STRING_AGG grows an ORDER BY, the order becomes the answer.
//
// None of these has a bounded summary that merges either — the state IS the
// multiset — so unlike the variance family (which ships (count, mean, M2)
// through decomposeVar and is finished on the final stage) there is nothing
// smaller to ship than the rows themselves.
//
// MODE is a similar caution: mode-of-modes is wrong in general (the most
// common of K per-task winners is not the most common of N values), but it
// happens to coincide whenever one value dominates every partial, so a
// fixture can agree with it by luck. The gate is what makes it right, and
// the planner test is what proves the gate engages.
//
// The trade taken here is the one COUNT(DISTINCT) already takes (#291) and
// the union work took for distinct: dispatch as a RawInputAggregate final
// over raw rows, so every row of a group is aggregated exactly once, in one
// place. Cost: no partial-aggregate reduction before the exchange, so the
// full input crosses it, and an UNGROUPED aggregate of this family collapses
// to a single task (Singleton) — its peak memory is the whole column. A
// GROUPED one still runs on every worker; the distribution pass clusters raw
// input on the group keys, so tasks hold disjoint groups and each group's
// rows land together. That bounds it at the largest single group.
//
// Correctness over scalability is the right way round here: the alternative
// on offer is a wrong number that looks right.
```

## approx_distinct (#1288)

`approx_distinct` joined the gated list in v0.25.6. Its accumulator is an
exact distinct set (`exec.distinctSet`, the one `COUNT(DISTINCT)` uses), not a
sketch, and the partial a task writes is that set's `count()` — an INT64, the
set itself never leaves the task. Split into partial-then-merge, the merge
stage re-ran `approx_distinct` over the per-task counts and answered the
number of distinct COUNTS: `SELECT approx_distinct(c_i32) FROM typemx WHERE
id < 20` answered 2 on every DAG arm (two tasks holding unequal shares of the
20 rows) and 1 over the whole table, where the single-process engine answers
20 and 4828. Gated, every arm runs the same exact set over the same rows and
answers the same value. The worker also refuses a merge-mode `approx_distinct`
outright (`buildFragmentHashAggregate`), since there is no correct merge of
counts. Pins: `dagplan.TestAggWholeInputGate_RawInputAggregate`,
`worker.TestBuildFragmentHashAggregateRefusesApproxDistinctMerge`, and the
five-arm `coordinator.TestApproxDistinctFiveArmsAgree`.
