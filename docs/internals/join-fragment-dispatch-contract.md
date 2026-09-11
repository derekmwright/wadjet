# Join fragment dispatch contract

Source: internal/coordinator/dag_compute.go — canMigrateJoin, moved 2026-09-11 (#1026)

Fused join + downstream exchange-sender: emit a fragment task
pipeline that runs the entire chain in one worker call without
writing the join output to S3 only to immediately re-read+hash
it in a separate shuffle task. Worker dispatches via
executeFragment (executor_fragment.go); the legacy single-op
task fields above still populate so the wire format stays
stable for any downstream consumer that hasn't yet learned
about Operators[].

Hash-join / broadcast_join migration: every join stage routes
through the multi-op fragment runner. Three terminal sink shapes:
  - downstream Repartition Exchange → OpExchangeSender
    (the fuseJoinShuffle case)
  - downstream is anything else → OpUnpartitionedSink
  - GroupByCols on a join stage isn't an emitted shape (joins
    and aggregates are separate stages in the planner today),
    so we don't model it.

SortKeys after fuseSortIntoPredecessor: legacy applyPostSort ran
in-process; the fragment runner uses OpSort as a multi-breaker
chain element. probeSplit's task input slicing flows through
taskInputs unchanged; the fragment source op is a uniform
sourceForAliasWithProjection (auto-detects parquet vs WSHF).
probeSplit migration depends on the fragment runner's spilled-
partition flush phase (worker/executor_fragment.go); without it,
Q05 SF100 build-cache chains return 0 rows when the primary's
only build partition spills under cumulative chain pressure.
