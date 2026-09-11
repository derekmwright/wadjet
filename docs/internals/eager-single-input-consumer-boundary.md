# Eager single input consumer boundary

Source: internal/coordinator/eager_feed.go — eagerEligibleConsumer, moved 2026-09-11 (#1026)

eagerEligibleConsumer reports whether stage s may clear dispatch on its
dependency's eager feed instead of the done barrier (memo §3.3, Phase C1
scope: non-join consumers only).

The gate requires:
  - exactly one dependency and no scalar-substitution deps (those keep
    the barrier: their values are extracted from completed outputs);
  - a fragment-migrated single-input stage type: aggregate variants, or
    sort variants with SortKeys (a keyless "sort" would fall to the
    legacy task path, which reads Task.Inputs and cannot feed eagerly);
  - the dependency backs an eager feed: a standalone
    exchange-repartition, or an A3 compute producer
    (eagerFeedableDep);
  - not the gather-fused stage (fusion disables task retry, and retry is
    the fencing recovery path — memo §5);
  - no dynamic-filter participation (provisional outputs carry no
    BuildStats);
  - a task count no larger than workerCount, so with the scheduler's
    eager-spread placement each worker holds at most one manifest-blocked
    task and keeps ≥ MaxConcurrent−1 lanes for producer progress (the
    §3.3 producer-lane reservation, v1 form).

Join edges (56 of the 57 repartition edges in TPC-H plans) are Phase C2:
they need the early skew decision before clearance.
