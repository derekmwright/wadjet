# Eager stage completion test seam

Source: internal/coordinator/execute_stage_dag.go — eagerDispatchStageCompletionHook, moved 2026-09-11 (#1026)

eagerDispatchStageCompletionHook, when non-nil, runs synchronously right
before a stage's `done` channel closes, with the coordinator and IDs
needed to inspect that stage's own eager feed. Test-only seam: nil in
production (zero cost, one nil check), so it changes no behavior there.

Eager consumer dispatch (docs/design/eager-consumer-dispatch.md §3.3)
races a producer's `done` (full completion) against its `f.dispatched`
(task layout fixed) — real work, so `dispatched` almost always wins. At
small scale (SF001, a handful of milliseconds of real work per task)
under CPU contention, the consumer's own per-dependency loop can be
scheduled late enough that BOTH channels are already closed by the time
it checks: Go's select then picks between them at random, so the eager
path can lose even though nothing was actually wrong. A test asserting
engagement needs the race decided every time, not most times.

The hook lets a test hold `done` open past that window — but only for
stages that are actually eager-feed producers, identified by their own
f.dispatched already being closed (runShuffleSide and the A3 compute
path always call feed.dispatch before returning, whether or not any
consumer ever registers as eager, so this reads true for exactly the
stage types that race a consumer and false for everything else, e.g.
scans and non-shuffle joins). Delaying only those, instead of every
stage in the DAG uniformly, avoids also delaying the consumer's own walk
through its OTHER (non-eager) dependencies, which would just push the
race back by the same amount instead of resolving it.
Package var so tests can pin it. See eager_dispatch_e2e_test.go.
