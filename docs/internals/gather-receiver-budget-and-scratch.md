# Gather receiver budget and scratch

Source: internal/coordinator/gather_receiver.go — subscribeGather, moved 2026-09-11 (#1026)

subscribeGather installs the NATS subscription. Must be called BEFORE
the Gather task is published so the subscriber is present when the
worker emits batches and the terminal marker — raw-subject publishes
are not buffered for late subscribers.

workers may be nil (test code that doesn't care about liveness); when
non-nil, every received gather batch updates LastSeen for the emitting
worker via WorkerRegistry.MarkWorkerSeen.

budget caps the decoded bytes the receiver holds in coordinator heap
(<=0 = uncapped). The receiver was the last big uncharged coordinator
accumulator: a distributed SELECT * or no-LIMIT high-cardinality GROUP
BY landed its entire result in coordinator heap and OOM-killed the
process — taking every in-flight query with it. Past the budget the
receiver degrades gracefully: the remaining payload frames are appended
raw (still WSHF-encoded, not decoded) to a local scratch file, and the
result is replayed lazily disk→wire by gatherReplayStream. Only if the
scratch write itself fails does the query fail cleanly (the process
still must not die for one query's result size).

The caller that successfully subscribes MUST `defer recv.discard()` so
scratch is removed on every path where wait() never claims the result.
