# Dag dispatch capacity semaphore

Source: internal/coordinator/dag_dispatch.go — dispatchSlots, moved 2026-09-11 (#1026)

Cap how many stages can be dispatching concurrently to keep coord-side
result-collection state (NATS subscriptions, in-flight task batches,
per-stage buffers) bounded. Without this, every "ready" stage in a
wave dispatches simultaneously — for Q18 SF10 (17 stages, multiple
fan-outs ready at once) the resulting coord+worker total RSS routinely
overshoots the host's physical memory and the OS OOM-kills a process.

2 * workerCount keeps workers saturated (each worker has typically
max_concurrent>=2 tasks) while bounding the number of in-flight stage
pipelines coord must track to a small multiple of the cluster size.
The semaphore is acquired AFTER all upstream dependencies are
satisfied, so it can never deadlock waiting on a producer that itself
can't acquire a slot.
Source the slot count from the actual cluster capacity (sum of each
worker's auto-tuned max_concurrent reported in heartbeats) when
available. Workers downscale max_concurrent under memory pressure
(auto-detected memory budget logic in cmd/wadjet), so this gives the
dispatcher a memory-aware backpressure signal: if every worker has
shrunk to max_concurrent=2 because the box is tight, dispatch only
queues that many stages at a time instead of stampeding 8+ in a wave.
Falls back to 2 * workerCount when no worker has reported
MaxConcurrent yet (cluster startup, legacy workers).
