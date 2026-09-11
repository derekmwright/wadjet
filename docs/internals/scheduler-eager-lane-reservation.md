# Scheduler eager lane reservation

Source: internal/coordinator/scheduler.go — pickWorkerFor, moved 2026-09-11 (#1026)

pickWorkerFor selects a worker for one task. Memory-aware bin-packing
applies only when the task carries an estimate AND heartbeat pool stats
exist — otherwise placement is the pre-existing round-robin. Estimates
of 0 deliberately round-robin: with no per-task charge, "most free"
would be static between heartbeats and pile every task of a fan-out
onto one worker.

batchAssigned carries this publish call's placements so far: bin-packed
tasks are restricted to the workers with the fewest same-batch tasks
before "most free pool" ranks them. Without that restriction the same
staleness pathology hits estimated tasks — heartbeats lag 10s and the
per-task in-flight charge is small, so an entire fan-out lands on
whichever worker last reported the most free pool.

Eager consumer tasks take their own placement path first: they can
block on producer manifests for the whole producer stage, so stacking
them on one worker starves that worker's producer lanes (targeted gRPC
dispatch has no work stealing).

The returned method ("eager" | "affine" | "local" | "binpack" | "rr")
feeds the placement attr on the "published tasks" line. The affinity
and locality tiers' relative order is controlled by
affinityBeforeLocality; see its doc for the rationale.
