# Scheduler affinity before locality

Source: internal/coordinator/scheduler.go — affinityBeforeLocality, moved 2026-09-11 (#1026)

affinityBeforeLocality gates the tier order in pickWorkerFor between
base-table cache affinity and input locality
(docs/adr/0008-task-placement-policy.md). A task whose base-table files
rendezvous-hash to one worker's NVMe cache carries Task.AffinityWorkerID;
a task whose streaming-exchange hints all point at one connected worker
qualifies for locality. Until 2026-08-22 locality ran first, which was
wrong for probe-split broadcast-join tasks: their InputLocations hints
point only at their (small, replicated) broadcast build's single
producer — never at their base-table probe files, which are never
hinted — so locality placed the whole task on the build producer, off
the cache that actually holds its bytes (the SF100 "straggler tier",
docs/design/scan-affinity.md §Probe-split affinity).

DEFAULT ON: affinity ahead of locality (the 2026-08-22 order). OFF
restores the pre-2026-08-22 order (locality, then affinity).

Both tiers are placement preferences over the same connected/live
worker set under the identical same-batch cap — a fallback placement
just misses the cache or the local mmap, exactly as before either tier
existed — so reordering them cannot change a row set. Registered as a
kill switch anyway: it is enumerated by the optimization-invariance
oracle and lets EC2 bisect this reorder independently of the same-
commit worker-side prefetch change (prefetchCacheSkip).
