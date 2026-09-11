# Skew split probe cover

Source: internal/coordinator/skew_split.go — planSkewSplitTasks, moved 2026-09-11 (#1026)

planSkewSplitTasks decides the skew-aware task layout for a shuffled
hash-join stage. Returns nil (dispatcher keeps the standard layout) when
the stage is ineligible or no group crosses the hot threshold; otherwise
one assignment per task, in group order.

Eligibility:
  - hash_join with hash-partitioned distribution (broadcast_join has
    probe-split; sort-merge-join needs aligned sorted runs — excluded v1)
  - probe-side join semantics only (inner/left/semi/anti). right/full
    emit unmatched BUILD rows, which a replicated build would duplicate
    k times.
  - both primary deps are OutputPartitioned with the same partition count
    and reported PartitionBytes (nil vectors = legacy workers → off).

A group is hot when it crosses the absolute floor (skewSplitMinGroupBytes)
AND carries at least skewSplitMinRatio× the mean group's probe bytes —
heavy-but-uniform stages (every group over the floor, ratio ≈ 1) keep the
standard layout. A hot group splits into k = ceil(probeBytes/skewSplitTargetBytes)
sub-tasks, capped by workerCount and by the group's probe file count
(v1 splits at file granularity; a hot partition written by T shuffle
tasks has up to T files). Each sub-task reads 1/k of the probe files and
the group's FULL build files — a probe row for key k needs the complete
build side for k, which replication preserves. Fused builds are already
replicated to every task by the dispatcher (task.FusedJoins[i].BuildFiles)
and need no handling here.
