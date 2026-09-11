# Broadcast join fusion budget

Source: internal/planner/physical/join_fusion.go — fuseJoinStages, moved 2026-09-11 (#1026)

```go
// fuseJoinStages absorbs broadcast join stages into their downstream consumer
// join stage when the broadcast join's output feeds directly as the probe or
// build side of another join. This avoids materializing the intermediate result
// to S3 and re-reading it — the worker chains probes batch-by-batch instead.
//
// Example: join-A (lineitem ⨝ part) → join-B (broadcast_join with nation)
// becomes: join-A with FusedJoins=[nation join spec], join-B removed.
//
// # FUSION DEPTH AND BYTE BUDGET
//
// Multi-level fusion is allowed when the cumulative build-side bytes on the
// consumer (its primary build, if broadcast_join, plus every existing fused
// build, plus the candidate's primary build, plus the candidate's existing
// fused builds) stays below maxFusedBuildBytes. The check exists because
// under probe-split each shard task loads ALL caches the fused stage
// references — a chain of M fused builds means each shard reads M cache
// files, so the cluster-wide S3/store amplification is workerCount × M ×
// per-build-bytes. Capping the cumulative byte total caps the amplification
// regardless of M.
//
// Pre-2026-04-30 the implementation hard-capped at depth 1 (a single fused
// entry per consumer) by skipping any candidate that already had FusedJoins.
// That excluded star-schema queries with multiple tiny dimension tables
// (nation, region, supplier) where the cumulative build size is well under
// the budget but the dispatch overhead of N separate broadcast stages is the
// dominant cost. The new cumulative-bytes check fails closed: when
// EstimatedBytes is unknown for any participant we treat it as conservative
// and do NOT extend the fusion past depth 1.
```
