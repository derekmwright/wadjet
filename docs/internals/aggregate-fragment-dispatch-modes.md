# Aggregate fragment dispatch modes

Source: internal/coordinator/dag_compute.go — canMigrateAggregate, moved 2026-09-11 (#1026)

Final/merge aggregate migration: dispatch via the fragment path
when the stage has no fused sort/limit. The fragment runs:
  [OpShuffleSource, OpHashAggregate(MergeMode), OpFilter? (HAVING),
   OpUnpartitionedSink]
Output shape (one .wshf per task) matches the legacy
executeStageAggregate path — same downstream consumers (gather,
further merge stages) read it identically. No file-count
amplification because the aggregate output is one row per group
per task and the sink is unpartitioned.

Eligibility: skipped when SortKeys/Limit are set (post-aggregate
sort needs an OpSort breaker not yet in the fragment runner) or
when ReplySubject is set (gather output — handled by the legacy
gather-task path today; gather fusion is a follow-up).
Aggregate-fragment migration. SortKeys / Limit on a final_aggregate
stage come from fuseSortIntoPredecessor folding a downstream
Singleton sort into this aggregate; the multi-breaker fragment
runner handles the resulting `[ShuffleSource, HashAggregate,
Filter?(HAVING), Sort?, Sink]` chain in one task.

"aggregate" (partial, MergeMode=false) is a standalone aggregate
stage consuming non-scan upstream input (e.g. join output);
"merge_aggregate" / "final_aggregate" (MergeMode=true) re-aggregate
partial outputs. buildAggregateFragment derives the mode from
stage.Type.
