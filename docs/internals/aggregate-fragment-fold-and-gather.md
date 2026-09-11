# Aggregate fragment fold and gather

Source: internal/coordinator/dag_fragments.go — buildAggregateFragment, moved 2026-09-11 (#1026)

buildAggregateFragment translates a final_aggregate / merge_aggregate
stage's task into a fragment Operators[] pipeline:

	[OpShuffleSource, OpHashAggregate(MergeMode=true), OpFilter?(HAVING),
	 OpSort?, OpUnpartitionedSink | OpGatherSink]

FoldAvg is set only for "final_aggregate" — intermediate "merge_aggregate"
tasks must keep __avg_sum#X / __avg_count#X synthetics intact for the
downstream final to fold (see executor_stage.go for the same gate on the
legacy path). Mirror behavior is byte-equivalent to the legacy
executeStageAggregate path including the post-aggregate sort folded in
by fuseSortIntoPredecessor.

OpSort is appended only when the stage carries SortKeys (the planner's
fuseSortIntoPredecessor pass set them by absorbing a downstream Singleton
Sort). Stage.Limit propagates through as SortLimit so the Sort operator's
post-Finalize Truncate fires for top-N. The multi-breaker runner chains
HashAggregate's drain into Sort's consume in-process — no S3 hop between
the two breakers.

When gatherReplySubject is non-empty, the terminal sink is OpGatherSink
streaming directly to the coordinator's NATS reply subscription instead
of an unpartitioned .wshf upload — fuses the downstream gather stage
into this fragment, eliminating one S3 PUT/GET hop and one coord round-
trip. Each task publishes its own terminal marker; coord pre-subscribes
with expectedTerminals = numTasks.
inputRowBound is the exact upper bound on the rows this task will read
(aggregateInputRowBound), or 0 when no exact bound exists. It rides the
OpHashAggregate spec and decides the group-index layout on the worker.
