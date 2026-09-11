# Sort fragment filter and gather order

Source: internal/coordinator/dag_fragments.go — buildSortFragment, moved 2026-09-11 (#1026)

buildSortFragment translates a sort / merge_sort stage's task into a
fragment Operators[] pipeline:

	[OpShuffleSource, OpSort, OpFilter?, OpProject?, OpUnpartitionedSink | OpGatherSink]

The filter and the projection run ABOVE the sort, which is what makes them
correct for the shapes that put them there: a WHERE above an `ORDER BY …
LIMIT` inside a CTE or derived table must see the LIMIT's rows, not the
pre-limit ones, and OpSort applies its SortLimit truncation in Finalize —
before any of its output reaches the next operator. Until #656 this builder
was the only one that dropped `t.PostFilterExprs` on the floor, so the
predicate walkStages had attached to the stage was never evaluated and the
query answered as if the WHERE were not there.

Same one-output-file-per-task shape the legacy executeStageSort emitted
when the terminal sink is OpUnpartitionedSink; downstream consumers
(gather, further merges) read it identically. Limit is forwarded as
SortLimit so the sort operator's Truncate fires after Finalize for
top-N optimization.

When gatherReplySubject is non-empty, the terminal sink is OpGatherSink
streaming the (single, ordered) sorted output directly to the
coordinator's NATS reply subscription instead of an unpartitioned .wshf
upload — fuses the downstream gather stage into this fragment,
eliminating one S3 PUT/GET hop and one coord round-trip. Only safe
when the upstream stage is DistSingleton (one task → one ordered
stream); canFuseGather enforces that.
