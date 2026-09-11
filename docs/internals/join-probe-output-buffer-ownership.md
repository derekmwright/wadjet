# Join probe output buffer ownership

Source: internal/engine/exec/join_emit_reuse.go — probeEmitBuf, moved 2026-09-11 (#1026)

probeEmitBuf is one HashJoinProbe's reusable late-materialization output:
the batch shell, its column slice, the probe/build index arrays the view
columns address through, the composition buffers a view-over-view needs,
and the owned storage of the eagerly-gathered build columns.

# Ownership rule

Everything here was written into the batch the probe returned on its LAST
call and is written over on the next one. That is sound because a consumer
that keeps a batch — or anything pointing into its column storage — past the
call that handed it over must claim it with RecordBatch.Detach. Sort,
Window, CollectSink, BatchSink, SpillableBatchCollector, SortMergeJoin, the
hash-join build and partitioned aggregation's per-partition views all do;
the consumers that do NOT copy what they need out before returning (the
stage and shuffle sinks' bulk row append, HashAggregate's key arena). That
is the same contract BatchPool recycling has always relied on — this buffer
only drops the additional requirement that the driver call Release(), which
the worker's fragment driver does not, which is why nothing was being reused
on the distributed path at all.

Detach records the claim on every column VECTOR, not just on the batch
shell, because the batch a retaining consumer holds is not always the batch
the probe emitted: ColumnPrune, the set-op emitter and partitioned
aggregation mint a derived RecordBatch over the same *Vector pointers, and a
view minted downstream over one of our gathered columns propagates the claim
through Vector.Base. reusable() therefore tests the columns, not just the
shell.

The buffer is surrendered whole or not at all: the shell, the index arrays
and the gather vectors are all reachable from a batch a consumer kept, so
one claim drops everything and the next batch is freshly allocated.
