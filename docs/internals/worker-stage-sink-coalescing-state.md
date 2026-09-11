# Worker stage sink coalescing state

Source: internal/worker/unpartitioned_stage_sink.go — coalesce   int8, moved 2026-09-11 (#1026)
Superseded: Producer-local slab accumulation is a separate enabled path with stageSlabBudgetFactor; the two-accumulator bound below describes only the shared-buffer path.

Chunk coalescing. A .wshf chunk is the downstream stage's batch AND
its decode unit (one s2 block + crc per chunk), so chunk size must be
the SINK's decision, not an echo of the caller's consume granularity —
morsel-parallel fragments consume at ~2048-row morsels, and writing
chunk-per-consume fragmented stage outputs ~100× (SF10 v1.5 A/B
2026-07-03: s2Decode 26.6s→35.6s, suite +15%). Consumed rows accumulate
in rowBuf and flush at unpartitionedFlushRows/-Bytes. Flat schemas only
(appendBatchRowsBulk/growBatchTo gained container arms in #397, but
row-at-a-time ones); container schemas keep the legacy
chunk-per-consume path. coalesce: 0=undecided,
1=coalescing, -1=legacy.

The flush is DOUBLE-BUFFERED (docs/design/morsel-execution.md §4.1.1
v1.7 follow-up): when the accumulator trips a threshold, it is swapped
out under mu and the s2 encode + write of up to flushBytes happens
OUTSIDE the lock, so concurrent consumers keep appending into the
spare buffer instead of stalling behind a 16 MB encode. `flushing`
admits exactly one flusher (the writer/bufFile are single-threaded);
flushCond backpressures a consumer whose freshly-refilled buffer trips
the threshold while a flush is still in flight — memory stays bounded
at two accumulators. The legacy (nested-schema) path never sets
flushing and keeps writer access under mu, which is exclusive with the
coalescing path by the coalesce flag.
