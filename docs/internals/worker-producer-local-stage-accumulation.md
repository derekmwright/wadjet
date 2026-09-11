# Worker producer local stage accumulation

Source: internal/worker/stage_sink_accum.go — var stageSinkAccum = optswitch.Register("stage-sink-accum", "WADJET_STAGE_SINK_ACCUM",, moved 2026-09-11 (#1026)

Producer-local row accumulation for unpartitionedStageSink.

Before this, Consume held the sink mutex across appendBatchRowsBulk: the
row COPY itself ran inside the critical section, so every producer
goroutine feeding a stage output serialized on one lock for the duration
of a full batch copy. The first SF100 block/mutex profiles (2026-08-21)
measured unpartitionedStageSink.Consume at 32.6% of all worker mutex
delay, 95.8% of it in the real sync.Mutex.Unlock handoff, 39-44s of mutex
delay per worker per suite run. The floor on total serialized time is the
total copy time, so amortizing lock ACQUISITIONS cannot fix it — the copy
has to leave the critical section.

partitionedShuffleSink had the identical shape (appendAndMaybeFlush was
64% of worker mutex block before e50fd1b, 0.8% after) and its fix is the
template: a consumer appends into a checked-out local buffer with no
shared lock and touches shared state only at a handoff boundary. The
unpartitioned sink goes one step further than that template. There is
exactly ONE output stream here (not numParts accumulators), so a filled
local slab is written as its own chunk instead of being copied a second
time into a shared accumulator: the sink lock is then only ever held to
hand over stream ownership and bump counters, never to copy a row.

Chunk sizing is unchanged. A slab flushes at the sink's own thresholds
(flushRows / flushBytesT), so a serial producer produces byte-identical
chunk boundaries to the pre-change accumulator — the coalescing decision
stays the SINK's, exactly as docs/design/morsel-execution.md §4.1.1
requires (chunk-per-consume fragmented stage output ~100x and cost 15% of
the SF10 suite). The one new bound is stageSlabBudgetFactor below.
