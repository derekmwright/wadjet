# Worker fragment backpressure

Source: internal/worker/executor_fragment.go — func (p *fragmentProgress) applyBackpressureSink(ctx context.Context, sink exec.Sink) error {, moved 2026-09-11 (#1026)
Superseded: This block now precedes applyBackpressureSink; applyBackpressure is the ordinary timed-pause helper below it.

applyBackpressure pauses the consume loop briefly when the process heap
is approaching GOMEMLIMIT. The signal (HeapBackpressureActive) fires at
70% of GOMEMLIMIT — well before the 95% spill backstop — and the pause
gives GC time to reclaim before the next batch lands. Without this hook,
scan-heavy stages allocate faster than GC can collect at SF100, the heap
climbs to the limit, and STW pauses lengthen until heartbeats starve and
coord reaps the worker (Q17 SF100, 2026-05-07).

Returns ctx.Err() if the context was cancelled during the pause; nil
otherwise. Cheap when no pressure: one cached atomic check per batch.

Backpressure is also installed at the engine level (exec.Pipeline.runSerial
/ runParallel) so single-process queries and breaker-phase consumes get
it for free. This wrapper exists for the linear/breaker-final loops that
don't go through Pipeline, and adds per-task counters + occasional logs.
applyBackpressureSink is the sink-aware variant for consume loops feeding
a pipeline breaker (#326): when the valve fires and the sink is a
spill-capable breaker holding the dominant tracked share, its spill path
runs instead of the 50ms sleep — sleeping the holder of live state
reclaims nothing. Clone sinks (tracking-only spill views) and sinks that
hold little fall through to the ordinary pause.
