# Worker linear morsel parallelism

Source: internal/worker/executor_fragment.go — func (e *Executor) runFragmentLinearParallel(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink fragmentSink, result *distributed.ResultNotification, k int, gate *widthGate) error {, moved 2026-09-11 (#1026)

runFragmentLinearParallel is the morsel-driven variant of
runFragmentLinear: a single producer feeds the byte-bounded morsel
dispenser (which splits row-group-sized decoded batches into ~2048-row
zero-copy views — see morsel_dispenser.go for the budget and view-safety
story), consumed by k goroutines that each run a private Clone()d copy of
the op chain. The fragment sink is shared and internally concurrent: the
exchange sink locks per PARTITION, the unpartitioned sink appends under
its lock and double-buffers the chunk encode outside it, and the gather
sink serializes internally (low-volume reply path). Every sink consumes
each batch synchronously and retains no reference afterward, so no Sel
snapshot is needed. The previous sink-WIDE mutex here serialized k
consumers through the fragment's dominant cost (hash+append+encode) and
held join/probe fragments +12-27% slower under morsel-auto (SF100
default-flip gate, 2026-07-07).

Pressure COLLAPSES k (the breaker-path rule, applied here): the linear
path's transients — dispenser in-flight bytes, join-probe output batches —
are tracker-invisible by design, so the collapse signal is the process
heap itself (memory.HeapBackpressureActive, 70% of GOMEMLIMIT). On the
first trip during parallel consume the consumers stop and the remaining
input drains serially through the original chain: parallelism is a
fair-weather optimization, and the pressure story is exactly today's
SF100-validated serial one. The SF100 2026-07-03 A/B failed on precisely
this gap — Q17/Q18 grace-join linear fragments blew the worker heap with
zero collapses because only the breaker path had a collapse rule.

One batch is pushed through the ORIGINAL ops before cloning ("warmup",
same pattern as exec.Pipeline.runParallel): operator scratch is per-clone,
but predicate/expression closures are SHARED across clones and resolve
column indices lazily on first use (exec.ColumnCompare's cachedIdx,
expr.ColRef's sync.Once). The warmup batch completes those writes while
the chain is still single-threaded; clones then only read the resolved
caches.
