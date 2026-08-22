package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// fragmentBackpressurePauseMS is how long the consume loop sleeps when
// HeapBackpressureActive() fires. 50ms is one to two GC cycles at typical
// SF100 allocation rates — long enough for live heap to drop, short enough
// that downstream consumers don't time out. Tuneable via env var.
const fragmentBackpressurePauseMS = 50

// fragmentProgressInterval is how often runFragmentLinear / runFragmentWithBreakers
// log per-task progress for tasks that run longer than the heartbeat reap window.
// The coordinator reaps a worker after ~90s without a heartbeat, so a 30s
// log cadence ensures at least one progress line lands on the worker side
// before the coord-side reap log fires — operators correlating coord reaps
// with worker progress get the worker's view without waiting on a heap dump.
const fragmentProgressInterval = 30 * time.Second

// fragmentProgress emits "fragment task progress" log lines on long-running
// tasks. Cheap: a time.Since check on each consume call. Logging happens at
// most once per fragmentProgressInterval. Used by both runFragmentLinear and
// runFragmentWithBreakers to make slow-but-progressing tasks visible in
// worker logs (the SF100 Q17 hang investigation needed this signal because
// the coord saw silence between dispatch and reap with no worker-side
// evidence of forward progress).
// All counters are atomic and the log timestamps are mutex-guarded: under
// morsel-parallel fragments (runFragmentLinearParallel) addRows and
// applyBackpressure are called concurrently from k consumer goroutines.
type fragmentProgress struct {
	logger    *slog.Logger
	taskID    string
	stageID   string
	stageType string
	exec      *Executor // for SharedPoolStats(); nil-safe in tests
	started   time.Time
	rows      atomic.Int64

	// Heap-backpressure tracking. The fragment runner calls applyBackpressure
	// between batches; when HeapBackpressureActive() fires it sleeps briefly
	// to let GC catch up. We summarize how often and how long that fired so
	// worker logs make slow-task root cause obvious without per-pause spam.
	backpressureCount   atomic.Int64
	backpressurePauseMS atomic.Int64
	// pressureDrains counts valve firings a breaker sink answered by
	// draining (or by declining to sleep over live state) instead of the
	// 50ms pause — the #326 drain-instead-of-sleep path.
	pressureDrains atomic.Int64

	// srcAcq, when set by the runner, folds the source's per-tier file
	// acquisition tallies (src_acq_stats.go) into the phases line — the
	// straggler-mode attribution counters. Read only in finish().
	srcAcq srcAcqReporter

	// Phase timing (ns), fed by the fragment runners. Splits a slow task's
	// wall into WHERE it went — the 2026-07-20 Q08 diagnosis had a task at
	// uniform 1/3 speed and no counter that could say whether the source,
	// the operator chain, or the sink was pacing it.
	//   srcNs       time inside src.Next (decode + IO, incl. its stalls)
	//   srcBlockedNs producer blocked handing a batch to the consumer
	//   inputWaitNs consumer blocked waiting for the producer
	//   opsNs       operator-chain Execute (join probe, filter, project)
	//   sinkNs      sink consume (shuffle/output write)
	srcNs        atomic.Int64
	srcBlockedNs atomic.Int64
	inputWaitNs  atomic.Int64
	opsNs        atomic.Int64
	sinkNs       atomic.Int64

	mu                  sync.Mutex // guards the two log timestamps below
	lastLog             time.Time
	lastBackpressureLog time.Time
}

func newFragmentProgress(logger *slog.Logger, task distributed.Task, e *Executor) *fragmentProgress {
	now := time.Now()
	return &fragmentProgress{
		logger:    logger,
		taskID:    task.ID,
		stageID:   task.StageID,
		stageType: task.StageType,
		exec:      e,
		started:   now,
		lastLog:   now,
	}
}

func (p *fragmentProgress) addRows(n int64) {
	total := p.rows.Add(n)
	p.mu.Lock()
	if time.Since(p.lastLog) < fragmentProgressInterval {
		p.mu.Unlock()
		return
	}
	p.lastLog = time.Now()
	p.mu.Unlock()
	elapsed := time.Since(p.started)
	attrs := []any{
		"task_id", p.taskID,
		"stage_id", p.stageID,
		"stage_type", p.stageType,
		"elapsed", elapsed.Round(time.Second),
		"rows", total,
	}
	if p.exec != nil {
		used, budget := p.exec.SharedPoolStats()
		if budget > 0 {
			attrs = append(attrs,
				"pool_used_mb", used/1024/1024,
				"pool_budget_mb", budget/1024/1024,
				"pool_pct", used*100/budget)
		}
	}
	if bp := p.backpressureCount.Load(); bp > 0 {
		attrs = append(attrs,
			"bp_count", bp,
			"bp_paused_ms", p.backpressurePauseMS.Load())
	}
	attrs = p.appendPhaseAttrs(attrs)
	p.logger.Info("fragment task progress", attrs...)
}

// appendPhaseAttrs adds the nonzero phase-timing splits to a log line.
func (p *fragmentProgress) appendPhaseAttrs(attrs []any) []any {
	for _, ph := range []struct {
		key string
		ns  *atomic.Int64
	}{
		{"src_ms", &p.srcNs},
		{"src_blocked_ms", &p.srcBlockedNs},
		{"input_wait_ms", &p.inputWaitNs},
		{"ops_ms", &p.opsNs},
		{"sink_ms", &p.sinkNs},
	} {
		if v := ph.ns.Load(); v > 0 {
			attrs = append(attrs, ph.key, v/1e6)
		}
	}
	return attrs
}

// fragmentPhaseLogFloor gates the completion-time phase line: tasks
// shorter than this log at Debug, keeping INFO volume flat for the
// thousands of sub-second tasks per suite run.
const fragmentPhaseLogFloor = 5 * time.Second

// finish emits one "fragment task phases" line with the task's final
// phase split. INFO for tasks long enough to matter, Debug otherwise.
func (p *fragmentProgress) finish(totalRows int64) {
	elapsed := time.Since(p.started)
	attrs := p.appendPhaseAttrs([]any{
		"task_id", p.taskID,
		"stage_id", p.stageID,
		"stage_type", p.stageType,
		"elapsed_ms", elapsed.Milliseconds(),
		"rows", totalRows,
	})
	notable := false
	if p.srcAcq != nil {
		attrs = append(attrs, p.srcAcq.srcAcqAttrs()...)
		notable = p.srcAcq.srcAcqNotable()
	}
	// The floor keeps INFO volume flat for the thousands of sub-second
	// tasks, but a task that stalled on another worker's upload is the one
	// case where the SHORT tasks are the finding — the gather-merge tail
	// emits 4 rows and never reaches 5s (window-2 §7.1). Escalate those.
	if elapsed >= fragmentPhaseLogFloor || notable {
		p.logger.Info("fragment task phases", attrs...)
		return
	}
	p.logger.Debug("fragment task phases", attrs...)
}

// timeSource wraps src so every Next is charged to p.srcNs.
func (p *fragmentProgress) timeSource(src exec.Source) exec.Source {
	return &timedSource{Source: src, ns: &p.srcNs}
}

// timedSource charges the wall spent inside the wrapped Source's Next to
// an atomic counter. Init/Close pass through untimed.
type timedSource struct {
	exec.Source
	ns *atomic.Int64
}

func (t *timedSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	t0 := time.Now()
	b, err := t.Source.Next(ctx)
	t.ns.Add(time.Since(t0).Nanoseconds())
	return b, err
}

// applyBackpressure pauses the consume loop briefly when the process heap
// is approaching GOMEMLIMIT. The signal (HeapBackpressureActive) fires at
// 70% of GOMEMLIMIT — well before the 95% spill backstop — and the pause
// gives GC time to reclaim before the next batch lands. Without this hook,
// scan-heavy stages allocate faster than GC can collect at SF100, the heap
// climbs to the limit, and STW pauses lengthen until heartbeats starve and
// coord reaps the worker (Q17 SF100, 2026-05-07).
//
// Returns ctx.Err() if the context was cancelled during the pause; nil
// otherwise. Cheap when no pressure: one cached atomic check per batch.
//
// Backpressure is also installed at the engine level (exec.Pipeline.runSerial
// / runParallel) so single-process queries and breaker-phase consumes get
// it for free. This wrapper exists for the linear/breaker-final loops that
// don't go through Pipeline, and adds per-task counters + occasional logs.
// applyBackpressureSink is the sink-aware variant for consume loops feeding
// a pipeline breaker (#326): when the valve fires and the sink is a
// spill-capable breaker holding the dominant tracked share, its spill path
// runs instead of the 50ms sleep — sleeping the holder of live state
// reclaims nothing. Clone sinks (tracking-only spill views) and sinks that
// hold little fall through to the ordinary pause.
func (p *fragmentProgress) applyBackpressureSink(ctx context.Context, sink exec.Sink) error {
	if !memory.HeapBackpressureActive() {
		return nil
	}
	if handled, err := exec.TryPressureDrain(ctx, sink); handled || err != nil {
		if err == nil {
			p.pressureDrains.Add(1)
		}
		return err
	}
	return p.applyBackpressure(ctx)
}

func (p *fragmentProgress) applyBackpressure(ctx context.Context) error {
	if !memory.HeapBackpressureActive() {
		return nil
	}
	p.backpressureCount.Add(1)
	started := time.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(fragmentBackpressurePauseMS * time.Millisecond):
	}
	p.backpressurePauseMS.Add(time.Since(started).Milliseconds())
	// Log on the first hit and at most every 30 s thereafter so sustained
	// backpressure shows up in logs without flooding them.
	p.mu.Lock()
	shouldLog := p.lastBackpressureLog.IsZero() || time.Since(p.lastBackpressureLog) > 30*time.Second
	if shouldLog {
		p.lastBackpressureLog = time.Now()
	}
	p.mu.Unlock()
	if shouldLog {
		attrs := []any{
			"task_id", p.taskID,
			"stage_id", p.stageID,
			"stage_type", p.stageType,
			"count", p.backpressureCount.Load(),
			"total_paused_ms", p.backpressurePauseMS.Load(),
			"pressure_drains", p.pressureDrains.Load(),
		}
		if p.exec != nil {
			used, budget := p.exec.SharedPoolStats()
			if budget > 0 {
				attrs = append(attrs,
					"pool_used_mb", used/1024/1024,
					"pool_budget_mb", budget/1024/1024)
			}
		}
		p.logger.Info("fragment task heap backpressure", attrs...)
	}
	return nil
}

// executeFragment runs a multi-operator pipeline described by task.Operators[]
// end-to-end on this worker, without intermediate S3 round-trips between
// operators. This is the long-term shape that dissolves the
// per-operator-per-task floor cost (S3 PUT + NATS dispatch + per-task setup).
//
// Pipeline shape: Operators[0] is the source, Operators[len-1] is the sink,
// and the operators in between are unary transforms applied per batch.
//
// Common shapes the planner emits:
//
//	[Scan, Filter?, ExchangeSender]                  — replaces scan + shuffle stages
//	[ShuffleSource, HashJoinProbe, ExchangeSender]   — replaces shuffle-recv + join stages
//	[ShuffleSource, HashAggregate, GatherSink]       — final-aggregate + gather
func (e *Executor) executeFragment(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Operators) == 0 {
		return fmt.Errorf("fragment task %s: empty Operators", task.ID)
	}
	specs := task.Operators
	sourceSpec := specs[0]
	sinkSpec := specs[len(specs)-1]
	middle := specs[1 : len(specs)-1]

	if !isFragmentSourceOp(sourceSpec.Type) {
		return fmt.Errorf("fragment task %s: first op %q is not a source", task.ID, sourceSpec.Type)
	}
	if !isFragmentSinkOp(sinkSpec.Type) {
		return fmt.Errorf("fragment task %s: last op %q is not a sink", task.ID, sinkSpec.Type)
	}
	// Locate every pipeline-breaker (HashAggregate, Sort) in middle. Reject
	// middle ops that aren't recognised as unary or breaker.
	var breakerIdxs []int
	for i, m := range middle {
		if isFragmentSourceOp(m.Type) || isFragmentSinkOp(m.Type) {
			return fmt.Errorf("fragment task %s: middle op %d is %q, must be unary or breaker", task.ID, i, m.Type)
		}
		if isFragmentBreakerOp(m.Type) {
			breakerIdxs = append(breakerIdxs, i)
		}
	}

	// Empty source files: legitimate "upstream produced nothing" case (e.g.,
	// an aggregate or semi-join filtered every row). Emit zero rows and no
	// result files; downstream stages handle the empty-input shape via their
	// own short-circuits; we short-circuit here before any S3 I/O happens.
	//
	// Exception: OpGatherSink. The coordinator's gather receiver counts
	// terminal markers, not messages — skipping finalize would leave it
	// hanging until the 10-minute gather timeout. Open + finalize the sink
	// (its Finalize publishes a terminal even with no batches consumed) so
	// the receiver unblocks immediately on empty fragments.
	//
	// Exception: eager-fed aliases (Task.EagerInputs). Their InputFiles is
	// empty BY CONSTRUCTION — the file set streams in as producer-task
	// manifests — so "no frozen files" carries no emptiness signal at all.
	// Short-circuiting here silently dropped the entire input (0 rows,
	// task success) when eager dispatch first went end-to-end.
	// Exception: an UNGROUPED aggregate fragment that owes SQL its identity
	// row. An ungrouped aggregate returns exactly one row for any input,
	// including none — COUNT()=0, SUM/MIN/MAX/AVG=NULL — and a downstream
	// consumer (scalar-subquery substitution, #292) blocks on that row
	// existing. Fall through with an empty source so the aggregate
	// finalizes and the row is written.
	//
	// Two shapes qualify, and the difference is whose type the row wears.
	//
	//  1. COUNT-family, at ANY stage: their output type (int64) is
	//     input-independent, so a partial identity row is typed the same as
	//     every sibling's and merges cleanly.
	//
	//  2. Anything else, ONLY on the ungrouped final (EmitEmptyIdentity) and
	//     ONLY when the planner declared every aggregate's output type
	//     (AggSpec.OutputType). MIN/MAX types follow the input column, which
	//     cannot be read here — zero input files, no schema — so without the
	//     declaration the row would guess. The final is also where guessing
	//     costs the least even if the declaration were wrong: its input being
	//     empty means there are no sibling partials to merge against, and the
	//     planner makes it a Singleton, so the identity row it emits is the
	//     one row of the answer, not one of N (#329).
	//
	// Everything else keeps the empty-output short-circuit: a partial or
	// merge_aggregate that produces nothing is absorbed by the final above
	// it, which emits the row instead — and one mistyped partial poisons the
	// merge for every sibling (a float64-typed NULL min among string-typed
	// partials broke the skew-parity left join before this gate).
	//
	// Exception: a RIGHT or FULL join whose PROBE partition is empty. Its
	// build rows are all unmatched by construction, and they are the rows the
	// join exists to preserve — one shuffle partition holding build rows and
	// no probe rows is the ordinary case, not a degenerate one. Falling
	// through builds the hash table, probes nothing, and lets the flush emit
	// them NULL-padded on the probe side, using the declared ProbeSchema for
	// their names (#352).
	emptyScalarAgg := false
	emptyProbeOuterJoin := false
	_, eagerSource := task.EagerInputs[sourceSpec.InputAlias]
	if len(sourceSpec.InputFiles) == 0 && !eagerSource {
		for _, m := range middle {
			if m.Type != distributed.OpHashJoinProbe && m.Type != distributed.OpBroadcastProbe {
				continue
			}
			if !preservesBuildSide(mapJoinTypeString(m.JoinType)) {
				continue
			}
			// A build with nothing to read owes nothing either; an eager
			// build's file list is empty BY CONSTRUCTION and carries no
			// emptiness signal at all (see eagerInputFor).
			_, eagerBuild := task.EagerInputs[m.BuildAlias]
			if len(m.BuildFiles) > 0 || eagerBuild {
				emptyProbeOuterJoin = true
				break
			}
		}
		for i, m := range middle {
			if m.Type != distributed.OpHashAggregate || len(m.GroupByCols) != 0 || m.GroupByAll {
				continue
			}
			if len(m.Aggregates) == 0 {
				break
			}
			countOnly, typed := true, m.EmitEmptyIdentity
			for _, a := range m.Aggregates {
				switch strings.ToLower(strings.TrimSpace(a.Func)) {
				case "count", "count_distinct", "approx_distinct":
				default:
					countOnly = false
				}
				if a.OutputType == nil {
					typed = false
				}
			}
			if countOnly || typed {
				emptyScalarAgg = true
				// There is nothing to merge, and merge mode would cost the
				// identity row its COUNT: the merge rewrite turns COUNT into
				// SUM over the partial counts, whose identity is NULL, not 0.
				// Drop it for this run only — over zero input the rewrite has
				// no other effect, since neither InputCol nor OutputCol is
				// ever read. specs is task.Operators' backing array, so copy
				// before writing (a redelivered task re-reads it).
				if m.MergeMode {
					specs = append([]distributed.OpSpec(nil), specs...)
					middle = specs[1 : len(specs)-1]
					middle[i].MergeMode = false
				}
			}
			break
		}
		if !emptyScalarAgg && !emptyProbeOuterJoin {
			if sinkSpec.Type == distributed.OpGatherSink {
				sink, err := e.openFragmentSink(task, sinkSpec)
				if err != nil {
					return fmt.Errorf("fragment task %s: open gather sink (empty source): %w", task.ID, err)
				}
				defer sink.close()
				if err := sink.finalize(ctx, task, result); err != nil {
					return fmt.Errorf("fragment task %s: finalize gather sink (empty source): %w", task.ID, err)
				}
			}
			return nil
		}
	}

	// Build the source.
	var src exec.Source
	var err error
	if emptyScalarAgg || emptyProbeOuterJoin {
		src = emptyFragmentSource{}
	} else {
		src, err = e.buildFragmentSource(task, sourceSpec)
	}
	if err != nil {
		return fmt.Errorf("fragment task %s: source: %w", task.ID, err)
	}
	if err := src.Init(ctx); err != nil {
		return fmt.Errorf("fragment task %s: source init: %w", task.ID, err)
	}
	defer src.Close()

	// Build the sink. Schema-discovery is lazy for the partitioned/
	// unpartitioned sinks; we capture the first batch's schema and construct
	// on demand.
	sink, err := e.openFragmentSink(task, sinkSpec)
	if err != nil {
		return fmt.Errorf("fragment task %s: sink: %w", task.ID, err)
	}
	defer sink.close()

	// Deferred (attach-on-arrival) consume registration happens BEFORE emit
	// op construction: guarded re-emits share these poll slots — the emit
	// op buffers rows until the consumed bloom settles, then retro-filters
	// (docs/design/attach-on-arrival-dynamic-filters.md §Guarded re-emit).
	deferredBlooms := e.registerDeferredBlooms(sourceSpec.DynamicFilters)

	// Dynamic-filter emit (Trino-style semi-join pushdown). When the source
	// spec carries DynamicFilterEmits, prepend a pass-through accumulator
	// op per emit to the unary chain. Snapshots are uploaded after the
	// pipeline finishes; refs land in result.DynamicFilterPartials.
	emitOps, err := buildDynamicFilterEmitOps(sourceSpec.DynamicFilterEmits)
	if err != nil {
		return fmt.Errorf("fragment task %s: dynamic filter emit ops: %w", task.ID, err)
	}
	for i, op := range emitOps {
		if err := op.Init(ctx); err != nil {
			return fmt.Errorf("fragment task %s: emit op init: %w", task.ID, err)
		}
		for _, guardID := range sourceSpec.DynamicFilterEmits[i].GuardConsumes {
			d, ok := deferredBlooms[guardID]
			if !ok {
				// Guard's consume is not deferred: either the bloom shipped
				// resolved (source filters from batch 0 — no guard needed)
				// or it was withheld (no filtering anywhere — unguarded
				// emit is the correct wider degradation).
				continue
			}
			op.AddGuard(guardID, d.column, pendingBloomProbe{pb: d.pb})
		}
	}
	// Output-side emits (DynamicFilterEmit.AtOutput — semi/anti build
	// filters): accumulate over the stage's OUTPUT stream by wrapping the
	// sink, which covers the linear, parallel, and breaker paths in one
	// place. The wrapper serializes accumulation — parallel fragments call
	// sink.consume concurrently, and racing `bloom[i] |= bit` read-modify-
	// writes would silently LOSE keys (false rejections downstream).
	sinkEmitOps, err := buildDynamicFilterEmitOps(sinkSpec.DynamicFilterEmits)
	if err != nil {
		return fmt.Errorf("fragment task %s: sink emit ops: %w", task.ID, err)
	}
	if len(sinkEmitOps) > 0 {
		for _, op := range sinkEmitOps {
			if err := op.Init(ctx); err != nil {
				return fmt.Errorf("fragment task %s: sink emit op init: %w", task.ID, err)
			}
		}
		sink = &emitCapturingSink{inner: sink, ops: sinkEmitOps}
	}
	allEmitOps := append(append([]*exec.DynamicFilterEmitOp(nil), emitOps...), sinkEmitOps...)
	allEmitSpecs := append(append([]distributed.DynamicFilterEmit(nil), sourceSpec.DynamicFilterEmits...), sinkSpec.DynamicFilterEmits...)

	// Row-level dynamic-filter consume: the source already applies specs at
	// row-group granularity (buildFragmentSource); uniform-key filters (join
	// keys) never prune a row group, so also filter row-level via selection
	// vectors before the first operator. Adaptive disable inside
	// BloomFilterOp bypasses non-selective blooms after 32 batches.
	src = e.wrapSourceWithBloomFilters(ctx, task, sourceSpec, src, deferredBlooms)

	if len(breakerIdxs) == 0 {
		// Linear pipeline: source → unary ops → sink.
		preOps, cleanup, err := e.buildUnaryChain(ctx, task, middle)
		if err != nil {
			return err
		}
		defer cleanup()
		ops := prependEmitOps(emitOps, preOps)
		if err := e.runFragmentLinear(ctx, task, src, ops, sink, result); err != nil {
			return err
		}
		return e.finalizeDynamicFilterEmits(ctx, task, allEmitOps, allEmitSpecs, result)
	}

	// One or more pipeline-breakers — split middle into N+1 unary segments
	// around them and run a chain of consume/drain phases. No materialization
	// to disk between phases; each breaker holds its state in memory (with
	// optional spill).
	//
	// Emit ops are prepended ONLY to the first segment (pre-source ops) so
	// they observe every input row regardless of breaker layout.
	if err := e.runFragmentWithBreakers(ctx, task, src, middle, breakerIdxs, sink, result, emitOps); err != nil {
		return err
	}
	return e.finalizeDynamicFilterEmits(ctx, task, allEmitOps, allEmitSpecs, result)
}

// emitCapturingSink decorates a fragmentSink with output-side dynamic-filter
// accumulators. The mutex is required: parallel fragment paths consume from
// k goroutines and the emit op's bloom writes are read-modify-write.
type emitCapturingSink struct {
	inner fragmentSink
	ops   []*exec.DynamicFilterEmitOp
	mu    sync.Mutex
}

func (s *emitCapturingSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	for _, op := range s.ops {
		// Late-materialized view columns carry empty typed backing; the
		// emit op reads typed vectors directly, so flatten first (same
		// contract runChain applies before every non-view-aware op). Only
		// marked stages pay this, on their post-join reduced output.
		exec.FlattenForConsumer(b, op)
		// Pass-through accumulator; never mutates b, never errors on data.
		if _, err := op.Execute(ctx, b); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Unlock()
	return s.inner.consume(ctx, b)
}

func (s *emitCapturingSink) finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	return s.inner.finalize(ctx, task, result)
}

func (s *emitCapturingSink) close() { s.inner.close() }

// wrapSourceWithBloomFilters applies the spec's dynamic-filter blooms at row
// level on top of whatever row-group pruning the source already does. A
// single wrap point ahead of the operator chain covers the linear, morsel-
// parallel, and breaker execution paths (Next is producer-single-threaded
// in all three). Returns src unchanged when there is nothing to apply.
func (e *Executor) wrapSourceWithBloomFilters(ctx context.Context, task distributed.Task, spec distributed.OpSpec, src exec.Source, deferred map[string]*deferredBloomFilter) exec.Source {
	if len(spec.DynamicFilters) == 0 || spec.Type != distributed.OpScan {
		return src
	}
	// Deferred (attach-on-arrival) specs were registered with the
	// singleflight poller by registerDeferredBlooms; the source installs
	// each bloom mid-scan when its artifact lands
	// (docs/design/attach-on-arrival-dynamic-filters.md). Iterate the spec
	// list (not the map) to keep install order deterministic.
	var pending []*deferredBloomFilter
	for _, s := range spec.DynamicFilters {
		if d, ok := deferred[s.FilterID]; ok {
			pending = append(pending, d)
		}
	}
	_, blooms, err := e.materializeDynamicFilters(spec.DynamicFilters)
	if err != nil {
		blooms = nil
	}
	var ops []*exec.BloomFilterOp
	for _, bf := range blooms {
		if bf == nil {
			continue
		}
		op := exec.NewBloomFilterOp(bf.Bloom, bf.BloomMask, []string{bf.Column}, bf.UseIntKey)
		if err := op.Init(ctx); err != nil {
			continue
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 && len(pending) == 0 {
		return src
	}
	// Late-resolving filters are also forwarded to the iterator layer when
	// the source supports it — deferred specs skip the buildFragmentSource
	// attach (nothing to materialize yet), and without this the row-group
	// pruning + prune-aware advise layers never see attach-on-arrival
	// filters at all (the EC2 SF100 finding in
	// docs/design/rowgroup-touch-ahead.md §third arm).
	var groupSink dynamicFilterAppender
	if dfLateGroupAttach {
		groupSink, _ = src.(dynamicFilterAppender)
	}
	e.logger.Info("dynamic_filter: row-level blooms active on fragment scan",
		"task_id", task.ID, "stage_id", task.StageID,
		"filters", len(ops), "deferred", len(pending))
	return &bloomFilteredSource{
		inner: src, ops: ops, pending: pending, groupSink: groupSink,
		logger: e.logger, taskID: task.ID, stageID: task.StageID,
	}
}

// deferredBloomFilter tracks one attach-on-arrival consume on a fragment
// scan: the shared poll slot plus enough identity to build the op and log
// the install when the bloom lands.
type deferredBloomFilter struct {
	pb       *pendingBloom
	filterID string
	column   string
	keyType  string // spec KeyType — typing for the late-attached range values
	batches  int    // batches seen before attach (engagement telemetry)
}

// bloomFilteredSource filters each batch through row-level bloom ops via
// selection vectors. Batches that reject every row are skipped (the
// consumer never sees them). Deferred filters promote into active ops
// mid-scan when their staged artifact resolves — Next is producer-single-
// threaded in every execution path (linear, morsel, breaker), so the
// promotion needs no synchronization beyond the pendingBloom's done channel.
type bloomFilteredSource struct {
	inner   exec.Source
	ops     []*exec.BloomFilterOp
	pending []*deferredBloomFilter
	// groupSink receives resolved deferred filters at iterator level (row-
	// group pruning); nil when the inner source has no parquet iterator
	// layer (e.g. manifest-fed eager inputs).
	groupSink dynamicFilterAppender
	logger    *slog.Logger
	taskID    string
	stageID   string
}

func (s *bloomFilteredSource) Init(ctx context.Context) error { return s.inner.Init(ctx) }

// promotePending installs any deferred blooms whose artifacts resolved —
// row-level op here, iterator-level forms into groupSink when the inner
// source has one. Non-blocking; called once per batch.
func (s *bloomFilteredSource) promotePending(ctx context.Context) {
	if len(s.pending) == 0 {
		return
	}
	s.pending = promoteDeferredFilters(ctx, s.pending, s.logger, s.taskID, s.stageID, s.groupSink != nil,
		func(op *exec.BloomFilterOp, ranges []exec.DynamicRange, bsf *exec.BloomScanFilter) {
			if op != nil {
				s.ops = append(s.ops, op)
			}
			if s.groupSink != nil {
				s.groupSink.AddDynamicFilters(ranges, []*exec.BloomScanFilter{bsf})
			}
		})
}

func (s *bloomFilteredSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		b, err := s.inner.Next(ctx)
		if err != nil || b == nil {
			// End of stream (the fragment's defer closes the INNER source,
			// not this wrapper, so this is the reliable end-of-scan hook):
			// deferred filters still outstanding went fully unfiltered.
			s.logOutstandingPending()
			return b, err
		}
		s.promotePending(ctx)
		for _, op := range s.ops {
			nb, err := op.Execute(ctx, b)
			if err != nil {
				return nil, err
			}
			if nb == nil || nb.ActiveLen() == 0 {
				b = nil
				break
			}
			b = nb
		}
		if b != nil {
			// DETACH the selection vector from the ops' reused scratch.
			// BloomFilterOp writes b.Sel into its per-op selBuf, which the
			// NEXT Execute overwrites. This source runs in the morsel
			// producer while consumers still hold earlier batches — without
			// a copy, batch N's Sel is corrupted by batch N+1's content
			// (SF100 Q04 2026-08-04: Filter.Eval walked a stale index past
			// the null bitmap, index out of range [1057*64+6]).
			if b.Sel != nil {
				b.Sel = append([]uint32(nil), b.Sel...)
			}
			return b, nil
		}
	}
}

// logOutstandingPending reports deferred filters that never installed —
// the head-coverage trade went fully unfiltered for these. Idempotent via
// the pending reset; called from the end-of-stream path and Close.
func (s *bloomFilteredSource) logOutstandingPending() {
	for _, d := range s.pending {
		if s.logger != nil {
			s.logger.Info("dynamic_filter: late attach never arrived",
				"task_id", s.taskID, "stage_id", s.stageID,
				"filter_id", d.filterID, "batches_seen", d.batches)
		}
	}
	s.pending = nil
}

func (s *bloomFilteredSource) Close() error {
	s.logOutstandingPending()
	return s.inner.Close()
}

func prependEmitOps(emit []*exec.DynamicFilterEmitOp, rest []exec.UnaryOperator) []exec.UnaryOperator {
	if len(emit) == 0 {
		return rest
	}
	out := make([]exec.UnaryOperator, 0, len(emit)+len(rest))
	for _, op := range emit {
		out = append(out, op)
	}
	out = append(out, rest...)
	return out
}

func buildDynamicFilterEmitOps(specs []distributed.DynamicFilterEmit) ([]*exec.DynamicFilterEmitOp, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]*exec.DynamicFilterEmitOp, 0, len(specs))
	for _, s := range specs {
		if s.FilterID == "" || s.KeyColumn == "" {
			return nil, fmt.Errorf("dynamic_filter_emit: FilterID and KeyColumn required")
		}
		if s.BloomBits <= 0 {
			return nil, fmt.Errorf("dynamic_filter_emit %s: BloomBits must be > 0", s.FilterID)
		}
		out = append(out, exec.NewDynamicFilterEmitOp(s.FilterID, s.KeyColumn, s.KeyType, s.BloomBits))
	}
	return out, nil
}

// fragmentDriver drives one operator chain, honouring the bounded-output
// protocol: an operator that fans one input batch out to far more rows than it
// contains (a hash-join probe) emits a bounded slice per call and suspends the
// rest, and this driver re-runs the chain below it for every resumed slice
// before handing it the next input (#317). Without it the probe has nowhere to
// put the remainder and materialises the whole fan-out in one live allocation —
// which GOMEMLIMIT cannot help with, because the memory is not garbage.
//
// One driver per chain: a chain is driven by exactly one goroutine (worker i
// owns chains[i]), so opsNs needs no atomics. deliver keeps the ownership rules
// each caller already had — the fragment paths never released batches, and
// still don't.
//
// Every worker loop that can be handed a join probe runs through this driver.
// The others stay unopted because no bounded operator can reach them: the
// gather stage drives a lone Limit (executor_stage.go), the shuffle task drives
// bloom filters and a column prune (executor.go), and filteredSource drives
// compiled build-side filters (shuffle_computed_cols.go). A probe only ever
// lands in a fragment's unary chain.
type fragmentDriver struct {
	d      *exec.ChainDriver
	sinkNs int64 // nanoseconds spent inside deliver during the current push
}

func newFragmentDriver(ops []exec.UnaryOperator, deliver func(context.Context, *batch.RecordBatch) error) *fragmentDriver {
	// The opt-in is this driver's promise to drain pending output; it must
	// happen before any Clone() so cloned chains inherit it.
	exec.EnableBoundedOutput(ops)
	fd := &fragmentDriver{}
	fd.d = exec.NewChainDriver(ops, func(ctx context.Context, b *batch.RecordBatch) error {
		t0 := time.Now()
		err := deliver(ctx, b)
		fd.sinkNs += time.Since(t0).Nanoseconds()
		return err
	})
	return fd
}

// push runs b through the chain, delivering every output batch. It returns the
// nanoseconds spent in the operators themselves — total minus deliver — for the
// caller's opsNs accounting.
func (fd *fragmentDriver) push(ctx context.Context, b *batch.RecordBatch) (int64, error) {
	t0 := time.Now()
	fd.sinkNs = 0
	_, err := fd.d.Push(ctx, b)
	return time.Since(t0).Nanoseconds() - fd.sinkNs, err
}

// runFragmentLinear streams batches from src through unary ops into the sink.
// No pipeline-breaker — every batch flows end-to-end before the next is read.
//
// Producer/consumer split: the source pull (parquet decode + S3 GET — CPU-
// and I/O-heavy) runs in one goroutine; the operator chain + sink consume
// (CPU-heavy too: HashJoin probe, shuffle write, agg merge) runs in another,
// connected by a small bounded channel. 2026-05-22 SF100 Q18 profile (v3)
// showed source.Next 22.7% cum + sink consume 15.2% cum running serially per
// batch in a single goroutine; workers used ~1.4 / 16 cores. Splitting lets
// source decode batch N+1 while the consumer processes batch N — wall time
// drops to max(producer, consumer) per batch instead of the sum.
//
// Channel cap is intentionally small (2 batches × ≤2048 rows). Memory is
// bounded; backpressure remains natural (producer blocks on send when consumer
// is slow). All fragmentProgress accesses are kept inside the consumer
// goroutine to avoid races on its counters; applyBackpressure (heap-pressure
// pause) lives in the consumer so the pause reflects the full pipeline state.
func (e *Executor) runFragmentLinear(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink fragmentSink, result *distributed.ResultNotification) error {
	// Morsel-driven parallel path (docs/design/morsel-execution.md §4). k > 1
	// only when the flag allows it, every op is Cloneable, and the fragment
	// clears the size gate. Active width is metered by the gate (§4.2.1);
	// in legacy mode tokens are held for the duration of the fragment.
	if k, gate, release := e.morselFragmentWorkers(task, ops); k > 1 {
		defer release()
		if dn, ok := src.(producerTokenDonor); ok && gate != nil {
			gate.donor = dn
		}
		e.logger.Debug("morsel parallel fragment",
			"task_id", task.ID,
			"stage_id", task.StageID,
			"k", k,
			"estimated_bytes", task.EstimatedBytes,
			"cpu_tokens_in_use", e.cpuTokens.InUse(),
			"cpu_tokens_cap", e.cpuTokens.Capacity())
		return e.runFragmentLinearParallel(ctx, task, src, ops, sink, result, k, gate)
	}
	progress := exec.ProgressReporterFromContext(ctx)
	fp := newFragmentProgress(e.logger, task, e)
	if r, ok := src.(srcAcqReporter); ok {
		fp.srcAcq = r
	}
	src = fp.timeSource(src)
	var totalRows int64
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		t0 := time.Now()
		err := sink.consume(ctx, b)
		fp.sinkNs.Add(time.Since(t0).Nanoseconds())
		if err != nil {
			return err
		}
		n := int64(b.ActiveLen())
		totalRows += n
		if progress != nil {
			progress.AddRows(n)
		}
		fp.addRows(n)
		return nil
	}

	const batchChanCap = 2
	ch := make(chan *batch.RecordBatch, batchChanCap)

	g, gctx := errgroup.WithContext(ctx)
	// Producer: source.Next → channel. Closes ch on EOF or error.
	g.Go(func() error {
		defer close(ch)
		for {
			b, err := src.Next(gctx)
			if err != nil {
				return fmt.Errorf("source next: %w", err)
			}
			if b == nil {
				return nil
			}
			t0 := time.Now()
			select {
			case <-gctx.Done():
				return gctx.Err()
			case ch <- b:
			}
			fp.srcBlockedNs.Add(time.Since(t0).Nanoseconds())
		}
	})
	// Consumer: ops + sink. Drives backpressure (heap pause) and progress.
	driver := newFragmentDriver(ops, func(ctx context.Context, b *batch.RecordBatch) error {
		if b.ActiveLen() == 0 {
			return nil
		}
		if err := consume(ctx, b); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		for {
			t0 := time.Now()
			b, ok := <-ch
			fp.inputWaitNs.Add(time.Since(t0).Nanoseconds())
			if !ok {
				return nil
			}
			if err := fp.applyBackpressure(gctx); err != nil {
				return err
			}
			opsNs, err := driver.push(gctx, b)
			fp.opsNs.Add(opsNs)
			if err != nil {
				return err
			}
		}
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("fragment task %s: %w", task.ID, err)
	}
	defer func() { fp.finish(totalRows) }() // closure: spill flush below still adds rows

	// Spilled-partition flush. Grace Hash Join probes route rows whose hash
	// partition was evicted to disk; without this drain, those probe rows
	// never re-enter the chain and the join silently drops matches. Mirror
	// of exec.Pipeline.flushSpilledOps. Q05 with shared-spill + multi-probe
	// chain (build pressure → primary's only build partition spills →
	// every probe row routes to disk) returns 0 rows without this drain.
	if err := drainFlushableOps(ctx, ops, false, consume); err != nil {
		return fmt.Errorf("fragment task %s: flush spilled ops: %w", task.ID, err)
	}
	result.NumRows = totalRows
	if err := sink.finalize(ctx, task, result); err != nil {
		return fmt.Errorf("fragment task %s: sink finalize: %w", task.ID, err)
	}
	return nil
}

// heapPressureActive is memory.HeapBackpressureActive behind a package var
// so the linear-collapse regression test can force a pressure trip without
// manipulating GOMEMLIMIT. Production never reassigns it.
var heapPressureActive = memory.HeapBackpressureActive

// rawHeapPressureActive is the unadjusted variant (no reclaimable-bytes
// deduction) for the consumers that free reclaimable bytes themselves:
// the cache-shed valve and the cache admission pause. Same test-seam
// rationale.
var rawHeapPressureActive = memory.RawHeapBackpressureActive

// pageCachePressureActive mirrors it for memory.PageCachePressureActive
// (the §9 refault-rate sensor), same test-seam rationale.
var pageCachePressureActive = memory.PageCachePressureActive

// pageCachePressureActiveBounded mirrors the episode-capped variant
// (refault-sensor v3), same test-seam rationale.
var pageCachePressureActiveBounded = memory.PageCachePressureActiveBounded

// refaultEpisodeCap bounds how long one cache-pressure activation episode
// keeps collapsing decode-ahead on non-edge envelopes. If the collapse
// were shedding the displacement cause (our own held window bytes), the
// refault rate would go quiet well inside this budget (measured ~2s on
// the Q06 self-displacement shape); an episode that outlives it is
// ambient thrash the collapse cannot relieve — v2 semantics locked the
// throttle on for minutes and cost +22-40% SF100 steady suite wall
// (2026-07-22 investigation, PR #259 arms). Override with
// WADJET_REFAULT_EPISODE_CAP (seconds); 0 restores unbounded v2
// semantics (the kill switch).
var refaultEpisodeCap = func() time.Duration {
	const def = 10 * time.Second
	v := os.Getenv("WADJET_REFAULT_EPISODE_CAP")
	if v == "" {
		return def
	}
	secs, err := strconv.ParseFloat(v, 64)
	if err != nil || secs < 0 {
		return def
	}
	return time.Duration(secs * float64(time.Second))
}()

// scanDecodeAheadPressure gates decode-ahead admission beyond the
// delivery cursor: Go-heap tide gauge OR kernel page-cache thrash.
// Discretionary decoded-ahead bytes are the first thing to yield under
// either pressure channel; the cursor group is exempt inside the
// iterator, so this can never stall delivery.
//
// The cache channel is episode-capped on non-edge envelopes (see
// refaultEpisodeCap). Strict/edge envelopes (< 2 GiB GOMEMLIMIT) keep
// the unbounded v2 semantics: the capped repro measured even one extra
// in-flight group as harmful there, and its thrash is EBS-priced —
// there is no cheap-refault regime to spare.
func scanDecodeAheadPressure() bool {
	if heapPressureActive() {
		return true
	}
	if scanDecodeAheadStrictPressure() {
		return pageCachePressureActive()
	}
	return pageCachePressureActiveBounded(refaultEpisodeCap)
}

// scanDecodeAheadStrictPressure reports whether pressure collapse should
// be cursor-only (no group ahead) rather than occupancy-floored 2-deep
// (memo §9.5): edge-class envelopes, mirroring the < 2 GiB GOMEMLIMIT
// classification the GC mode uses — the capped repro measured even one
// extra in-flight group as harmful there. Cached once: GOMEMLIMIT is
// set at process start.
var scanDecodeAheadStrictPressure = sync.OnceValue(func() bool {
	lim := debug.SetMemoryLimit(-1)
	return lim > 0 && lim != math.MaxInt64 && lim < 2<<30
})

// morselMinFragmentBytes is the auto-mode size gate: fragments whose
// EstimatedBytes fall below it stay serial. Parallelism pays only above a
// size threshold — the parallel join build learned this the hard way
// (join.go, size-gated for the same reason). Package-level var so tests can
// lower it.
var morselMinFragmentBytes int64 = 64 << 20

// morselFragmentParallelismCap bounds per-fragment width in legacy
// fixed-width mode (WADJET_MORSEL_YIELD=0), where every consumer holds a
// token for the fragment's lifetime. Work-conserving mode is bounded by the
// token pool instead: active width can never exceed the baseline slot plus
// pool capacity, so consumers past that are unreachable dead weight — and
// the old universal cap of 8 left cores idle on single-fragment workers
// whose producer (the multi-group decode-ahead scanner) was blocked on a
// full window, not saturated (SF100 q08 probe-split, 2026-08-14).
const morselFragmentParallelismCap = 8

// morselFragmentWorkers decides the parallel width for a linear fragment.
// Returns the consumer count k, the width gate metering their ACTIVE
// concurrency, and a release function (call exactly once, after the
// fragment finishes). k == 1 means serial — gate is nil and release a no-op.
//
// Work-conserving mode (default): no tokens are taken here at all. k is the
// policy target and the returned widthGate admits consumers morsel-by-morsel
// — one fragment baseline slot is free, extras claim pool tokens only while
// processing and yield them when the dispenser runs dry, so input-starved
// width never starves the decode that would feed it (§4.2.1).
//
// Legacy mode (WADJET_MORSEL_YIELD=0): the first consumer is free, each
// extra consumer takes one token at fragment start and holds it for the
// fragment's lifetime; under token exhaustion the fragment degrades toward
// serial. gate is nil.
func (e *Executor) morselFragmentWorkers(task distributed.Task, ops []exec.UnaryOperator) (int, *widthGate, func()) {
	noop := func() {}
	policy := e.morselWorkers
	if policy == 0 || policy == 1 || e.cpuTokens == nil {
		return 1, nil, noop
	}
	// Every op must be Cloneable — non-cloneable ops (e.g. the
	// DynamicFilterEmitOp accumulator) keep the fragment serial.
	for _, op := range ops {
		if _, ok := op.(exec.Cloneable); !ok {
			return 1, nil, noop
		}
	}
	target := policy
	if policy < 0 { // auto: size-gated, width up to core count
		if task.EstimatedBytes < morselMinFragmentBytes {
			return 1, nil, noop
		}
		target = runtime.GOMAXPROCS(0)
	}
	if morselWidthYield {
		// The gate admits active consumers against the pool per morsel, so
		// baseline + capacity is the reachable width; more clones can never run.
		//
		// Sizing target from the producer's observed FEED RATE instead (the
		// SF100 2026-08-22 §7.4 second-order item — fragments measured an
		// effective width of 2.88 of 15) was considered and declined: under
		// the work-conserving gate an idle consumer holds nothing, so token
		// demand is per ACTIVE morsel, not per consumer. An over-wide fan
		// costs k−1 cloned op chains and parked goroutines, not pool
		// pressure; what starved decode was the ADMISSION rule, and that is
		// fixed in cpu_tokens.go. Revisit only if clone footprint, not
		// admission, shows up in a profile.
		if bound := 1 + int(e.cpuTokens.Capacity()); target > bound {
			target = bound
		}
	} else if target > morselFragmentParallelismCap {
		target = morselFragmentParallelismCap
	}
	if target <= 1 {
		return 1, nil, noop
	}
	if morselWidthYield {
		return target, newWidthGate(e.cpuTokens), noop
	}
	got := e.cpuTokens.TryAcquire(target - 1)
	if got == 0 {
		return 1, nil, noop
	}
	return 1 + got, nil, func() { e.cpuTokens.Release(got) }
}

// consumeMorsels is one consumer goroutine's loop, shared by the linear and
// breaker parallel paths. process must retire the morsel on every path and
// is called while the consumer holds its width slot; stop (nil-safe) is
// checked after each morsel to end the parallel section early (pressure
// collapse). A nil gate reproduces the fixed-width shape: the consumer's
// concurrency is pre-paid, so it just drains the channel.
//
// The slot dance is the work-conserving core: hold a slot across
// back-to-back morsels (the steady-flow fast path — no per-morsel pool
// traffic), yield it before blocking on an empty channel, re-claim on the
// next morsel.
func consumeMorsels(ctx context.Context, d *morselDispenser, gate *widthGate, process func(m morsel) error, stop func() bool) error {
	slot := slotNone
	defer func() {
		if gate != nil {
			gate.yield(slot)
		}
	}()
	for {
		var m morsel
		var ok bool
		select {
		case m, ok = <-d.ch:
		default:
			if gate != nil && slot != slotNone {
				gate.yield(slot)
				slot = slotNone
			}
			tDry := time.Now()
			select {
			case m, ok = <-d.ch:
			case <-ctx.Done():
				return ctx.Err()
			}
			d.consumerDryNs.Add(time.Since(tDry).Nanoseconds())
		}
		if !ok {
			return nil
		}
		if gate != nil && slot == slotNone {
			// len(d.ch) is the ring's depth BEHIND the morsel in hand: it
			// tells the pool whether feeding this consumer can buy
			// throughput (fed) or whether only a decoder can (dry). See
			// cpu_tokens.go's two admission rules.
			s, err := gate.claim(ctx, len(d.ch) > 0)
			if err != nil {
				m.retire()
				return err
			}
			slot = s
		}
		tProc := time.Now()
		err := process(m)
		d.processNs.Add(time.Since(tProc).Nanoseconds())
		if err != nil {
			return err
		}
		if stop != nil && stop() {
			return nil
		}
	}
}

// runFragmentLinearParallel is the morsel-driven variant of
// runFragmentLinear: a single producer feeds the byte-bounded morsel
// dispenser (which splits row-group-sized decoded batches into ~2048-row
// zero-copy views — see morsel_dispenser.go for the budget and view-safety
// story), consumed by k goroutines that each run a private Clone()d copy of
// the op chain. The fragment sink is shared and internally concurrent: the
// exchange sink locks per PARTITION, the unpartitioned sink appends under
// its lock and double-buffers the chunk encode outside it, and the gather
// sink serializes internally (low-volume reply path). Every sink consumes
// each batch synchronously and retains no reference afterward, so no Sel
// snapshot is needed. The previous sink-WIDE mutex here serialized k
// consumers through the fragment's dominant cost (hash+append+encode) and
// held join/probe fragments +12-27% slower under morsel-auto (SF100
// default-flip gate, 2026-07-07).
//
// Pressure COLLAPSES k (the breaker-path rule, applied here): the linear
// path's transients — dispenser in-flight bytes, join-probe output batches —
// are tracker-invisible by design, so the collapse signal is the process
// heap itself (memory.HeapBackpressureActive, 70% of GOMEMLIMIT). On the
// first trip during parallel consume the consumers stop and the remaining
// input drains serially through the original chain: parallelism is a
// fair-weather optimization, and the pressure story is exactly today's
// SF100-validated serial one. The SF100 2026-07-03 A/B failed on precisely
// this gap — Q17/Q18 grace-join linear fragments blew the worker heap with
// zero collapses because only the breaker path had a collapse rule.
//
// One batch is pushed through the ORIGINAL ops before cloning ("warmup",
// same pattern as exec.Pipeline.runParallel): operator scratch is per-clone,
// but predicate/expression closures are SHARED across clones and resolve
// column indices lazily on first use (exec.ColumnCompare's cachedIdx,
// expr.ColRef's sync.Once). The warmup batch completes those writes while
// the chain is still single-threaded; clones then only read the resolved
// caches.
func (e *Executor) runFragmentLinearParallel(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink fragmentSink, result *distributed.ResultNotification, k int, gate *widthGate) error {
	fragStart := time.Now()
	progress := exec.ProgressReporterFromContext(ctx)
	fp := newFragmentProgress(e.logger, task, e)
	if r, ok := src.(srcAcqReporter); ok {
		fp.srcAcq = r
	}
	src = fp.timeSource(src)
	var totalRows atomic.Int64
	defer func() { fp.finish(totalRows.Load()) }()
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		t0 := time.Now()
		err := sink.consume(ctx, b)
		fp.sinkNs.Add(time.Since(t0).Nanoseconds())
		if err != nil {
			return err
		}
		n := int64(b.ActiveLen())
		totalRows.Add(n)
		if progress != nil {
			progress.AddRows(n)
		}
		fp.addRows(n)
		return nil
	}

	// deliver is every chain's terminal step. Each chain gets its own driver
	// (below) so a suspended fan-out resumes into the chain that produced it.
	deliver := func(ctx context.Context, b *batch.RecordBatch) error {
		if b.ActiveLen() == 0 {
			return nil
		}
		if err := consume(ctx, b); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
		return nil
	}

	// Warmup: one batch through the original chain before any clone exists.
	// The driver is built first: its EnableBoundedOutput must run before
	// Clone() so the clones inherit the opt-in.
	warmup, err := src.Next(ctx)
	if err != nil {
		return fmt.Errorf("fragment task %s: source next: %w", task.ID, err)
	}
	chains := make([][]exec.UnaryOperator, 1, k)
	chains[0] = ops
	drivers := make([]*fragmentDriver, 1, k)
	drivers[0] = newFragmentDriver(ops, deliver)
	if warmup != nil {
		opsNs, err := drivers[0].push(ctx, warmup)
		fp.opsNs.Add(opsNs)
		if err != nil {
			return fmt.Errorf("fragment task %s: %w", task.ID, err)
		}

		// Build the k−1 cloned chains. Clones that return the original
		// instance (Limit-style shared state) are neither re-Init'd nor
		// closed — the original's owner handles both.
		defer func() {
			for _, chain := range chains[1:] {
				for j, op := range chain {
					if op == ops[j] {
						continue
					}
					op.Close()
				}
			}
		}()
		for i := 1; i < k; i++ {
			chain := make([]exec.UnaryOperator, len(ops))
			for j, op := range ops {
				chain[j] = op.(exec.Cloneable).Clone()
			}
			chains = append(chains, chain)
			drivers = append(drivers, newFragmentDriver(chain, deliver))
			for j, cop := range chain {
				if cop == ops[j] {
					continue
				}
				if err := cop.Init(ctx); err != nil {
					return fmt.Errorf("fragment task %s: cloned op init: %w", task.ID, err)
				}
			}
		}

		// Producer: source → byte-bounded dispenser. Decode stays
		// single-threaded inside the source (WSHF chunk / parquet row-group
		// state machines keep their unguarded cursors). The producer runs on
		// its own cancel scope, not the consumer errgroup's: after a
		// pressure collapse it keeps feeding the serial continuation.
		d := newMorselDispenser(k, true)
		prodCtx, prodCancel := context.WithCancel(ctx)
		defer prodCancel()
		prodErr := make(chan error, 1)
		go func() {
			prodErr <- d.run(prodCtx, src)
		}()

		var collapsed atomic.Bool
		g, gctx := errgroup.WithContext(ctx)
		for i := 0; i < k; i++ {
			driver := drivers[i]
			g.Go(func() error {
				return consumeMorsels(gctx, d, gate, func(m morsel) error {
					if err := fp.applyBackpressure(gctx); err != nil {
						m.retire()
						return err
					}
					// The morsel is retired only after the whole chain —
					// including every resumed slice of a suspended fan-out —
					// is done with it: a suspended probe still reads its
					// probe-side columns.
					opsNs, err := driver.push(gctx, m.b)
					fp.opsNs.Add(opsNs)
					m.retire()
					return err
				}, func() bool {
					if collapsed.Load() {
						return true
					}
					if heapPressureActive() {
						collapsed.Store(true)
						return true
					}
					return false
				})
			})
		}
		if err := g.Wait(); err != nil {
			return fmt.Errorf("fragment task %s: %w", task.ID, err)
		}
		if collapsed.Load() {
			e.morselCollapses.Add(1)
			e.logger.Info("morsel pressure collapse: linear fragment continuing serial",
				append([]any{"task_id", task.ID, "stage_id", task.StageID, "k", k}, d.logAttrs()...)...)
		}

		// Serial continuation. After a normal completion the channel is
		// closed and empty — zero iterations. After a collapse this drains
		// the rest of the input through the ORIGINAL chain, at serial memory
		// footprint.
		for m := range d.ch {
			if err := fp.applyBackpressure(ctx); err != nil {
				m.retire()
				return fmt.Errorf("fragment task %s: %w", task.ID, err)
			}
			opsNs, err := drivers[0].push(ctx, m.b)
			fp.opsNs.Add(opsNs)
			m.retire()
			if err != nil {
				return fmt.Errorf("fragment task %s: %w", task.ID, err)
			}
		}
		if err := <-prodErr; err != nil {
			return fmt.Errorf("fragment task %s: %w", task.ID, err)
		}
		// Info (was Debug, invisible in production wlogs — which is why the
		// width plateau went unattributed): with elapsed_ms alongside
		// process_ms / consumer_dry_wait_ms / width_wait_ms this one line
		// names the fragment's pacer. One line per fragment task.
		attrs := append([]any{"task_id", task.ID, "stage_id", task.StageID, "k", k,
			"elapsed_ms", time.Since(fragStart).Milliseconds()}, d.logAttrs()...)
		if gate != nil {
			attrs = append(attrs, gate.logAttrs()...)
		}
		e.logger.Info("morsel parallel fragment done", attrs...)
	}

	// Spilled-partition flush, over EVERY chain. Clone probes share the
	// underlying join's spillState, so the first chain's drain processes all
	// spilled partitions (including rows routed there by other clones) and
	// its terminal cleanup() clears the partition maps — later chains see
	// nothing pending and no-op. Iterating all chains keeps this correct if
	// a future FlushableOperator holds per-instance flush state.
	for _, chain := range chains {
		if err := drainFlushableOps(ctx, chain, false, consume); err != nil {
			return fmt.Errorf("fragment task %s: flush spilled ops: %w", task.ID, err)
		}
	}
	result.NumRows = totalRows.Load()
	if err := sink.finalize(ctx, task, result); err != nil {
		return fmt.Errorf("fragment task %s: sink finalize: %w", task.ID, err)
	}
	return nil
}

// runBreakerConsumeParallel is the morsel-parallel variant of the breaker
// consume phase (source → first breaker): the single producer feeds a
// bounded channel consumed by k goroutines, each running a Clone()d op
// chain into its own CloneSink partial; partials merge into the primary at
// the barrier (the exec.Pipeline.runParallel shape, with two additions the
// never-OOM rules require — memo §4.3):
//
//  1. Clones RESERVE. Each clone sink charges its accumulated state to a
//     tracking-only view of the shared SpillManager, so admission and the
//     primary's spill trigger see the k× partial footprint. Clones never
//     spill — there is no concurrent spill format.
//  2. Pressure COLLAPSES k. When the real SpillManager's ShouldSpillFor
//     trips during parallel consume, the consumers stop, partials merge
//     into the spill-armed primary (the merge transfers the memory
//     accounting), and the remaining input drains serially through the
//     ORIGINAL chain into the primary — whose own partial-drain spill
//     machinery is exactly today's SF100-validated path. Parallelism is a
//     fair-weather optimization; the pressure story is unchanged serial.
//
// Sel is snapshotted before every breaker Consume: breakers retain batches
// (Sort stores them) while upstream Filters reuse per-instance Sel scratch —
// same rule as drainThroughBreaker. Finalize on the primary is called here,
// matching the serial Pipeline.Run contract for the j==0 phase.
func (e *Executor) runBreakerConsumeParallel(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink exec.MergeableSink, k int, gate *widthGate) error {
	fragStart := time.Now()
	fp := newFragmentProgress(e.logger, task, e)
	if r, ok := src.(srcAcqReporter); ok {
		fp.srcAcq = r
	}

	consumeInto := func(ctx context.Context, dst exec.Sink, b *batch.RecordBatch) error {
		if b.Sel != nil {
			selCopy := make([]uint32, len(b.Sel))
			copy(selCopy, b.Sel)
			b.Sel = selCopy
		}
		return dst.Consume(ctx, b)
	}
	// One driver per (chain, sink) pair: a suspended fan-out resumes into the
	// chain that produced it, and its rows must land in that worker's partial.
	newBreakerDriver := func(chain []exec.UnaryOperator, dst exec.Sink) *fragmentDriver {
		fd := newFragmentDriver(chain, func(ctx context.Context, b *batch.RecordBatch) error {
			if b.ActiveLen() == 0 {
				return nil
			}
			if err := consumeInto(ctx, dst, b); err != nil {
				return fmt.Errorf("sink consume: %w", err)
			}
			return nil
		})
		// #277 forensics: a rows-but-no-columns batch downstream of an
		// operator is the panic-then-retry signature seen on Q18's
		// fused-chain breaker at SF100. Name the producer here — the
		// aggregate's own structured error cannot see past the sink
		// boundary. Rate-limited by the progress logger's once-ish
		// cadence being unnecessary: this fires at most once per task
		// before the task errors out.
		fd.d.Inspect(func(oi int, op exec.UnaryOperator, out *batch.RecordBatch) {
			if len(out.Columns) == 0 && out.ActiveLen() > 0 {
				e.logger.Warn("fragment op emitted schemaless batch (#277)",
					"task_id", fp.taskID, "stage_id", fp.stageID,
					"op_index", oi, "op_type", fmt.Sprintf("%T", op),
					"rows", out.ActiveLen(), "len", out.Len, "sel", out.Sel != nil)
			}
		})
		return fd
	}
	flushAll := func(chains [][]exec.UnaryOperator) error {
		for _, chain := range chains {
			if err := drainFlushableOps(ctx, chain, true, func(c context.Context, b *batch.RecordBatch) error {
				return sink.Consume(c, b)
			}); err != nil {
				return fmt.Errorf("flush spilled ops: %w", err)
			}
		}
		return nil
	}

	// Warmup through the ORIGINAL chain into the primary — resolves the
	// lazily-cached column indices in shared predicate/expression closures
	// before any clone exists, and gives the primary sink its schema (the
	// MergeSink schema-inherit path covers the fully-filtered-warmup case).
	warmup, err := src.Next(ctx)
	if err != nil {
		return fmt.Errorf("source next: %w", err)
	}
	if warmup == nil {
		if err := flushAll([][]exec.UnaryOperator{ops}); err != nil {
			return err
		}
		return sink.Finalize(ctx)
	}
	// Built before the clones exist: newFragmentDriver's bounded-output
	// opt-in has to precede Clone() for the clones to inherit it.
	primaryDriver := newBreakerDriver(ops, sink)
	if _, err := primaryDriver.push(ctx, warmup); err != nil {
		return err
	}

	// Cloned chains + clone sinks. Worker 0 keeps the originals and the
	// primary. Clone sinks charge a tracking-only SpillManager view; the
	// deferred Closes release whatever charge a clone still holds (Close is
	// idempotent about it — post-merge the clone's charge is zero for Sort
	// and released-once for HashAggregate).
	var trackingSpill *memory.SpillManager
	if sm := e.spillFor(ctx); sm != nil {
		trackingSpill = sm.TrackingOnlyView()
	}
	chains := make([][]exec.UnaryOperator, 1, k)
	chains[0] = ops
	sinks := make([]exec.Sink, 1, k)
	sinks[0] = sink
	drivers := make([]*fragmentDriver, 1, k)
	drivers[0] = primaryDriver
	defer func() {
		for _, chain := range chains[1:] {
			for j, op := range chain {
				if op == ops[j] {
					continue
				}
				op.Close()
			}
		}
		for _, ws := range sinks[1:] {
			ws.Close()
		}
	}()
	for i := 1; i < k; i++ {
		chain := make([]exec.UnaryOperator, len(ops))
		for j, op := range ops {
			chain[j] = op.(exec.Cloneable).Clone()
		}
		chains = append(chains, chain)
		for j, cop := range chain {
			if cop == ops[j] {
				continue
			}
			if err := cop.Init(ctx); err != nil {
				return fmt.Errorf("cloned op init: %w", err)
			}
		}
		cloned := sink.CloneSink()
		switch cs := cloned.(type) {
		case *exec.HashAggregate:
			cs.Spill = trackingSpill
			// Bound the clone partial: clones cannot spill (tracking-only
			// view), so a high-NDV GROUP BY would otherwise accumulate ~the
			// full key set in EVERY clone — k× serial state, the SF100 Q17
			// worker deaths (morsel-agg-partials-v2.md §3.A). Past the
			// threshold the clone self-drains to canonical partial-state
			// runs that MergeSink hands to the primary. Per-task budget
			// split across 2k partials mirrors the dispenser's
			// budget/(2k) gate shape; 0 (unlimited budget) disables.
			if e.memoryBudget > 0 {
				cs.PartialDrainBytes = e.memoryBudget / int64(2*k)
			}
		case *exec.Sort:
			cs.Spill = trackingSpill
		}
		if err := cloned.Init(ctx); err != nil {
			cloned.Close()
			return fmt.Errorf("cloned sink init: %w", err)
		}
		sinks = append(sinks, cloned)
		drivers = append(drivers, newBreakerDriver(chain, cloned))
	}

	// Producer: source → byte-bounded morsel dispenser. Runs until EOF or
	// cancel; it keeps feeding the serial continuation after a collapse, so
	// its lifetime is the whole function, not the parallel section. Views
	// (split row groups) are safe into a HashAggregate — it copies rows out
	// during Consume — but NOT into a retaining sink (Sort stores the batch
	// and charges b.MemBytes(), which is Sel-blind: each retained view would
	// charge the full parent), so splitting is gated on the sink type.
	_, sinkTakesViews := sink.(*exec.HashAggregate)
	d := newMorselDispenser(k, sinkTakesViews)
	prodCtx, prodCancel := context.WithCancel(ctx)
	defer prodCancel()
	prodErr := make(chan error, 1)
	go func() {
		prodErr <- d.run(prodCtx, src)
	}()

	var collapsed atomic.Bool
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < k; i++ {
		driver := drivers[i]
		// Worker 0 feeds the spill-armed primary, which may answer heap
		// pressure by draining (#326); clones hold tracking-only spill
		// views and always fall through to the pause.
		consumerSink := sinks[i]
		g.Go(func() error {
			return consumeMorsels(gctx, d, gate, func(m morsel) error {
				if err := fp.applyBackpressureSink(gctx, consumerSink); err != nil {
					m.retire()
					return err
				}
				// Retire only once the chain — every resumed slice of a
				// suspended fan-out included — is done reading the morsel.
				_, err := driver.push(gctx, m.b)
				m.retire()
				return err
			}, func() bool {
				if collapsed.Load() {
					return true
				}
				if e.sharedSpill != nil && e.sharedSpill.ShouldSpillFor(memory.SpillCheap) {
					collapsed.Store(true)
					return true
				}
				return false
			})
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Barrier: merge clone partials into the spill-armed primary. MergeSink
	// transfers the memory accounting with the state, so the primary's next
	// ShouldSpillFor sees the full merged footprint.
	for i := 1; i < k; i++ {
		sink.MergeSink(sinks[i].(exec.SinkSource))
	}
	if collapsed.Load() {
		e.morselCollapses.Add(1)
		e.logger.Info("morsel pressure collapse: breaker consume continuing serial",
			"task_id", task.ID,
			"stage_id", task.StageID,
			"k", k)
	}

	// Serial continuation. After a normal completion the channel is closed
	// and empty — zero iterations. After a collapse this drains the rest of
	// the input through the ORIGINAL chain into the primary, whose own
	// spill machinery now governs memory exactly like the serial path.
	for m := range d.ch {
		if err := fp.applyBackpressureSink(ctx, sink); err != nil {
			m.retire()
			return err
		}
		_, err := drivers[0].push(ctx, m.b)
		m.retire()
		if err != nil {
			return err
		}
	}
	if err := <-prodErr; err != nil {
		return err
	}
	// Info for the same reason as the linear path's done-line: the width
	// attribution is useless at Debug in production wlogs.
	doneAttrs := append([]any{"task_id", task.ID, "stage_id", task.StageID, "k", k,
		"elapsed_ms", time.Since(fragStart).Milliseconds()}, d.logAttrs()...)
	if gate != nil {
		doneAttrs = append(doneAttrs, gate.logAttrs()...)
	}
	e.logger.Info("morsel parallel breaker consume done", doneAttrs...)

	// Spilled-partition flush over every chain (shared spillState; the first
	// drain clears it, later chains no-op — same rule as the linear path),
	// then finalize the primary, matching the serial Pipeline.Run contract.
	if err := flushAll(chains); err != nil {
		return err
	}
	return sink.Finalize(ctx)
}

// drainFlushableOps drains any FlushableOperators (Grace Hash Join probes that
// spilled partitions to disk) through the remaining unary chain into consume.
// Mirrors exec.Pipeline.flushSpilledOps.
//
// snapshotSel matters when the downstream consumer retains batch references
// across calls (Sort, HashAggregate consuming into their internal store) and
// upstream ops in the chain reuse a per-instance Sel buffer (exec.Filter and
// the comparison/expression filters do — their selBuf is shared scratch).
// Without the copy, the next iteration's Filter clobbers the Sel of the
// already-stored batch. Sinks that consume each batch in one shot (S3 PUT,
// NATS publish) don't need the copy.
func drainFlushableOps(ctx context.Context, ops []exec.UnaryOperator, snapshotSel bool, consume func(context.Context, *batch.RecordBatch) error) error {
	for opIdx, op := range ops {
		fo, ok := op.(exec.FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			continue
		}
		// The downstream chain gets a bounded-output driver of its own: a
		// second probe below the flushing one fans its spilled-partition
		// output out just as far as it does the streaming input (#317).
		driver := newFragmentDriver(ops[opIdx+1:], func(ctx context.Context, cur *batch.RecordBatch) error {
			if cur.ActiveLen() == 0 {
				return nil
			}
			if snapshotSel && cur.Sel != nil {
				selCopy := make([]uint32, len(cur.Sel))
				copy(selCopy, cur.Sel)
				cur.Sel = selCopy
			}
			return consume(ctx, cur)
		})
		for {
			b, err := fo.NextFlush(ctx)
			if err != nil {
				return err
			}
			if b == nil {
				break
			}
			if _, err := driver.push(ctx, b); err != nil {
				return err
			}
		}
	}
	return nil
}

// fragmentBreaker bundles an exec.SinkSource pipeline-breaker (HashAggregate,
// Sort) with the optional pre- and post- hooks that operator type needs.
// PrependOps run as the tail of the consume-phase unary chain (e.g.
// HashAggregate's derived-input Project). DrainXform runs per-batch during
// the drain phase before postOps (e.g. HashAggregate's AVG-fold). PostFinalize
// runs once between Finalize and the first Next (e.g. Sort.Truncate for the
// top-N optimization).
type fragmentBreaker struct {
	Op           exec.SinkSource
	Label        string
	PrependOps   []exec.UnaryOperator
	DrainXform   func(*batch.RecordBatch) (*batch.RecordBatch, error)
	PostFinalize func()
	Cleanup      func()
}

// runFragmentWithBreakers handles fragments containing one or more pipeline-
// breakers (HashAggregate, Sort). middle is split into N+1 unary segments
// around the breaker indices: seg[0] feeds the first breaker, seg[k] sits
// between breaker[k-1] and breaker[k], and seg[N] sits between the last
// breaker and the terminal sink.
//
// Each breaker is consumed entirely (Phase k consume) before its drain
// stream is piped into the next phase's consume. State stays in memory
// (with operator-level spill) — no S3 hop between phases.
//
// Today the planner emits at most one breaker per fragment; the multi-
// breaker path is exercised by tests and ready for future shapes
// (e.g. final_aggregate + sort fused into one fragment when the planner
// stops emitting them as separate Sort stages).
func (e *Executor) runFragmentWithBreakers(ctx context.Context, task distributed.Task, src exec.Source, middle []distributed.OpSpec, breakerIdxs []int, sink fragmentSink, result *distributed.ResultNotification, emitOps []*exec.DynamicFilterEmitOp) error {
	progress := exec.ProgressReporterFromContext(ctx)
	// Created up front (not at the final phase) so the elapsed clock and
	// src timing cover the breaker-consume phases too.
	fp := newFragmentProgress(e.logger, task, e)
	if r, ok := src.(srcAcqReporter); ok {
		fp.srcAcq = r
	}
	src = fp.timeSource(src)

	breakers := make([]*fragmentBreaker, len(breakerIdxs))
	for i, idx := range breakerIdxs {
		fb, err := e.buildFragmentBreaker(ctx, task, middle[idx])
		if err != nil {
			return fmt.Errorf("fragment task %s: build breaker %d (%s): %w", task.ID, i, middle[idx].Type, err)
		}
		breakers[i] = fb
		defer fb.Cleanup()
	}

	// Phase 0..N-1: feed the previous source (src for j=0, breakers[j-1].Op
	// otherwise) through middle[prevIdx+1:idx] + breakers[j].PrependOps into
	// breakers[j]. Apply breakers[j-1].DrainXform per-batch when draining a
	// previous breaker.
	currentSrc := src
	for j, idx := range breakerIdxs {
		segStart := 0
		if j > 0 {
			segStart = breakerIdxs[j-1] + 1
		}
		segOps, segCleanup, err := e.buildUnaryChain(ctx, task, middle[segStart:idx])
		if err != nil {
			return err
		}
		defer segCleanup()
		// Append breaker prepend-ops AFTER the segment ops; init each so the
		// pipeline can call Execute without further setup. PrependOps come
		// already-init'd from buildFragmentBreaker (HashAggregate's Project),
		// so don't double-init here.
		phaseOps := append([]exec.UnaryOperator{}, segOps...)
		if j == 0 {
			// Emit ops sit at the very head of the pipeline so they observe
			// every source-emitted row before any breaker / segment op.
			phaseOps = prependEmitOps(emitOps, phaseOps)
		}
		phaseOps = append(phaseOps, breakers[j].PrependOps...)

		if j == 0 {
			// Morsel-parallel breaker consume (docs/design/morsel-execution.md
			// §4.3): when the width gate passes and the breaker supports
			// per-worker partials, run the source→first-breaker phase with k
			// cloned chains + CloneSink partials, collapsing back to the
			// serial spill path under memory pressure. Otherwise today's
			// serial Pipeline.
			mergeable, isMergeable := breakers[j].Op.(exec.MergeableSink)
			k, release := 1, func() {}
			var gate *widthGate
			if isMergeable {
				k, gate, release = e.morselFragmentWorkers(task, phaseOps)
			}
			if k > 1 {
				if dn, ok := currentSrc.(producerTokenDonor); ok && gate != nil {
					gate.donor = dn
				}
				err = func() error {
					defer release()
					e.logger.Debug("morsel parallel breaker consume",
						"task_id", task.ID,
						"stage_id", task.StageID,
						"breaker", breakers[j].Label,
						"k", k,
						"estimated_bytes", task.EstimatedBytes,
						"cpu_tokens_in_use", e.cpuTokens.InUse(),
						"cpu_tokens_cap", e.cpuTokens.Capacity())
					return e.runBreakerConsumeParallel(ctx, task, currentSrc, phaseOps, mergeable, k, gate)
				}()
				if err != nil {
					return fmt.Errorf("fragment task %s: %s consume: %w", task.ID, breakers[j].Label, err)
				}
			} else {
				release()
				pipe := &exec.Pipeline{Source: currentSrc, Ops: phaseOps, Sink: breakers[j].Op}
				if err := pipe.Run(ctx); err != nil {
					return fmt.Errorf("fragment task %s: %s consume: %w", task.ID, breakers[j].Label, err)
				}
			}
		} else {
			if err := drainThroughBreaker(ctx, currentSrc, breakers[j-1].DrainXform, phaseOps, breakers[j].Op); err != nil {
				return fmt.Errorf("fragment task %s: %s consume: %w", task.ID, breakers[j].Label, err)
			}
			if err := breakers[j].Op.Finalize(ctx); err != nil {
				return fmt.Errorf("fragment task %s: %s finalize: %w", task.ID, breakers[j].Label, err)
			}
		}
		if breakers[j].PostFinalize != nil {
			breakers[j].PostFinalize()
		}
		currentSrc = breakers[j].Op
	}

	// Final phase: drain the last breaker through middle[lastIdx+1:end] into
	// sink. Apply the last breaker's DrainXform per-batch before postOps.
	lastIdx := breakerIdxs[len(breakerIdxs)-1]
	finalOps, finalCleanup, err := e.buildUnaryChain(ctx, task, middle[lastIdx+1:])
	if err != nil {
		return err
	}
	defer finalCleanup()

	last := breakers[len(breakers)-1]
	var totalRows int64
	defer func() { fp.finish(totalRows) }()
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		t0 := time.Now()
		err := sink.consume(ctx, b)
		fp.sinkNs.Add(time.Since(t0).Nanoseconds())
		if err != nil {
			return err
		}
		n := int64(b.ActiveLen())
		totalRows += n
		if progress != nil {
			progress.AddRows(n)
		}
		fp.addRows(n)
		return nil
	}
	driver := newFragmentDriver(finalOps, func(ctx context.Context, b *batch.RecordBatch) error {
		if b.ActiveLen() == 0 {
			return nil
		}
		if err := consume(ctx, b); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
		return nil
	})
	for {
		b, err := currentSrc.Next(ctx)
		if err != nil {
			return fmt.Errorf("fragment task %s: %s next: %w", task.ID, last.Label, err)
		}
		if b == nil {
			break
		}
		cur := b
		tOps := time.Now()
		if last.DrainXform != nil {
			cur, err = last.DrainXform(cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: %s drain xform: %w", task.ID, last.Label, err)
			}
			if cur == nil {
				fp.opsNs.Add(time.Since(tOps).Nanoseconds())
				continue
			}
		}
		xformNs := time.Since(tOps).Nanoseconds()
		opsNs, err := driver.push(ctx, cur)
		fp.opsNs.Add(xformNs + opsNs)
		if err != nil {
			return fmt.Errorf("fragment task %s: post-breaker exec: %w", task.ID, err)
		}
	}
	// Spilled-partition flush for any HashJoin probes in finalOps.
	if err := drainFlushableOps(ctx, finalOps, false, consume); err != nil {
		return fmt.Errorf("fragment task %s: flush spilled finalOps: %w", task.ID, err)
	}
	result.NumRows = totalRows
	if err := sink.finalize(ctx, task, result); err != nil {
		return fmt.Errorf("fragment task %s: sink finalize: %w", task.ID, err)
	}
	return nil
}

// drainThroughBreaker pulls batches from src, applies an optional per-batch
// transform, pipes them through the unary chain, and pushes each non-empty
// result into sink.Consume. Caller is responsible for sink.Finalize.
//
// The Sel snapshot before sink.Consume mirrors the legacy applyPostFilter
// pattern: exec.Filter (and other Sel-emitting ops) reuses its internal
// selBuf across calls. When the sink is a retaining SinkSource (Sort,
// HashAggregate), it stores the batch reference; without the copy, the
// next iteration's Filter execution would overwrite the same selBuf and
// corrupt the previously-stored batch's Sel — a Q07-style bug documented
// at executor_stage.go:applyPostFilter.
func drainThroughBreaker(ctx context.Context, src exec.Source, xform func(*batch.RecordBatch) (*batch.RecordBatch, error), ops []exec.UnaryOperator, sink exec.Sink) error {
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if b.Sel != nil {
			selCopy := make([]uint32, len(b.Sel))
			copy(selCopy, b.Sel)
			b.Sel = selCopy
		}
		return sink.Consume(ctx, b)
	}
	driver := newFragmentDriver(ops, func(ctx context.Context, b *batch.RecordBatch) error {
		if b.ActiveLen() == 0 {
			return nil
		}
		return consume(ctx, b)
	})
	for {
		// Heap-aware backpressure between batches; mirrors the runFragment*
		// callers and the engine-level Pipeline.Run hook. The breaker sink
		// drains instead of sleeping when it holds the dominant tracked
		// share (#326).
		if err := exec.PauseOrDrainOnHeapBackpressure(ctx, sink); err != nil {
			return err
		}
		b, err := src.Next(ctx)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		cur := b
		if xform != nil {
			cur, err = xform(cur)
			if err != nil {
				return err
			}
			if cur == nil {
				continue
			}
		}
		if _, err := driver.push(ctx, cur); err != nil {
			return err
		}
	}
	// Spilled-partition flush — same reason as runFragmentLinear. The
	// downstream sink here is a retaining SinkSource (Sort/HashAggregate),
	// so snapshotSel=true.
	return drainFlushableOps(ctx, ops, true, consume)
}

// buildFragmentBreaker dispatches per-OpType breaker construction. Returns a
// fully-initialised SinkSource plus the optional pre/drain/post hooks the
// breaker needs.
func (e *Executor) buildFragmentBreaker(ctx context.Context, task distributed.Task, spec distributed.OpSpec) (*fragmentBreaker, error) {
	switch spec.Type {
	case distributed.OpHashAggregate:
		hashAgg, err := e.buildFragmentHashAggregate(ctx, spec)
		if err != nil {
			return nil, err
		}
		fb := &fragmentBreaker{
			Op:      hashAgg,
			Label:   "hash_aggregate",
			Cleanup: func() { hashAgg.Close() },
		}
		// Optional derived-input projection via buildAggInputProjection —
		// required for partial aggregates whose inputs are SQL expressions
		// (e.g. SUM(l_extendedprice*(1-l_discount))). Skipped for merge mode:
		// the partial stage already computed the derived column under OutputCol.
		if spec.BuildProject && !spec.MergeMode {
			project, _, perr := buildAggInputProjection(spec.GroupByCols, spec.Aggregates, nil, spec.GroupByTypes)
			if perr != nil {
				return nil, fmt.Errorf("agg input project: %w", perr)
			}
			if project != nil {
				if perr := project.Init(ctx); perr != nil {
					return nil, fmt.Errorf("project init: %w", perr)
				}
				fb.PrependOps = append(fb.PrependOps, project)
			}
		}
		// Partial-state fold, FINAL stage only: __avg_sum#X / __avg_count#X
		// collapse into the single AVG column X, __var_state#kind#X (the
		// STDDEV/VARIANCE (count, mean, M2) triple) into X, and
		// __covar_state#kind#X (the CORR/COVAR sextuple) into X. Every one
		// of those synthetics has to survive intermediate merge_aggregate
		// stages intact, which is what spec.FoldAvg gates. Wrap the
		// slice-taking helpers as one per-batch transform.
		if spec.FoldAvg {
			fb.DrainXform = func(b *batch.RecordBatch) (*batch.RecordBatch, error) {
				folded, ferr := applyAvgFold([]*batch.RecordBatch{b})
				if ferr != nil {
					return nil, ferr
				}
				folded, ferr = applyVarFold(folded)
				if ferr != nil {
					return nil, ferr
				}
				folded, ferr = applyCovarFold(folded)
				if ferr != nil {
					return nil, ferr
				}
				if len(folded) == 0 {
					return nil, nil
				}
				return folded[0], nil
			}
		}
		return fb, nil

	case distributed.OpSort:
		sorter, err := e.buildFragmentSort(ctx, spec)
		if err != nil {
			return nil, err
		}
		if err := sorter.Init(ctx); err != nil {
			sorter.Close()
			return nil, fmt.Errorf("sort init: %w", err)
		}
		fb := &fragmentBreaker{
			Op:      sorter,
			Label:   "sort",
			Cleanup: func() { sorter.Close() },
		}
		// Truncate to the top-N rows after Finalize. Truncate runs ONCE
		// between Finalize and the first Next so the materialized output
		// is bounded.
		if spec.SortLimit > 0 {
			limit := spec.SortLimit
			fb.PostFinalize = func() { sorter.Truncate(limit) }
		}
		return fb, nil

	case distributed.OpWindow:
		win, err := e.buildFragmentWindow(ctx, spec)
		if err != nil {
			return nil, err
		}
		if err := win.Init(ctx); err != nil {
			win.Close()
			return nil, fmt.Errorf("window init: %w", err)
		}
		return &fragmentBreaker{
			Op:      win,
			Label:   "window",
			Cleanup: func() { win.Close() },
		}, nil

	case distributed.OpSortMergeJoin:
		return e.buildFragmentSortMergeJoin(ctx, task, spec)

	default:
		return nil, fmt.Errorf("unsupported breaker op %q", spec.Type)
	}
}

// smjBreakerOp wraps SortMergeJoin for the fragment breaker path: the
// consume-phase Pipeline.Run finalizes its sink itself, and the SMJ build
// side drains in a concurrent goroutine — Finalize must wait for the build
// barrier before starting the merge.
type smjBreakerOp struct {
	*exec.SortMergeJoin
	barrier  <-chan struct{}
	buildErr *error
}

func (s *smjBreakerOp) Finalize(ctx context.Context) error {
	<-s.barrier
	if *s.buildErr != nil {
		return *s.buildErr
	}
	return s.SortMergeJoin.Finalize(ctx)
}

// buildFragmentSortMergeJoin constructs the sort-merge join breaker: the
// build side's co-partitioned shuffle files (spec.BuildFiles) drain into the
// operator in a goroutine — overlapping the probe-side consume phase, since
// the two side buffers are independent — and the probe side arrives via the
// breaker's Consume. Inner-only in v1; the planner gate guarantees it.
func (e *Executor) buildFragmentSortMergeJoin(ctx context.Context, task distributed.Task, spec distributed.OpSpec) (*fragmentBreaker, error) {
	if len(spec.LeftKeys) == 0 || len(spec.RightKeys) == 0 {
		return nil, fmt.Errorf("sort_merge_join: LeftKeys and RightKeys required")
	}
	if jt := mapJoinTypeString(spec.JoinType); jt != exec.InnerJoin {
		return nil, fmt.Errorf("sort_merge_join: join type %q not supported (v1 is inner-only)", spec.JoinType)
	}

	j := exec.NewSortMergeJoin(spec.LeftKeys, spec.RightKeys)
	j.BuildTableAlias = spec.BuildAlias
	j.QualifyAllBuildCols = spec.QualifyAllBuildCols
	j.BuildColOrigins = spec.BuildColOrigins
	if len(spec.OutputColumns) > 0 {
		filter := make(map[string]bool, len(spec.OutputColumns))
		for _, c := range spec.OutputColumns {
			filter[c] = true
		}
		j.OutputFilter = filter
	}
	if sm := e.spillFor(ctx); sm != nil {
		j.Spill = sm
	}

	// Build source: this task's partition of the build-side exchange. Empty
	// BuildFiles is the legitimate "upstream produced nothing for this
	// partition" case — Build over an empty source completes immediately and
	// the inner join emits nothing.
	var buildSrc exec.Source
	if len(spec.BuildFiles) == 0 {
		buildSrc = exec.NewBatchSource(nil)
	} else {
		bucket := spec.BuildBucket
		if bucket == "" {
			bucket = task.DataBucket
		}
		if bucket == "" {
			bucket = task.ResultBucket
		}
		src, err := e.sourceForAlias(task.QueryID, bucket, spec.BuildAlias, spec.BuildFiles)
		if err != nil {
			return nil, fmt.Errorf("sort_merge_join build source: %w", err)
		}
		buildSrc = src
	}

	buildDone := make(chan struct{})
	var buildErr error
	go func() {
		defer close(buildDone)
		if err := j.Build(ctx, buildSrc); err != nil {
			buildErr = fmt.Errorf("sort_merge_join build side: %w", err)
		}
	}()

	return &fragmentBreaker{
		Op:    &smjBreakerOp{SortMergeJoin: j, barrier: buildDone, buildErr: &buildErr},
		Label: "sort_merge_join",
		Cleanup: func() {
			// The build goroutine owns join state until the barrier closes;
			// closing under it would release tracker bytes it is still adding.
			<-buildDone
			j.Close()
		},
	}, nil
}

// buildFragmentSort constructs an exec.Sort from an OpSpec. Converts each
// SortKeySpec.Desc into exec.Descending and carries its null placement, which
// defaults to what the SQL means for the direction rather than to Go's zero
// value — the zero value put NULLs first in ascending order (#330). Spill is
// wired from the executor's shared spill manager when present.
func (e *Executor) buildFragmentSort(ctx context.Context, spec distributed.OpSpec) (*exec.Sort, error) {
	if len(spec.SortKeySpecs) == 0 {
		return nil, fmt.Errorf("sort: SortKeySpecs required")
	}
	keys := make([]exec.SortKey, len(spec.SortKeySpecs))
	for i, k := range spec.SortKeySpecs {
		order := exec.Ascending
		if k.Desc {
			order = exec.Descending
		}
		keys[i] = exec.SortKey{Column: k.Column, Order: order, NullsLast: k.PlaceNullsLast()}
	}
	sorter := exec.NewSort(keys)
	if sm := e.spillFor(ctx); sm != nil {
		sorter.Spill = sm
	}
	return sorter, nil
}

// buildFragmentWindow constructs an exec.Window from an OpSpec. Every value
// it needs was resolved by the planner (distributed.WindowColSpec), so this
// is a translation: an unknown function name or an empty column list is a
// coordinator bug and fails the task rather than computing something.
//
// A nil OutputType means the coordinator declared none — an older
// coordinator, or a value function whose input column it declined to type. The
// conservative float64 the planner itself falls back to is what the operator
// then gets, and exec.Window re-types the five value functions from the
// vector it actually reads (#345). Spill is wired from the executor's shared
// manager: a window's peak is its largest partition, and that is exactly the
// bound the spill path exists to survive.
func (e *Executor) buildFragmentWindow(ctx context.Context, spec distributed.OpSpec) (*exec.Window, error) {
	if len(spec.WindowCols) == 0 {
		return nil, fmt.Errorf("window: WindowCols required")
	}
	cols := make([]exec.WindowColumn, len(spec.WindowCols))
	for i, wc := range spec.WindowCols {
		fn, ok := exec.ParseWindowFunc(wc.Func)
		if !ok {
			return nil, fmt.Errorf("window: unsupported function %q for output column %q", wc.Func, wc.OutputCol)
		}
		var orderBy []exec.SortKey
		for _, k := range wc.OrderBy {
			order := exec.Ascending
			if k.Desc {
				order = exec.Descending
			}
			orderBy = append(orderBy, exec.SortKey{Column: k.Column, Order: order, NullsLast: k.PlaceNullsLast()})
		}
		outType := parquet.TypeFloat64
		if wc.OutputType != nil {
			outType = parquet.TypeID(*wc.OutputType)
		}
		cols[i] = exec.WindowColumn{
			Func:           fn,
			InputCol:       wc.InputCol,
			OutputCol:      wc.OutputCol,
			OutputType:     outType,
			PartitionBy:    append([]string(nil), wc.PartitionBy...),
			OrderBy:        orderBy,
			LagLeadOffset:  wc.LagLeadOffset,
			LagLeadDefault: wc.LagLeadDefault,
			NtileBuckets:   wc.NtileBuckets,
			NthValueN:      wc.NthValueN,
		}
		if wc.Frame != nil {
			cols[i].Frame = &exec.WindowFrameSpec{
				Mode:  wc.Frame.Mode,
				Start: exec.WindowBound{Type: wc.Frame.Start.Type, Offset: wc.Frame.Start.Offset},
				End:   exec.WindowBound{Type: wc.Frame.End.Type, Offset: wc.Frame.End.Offset},
			}
		}
	}
	win := exec.NewWindow(cols)
	if sm := e.spillFor(ctx); sm != nil {
		win.Spill = sm
	}
	return win, nil
}

// buildFragmentHashAggregate constructs an exec.HashAggregate from an OpSpec.
// In merge mode the spec's Aggregates are rewritten so InputCol = OutputCol
// (the partial-output column) and COUNT becomes SUM (counting partial rows
// re-counts groups, not source rows).
func (e *Executor) buildFragmentHashAggregate(ctx context.Context, spec distributed.OpSpec) (*exec.HashAggregate, error) {
	if !spec.GroupByAll && len(spec.Aggregates) == 0 && len(spec.GroupByCols) == 0 {
		return nil, fmt.Errorf("hash_aggregate: at least one of GroupByCols, Aggregates, or GroupByAll is required")
	}
	aggCols := make([]exec.AggColumn, len(spec.Aggregates))
	for i, a := range spec.Aggregates {
		inputCol := a.InputCol
		fn, known := parseAggFuncString(a.Func)
		if !known {
			// Refusing is the point: the old default was AggSum, so an
			// aggregate this worker had no case for answered with the sum
			// of its input and nothing said a word (#353).
			return nil, fmt.Errorf("hash_aggregate: unknown aggregate function %q for output column %q", a.Func, a.OutputCol)
		}
		if spec.MergeMode {
			if a.OutputCol != "" {
				inputCol = a.OutputCol
			}
			if fn == exec.AggCount {
				fn = exec.AggSum
			}
			// The partial emitted an encoded (count, mean, M2) triple per
			// group; merging reads those triples and combines them pairwise
			// instead of accumulating raw values (which is what AggVarState
			// would do to a 48-char hex string: nothing).
			if fn == exec.AggVarState {
				fn = exec.AggVarStateMerge
			}
			if fn == exec.AggCovarState {
				fn = exec.AggCovarStateMerge
			}
		}
		aggCols[i] = exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			InputCol2:  a.InputCol2,
			Separator:  a.Separator,
			Percentile: a.Percentile,
			OutputCol:  a.OutputCol,
			OutputType: aggSpecOutputType(a),
		}
	}
	hashAgg := exec.NewHashAggregate(spec.GroupByCols, aggCols)
	hashAgg.GroupByAll = spec.GroupByAll
	if sm := e.spillFor(ctx); sm != nil {
		hashAgg.Spill = sm
	}
	return hashAgg, nil
}

// buildUnaryChain builds and inits each unary op in specs. Returns a single
// cleanup that closes every op (and any owned resources like build-side hash
// tables) in reverse order.
//
// The expensive specs are hash-join probes, whose builds read independent
// build-side inputs. We build them CONCURRENTLY (bounded) rather than
// sequentially: in a multi-way join fragment each join's hashtable build is
// independent, so overlapping them cuts build latency (#23). This mirrors what
// the single-process planner already does (build goroutine overlapped with
// probe-side preparation in physical.buildJoin). It's byte-identical: each
// hashtable is still built by a single goroutine exactly as before — only the
// builds of *different* joins overlap. The shared spill manager + tracker
// already support concurrent Build via cooperative spill, and the broadcast
// cache dedups concurrent Acquire. Op order and cleanup order are preserved by
// indexing into a per-spec result slice.
func (e *Executor) buildUnaryChain(ctx context.Context, task distributed.Task, specs []distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	type built struct {
		ops     []exec.UnaryOperator
		cleanup func()
	}
	results := make([]built, len(specs))

	switch len(specs) {
	case 0:
		return nil, func() {}, nil
	case 1:
		opChain, opCleanup, err := e.buildFragmentUnary(ctx, task, specs[0])
		if err != nil {
			return nil, nil, fmt.Errorf("fragment task %s: unary op 0 (%s): %w", task.ID, specs[0].Type, err)
		}
		results[0] = built{ops: opChain, cleanup: opCleanup}
	default:
		g, gctx := errgroup.WithContext(ctx)
		if e.cpuTokens != nil {
			// Burst-section token adoption (morsel-execution.md §4.2): this
			// concurrent op-build burst (broadcast build decode + hash-index
			// construction is CPU-heavy) draws from the same worker-wide
			// token pool as the morsel consumers, so Σ(compute goroutines)
			// across concurrent tasks stays within the core budget. The
			// first build slot is free, mirroring the consumer rule; token
			// scarcity narrows the burst, never blocks it.
			want := len(specs)
			if m := runtime.GOMAXPROCS(0); m < want {
				want = m
			}
			extra := e.cpuTokens.TryAcquire(want - 1)
			defer e.cpuTokens.Release(extra)
			g.SetLimit(1 + extra)
		} else if limit := runtime.GOMAXPROCS(0); limit < len(specs) {
			g.SetLimit(limit)
		}
		for i := range specs {
			i := i
			g.Go(func() error {
				opChain, opCleanup, err := e.buildFragmentUnary(gctx, task, specs[i])
				if err != nil {
					return fmt.Errorf("fragment task %s: unary op %d (%s): %w", task.ID, i, specs[i].Type, err)
				}
				results[i] = built{ops: opChain, cleanup: opCleanup}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			// Release any builds that completed before the failure (reverse order).
			for j := len(results) - 1; j >= 0; j-- {
				if results[j].cleanup != nil {
					results[j].cleanup()
				}
			}
			return nil, nil, err
		}
	}

	// Assemble in spec order; Init in order; accumulate cleanups for reverse-order close.
	var ops []exec.UnaryOperator
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	for i := range results {
		if results[i].cleanup != nil {
			cleanups = append(cleanups, results[i].cleanup)
		}
		for _, op := range results[i].ops {
			if err := op.Init(ctx); err != nil {
				cleanup()
				return nil, nil, fmt.Errorf("fragment task %s: unary op %d init: %w", task.ID, i, err)
			}
		}
		ops = append(ops, results[i].ops...)
	}

	// A LIMIT with no ORDER BY, pushed down by the planner (#311). Bounding
	// the task's own output lets the scan stop pulling batches instead of
	// reading its whole input for rows the coordinator will discard: opening
	// a 15M-row table read all of it for 501 rows. Safe per task because the
	// planner only sets RowLimit when nothing between the scan and the LIMIT
	// changes cardinality (physical.limitPushdownSafe), and because a bare
	// LIMIT does not specify which rows it returns — the coordinator trims
	// the union of tasks to the real limit.
	if task.RowLimit > 0 {
		lim := exec.NewLimit(int64(task.RowLimit), 0)
		if err := lim.Init(ctx); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("fragment task %s: row limit init: %w", task.ID, err)
		}
		ops = append(ops, lim)
	}
	return ops, cleanup, nil
}

func isFragmentSourceOp(t distributed.OpType) bool {
	return t == distributed.OpScan || t == distributed.OpShuffleSource
}

func isFragmentSinkOp(t distributed.OpType) bool {
	return t == distributed.OpExchangeSender || t == distributed.OpUnpartitionedSink || t == distributed.OpGatherSink
}

func isFragmentBreakerOp(t distributed.OpType) bool {
	return t == distributed.OpHashAggregate || t == distributed.OpSort ||
		t == distributed.OpSortMergeJoin || t == distributed.OpWindow
}

// emptyFragmentSource is the zero-input source for a scalar-aggregate
// fragment over an empty upstream (#292): the aggregate must still
// finalize and emit its identity row, so the pipeline needs a source that
// is simply exhausted — not an error, and not a skipped run.
type emptyFragmentSource struct{}

func (emptyFragmentSource) Init(context.Context) error                       { return nil }
func (emptyFragmentSource) Next(context.Context) (*batch.RecordBatch, error) { return nil, nil }
func (emptyFragmentSource) Close() error                                     { return nil }

func (e *Executor) buildFragmentSource(task distributed.Task, spec distributed.OpSpec) (exec.Source, error) {
	bucket := spec.InputBucket
	if bucket == "" {
		bucket = task.DataBucket
	}
	if bucket == "" {
		bucket = task.ResultBucket
	}
	// Eager consumer dispatch: an alias registered in task.EagerInputs is
	// fed by producer-task manifests instead of a frozen file list
	// (docs/design/eager-consumer-dispatch.md §3.2). InputFiles is empty
	// by construction for these aliases — which is also the guard: a spec
	// carrying REAL files must never be rerouted to a feed, even when its
	// alias string collides with an eager alias (a fused chain's op reusing
	// the primary build's alias hijacked the primary's manifest feed and
	// turned the chained semi-join into a pass-through — Q18 §14.2).
	if eager, ok := eagerInputFor(task, spec.InputAlias, spec.InputFiles); ok {
		return newManifestStreamSource(e, task.QueryID, bucket, eager), nil
	}
	if len(spec.InputFiles) == 0 {
		return nil, fmt.Errorf("source %q: empty InputFiles", spec.Type)
	}
	src, err := e.sourceForAliasWithProjection(task.QueryID, bucket, spec.InputAlias, spec.InputFiles, spec.Columns)
	if err != nil {
		return nil, err
	}
	// Row-group sharding for OpScan over a single compacted parquet file.
	if spec.ScanShardCount > 1 {
		if cs, ok := src.(*cachedFileStreamSource); ok {
			cs.SetShard(spec.ScanShardIndex, spec.ScanShardCount)
		} else {
			e.logger.Warn("fragment source: shard params set but source is not cachedFileStreamSource",
				"alias", spec.InputAlias, "shard_idx", spec.ScanShardIndex, "shard_count", spec.ScanShardCount)
		}
	}
	// Dynamic-filter pushdown (Trino-style semi-join). When the OpScan spec
	// carries DynamicFilters, materialize each into a BloomScanFilter and
	// attach to the source so row groups get pruned before any S3 fetch.
	if len(spec.DynamicFilters) > 0 {
		if cs, ok := src.(*cachedFileStreamSource); ok {
			ranges, blooms, err := e.materializeDynamicFilters(spec.DynamicFilters)
			if err != nil {
				e.logger.Warn("fragment source: dynamic-filter materialize failed; proceeding without filter",
					"alias", spec.InputAlias, "error", err)
			} else if len(ranges) > 0 || len(blooms) > 0 {
				cs.SetDynamicFilters(ranges, blooms)
			}
		}
	}
	return src, nil
}

func (e *Executor) buildFragmentUnary(ctx context.Context, task distributed.Task, spec distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	switch spec.Type {
	case distributed.OpFilter:
		ops, _, err := compileFilterExprs(spec.Predicates)
		if err != nil {
			return nil, nil, err
		}
		return ops, nil, nil
	case distributed.OpColumnPrune:
		// Scan-output projection (Q13 class): the scan READS its filter
		// columns but must not SHIP them — downstream declared exactly
		// which columns it consumes. Zero-copy column drop.
		return []exec.UnaryOperator{exec.NewColumnPrune(spec.OutputColumns)}, nil, nil

	case distributed.OpHashJoinProbe, distributed.OpBroadcastProbe:
		return e.buildFragmentJoinProbe(ctx, task, spec)

	case distributed.OpProject:
		proj, err := buildSelectProjection(spec.Projections)
		if err != nil {
			return nil, nil, err
		}
		return []exec.UnaryOperator{proj}, nil, nil

	case distributed.OpSetOpEmit:
		// INTERSECT/EXCEPT emit (#346): the counting aggregate's drain rows
		// carry per-arm multiplicities; this op applies the operation's
		// count rule and drops the count columns.
		emit, err := exec.NewSetOpEmit(spec.SetOp, spec.SetOpAll, spec.SetOpLeftCol, spec.SetOpRightCol)
		if err != nil {
			return nil, nil, err
		}
		return []exec.UnaryOperator{emit}, nil, nil

	default:
		return nil, nil, fmt.Errorf("unsupported unary op %q", spec.Type)
	}
}

// eagerInputFor resolves an alias to its manifest feed ONLY when the spec
// carries no frozen files. Eager-fed aliases have empty file lists by
// construction (the file set streams in as manifests), so non-empty files
// are proof the spec was built from real completed outputs — an alias
// string that happens to match an EagerInputs key (fused-chain ops reuse
// build alias names) must not reroute those reads to a feed.
func eagerInputFor(task distributed.Task, alias string, frozenFiles []string) (distributed.EagerInput, bool) {
	if len(frozenFiles) > 0 {
		return distributed.EagerInput{}, false
	}
	ei, ok := task.EagerInputs[alias]
	return ei, ok
}

func (e *Executor) buildFragmentJoinProbe(ctx context.Context, task distributed.Task, spec distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	joinType := mapJoinTypeString(spec.JoinType)
	// A CROSS join has no ON clause and therefore no keys: `FROM a, b WHERE
	// a.x < b.y` is legal SQL that the single-process path runs as a
	// Cartesian product with the predicate above it. An OUTER join whose ON
	// held no bare-column equality is keyless too — its JoinFilter residual
	// does all of the matching (#358). Every other join type is an equi-join
	// here and its keys are mandatory.
	keylessOuterResidual := spec.JoinFilter != "" &&
		(joinType == exec.LeftJoin || joinType == exec.RightJoin || joinType == exec.FullOuterJoin)
	if joinType != exec.CrossJoin && !keylessOuterResidual &&
		(len(spec.LeftKeys) == 0 || len(spec.RightKeys) == 0) {
		return nil, nil, fmt.Errorf("hash_join_probe: LeftKeys and RightKeys required")
	}
	// Eager consumer dispatch: a build alias registered in task.EagerInputs
	// is fed by producer-task manifests — its BuildFiles is empty BY
	// CONSTRUCTION, so the empty-build short-circuit below must not fire
	// (it silently emitted zero join rows, the build-side twin of the
	// executeFragment empty-InputFiles bug the C1 e2e caught). The empty-
	// by-construction property is also the eligibility guard (eagerInputFor):
	// a chained op carrying REAL BuildFiles whose alias collides with the
	// primary build's eager alias must use its files — routing it to the
	// primary's manifest feed built the chained semi-join over the ENTIRE
	// primary build side and made it a pass-through (Q18 §14.2: 70 → 100
	// rows via LIMIT-masked value corruption).
	eagerBuild, buildIsEager := eagerInputFor(task, spec.BuildAlias, spec.BuildFiles)
	if len(spec.BuildFiles) == 0 && !buildIsEager && !preservesProbeSide(joinType) {
		// Empty build → inner/semi/right/cross joins emit no rows at all.
		// Returning an op chain that drops every row keeps the pipeline
		// well-formed without special-casing the runner.
		//
		// LEFT, FULL and ANTI are NOT in that set: they preserve probe rows
		// the build never matched, which is every probe row when the build is
		// empty. Dropping them turned a LEFT JOIN into an inner one on the
		// DAG — `customer LEFT JOIN orders ON ... AND o_orderkey < 0`
		// answered 0 where the truth is 1500 (#348). Those types fall through
		// and build an EMPTY hash table instead, whose declared BuildSchema
		// gives the join the NULL-padded build columns it owes.
		return []exec.UnaryOperator{exec.NewFilter(func(_ *batch.RecordBatch, _ int) bool {
			return false
		})}, nil, nil
	}
	bucket := spec.BuildBucket
	if bucket == "" {
		bucket = task.DataBucket
	}
	if bucket == "" {
		bucket = task.ResultBucket
	}
	// Build the HashJoin once, then share it across same-worker probe tasks
	// for the same broadcast. HashJoinProbe is designed for concurrent use:
	// each Probe() call returns its own scratch buffers, and lazy probeKeyIdx
	// resolution is atomic.Bool + mutex guarded. Build()-completed HashJoin
	// state (intIndex, arena, buildBatches) is read-only.
	//
	// Hash-shuffle probes don't share a build (each task probes its own
	// partition) so they take the direct path.
	buildHJ := func() (*exec.HashJoin, error) {
		var src exec.Source
		switch {
		case buildIsEager:
			// Manifest-fed build: HashJoin.Build drains the source, which
			// blocks between manifests until every producer candidate
			// resolves — the build completes exactly when the producer
			// side's files for this task's partition range all arrived.
			src = newManifestStreamSource(e, task.QueryID, bucket, eagerBuild)
		case len(spec.BuildFiles) == 0:
			// A probe-preserving join over an empty build (the branch above
			// let it through): there is nothing to read, but the join still
			// runs so every probe row survives with NULL build columns.
			src = emptyFragmentSource{}
		default:
			var err error
			src, err = e.sourceForAlias(task.QueryID, bucket, spec.BuildAlias, spec.BuildFiles)
			if err != nil {
				return nil, fmt.Errorf("build source: %w", err)
			}
		}
		if err := src.Init(ctx); err != nil {
			src.Close()
			return nil, fmt.Errorf("build source init: %w", err)
		}
		defer src.Close()
		if len(spec.BuildFilterExprs) > 0 {
			// Exchange subsumption dedup: this build was rewired from a
			// dropped filtered exchange to a subsuming raw one — apply the
			// dropped scan's filter (or its computed flag) to the build rows
			// before insertion. Semantically identical to the dropped scan's
			// own filter.
			fops, _, err := compileFilterExprs(spec.BuildFilterExprs)
			if err != nil {
				return nil, fmt.Errorf("build filter: %w", err)
			}
			src = &filteredSource{Source: src, ops: fops}
		}

		hj := exec.NewHashJoin(joinType, spec.LeftKeys, spec.RightKeys)
		hj.BuildTableAlias = spec.BuildAlias
		hj.QualifyAllBuildCols = spec.QualifyAllBuildCols
		hj.BuildColOrigins = spec.BuildColOrigins
		// Plan-declared side schemas, read only when a side is empty.
		hj.BuildSchemaHint = execColumns(spec.BuildSchema)
		hj.ProbeSchemaHint = execColumns(spec.ProbeSchema)
		if spec.BuildRowHint > 0 {
			hj.BuildRowHint = spec.BuildRowHint
		}
		hj.SemiAntiKeyOnly = spec.SemiAntiKeyOnly
		switch {
		case spec.JoinFilter != "" &&
			(joinType == exec.LeftJoin || joinType == exec.RightJoin || joinType == exec.FullOuterJoin):
			// Outer-join ON residual (#358): evaluated on the combined row
			// per key candidate, with the unmatched semantics that keeps a
			// residual-failed probe row NULL-padded. NOT SemiAntiFilter —
			// that one only runs on the semi/anti probe path.
			hj.Residual = physical.BuildJoinResidualFilter(spec.JoinFilter, spec.BuildAlias)
			if hj.Residual == nil {
				return nil, fmt.Errorf("hash_join_probe: join residual %q is not evaluable", spec.JoinFilter)
			}
		case spec.JoinFilter != "":
			hj.SemiAntiFilter = physical.BuildSemiAntiFilter(spec.JoinFilter)
			// Filtered semi/anti builds store only keys + filter columns —
			// the worker has no post-build prune at all, so without this a
			// broadcast lineitem build retains every scanned column.
			if hj.JoinType == exec.SemiJoin || hj.JoinType == exec.AntiJoin {
				hj.BuildStoreCols = physical.SemiAntiBuildStoreCols(spec.RightKeys, spec.JoinFilter)
				// Distinct-pair fast path for `probe.col <> build.col`
				// filters (exec/join_semianti_ne.go): the build collapses
				// to key -> ≤2 distinct values, no batch storage.
				if pc, bc, ok := physical.ParseSemiAntiNE(spec.JoinFilter); ok {
					hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = pc, bc
				}
			}
		}
		if e.sharedSpill != nil {
			// Wiring a Spill manager + tracker makes Build partition on arrival
			// unconditionally (O(partition) spill). There is no at-entry
			// pressure heuristic: the stale 30% snapshot used to leave shuffle
			// builds on the flat path, which could then need an O(total)
			// reactive repartition under pressure (the Q17/Q18 mc=4 killer).
			hj.Spill = e.spillFor(ctx)
			hj.MemTracker = e.sharedTracker
		}
		if err := hj.Build(ctx, src); err != nil {
			return nil, fmt.Errorf("building hash table: %w", err)
		}
		if hj.NEActive() {
			e.logger.Info("semi_anti_ne: distinct-pair build active",
				"join_type", spec.JoinType, "build_alias", spec.BuildAlias,
				"probe_col", hj.SemiAntiNEProbeCol, "build_col", hj.SemiAntiNEBuildCol)
		}
		if hj.FixKeyAssignment() {
			slog.Warn("join key repair fired at runtime — plan-time side assignment missed a pair",
				"left_keys", hj.LeftKeys, "right_keys", hj.RightKeys)
		}
		return hj, nil
	}

	var (
		hj      *exec.HashJoin
		cleanup func()
	)
	if spec.Type == distributed.OpBroadcastProbe && e.broadcastCache != nil {
		key := computeBroadcastJoinKey(task.QueryID, spec)
		built, release, err := e.broadcastCache.Acquire(ctx, key, task.QueryID, buildHJ)
		if err != nil {
			return nil, nil, err
		}
		hj = built
		cleanup = release
	} else {
		built, err := buildHJ()
		if err != nil {
			return nil, nil, err
		}
		hj = built
		cleanup = func() { hj.Close() }
	}

	probe := hj.Probe()
	probe.LateMaterialize = spec.LateMaterialize
	if len(spec.OutputColumns) > 0 {
		filter := make(map[string]bool, len(spec.OutputColumns))
		for _, c := range spec.OutputColumns {
			filter[c] = true
		}
		probe.OutputFilter = filter
	}
	return []exec.UnaryOperator{probe}, cleanup, nil
}

// fragmentSink is the internal interface every fragment-sink kind implements:
// stream batches in via consume, finalize once at end, then upload / publish.
// Distinct from exec.Sink because Finalize here also handles the sink-specific
// upload + KV cache + LocalStageCache adoption.
type fragmentSink interface {
	consume(ctx context.Context, b *batch.RecordBatch) error
	finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error
	close()
}

func (e *Executor) openFragmentSink(task distributed.Task, spec distributed.OpSpec) (fragmentSink, error) {
	switch spec.Type {
	case distributed.OpExchangeSender:
		if spec.NumPartitions <= 0 {
			return nil, fmt.Errorf("exchange_sender: NumPartitions must be > 0")
		}
		spillDir := stageSpillDir(e, task)
		if err := os.MkdirAll(spillDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating spill dir: %w", err)
		}
		return &fragmentExchangeSink{
			executor:    e,
			spillDir:    spillDir,
			shuffleKeys: spec.ShuffleKeys,
			numParts:    spec.NumPartitions,
		}, nil

	case distributed.OpUnpartitionedSink:
		spillDir := stageSpillDir(e, task)
		if err := os.MkdirAll(spillDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating spill dir: %w", err)
		}
		s := newUnpartitionedStageSink(spillDir, task.ID)
		if err := s.Init(context.Background()); err != nil {
			return nil, fmt.Errorf("unpartitioned sink init: %w", err)
		}
		return &fragmentUnpartitionedSink{executor: e, sink: s, spillDir: spillDir}, nil

	case distributed.OpGatherSink:
		if spec.ReplySubject == "" {
			return nil, fmt.Errorf("gather_sink: ReplySubject required")
		}
		if e.nc == nil {
			return nil, fmt.Errorf("gather_sink: NATS connection required")
		}
		// Schema is captured on first Consume so we don't need to plumb it
		// through openFragmentSink. nil here is the same shape gather uses
		// in writeStageOutput.
		s := newGatherReplySink(e.nc, spec.ReplySubject, "", nil).withDataPlane(e.dpClient)
		return &fragmentGatherSink{sink: s}, nil

	default:
		return nil, fmt.Errorf("unsupported sink op %q", spec.Type)
	}
}

func stageSpillDir(e *Executor, task distributed.Task) string {
	if e.spillDir == "" {
		return filepath.Join(os.TempDir(), "stage-"+task.ID)
	}
	return filepath.Join(e.spillDir, "stage-"+task.ID)
}

// fragmentExchangeSink wraps partitionedShuffleSink with lazy schema discovery
// (sink construction deferred until the first non-empty batch carries a
// schema). consume is safe for concurrent callers: lazy construction is
// mutex-guarded, and partitionedShuffleSink.Consume is internally concurrent
// (per-partition locks — see its type comment).
type fragmentExchangeSink struct {
	executor    *Executor
	spillDir    string
	shuffleKeys []string
	numParts    int

	initMu sync.Mutex
	sink   *partitionedShuffleSink
}

func (s *fragmentExchangeSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	s.initMu.Lock()
	if s.sink == nil {
		sink := newPartitionedShuffleSink(s.spillDir, s.shuffleKeys, s.numParts, b.Schema)
		if err := sink.Init(ctx); err != nil {
			s.initMu.Unlock()
			return fmt.Errorf("exchange sink init: %w", err)
		}
		s.sink = sink
	}
	sink := s.sink
	s.initMu.Unlock()
	return sink.Consume(ctx, b)
}

func (s *fragmentExchangeSink) finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	defer os.RemoveAll(s.spillDir)
	if s.sink == nil {
		return nil
	}
	if err := s.sink.Finalize(ctx); err != nil {
		return err
	}
	// Per-phase sink attribution (see the counter block in
	// partitionedShuffleSink): says where this task's sink_ms went.
	// encode overlaps append (the flushing consumer's append window
	// includes its encode); the buckets are otherwise disjoint.
	if calls := s.sink.consumeCalls.Load(); calls > 0 {
		s.executor.logger.Info("shuffle sink phases",
			"task_id", task.ID, "stage_id", task.StageID,
			"consumes", calls, "rows", s.sink.consumeRows.Load(),
			"flatten_ms", s.sink.phaseFlattenNs.Load()/1e6,
			"hash_ms", s.sink.phaseHashNs.Load()/1e6,
			"append_ms", s.sink.phaseAppendNs.Load()/1e6,
			"encode_ms", s.sink.phaseEncodeNs.Load()/1e6)
	}
	return s.executor.uploadPartitionedShuffleFiles(ctx, task, s.sink, result)
}

func (s *fragmentExchangeSink) close() {
	if s.sink != nil {
		s.sink.Close()
	}
}

// fragmentUnpartitionedSink delegates to unpartitionedStageSink, whose
// Consume is safe for concurrent callers (append under its lock; the chunk
// encode+write is double-buffered outside it).
type fragmentUnpartitionedSink struct {
	executor *Executor
	sink     *unpartitionedStageSink
	spillDir string
}

func (s *fragmentUnpartitionedSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	return s.sink.Consume(ctx, b)
}

func (s *fragmentUnpartitionedSink) finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	defer os.RemoveAll(s.spillDir)
	if err := s.sink.Finalize(ctx); err != nil {
		return err
	}
	if s.sink.NumChunks() == 0 {
		return nil
	}
	return s.executor.uploadUnpartitionedSpill(ctx, task, s.sink, result)
}

func (s *fragmentUnpartitionedSink) close() { s.sink.Close() }

// fragmentGatherSink serializes all consumes through mu: gather is the
// coordinator-reply path (small final results published over NATS/data-plane),
// so its volume never justifies internal concurrency — the mutex simply makes
// the adapter safe under morsel-parallel consumers like the other sinks.
type fragmentGatherSink struct {
	mu       sync.Mutex
	sink     *gatherReplySink
	finished bool
}

func (s *fragmentGatherSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finished {
		// Init only on first non-empty batch; the sink captures schema lazily
		// from gather's NATS publish path.
		if err := s.sink.Init(ctx); err != nil {
			return err
		}
		s.finished = true
	}
	return s.sink.Consume(ctx, b)
}

func (s *fragmentGatherSink) finalize(ctx context.Context, _ distributed.Task, _ *distributed.ResultNotification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finished {
		// No batches consumed: still need to publish a terminal marker so
		// the coord's gather subscriber unblocks.
		if err := s.sink.Init(ctx); err != nil {
			return err
		}
	}
	return s.sink.Finalize(ctx)
}

func (s *fragmentGatherSink) close() {}
