package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/planner/physical"
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
	p.logger.Info("fragment task progress", attrs...)
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
	if len(sourceSpec.InputFiles) == 0 {
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

	// Build the source.
	src, err := e.buildFragmentSource(task, sourceSpec)
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

	// Dynamic-filter emit (Trino-style semi-join pushdown). When the source
	// spec carries DynamicFilterEmits, prepend a pass-through accumulator
	// op per emit to the unary chain. Snapshots are uploaded after the
	// pipeline finishes; refs land in result.DynamicFilterPartials.
	emitOps, err := buildDynamicFilterEmitOps(sourceSpec.DynamicFilterEmits)
	if err != nil {
		return fmt.Errorf("fragment task %s: dynamic filter emit ops: %w", task.ID, err)
	}
	for _, op := range emitOps {
		if err := op.Init(ctx); err != nil {
			return fmt.Errorf("fragment task %s: emit op init: %w", task.ID, err)
		}
	}

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
		return e.finalizeDynamicFilterEmits(ctx, task, emitOps, sourceSpec.DynamicFilterEmits, result)
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
	return e.finalizeDynamicFilterEmits(ctx, task, emitOps, sourceSpec.DynamicFilterEmits, result)
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
	// only when the flag allows it, every op is Cloneable, the fragment
	// clears the size gate, and CPU tokens are available. Tokens are held for
	// the duration of the fragment.
	if k, release := e.morselFragmentWorkers(task, ops); k > 1 {
		defer release()
		e.logger.Debug("morsel parallel fragment",
			"task_id", task.ID,
			"stage_id", task.StageID,
			"k", k,
			"estimated_bytes", task.EstimatedBytes,
			"cpu_tokens_in_use", e.cpuTokens.InUse(),
			"cpu_tokens_cap", e.cpuTokens.Capacity())
		return e.runFragmentLinearParallel(ctx, task, src, ops, sink, result, k)
	}
	progress := exec.ProgressReporterFromContext(ctx)
	fp := newFragmentProgress(e.logger, task, e)
	var totalRows int64
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if err := sink.consume(ctx, b); err != nil {
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
			select {
			case <-gctx.Done():
				return gctx.Err()
			case ch <- b:
			}
		}
	})
	// Consumer: ops + sink. Drives backpressure (heap pause) and progress.
	g.Go(func() error {
		for b := range ch {
			if err := fp.applyBackpressure(gctx); err != nil {
				return err
			}
			cur := b
			var err error
			for _, op := range ops {
				cur, err = op.Execute(gctx, cur)
				if err != nil {
					return fmt.Errorf("unary exec: %w", err)
				}
				if cur == nil {
					break
				}
			}
			if cur == nil || cur.ActiveLen() == 0 {
				continue
			}
			if err := consume(gctx, cur); err != nil {
				return fmt.Errorf("sink consume: %w", err)
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("fragment task %s: %w", task.ID, err)
	}

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

// morselMinFragmentBytes is the auto-mode size gate: fragments whose
// EstimatedBytes fall below it stay serial. Parallelism pays only above a
// size threshold — the parallel join build learned this the hard way
// (join.go, size-gated for the same reason). Package-level var so tests can
// lower it.
var morselMinFragmentBytes int64 = 64 << 20

// morselFragmentParallelismCap bounds per-fragment width regardless of
// policy: past ~8 consumers the single producer (source decode) is the
// bottleneck and extra clones only add merge/scratch overhead.
const morselFragmentParallelismCap = 8

// morselFragmentWorkers decides the parallel width for a linear fragment and
// acquires CPU tokens for the extra consumers. Returns k and a release
// function (call exactly once, after the fragment finishes). k == 1 means
// serial — the returned release is a no-op.
//
// The first consumer is free (the serial baseline is always allowed); each
// extra consumer takes one token. Acquisition is non-blocking: under token
// exhaustion the fragment degrades toward serial rather than queueing.
func (e *Executor) morselFragmentWorkers(task distributed.Task, ops []exec.UnaryOperator) (int, func()) {
	noop := func() {}
	policy := e.morselWorkers
	if policy == 0 || policy == 1 || e.cpuTokens == nil {
		return 1, noop
	}
	// Every op must be Cloneable — non-cloneable ops (e.g. the
	// DynamicFilterEmitOp accumulator) keep the fragment serial.
	for _, op := range ops {
		if _, ok := op.(exec.Cloneable); !ok {
			return 1, noop
		}
	}
	target := policy
	if policy < 0 { // auto: size-gated, width up to core count
		if task.EstimatedBytes < morselMinFragmentBytes {
			return 1, noop
		}
		target = runtime.GOMAXPROCS(0)
	}
	if target > morselFragmentParallelismCap {
		target = morselFragmentParallelismCap
	}
	if target <= 1 {
		return 1, noop
	}
	got := e.cpuTokens.TryAcquire(target - 1)
	if got == 0 {
		return 1, noop
	}
	return 1 + got, func() { e.cpuTokens.Release(got) }
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
func (e *Executor) runFragmentLinearParallel(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink fragmentSink, result *distributed.ResultNotification, k int) error {
	progress := exec.ProgressReporterFromContext(ctx)
	fp := newFragmentProgress(e.logger, task, e)
	var totalRows atomic.Int64
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if err := sink.consume(ctx, b); err != nil {
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

	// Warmup: one batch through the original chain before any clone exists.
	warmup, err := src.Next(ctx)
	if err != nil {
		return fmt.Errorf("fragment task %s: source next: %w", task.ID, err)
	}
	chains := make([][]exec.UnaryOperator, 1, k)
	chains[0] = ops
	if warmup != nil {
		cur := warmup
		for _, op := range ops {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: unary exec: %w", task.ID, err)
			}
			if cur == nil {
				break
			}
		}
		if cur != nil && cur.ActiveLen() > 0 {
			if err := consume(ctx, cur); err != nil {
				return fmt.Errorf("fragment task %s: sink consume: %w", task.ID, err)
			}
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

		runChain := func(ctx context.Context, chain []exec.UnaryOperator, b *batch.RecordBatch) (*batch.RecordBatch, error) {
			cur := b
			var err error
			for _, op := range chain {
				cur, err = op.Execute(ctx, cur)
				if err != nil {
					return nil, fmt.Errorf("unary exec: %w", err)
				}
				if cur == nil {
					return nil, nil
				}
			}
			return cur, nil
		}

		var collapsed atomic.Bool
		g, gctx := errgroup.WithContext(ctx)
		for i := 0; i < k; i++ {
			chain := chains[i]
			g.Go(func() error {
				for m := range d.ch {
					if err := fp.applyBackpressure(gctx); err != nil {
						m.retire()
						return err
					}
					cur, err := runChain(gctx, chain, m.b)
					if err != nil {
						m.retire()
						return err
					}
					if cur != nil && cur.ActiveLen() > 0 {
						if err := consume(gctx, cur); err != nil {
							m.retire()
							return fmt.Errorf("sink consume: %w", err)
						}
					}
					m.retire()
					if collapsed.Load() {
						return nil
					}
					if heapPressureActive() {
						collapsed.Store(true)
						return nil
					}
				}
				return nil
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
			cur, err := runChain(ctx, ops, m.b)
			if err != nil {
				m.retire()
				return fmt.Errorf("fragment task %s: %w", task.ID, err)
			}
			if cur != nil && cur.ActiveLen() > 0 {
				if err := consume(ctx, cur); err != nil {
					m.retire()
					return fmt.Errorf("fragment task %s: sink consume: %w", task.ID, err)
				}
			}
			m.retire()
		}
		if err := <-prodErr; err != nil {
			return fmt.Errorf("fragment task %s: %w", task.ID, err)
		}
		e.logger.Debug("morsel parallel fragment done",
			append([]any{"task_id", task.ID, "k", k}, d.logAttrs()...)...)
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
func (e *Executor) runBreakerConsumeParallel(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink exec.MergeableSink, k int) error {
	fp := newFragmentProgress(e.logger, task, e)

	consumeInto := func(ctx context.Context, dst exec.Sink, b *batch.RecordBatch) error {
		if b.Sel != nil {
			selCopy := make([]uint32, len(b.Sel))
			copy(selCopy, b.Sel)
			b.Sel = selCopy
		}
		return dst.Consume(ctx, b)
	}
	runChain := func(ctx context.Context, chain []exec.UnaryOperator, b *batch.RecordBatch) (*batch.RecordBatch, error) {
		cur := b
		var err error
		for _, op := range chain {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return nil, fmt.Errorf("unary exec: %w", err)
			}
			if cur == nil {
				return nil, nil
			}
		}
		return cur, nil
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
	if cur, err := runChain(ctx, ops, warmup); err != nil {
		return err
	} else if cur != nil && cur.ActiveLen() > 0 {
		if err := consumeInto(ctx, sink, cur); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
	}

	// Cloned chains + clone sinks. Worker 0 keeps the originals and the
	// primary. Clone sinks charge a tracking-only SpillManager view; the
	// deferred Closes release whatever charge a clone still holds (Close is
	// idempotent about it — post-merge the clone's charge is zero for Sort
	// and released-once for HashAggregate).
	var trackingSpill *memory.SpillManager
	if e.sharedSpill != nil {
		trackingSpill = e.sharedSpill.TrackingOnlyView()
	}
	chains := make([][]exec.UnaryOperator, 1, k)
	chains[0] = ops
	sinks := make([]exec.Sink, 1, k)
	sinks[0] = sink
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
		chain, wsink := chains[i], sinks[i]
		g.Go(func() error {
			for m := range d.ch {
				if err := fp.applyBackpressure(gctx); err != nil {
					m.retire()
					return err
				}
				cur, err := runChain(gctx, chain, m.b)
				if err != nil {
					m.retire()
					return err
				}
				if cur != nil && cur.ActiveLen() > 0 {
					if err := consumeInto(gctx, wsink, cur); err != nil {
						m.retire()
						return fmt.Errorf("sink consume: %w", err)
					}
				}
				m.retire()
				if collapsed.Load() {
					return nil
				}
				if e.sharedSpill != nil && e.sharedSpill.ShouldSpillFor(memory.SpillCheap) {
					collapsed.Store(true)
					return nil
				}
			}
			return nil
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
		if err := fp.applyBackpressure(ctx); err != nil {
			m.retire()
			return err
		}
		cur, err := runChain(ctx, ops, m.b)
		if err != nil {
			m.retire()
			return err
		}
		if cur != nil && cur.ActiveLen() > 0 {
			if err := consumeInto(ctx, sink, cur); err != nil {
				m.retire()
				return fmt.Errorf("sink consume: %w", err)
			}
		}
		m.retire()
	}
	if err := <-prodErr; err != nil {
		return err
	}
	e.logger.Debug("morsel parallel breaker consume done",
		append([]any{"task_id", task.ID, "k", k}, d.logAttrs()...)...)

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
		downstream := ops[opIdx+1:]
		for {
			b, err := fo.NextFlush(ctx)
			if err != nil {
				return err
			}
			if b == nil {
				break
			}
			cur := b
			for _, dop := range downstream {
				cur, err = dop.Execute(ctx, cur)
				if err != nil {
					return err
				}
				if cur == nil {
					break
				}
			}
			if cur == nil || cur.ActiveLen() == 0 {
				continue
			}
			if snapshotSel && cur.Sel != nil {
				selCopy := make([]uint32, len(cur.Sel))
				copy(selCopy, cur.Sel)
				cur.Sel = selCopy
			}
			if err := consume(ctx, cur); err != nil {
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

	breakers := make([]*fragmentBreaker, len(breakerIdxs))
	for i, idx := range breakerIdxs {
		fb, err := e.buildFragmentBreaker(ctx, middle[idx])
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
			if isMergeable {
				k, release = e.morselFragmentWorkers(task, phaseOps)
			}
			if k > 1 {
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
					return e.runBreakerConsumeParallel(ctx, task, currentSrc, phaseOps, mergeable, k)
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
	fp := newFragmentProgress(e.logger, task, e)
	var totalRows int64
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if err := sink.consume(ctx, b); err != nil {
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
	for {
		b, err := currentSrc.Next(ctx)
		if err != nil {
			return fmt.Errorf("fragment task %s: %s next: %w", task.ID, last.Label, err)
		}
		if b == nil {
			break
		}
		cur := b
		if last.DrainXform != nil {
			cur, err = last.DrainXform(cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: %s drain xform: %w", task.ID, last.Label, err)
			}
			if cur == nil {
				continue
			}
		}
		for _, op := range finalOps {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: post-breaker exec: %w", task.ID, err)
			}
			if cur == nil {
				break
			}
		}
		if cur == nil || cur.ActiveLen() == 0 {
			continue
		}
		if err := consume(ctx, cur); err != nil {
			return fmt.Errorf("fragment task %s: sink consume: %w", task.ID, err)
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
	for {
		// Heap-aware backpressure between batches; mirrors the runFragment*
		// callers and the engine-level Pipeline.Run hook.
		if err := memory.PauseOnHeapBackpressure(ctx); err != nil {
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
		for _, op := range ops {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return err
			}
			if cur == nil {
				break
			}
		}
		if cur == nil || cur.ActiveLen() == 0 {
			continue
		}
		if err := consume(ctx, cur); err != nil {
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
func (e *Executor) buildFragmentBreaker(ctx context.Context, spec distributed.OpSpec) (*fragmentBreaker, error) {
	switch spec.Type {
	case distributed.OpHashAggregate:
		hashAgg, err := e.buildFragmentHashAggregate(spec)
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
			project, _, perr := buildAggInputProjection(spec.GroupByCols, spec.Aggregates, nil)
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
		// AVG-fold collapses __avg_sum#X / __avg_count#X synthetics into the
		// single AVG output column on the FINAL stage only. Wrap the slice-
		// taking applyAvgFold helper as a per-batch transform.
		if spec.FoldAvg {
			fb.DrainXform = func(b *batch.RecordBatch) (*batch.RecordBatch, error) {
				folded, ferr := applyAvgFold([]*batch.RecordBatch{b})
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
		sorter, err := e.buildFragmentSort(spec)
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

	default:
		return nil, fmt.Errorf("unsupported breaker op %q", spec.Type)
	}
}

// buildFragmentSort constructs an exec.Sort from an OpSpec. Converts each
// SortKeySpec.Desc into exec.Descending. Spill is wired from the executor's
// shared spill manager when present.
func (e *Executor) buildFragmentSort(spec distributed.OpSpec) (*exec.Sort, error) {
	if len(spec.SortKeySpecs) == 0 {
		return nil, fmt.Errorf("sort: SortKeySpecs required")
	}
	keys := make([]exec.SortKey, len(spec.SortKeySpecs))
	for i, k := range spec.SortKeySpecs {
		order := exec.Ascending
		if k.Desc {
			order = exec.Descending
		}
		keys[i] = exec.SortKey{Column: k.Column, Order: order}
	}
	sorter := exec.NewSort(keys)
	if e.sharedSpill != nil {
		sorter.Spill = e.sharedSpill
	}
	return sorter, nil
}

// buildFragmentHashAggregate constructs an exec.HashAggregate from an OpSpec.
// In merge mode the spec's Aggregates are rewritten so InputCol = OutputCol
// (the partial-output column) and COUNT becomes SUM (counting partial rows
// re-counts groups, not source rows).
func (e *Executor) buildFragmentHashAggregate(spec distributed.OpSpec) (*exec.HashAggregate, error) {
	if !spec.GroupByAll && len(spec.Aggregates) == 0 && len(spec.GroupByCols) == 0 {
		return nil, fmt.Errorf("hash_aggregate: at least one of GroupByCols, Aggregates, or GroupByAll is required")
	}
	aggCols := make([]exec.AggColumn, len(spec.Aggregates))
	for i, a := range spec.Aggregates {
		inputCol := a.InputCol
		fn := parseAggFuncString(a.Func)
		if spec.MergeMode {
			if a.OutputCol != "" {
				inputCol = a.OutputCol
			}
			if fn == exec.AggCount {
				fn = exec.AggSum
			}
		}
		aggCols[i] = exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			OutputCol:  a.OutputCol,
			OutputType: aggOutputTypeString(a.Func),
		}
	}
	hashAgg := exec.NewHashAggregate(spec.GroupByCols, aggCols)
	hashAgg.GroupByAll = spec.GroupByAll
	if e.sharedSpill != nil {
		hashAgg.Spill = e.sharedSpill
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
	return ops, cleanup, nil
}

func isFragmentSourceOp(t distributed.OpType) bool {
	return t == distributed.OpScan || t == distributed.OpShuffleSource
}

func isFragmentSinkOp(t distributed.OpType) bool {
	return t == distributed.OpExchangeSender || t == distributed.OpUnpartitionedSink || t == distributed.OpGatherSink
}

func isFragmentBreakerOp(t distributed.OpType) bool {
	return t == distributed.OpHashAggregate || t == distributed.OpSort
}

func (e *Executor) buildFragmentSource(task distributed.Task, spec distributed.OpSpec) (exec.Source, error) {
	bucket := spec.InputBucket
	if bucket == "" {
		bucket = task.DataBucket
	}
	if bucket == "" {
		bucket = task.ResultBucket
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

	case distributed.OpHashJoinProbe, distributed.OpBroadcastProbe:
		return e.buildFragmentJoinProbe(ctx, task, spec)

	case distributed.OpProject:
		proj, err := buildSelectProjection(spec.Projections)
		if err != nil {
			return nil, nil, err
		}
		return []exec.UnaryOperator{proj}, nil, nil

	default:
		return nil, nil, fmt.Errorf("unsupported unary op %q", spec.Type)
	}
}

func (e *Executor) buildFragmentJoinProbe(ctx context.Context, task distributed.Task, spec distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	if len(spec.LeftKeys) == 0 || len(spec.RightKeys) == 0 {
		return nil, nil, fmt.Errorf("hash_join_probe: LeftKeys and RightKeys required")
	}
	if len(spec.BuildFiles) == 0 {
		// Empty build → inner/semi joins emit no rows; upstream planner may
		// have decided this fragment should not run, but we treat empty
		// build as "no output". Returning an op chain that drops every row
		// keeps the pipeline well-formed without special-casing the runner.
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
		src, err := e.sourceForAlias(task.QueryID, bucket, spec.BuildAlias, spec.BuildFiles)
		if err != nil {
			return nil, fmt.Errorf("build source: %w", err)
		}
		if err := src.Init(ctx); err != nil {
			src.Close()
			return nil, fmt.Errorf("build source init: %w", err)
		}
		defer src.Close()

		hj := exec.NewHashJoin(mapJoinTypeString(spec.JoinType), spec.LeftKeys, spec.RightKeys)
		hj.BuildTableAlias = spec.BuildAlias
		hj.QualifyAllBuildCols = spec.QualifyAllBuildCols
		if spec.BuildRowHint > 0 {
			hj.BuildRowHint = spec.BuildRowHint
		}
		hj.SemiAntiKeyOnly = spec.SemiAntiKeyOnly
		if spec.JoinFilter != "" {
			hj.SemiAntiFilter = physical.BuildSemiAntiFilter(spec.JoinFilter)
			// Filtered semi/anti builds store only keys + filter columns —
			// the worker has no post-build prune at all, so without this a
			// broadcast lineitem build retains every scanned column.
			if hj.JoinType == exec.SemiJoin || hj.JoinType == exec.AntiJoin {
				hj.BuildStoreCols = physical.SemiAntiBuildStoreCols(spec.RightKeys, spec.JoinFilter)
			}
		}
		if e.sharedSpill != nil {
			// Wiring a Spill manager + tracker makes Build partition on arrival
			// unconditionally (O(partition) spill). There is no at-entry
			// pressure heuristic: the stale 30% snapshot used to leave shuffle
			// builds on the flat path, which could then need an O(total)
			// reactive repartition under pressure (the Q17/Q18 mc=4 killer).
			hj.Spill = e.sharedSpill
			hj.MemTracker = e.sharedTracker
		}
		if err := hj.Build(ctx, src); err != nil {
			return nil, fmt.Errorf("building hash table: %w", err)
		}
		hj.FixKeyAssignment()
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
