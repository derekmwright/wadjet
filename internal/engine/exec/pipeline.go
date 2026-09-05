package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// HeldStateSource marks sources that serve a pipeline-breaker's HELD
// state (aggregate/sort/window output phases). Heap-backpressure pauses
// must not throttle pipelines draining such sources: the held state IS
// the memory pressure, and draining it is the only way the pressure
// clears — pausing the drain turned ClickBench Q33's output phase into
// 100+ seconds of sleeping on a single goroutine while 15GB of finished
// aggregate state waited to be streamed out.
type HeldStateSource interface {
	ServesHeldState() bool
}

func sourceServesHeldState(src Source) bool {
	h, ok := src.(HeldStateSource)
	return ok && h.ServesHeldState()
}

// Pipeline represents Source → [UnaryOps...] → Sink.
type Pipeline struct {
	Source  Source
	Ops     []UnaryOperator
	Sink    Sink
	Workers int // number of parallel workers (0 or 1 = serial)

	// Morsel-parallel clones, held so Close() can reach them.
	//
	// runParallel builds a cloned operator chain and a cloned sink per
	// worker 1..N-1. On the normal path it closes the op chains and either
	// closes or hands the clone sinks to the primary (partitioned
	// adoption), and calls releaseClones so nothing below double-closes.
	// On every OTHER path — a worker error, a cancelled context, a clone's
	// own Init failing — it returns ABOVE that teardown, and Close() held
	// no reference to the clones at all: p.Ops is opChains[0] and p.Sink is
	// workerSinks[0]. A cancelled morsel-parallel GROUP BY therefore left
	// 6,164,505 B in 163 aggregate partial-state files that no correctly
	// placed defer could reach, because the files belong to CLONE sinks
	// (#625 M2, ADR-0028). Guarded by a mutex only because a clone's Init
	// error can return while other clones are still being constructed;
	// every worker goroutine has finished by the time Close runs.
	cloneMu    sync.Mutex
	cloneOps   [][]UnaryOperator
	cloneSinks []Sink
}

// trackClone records a clone chain / sink so Close() can reclaim it if
// runParallel never reaches its teardown.
func (p *Pipeline) trackCloneOps(chain []UnaryOperator) {
	p.cloneMu.Lock()
	p.cloneOps = append(p.cloneOps, chain)
	p.cloneMu.Unlock()
}

func (p *Pipeline) trackCloneSink(s Sink) {
	p.cloneMu.Lock()
	p.cloneSinks = append(p.cloneSinks, s)
	p.cloneMu.Unlock()
}

// releaseCloneOps / releaseCloneSinks drop the tracking once runParallel's
// own teardown has taken ownership (closed them, or handed the sinks to the
// primary's adoptedPartitions, whose Close closes them in turn).
func (p *Pipeline) releaseCloneOps() {
	p.cloneMu.Lock()
	p.cloneOps = nil
	p.cloneMu.Unlock()
}

func (p *Pipeline) releaseCloneSinks() {
	p.cloneMu.Lock()
	p.cloneSinks = nil
	p.cloneMu.Unlock()
}

// closeTrackedClones closes every clone runParallel did not hand off. It is
// idempotent: the tracking slices are cleared as they are drained.
func (p *Pipeline) closeTrackedClones() error {
	p.cloneMu.Lock()
	ops, sinks := p.cloneOps, p.cloneSinks
	p.cloneOps, p.cloneSinks = nil, nil
	p.cloneMu.Unlock()

	var firstErr error
	for _, chain := range ops {
		for _, op := range chain {
			if err := op.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, s := range sinks {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FatalEvalPanic marks a panic value that carries a query ERROR rather than a
// bug: an expression that cannot produce a correct answer and must not fall
// back to NULL. Expression evaluation has no error return (Expr.Eval yields a
// value and nothing else), so the one such condition — a correlated subquery
// whose outer column is not in the batch, issue #347 — raises a panic carrying
// a value that implements this, and the pipeline drivers convert it back into
// an error. Panics that do not implement it are re-raised untouched.
type FatalEvalPanic interface {
	error
	FatalEvalError() error
}

// recoverFatalEval turns a FatalEvalPanic recovered from r into an error, and
// re-panics anything else. Call it only with a non-nil recover() result.
func recoverFatalEval(r any) error {
	if fe, ok := r.(FatalEvalPanic); ok {
		return fe.FatalEvalError()
	}
	panic(r)
}

// fatalEvalError is exec's own FatalEvalPanic carrier, for the closure-based
// evaluators this package still owns (ArithExpr). expr has its twin.
type fatalEvalError struct{ err error }

func (f fatalEvalError) Error() string { return f.err.Error() }

// FatalEvalError satisfies FatalEvalPanic.
func (f fatalEvalError) FatalEvalError() error { return f.err }

// RecoverFatalEval is recoverFatalEval for the drivers that live outside
// this package: the embedded API's Query/Execute and the coordinator's
// ExecuteSQL, which reach batch-writing code (DML row building, result
// merging) without ever entering Pipeline.Run. batch.TypeMismatchError
// (#361's silent-write guard) rides this contract too, so those entries
// must convert it the same way. Call only with a non-nil recover() result;
// anything that is not a FatalEvalPanic is re-raised untouched.
func RecoverFatalEval(r any) error { return recoverFatalEval(r) }

// Run executes the pipeline by pulling from source, transforming through
// operators, and pushing to sink. When Workers > 1 and all operators
// implement Cloneable, batches are processed by multiple goroutines
// concurrently. Otherwise falls back to serial execution.
func (p *Pipeline) Run(ctx context.Context) (err error) {
	// The query-scoped boundary (#511): a FatalEvalPanic still becomes its
	// own precise error, and anything else becomes an internal-error XX000
	// with a logged stack rather than a dead process. The caller's deferred
	// Close still runs, so the pipeline tears down normally either way.
	defer func() {
		if r := recover(); r != nil {
			err = RecoverQueryPanic(ctx, "pipeline", r)
		}
	}()
	if err := p.Source.Init(ctx); err != nil {
		return fmt.Errorf("source init: %w", err)
	}
	for _, op := range p.Ops {
		if err := op.Init(ctx); err != nil {
			return fmt.Errorf("operator init: %w", err)
		}
	}
	if err := p.Sink.Init(ctx); err != nil {
		return fmt.Errorf("sink init: %w", err)
	}
	// This driver drains pending output after every Execute (ChainDriver), so
	// operators may bound their per-call fan-out. Clones inherit the setting
	// through their own Clone().
	EnableBoundedOutput(p.Ops)

	if p.Workers > 1 && p.allOpsCloneable() && SinkSurvivesCloning(p.Sink) {
		return p.runParallel(ctx)
	}
	return p.runSerial(ctx)
}

// SinkSurvivesCloning reports whether the sink's state can be split across
// morsel-parallel clones and merged back.
//
// EXPORTED because CloneSink has TWO call sites, not one: this package's
// Pipeline.runParallel and the worker's runBreakerConsumeParallel. Guarding
// only the first left the whole defect below reachable on the stage DAG
// whenever a fragment's breaker ran morsel-parallel — `SUM(DISTINCT a)`
// answered 64.96 for 16.24 with four morsel workers, exactly four times over,
// and `STRING_AGG(DISTINCT …)` emitted every value four times in a row.
// TestCloneSinkCallersConsultTheCloneFence enumerates the call sites so a
// third one cannot be added without consulting this.
//
// A DISTINCT aggregate other than COUNT cannot (#703). Its accumulator holds a
// SUM (or a mean, or a variance triple) that each clone has already folded its
// own copy of a shared value into, and mergeSinkState adds those accumulators:
// two clones that both saw 12.75 contribute it twice, so
// `SELECT SUM(DISTINCT a) FROM revdup` answered 97.44, 129.92 and 194.88 for
// 16.24 across four runs of the same binary over the same bytes — the
// multiplier being however many clones happened to receive rows. Unioning the
// distinct SETS at the merge, which the code already does, cannot undo an
// addition that already happened.
//
// COUNT(DISTINCT) and APPROX_DISTINCT are unaffected and keep their
// parallelism: their whole state IS the set, so the union is the merge. So is
// every non-distinct aggregate.
//
// This is #291's rule — "a DISTINCT aggregate has no mergeable partial form" —
// applied to the in-process clone merge, where the stage DAG already applies it
// by routing every distinct aggregate through the one-level RawInputAggregate
// shape. The cost is morsel parallelism for queries that were WRONG before, and
// only for those; the spilled path is unaffected because it re-aggregates from
// raw rows.
func SinkSurvivesCloning(s Sink) bool {
	h, ok := s.(*HashAggregate)
	if !ok {
		return true
	}
	for _, a := range h.Aggs {
		if a.Distinct {
			return false
		}
	}
	return true
}

// ChainDriver pushes a batch through an operator chain and hands every
// resulting batch to deliver. It exists because an operator's output for one
// input batch is not always one batch: a hash-join probe bounds its fan-out
// per call and suspends the rest (see BoundedOutputOperator), so the chain
// below it has to be re-driven for each resumed slice before the operator is
// given its next input.
//
// Push keeps its input alive across the whole resume loop, because a
// suspended fan-out still reads the probe-side columns of the batch it was
// handed. That is the driver half of the BoundedOutputOperator contract, so
// a chain driven through a ChainDriver may be opted in with
// EnableBoundedOutput — and one that is not stays unbounded.
//
// It is exported because the drivers that matter live outside this package:
// the worker's fragment executors run the same operator chains on the
// distributed path, and a probe that only suspends under exec.Pipeline is a
// probe that still OOMs a worker (#317).
type ChainDriver struct {
	ops     []UnaryOperator
	bounded []BoundedOutputOperator // parallel to ops; nil = single-shot operator
	deliver func(context.Context, *batch.RecordBatch) error
	release bool
	inspect func(opIdx int, op UnaryOperator, out *batch.RecordBatch)
}

// NewChainDriver returns a driver for ops that hands every output batch to
// deliver. The driver does not touch batch ownership: callers that pool their
// batches opt in with ReleaseInputs.
func NewChainDriver(ops []UnaryOperator, deliver func(context.Context, *batch.RecordBatch) error) *ChainDriver {
	d := &ChainDriver{ops: ops, deliver: deliver, bounded: make([]BoundedOutputOperator, len(ops))}
	for i, op := range ops {
		if bo, ok := op.(BoundedOutputOperator); ok {
			d.bounded[i] = bo
		}
	}
	return d
}

// ReleaseInputs makes the driver release every operator's input batch once
// that operator is finished reading it — which is after the last resumption,
// not after Execute. Only for callers whose deliver also releases: the
// batches travel to a pool, and a caller that releases what it does not own
// hands the same batch out twice.
func (d *ChainDriver) ReleaseInputs() *ChainDriver { d.release = true; return d }

// Inspect installs a callback run on every operator's output, including each
// resumed slice. Its one caller is the worker's #277 forensics, which needs
// the producing operator's identity — something deliver, one step past the
// end of the chain, cannot see.
func (d *ChainDriver) Inspect(fn func(opIdx int, op UnaryOperator, out *batch.RecordBatch)) *ChainDriver {
	d.inspect = fn
	return d
}

// Push runs b through the whole chain. The bool result is the pipeline's
// `exhausted` signal: a DoneSignaler operator (LIMIT) reported satisfaction.
//
// A panic raised by an expression inside the chain is converted to an error
// here, the same contract Pipeline.Run provides. ChainDriver's callers (the
// worker's fragment drivers) run on errgroup goroutines with no recover of
// their own, so without this a runtime query error — 22012 division by zero,
// 22P02 invalid cast — would take the worker process down instead of failing
// the query. Since #511 that holds for an UNEXPECTED panic too: re-raising it
// past this point reached an errgroup goroutine and ended the process.
//
// The defer is per batch, not per row, and it already existed — the boundary
// added no new cost to this path.
func (d *ChainDriver) Push(ctx context.Context, b *batch.RecordBatch) (exhausted bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = RecoverQueryPanic(ctx, "operator chain", r)
		}
	}()
	return d.push(ctx, 0, b)
}

// push runs b through ops[i:].
func (d *ChainDriver) push(ctx context.Context, i int, b *batch.RecordBatch) (bool, error) {
	if i == len(d.ops) {
		return false, d.deliver(ctx, b)
	}
	op := d.ops[i]
	FlattenForConsumer(b, op)
	out, err := op.Execute(ctx, b)
	if err != nil {
		return false, fmt.Errorf("operator execute: %w", err)
	}
	// When the operator returned its input in place, ownership travelled with
	// it; otherwise the input is ours to release once the operator is done
	// reading it, which is after the last resumption, not after Execute.
	ownInput := d.release && out != b
	exhausted := false
	for {
		if out == nil {
			if ds, ok := op.(DoneSignaler); ok && ds.Done() {
				exhausted = true
			}
			break
		}
		// A cancelled statement must stop BETWEEN output batches, here in the
		// fan-out loop and not only at the source pull: a keyless join's
		// fan-out for one input batch is quadratic work (#368 — a CancelRequest
		// was accepted and the cross product ran 11 more seconds to a normal
		// completion, because the pump only polls per SOURCE batch and the
		// whole join lived under a handful of those). One atomic load per
		// output batch of ≤2048 rows is noise.
		if cerr := ctx.Err(); cerr != nil {
			err = fmt.Errorf("operator chain cancelled: %w", cerr)
			break
		}
		if d.inspect != nil {
			d.inspect(i, op, out)
		}
		ex, perr := d.push(ctx, i+1, out)
		if ex {
			exhausted = true
		}
		if perr != nil {
			err = perr
			break
		}
		bo := d.bounded[i]
		if bo == nil || !bo.HasPendingOutput() {
			break
		}
		out, err = bo.NextOutput(ctx)
		if err != nil {
			err = fmt.Errorf("operator resume: %w", err)
			break
		}
	}
	if ownInput && b != nil {
		b.Release()
	}
	return exhausted, err
}

// allOpsCloneable returns true if every operator in the chain implements Cloneable.
func (p *Pipeline) allOpsCloneable() bool {
	for _, op := range p.Ops {
		if _, ok := op.(Cloneable); !ok {
			return false
		}
	}
	return true
}

// runSerial is the original single-threaded pipeline loop.
func (p *Pipeline) runSerial(ctx context.Context) error {
	progress := ProgressReporterFromContext(ctx)
	drainPhase := sourceServesHeldState(p.Source)
	driver := NewChainDriver(p.Ops, func(ctx context.Context, b *batch.RecordBatch) error {
		FlattenForConsumer(b, p.Sink)
		if err := p.Sink.Consume(ctx, b); err != nil {
			return fmt.Errorf("sink consume: %w", err)
		}
		b.Release()
		return nil
	}).ReleaseInputs()
	for {
		// Per-batch, not per-64: a batch is ≥3 orders of magnitude more work
		// than this atomic load, and a CancelRequest must be observed between
		// batches everywhere (#368).
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("pipeline cancelled: %w", err)
		}

		// Heap-aware backpressure: when process heap is approaching
		// GOMEMLIMIT, pause briefly so GC can reclaim before pulling more
		// data. Cheap (cached check) when no pressure; sleeps 50ms when
		// fired. See memory.HeapBackpressureActive for rationale. A
		// spill-capable breaker sink holding the dominant tracked share
		// drains instead of sleeping (#326, see pressure_drain.go).
		if !drainPhase {
			if err := PauseOrDrainOnHeapBackpressure(ctx, p.Sink); err != nil {
				return err
			}
		}

		b, err := p.Source.Next(ctx)
		if err != nil {
			return fmt.Errorf("source next: %w", err)
		}
		if b == nil {
			break
		}
		if progress != nil {
			// Report rows pulled from the source. Captures forward
			// progress for the worker's task heartbeat regardless of
			// downstream operator behaviour (filter dropping all,
			// aggregator buffering, etc).
			progress.AddRows(int64(b.ActiveLen()))
		}

		exhausted, err := driver.Push(ctx, b)
		if err != nil {
			return err
		}
		if exhausted {
			break
		}
	}

	// Flush spilled partition data from Grace Hash Join operators.
	// Results are pushed through the remaining operator chain and into the sink.
	if err := p.flushSpilledOps(ctx, p.Ops, p.consumeIntoSink); err != nil {
		return err
	}

	return p.Sink.Finalize(ctx)
}

// consumeIntoSink is the ordinary flush consumer: one sink, every batch.
func (p *Pipeline) consumeIntoSink(ctx context.Context, b *batch.RecordBatch) error {
	FlattenForConsumer(b, p.Sink)
	if err := p.Sink.Consume(ctx, b); err != nil {
		return fmt.Errorf("sink consume (flush): %w", err)
	}
	b.Release()
	return nil
}

// flushSpilledOps checks each operator for pending spilled data and pushes
// results through the remaining operators into consume.
//
// consume is a parameter because WHERE a flushed batch belongs is not the same
// question on both runners: runSerial has one sink, but under partitioned
// aggregation the flushed rows carry group keys that belong to whichever sink
// OWNS them (runParallel's flushIntoOwners). Sending them all to the primary
// puts a key in two sinks and emits it twice (#782).
func (p *Pipeline) flushSpilledOps(ctx context.Context, ops []UnaryOperator, consume func(context.Context, *batch.RecordBatch) error) error {
	for opIdx, op := range ops {
		fo, ok := op.(FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			continue
		}
		driver := NewChainDriver(ops[opIdx+1:], consume).ReleaseInputs()
		for {
			b, err := fo.NextFlush(ctx)
			if err != nil {
				return fmt.Errorf("flushing spilled data: %w", err)
			}
			if b == nil {
				break
			}
			if _, err := driver.Push(ctx, b); err != nil {
				return fmt.Errorf("flush: %w", err)
			}
		}
	}
	return nil
}

// runParallel processes batches through cloned operator chains in parallel.
// The source and sink are shared; each worker gets its own cloned operators.
func (p *Pipeline) runParallel(ctx context.Context) error {
	// The first-error slot is a FirstError, not a bare atomic.Value: the
	// panic boundary and the ordinary return paths below store errors of
	// different concrete types, and an atomic.Value panics on the second
	// shape (#512). Declared up here because the partition-queue closer,
	// spawned before the workers are, reports into it too.
	var firstErr FirstError

	// Warm-up: process one batch through the original ops to resolve lazy
	// column indices in expressions (e.g., KernelFilter, ColumnCompare).
	// This ensures clones inherit resolved state where possible.
	// Partitioned aggregation: when the sink is a grouped HashAggregate,
	// each worker OWNS a hash partition of the key space instead of
	// aggregating an arbitrary row slice. See partitioned_agg.go for the
	// rationale (kills clone key duplication and the drain-sort-merge tax
	// on high-cardinality GROUP BY). Requires per-worker sinks and no
	// buffer-reusing operator upstream (their outputs can't be shared
	// across asynchronous consumers).
	primaryAgg, sinkIsAgg := p.Sink.(*HashAggregate)
	_, sinkMergeable := p.Sink.(MergeableSink)
	usePartitioned := partitionedAggToggle.On() && sinkIsAgg && sinkMergeable &&
		len(primaryAgg.GroupByCols) > 0 && len(primaryAgg.GroupingSets) == 0 &&
		!primaryAgg.GroupByAll && !opsReuseBuffers(p.Ops)
	var partQueues []chan partitionItem
	var producersWG sync.WaitGroup
	// Set immediately before the worker goroutines are launched. Until it
	// is, every producer slot producersWG holds is owed by a worker that
	// does not exist, and the returning goroutine owes them itself.
	workersSpawned := false
	if usePartitioned {
		PartitionedAggRuns.Add(1)
		primaryAgg.PartitionedDisjoint = true
		// Each clone owns 1/Workers of the key space — divide the NDV
		// presize hint accordingly (CloneSink propagates it).
		primaryAgg.cloneNDVDivisor = p.Workers
		partQueues = make([]chan partitionItem, p.Workers)
		for i := range partQueues {
			partQueues[i] = make(chan partitionItem, 8)
		}
		producersWG.Add(p.Workers)
		go func() {
			// This goroutine only waits and closes channels, but it is
			// still a goroutine a query spawned: a panic here unrecovered
			// ends the process rather than the query (#511).
			//
			// Closing the queues is this goroutine's obligation, and every
			// worker's drainPartitionQueue is blocked until it happens — so
			// recovering WITHOUT closing would trade the crash for a hung
			// query. closed tracks how far it got; the boundary finishes the
			// job for the rest (a second close of the same queue is what the
			// index, not a fresh loop, prevents).
			closed := 0
			defer CatchQueryPanic(ctx, "partition queue closer", func(err error) {
				firstErr.Set(err)
				for ; closed < len(partQueues); closed++ {
					close(partQueues[closed])
				}
			})
			producersWG.Wait()
			for ; closed < len(partQueues); closed++ {
				close(partQueues[closed])
			}
		}()
		// Every return between here and the worker spawn is a return that
		// produced NOTHING, so no worker will ever call producersWG.Done()
		// and the closer above would block in Wait() forever — one leaked
		// goroutine and p.Workers channels per query. The commonest of
		// those returns is the limit-exhausted early-out below, which is
		// the ordinary "GROUP BY over a derived LIMIT" plan (#783).
		defer func() {
			if !workersSpawned {
				producersWG.Add(-p.Workers)
			}
		}()
	}

	warmupBatch, err := p.Source.Next(ctx)
	if err != nil {
		return fmt.Errorf("source next: %w", err)
	}
	if warmupBatch == nil {
		// No data at all — just finalize and return.
		return p.Sink.Finalize(ctx)
	}

	// Process warmup batch through original ops.
	var pendingWarmup []*batch.RecordBatch
	{
		emitted := false
		warmupDriver := NewChainDriver(p.Ops, func(ctx context.Context, b *batch.RecordBatch) error {
			emitted = true
			FlattenForConsumer(b, p.Sink)
			if usePartitioned {
				// Partitioned mode: the warmup batch must be hash-routed
				// like every other batch — consuming it whole into the
				// primary splits its groups across sinks (partial
				// duplicates in the output). Worker 0 partitions it once
				// the queues are live. A bounded operator can emit the
				// warmup batch's output as several batches, so this is a
				// slice, not a single batch.
				pendingWarmup = append(pendingWarmup, b)
				return nil
			}
			if err := p.Sink.Consume(ctx, b); err != nil {
				return fmt.Errorf("sink consume: %w", err)
			}
			b.Release()
			return nil
		}).ReleaseInputs()
		exhausted, err := warmupDriver.Push(ctx, warmupBatch)
		if err != nil {
			return err
		}
		if emitted {
			// Check DoneSignalers even when a batch was emitted — Limit
			// may have returned a truncated batch and is now satisfied.
			for _, op := range p.Ops {
				if ds, ok := op.(DoneSignaler); ok && ds.Done() {
					exhausted = true
				}
			}
		}
		if exhausted {
			// The LIMIT was satisfied by the warm-up batch, so no worker is
			// launched. In PARTITIONED mode that batch was not given to the
			// sink: it was parked in pendingWarmup for worker 0, and worker
			// 0 is what this return skips. Consuming it here is the whole
			// answer — every row of a `GROUP BY over a derived LIMIT` plan
			// is in that slice, and finalizing without it published an empty
			// aggregate: zero groups where PostgreSQL has 100 (#783).
			//
			// It goes to p.Sink directly rather than through
			// partitionAndDeliver: routing needs owners, and the owners are
			// the workers that are not being launched. One sink consuming
			// every row is trivially disjoint — but the flag is cleared
			// anyway, because it is an assertion about a partitioning that
			// did not happen.
			if usePartitioned {
				primaryAgg.PartitionedDisjoint = false
				for _, wb := range pendingWarmup {
					if err := p.Sink.Consume(ctx, wb); err != nil {
						return fmt.Errorf("sink consume: %w", err)
					}
					wb.Release()
				}
				pendingWarmup = nil
			}
			return p.Sink.Finalize(ctx)
		}
	}

	// Build cloned op chains for each worker. Worker 0 reuses the original ops.
	workers := p.Workers
	opChains := make([][]UnaryOperator, workers)
	opChains[0] = p.Ops
	for i := 1; i < workers; i++ {
		chain := make([]UnaryOperator, len(p.Ops))
		for j, op := range p.Ops {
			chain[j] = op.(Cloneable).Clone()
		}
		// Init only truly new cloned ops — skip ops that are the same
		// instance as the original (e.g., Limit.Clone() returns self to
		// share atomic counters; re-Init would reset those counters).
		for j, cop := range chain {
			if cop == p.Ops[j] {
				continue // shared instance, already initialized
			}
			if err := cop.Init(ctx); err != nil {
				return fmt.Errorf("cloned operator init: %w", err)
			}
		}
		opChains[i] = chain
		p.trackCloneOps(chain)
	}

	// Per-worker sinks for MergeableSink: each worker gets its own sink
	// to avoid mutex contention. After all workers finish, partial sinks
	// are merged into the primary sink.
	var workerSinks []Sink
	mergeable, isMergeable := p.Sink.(MergeableSink)
	if isMergeable {
		workerSinks = make([]Sink, workers)
		workerSinks[0] = p.Sink
		for i := 1; i < workers; i++ {
			cloned := mergeable.CloneSink()
			wireCloneSinkSpill(cloned, p.Sink, workers)
			if usePartitioned {
				cs := cloned.(*HashAggregate)
				cs.PartitionedDisjoint = true
				// Ownership means clone state is DISJOINT — total across
				// clones ≈ what a serial aggregate would hold, so the
				// duplicated-state bound budget/(2k) over-drains. Give each
				// partition its fair share with headroom.
				if prim := p.Sink.(*HashAggregate); prim.Spill != nil {
					if b := prim.Spill.Tracker().Budget(); b > 0 {
						cs.PartialDrainBytes = b * 3 / int64(4*workers)
					}
				}
			}
			if err := cloned.Init(ctx); err != nil {
				return fmt.Errorf("cloned sink init: %w", err)
			}
			workerSinks[i] = cloned
			p.trackCloneSink(cloned)
		}
	}

	// Collect DoneSignalers from the original chain (shared across workers
	// for Limit, which uses atomics).
	var doneSignalers []DoneSignaler
	for _, op := range p.Ops {
		if ds, ok := op.(DoneSignaler); ok {
			doneSignalers = append(doneSignalers, ds)
		}
	}

	drainPhase := sourceServesHeldState(p.Source)

	// Launch workers.
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// From here every producer slot is owed by a worker that will run and
	// call stopProducing, so the early-return release above must not fire.
	// Set before the loop, not inside it: a panic between two iterations
	// would otherwise leave the count released twice.
	workersSpawned = true
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int, ops []UnaryOperator) {
			defer wg.Done()
			// A panic raised here escapes Run's own recover — it happens on
			// this goroutine, not the caller's — and takes the process with
			// it. Convert ANY of them to the first error and cancel (#511);
			// a FatalEvalPanic still arrives as its own precise error.
			defer CatchQueryPanic(workerCtx, "pipeline worker", func(err error) {
				firstErr.Set(err)
				cancel()
			})
			// Each worker uses its own sink when MergeableSink, else shared sink
			var sink Sink
			if workerSinks != nil {
				sink = workerSinks[workerID]
			} else {
				sink = p.Sink
			}
			// Partitioned mode: however the pull loop exits, this worker
			// stops producing and then drains its own partition queue until
			// every producer is done and the closer shuts the queues.
			producing := usePartitioned
			stopProducing := func() {
				if producing {
					producing = false
					producersWG.Done()
					drainPartitionQueue(workerCtx, sink, partQueues[workerID])
				}
			}
			// Per-worker routing scratch (histogram + per-row hash buffer).
			// The routed row-id/hash arrays it hands out are allocated per
			// batch because they travel to other workers' queues.
			var partScratch partitionScratch
			if usePartitioned {
				defer stopProducing()
				if workerID == 0 && pendingWarmup != nil {
					for _, wb := range pendingWarmup {
						if err := partitionAndDeliver(workerCtx, primaryAgg, sink, wb, 0, partQueues, &partScratch); err != nil {
							firstErr.Set(err)
							cancel()
							return
						}
					}
					pendingWarmup = nil
				}
			}
			driver := NewChainDriver(ops, func(ctx context.Context, b *batch.RecordBatch) error {
				FlattenForConsumer(b, sink)
				if usePartitioned {
					return partitionAndDeliver(ctx, primaryAgg, sink, b, workerID, partQueues, &partScratch)
				}
				if err := sink.Consume(ctx, b); err != nil {
					return fmt.Errorf("sink consume: %w", err)
				}
				b.Release()
				return nil
			}).ReleaseInputs()
			for {
				// Check for cancellation or done signaling.
				if workerCtx.Err() != nil {
					return
				}
				for _, ds := range doneSignalers {
					if ds.Done() {
						return
					}
				}

				// Heap-aware backpressure (parallel variant). All workers
				// pause concurrently when fired, so the system-wide pause
				// is the same 50ms regardless of Workers count. Drain-phase
				// pipelines are exempt (see HeldStateSource). A breaker
				// sink holding the dominant tracked share drains instead
				// of sleeping (#326) — its DrainOnHeapPressure serializes
				// on the operator mutex, so concurrent workers cannot
				// double-drain.
				if err := pauseOrDrainUnless(workerCtx, drainPhase, sink); err != nil {
					if workerCtx.Err() == nil {
						firstErr.Set(err)
						cancel()
					}
					return
				}

				b, err := p.Source.Next(workerCtx)
				if err != nil {
					if workerCtx.Err() != nil {
						return // context cancelled, not a real error
					}
					firstErr.Set(fmt.Errorf("source next: %w", err))
					cancel()
					return
				}
				if b == nil {
					return // source exhausted
				}

				exhausted, err := driver.Push(workerCtx, b)
				if err != nil {
					// The chain driver now fails fast on a cancelled context
					// (#368). When only workerCtx is cancelled — another
					// worker hit LIMIT and called cancel() — that is the
					// pipeline stopping itself, not a failure. A cancelled
					// PARENT context stays an error: the caller must never
					// see a silently truncated result as success.
					if workerCtx.Err() != nil && ctx.Err() == nil {
						return
					}
					firstErr.Set(err)
					cancel()
					return
				}

				if exhausted {
					cancel() // signal other workers to stop
					return
				}
			}
		}(i, opChains[i])
	}

	wg.Wait()

	// Check for worker errors.
	if err := firstErr.Err(); err != nil {
		return err
	}
	// Workers that noticed a cancelled parent context return silently, so a
	// CancelRequest used to yield whatever the workers had collected, with a
	// nil error — the pgwire layer had to defend against rows from a cancelled
	// statement (#368). Surface the cancellation as the error it is.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pipeline cancelled: %w", err)
	}

	// Flush spilled partition data (use original op chain — worker 0). This
	// runs BEFORE the demotion check and BEFORE the merge, and in partitioned
	// mode it ROUTES: a grace-partitioned join replays its spilled probe rows
	// here, and those rows carry group keys whose owner is some clone's sink.
	// Consuming them into the primary instead — which is what happened until
	// #782 — creates a second copy of every one of those keys beside the
	// clones' own, and adoption then emits both: 15 rows for PostgreSQL's 8,
	// split at a point that moves with probe-spill timing. Ordering matters
	// as much as routing: after adoption the clones' state is no longer a
	// place a row can be added to, and the routeFallback demotion below only
	// covers batches the WORKERS could not route.
	if usePartitioned {
		var flushScratch partitionScratch
		if err := p.flushSpilledOps(ctx, p.Ops, func(ctx context.Context, b *batch.RecordBatch) error {
			FlattenForConsumer(b, p.Sink)
			return partitionAndConsumeOwned(ctx, primaryAgg, workerSinks, b, &flushScratch)
		}); err != nil {
			return err
		}
	} else if err := p.flushSpilledOps(ctx, p.Ops, p.consumeIntoSink); err != nil {
		return err
	}

	// A batch the router could not hash was consumed whole by the worker
	// that pulled it, so several sinks may now hold the same group key and
	// adoption would emit that group once per sink (#338: GROUP BY over a
	// column the router cannot hash returned one partial row per worker,
	// counts split k ways). Demote to the ordinary merge, which combines
	// partial states by key.
	if usePartitioned && primaryAgg.routeFallback.Load() {
		usePartitioned = false
		primaryAgg.PartitionedDisjoint = false
		for i := 1; i < workers; i++ {
			if cs, ok := workerSinks[i].(*HashAggregate); ok {
				cs.PartitionedDisjoint = false
			}
		}
	}

	// Merge per-worker partial sinks into the primary sink. In partitioned
	// mode the primary ADOPTS clone state (streams it at Next) and owns the
	// clones' lifecycle — closing them here would free live state.
	if isMergeable {
		for i := 1; i < workers; i++ {
			mergeable.MergeSink(workerSinks[i].(SinkSource))
			if !usePartitioned {
				workerSinks[i].Close()
			}
		}
		// Ownership has moved: closed above, or adopted by the primary
		// (whose Close closes its adoptedPartitions). Anything Close()
		// still tracks after this point would be a double close.
		p.releaseCloneSinks()
	}

	// Close cloned ops (workers 1..N).
	for i := 1; i < workers; i++ {
		for _, op := range opChains[i] {
			op.Close()
		}
	}
	p.releaseCloneOps()

	return p.Sink.Finalize(ctx)
}

// Close releases all resources in the pipeline.
func (p *Pipeline) Close() error {
	// Clones first: a clone sink's Close is what removes its aggregate
	// partial-state and drained-run files, and on an error or cancel path
	// runParallel never got to its own teardown (#625 M2).
	firstErr := p.closeTrackedClones()
	if err := p.Source.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, op := range p.Ops {
		if err := op.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := p.Sink.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// DualSource emits a single batch with 1 row and 0 columns, then EOF.
// Used for table-less SELECT (e.g., SELECT CURRENT_DATE, SELECT 1+1).
type DualSource struct {
	done bool
}

func (d *DualSource) Init(_ context.Context) error { return nil }

func (d *DualSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if d.done {
		return nil, nil
	}
	d.done = true
	return &batch.RecordBatch{Len: 1}, nil
}

func (d *DualSource) Close() error { return nil }

// BatchSource is a source that yields pre-loaded record batches.
type BatchSource struct {
	batches []*batch.RecordBatch
	idx     int
}

// NewBatchSource creates a source from pre-loaded record batches.
func NewBatchSource(batches []*batch.RecordBatch) *BatchSource {
	return &BatchSource{batches: batches}
}

func (s *BatchSource) Init(_ context.Context) error { return nil }

func (s *BatchSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.batches[s.idx] = nil // release reference so GC can reclaim after spill
	s.idx++
	return b, nil
}

func (s *BatchSource) Close() error { return nil }

// SliceSource is a simple Source that yields batches from a slice of rows.
type SliceSource struct {
	schema []parquet.Column
	rows   []map[string]any
	offset int
	pool   *batch.BatchPool
	mu     sync.Mutex
}

// Column is imported from parquet for convenience.
type Column = parquet.Column

// NewSliceSource creates a source from in-memory rows.
func NewSliceSource(schema []Column, rows []map[string]any) *SliceSource {
	return &SliceSource{schema: schema, rows: rows}
}

func (s *SliceSource) Init(_ context.Context) error {
	s.pool = batch.NewBatchPool(s.schema, batch.DefaultBatchSize)
	s.offset = 0
	return nil
}

func (s *SliceSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	if s.offset >= len(s.rows) {
		s.mu.Unlock()
		return nil, nil
	}

	end := s.offset + batch.DefaultBatchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}

	chunk := s.rows[s.offset:end]
	s.offset = end
	s.mu.Unlock()

	return batch.FromRows(s.schema, chunk), nil
}

func (s *SliceSource) Close() error { return nil }

// ErrCollectBudget is returned by CollectSink.Consume when the accumulated
// result exceeds CollectSink.MaxBytes. Callers match with errors.Is to
// distinguish "result too big for this execution path" from real failures.
var ErrCollectBudget = errors.New("collect sink result budget exceeded")

// CollectSink collects all consumed batches. Data is stored columnar internally.
// Rows are converted lazily on first access to ToRows(), not during Finalize.
// Use Batches() for zero-copy columnar access.
// Thread-safe: Consume() is protected by a mutex for parallel pipeline workers.
// BatchSink is a Sink that only stores RecordBatches, never converting to
// rows. Use it from internal pipelines (e.g. reverseBloomBridge) that consume
// the batches directly and never need the row representation. CollectSink's
// Finalize unconditionally calls ToRows() for backward compatibility, which
// is unnecessary work and — more importantly — panics if any batch has a
// row count beyond the underlying bitmap capacity (as we hit when the
// reverseBloomBridge collected probe-side batches at SF100).
type BatchSink struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
}

func (s *BatchSink) Init(_ context.Context) error {
	s.mu.Lock()
	s.batches = nil
	s.mu.Unlock()
	return nil
}

func (s *BatchSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	FlattenForConsumer(b, nil) // retained past the batch cycle: views must not survive
	b.Detach()
	// Snapshot the selection vector. Many UnaryOperators (KernelFilter,
	// AndFilter, OrFilter, the comparison/expression filters) reuse a
	// per-instance scratch buffer for the output Sel and then assign
	// `in.Sel = sel`. The returned batch's Sel field is a slice into that
	// scratch buffer, which gets clobbered on the operator's next Execute
	// call. Sinks that hold batches across calls (like this one — the
	// reverse-bloom bridge collects the entire child pipeline before
	// consuming) would otherwise see garbage Sel data on later iteration.
	if b.Sel != nil {
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
	s.batches = append(s.batches, b)
	s.mu.Unlock()
	return nil
}

func (s *BatchSink) Finalize(_ context.Context) error { return nil }
func (s *BatchSink) Close() error                     { return nil }

// Batches returns the collected RecordBatches. Safe to call after Finalize.
func (s *BatchSink) Batches() []*batch.RecordBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batches
}

type CollectSink struct {
	Rows       []map[string]any     // populated lazily on first access
	rowValues  [][]any              // positional form; see ToRowValues
	batches    []*batch.RecordBatch // columnar storage; released by ToRows
	schema     []parquet.Column     // captured from the first batch; survives ToRows
	rowsDone   bool
	accumBytes int64 // running MemBytes of collected batches
	// MaxBytes, when >0, bounds the collected result: Consume returns
	// ErrCollectBudget once accumulated batch bytes exceed it. Callers that
	// have a cheaper place to put oversized results (the coordinator's
	// local fast path re-dispatches to the DAG, whose gather spills to
	// scratch) set this to bail out instead of growing the heap unboundedly.
	MaxBytes int64
	mu       sync.Mutex
	// SkipFinalizeToRows, when true, makes Finalize a no-op instead of
	// eagerly materializing ToRows. Callers that consume Batches() directly
	// (native-DAG worker stage path) should set this — otherwise Finalize
	// allocates a map[string]any per row of every collected batch.
	// At SF10 Q18, this single allocation pattern held 21 GB of live heap
	// (heap-1347079-130.pprof: CollectSink.ToRows = 68% inuse_space —
	// project_q18_sf10_native_dag_oom_2026-04-24).
	SkipFinalizeToRows bool
	// SchemaHint is the PLAN-DERIVED output schema, set by the planner
	// before the pipeline runs. Schema() falls back to it when the sink
	// consumed nothing, which is the only case it can be needed and the only
	// case it is consulted.
	//
	// A query's output schema is a property of the PLAN, not of the data,
	// but this sink learned it from the first batch it consumed — so
	// `SELECT a, b FROM t WHERE false` had no schema, and every route that
	// reads names or types off this sink handed the client nothing:
	// pgwire's coord path then declared OID 25 (text) for every column, and
	// the coordinator's correlated-local route a RowDescription with ZERO
	// FIELDS — not an empty table with headers, no table at all (#416).
	//
	// It is a HINT, not an override: a consumed batch always wins, because
	// the runtime saw the vectors and the planner only predicted them. Init
	// deliberately does not clear it — it is configuration the planner
	// attaches once, not per-run state.
	SchemaHint []parquet.Column
	// OutputNames is the PLAN's published name per output column, positional
	// (plansql.OutputColumnName, #732). Empty means "the names the pipeline
	// emitted", which is every caller but the top-level query planner: a
	// worker fragment's sink publishes the names the NEXT stage resolves
	// against, not the ones a client reads.
	OutputNames []string
	namesDone   bool
	// SchemaHintWireUnconstrainedDecimal names the DECIMAL output columns
	// whose PostgreSQL wire typmod must say "unconstrained" (-1) — an
	// aggregate function call, never a bare column reference. Unlike
	// SchemaHint, this is consulted for EVERY result, zero-row or not:
	// which columns are aggregate output is a property of the PLAN, not of
	// whether Consume ever ran (FIX 2, #457/#458 fold-in).
	SchemaHintWireUnconstrainedDecimal map[string]bool
	// SchemaHintStringLength names the output columns whose declaration
	// carries a string LENGTH — `CAST(x AS VARCHAR(4))` — and what it is.
	// Consulted on every result for the same reason as the field above: which
	// columns a CAST bounds is a property of the PLAN (#838).
	SchemaHintStringLength map[string]int
}

func (s *CollectSink) Init(_ context.Context) error {
	s.Rows = nil
	s.rowValues = nil
	s.batches = nil
	s.schema = nil
	s.rowsDone = false
	return nil
}

func (s *CollectSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	if s.schema == nil {
		s.schema = b.Schema
	}
	FlattenForConsumer(b, nil) // retained past the batch cycle: views must not survive
	b.Detach()                 // prevent pool recycle — pipeline calls Release() after Consume()
	// A RESULT batch may not carry a shape-only column. The scan decodes one
	// as lengths-and-no-bytes only because the planner proved every use of it
	// reads its shape, and the analysis enforces that by refusing to run at
	// all unless the plan's output comes from a Project or an Aggregate —
	// whose output schema is an explicit list in which a column is a VALUE
	// use. So this cannot happen, and that is exactly why it is checked here:
	// since #791 the row boundary hands back batch.ShapeOnlyLen instead of
	// panicking, and a shape-only column that DID reach the client would come
	// out as an integer where a string belongs. Loud, at the boundary that
	// knows, rather than plausible in a result set.
	for j, col := range b.Columns {
		if col.IsShapeOnly() {
			name := ""
			if j < len(b.Schema) {
				name = b.Schema[j].Name
			}
			s.mu.Unlock()
			return fmt.Errorf("result column %q is shape-only: the scan decoded its lengths and "+
				"no bytes, so its values are not available to a client — some consumer of this "+
				"column is not a shape consumer (planner analysis bug)", name)
		}
	}
	// Snapshot the selection vector — see BatchSink.Consume for the full
	// rationale. Filter operators reuse outSel across calls; sinks that
	// hold batches across calls would otherwise see clobbered Sel data.
	if b.Sel != nil {
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
	s.batches = append(s.batches, b)
	s.accumBytes += b.MemBytes()
	over := s.MaxBytes > 0 && s.accumBytes > s.MaxBytes
	s.mu.Unlock()
	if over {
		return fmt.Errorf("collect sink at %d bytes: %w", s.accumBytes, ErrCollectBudget)
	}
	return nil
}

func (s *CollectSink) Finalize(_ context.Context) error {
	if s.SkipFinalizeToRows {
		return nil
	}
	s.convert() // populate Rows for backward compatibility
	return nil
}

// ToRows returns all results as rows, converting from batches on first call.
// Each batch reference is dropped as it is boxed: holding both forms alive
// for the sink's lifetime doubled the result's residency (columnar + boxed)
// on every row-consuming path. Dropping is safe — Consume Detach()ed the
// batches from their pools, so the arenas that boxed TypeBytes values alias
// are never recycled out from under them. Batches() returns nil after this;
// use Schema() for post-conversion schema access.
func (s *CollectSink) ToRows() []map[string]any {
	s.convert()
	return s.Rows
}

// ToRowValues returns the result POSITIONALLY — one []any per row, aligned
// with Schema() — or nil when the map form is already lossless.
//
// A result may legally carry two output columns of the SAME NAME: PostgreSQL
// answers `SELECT abs(a), abs(b)` with two columns both called `abs`, and
// #513 made this engine agree. A map keyed by name cannot hold both, so the
// second overwrites the first and a consumer reads column 0's value under
// column 1's name — a wrong VALUE, which is strictly worse than a wrong name.
//
// nil is the answer when the schema's names are unique, and it is not a
// hedge: it says the map IS the positional form, and a caller reading it by
// name gets the same cells. Materializing a []any per row unconditionally
// would add to a sink that has been measured at 68% of inuse_space on large
// results (SkipFinalizeToRows' comment), for a shape almost no query has,
// so the cost is paid exactly where the loss would otherwise happen.
func (s *CollectSink) ToRowValues() [][]any {
	s.convert()
	return s.rowValues
}

// convert boxes the batches once, into the map form and — when the schema
// makes the map lossy — the positional form as well. Both share the same
// value boxes, so the second form costs one slice header per row.
func (s *CollectSink) convert() {
	if s.rowsDone {
		return
	}
	s.rowsDone = true
	s.applyOutputNames()
	positional := hasDuplicateColumnName(s.Schema())
	for i, b := range s.batches {
		s.Rows = append(s.Rows, b.ToRows()...)
		if positional {
			s.rowValues = append(s.rowValues, b.ToRowValues()...)
		}
		s.batches[i] = nil
	}
	s.batches = nil
}

// hasDuplicateColumnName reports whether two columns of a schema share a
// name, which is exactly when boxing rows into a map loses a value.
func hasDuplicateColumnName(schema []parquet.Column) bool {
	if len(schema) < 2 {
		return false
	}
	seen := make(map[string]bool, len(schema))
	for _, col := range schema {
		if seen[col.Name] {
			return true
		}
		seen[col.Name] = true
	}
	return false
}

// Batches returns the raw columnar batches (zero-copy, no conversion).
// Returns nil once ToRows has converted the result.
func (s *CollectSink) Batches() []*batch.RecordBatch {
	return s.batches
}

// Schema returns the schema of the first consumed batch, or the planner's
// SchemaHint when no batch was consumed. Unlike Batches()[0].Schema, it
// remains available after ToRows releases the batches.
func (s *CollectSink) Schema() []parquet.Column {
	s.applyOutputNames()
	if s.schema == nil {
		return s.SchemaHint
	}
	return s.schema
}

// applyOutputNames renames this sink's output columns to the names the CLIENT
// is owed, positionally.
//
// PostgreSQL names an unaliased SELECT item by its own rule — `?column?` for an
// operator expression, the function's name for a call, the ARGUMENT's name for
// a cast (plansql.OutputColumnName, #732) — and that name is not always the one
// the engine RESOLVES the value by. An aggregate's output column IS its
// `AggExpr.OutputCol`, which GROUP BY, HAVING, ORDER BY and the stage's rename
// source all spell against, and two unaliased aggregates legally publish ONE
// name (`SELECT COUNT(*), COUNT(g)` is two columns called `count`). So the
// published name is applied HERE, at the boundary where the values leave the
// engine and nothing resolves by name any more — not inside the plan, where an
// earlier attempt broke a sort key that named the column the projection had
// just renamed away.
//
// Positional, and only when the arity matches: the sink's schema is what the
// pipeline actually emitted, and a plan that emits a different number of
// columns from the SELECT list (a hidden ORDER BY term that outlived its trim)
// is one this rename must not reorder names across.
func (s *CollectSink) applyOutputNames() {
	if len(s.OutputNames) == 0 || s.namesDone {
		return
	}
	s.namesDone = true
	rename := func(schema []parquet.Column) []parquet.Column {
		if len(schema) != len(s.OutputNames) {
			return schema
		}
		out := append([]parquet.Column(nil), schema...)
		for i := range out {
			if s.OutputNames[i] != "" {
				out[i].Name = s.OutputNames[i]
			}
		}
		return out
	}
	s.schema = rename(s.schema)
	s.SchemaHint = rename(s.SchemaHint)
	for _, b := range s.batches {
		if b != nil {
			b.Schema = rename(b.Schema)
		}
	}
}

func (s *CollectSink) Close() error { return nil }

// wireCloneSinkSpill is the embedded-pipeline mirror of the worker
// executor's clone spill wiring (executor_fragment.go): clone sinks charge
// a tracking-only SpillManager view so their state is visible to the
// memory ledger, and HashAggregate clones get a partial-drain bound
// because they cannot spill themselves — without it a high-cardinality
// GROUP BY accumulates ~the full key set in EVERY clone, k× serial state,
// invisible to the tracker. That exact shape OOM-killed the ClickBench
// c6a recon on Q19 (heap 30.5GB, reclaimable 0, GOMEMLIMIT 27.6GB) — the
// protection previously existed only on the distributed path.
func wireCloneSinkSpill(cloned Sink, primary Sink, workers int) {
	switch cs := cloned.(type) {
	case *HashAggregate:
		if prim, ok := primary.(*HashAggregate); ok && prim.Spill != nil {
			cs.Spill = prim.Spill.TrackingOnlyView()
			if b := prim.Spill.Tracker().Budget(); b > 0 && workers > 0 {
				cs.PartialDrainBytes = b / int64(2*workers)
			}
		}
	case *Sort:
		if prim, ok := primary.(*Sort); ok && prim.Spill != nil {
			cs.Spill = prim.Spill.TrackingOnlyView()
		}
	}
}
