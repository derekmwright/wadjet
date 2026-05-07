package exec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Pipeline represents Source → [UnaryOps...] → Sink.
type Pipeline struct {
	Source  Source
	Ops    []UnaryOperator
	Sink   Sink
	Workers int // number of parallel workers (0 or 1 = serial)
}

// Run executes the pipeline by pulling from source, transforming through
// operators, and pushing to sink. When Workers > 1 and all operators
// implement Cloneable, batches are processed by multiple goroutines
// concurrently. Otherwise falls back to serial execution.
func (p *Pipeline) Run(ctx context.Context) error {
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

	if p.Workers > 1 && p.allOpsCloneable() {
		return p.runParallel(ctx)
	}
	return p.runSerial(ctx)
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
	batchCount := 0
	for {
		if batchCount&63 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("pipeline cancelled: %w", err)
			}
		}
		batchCount++

		// Heap-aware backpressure: when process heap is approaching
		// GOMEMLIMIT, pause briefly so GC can reclaim before pulling more
		// data. Cheap (cached check) when no pressure; sleeps 50ms when
		// fired. See memory.HeapBackpressureActive for rationale.
		if err := memory.PauseOnHeapBackpressure(ctx); err != nil {
			return err
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

		exhausted := false
		for _, op := range p.Ops {
			prev := b
			b, err = op.Execute(ctx, b)
			if err != nil {
				return fmt.Errorf("operator execute: %w", err)
			}
			// Release intermediate batch when operator created a new one.
			// Returns source batches to scan pool, probe output to probe pool.
			if b != prev && prev != nil {
				prev.Release()
			}
			if b == nil {
				if ds, ok := op.(DoneSignaler); ok && ds.Done() {
					exhausted = true
				}
				break
			}
		}

		if b != nil {
			if err := p.Sink.Consume(ctx, b); err != nil {
				return fmt.Errorf("sink consume: %w", err)
			}
			b.Release()
		}

		if exhausted {
			break
		}
	}

	// Flush spilled partition data from Grace Hash Join operators.
	// Results are pushed through the remaining operator chain and into the sink.
	if err := p.flushSpilledOps(ctx, p.Ops); err != nil {
		return err
	}

	return p.Sink.Finalize(ctx)
}

// flushSpilledOps checks each operator for pending spilled data and pushes
// results through the remaining operators into the sink.
func (p *Pipeline) flushSpilledOps(ctx context.Context, ops []UnaryOperator) error {
	for opIdx, op := range ops {
		fo, ok := op.(FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			continue
		}
		remainingOps := ops[opIdx+1:]
		for {
			b, err := fo.NextFlush(ctx)
			if err != nil {
				return fmt.Errorf("flushing spilled data: %w", err)
			}
			if b == nil {
				break
			}
			for _, rop := range remainingOps {
				prev := b
				b, err = rop.Execute(ctx, b)
				if err != nil {
					return fmt.Errorf("operator execute (flush): %w", err)
				}
				if b != prev && prev != nil {
					prev.Release()
				}
				if b == nil {
					break
				}
			}
			if b != nil {
				if err := p.Sink.Consume(ctx, b); err != nil {
					return fmt.Errorf("sink consume (flush): %w", err)
				}
				b.Release()
			}
		}
	}
	return nil
}

// runParallel processes batches through cloned operator chains in parallel.
// The source and sink are shared; each worker gets its own cloned operators.
func (p *Pipeline) runParallel(ctx context.Context) error {
	// Warm-up: process one batch through the original ops to resolve lazy
	// column indices in expressions (e.g., KernelFilter, ColumnCompare).
	// This ensures clones inherit resolved state where possible.
	warmupBatch, err := p.Source.Next(ctx)
	if err != nil {
		return fmt.Errorf("source next: %w", err)
	}
	if warmupBatch == nil {
		// No data at all — just finalize and return.
		return p.Sink.Finalize(ctx)
	}

	// Process warmup batch through original ops.
	{
		b := warmupBatch
		exhausted := false
		for _, op := range p.Ops {
			prev := b
			b, err = op.Execute(ctx, b)
			if err != nil {
				return fmt.Errorf("operator execute: %w", err)
			}
			if b != prev && prev != nil {
				prev.Release()
			}
			if b == nil {
				if ds, ok := op.(DoneSignaler); ok && ds.Done() {
					exhausted = true
				}
				break
			}
		}
		if b != nil {
			if err := p.Sink.Consume(ctx, b); err != nil {
				return fmt.Errorf("sink consume: %w", err)
			}
			b.Release()
			// Check DoneSignalers even when batch was non-nil — Limit
			// may have returned a truncated batch and is now satisfied.
			for _, op := range p.Ops {
				if ds, ok := op.(DoneSignaler); ok && ds.Done() {
					exhausted = true
				}
			}
		}
		if exhausted {
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
			if err := cloned.Init(ctx); err != nil {
				return fmt.Errorf("cloned sink init: %w", err)
			}
			workerSinks[i] = cloned
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

	// Launch workers.
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var firstErr atomic.Value // stores first error

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int, ops []UnaryOperator) {
			defer wg.Done()
			// Each worker uses its own sink when MergeableSink, else shared sink
			var sink Sink
			if workerSinks != nil {
				sink = workerSinks[workerID]
			} else {
				sink = p.Sink
			}
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
				// is the same 50ms regardless of Workers count.
				if err := memory.PauseOnHeapBackpressure(workerCtx); err != nil {
					if workerCtx.Err() == nil {
						firstErr.CompareAndSwap(nil, err)
						cancel()
					}
					return
				}

				b, err := p.Source.Next(workerCtx)
				if err != nil {
					if workerCtx.Err() != nil {
						return // context cancelled, not a real error
					}
					firstErr.CompareAndSwap(nil, fmt.Errorf("source next: %w", err))
					cancel()
					return
				}
				if b == nil {
					return // source exhausted
				}

				exhausted := false
				for _, op := range ops {
					prev := b
					b, err = op.Execute(workerCtx, b)
					if err != nil {
						firstErr.CompareAndSwap(nil, fmt.Errorf("operator execute: %w", err))
						cancel()
						return
					}
					if b != prev && prev != nil {
						prev.Release()
					}
					if b == nil {
						if ds, ok := op.(DoneSignaler); ok && ds.Done() {
							exhausted = true
						}
						break
					}
				}

				if b != nil {
					if err := sink.Consume(workerCtx, b); err != nil {
						firstErr.CompareAndSwap(nil, fmt.Errorf("sink consume: %w", err))
						cancel()
						return
					}
					b.Release()
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
	if v := firstErr.Load(); v != nil {
		return v.(error)
	}

	// Merge per-worker partial sinks into the primary sink.
	if isMergeable {
		for i := 1; i < workers; i++ {
			mergeable.MergeSink(workerSinks[i].(SinkSource))
			workerSinks[i].Close()
		}
	}

	// Close cloned ops (workers 1..N).
	for i := 1; i < workers; i++ {
		for _, op := range opChains[i] {
			op.Close()
		}
	}

	// Flush spilled partition data (use original op chain — worker 0).
	if err := p.flushSpilledOps(ctx, p.Ops); err != nil {
		return err
	}

	return p.Sink.Finalize(ctx)
}

// Close releases all resources in the pipeline.
func (p *Pipeline) Close() error {
	var firstErr error
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
	Rows    []map[string]any     // populated lazily on first access
	batches []*batch.RecordBatch // columnar storage
	rowsDone bool
	mu       sync.Mutex
	// SkipFinalizeToRows, when true, makes Finalize a no-op instead of
	// eagerly materializing ToRows. Callers that consume Batches() directly
	// (native-DAG worker stage path) should set this — otherwise Finalize
	// allocates a map[string]any per row of every collected batch.
	// At SF10 Q18, this single allocation pattern held 21 GB of live heap
	// (heap-1347079-130.pprof: CollectSink.ToRows = 68% inuse_space —
	// project_q18_sf10_native_dag_oom_2026-04-24).
	SkipFinalizeToRows bool
}

func (s *CollectSink) Init(_ context.Context) error {
	s.Rows = nil
	s.batches = nil
	s.rowsDone = false
	return nil
}

func (s *CollectSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	b.Detach() // prevent pool recycle — pipeline calls Release() after Consume()
	// Snapshot the selection vector — see BatchSink.Consume for the full
	// rationale. Filter operators reuse outSel across calls; sinks that
	// hold batches across calls would otherwise see clobbered Sel data.
	if b.Sel != nil {
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
	s.batches = append(s.batches, b)
	s.mu.Unlock()
	return nil
}

func (s *CollectSink) Finalize(_ context.Context) error {
	if s.SkipFinalizeToRows {
		return nil
	}
	s.ToRows() // populate Rows for backward compatibility
	return nil
}

// ToRows returns all results as rows, converting from batches on first call.
func (s *CollectSink) ToRows() []map[string]any {
	if !s.rowsDone {
		s.rowsDone = true
		for _, b := range s.batches {
			s.Rows = append(s.Rows, b.ToRows()...)
		}
	}
	return s.Rows
}

// Batches returns the raw columnar batches (zero-copy, no conversion).
func (s *CollectSink) Batches() []*batch.RecordBatch {
	return s.batches
}

func (s *CollectSink) Close() error { return nil }
