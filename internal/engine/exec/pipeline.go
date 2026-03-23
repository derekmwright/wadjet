package exec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
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
	batchCount := 0
	for {
		if batchCount&63 == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("pipeline cancelled: %w", err)
			}
		}
		batchCount++

		b, err := p.Source.Next(ctx)
		if err != nil {
			return fmt.Errorf("source next: %w", err)
		}
		if b == nil {
			break
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
// Used by scan-split pipeline mode where scan I/O is distributed across
// workers and the compute pipeline reads from materialized scan results.
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
type CollectSink struct {
	Rows    []map[string]any     // populated lazily on first access
	batches []*batch.RecordBatch // columnar storage
	rowsDone bool
	mu       sync.Mutex
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
	s.batches = append(s.batches, b)
	s.mu.Unlock()
	return nil
}

func (s *CollectSink) Finalize(_ context.Context) error {
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
