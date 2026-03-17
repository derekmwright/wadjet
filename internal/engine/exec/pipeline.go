package exec

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Pipeline represents Source → [UnaryOps...] → Sink.
type Pipeline struct {
	Source Source
	Ops    []UnaryOperator
	Sink   Sink
}

// Run executes the pipeline by pulling from source, transforming through
// operators, and pushing to sink. Designed to be run per goroutine (one per partition).
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

	for {
		// Check for context cancellation (timeout, user cancel)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("pipeline cancelled: %w", err)
		}

		b, err := p.Source.Next(ctx)
		if err != nil {
			return fmt.Errorf("source next: %w", err)
		}
		if b == nil {
			break
		}

		exhausted := false
		for _, op := range p.Ops {
			b, err = op.Execute(ctx, b)
			if err != nil {
				return fmt.Errorf("operator execute: %w", err)
			}
			if b == nil {
				// Check if operator is done (e.g., LIMIT satisfied)
				if ds, ok := op.(DoneSignaler); ok && ds.Done() {
					exhausted = true
				}
				break // fully filtered out
			}
		}

		if b != nil {
			if err := p.Sink.Consume(ctx, b); err != nil {
				return fmt.Errorf("sink consume: %w", err)
			}
		}

		if exhausted {
			break // early termination: operator signaled completion
		}
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

// SliceSource is a simple Source that yields batches from a slice of rows.
type SliceSource struct {
	schema []parquet.Column
	rows   []map[string]any
	offset int
	pool   *batch.BatchPool
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
	if s.offset >= len(s.rows) {
		return nil, nil
	}

	end := s.offset + batch.DefaultBatchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}

	chunk := s.rows[s.offset:end]
	s.offset = end

	return batch.FromRows(s.schema, chunk), nil
}

func (s *SliceSource) Close() error { return nil }

// CollectSink collects all consumed batches. Data is stored columnar internally.
// Rows are converted lazily on first access to ToRows(), not during Finalize.
// Use Batches() for zero-copy columnar access.
type CollectSink struct {
	Rows    []map[string]any     // populated lazily on first access
	batches []*batch.RecordBatch // columnar storage
	rowsDone bool
}

func (s *CollectSink) Init(_ context.Context) error {
	s.Rows = nil
	s.batches = nil
	s.rowsDone = false
	return nil
}

func (s *CollectSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.batches = append(s.batches, b)
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
