// Package exec provides the push-based pipeline execution framework.
package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// Source produces batches (table scan, hash table probe side).
type Source interface {
	Init(ctx context.Context) error
	Next(ctx context.Context) (*batch.RecordBatch, error) // nil batch = end of data
	Close() error
}

// UnaryOperator transforms batches in-place (filter, project) — non-blocking.
type UnaryOperator interface {
	Init(ctx context.Context) error
	Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) // nil = fully filtered
	Close() error
}

// Sink consumes all input before results can be read (pipeline breaker).
// Must handle concurrent Consume() calls from multiple goroutines.
type Sink interface {
	Init(ctx context.Context) error
	Consume(ctx context.Context, b *batch.RecordBatch) error
	Finalize(ctx context.Context) error
	Close() error
}

// SinkSource is a Sink that can also act as a Source after Finalize (e.g., hash aggregate).
type SinkSource interface {
	Sink
	Source
}

// Cloneable is implemented by operators that can be cloned for parallel
// pipeline execution. Each clone gets its own scratch buffers so that
// multiple goroutines can call Execute concurrently without data races.
type Cloneable interface {
	Clone() UnaryOperator
}

// DoneSignaler is implemented by operators (like Limit) that can signal early
// pipeline termination. When Done() returns true, the pipeline stops pulling
// from the source, enabling LIMIT pushdown without scanning the full table.
type DoneSignaler interface {
	Done() bool
}

// MergeableSink is a SinkSource that supports per-worker partial aggregation.
// When the pipeline has multiple workers, each worker gets its own cloned sink.
// After all workers finish, partial sinks are merged into the primary sink.
type MergeableSink interface {
	SinkSource
	CloneSink() SinkSource
	MergeSink(other SinkSource)
}

// ScanStatsProvider is implemented by sources that can report scan statistics.
type ScanStatsProvider interface {
	RowsScanned() int64
}
