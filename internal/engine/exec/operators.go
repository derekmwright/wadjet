// Package exec provides the push-based pipeline execution framework.
package exec

import (
	"context"

	"github.com/derekmwright/caelum/internal/engine/batch"
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

// ScanStatsProvider is implemented by sources that can report scan statistics.
type ScanStatsProvider interface {
	RowsScanned() int64
}
