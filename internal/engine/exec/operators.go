// Package exec provides the push-based pipeline execution framework.
package exec

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
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

// FlushableOperator is implemented by operators that may need to emit
// additional batches after the main pipeline loop completes. Used by the
// Grace Hash Join to process spilled partitions after the streaming probe.
type FlushableOperator interface {
	HasPendingFlush() bool
	NextFlush(ctx context.Context) (*batch.RecordBatch, error)
}

// BoundedOutputOperator is implemented by operators whose output for a single
// input batch can be far larger than that batch — a hash-join probe fans one
// probe row out to every build row sharing its key — and which can therefore
// suspend mid-input and emit the remainder across later calls.
//
// The protocol is opt-in, and the opt-in is a promise by the driver:
// EnableBoundedOutput says "after every Execute I will drain NextOutput until
// HasPendingOutput reports false, before handing you another input batch, and
// I will keep the input batch alive until then". Until a driver opts in the
// operator emits an input's whole output from Execute in one batch, so chain
// drivers that do not implement the protocol keep working unchanged.
//
// Bounding the producer is what makes back-pressure possible at all: an
// operator that materialises O(batch x fan-out) rows in one live allocation
// gives the memory tracker nothing to reclaim and blows straight through
// GOMEMLIMIT, because the memory is live rather than garbage (#317).
type BoundedOutputOperator interface {
	EnableBoundedOutput()
	HasPendingOutput() bool
	NextOutput(ctx context.Context) (*batch.RecordBatch, error)
}

// enableBoundedOutput opts every operator in a chain that supports the
// bounded-output protocol into it. Only call it from a driver that drains
// NextOutput after each Execute.
func enableBoundedOutput(ops []UnaryOperator) {
	for _, op := range ops {
		if bo, ok := op.(BoundedOutputOperator); ok {
			bo.EnableBoundedOutput()
		}
	}
}

// ScanStatsProvider is implemented by sources that can report scan statistics.
type ScanStatsProvider interface {
	RowsScanned() int64
}
