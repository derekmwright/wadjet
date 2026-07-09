package exec

import (
	"context"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// Late-materialization runtime counters. Process-wide, monotonic — they exist
// so every A/B arm can PROVE the treatment engaged (the dynamic-filter lesson:
// a silent feature that never fires reads as a wash). Zero while the flag is
// off is the dormancy assertion; >0 under the flag is the engagement marker.
var (
	// LateMatBatchesEmitted counts join-probe output batches emitted in view
	// (reference) form instead of eagerly gathered.
	LateMatBatchesEmitted atomic.Int64
	// LateMatViewColumns counts view columns across those batches.
	LateMatViewColumns atomic.Int64
	// LateMatFlattens counts FlattenViews calls made on behalf of consumers
	// that don't accept views — each one is a deferred gather finally paid.
	LateMatFlattens atomic.Int64
	// LateMatViewColumnsSerialized counts view columns serialized straight
	// through their indirection by the shuffle writers (no flatten copy at
	// all) — the phase-4 engagement marker.
	LateMatViewColumnsSerialized atomic.Int64
)

// ViewAware marks an operator or sink that can consume batches containing
// view (dictionary) columns directly — composing or reading through the
// indirection — so the pipeline skips the defensive flatten in front of it.
type ViewAware interface {
	AcceptsViews() bool
}

// FlattenForConsumer materializes any view columns before handing the batch
// to a consumer that hasn't declared view-awareness. This is the structural
// correctness guarantee: no operator or sink ever sees a view unless it
// opted in, so a missed call site degrades to an eager copy, never to
// reading nil typed slices. Exported because manual operator-chain loops
// exist outside this package (physical.pipelineSource, the worker fragment
// runner) and must apply the same guard the Pipeline loops do.
func FlattenForConsumer(b *batch.RecordBatch, consumer any) {
	if b == nil || !b.HasViews() {
		return
	}
	if va, ok := consumer.(ViewAware); ok && va.AcceptsViews() {
		return
	}
	b.FlattenViews()
	LateMatFlattens.Add(1)
}

// flattenSource wraps a Source so every pulled batch is view-free. It guards
// consumers that pull directly from a Source outside any pipeline loop —
// HashJoin.Build and its partition-on-arrival / key-only variants, which
// retain or read typed storage from what they pull.
type flattenSource struct {
	inner Source
}

func (f *flattenSource) Init(ctx context.Context) error { return f.inner.Init(ctx) }
func (f *flattenSource) Close() error                   { return f.inner.Close() }
func (f *flattenSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	b, err := f.inner.Next(ctx)
	FlattenForConsumer(b, nil)
	return b, err
}
