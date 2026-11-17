// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// failingSink refuses every batch, the way a HashAggregate refuses a batch
// whose columns it cannot resolve.
type failingSink struct {
	consumed int
	err      error
}

func (s *failingSink) Init(context.Context) error { return nil }
func (s *failingSink) Consume(context.Context, *batch.RecordBatch) error {
	s.consumed++
	return s.err
}
func (s *failingSink) Finalize(context.Context) error { return nil }
func (s *failingSink) Close() error                   { return nil }

// A partition owner that has stopped producing consumes the rest of its
// deliveries in drainPartitionQueue, and nobody else will ever see what that
// consume says. The drain used to drop the error ("surfaced by the worker
// that hit it first (or by Finalize)", which neither did), so an owner whose
// first batch arrived there stayed unresolved and emitted no group: a
// grouped aggregate over a computed second argument answered no rows where
// the ungrouped one refused (#1694). The drain reports the first error once
// and releases every delivery after it without consuming.
func TestDrainPartitionQueueReportsConsumeError(t *testing.T) {
	ctx := context.Background()
	src := NewSliceSource([]parquet.Column{{Name: "g", Type: parquet.TypeInt64}},
		[]map[string]any{{"g": int64(1)}, {"g": int64(2)}})
	b, err := src.Next(ctx)
	if err != nil || b == nil {
		t.Fatalf("source: %v %v", b, err)
	}
	const items = 3
	sh := &sharedBatch{b: b}
	sh.refs.Store(items)
	q := make(chan partitionItem, items)
	for i := 0; i < items; i++ {
		q <- partitionItem{view: selView(b, []uint32{0}), src: sh}
	}
	close(q)
	want := errors.New("aggregate input \"y + 0\" is not a column of its input")
	sink := &failingSink{err: want}
	var reported []error
	drainPartitionQueue(ctx, sink, q, func(err error) { reported = append(reported, err) })
	if len(reported) != 1 || !errors.Is(reported[0], want) {
		t.Fatalf("drain reported %v, want the consume error once", reported)
	}
	if sink.consumed != 1 {
		t.Errorf("consumed %d deliveries after the failure, want only the failing one", sink.consumed)
	}
	if got := sh.refs.Load(); got != 0 {
		t.Errorf("%d deliveries left unreleased", got)
	}
}
