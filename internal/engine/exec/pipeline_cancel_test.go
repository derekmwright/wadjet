package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// cancelAfterFirstSink cancels the pipeline's context as soon as the first
// batch arrives, then counts how many more the chain delivers anyway.
type cancelAfterFirstSink struct {
	cancel  context.CancelFunc
	batches int
}

func (s *cancelAfterFirstSink) Init(context.Context) error { return nil }

func (s *cancelAfterFirstSink) Consume(context.Context, *batch.RecordBatch) error {
	s.batches++
	if s.batches == 1 {
		s.cancel()
	}
	return nil
}

func (s *cancelAfterFirstSink) Finalize(context.Context) error { return nil }
func (s *cancelAfterFirstSink) Close() error                   { return nil }

// TestChainDriverObservesCancelMidFanOut is the executor half of #368: a
// CancelRequest cancelled the statement context correctly and the query ran 11
// more seconds to completion, because a keyless join's fan-out lives almost
// entirely inside the resume loop for a handful of source batches — and that
// loop never looked at the context. The serial pump's check only counts SOURCE
// batches, so it never fired.
//
// Same shape as TestHashJoinProbeBoundsFanOut (#317): one 512-row probe batch
// against a single-key 2000-row build side fans out to 1,024,000 rows ≈ 500
// output batches, all descended from ONE source batch. The sink cancels on the
// first delivered batch; the chain must stop within a batch or two, not walk
// the remaining ~499.
func TestChainDriverObservesCancelMidFanOut(t *testing.T) {
	const buildN = 2000
	const probeN = 512

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rk": int64(7), "rv": int64(i)}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"k": int64(7), "amount": int64(i)}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}

	sink := &cancelAfterFirstSink{cancel: cancel}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, probeRows)),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	err := pipe.Run(ctx)
	if err == nil {
		t.Fatal("pipeline returned nil error after its context was cancelled mid-fan-out")
	}
	if !strings.Contains(err.Error(), "cancelled") && !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Errorf("error = %v, want a cancellation", err)
	}
	// One batch triggered the cancel; the check runs per output batch, so at
	// most one more can slip through before the loop notices.
	if sink.batches > 2 {
		t.Errorf("chain delivered %d batches after the cancel; the fan-out loop is not polling the context", sink.batches)
	}
}
