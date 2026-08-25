package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// crossSink counts rows and remembers the largest batch it was handed.
type crossSink struct {
	rows    int64
	maxRows int
}

func (s *crossSink) Init(ctx context.Context) error { return nil }
func (s *crossSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	n := b.ActiveLen()
	s.rows += int64(n)
	if n > s.maxRows {
		s.maxRows = n
	}
	return nil
}
func (s *crossSink) Finalize(ctx context.Context) error { return nil }
func (s *crossSink) Close() error                       { return nil }

func crossRows(n int, key string, pad bool) []map[string]any {
	out := make([]map[string]any, n)
	for i := range out {
		r := map[string]any{key: int64(i)}
		if pad {
			r["pad"] = "cross-join-build-row-padding-to-give-the-budget-something-to-count"
		}
		out[i] = r
	}
	return out
}

// TestCrossJoinBuildFailsLoudlyOverBudget is the other half of ADR-0006: a
// build side the budget genuinely cannot hold must come back as a memory
// error naming the budget, not as a kernel OOM kill that takes the process
// (and, in #593, the whole test binary) with it and reports nothing about the
// query that did it.
//
// The no-spill flat path is the one under test: with no SpillManager
// configured there is no recourse, so Reserve's failure has to surface.
func TestCrossJoinBuildFailsLoudlyOverBudget(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "pad", Type: parquet.TypeString}}
	ctx := context.Background()
	tracker := memory.NewTracker("cross-build-tiny", 64<<10) // 64 KiB

	hj := NewHashJoin(CrossJoin, nil, nil)
	hj.MemTracker = tracker
	err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, crossRows(20000, "rk", true))))
	if err == nil {
		t.Fatal("a 20k-row build under a 64 KiB budget succeeded — the budget is not being charged")
	}
	if !errors.Is(err, memory.ErrMemoryExceeded) {
		t.Fatalf("build failed with %v, want a memory-budget error (ADR-0006: degrade or fail loudly, never OOM)", err)
	}
}

// TestCrossJoinSmallInputsStillWork guards the other direction: bounding the
// fan-out must not break the ordinary small Cartesian product.
func TestCrossJoinSmallInputsStillWork(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}}
	probeSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	ctx := context.Background()

	hj := NewHashJoin(CrossJoin, nil, nil)
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, crossRows(25, "rk", false)))); err != nil {
		t.Fatalf("build: %v", err)
	}
	sink := &crossSink{}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, crossRows(5, "k", false))),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if sink.rows != 125 {
		t.Fatalf("5 x 25 cross join produced %d rows, want 125", sink.rows)
	}
}
