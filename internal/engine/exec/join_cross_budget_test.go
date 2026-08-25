package exec

import (
	"context"
	"errors"
	"runtime"
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

// TestCrossJoinFanOutStaysBounded is ADR-0006 ("never OOM") for the one
// operator whose output is quadratic in its inputs by definition.
//
// #593 reached 30 GB and a kernel OOM kill on a 3 MB fixture. The planner
// defect that put a real cross product there is fixed, but "the planner will
// never emit one" is not a memory bound — a query can ask for a genuine
// Cartesian product, and the engine has to answer it without dying. A cross
// join's fan-out is the WHOLE build side per probe row, so a single
// unbounded Execute would materialise 400M rows in one live allocation, which
// is memory the tracker cannot reclaim and GOMEMLIMIT cannot help with
// (the #317 argument, one operator further).
//
// 20,000 x 20,000 = 400,000,000 pairs under a 64 MiB budget. The answer must
// be exact and every emitted batch must respect the per-call bound.
func TestCrossJoinFanOutStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("400M-pair cross product")
	}
	const n = 20000
	buildSchema := []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "pad", Type: parquet.TypeString}}
	probeSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}

	ctx := context.Background()
	tracker := memory.NewTracker("cross-fanout", 64<<20)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(CrossJoin, nil, nil)
	hj.MemTracker = tracker
	hj.Spill = sm
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, crossRows(n, "rk", true)))); err != nil {
		t.Fatalf("build: %v", err)
	}

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	sink := &crossSink{}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, crossRows(n, "k", false))),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if want := int64(n) * int64(n); sink.rows != want {
		t.Fatalf("cross join produced %d rows, want %d", sink.rows, want)
	}
	if sink.maxRows > MaxProbeOutputRows {
		t.Fatalf("a single cross-join output batch carried %d rows; the per-call bound is %d — an unbounded "+
			"fan-out is one live allocation the tracker cannot reclaim (ADR-0006, #593)",
			sink.maxRows, MaxProbeOutputRows)
	}
	// The build side is ~20k padded rows; the fan-out itself must add nothing
	// that accumulates. A generous ceiling still fails loudly on an unbounded
	// materialisation, which would be 400M rows x 2 columns.
	if peak := tracker.Peak(); peak > tracker.Budget() {
		t.Fatalf("tracker peaked at %d bytes over a %d-byte budget", peak, tracker.Budget())
	}
	if grew := int64(after.HeapSys) - int64(before.HeapSys); grew > 1<<30 {
		t.Fatalf("heap grew %d MiB across a 400M-pair cross product — the fan-out is accumulating",
			grew>>20)
	}
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
