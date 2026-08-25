//go:build !race

// The 400M-pair fan-out gate is excluded under -race: the detector
// instruments every one of those row copies, which turns a 2-3 minute test
// into an unusable one. Nothing here is concurrency-sensitive — the property
// is that a serial pipeline's per-call output and tracked memory stay
// bounded — so -race has nothing to add. The two fast cross-join budget tests
// live in join_cross_budget_test.go and DO run under -race.

package exec

import (
	"context"
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

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
