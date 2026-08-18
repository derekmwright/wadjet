package exec

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// highCardBatches builds numBatches batches of rowsPerBatch rows whose
// (k1, k2) pairs are globally distinct, so the group count grows without
// bound as batches arrive — the SF100 `GROUP BY l_partkey, l_suppkey`
// shape from #325 in miniature.
func highCardBatches(numBatches, rowsPerBatch int) ([]*batch.RecordBatch, map[[2]int64]int64) {
	schema := []parquet.Column{
		{Name: "k1", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	batches := make([]*batch.RecordBatch, 0, numBatches)
	expected := make(map[[2]int64]int64, numBatches*rowsPerBatch)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			n := int64(bi*rowsPerBatch + ri)
			k1, k2 := n, n*7+1
			v := n + 1
			rows = append(rows, map[string]any{"k1": k1, "k2": k2, "v": v})
			expected[[2]int64{k1, k2}] += v
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}
	return batches, expected
}

// TestHighCardinalityGroupBy_ConvergesUnderForeignPressure is the #325
// regression: a peer operator holds most of a shared memory budget, so
// SpillManager.ShouldSpillFor stays true for every Consume no matter what
// this aggregate does. Before the fix the aggregate answered that signal
// with a whole-table drain on EVERY batch — one run file per batch, each
// freeing essentially nothing, because the bytes under pressure were never
// this operator's to release. That is the livelock the SF100 report shows:
// spill, refill, spill again, no convergence.
//
// The aggregate must (a) still produce correct results and (b) not emit a
// drain per batch when draining cannot relieve the pressure.
func TestHighCardinalityGroupBy_ConvergesUnderForeignPressure(t *testing.T) {
	const numBatches = 120
	const rowsPerBatch = 64
	batches, expected := highCardBatches(numBatches, rowsPerBatch)

	// Shared budget with a foreign holder pinning 90% of it. The aggregate
	// can never bring tracker.Used() back under the SpillCheap threshold.
	tracker := memory.NewTracker("test", 1_000_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900_000)

	h := NewHashAggregate([]string{"k1", "k2"}, []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm

	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
	}
	drains := len(h.partialSpillFiles)
	t.Logf("drains=%d over %d batches (state=%d bytes)", drains, numBatches, h.trackedGroupMem)

	// A drain per batch is the livelock signature. Draining is only useful
	// when this operator actually holds bytes worth releasing, so the count
	// must stay far below the batch count.
	if drains >= numBatches/2 {
		t.Errorf("aggregate drained %d times over %d batches — whole-table drain per batch "+
			"is the #325 livelock (each drain freed nothing; the pressure is a peer's)",
			drains, numBatches)
	}

	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := make(map[[2]int64]int64, len(expected))
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		for _, r := range out.ToRows() {
			got[[2]int64{r["k1"].(int64), r["k2"].(int64)}] = r["total"].(int64)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("group count: got %d want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if got[k] != want {
			t.Errorf("group %v: got %d want %d", k, got[k], want)
		}
	}
}

// BenchmarkAggregateDrainGate measures the #325 fix on the path it changes:
// a high-cardinality GROUP BY consuming under memory pressure it does not
// own. Both arms run in the same process, interleaved by the benchmark
// harness, so the comparison carries no cross-window drift. "ungated" is the
// pre-fix behavior (floor 0 = drain whenever ShouldSpillFor says so).
func BenchmarkAggregateDrainGate(b *testing.B) {
	batches, _ := highCardBatches(120, 64)
	arms := []struct {
		name    string
		divisor int64
	}{
		{"ungated", 0},
		{"gated", 8},
	}
	for _, arm := range arms {
		b.Run(arm.name, func(b *testing.B) {
			defer func(d int64) { drainFloorDivisor = d }(drainFloorDivisor)
			drainFloorDivisor = arm.divisor
			ctx := context.Background()
			dir := b.TempDir()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tracker := memory.NewTracker("bench", 1_000_000)
				sm, err := memory.NewSpillManager(dir, tracker)
				if err != nil {
					b.Fatal(err)
				}
				tracker.ForceReserve(900_000)
				h := NewHashAggregate([]string{"k1", "k2"}, []AggColumn{
					{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
				})
				h.Spill = sm
				if err := h.Init(ctx); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for _, batch := range batches {
					if err := h.Consume(ctx, batch); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				drains := len(h.partialSpillFiles)
				if err := h.Close(); err != nil {
					b.Fatal(err)
				}
				if i == 0 {
					b.ReportMetric(float64(drains), "drains/op")
				}
				b.StartTimer()
			}
		})
	}
}

// TestCheckDrainProgress is the #325 point-3 diagnostic: when an aggregate's
// drains stop reclaiming, it must say so itself — accurately and with the
// counters — instead of going quiet and letting the coordinator watchdog
// report "likely worker crash, deadlock, or lost result publish" ten minutes
// later for a crash that never happened.
func TestCheckDrainProgress(t *testing.T) {
	const budget = 1 << 30 // floor = budget/8 = 128 MiB
	newAgg := func(t *testing.T) *HashAggregate {
		t.Helper()
		tracker := memory.NewTracker("test", budget)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		return &HashAggregate{Spill: sm}
	}

	tests := []struct {
		name      string
		cycles    int
		window    time.Duration
		drainTime time.Duration
		freed     int64
		wantErr   bool
	}{
		{
			name: "too few cycles to judge", cycles: 4, window: time.Minute,
			drainTime: 59 * time.Second, freed: 0, wantErr: false,
		},
		{
			name: "window too short to judge", cycles: 64, window: time.Second,
			drainTime: time.Second, freed: 0, wantErr: false,
		},
		{
			name: "drains are not dominating the wall clock", cycles: 64, window: time.Minute,
			drainTime: 6 * time.Second, freed: 0, wantErr: false,
		},
		{
			// A tight budget drains often, but each drain reclaims far more
			// than the small floor that admitted it. That is progress, not
			// thrash, and must never be failed.
			name: "drains dominate but are reclaiming", cycles: 64, window: time.Minute,
			drainTime: 59 * time.Second, freed: 64 * (256 << 20), wantErr: false,
		},
		{
			name: "drains dominate and reclaim nothing", cycles: 64, window: time.Minute,
			drainTime: 59 * time.Second, freed: 0, wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newAgg(t)
			h.drainCount = tc.cycles
			h.drainNanos = tc.drainTime.Nanoseconds()
			h.drainFreedBytes = tc.freed
			h.firstDrainAt = time.Now().Add(-tc.window)

			err := h.checkDrainProgress()
			if tc.wantErr != (err != nil) {
				t.Fatalf("checkDrainProgress: err=%v, want error: %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				return
			}
			msg := err.Error()
			// The message is the whole point: it must name the real cause and
			// carry the counters that identify it.
			for _, want := range []string{
				"could not make progress within its memory budget",
				"64 spill cycles",
				"128 MiB", // the floor the drains failed to clear
			} {
				if !strings.Contains(msg, want) {
					t.Errorf("error message missing %q:\n%s", want, msg)
				}
			}
			if strings.Contains(msg, "crash") || strings.Contains(msg, "deadlock") {
				t.Errorf("error message blames a phantom failure:\n%s", msg)
			}
		})
	}
}

// TestDrainAndAccountSurfacesNonConvergence checks the wiring: a drain that
// reclaims nothing is reported through the same call Consume makes, so the
// error reaches the task instead of being swallowed.
func TestDrainAndAccountSurfacesNonConvergence(t *testing.T) {
	defer func(c int, w time.Duration, r float64) {
		drainStallMinCycles, drainStallMinWindow, drainStallRatio = c, w, r
	}(drainStallMinCycles, drainStallMinWindow, drainStallRatio)
	drainStallMinCycles, drainStallMinWindow, drainStallRatio = 1, 0, 0

	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	// No group state, so the drain reclaims nothing.
	h := &HashAggregate{Spill: sm}
	if err := h.drainAndAccount(0); err == nil {
		t.Fatal("drainAndAccount: want non-convergence error, got nil")
	} else if !strings.Contains(err.Error(), "could not make progress within its memory budget") {
		t.Fatalf("drainAndAccount: wrong error: %v", err)
	}
}
