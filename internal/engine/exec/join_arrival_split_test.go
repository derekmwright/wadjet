package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #598: a build fails on its FIRST arrival batch when that batch alone is
// bigger than the pool — largestInMemoryPartition returns -1 with nothing
// stored, so spillUntilCanReserve frees 0 and the retry fails for exactly the
// reason the first attempt did. The same rows delivered in smaller batches
// build fine, which is the whole content of the defect: the failure is about
// the BATCHING, not about the data. The pair below is therefore the gate —
// asserting only that the chunked arm builds would pass on the broken tree.
//
// arrivalBuildRows and arrivalSource live in join_index_budget_test.go.

func TestGraceBuildAbsorbsAnArrivalBatchBiggerThanThePool(t *testing.T) {
	const n = 40000
	schema, rows := arrivalBuildRows(n, strings.Repeat("x", 128))
	oneBatchBytes := hashBuildBytes(batch.FromRows(schema, rows))
	const budget = 4 << 20
	if oneBatchBytes <= budget {
		t.Fatalf("fixture no longer reproduces the shape: one batch is %d bytes, budget %d — "+
			"the whole point is a single arrival batch LARGER than the pool", oneBatchBytes, budget)
	}

	for _, arm := range []struct {
		name  string
		chunk int
	}{
		{"one_arrival_batch", 0},
		{"chunked_2048", 2048},
	} {
		t.Run(arm.name, func(t *testing.T) {
			tracker := memory.NewTracker("test", budget)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()

			hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
			hj.Spill = sm
			hj.MemTracker = tracker

			before := JoinPartitionsEvicted.Load()
			if err := hj.Build(context.Background(), arrivalSource(schema, rows, arm.chunk)); err != nil {
				t.Fatalf("BUILD FAILED with a %d-byte arrival batch against a %d-byte budget: %v\n"+
					"the same rows in %d-row batches build — the batching, not the data, is what failed",
					oneBatchBytes, int64(budget), err, arm.chunk)
			}
			if d := JoinPartitionsEvicted.Load() - before; d == 0 {
				t.Fatalf("no partition was evicted: this arm never reached the grace path, so it "+
					"proves nothing about a build that does not fit (%d bytes against %d)",
					oneBatchBytes, int64(budget))
			}

			// EVERY build key must come back exactly once. A split that drops a
			// chunk, replays one twice, or assembles a chunk out of two
			// different rows shows up here and nowhere else — buildRows counts
			// only the rows that stayed indexed, so it cannot see any of it.
			probeSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
			probeRows := make([]map[string]any, n)
			for i := range probeRows {
				probeRows[i] = map[string]any{"k": int64(i)}
			}
			sink := &CollectSink{}
			pipe := &Pipeline{
				Source: NewSliceSource(probeSchema, probeRows),
				Ops:    []UnaryOperator{hj.Probe()},
				Sink:   sink,
			}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatalf("probe pipeline: %v", err)
			}
			if len(sink.Rows) != n {
				t.Fatalf("probe matched %d of %d build keys — the split dropped or duplicated rows",
					len(sink.Rows), n)
			}
			seen := make(map[int64]bool, n)
			for _, r := range sink.Rows {
				id, ok := r["id"].(int64)
				if !ok {
					t.Fatalf("row %v: id is %T, want int64", r, r["id"])
				}
				if seen[id] {
					t.Fatalf("build row %d came back twice", id)
				}
				seen[id] = true
				if r["k"] != id {
					t.Fatalf("row %v: id and k disagree, so a chunk was assembled from two different rows", r)
				}
			}
		})
	}
}

// A budget below the operator's floor must REFUSE, loudly and quickly, rather
// than grind the build into single-row reservations. minArrivalChunkRows is
// where "split it smaller" stops being an answer (ADR-0006).
func TestGraceBuildRefusesBelowItsArrivalFloor(t *testing.T) {
	schema, rows := arrivalBuildRows(2000, strings.Repeat("x", 512))
	tracker := memory.NewTracker("test", 4096)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.Spill = sm
	hj.MemTracker = tracker

	err = hj.Build(context.Background(), arrivalSource(schema, rows, 0))
	if err == nil {
		t.Fatal("a 4 KiB budget built a 2000-row, 512-byte-padded build side — either the fixture " +
			"stopped being oversized or the split lost its floor")
	}
	if !strings.Contains(err.Error(), "memory budget exceeded") {
		t.Fatalf("want a loud budget refusal, got %v", err)
	}
}
