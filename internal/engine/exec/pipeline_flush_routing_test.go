package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A grace-partitioned join's spilled probe rows must reach the sink that OWNS
// their group key, not the primary (#782).
//
// Mechanism. Under partitioned aggregation every worker owns a hash partition
// of the key space, and MergeSink ADOPTS each clone's state rather than merging
// it by key — which is only sound while every key lives in exactly one sink.
// Pipeline.flushSpilledOps replayed the join's spilled-partition probe output
// after that merge and consumed it straight into the primary, unrouted. The
// primary then held its own copy of keys the clones already owned, and adoption
// emitted both: the fixture below answered 14 rows for 7 keys, with each key's
// count split at a point that moves with which probe rows fell into spilled
// partitions. The routeFallback demotion could not cover it — it runs on
// batches the WORKERS failed to route, and it ran before the flush besides.
//
// This is deterministic where the SQL-level form is not: the join is forced to
// spill by a budget far below its build side, and the test asserts both that
// probe partitions spilled and that partitioned mode engaged, so a fixture that
// stops exercising either path fails instead of passing silently.
//
// Reverting the fix — consuming the flush into p.Sink, after the merge — fails
// this with "emitted 14 rows for 7 distinct keys".
func TestSpilledJoinFlushReachesTheOwningAggregateSink(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "tag", Type: parquet.TypeString},
	}
	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
	}
	// Build small enough to fit the budget's first batches and spill the rest;
	// probe large enough that every join partition — spilled and resident —
	// carries rows of every group key, so a flush that lands in the wrong sink
	// duplicates every key rather than a lucky one.
	const buildN, n = 5000, 20000
	var buildRows, probeRows []map[string]any
	for i := 0; i < buildN; i++ {
		buildRows = append(buildRows, map[string]any{"id": int64(i), "tag": "b"})
	}
	for i := 0; i < n; i++ {
		probeRows = append(probeRows, map[string]any{"id": int64(i % buildN), "g": int64(i % 7)})
	}

	run := func(workers int) (map[int64]int64, int) {
		t.Helper()
		// A budget far under the build side forces grace partitioning, so
		// probe rows hashing into a spilled partition go to disk and come back
		// through NextFlush — the batches this gate is about.
		tracker := memory.NewTracker("test", 250_000)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Cleanup()
		hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
		hj.Spill = sm
		hj.MemTracker = tracker
		if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
			t.Fatalf("build: %v", err)
		}
		if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
			t.Fatal("the join did not spill: this fixture no longer reaches the flush path")
		}
		probe := hj.Probe()
		agg := NewHashAggregate([]string{"g"}, []AggColumn{
			{Func: AggCount, InputCol: "id", OutputCol: "cnt", OutputType: parquet.TypeInt64},
		})
		pipe := &Pipeline{
			Source:  NewSliceSource(probeSchema, probeRows),
			Ops:     []UnaryOperator{probe},
			Sink:    agg,
			Workers: workers,
		}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		out := map[int64]int64{}
		emitted := 0
		for {
			b, err := agg.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				emitted++
				out[r["g"].(int64)] += r["cnt"].(int64)
			}
		}
		if err := agg.Close(); err != nil {
			t.Fatalf("agg close: %v", err)
		}
		return out, emitted
	}

	beforeRuns := PartitionedAggRuns.Load()
	serial, serialEmitted := run(1)
	parallel, parallelEmitted := run(8)
	if PartitionedAggRuns.Load() == beforeRuns {
		t.Fatal("partitioned aggregation never engaged: this gate no longer covers the adoption path")
	}

	if serialEmitted != len(serial) {
		t.Errorf("serial emitted %d rows for %d distinct keys", serialEmitted, len(serial))
	}
	if parallelEmitted != len(parallel) {
		t.Errorf("parallel emitted %d rows for %d distinct keys — a group is split across the primary and its owner",
			parallelEmitted, len(parallel))
	}
	if len(parallel) != len(serial) {
		t.Fatalf("group count: parallel %d vs serial %d", len(parallel), len(serial))
	}
	for k, want := range serial {
		if got := parallel[k]; got != want {
			t.Errorf("key %d: parallel count %d, serial %d", k, got, want)
		}
	}
	// The counts are recomputable from the fixture, so a run in which BOTH
	// arms lost the same rows still fails.
	for k := int64(0); k < 7; k++ {
		want := int64(n/7) + map[bool]int64{true: 1, false: 0}[int64(n%7) > k]
		if got := serial[k]; got != want {
			t.Errorf("key %d: serial count %d, want %d from the generator", k, got, want)
		}
	}
	if t.Failed() {
		t.Logf("serial=%v parallel=%v", fmt.Sprint(serial), fmt.Sprint(parallel))
	}
}
