package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The same routing as TestSpilledJoinFlushReachesTheOwningAggregateSink, for
// the OTHER thing flushSpilledOps replays: a RIGHT or FULL join's UNMATCHED
// BUILD ROWS (#782, coverage follow-up).
//
// HashJoinProbe.HasPendingFlush is true for two independent reasons — spilled
// probe partitions, and a RIGHT/FULL join that still owes the build rows
// nothing probed. Both arrive through the same NextFlush loop after the
// workers have stopped, so both were consumed into the primary UNROUTED, and
// both put a second copy of a key in the primary beside the clone that owns
// it. The sibling gate covers the first; this covers the second, with the join
// deliberately NOT spilling so the unmatched rows are the only thing the flush
// carries and the mechanism is not confounded with the other one.
//
// The group key is on the BUILD side on purpose: an unmatched build row has
// NULL for every probe column, so grouping by a probe column would put every
// flushed row into the single NULL group and hide the split.
//
// Reverting the fix — the flush back after the merge, into p.Sink — fails this
// with "parallel emitted 9 rows for 5 distinct keys".
func TestUnmatchedBuildRowFlushReachesTheOwningAggregateSink(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "bg", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	const n = 20000
	var buildRows, probeRows []map[string]any
	for i := 0; i < n; i++ {
		buildRows = append(buildRows, map[string]any{"id": int64(i), "bg": int64(i % 5)})
		// Only every third id is probed, so two thirds of the build rows are
		// unmatched and reach the sink through the flush — spread across all
		// five keys, so an unrouted flush duplicates every one of them rather
		// than a lucky one.
		if i%3 == 0 {
			probeRows = append(probeRows, map[string]any{"id": int64(i)})
		}
	}

	run := func(workers int) (map[int64]int64, int) {
		t.Helper()
		hj := NewHashJoin(RightJoin, []string{"id"}, []string{"id"})
		if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
			t.Fatalf("build: %v", err)
		}
		probe := hj.Probe()
		if !probe.HasPendingFlush() {
			t.Fatal("the probe owes no flush before it runs: this fixture no longer reaches the unmatched-row path")
		}
		agg := NewHashAggregate([]string{"bg"}, []AggColumn{
			{Func: AggCount, InputCol: "bg", OutputCol: "cnt", OutputType: parquet.TypeInt64},
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
				out[r["bg"].(int64)] += r["cnt"].(int64)
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
		t.Errorf("parallel emitted %d rows for %d distinct keys — an unmatched-build-row flush landed outside its owner",
			parallelEmitted, len(parallel))
	}
	// Recomputed from the generator: a RIGHT join emits every build row once,
	// so each key holds exactly the build rows carrying it.
	for k := int64(0); k < 5; k++ {
		want := int64(n / 5)
		if got := serial[k]; got != want {
			t.Errorf("key %d: serial count %d, want %d from the generator", k, got, want)
		}
		if got := parallel[k]; got != want {
			t.Errorf("key %d: parallel count %d, want %d from the generator", k, got, want)
		}
	}
}
