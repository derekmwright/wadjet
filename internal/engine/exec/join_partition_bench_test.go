package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// BenchmarkHashJoinBuildNoSpill is the Option A gate for HashJoin partial-drain.
//
// It measures build-side cost of the legacy flat path (PartitionOnArrival=false)
// vs the partition-on-arrival path (true) in the NO-SPILL regime — the regime
// the flat "fast path" exists to optimize. Partition-on-arrival is already the
// correct partial-drain path (O(partition) eviction, cooperative relief); the
// only thing keeping the legacy flat path alive is the hypothesis that its
// per-batch scatter is too expensive when no spill happens. If the overhead
// here is within noise, the flat path — and its reactive O(total) repartition +
// full hash-table rebuild (the Q17/Q18 mc=4 killer) — can be retired.
//
// Both paths get MemTracker+Spill with a budget far above the working set, so
// nothing spills and the only variable is PartitionOnArrival.
func BenchmarkHashJoinBuildNoSpill(b *testing.B) {
	const buildN = 200_000
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := make([]map[string]any, buildN)
	for i := range rows {
		rows[i] = map[string]any{
			"id":  int64(i),
			"val": "lineitem-ish-string-payload-row",
		}
	}

	ctx := context.Background()

	// Convert rows -> fresh batches (untimed). A fresh set each iteration so
	// neither build path can observe state left by the previous one.
	freshBatches := func() []*batch.RecordBatch {
		src := NewSliceSource(schema, rows)
		if err := src.Init(ctx); err != nil {
			b.Fatal(err)
		}
		var out []*batch.RecordBatch
		for {
			rb, err := src.Next(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if rb == nil {
				break
			}
			out = append(out, rb)
		}
		return out
	}

	tmpDir := b.TempDir() // unused target: no spill at this budget

	for _, partition := range []bool{false, true} {
		name := "flat"
		if partition {
			name = "partitioned"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batches := freshBatches()
				tracker := memory.NewTracker("bench", 8<<30) // 8 GiB: no spill
				sm, err := memory.NewSpillManager(tmpDir, tracker)
				if err != nil {
					b.Fatal(err)
				}
				hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
				hj.Spill = sm
				hj.MemTracker = tracker
				hj.PartitionOnArrival = partition
				src := NewBatchSource(batches)
				b.StartTimer()

				if err := hj.Build(ctx, src); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				_ = hj.Close()
				sm.Cleanup()
				b.StartTimer()
			}
		})
	}
}
