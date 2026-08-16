package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkHashJoinBuildNoSpill measures the permanent no-spill build cost of
// the two surviving build paths after the flat-path unification:
//   - "flat": a no-spill in-memory build (no MemTracker/Spill — embedded
//     queries, tests, spill-replay rebuilds). Zero-copy: indexes arriving
//     batches in place.
//   - "partitioned": buildPartitioned, taken by every spill-eligible build
//     (MemTracker + Spill set). It scatters rows into growable per-partition
//     accumulators (one physical copy per row), the cost of making spill
//     O(partition) and cooperative relief always available.
//
// The "partitioned" arm is given an 8 GiB budget so nothing spills — the
// measured delta is the per-row copy floor that spill-eligible builds now pay
// unconditionally (the accepted tax for retiring the reactive O(total)
// repartition that was the Q17/Q18 mc=4 killer). Guards against regressing
// either path's no-spill cost.
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

	// Path selection is driven by spill-eligibility: a build with a
	// MemTracker + Spill manager partitions on arrival; one without takes the
	// no-spill flat path. (The PartitionOnArrival flag was retired with the
	// flat-path unification.)
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
				hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
				var sm *memory.SpillManager
				if partition {
					tracker := memory.NewTracker("bench", 8<<30) // 8 GiB: no spill
					var err error
					sm, err = memory.NewSpillManager(tmpDir, tracker)
					if err != nil {
						b.Fatal(err)
					}
					hj.Spill = sm
					hj.MemTracker = tracker
				}
				src := NewBatchSource(batches)
				b.StartTimer()

				if err := hj.Build(ctx, src); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				_ = hj.Close()
				if sm != nil {
					sm.Cleanup()
				}
				b.StartTimer()
			}
		})
	}
}
