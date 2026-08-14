package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// BenchmarkUnpartitionedSink_TinyConsumeConvoy reproduces the q17 join-5
// consume shape (SF100 2026-08-14 diagnosis): a ~0.1%-selectivity semi-join
// probe emits ~2 surviving rows per 2048-row morsel, so k parallel consumers
// hit the shared sink with ~100K tiny Sel'd consumes per task. Measured
// sink_ms was 55-56s cumulative per task for ~200K output rows. This bench
// measures cumulative in-consume time under the same shape.
func BenchmarkUnpartitionedSink_TinyConsumeConvoy(b *testing.B) {
	for _, k := range []int{1, 8} {
		b.Run(fmt.Sprintf("consumers=%d", k), func(b *testing.B) {
			dir := b.TempDir()
			sink := newUnpartitionedStageSink(dir, "bench-convoy")
			if err := sink.Init(context.Background()); err != nil {
				b.Fatal(err)
			}
			defer sink.Close()

			schema := []parquet.Column{
				{Name: "l_partkey", Type: parquet.TypeInt64},
				{Name: "l_quantity", Type: parquet.TypeDecimal},
				{Name: "l_extendedprice", Type: parquet.TypeDecimal},
			}
			// Per-consumer parent batch: 2048 rows, Sel picks 2 survivors.
			mkParent := func() *batch.RecordBatch {
				pb := batch.NewRecordBatch(schema, batch.DefaultBatchSize)
				pb.Len = batch.DefaultBatchSize
				for i := 0; i < pb.Len; i++ {
					pb.Columns[0].Int64Data = append(pb.Columns[0].Int64Data, int64(i))
					pb.Columns[1].Int64Data = append(pb.Columns[1].Int64Data, int64(i*100))
					pb.Columns[2].Int64Data = append(pb.Columns[2].Int64Data, int64(i*1000))
				}
				pb.Sel = []uint32{17, 1900}
				return pb
			}

			var cum atomic.Int64
			b.ResetTimer()
			var wg sync.WaitGroup
			per := b.N/k + 1
			for c := 0; c < k; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pb := mkParent()
					for i := 0; i < per; i++ {
						t0 := time.Now()
						if err := sink.Consume(context.Background(), pb); err != nil {
							panic(err)
						}
						cum.Add(time.Since(t0).Nanoseconds())
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(cum.Load())/float64(b.N), "cum-ns/consume")
		})
	}
}
