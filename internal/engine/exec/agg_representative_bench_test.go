// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkDecimalKeyGroupBy is the hot path the representative rule (#1713)
// touches: a DECIMAL-keyed GROUP BY whose every hit compares the incoming
// member's display scale with the group's. "same" spells every member of a
// group alike (the ordinary column: one uniform display-scale code); "two"
// spells half of them with a trailing zero (12.3 / 12.30), so every group
// meets two representations. 1 worker consumes every batch; 8 workers
// consume a slice each in a clone and merge into the primary, as the
// morsel-parallel driver does.
func BenchmarkDecimalKeyGroupBy(b *testing.B) {
	const groups, nBatches, rows = 1024, 32, 2048
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeDecimal, Precision: 12, Scale: 2},
		{Name: "v", Type: parquet.TypeInt64},
	}
	build := func(two bool) []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for bi := 0; bi < nBatches; bi++ {
			bt := batch.NewRecordBatch(schema, rows)
			for r := 0; r < rows; r++ {
				g := (bi*rows + r) % groups
				if two && r%2 == 1 {
					bt.Columns[0].SetValue(r, fmt.Sprintf("%d.%d", g, g%9+1))
				} else if two {
					bt.Columns[0].SetValue(r, fmt.Sprintf("%d.%d0", g, g%9+1))
				} else {
					bt.Columns[0].DecimalData.Data[r] = batch.Int128From(int64(g*100 + (g%9+1)*10))
				}
				bt.Columns[0].Nulls.SetValid(r)
				bt.Columns[1].Int64Data[r] = int64(r)
				bt.Columns[1].Nulls.SetValid(r)
			}
			out = append(out, bt)
		}
		return out
	}
	aggs := []AggColumn{
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
		{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64},
	}
	ctx := context.Background()
	for _, reps := range []string{"same", "two"} {
		input := build(reps == "two")
		for _, workers := range []int{1, 8} {
			b.Run(fmt.Sprintf("reps=%s/workers=%d", reps, workers), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					parent := NewHashAggregate([]string{"k"}, aggs)
					if err := parent.Init(ctx); err != nil {
						b.Fatal(err)
					}
					if workers == 1 {
						for _, bt := range input {
							if err := parent.Consume(ctx, bt); err != nil {
								b.Fatal(err)
							}
						}
					} else {
						// Warmup batch on the primary, the rest across clones.
						if err := parent.Consume(ctx, input[0]); err != nil {
							b.Fatal(err)
						}
						clones := make([]*HashAggregate, workers)
						for w := range clones {
							clones[w] = parent.CloneSink().(*HashAggregate)
							if err := clones[w].Init(ctx); err != nil {
								b.Fatal(err)
							}
						}
						var wg sync.WaitGroup
						for w := range clones {
							wg.Add(1)
							go func(w int) {
								defer wg.Done()
								for j := 1 + w; j < len(input); j += workers {
									_ = clones[w].Consume(ctx, input[j])
								}
							}(w)
						}
						wg.Wait()
						for _, c := range clones {
							parent.MergeSink(c)
						}
					}
					if err := parent.Finalize(ctx); err != nil {
						b.Fatal(err)
					}
					for {
						r, err := parent.Next(ctx)
						if err != nil {
							b.Fatal(err)
						}
						if r == nil {
							break
						}
					}
					parent.Close()
				}
			})
		}
	}
}
