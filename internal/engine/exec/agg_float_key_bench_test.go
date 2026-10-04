// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkGroupByFloatKey measures a GROUP BY over a float key (arc FO): a
// float key's group publishes a member's own value, so the generic path boxes
// the first member's key per NEW group instead of rebuilding it from the
// canonical key bytes. lowNDV is the per-row cost (1,024 groups); highNDV is
// the per-group cost (every row a new group).
func BenchmarkGroupByFloatKey(b *testing.B) {
	for _, tc := range []struct {
		name string
		typ  parquet.TypeID
		ndv  int
	}{
		{"f64/lowNDV", parquet.TypeFloat64, 1024},
		{"f64/highNDV", parquet.TypeFloat64, 1 << 30},
		{"f32/lowNDV", parquet.TypeFloat32, 1024},
		{"f32/highNDV", parquet.TypeFloat32, 1 << 30},
	} {
		b.Run(tc.name, func(b *testing.B) {
			schema := []parquet.Column{{Name: "k", Type: tc.typ}, {Name: "v", Type: parquet.TypeInt64}}
			const nBatches = 32
			batches := make([]*batch.RecordBatch, nBatches)
			for bi := range batches {
				rows := make([]map[string]any, batch.DefaultBatchSize)
				for i := range rows {
					n := (bi*batch.DefaultBatchSize + i) % tc.ndv
					var k any = float64(n) + 0.5
					if tc.typ == parquet.TypeFloat32 {
						k = float32(n) + 0.5
					}
					rows[i] = map[string]any{"k": k, "v": int64(i)}
				}
				batches[bi] = batch.FromRows(schema, rows)
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for it := 0; it < b.N; it++ {
				h := NewHashAggregate([]string{"k"}, []AggColumn{{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64}})
				if err := h.Init(ctx); err != nil {
					b.Fatal(err)
				}
				for _, bt := range batches {
					if err := h.Consume(ctx, bt); err != nil {
						b.Fatal(err)
					}
				}
				if err := h.Finalize(ctx); err != nil {
					b.Fatal(err)
				}
				for {
					out, err := h.Next(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if out == nil {
						break
					}
				}
				h.Close()
			}
		})
	}
}
