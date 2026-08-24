package exec

import (
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// BenchmarkScatterMinMaxFloat64 measures scatterMinFloat/scatterMaxFloat
// (the int/packed-keyed GROUP BY SoA fast path, agg_scatter.go) on the
// same fold-in to #457 the row/slice kernels in kernel/agg.go were
// measured on: kernel.CompareFloat64 vs the cheap `v < acc || acc != acc`
// (MIN) / `v > acc || v != v` (MAX) replace-test, proved pointwise
// identical to CompareFloat64's decision (except a harmless "replace NaN
// with NaN" no-op) by kernel.TestCheapNaNMinMaxFormsMatchCompareFloat64.
// numGroups=64 keeps minArr/hasMin small enough to stay cache-resident, so
// this isolates the per-element comparison cost from cache-miss cost —
// the same reason the reviewer's audit called the grouped path
// "unaffected": group-array indirection dominates there, not the compare.
func BenchmarkScatterMinMaxFloat64(b *testing.B) {
	const n = batch.DefaultBatchSize
	const numGroups = 64
	r := rand.New(rand.NewSource(1))
	data := make([]float64, n)
	gi := make([]int32, n)
	for i := range data {
		data[i] = r.Float64()*2000 - 1000
		gi[i] = int32(i % numGroups)
	}
	nulls := batch.NewBitmap(n)

	b.Run("Min", func(b *testing.B) {
		b.ReportAllocs()
		minArr := make([]float64, numGroups)
		hasMin := make([]bool, numGroups)
		for i := 0; i < b.N; i++ {
			for g := range hasMin {
				hasMin[g] = false
			}
			scatterMinFloat(minArr, hasMin, data, gi, &nulls, nil, n)
		}
	})
	b.Run("Max", func(b *testing.B) {
		b.ReportAllocs()
		maxArr := make([]float64, numGroups)
		hasMax := make([]bool, numGroups)
		for i := 0; i < b.N; i++ {
			for g := range hasMax {
				hasMax[g] = false
			}
			scatterMaxFloat(maxArr, hasMax, data, gi, &nulls, nil, n)
		}
	})
}
