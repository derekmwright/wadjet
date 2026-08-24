package kernel

import (
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Benchmarks for the fold-in to #457: the row/slice MIN/MAX float updaters
// (minRowFloat64/32, maxRowFloat64/32 and their NoNulls variants,
// minSliceFloat64/32, maxSliceFloat64/32) switched from raw IEEE `<`/`>` to
// kernel.CompareFloat64/CompareFloat32, which is correct but is a function
// call carrying extra NaN-detection branches on every element instead of an
// inlined comparison. These benchmarks measure that cost on the hottest
// arm of each — the scalar (no GROUP BY) slice path with no NULLs and no
// selection vector, exercised through the exported ResolveBatchMin/
// ResolveBatchMax entry points the same way HashAggregate's scalar fast
// path calls them (kernel.go's h.batchAggKernels) — at DefaultBatchSize,
// wadjet's standard batch width.
//
// Run interleaved to cancel drift: go test -bench BenchmarkScalarMinMax
// -benchmem -count=8 ./internal/engine/exec/kernel/ | tee out.txt, twice
// (before/after the cheap-form substitution), then
// benchstat before.txt after.txt.

func benchFloat64Slice(n int) []float64 {
	r := rand.New(rand.NewSource(1))
	data := make([]float64, n)
	for i := range data {
		data[i] = r.Float64()*2000 - 1000
	}
	return data
}

func benchFloat32Slice(n int) []float32 {
	r := rand.New(rand.NewSource(1))
	data := make([]float32, n)
	for i := range data {
		data[i] = r.Float32()*2000 - 1000
	}
	return data
}

func BenchmarkScalarMinMaxFloat64(b *testing.B) {
	const n = batch.DefaultBatchSize
	data := benchFloat64Slice(n)
	vec := &batch.Vector{Type: batch.TypeFloat64, Float64Data: data, Len: n, Nulls: batch.NewBitmap(n)}
	minK := ResolveBatchMin(batch.TypeFloat64)
	maxK := ResolveBatchMax(batch.TypeFloat64)

	b.Run("Min", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var acc Accumulator
			minK(&acc, vec, nil, n)
		}
	})
	b.Run("Max", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var acc Accumulator
			maxK(&acc, vec, nil, n)
		}
	})
}

func BenchmarkScalarMinMaxFloat32(b *testing.B) {
	const n = batch.DefaultBatchSize
	data := benchFloat32Slice(n)
	vec := &batch.Vector{Type: batch.TypeFloat32, Float32Data: data, Len: n, Nulls: batch.NewBitmap(n)}
	minK := ResolveBatchMin(batch.TypeFloat32)
	maxK := ResolveBatchMax(batch.TypeFloat32)

	b.Run("Min", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var acc Accumulator
			minK(&acc, vec, nil, n)
		}
	})
	b.Run("Max", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var acc Accumulator
			maxK(&acc, vec, nil, n)
		}
	})
}

// BenchmarkRowMinMaxFloat64 exercises the row updater one row at a time
// (minRowFloat64/maxRowFloat64), the shape the generic per-group
// (non-int-keyed) HashAggregate path calls per input row.
func BenchmarkRowMinMaxFloat64(b *testing.B) {
	const n = batch.DefaultBatchSize
	data := benchFloat64Slice(n)
	vec := &batch.Vector{Type: batch.TypeFloat64, Float64Data: data, Len: n, Nulls: batch.NewBitmap(n)}
	minK := ResolveRowMin(batch.TypeFloat64)
	maxK := ResolveRowMax(batch.TypeFloat64)

	b.Run("Min", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var acc Accumulator
			for row := 0; row < n; row++ {
				minK(&acc, vec, row)
			}
		}
	})
	b.Run("Max", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var acc Accumulator
			for row := 0; row < n; row++ {
				maxK(&acc, vec, row)
			}
		}
	})
}
