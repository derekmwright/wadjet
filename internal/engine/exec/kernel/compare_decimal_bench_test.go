package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// BenchmarkCompareFilterDecimal measures the vectorized DECIMAL filter kernel
// FIX-2 targets: compareFilterDecimal's keep() closure inlines
// batch.ScaledDecimal.Order only when Order itself is small enough for the
// compiler's inliner budget. Before FIX-2, Order's Sat switch cost 83 against
// an 80-instruction budget, so Order — and everything that calls it — paid a
// real function call per row instead of getting folded into this loop.
func BenchmarkCompareFilterDecimal(b *testing.B) {
	const n = 2048
	vec := batch.NewVectorWithScale(batch.TypeDecimal, n, 4)
	for i := 0; i < n; i++ {
		vec.DecimalData.Data[i] = batch.Int128From(int64(i))
	}
	kern := compareFilterDecimal(OpGt, float64(100))
	if kern == nil {
		b.Fatal("expected a kernel")
	}
	out := make([]uint32, 0, n)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out = kern(vec, nil, n, out[:0])
	}
}
