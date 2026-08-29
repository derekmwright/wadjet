package batch

import (
	"math/rand"
	"testing"
)

// Benchmarks for the DECIMAL arithmetic primitives at a full record batch:
// 2048 elements per iteration, the width every kernel in the engine works in.
// ns/op is therefore per BATCH; each benchmark also reports ns/elem, which is
// the number a per-row cost model wants.
//
// The shapes are the ones ADR-0024's result-type rule actually produces:
// DECIMAL(15,2) is TPC-H's money column, its product is (31,4), and its
// quotient is (33,18) — so the division benchmark is measuring the scale the
// planner will really ask for, not a convenient small one.

const benchDecimalN = DefaultBatchSize

// benchDecimalOperands draws two 2048-element operand slices of unscaled
// values inside 10^p, plus a destination.
func benchDecimalOperands(seed int64, p int) (a, b, out []Int128) {
	rng := rand.New(rand.NewSource(seed))
	a = make([]Int128, benchDecimalN)
	b = make([]Int128, benchDecimalN)
	out = make([]Int128, benchDecimalN)
	for i := range a {
		a[i] = benchDecimalValue(rng, p)
		v := benchDecimalValue(rng, p)
		if v.IsZero() {
			v = Int128From(1) // a zero divisor is not an arithmetic cost
		}
		b[i] = v
	}
	return a, b, out
}

// benchDecimalValue draws one unscaled value with up to p digits, half of them
// negative, built from 64-bit halves so the wide arms are exercised too.
func benchDecimalValue(rng *rand.Rand, p int) Int128 {
	v := Int128{Lo: rng.Uint64()}
	if p > 19 {
		v.Hi = int64(rng.Uint64() >> 8)
	}
	limit, ok := DecimalPow10(p)
	if ok {
		if _, r, ok := v.QuoRem(limit); ok {
			v = r
		}
	}
	if rng.Intn(2) == 0 {
		v = v.Neg()
	}
	return v
}

func benchDecimalReport(b *testing.B, n int) {
	b.Helper()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/elem")
}

// BenchmarkDecimalAdd2048 adds two DECIMAL(15,2) batches at scale 2 — the
// same-scale case, where no rescale runs at all.
func BenchmarkDecimalAdd2048(b *testing.B) {
	x, y, out := benchDecimalOperands(1, 15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalAdd(x[j], 2, y[j], 2, 2)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalAddCrossScale2048 adds DECIMAL(15,2) to DECIMAL(18,6) at
// scale 6: both operands reach a common scale first.
func BenchmarkDecimalAddCrossScale2048(b *testing.B) {
	x, _, out := benchDecimalOperands(2, 15)
	_, y, _ := benchDecimalOperands(3, 18)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalAdd(x[j], 2, y[j], 6, 6)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalMul2048 multiplies two DECIMAL(15,2) batches at the result
// type's own scale 4 — the product with no rounding.
func BenchmarkDecimalMul2048(b *testing.B) {
	x, y, out := benchDecimalOperands(4, 15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalMul(x[j], 2, y[j], 2, 4)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalMulRescaled2048 multiplies the same batches but asks for
// scale 2, so every product is divided by 100 and rounded.
func BenchmarkDecimalMulRescaled2048(b *testing.B) {
	x, y, out := benchDecimalOperands(5, 15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalMul(x[j], 2, y[j], 2, 2)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalMulWide2048 multiplies two DECIMAL(38,10) batches at scale
// 6, where the 128-bit product overflows for most rows and the exact big.Int
// fallback runs: the cost of the arm that keeps a representable answer from
// being reported as an error.
func BenchmarkDecimalMulWide2048(b *testing.B) {
	x, y, out := benchDecimalOperands(6, 38)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalMul(x[j], 10, y[j], 10, 6)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalDiv2048 divides two DECIMAL(15,2) batches at scale 18, the
// scale ADR-0024 item 3's division rule gives that pair.
func BenchmarkDecimalDiv2048(b *testing.B) {
	x, y, out := benchDecimalOperands(7, 15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalDiv(x[j], 2, y[j], 2, 18)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalDivScale6_2048 divides at the rule's floor scale of 6, the
// cheaper shape a narrow operand pair produces.
func BenchmarkDecimalDivScale6_2048(b *testing.B) {
	x, y, out := benchDecimalOperands(8, 9)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalDiv(x[j], 2, y[j], 2, 6)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalDivWide2048 divides two DECIMAL(38,10) batches at scale 6,
// the type ADR-0024 item 3 gives that pair. The rescaled dividend needs 44
// digits, so this is the exact big.Int arm — the one shape of the five that
// still allocates, and the number that would justify a 256-by-128 division.
func BenchmarkDecimalDivWide2048(b *testing.B) {
	x, y, out := benchDecimalOperands(13, 38)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalDiv(x[j], 10, y[j], 10, 6)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalMod2048 takes the remainder of two DECIMAL(15,2) batches.
func BenchmarkDecimalMod2048(b *testing.B) {
	x, y, out := benchDecimalOperands(9, 15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = DecimalMod(x[j], 2, y[j], 2, 2)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalRescaleDown2048 is the rounding half on its own: scale 6 to
// scale 2 over a batch.
func BenchmarkDecimalRescaleDown2048(b *testing.B) {
	x, _, out := benchDecimalOperands(10, 18)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = Rescale(x[j], 6, 2)
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalInt128Mul is the checked 128-bit multiply alone.
func BenchmarkDecimalInt128Mul(b *testing.B) {
	x, y, out := benchDecimalOperands(11, 19)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _ = x[j].Mul(y[j])
		}
	}
	benchDecimalReport(b, len(x))
}

// BenchmarkDecimalInt128QuoRem is the 128-bit division alone, with a divisor
// past 64 bits so the normalized-estimate path is the one measured.
func BenchmarkDecimalInt128QuoRem(b *testing.B) {
	x, y, out := benchDecimalOperands(12, 38)
	for j := range y {
		if y[j].FitsInt64() {
			if wide, ok := y[j].MulPow10(19); ok {
				y[j] = wide
			}
		}
		if y[j].IsZero() {
			y[j] = Int128From(1)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range x {
			out[j], _, _ = x[j].QuoRem(y[j])
		}
	}
	benchDecimalReport(b, len(x))
}
