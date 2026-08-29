package kernel

import (
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// decArithVec is a column of unscaled carriers at a scale.
func decArithVec(scale int, unscaled ...int64) DecimalOperandVec {
	d := make([]batch.Int128, len(unscaled))
	for i, v := range unscaled {
		d[i] = batch.Int128From(v)
	}
	return DecimalOperandVec{Data: d, Scale: scale}
}

func decArithConst(scale int, unscaled int64) DecimalOperandVec {
	return DecimalOperandVec{Const: batch.Int128From(unscaled), Scale: scale}
}

func decArithText(v batch.Int128, scale int) string { return v.FormatDecimal(scale) }

// TestDecimalArithVecComputesEveryShapeIdentically is the kernel's core
// obligation: the three operand SHAPES — column/column, column/constant,
// constant/column — are a property of the expression and must never change the
// VALUE. The same three numbers go in each way round and the same answer comes
// out.
func TestDecimalArithVecComputesEveryShapeIdentically(t *testing.T) {
	// 12.75 (scale 2) against 12.7500 (scale 4), the ±1-ulp neighbourhood
	// where a float answer and an exact one visibly disagree.
	const n = 3
	for _, tc := range []struct {
		name     string
		op       DecimalOp
		outP     int
		outS     int
		want     string
		wantFlip string // the answer with the operands swapped
	}{
		{"add", DecimalOpAdd, 19, 4, "25.5000", "25.5000"},
		{"sub", DecimalOpSub, 19, 4, "0.0000", "0.0000"},
		{"mul", DecimalOpMul, 28, 6, "162.562500", "162.562500"},
		{"div", DecimalOpDiv, 32, 21, "1.000000000000000000000", "1.000000000000000000000"},
		{"mod", DecimalOpMod, 11, 4, "0.0000", "0.0000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := decArithVec(2, 1275, 1275, 1275)
			r := decArithVec(4, 127500, 127500, 127500)
			lc := decArithConst(2, 1275)
			rc := decArithConst(4, 127500)
			for _, shape := range []struct {
				name string
				l, r DecimalOperandVec
				want string
			}{
				{"col/col", l, r, tc.want},
				{"col/const", l, rc, tc.want},
				{"const/col", lc, r, tc.want},
				{"const/const", lc, rc, tc.want},
				{"flipped col/col", r, l, tc.wantFlip},
			} {
				out := make([]batch.Int128, n)
				f := DecimalArithVec(tc.op, out, shape.l, shape.r, tc.outP, tc.outS, n, nil)
				if !f.Fine() {
					t.Fatalf("%s: fault %v at row %d", shape.name, f.Status, f.Row)
				}
				for i := 0; i < n; i++ {
					if got := decArithText(out[i], tc.outS); got != shape.want {
						t.Errorf("%s row %d = %s, want %s", shape.name, i, got, shape.want)
					}
				}
			}
		})
	}
}

// TestDecimalArithVecSkipsNullRows: a NULL operand answers NULL, and the row's
// carrier is whatever the vector happened to hold. Running the operation over
// it would raise 22012 for a divisor that was never there — so the kernel must
// SKIP a masked row rather than compute it.
func TestDecimalArithVecSkipsNullRows(t *testing.T) {
	const n = 3
	l := decArithVec(2, 100, 100, 100)
	// Row 1's divisor is a zero carrier under a NULL.
	r := decArithVec(2, 200, 0, 400)
	nulls := batch.NewBitmap(n)
	nulls.SetNull(1)

	out := make([]batch.Int128, n)
	for i := range out {
		out[i] = batch.Int128From(-999) // a value the kernel must not leave behind
	}
	f := DecimalArithVec(DecimalOpDiv, out, l, r, 20, 6, n, &nulls)
	if !f.Fine() {
		t.Fatalf("a NULL divisor must not raise: got %v at row %d", f.Status, f.Row)
	}
	if got := decArithText(out[0], 6); got != "0.500000" {
		t.Errorf("row 0 = %s, want 0.500000", got)
	}
	if got := decArithText(out[2], 6); got != "0.250000" {
		t.Errorf("row 2 = %s, want 0.250000", got)
	}
	if out[1] != batch.Int128From(-999) {
		t.Errorf("row 1 was written; a null row must be left to the caller's mask")
	}
}

// TestDecimalArithVecReportsTheFirstFault: an unrepresentable value is an
// error, not a number (ADR-0024 item 4), and the ROW travels with it so the
// caller can name the value without running the operation twice.
func TestDecimalArithVecReportsTheFirstFault(t *testing.T) {
	t.Run("overflow past the declared precision", func(t *testing.T) {
		// 999.99 * 999.99 needs six integer digits; DECIMAL(5,0) allows five.
		l := decArithVec(2, 1, 99999)
		r := decArithVec(2, 1, 99999)
		out := make([]batch.Int128, 2)
		f := DecimalArithVec(DecimalOpMul, out, l, r, 5, 0, 2, nil)
		if f.Status != batch.DecimalOverflow || f.Row != 1 {
			t.Errorf("fault = (%v, row %d), want (DecimalOverflow, row 1)", f.Status, f.Row)
		}
	})
	t.Run("division by zero is its own status", func(t *testing.T) {
		l := decArithVec(2, 100, 100)
		r := decArithVec(2, 200, 0)
		out := make([]batch.Int128, 2)
		f := DecimalArithVec(DecimalOpDiv, out, l, r, 20, 6, 2, nil)
		if f.Status != batch.DecimalDivByZero || f.Row != 1 {
			t.Errorf("fault = (%v, row %d), want (DecimalDivByZero, row 1)", f.Status, f.Row)
		}
	})
	t.Run("modulo by zero too", func(t *testing.T) {
		l := decArithVec(2, 100)
		r := decArithVec(2, 0)
		out := make([]batch.Int128, 1)
		f := DecimalArithVec(DecimalOpMod, out, l, r, 20, 2, 1, nil)
		if f.Status != batch.DecimalDivByZero {
			t.Errorf("fault = %v, want DecimalDivByZero", f.Status)
		}
	})
}

// TestDecimalScalarVecMatchesPostgres pins the scalar math functions against
// values verified live on postgres:17.11 — round/trunc half away from zero,
// ceil and floor in their fixed directions.
func TestDecimalScalarVecMatchesPostgres(t *testing.T) {
	for _, tc := range []struct {
		name   string
		op     batch.DecimalScalarOp
		in     []int64 // unscaled at scale 2
		digits int
		outP   int
		outS   int
		want   []string
	}{
		{"abs", batch.DecimalScalarAbs, []int64{1275, -1275, 0}, 0, 9, 2,
			[]string{"12.75", "12.75", "0.00"}},
		{"ceil", batch.DecimalScalarCeil, []int64{1275, -1275, 1200}, 0, 8, 0,
			[]string{"13", "-12", "12"}},
		{"floor", batch.DecimalScalarFloor, []int64{1275, -1275, 1200}, 0, 8, 0,
			[]string{"12", "-13", "12"}},
		// round(12.75) = 13, round(-12.75) = -13: half AWAY from zero.
		{"round to integer", batch.DecimalScalarRound, []int64{1275, -1275, 250, 150}, 0, 8, 0,
			[]string{"13", "-13", "3", "2"}},
		{"round to one digit", batch.DecimalScalarRound, []int64{1275, -1275}, 1, 9, 1,
			[]string{"12.8", "-12.8"}},
		{"trunc to integer", batch.DecimalScalarTrunc, []int64{1289, -1289}, 0, 7, 0,
			[]string{"12", "-12"}},
		{"trunc to one digit", batch.DecimalScalarTrunc, []int64{1289, -1289}, 1, 8, 1,
			[]string{"12.8", "-12.8"}},
		{"sign", batch.DecimalScalarSign, []int64{1275, -1275, 0}, 0, 1, 0,
			[]string{"1", "-1", "0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := make([]batch.Int128, len(tc.in))
			for i, v := range tc.in {
				in[i] = batch.Int128From(v)
			}
			out := make([]batch.Int128, len(in))
			f := DecimalScalarVec(tc.op, out, in, 2, tc.digits, tc.outP, tc.outS, len(in), nil)
			if !f.Fine() {
				t.Fatalf("fault %v at row %d", f.Status, f.Row)
			}
			for i := range out {
				if got := decArithText(out[i], tc.outS); got != tc.want[i] {
					t.Errorf("row %d = %s, want %s", i, got, tc.want[i])
				}
			}
		})
	}
}

// TestDecimalScalarNegativeDigits is round/trunc to a power of ten ABOVE the
// point, verified live: round(1234.56, -2) = 1200 and round(1250, -2) = 1300.
// Rounding to scale 0 first and adjusting would round twice, and 1249 would
// come back 1300.
func TestDecimalScalarNegativeDigits(t *testing.T) {
	for _, tc := range []struct {
		name     string
		op       batch.DecimalScalarOp
		unscaled int64
		inScale  int
		digits   int
		want     string
	}{
		{"round 1234.56 to -2", batch.DecimalScalarRound, 123456, 2, -2, "1200"},
		{"round 1250 to -2", batch.DecimalScalarRound, 1250, 0, -2, "1300"},
		{"round 1249 to -2", batch.DecimalScalarRound, 1249, 0, -2, "1200"},
		{"round -1250 to -2", batch.DecimalScalarRound, -1250, 0, -2, "-1300"},
		{"trunc 1234.56 to -2", batch.DecimalScalarTrunc, 123456, 2, -2, "1200"},
		{"trunc 1299 to -2", batch.DecimalScalarTrunc, 1299, 0, -2, "1200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, st := batch.DecimalScalar(tc.op, batch.Int128From(tc.unscaled), tc.inScale, tc.digits, 38, 0)
			if st != batch.DecimalOK {
				t.Fatalf("status %v", st)
			}
			if got := decArithText(v, 0); got != tc.want {
				t.Errorf("= %s, want %s", got, tc.want)
			}
		})
	}
}

// --- Benchmarks -------------------------------------------------------------
//
// The claim these hold is ZERO allocations per batch on the two shapes a
// projection actually takes: a DECIMAL column against another DECIMAL column,
// and a DECIMAL column against a broadcast constant. An allocation here is one
// per ROW at 2048 rows a batch, which is the cost the big.Int arm carries and
// the reason the Int128 fast paths exist.

func benchDecimalOperands(n int) (l, r DecimalOperandVec, out []batch.Int128) {
	rng := rand.New(rand.NewSource(1))
	ld := make([]batch.Int128, n)
	rd := make([]batch.Int128, n)
	for i := range ld {
		// Values around 1e6 at scale 2 and 1e4 at scale 4: an ordinary
		// monetary batch, well inside every intermediate.
		ld[i] = batch.Int128From(rng.Int63n(100_000_000) + 1)
		rd[i] = batch.Int128From(rng.Int63n(100_000_000) + 1)
	}
	return DecimalOperandVec{Data: ld, Scale: 2},
		DecimalOperandVec{Data: rd, Scale: 4},
		make([]batch.Int128, n)
}

func benchDecimalArith(b *testing.B, op DecimalOp, outP, outS int, constRHS bool) {
	const n = 2048 // batch.DefaultBatchSize
	l, r, out := benchDecimalOperands(n)
	if constRHS {
		r = DecimalOperandVec{Const: batch.Int128From(125000), Scale: 4}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f := DecimalArithVec(op, out, l, r, outP, outS, n, nil); !f.Fine() {
			b.Fatalf("fault %v at row %d", f.Status, f.Row)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
}

func BenchmarkDecimalAddColCol(b *testing.B)   { benchDecimalArith(b, DecimalOpAdd, 19, 4, false) }
func BenchmarkDecimalAddColConst(b *testing.B) { benchDecimalArith(b, DecimalOpAdd, 19, 4, true) }
func BenchmarkDecimalSubColCol(b *testing.B)   { benchDecimalArith(b, DecimalOpSub, 19, 4, false) }
func BenchmarkDecimalMulColCol(b *testing.B)   { benchDecimalArith(b, DecimalOpMul, 28, 6, false) }
func BenchmarkDecimalMulColConst(b *testing.B) { benchDecimalArith(b, DecimalOpMul, 28, 6, true) }
func BenchmarkDecimalDivColCol(b *testing.B)   { benchDecimalArith(b, DecimalOpDiv, 32, 21, false) }
func BenchmarkDecimalDivColConst(b *testing.B) { benchDecimalArith(b, DecimalOpDiv, 32, 21, true) }
func BenchmarkDecimalModColCol(b *testing.B)   { benchDecimalArith(b, DecimalOpMod, 11, 4, false) }

func BenchmarkDecimalRoundScalar(b *testing.B) {
	const n = 2048
	l, _, out := benchDecimalOperands(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f := DecimalScalarVec(batch.DecimalScalarRound, out, l.Data, 2, 1, 9, 1, n, nil); !f.Fine() {
			b.Fatalf("fault %v", f.Status)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
}
