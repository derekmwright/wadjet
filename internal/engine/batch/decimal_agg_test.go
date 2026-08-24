package batch

import (
	"math/big"
	"testing"
)

// The exact-arithmetic half of #455: SUM over a DECIMAL accumulates in an
// Int128 and AVG divides one, so both need an answer for "the exact result
// does not fit" that is not a wrapped or rounded number.

func TestInt128AddCheckedReportsOverflow(t *testing.T) {
	max := Int128{Hi: 1<<63 - 1, Lo: ^uint64(0)} // 2^127 - 1
	min := Int128{Hi: -1 << 63, Lo: 0}           // -2^127
	one := Int128From(1)
	cases := []struct {
		name    string
		a, b    Int128
		wantOK  bool
		wantSum string // exact base-10 sum, checked only when wantOK
	}{
		{"small", Int128From(7), Int128From(35), true, "42"},
		{"mixed signs cannot overflow", max, min, true, "-1"},
		{"negative small", Int128From(-7), Int128From(-35), true, "-42"},
		{"at the ceiling", Int128{Hi: 1<<63 - 1, Lo: ^uint64(0) - 1}, one, true, "170141183460469231731687303715884105727"},
		{"past the ceiling", max, one, false, ""},
		{"past the floor", min, Int128From(-1), false, ""},
		{"two halves of the range", Int128{Hi: 1 << 62}, Int128{Hi: 1 << 62}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.a.AddChecked(tc.b)
			if ok != tc.wantOK {
				t.Fatalf("AddChecked ok=%v, want %v (sum rendered %s)", ok, tc.wantOK, got.String())
			}
			if !ok {
				return
			}
			// The exact answer, computed independently in big.Int.
			want := new(big.Int).Add(tc.a.BigInt(), tc.b.BigInt())
			if got.BigInt().Cmp(want) != 0 {
				t.Fatalf("AddChecked = %s, want %s", got.String(), want.String())
			}
			if tc.wantSum != "" && got.String() != tc.wantSum {
				t.Fatalf("AddChecked = %s, want %s", got.String(), tc.wantSum)
			}
		})
	}
}

// AddChecked must agree with big.Int on every addition it calls exact — a
// property test over the boundaries, since the sign rule is the only thing
// standing between a wrapped sum and a reported one.
func TestInt128AddCheckedAgreesWithBigInt(t *testing.T) {
	vals := []Int128{
		{}, Int128From(1), Int128From(-1), Int128From(1 << 62), Int128From(-1 << 62),
		{Hi: 1, Lo: 0}, {Hi: -1, Lo: 0}, {Hi: 1 << 62}, {Hi: -1 << 62},
		{Hi: 1<<63 - 1, Lo: ^uint64(0)}, {Hi: -1 << 63, Lo: 0},
		ParseDecimalString("977777777887777.7577887713", 10),
		ParseDecimalString("-888888888988888.8888988830", 10),
	}
	lo := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	hi := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	for _, a := range vals {
		for _, b := range vals {
			sum, ok := a.AddChecked(b)
			want := new(big.Int).Add(a.BigInt(), b.BigInt())
			fits := want.Cmp(lo) >= 0 && want.Cmp(hi) <= 0
			if ok != fits {
				t.Fatalf("%s + %s: ok=%v, but the exact sum %s %s in an Int128",
					a.String(), b.String(), ok, want.String(),
					map[bool]string{true: "DOES fit", false: "does NOT fit"}[fits])
			}
			if ok && sum.BigInt().Cmp(want) != 0 {
				t.Fatalf("%s + %s = %s, want %s", a.String(), b.String(), sum.String(), want.String())
			}
		}
	}
}

func TestAvgScaleWidensByFourAndCaps(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, 4}, {2, 6}, {10, 14}, {33, 37}, {34, 38}, {35, 38}, {38, 38},
	} {
		if got := AvgScale(tc.in); got != tc.want {
			t.Errorf("AvgScale(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// DecimalAvg against big.Rat: the quotient rounded half away from zero at the
// requested scale, for values on both sides of the int64 fast path.
func TestDecimalAvgMatchesBigRat(t *testing.T) {
	cases := []struct {
		name     string
		unscaled string
		scale    int
		count    int64
	}{
		{"exact halves", "1000", 2, 2},
		{"repeating third", "100", 2, 3},
		{"negative repeating", "-100", 2, 3},
		{"rounds up at the boundary", "1", 0, 32},
		{"rounds down away from the boundary", "1", 0, 33},
		{"negative rounds away from zero", "-1", 0, 32},
		{"wide sum past 64 bits", "9777777778877777577887713", 10, 7},
		{"wide negative sum", "-8888888889888888888988830", 10, 13},
		{"very wide sum", "99999999999999999999999999999999999999", 10, 3},
		{"single row keeps the value", "9777777778877777577887713", 10, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, ok := new(big.Int).SetString(tc.unscaled, 10)
			if !ok {
				t.Fatalf("bad fixture %q", tc.unscaled)
			}
			sum := int128FromBig(n)
			if sum.BigInt().Cmp(n) != 0 {
				t.Fatalf("fixture %s has no Int128", tc.unscaled)
			}
			outScale := AvgScale(tc.scale)
			got, gotOK := DecimalAvg(sum, tc.count, outScale-tc.scale)

			// Reference: the true rational average, rendered at outScale
			// with half-away-from-zero rounding.
			want := new(big.Rat).SetFrac(n, big.NewInt(tc.count))
			want.Mul(want, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(outScale-tc.scale)), nil)))
			num, den := want.Num(), want.Denom()
			q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
			rem.Abs(rem)
			if rem.Lsh(rem, 1).Cmp(den) >= 0 {
				if num.Sign() < 0 {
					q.Sub(q, big.NewInt(1))
				} else {
					q.Add(q, big.NewInt(1))
				}
			}
			wantOK := fitsInt128(q)
			if gotOK != wantOK {
				t.Fatalf("DecimalAvg ok=%v, want %v (exact quotient %s)", gotOK, wantOK, q.String())
			}
			if !wantOK {
				return
			}
			if got.BigInt().Cmp(q) != 0 {
				t.Fatalf("DecimalAvg = %s, want %s (unscaled at scale %d)", got.String(), q.String(), outScale)
			}
		})
	}
}

// The reachable overflow: a sum that fits an Int128 has an average that does
// not, because the scale widening multiplies before it divides.
func TestDecimalAvgReportsAnUnrepresentableQuotient(t *testing.T) {
	sum := ParseDecimalString("99999999999999999999999999.9999999999", 10) // 10^36 - ulp
	if _, ok := DecimalAvg(sum, 1, 4); ok {
		t.Fatal("an average needing 40 digits reported an exact Int128 answer")
	}
	if _, ok := DecimalAvg(sum, 1000000, 4); !ok {
		t.Fatal("the same sum over a million rows does fit and should have answered")
	}
}

func TestDecimalAvgRejectsAnEmptyCount(t *testing.T) {
	if _, ok := DecimalAvg(Int128From(5), 0, 4); ok {
		t.Fatal("DecimalAvg answered for a zero row count; AVG over no rows is NULL")
	}
}
