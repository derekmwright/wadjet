package batch

import (
	"math"
	"math/big"
	"math/rand"
	"testing"
)

// The oracle for every property test below is math/big, built here from
// scratch rather than borrowed from the implementation: an exact big.Rat for
// the VALUE the two operands name, an exact big.Int for the carrier's range,
// and this file's own half-away-from-zero rounding. A test that reuses the
// production narrowing or the production rounding proves only that the code
// agrees with itself.

var (
	// The signed 128-bit range, spelled out.
	arithMaxI128, _ = new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	arithMinI128, _ = new(big.Int).SetString("-170141183460469231731687303715884105728", 10)
	arithTwo128     = new(big.Int).Lsh(big.NewInt(1), 128)
	arithMask64     = new(big.Int).SetUint64(math.MaxUint64)
)

// arithBig renders an Int128 as an exact big.Int: Hi*2^64 + Lo.
func arithBig(v Int128) *big.Int {
	b := new(big.Int).SetInt64(v.Hi)
	b.Lsh(b, 64)
	return b.Add(b, new(big.Int).SetUint64(v.Lo))
}

// arithFits reports whether an exact big.Int has an Int128.
func arithFits(b *big.Int) bool {
	return b.Cmp(arithMinI128) >= 0 && b.Cmp(arithMaxI128) <= 0
}

// arithI128 converts an exact big.Int inside the range to an Int128, by two's
// complement, independently of int128FromBig.
func arithI128(tb testing.TB, b *big.Int) Int128 {
	tb.Helper()
	if !arithFits(b) {
		tb.Fatalf("value %s has no Int128", b)
	}
	m := new(big.Int).Mod(b, arithTwo128)
	hi := new(big.Int).Rsh(m, 64).Uint64()
	lo := new(big.Int).And(m, arithMask64).Uint64()
	return Int128{Hi: int64(hi), Lo: lo}
}

// arithParse builds an Int128 from base-10 text.
func arithParse(tb testing.TB, s string) Int128 {
	tb.Helper()
	b, ok := new(big.Int).SetString(s, 10)
	if !ok {
		tb.Fatalf("not a number: %q", s)
	}
	return arithI128(tb, b)
}

func arithPow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// arithRoundRatHalfAway multiplies the exact value r by 10^scale and rounds
// half AWAY FROM ZERO, returning the exact unscaled integer.
func arithRoundRatHalfAway(r *big.Rat, scale int) *big.Int {
	x := new(big.Rat).Mul(r, new(big.Rat).SetInt(arithPow10(scale)))
	num, den := x.Num(), x.Denom() // Denom is always positive and in lowest terms
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	if rem.Sign() != 0 {
		twice := new(big.Int).Lsh(new(big.Int).Abs(rem), 1)
		if twice.Cmp(den) >= 0 {
			if x.Sign() < 0 {
				q.Sub(q, big.NewInt(1))
			} else {
				q.Add(q, big.NewInt(1))
			}
		}
	}
	return q
}

// arithRat is the exact value an unscaled integer names at a scale.
func arithRat(v Int128, scale int) *big.Rat {
	return new(big.Rat).SetFrac(arithBig(v), arithPow10(scale))
}

// arithRandI128 draws a random Int128 whose magnitude has up to 127 bits,
// weighted so small values, wide values and the extremes all appear.
func arithRandI128(rng *rand.Rand) Int128 {
	switch rng.Intn(16) {
	case 0:
		return Int128{}
	case 1:
		return Int128Max
	case 2:
		return Int128Min
	case 3:
		return Int128From(1)
	case 4:
		return Int128From(-1)
	case 5:
		// A power of ten, including the 38-digit boundary.
		p, _ := DecimalPow10(rng.Intn(MaxDecimalPrecision + 1))
		if rng.Intn(2) == 0 {
			p = p.Neg()
		}
		return p
	}
	w := rng.Intn(127) + 1
	mag := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), uint(w)))
	if rng.Intn(2) == 0 {
		mag.Neg(mag)
	}
	m := new(big.Int).Mod(mag, arithTwo128)
	hi := new(big.Int).Rsh(m, 64).Uint64()
	lo := new(big.Int).And(m, arithMask64).Uint64()
	return Int128{Hi: int64(hi), Lo: lo}
}

// arithRandUnscaled draws a random unscaled value strictly inside 10^p, the
// bound a DECIMAL(p,s) column declares, with the boundary itself represented.
func arithRandUnscaled(tb testing.TB, rng *rand.Rand, p int) Int128 {
	tb.Helper()
	limit := arithPow10(p)
	var b *big.Int
	switch rng.Intn(8) {
	case 0:
		b = big.NewInt(0)
	case 1:
		b = new(big.Int).Sub(limit, big.NewInt(1)) // widest value the precision holds
	case 2:
		b = big.NewInt(1)
	default:
		b = new(big.Int).Rand(rng, limit)
	}
	if rng.Intn(2) == 0 {
		b = new(big.Int).Neg(b)
	}
	return arithI128(tb, b)
}

// --- Int128.Mul ---

func TestInt128Mul(t *testing.T) {
	tests := []struct {
		name    string
		a, b    string
		want    string
		wantErr bool
	}{
		{name: "zero times zero", a: "0", b: "0", want: "0"},
		{name: "zero times max", a: "0", b: "170141183460469231731687303715884105727", want: "0"},
		{name: "one times min", a: "1", b: "-170141183460469231731687303715884105728", want: "-170141183460469231731687303715884105728"},
		{name: "small positive", a: "6", b: "7", want: "42"},
		{name: "mixed signs", a: "-6", b: "7", want: "-42"},
		{name: "both negative", a: "-6", b: "-7", want: "42"},
		{name: "10^19 times 10^19", a: "10000000000000000000", b: "10000000000000000000", want: "100000000000000000000000000000000000000"},
		{name: "10^19 times 10^20 overflows", a: "10000000000000000000", b: "100000000000000000000", wantErr: true},
		{name: "2^63 squared", a: "9223372036854775808", b: "9223372036854775808", want: "85070591730234615865843651857942052864"},
		{name: "2^64 times 2^63 overflows the positive range", a: "18446744073709551616", b: "9223372036854775808", wantErr: true},
		{name: "negative 2^64 times 2^63 is exactly the minimum", a: "-18446744073709551616", b: "9223372036854775808", want: "-170141183460469231731687303715884105728"},
		{name: "max times minus one", a: "170141183460469231731687303715884105727", b: "-1", want: "-170141183460469231731687303715884105727"},
		{name: "min times minus one overflows", a: "-170141183460469231731687303715884105728", b: "-1", wantErr: true},
		{name: "min times two overflows", a: "-170141183460469231731687303715884105728", b: "2", wantErr: true},
		{name: "max times two overflows", a: "170141183460469231731687303715884105727", b: "2", wantErr: true},
		{name: "both wide, product still fits", a: "18446744073709551617", b: "9223372036854775807", want: "170141183460469231722463931679029329919"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := arithParse(t, tt.a), arithParse(t, tt.b)
			got, ok := a.Mul(b)
			if ok == tt.wantErr {
				t.Fatalf("Mul(%s, %s) ok = %v, want %v", tt.a, tt.b, ok, !tt.wantErr)
			}
			if tt.wantErr {
				if !got.IsZero() {
					t.Fatalf("an overflowing Mul must return the zero value, got %s", got)
				}
				return
			}
			if got.String() != tt.want {
				t.Fatalf("Mul(%s, %s) = %s, want %s", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestInt128MulMatchesBigInt(t *testing.T) {
	rng := rand.New(rand.NewSource(20260829))
	extremes := []Int128{
		{}, Int128From(1), Int128From(-1), Int128Max, Int128Min,
		{Hi: 0, Lo: math.MaxUint64}, {Hi: -1, Lo: 0}, {Hi: 1, Lo: 0},
	}
	pairs := 0
	check := func(a, b Int128) {
		t.Helper()
		want := new(big.Int).Mul(arithBig(a), arithBig(b))
		got, ok := a.Mul(b)
		if ok != arithFits(want) {
			t.Fatalf("Mul(%s, %s) ok = %v, want %v (exact product %s)", a, b, ok, arithFits(want), want)
		}
		if ok && got.String() != want.String() {
			t.Fatalf("Mul(%s, %s) = %s, want %s", a, b, got, want)
		}
		pairs++
	}
	for _, a := range extremes {
		for _, b := range extremes {
			check(a, b)
		}
	}
	for i := 0; i < 50000; i++ {
		check(arithRandI128(rng), arithRandI128(rng))
	}
	if pairs < 50000 {
		t.Fatalf("only %d pairs checked", pairs)
	}
}

// --- Int128.QuoRem ---

func TestInt128QuoRem(t *testing.T) {
	tests := []struct {
		name    string
		a, b    string
		q, r    string
		wantErr bool
	}{
		{name: "truncates toward zero", a: "7", b: "2", q: "3", r: "1"},
		{name: "negative dividend keeps its sign in the remainder", a: "-7", b: "2", q: "-3", r: "-1"},
		{name: "negative divisor", a: "7", b: "-2", q: "-3", r: "1"},
		{name: "both negative", a: "-7", b: "-2", q: "3", r: "-1"},
		{name: "exact", a: "100", b: "5", q: "20", r: "0"},
		{name: "divisor larger than dividend", a: "3", b: "100", q: "0", r: "3"},
		{name: "zero divisor", a: "1", b: "0", wantErr: true},
		{name: "minimum over minus one has no quotient", a: "-170141183460469231731687303715884105728", b: "-1", wantErr: true},
		{name: "minimum over one is itself", a: "-170141183460469231731687303715884105728", b: "1", q: "-170141183460469231731687303715884105728", r: "0"},
		{name: "minimum over two", a: "-170141183460469231731687303715884105728", b: "2", q: "-85070591730234615865843651857942052864", r: "0"},
		{name: "maximum over the minimum", a: "170141183460469231731687303715884105727", b: "-170141183460469231731687303715884105728", q: "0", r: "170141183460469231731687303715884105727"},
		{name: "wide over wide", a: "170141183460469231731687303715884105727", b: "18446744073709551616", q: "9223372036854775807", r: "18446744073709551615"},
		{name: "wide over 10^19", a: "100000000000000000000000000000000000000", b: "10000000000000000000", q: "10000000000000000000", r: "0"},
		{name: "divisor past 64 bits", a: "99999999999999999999999999999999999999", b: "12345678901234567890123", q: "8100000072900000", r: "8190003700810033299999"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := arithParse(t, tt.a), arithParse(t, tt.b)
			q, r, ok := a.QuoRem(b)
			if ok == tt.wantErr {
				t.Fatalf("QuoRem(%s, %s) ok = %v, want %v", tt.a, tt.b, ok, !tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if q.String() != tt.q || r.String() != tt.r {
				t.Fatalf("QuoRem(%s, %s) = (%s, %s), want (%s, %s)", tt.a, tt.b, q, r, tt.q, tt.r)
			}
		})
	}
}

func TestInt128QuoRemMatchesBigInt(t *testing.T) {
	rng := rand.New(rand.NewSource(4242))
	check := func(a, b Int128) {
		t.Helper()
		q, r, ok := a.QuoRem(b)
		bb := arithBig(b)
		if bb.Sign() == 0 {
			if ok {
				t.Fatalf("QuoRem(%s, 0) must not answer", a)
			}
			return
		}
		ab := arithBig(a)
		wantQ, wantR := new(big.Int).QuoRem(ab, bb, new(big.Int))
		if !arithFits(wantQ) {
			if ok {
				t.Fatalf("QuoRem(%s, %s) answered %s for a quotient outside the carrier (%s)", a, b, q, wantQ)
			}
			return
		}
		if !ok {
			t.Fatalf("QuoRem(%s, %s) refused a representable quotient %s", a, b, wantQ)
		}
		if q.String() != wantQ.String() || r.String() != wantR.String() {
			t.Fatalf("QuoRem(%s, %s) = (%s, %s), want (%s, %s)", a, b, q, r, wantQ, wantR)
		}
		// The identity that makes the pair meaningful.
		back := new(big.Int).Add(new(big.Int).Mul(arithBig(q), bb), arithBig(r))
		if back.Cmp(ab) != 0 {
			t.Fatalf("q*b+r = %s, want %s", back, ab)
		}
	}
	for i := 0; i < 20000; i++ {
		check(arithRandI128(rng), arithRandI128(rng))
	}
	// Divisors past 64 bits exercise the normalized-estimate path, and small
	// ones the two-step 64-bit path; draw both deliberately.
	for i := 0; i < 20000; i++ {
		a := arithRandI128(rng)
		var b Int128
		if i%2 == 0 {
			b = Int128From(rng.Int63n(1<<40) + 1)
		} else {
			wide := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), 126))
			wide.Add(wide, new(big.Int).Lsh(big.NewInt(1), 64))
			if rng.Intn(2) == 0 {
				wide.Neg(wide)
			}
			b = arithI128(t, wide)
		}
		check(a, b)
	}
}

func TestSubChecked(t *testing.T) {
	tests := []struct {
		name    string
		a, b    string
		want    string
		wantErr bool
	}{
		{name: "plain", a: "10", b: "3", want: "7"},
		{name: "crosses zero", a: "3", b: "10", want: "-7"},
		{name: "minimum minus one overflows", a: "-170141183460469231731687303715884105728", b: "1", wantErr: true},
		{name: "maximum minus minus one overflows", a: "170141183460469231731687303715884105727", b: "-1", wantErr: true},
		{name: "maximum minus maximum", a: "170141183460469231731687303715884105727", b: "170141183460469231731687303715884105727", want: "0"},
		{name: "minimum minus minimum", a: "-170141183460469231731687303715884105728", b: "-170141183460469231731687303715884105728", want: "0"},
		{name: "zero minus minimum overflows", a: "0", b: "-170141183460469231731687303715884105728", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := arithParse(t, tt.a), arithParse(t, tt.b)
			got, ok := a.SubChecked(b)
			if ok == tt.wantErr {
				t.Fatalf("SubChecked(%s, %s) ok = %v, want %v", tt.a, tt.b, ok, !tt.wantErr)
			}
			if !tt.wantErr && got.String() != tt.want {
				t.Fatalf("SubChecked(%s, %s) = %s, want %s", tt.a, tt.b, got, tt.want)
			}
		})
	}
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 20000; i++ {
		a, b := arithRandI128(rng), arithRandI128(rng)
		want := new(big.Int).Sub(arithBig(a), arithBig(b))
		got, ok := a.SubChecked(b)
		if ok != arithFits(want) {
			t.Fatalf("SubChecked(%s, %s) ok = %v, want %v", a, b, ok, arithFits(want))
		}
		if ok && got.String() != want.String() {
			t.Fatalf("SubChecked(%s, %s) = %s, want %s", a, b, got, want)
		}
	}
}

// --- Rescale ---

func TestRescale(t *testing.T) {
	tests := []struct {
		name     string
		v        string
		from, to int
		want     string
		wantErr  bool
	}{
		{name: "up is exact", v: "12345", from: 2, to: 4, want: "1234500"},
		{name: "same scale", v: "12345", from: 2, to: 2, want: "12345"},
		{name: "down truncating part below half", v: "12345", from: 2, to: 0, want: "123"},
		{name: "down at exactly half rounds away from zero", v: "12550", from: 2, to: 0, want: "126"},
		{name: "negative at exactly half rounds away from zero", v: "-12550", from: 2, to: 0, want: "-126"},
		{name: "one and a half", v: "15", from: 1, to: 0, want: "2"},
		{name: "minus one and a half", v: "-15", from: 1, to: 0, want: "-2"},
		{name: "two and a half is three, not two", v: "25", from: 1, to: 0, want: "3"},
		{name: "minus two and a half is minus three", v: "-25", from: 1, to: 0, want: "-3"},
		{name: "zero stays zero", v: "0", from: 10, to: 0, want: "0"},
		{name: "maximum cannot widen", v: "170141183460469231731687303715884105727", from: 0, to: 1, wantErr: true},
		{name: "minimum narrows and rounds away from zero", v: "-170141183460469231731687303715884105728", from: 1, to: 0, want: "-17014118346046923173168730371588410573"},
		{name: "a drop past 38 digits is zero, not an error", v: "170141183460469231731687303715884105727", from: 45, to: 0, want: "0"},
		{name: "a negative scale is not a scale", v: "1", from: -1, to: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Rescale(arithParse(t, tt.v), tt.from, tt.to)
			if ok == tt.wantErr {
				t.Fatalf("Rescale(%s, %d, %d) ok = %v, want %v", tt.v, tt.from, tt.to, ok, !tt.wantErr)
			}
			if !tt.wantErr && got.String() != tt.want {
				t.Fatalf("Rescale(%s, %d, %d) = %s, want %s", tt.v, tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestRescaleMatchesBigRat(t *testing.T) {
	rng := rand.New(rand.NewSource(777))
	for i := 0; i < 20000; i++ {
		v := arithRandI128(rng)
		from := rng.Intn(MaxDecimalScale + 1)
		to := rng.Intn(MaxDecimalScale + 1)
		want := arithRoundRatHalfAway(arithRat(v, from), to)
		got, ok := Rescale(v, from, to)
		if ok != arithFits(want) {
			t.Fatalf("Rescale(%s, %d, %d) ok = %v, want %v (exact %s)", v, from, to, ok, arithFits(want), want)
		}
		if ok && got.String() != want.String() {
			t.Fatalf("Rescale(%s, %d, %d) = %s, want %s", v, from, to, got, want)
		}
	}
	// The upward half must agree with the exact shift the comparison kernels
	// already use, or two paths would read one value two ways.
	for i := 0; i < 5000; i++ {
		v := arithRandI128(rng)
		n := rng.Intn(MaxDecimalScale + 1)
		wantV, wantOK := v.MulPow10(n)
		gotV, gotOK := Rescale(v, 0, n)
		if wantOK != gotOK || (gotOK && !gotV.Equal(wantV)) {
			t.Fatalf("Rescale(%s, 0, %d) = (%s, %v), MulPow10 says (%s, %v)", v, n, gotV, gotOK, wantV, wantOK)
		}
	}
}

// --- DecimalFitsPrecision ---

func TestDecimalFitsPrecision(t *testing.T) {
	tests := []struct {
		name string
		v    string
		p    int
		want bool
	}{
		{name: "inside", v: "999", p: 3, want: true},
		{name: "at the bound", v: "1000", p: 3, want: false},
		{name: "negative at the bound", v: "-1000", p: 3, want: false},
		{name: "negative inside", v: "-999", p: 3, want: true},
		{name: "zero fits every precision", v: "0", p: 1, want: true},
		{name: "widest DECIMAL(38) value", v: "99999999999999999999999999999999999999", p: 38, want: true},
		{name: "10^38 does not fit 38 digits", v: "100000000000000000000000000000000000000", p: 38, want: false},
		{name: "the minimum has no magnitude and fits nothing", v: "-170141183460469231731687303715884105728", p: 38, want: false},
		{name: "precision zero is the unconstrained sentinel", v: "170141183460469231731687303715884105727", p: 0, want: true},
		{name: "a precision the carrier cannot express is no bound to check", v: "170141183460469231731687303715884105727", p: 50, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DecimalFitsPrecision(arithParse(t, tt.v), tt.p); got != tt.want {
				t.Fatalf("DecimalFitsPrecision(%s, %d) = %v, want %v", tt.v, tt.p, got, tt.want)
			}
		})
	}
	// Against big.Int over the whole declarable range.
	rng := rand.New(rand.NewSource(313))
	for i := 0; i < 20000; i++ {
		v := arithRandI128(rng)
		p := rng.Intn(MaxDecimalPrecision) + 1
		want := arithBig(v).CmpAbs(arithPow10(p)) < 0
		if got := DecimalFitsPrecision(v, p); got != want {
			t.Fatalf("DecimalFitsPrecision(%s, %d) = %v, want %v", v, p, got, want)
		}
	}
}

// TestDecimalFitsPrecisionMatchesTheUnexportedTwins pins the behaviour the two
// unexported copies have today — exec.decimalFitsPrecision and
// physical.setOpDecimalFitsPrecision — so converging their callers onto this
// one function cannot change a single overflow decision.
func TestDecimalFitsPrecisionMatchesTheUnexportedTwins(t *testing.T) {
	twin := func(v Int128, precision int) bool {
		if precision <= 0 || precision > MaxDecimalPrecision {
			return true
		}
		limit, ok := Int128From(1).MulPow10(precision)
		if !ok {
			return true
		}
		mag := v
		if mag.IsNegative() {
			mag = mag.Neg()
			if mag.IsNegative() {
				return false
			}
		}
		return mag.Cmp(limit) < 0
	}
	rng := rand.New(rand.NewSource(515))
	for i := 0; i < 20000; i++ {
		v := arithRandI128(rng)
		p := rng.Intn(45) - 3 // covers the <= 0 and > 38 conventions too
		if got, want := DecimalFitsPrecision(v, p), twin(v, p); got != want {
			t.Fatalf("DecimalFitsPrecision(%s, %d) = %v, the twin says %v", v, p, got, want)
		}
	}
}

// --- the fixed-point operations ---

func TestDecimalOps(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		a        string
		aScale   int
		b        string
		bScale   int
		outScale int
		want     string
		wantErr  bool
	}{
		{name: "add across scales", op: "+", a: "105", aScale: 2, b: "25", bScale: 1, outScale: 2, want: "355"},
		{name: "add rounds half away from zero", op: "+", a: "5", aScale: 1, b: "0", bScale: 0, outScale: 0, want: "1"},
		{name: "add rounds negative half away from zero", op: "+", a: "-5", aScale: 1, b: "0", bScale: 0, outScale: 0, want: "-1"},
		{name: "add overflows the carrier", op: "+", a: "170141183460469231731687303715884105727", aScale: 0, b: "1", bScale: 0, outScale: 0, wantErr: true},
		{name: "add whose intermediate overflows but whose answer fits", op: "+", a: "170141183460469231731687303715884105727", aScale: 2, b: "1", bScale: 0, outScale: 0, want: "1701411834604692317316873037158841058"},
		{name: "sub across scales", op: "-", a: "105", aScale: 2, b: "25", bScale: 1, outScale: 2, want: "-145"},
		{name: "sub to zero", op: "-", a: "12345", aScale: 2, b: "12345", bScale: 2, outScale: 2, want: "0"},
		{name: "sub underflows the carrier", op: "-", a: "-170141183460469231731687303715884105728", aScale: 0, b: "1", bScale: 0, outScale: 0, wantErr: true},
		{name: "mul at the natural scale", op: "*", a: "12345", aScale: 2, b: "100", bScale: 2, outScale: 4, want: "1234500"},
		{name: "mul rescaled down", op: "*", a: "12345", aScale: 2, b: "100", bScale: 2, outScale: 2, want: "12345"},
		{name: "mul rounds half away from zero", op: "*", a: "5", aScale: 1, b: "3", bScale: 1, outScale: 1, want: "2"},
		{name: "mul of negatives", op: "*", a: "-250", aScale: 2, b: "-400", bScale: 2, outScale: 4, want: "100000"},
		{name: "mul whose product needs 256 bits but whose answer fits", op: "*", a: "100000000000000000000000000000000000000", aScale: 38, b: "100000000000000000000000000000000000000", bScale: 38, outScale: 2, want: "100"},
		{name: "mul that cannot fit at any scale it is asked for", op: "*", a: "100000000000000000000000000000000000000", aScale: 0, b: "100000000000000000000000000000000000000", bScale: 0, outScale: 0, wantErr: true},
		{name: "one third", op: "/", a: "1", aScale: 0, b: "3", bScale: 0, outScale: 6, want: "333333"},
		{name: "two thirds rounds away from zero", op: "/", a: "2", aScale: 0, b: "3", bScale: 0, outScale: 6, want: "666667"},
		{name: "minus two thirds rounds away from zero", op: "/", a: "-2", aScale: 0, b: "3", bScale: 0, outScale: 6, want: "-666667"},
		{name: "exact division", op: "/", a: "1000", aScale: 2, b: "250", bScale: 2, outScale: 2, want: "400"},
		{name: "division by a fraction", op: "/", a: "1", aScale: 0, b: "5", bScale: 1, outScale: 2, want: "200"},
		{name: "a half rounds up at scale zero", op: "/", a: "1", aScale: 0, b: "2", bScale: 0, outScale: 0, want: "1"},
		{name: "minus a half rounds down at scale zero", op: "/", a: "-1", aScale: 0, b: "2", bScale: 0, outScale: 0, want: "-1"},
		{name: "division by zero has no answer", op: "/", a: "1", aScale: 0, b: "0", bScale: 0, outScale: 2, wantErr: true},
		{name: "division whose quotient is too wide", op: "/", a: "170141183460469231731687303715884105727", aScale: 0, b: "1", bScale: 2, outScale: 0, wantErr: true},
		{name: "mod keeps the dividend's sign", op: "%", a: "-7", aScale: 0, b: "3", bScale: 0, outScale: 0, want: "-1"},
		{name: "mod ignores the divisor's sign", op: "%", a: "7", aScale: 0, b: "-3", bScale: 0, outScale: 0, want: "1"},
		{name: "mod across scales", op: "%", a: "55", aScale: 1, b: "25", bScale: 1, outScale: 1, want: "5"},
		{name: "mod smaller than its divisor", op: "%", a: "105", aScale: 2, b: "25", bScale: 1, outScale: 2, want: "105"},
		{name: "mod by zero has no answer", op: "%", a: "1", aScale: 0, b: "0", bScale: 0, outScale: 0, wantErr: true},
		{name: "a sum that fits but not at the scale it was asked for", op: "+", a: "170141183460469231731687303715884105727", aScale: 0, b: "0", bScale: 0, outScale: 1, wantErr: true},
		{name: "a product that fits but not at the scale it was asked for", op: "*", a: "170141183460469231731687303715884105727", aScale: 0, b: "1", bScale: 0, outScale: 1, wantErr: true},
		{name: "div with the power of ten on the divisor", op: "/", a: "12345", aScale: 4, b: "2", bScale: 0, outScale: 0, want: "1"},
		{name: "div whose divisor cannot be widened in the carrier", op: "/", a: "1", aScale: 38, b: "3", bScale: 0, outScale: 0, want: "0"},
		{name: "add refuses a negative scale", op: "+", a: "1", aScale: -1, b: "1", bScale: 0, outScale: 0, wantErr: true},
		{name: "sub refuses a negative out scale", op: "-", a: "1", aScale: 0, b: "1", bScale: 0, outScale: -1, wantErr: true},
		{name: "mul refuses a negative scale", op: "*", a: "1", aScale: 0, b: "1", bScale: -2, outScale: 0, wantErr: true},
		{name: "div refuses a negative scale", op: "/", a: "1", aScale: 0, b: "1", bScale: -1, outScale: 0, wantErr: true},
		{name: "mod refuses a negative scale", op: "%", a: "1", aScale: -1, b: "1", bScale: 0, outScale: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := arithParse(t, tt.a), arithParse(t, tt.b)
			got, ok := decimalOp(tt.op, a, tt.aScale, b, tt.bScale, tt.outScale)
			if ok == tt.wantErr {
				t.Fatalf("%s(%s@%d, %s@%d)@%d ok = %v, want %v",
					tt.op, tt.a, tt.aScale, tt.b, tt.bScale, tt.outScale, ok, !tt.wantErr)
			}
			if !tt.wantErr && got.String() != tt.want {
				t.Fatalf("%s(%s@%d, %s@%d)@%d = %s, want %s",
					tt.op, tt.a, tt.aScale, tt.b, tt.bScale, tt.outScale, got, tt.want)
			}
		})
	}
}

// decimalOp dispatches by SQL operator, so the tables and the differential
// test can name an operation the way the planner will.
func decimalOp(op string, a Int128, aScale int, b Int128, bScale, outScale int) (Int128, bool) {
	switch op {
	case "+":
		return DecimalAdd(a, aScale, b, bScale, outScale)
	case "-":
		return DecimalSub(a, aScale, b, bScale, outScale)
	case "*":
		return DecimalMul(a, aScale, b, bScale, outScale)
	case "/":
		return DecimalDiv(a, aScale, b, bScale, outScale)
	case "%":
		return DecimalMod(a, aScale, b, bScale, outScale)
	}
	panic("unknown op " + op)
}

// arithRefOp is the oracle: the exact rational value of the operation, or
// nil when the operation has no value at all (a zero divisor).
func arithRefOp(op string, a Int128, aScale int, b Int128, bScale int) *big.Rat {
	ra, rb := arithRat(a, aScale), arithRat(b, bScale)
	switch op {
	case "+":
		return new(big.Rat).Add(ra, rb)
	case "-":
		return new(big.Rat).Sub(ra, rb)
	case "*":
		return new(big.Rat).Mul(ra, rb)
	case "/":
		if rb.Sign() == 0 {
			return nil
		}
		return new(big.Rat).Quo(ra, rb)
	case "%":
		if rb.Sign() == 0 {
			return nil
		}
		// a - b*trunc(a/b): the remainder of a TRUNCATING division, which is
		// what PostgreSQL's numeric mod and Go's % both answer.
		q := new(big.Rat).Quo(ra, rb)
		t := new(big.Int).Quo(q.Num(), q.Denom())
		return new(big.Rat).Sub(ra, new(big.Rat).Mul(rb, new(big.Rat).SetInt(t)))
	}
	panic("unknown op " + op)
}

// TestDecimalOpsMatchBigRatAtTheADRResultType is the differential the arc
// rests on: for random declared types and random values, the exact value is
// computed with math/big at the (p,s) ADR-0024 item 3 gives the operation,
// and the engine must answer it bit for bit when it fits, refuse exactly when
// it does not, and report the declared-precision bound the same way.
func TestDecimalOpsMatchBigRatAtTheADRResultType(t *testing.T) {
	rng := rand.New(rand.NewSource(555))
	ops := []string{"+", "-", "*", "/", "%"}
	adjusted, fitted, refused, precisionOverflows := 0, 0, 0, 0
	for i := 0; i < 4000; i++ {
		p1 := rng.Intn(MaxDecimalPrecision) + 1
		s1 := rng.Intn(p1 + 1)
		p2 := rng.Intn(MaxDecimalPrecision) + 1
		s2 := rng.Intn(p2 + 1)
		a := arithRandUnscaled(t, rng, p1)
		b := arithRandUnscaled(t, rng, p2)
		for _, op := range ops {
			p, s := DecimalResultType(op, p1, s1, p2, s2)
			if p != rawDecimalResultPrecision(op, p1, s1, p2, s2) {
				adjusted++
			}
			ref := arithRefOp(op, a, s1, b, s2)
			got, ok := decimalOp(op, a, s1, b, s2, s)
			if ref == nil {
				if ok {
					t.Fatalf("%s(%s@%d, %s@%d) answered %s for a zero divisor", op, a, s1, b, s2, got)
				}
				continue
			}
			want := arithRoundRatHalfAway(ref, s)
			wantOK := arithFits(want)
			if ok != wantOK {
				t.Fatalf("%s(%s@%d, %s@%d)@%d ok = %v, want %v (exact %s, type DECIMAL(%d,%d))",
					op, a, s1, b, s2, s, ok, wantOK, want, p, s)
			}
			if !ok {
				refused++
				continue
			}
			fitted++
			if got.String() != want.String() {
				t.Fatalf("%s(%s@%d, %s@%d)@%d = %s, want %s (type DECIMAL(%d,%d))",
					op, a, s1, b, s2, s, got, want, p, s)
			}
			// The declared-precision bound, the second half of "no exact
			// carrier at its declared type is an error".
			wantFits := want.CmpAbs(arithPow10(p)) < 0
			if gotFits := DecimalFitsPrecision(got, p); gotFits != wantFits {
				t.Fatalf("DecimalFitsPrecision(%s, %d) = %v, want %v", got, p, gotFits, wantFits)
			}
			if !wantFits {
				precisionOverflows++
			}
		}
	}
	// The corpus has to actually reach each interesting arm, or the test is
	// asserting over an empty set.
	if adjusted == 0 {
		t.Fatal("no case exercised the p > 38 adjustment")
	}
	if fitted == 0 || refused == 0 {
		t.Fatalf("one-sided corpus: %d results fit, %d were refused", fitted, refused)
	}
	if precisionOverflows == 0 {
		t.Fatal("no case landed inside the carrier but past its declared precision")
	}
}

// rawDecimalResultPrecision is item 3's precision BEFORE the >38 adjustment,
// so the differential above can tell when the adjustment fired.
func rawDecimalResultPrecision(op string, p1, s1, p2, s2 int) int {
	switch op {
	case "+", "-":
		return max(s1, s2) + max(p1-s1, p2-s2) + 1
	case "*":
		return p1 + p2 + 1
	case "/":
		s := max(6, s1+p2+1)
		return p1 - s1 + s2 + s
	case "%":
		return min(p1-s1, p2-s2) + max(s1, s2)
	}
	return 0
}

// TestDecimalMulWideMatchesBigRat drives the 256-bit multiply-and-rescale
// path — the one a wide DECIMAL product takes when the 128-bit product
// overflows and the declared scale brings the answer back inside — against
// the exact rational, including the two values only that path can produce.
func TestDecimalMulWideMatchesBigRat(t *testing.T) {
	table := []struct {
		name           string
		a, b           string
		aScale, bScale int
		outScale       int
		want           string
		wantErr        bool
	}{
		{
			name: "a remainder of exactly half rounds away from zero", a: "50", b: "170141183460469231731687303715884105727",
			aScale: 2, bScale: 0, outScale: 0, want: "85070591730234615865843651857942052864",
		},
		{
			name: "the negative twin", a: "-50", b: "170141183460469231731687303715884105727",
			aScale: 2, bScale: 0, outScale: 0, want: "-85070591730234615865843651857942052864",
		},
		{
			name: "a quotient of exactly 2^127 exists only on the negative side", a: "18446744073709551616", b: "-92233720368547758080",
			aScale: 1, bScale: 0, outScale: 0, want: "-170141183460469231731687303715884105728",
		},
		{
			name: "and has no positive counterpart", a: "18446744073709551616", b: "92233720368547758080",
			aScale: 1, bScale: 0, outScale: 0, wantErr: true,
		},
		{
			name: "a quotient still wider than the carrier", a: "170141183460469231731687303715884105727", b: "170141183460469231731687303715884105727",
			aScale: 1, bScale: 0, outScale: 0, wantErr: true,
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			a, b := arithParse(t, tt.a), arithParse(t, tt.b)
			if _, ok := a.Mul(b); ok {
				t.Fatalf("this case must overflow the 128-bit product to reach the wide path")
			}
			got, ok := DecimalMul(a, tt.aScale, b, tt.bScale, tt.outScale)
			if ok == tt.wantErr {
				t.Fatalf("DecimalMul ok = %v, want %v", ok, !tt.wantErr)
			}
			if !tt.wantErr && got.String() != tt.want {
				t.Fatalf("DecimalMul = %s, want %s", got, tt.want)
			}
		})
	}

	rng := rand.New(rand.NewSource(31337))
	wide := 0
	for i := 0; i < 20000; i++ {
		a := arithRandUnscaled(t, rng, rng.Intn(MaxDecimalPrecision)+1)
		b := arithRandUnscaled(t, rng, rng.Intn(MaxDecimalPrecision)+1)
		aScale := rng.Intn(MaxDecimalScale + 1)
		bScale := rng.Intn(MaxDecimalScale + 1)
		// Keep the drop inside the single-word divisor the wide path uses,
		// so this test is aimed at it and not at the big.Int arm.
		drop := rng.Intn(19) + 1
		outScale := aScale + bScale - drop
		if outScale < 0 {
			continue
		}
		if _, ok := a.Mul(b); !ok {
			wide++
		}
		want := arithRoundRatHalfAway(
			new(big.Rat).Mul(arithRat(a, aScale), arithRat(b, bScale)), outScale)
		got, ok := DecimalMul(a, aScale, b, bScale, outScale)
		if ok != arithFits(want) {
			t.Fatalf("DecimalMul(%s@%d, %s@%d)@%d ok = %v, want %v (exact %s)",
				a, aScale, b, bScale, outScale, ok, arithFits(want), want)
		}
		if ok && got.String() != want.String() {
			t.Fatalf("DecimalMul(%s@%d, %s@%d)@%d = %s, want %s",
				a, aScale, b, bScale, outScale, got, want)
		}
	}
	if wide < 1000 {
		t.Fatalf("only %d cases reached the wide path", wide)
	}
}

// TestDecimalDivRoundsOnceNotTwice is the double-rounding case in the flesh:
// 0.1249 at scale 3 is 0.125, and 0.125 rounded again at scale 2 is 0.13,
// where the one correct half-away-from-zero answer at scale 2 is 0.12.
func TestDecimalDivRoundsOnceNotTwice(t *testing.T) {
	cases := []struct {
		name           string
		a, b           int64
		aScale, bScale int
		outScale       int
		want           string
	}{
		{name: "1249/10000 at scale 2", a: 1249, b: 10000, outScale: 2, want: "12"},
		{name: "1250/10000 at scale 2", a: 1250, b: 10000, outScale: 2, want: "13"},
		{name: "-1249/10000 at scale 2", a: -1249, b: 10000, outScale: 2, want: "-12"},
		{name: "14999/100000 at scale 1", a: 14999, b: 100000, outScale: 1, want: "1"},
		{name: "44999/100000 at scale 1", a: 44999, b: 100000, outScale: 1, want: "4"},
		{name: "1/8 at scale 2 is a true half", a: 1, b: 8, outScale: 2, want: "13"},
		{name: "-1/8 at scale 2 is a true half", a: -1, b: 8, outScale: 2, want: "-13"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DecimalDiv(Int128From(tc.a), tc.aScale, Int128From(tc.b), tc.bScale, tc.outScale)
			if !ok {
				t.Fatalf("DecimalDiv(%d, %d) refused", tc.a, tc.b)
			}
			if got.String() != tc.want {
				t.Fatalf("DecimalDiv(%d, %d)@%d = %s, want %s", tc.a, tc.b, tc.outScale, got, tc.want)
			}
			// And the same answer as a single exact rational rounding.
			want := arithRoundRatHalfAway(
				new(big.Rat).SetFrac(big.NewInt(tc.a), big.NewInt(tc.b)), tc.outScale)
			if got.String() != want.String() {
				t.Fatalf("DecimalDiv(%d, %d)@%d = %s, exact rounding says %s", tc.a, tc.b, tc.outScale, got, want)
			}
		})
	}
}

// --- result types ---

func TestDecimalResultTypeFollowsADR0024(t *testing.T) {
	tests := []struct {
		name           string
		op             string
		p1, s1, p2, s2 int
		wantP, wantS   int
	}{
		{name: "add of two money columns", op: "+", p1: 15, s1: 2, p2: 15, s2: 2, wantP: 16, wantS: 2},
		{name: "add across scales", op: "+", p1: 18, s1: 4, p2: 9, s2: 2, wantP: 19, wantS: 4},
		{name: "sub uses the same rule as add", op: "-", p1: 18, s1: 4, p2: 9, s2: 2, wantP: 19, wantS: 4},
		{name: "add of a decimal and an integer", op: "+", p1: 10, s1: 2, p2: 19, s2: 0, wantP: 22, wantS: 2},
		{name: "mul of two money columns", op: "*", p1: 15, s1: 2, p2: 15, s2: 2, wantP: 31, wantS: 4},
		{name: "mul past 38 gives up fraction digits, never integer digits", op: "*", p1: 30, s1: 10, p2: 30, s2: 10, wantP: 38, wantS: 6},
		{name: "div of two money columns", op: "/", p1: 15, s1: 2, p2: 15, s2: 2, wantP: 33, wantS: 18},
		{name: "div keeps at least six fraction digits", op: "/", p1: 10, s1: 0, p2: 4, s2: 0, wantP: 16, wantS: 6},
		{name: "mod takes the narrower integer part", op: "%", p1: 18, s1: 4, p2: 9, s2: 2, wantP: 11, wantS: 4},
		{name: "an unknown operator has no rule", op: "||", p1: 10, s1: 2, p2: 10, s2: 2, wantP: 0, wantS: 0},
		{name: "mod spelled as a function", op: "mod", p1: 18, s1: 4, p2: 9, s2: 2, wantP: 11, wantS: 4},
		{name: "an operand type no column could declare is repaired", op: "+", p1: 0, s1: -3, p2: 5, s2: 2, wantP: 6, wantS: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s := DecimalResultType(tt.op, tt.p1, tt.s1, tt.p2, tt.s2)
			if p != tt.wantP || s != tt.wantS {
				t.Fatalf("DecimalResultType(%q, %d,%d, %d,%d) = (%d,%d), want (%d,%d)",
					tt.op, tt.p1, tt.s1, tt.p2, tt.s2, p, s, tt.wantP, tt.wantS)
			}
		})
	}
}

func TestDecimalResultTypeIsAlwaysDeclarable(t *testing.T) {
	for _, op := range []string{"+", "-", "*", "/", "%"} {
		for p1 := 1; p1 <= MaxDecimalPrecision; p1++ {
			for s1 := 0; s1 <= p1; s1++ {
				for p2 := 1; p2 <= MaxDecimalPrecision; p2 += 7 {
					for s2 := 0; s2 <= p2; s2 += 5 {
						p, s := DecimalResultType(op, p1, s1, p2, s2)
						if p < 1 || p > MaxDecimalPrecision {
							t.Fatalf("%s(%d,%d / %d,%d) gave precision %d", op, p1, s1, p2, s2, p)
						}
						if s < 0 || s > p || s > MaxDecimalScale {
							t.Fatalf("%s(%d,%d / %d,%d) gave scale %d for precision %d", op, p1, s1, p2, s2, s, p)
						}
					}
				}
			}
		}
	}
}

func TestAdjustDecimalPrecisionScale(t *testing.T) {
	tests := []struct {
		name         string
		p, s         int
		wantP, wantS int
	}{
		{name: "inside the carrier is untouched", p: 38, s: 10, wantP: 38, wantS: 10},
		{name: "a narrow type is untouched", p: 9, s: 2, wantP: 9, wantS: 2},
		{name: "past 38 gives up fraction digits", p: 45, s: 40, wantP: 38, wantS: 33},
		{name: "never below six fraction digits", p: 77, s: 20, wantP: 38, wantS: 6},
		{name: "a scale already below six is not raised", p: 45, s: 2, wantP: 38, wantS: 2},
		{name: "an integer type past 38 keeps scale zero", p: 60, s: 0, wantP: 38, wantS: 0},
		{name: "the integer part yields only once the fraction floor binds", p: 40, s: 4, wantP: 38, wantS: 4},
		{name: "a wide integer part keeps every fraction digit it is allowed", p: 40, s: 8, wantP: 38, wantS: 6},
		{name: "a scale wider than its precision is repaired", p: 2, s: 5, wantP: 5, wantS: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s := AdjustDecimalPrecisionScale(tt.p, tt.s)
			if p != tt.wantP || s != tt.wantS {
				t.Fatalf("AdjustDecimalPrecisionScale(%d, %d) = (%d,%d), want (%d,%d)",
					tt.p, tt.s, p, s, tt.wantP, tt.wantS)
			}
		})
	}
}

// TestAdjustDecimalPrecisionScaleGivesUpFractionDigitsFirst states ADR-0024's
// clause as an invariant over the whole input space: the adjustment spends
// FRACTION digits, never widens a scale, and reduces the integer part only
// once the fraction floor min(s,6) binds — the ordering that keeps this from
// becoming #552's range reduction, which shrinks the integer part outright.
func TestAdjustDecimalPrecisionScaleGivesUpFractionDigitsFirst(t *testing.T) {
	for p := 1; p <= 90; p++ {
		for s := 0; s <= p; s++ {
			gotP, gotS := AdjustDecimalPrecisionScale(p, s)
			if gotS > s {
				t.Fatalf("(%d,%d) -> (%d,%d): the scale grew", p, s, gotP, gotS)
			}
			if p <= MaxDecimalPrecision {
				if gotP != p || gotS != s {
					t.Fatalf("(%d,%d) -> (%d,%d): a declarable type must pass through", p, s, gotP, gotS)
				}
				continue
			}
			if gotP != MaxDecimalPrecision {
				t.Fatalf("(%d,%d) -> (%d,%d): precision must land on the carrier's width", p, s, gotP, gotS)
			}
			floor := min(s, 6)
			if want := max(MaxDecimalPrecision-(p-s), floor); gotS != want {
				t.Fatalf("(%d,%d) -> scale %d, want %d", p, s, gotS, want)
			}
			// Integer digits are only given up at the floor: while the
			// fraction still has room above min(s,6), every one is kept.
			if (gotP-gotS) < (p-s) && gotS != floor {
				t.Fatalf("(%d,%d) -> (%d,%d): integer digits given up with the fraction still above its floor %d",
					p, s, gotP, gotS, floor)
			}
		}
	}
}

// TestDecimalPow10 checks the table the kernels index into.
func TestDecimalPow10(t *testing.T) {
	for n := 0; n <= MaxDecimalPrecision; n++ {
		got, ok := DecimalPow10(n)
		if !ok {
			t.Fatalf("DecimalPow10(%d) refused", n)
		}
		if want := arithPow10(n).String(); got.String() != want {
			t.Fatalf("DecimalPow10(%d) = %s, want %s", n, got, want)
		}
	}
	if _, ok := DecimalPow10(MaxDecimalPrecision + 1); ok {
		t.Fatal("10^39 has no Int128 and must not be claimed")
	}
	if _, ok := DecimalPow10(-1); ok {
		t.Fatal("a negative power of ten is not a scale")
	}
}
