package kernel

import (
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestDecimalLiteralOrdersExactlyPastFloat64 is #452 at the unit: the literal
// is compared as the number it names, not as the nearest double.
//
// The values here need 25 significant digits, which is past a float64 by ten
// and past an int64's unscaled range entirely. Rounding either side collapses
// the three rows into one number, so an implementation that goes through a
// double cannot pass any row of this table.
func TestDecimalLiteralOrdersExactlyPastFloat64(t *testing.T) {
	const scale = 10
	// One unit of the last decimal place apart. A float64 renders all three
	// as 493827160549382.6875.
	below := "493827160549382.7160549349"
	exact := "493827160549382.7160549350"
	above := "493827160549382.7160549351"
	vec := decimalVec(t, scale, below, exact, above, "-888888888988888.8888988830", "0.0000000000")

	for _, tc := range []struct {
		lit  string
		want []int // Order() for each row, in row order
	}{
		{exact, []int{-1, 0, 1, -1, -1}},
		{below, []int{0, 1, 1, -1, -1}},
		{above, []int{-1, -1, 0, -1, -1}},
		{"-888888888988888.8888988830", []int{1, 1, 1, 0, 1}},
		{"0", []int{1, 1, 1, -1, 0}},
		// More fractional digits than the column holds: equal to nothing,
		// and strictly between the value it extends and the next one up.
		{exact + "5", []int{-1, -1, 1, -1, -1}},
		// Trailing zeros do not change the number.
		{exact + "000", []int{-1, 0, 1, -1, -1}},
	} {
		lit := NewDecimalLiteral(tc.lit)
		for row, want := range tc.want {
			if got := lit.Order(vec, row); got != want {
				t.Errorf("NewDecimalLiteral(%q).Order(row %d) = %d, want %d",
					tc.lit, row, got, want)
			}
		}
	}
}

// TestDecimalLiteralComparesEveryOperator checks that Compare reads the order
// the way each operator does, including the one a truncated literal gets
// wrong: `> x.xxxx5` must EXCLUDE the row holding exactly x.xxxx.
func TestDecimalLiteralComparesEveryOperator(t *testing.T) {
	vec := decimalVec(t, 4, "2499.5074")
	for _, tc := range []struct {
		lit  string
		op   CompareOp
		want bool
	}{
		{"2499.5074", OpEq, true},
		{"2499.5074", OpNe, false},
		{"2499.5074", OpLt, false},
		{"2499.5074", OpLe, true},
		{"2499.5074", OpGt, false},
		{"2499.5074", OpGe, true},
		{"2499.5074494849528", OpEq, false},
		{"2499.5074494849528", OpNe, true},
		{"2499.5074494849528", OpLt, true},
		{"2499.5074494849528", OpGt, false},
		{"2499.5074494849528", OpGe, false},
	} {
		if got := NewDecimalLiteral(tc.lit).Compare(vec, 0, tc.op); got != tc.want {
			t.Errorf("2499.5074 %v %s = %v, want %v", tc.op, tc.lit, got, tc.want)
		}
	}
}

// TestDecimalLiteralIsKeyedByScale: one literal can meet two columns, so the
// cached resolution has to carry the scale it was resolved at.
func TestDecimalLiteralIsKeyedByScale(t *testing.T) {
	lit := NewDecimalLiteral("1.50")
	v2 := decimalVec(t, 2, "1.50", "1.51")
	v4 := decimalVec(t, 4, "1.5000", "1.5001")
	for i := 0; i < 3; i++ { // repeat: a cache that only re-keys forward fails
		if got := lit.Order(v2, 0); got != 0 {
			t.Fatalf("scale 2 pass %d: Order = %d, want 0", i, got)
		}
		if got := lit.Order(v4, 0); got != 0 {
			t.Fatalf("scale 4 pass %d: Order = %d, want 0", i, got)
		}
		if got := lit.Order(v2, 1); got != 1 {
			t.Fatalf("scale 2 pass %d: Order(1.51) = %d, want 1", i, got)
		}
	}
}

// TestDecimalLiteralIsConcurrencySafe: one literal is shared by every worker
// evaluating the predicate, which is the shape a plain memo field turns into a
// data race. Run with -race.
func TestDecimalLiteralIsConcurrencySafe(t *testing.T) {
	lit := NewDecimalLiteral("493827160549382.7160549350")
	vecs := []*batch.Vector{
		decimalVec(t, 10, "493827160549382.7160549350"),
		decimalVec(t, 4, "493827160549382.7161"),
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			vec := vecs[w%2]
			want := 0
			if w%2 == 1 {
				want = 1 // 493827160549382.7161 > the literal
			}
			for i := 0; i < 500; i++ {
				if got := lit.Order(vec, 0); got != want {
					t.Errorf("worker %d: Order = %d, want %d", w, got, want)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestDecimalLiteralExponentFormIsStillRounded pins a KNOWN LIMIT, not a
// property: an exponent-form literal is expanded through a float64 by
// normalizeDecimalText (compare.go), so it arrives already rounded and the
// exactness above does not reach it. Plain decimal text — every literal the
// SQL parser sees written out, and everything #452 was about — is exact.
//
// The fix is one hunk in normalizeDecimalText: expand the exponent by moving
// the decimal POINT across the digit string instead of round-tripping through
// strconv.ParseFloat. It belongs there rather than here because
// compareFilterDecimal shares that function, and one comparison rule per
// predicate is the property that keeps the vectorized and row paths from
// disagreeing (#394). When that lands, this test fails and is deleted.
func TestDecimalLiteralExponentFormIsStillRounded(t *testing.T) {
	vec := decimalVec(t, 10, "493827160549382.7160549350")
	plain := NewDecimalLiteral("493827160549382.7160549350")
	if got := plain.Order(vec, 0); got != 0 {
		t.Fatalf("plain text: Order = %d, want 0", got)
	}
	exp := NewDecimalLiteral("4.938271605493827160549350e14")
	if got := exp.Order(vec, 0); got == 0 {
		t.Fatal("exponent form is exact now — normalizeDecimalText no longer " +
			"round-trips through float64, so delete this pin")
	}
}

// TestDecimalLiteralSaturatesPastTheCarrier is #462 at the unit. A literal
// wider than Int128 at the column's scale used to be narrowed by
// two's-complement wraparound and landed back inside the ordinary range: the
// issue's own probe, NewDecimalLiteral("1e39").Order(a vector holding 1.50 at
// scale 2), answered +1 — "the column's 1.50 is GREATER than 1e39".
func TestDecimalLiteralSaturatesPastTheCarrier(t *testing.T) {
	vec := decimalVec(t, 2, "1.50", "-1.50", "0.00")
	for _, tc := range []struct {
		lit  string
		want int // Order() for every row: the literal is off the scale
	}{
		{"1000000000000000000000000000000000000000", -1},  // 1e39
		{"-1000000000000000000000000000000000000000", 1},  // -1e39
		{"99999999999999999999999999999999999999999", -1}, // 1e41 - 1
		{"-99999999999999999999999999999999999999999", 1},
	} {
		lit := NewDecimalLiteral(tc.lit)
		for row := 0; row < 3; row++ {
			if got := lit.Order(vec, row); got != tc.want {
				t.Errorf("NewDecimalLiteral(%q).Order(row %d) = %d, want %d",
					tc.lit, row, got, tc.want)
			}
		}
		// Every operator reads that one order, so the whole family follows.
		if lit.Compare(vec, 0, OpEq) {
			t.Errorf("%s: a value past the carrier equals a stored 1.50", tc.lit)
		}
		wantGt := tc.want > 0
		if got := lit.Compare(vec, 0, OpGt); got != wantGt {
			t.Errorf("%s: 1.50 > literal = %v, want %v", tc.lit, got, wantGt)
		}
	}
}

// TestDecimalLiteralSaturationIsScaleDependent: an in-range literal at one
// scale is out of range at another, because the carrier holds the UNSCALED
// integer. 10^30 fits Int128 at scale 0 and needs 40 digits at scale 10.
func TestDecimalLiteralSaturationIsScaleDependent(t *testing.T) {
	const wide = "1000000000000000000000000000000" // 10^30
	lit := NewDecimalLiteral(wide)
	if got := lit.Order(decimalVec(t, 0, "1"), 0); got != -1 {
		t.Errorf("scale 0: Order = %d, want -1", got)
	}
	if got := lit.Order(decimalVec(t, 10, "1.0000000000"), 0); got != -1 {
		t.Errorf("scale 10 (saturating): Order = %d, want -1", got)
	}
}
