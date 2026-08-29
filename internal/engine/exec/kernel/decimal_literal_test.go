package kernel

import (
	"math"
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

// TestDecimalLiteralExponentFormIsExact replaces the pin that stood here for
// the opposite claim. An exponent-form literal used to be expanded through
// strconv.ParseFloat before any digit was scaled, so it arrived already
// rounded; the exponent is now folded into the scaling itself, on the digit
// string, and every spelling of the same number resolves to the same value
// (#463).
func TestDecimalLiteralExponentFormIsExact(t *testing.T) {
	vec := decimalVec(t, 10, "493827160549382.7160549350")
	for _, text := range []string{
		"493827160549382.7160549350",
		"4.938271605493827160549350e14",
		"4938271605493827160549350e-10",
		"49382716054938271605493.50E-8",
		"+4.93827160549382716054935E+14",
	} {
		if got := NewDecimalLiteral(text).Order(vec, 0); got != 0 {
			t.Errorf("NewDecimalLiteral(%q).Order = %d, want 0 (the same number)", text, got)
		}
	}
	// One unit of the last place either side, in exponent form: a float64
	// round trip renders all three identically.
	if got := NewDecimalLiteral("4.938271605493827160549351e14").Order(vec, 0); got != -1 {
		t.Errorf("one ulp above: Order = %d, want -1", got)
	}
	if got := NewDecimalLiteral("4.938271605493827160549349e14").Order(vec, 0); got != 1 {
		t.Errorf("one ulp below: Order = %d, want 1", got)
	}
}

// TestDecimalLiteralOutOfFloatRangeIsOrdered is #463's headline: a literal
// past float64's range used to be unreadable — ParseFloat reported ErrRange,
// the expansion gave up, and the parser that received the untouched "1e400"
// returned ZERO, so `WHERE d = 1e400` matched the row holding 0.00. It is an
// ordinary number with a large exponent; it saturates and orders above
// everything.
func TestDecimalLiteralOutOfFloatRangeIsOrdered(t *testing.T) {
	vec := decimalVec(t, 2, "0.00", "1.50", "-1.50")
	for _, tc := range []struct {
		lit  string
		want int
	}{
		{"1e400", -1},
		{"-1e400", 1},
		{"1E400", -1},
		// Not zero, and not rounded to zero: a positive value below one unit
		// of the column's scale sits strictly ABOVE the stored 0.00.
		{"1e-400", -1},
		{"-1e-400", 1},
	} {
		lit := NewDecimalLiteral(tc.lit)
		if !lit.Numeric() {
			t.Fatalf("%q is a number", tc.lit)
		}
		if got := lit.Order(vec, 0); got != tc.want {
			t.Errorf("NewDecimalLiteral(%q).Order(0.00) = %d, want %d", tc.lit, got, tc.want)
		}
	}
	// Nothing equals a value the scale cannot hold, and the residual is what
	// keeps `< 1e-400` from admitting the row holding 0.00 that `<= 0` does.
	tiny := NewDecimalLiteral("1e-400")
	if tiny.Compare(vec, 0, OpEq) {
		t.Error("1e-400 equals the stored 0.00")
	}
	if !tiny.Compare(vec, 0, OpLt) {
		t.Error("the stored 0.00 is not below 1e-400")
	}
}

// TestDecimalLiteralRejectsWhatIsNotANumber: the constant reaches the
// comparison so the comparison can REFUSE it. Reading it as zero is what made
// `WHERE d = 'abc'` answer the rows holding zero.
func TestDecimalLiteralRejectsWhatIsNotANumber(t *testing.T) {
	for _, text := range []string{
		"abc", "", "  ", "1.2.3", "1e", "1eX", "--1", "0x10", "1,000",
		// PostgreSQL refuses a SIGNED NaN and every partial spelling of the
		// infinities, so the widening of #534 must not reach them either.
		"+NaN", "-NaN", "NaN0", "Infin", "infinit", "- inf",
	} {
		if NewDecimalLiteral(text).Numeric() {
			t.Errorf("NewDecimalLiteral(%q).Numeric() = true", text)
		}
		if _, ok := DecimalConstText(text); ok {
			t.Errorf("DecimalConstText(%q) accepted a non-number", text)
		}
	}
	for _, text := range []string{
		"0", "-0", "+1", ".5", "5.", "1e400", "1E-400", " 12.75 ",
		// #534: NaN and ±Infinity are values PostgreSQL's numeric HAS, so a
		// DECIMAL column can be compared against them even though it can hold
		// none of them (ADR-0024 item 6). Numeric() answers "is this a literal
		// this column can be compared against", and after #534 that is yes.
		"NaN", "nan", "Infinity", "inf", "+inf", "-Infinity", "-inf", " NaN ",
	} {
		if !NewDecimalLiteral(text).Numeric() {
			t.Errorf("NewDecimalLiteral(%q).Numeric() = false", text)
		}
		if _, ok := DecimalConstText(text); !ok {
			t.Errorf("DecimalConstText(%q) refused a comparison literal", text)
		}
	}
	// FiniteDecimalText is the narrow reader the unary-minus fold keeps: it
	// answers "is this a finite number", and the three specials are not.
	for _, text := range []string{"NaN", "Infinity", "-inf"} {
		if FiniteDecimalText(text) {
			t.Errorf("FiniteDecimalText(%q) = true", text)
		}
	}
	for _, text := range []string{"0", "1e400", " 12.75 "} {
		if !FiniteDecimalText(text) {
			t.Errorf("FiniteDecimalText(%q) = false", text)
		}
	}
	// Constants that are not text at all.
	for _, v := range []any{true, math.NaN(), math.Inf(1), math.Inf(-1), []byte("abc")} {
		if _, ok := DecimalConstText(v); ok {
			t.Errorf("DecimalConstText(%#v) accepted a non-number", v)
		}
	}
	if _, ok := DecimalConstText([]byte("12.75")); !ok {
		t.Error("DecimalConstText refused a numeric []byte parameter")
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
