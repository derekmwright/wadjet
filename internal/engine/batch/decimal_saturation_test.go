package batch

import (
	"math/big"
	"testing"
)

// #462: a literal too wide for Int128 at the column's scale used to be
// narrowed by two's-complement WRAPAROUND, which put it back inside the
// ordinary range as a perfectly plausible number — often of the opposite
// sign. `WHERE d < 1e39`, true of every row a DECIMAL(9,2) can hold, selected
// none of them. The contract is saturation: a magnitude past the carrier
// orders above (or below) every value the column can hold.

func TestDecimalTextAtSaturatesOutsideInt128(t *testing.T) {
	for _, tc := range []struct {
		text  string
		scale int
		sat   int
	}{
		// 39 digits is the widest magnitude Int128 can hold, and only some of
		// them: 2^127-1 is 170141183460469231731687303715884105727.
		{"170141183460469231731687303715884105727", 0, 0},
		{"170141183460469231731687303715884105728", 0, 1},
		{"-170141183460469231731687303715884105728", 0, 0},
		{"-170141183460469231731687303715884105729", 0, -1},
		{"1000000000000000000000000000000000000000", 0, 1}, // 1e39
		{"-1000000000000000000000000000000000000000", 0, -1},
		// The SCALE is what pushes an ordinary-looking literal out of range:
		// 10^30 needs 40 unscaled digits at scale 10.
		{"1000000000000000000000000000000", 10, 1},
		{"1000000000000000000000000000000", 0, 0},
		// In range at every scale.
		{"1.50", 2, 0},
		{"0", 38, 0},
		{"-0.000000001", 2, 0},
	} {
		got, ok := DecimalTextAt(tc.text, tc.scale)
		if !ok {
			t.Fatalf("DecimalTextAt(%q, %d) rejected a number", tc.text, tc.scale)
		}
		if got.Sat != tc.sat {
			t.Errorf("DecimalTextAt(%q, %d).Sat = %d, want %d",
				tc.text, tc.scale, got.Sat, tc.sat)
		}
	}
}

// TestScaledDecimalOrderMatchesExactRationals is the property the wraparound
// broke: for every stored value and every constant, the order the comparison
// reports is the order of the two RATIONAL numbers. Agreement with an exact
// oracle is stronger than transitivity on its own — a comparator that agrees
// with a total order IS one — and it is checked at the boundary, where the
// constants sit one unit either side of the carrier's ends.
func TestScaledDecimalOrderMatchesExactRationals(t *testing.T) {
	const scale = 2
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)

	cells := []Int128{
		Int128Min, Int128Min.Add(Int128From(1)),
		int128OrDie(t, "-100000000000000000000"),
		Int128From(-150), Int128From(-1), Int128From(0), Int128From(1), Int128From(150),
		int128OrDie(t, "100000000000000000000"),
		Int128Max.Sub(Int128From(1)), Int128Max,
	}
	// Constants spanning: in range, finer than the scale, and both sides of
	// the carrier's ends.
	consts := []string{
		"-1e400", "-1000000000000000000000000000000000000000",
		"-1701411834604692317316873037158841057.29",
		"-1701411834604692317316873037158841057.28",
		"-1701411834604692317316873037158841057.27",
		"-1000000000000000000.005", "-1.5", "-0.001", "0", "0.001", "1.5",
		"1.505", "1000000000000000000.005",
		"1701411834604692317316873037158841057.26",
		"1701411834604692317316873037158841057.27",
		"1701411834604692317316873037158841057.28",
		"1000000000000000000000000000000000000000", "1e400",
	}

	for _, text := range consts {
		sd, ok := DecimalTextAt(text, scale)
		if !ok {
			// Exponent form is not read by this parser yet (#463); the plain
			// spellings on either side of it carry the boundary.
			continue
		}
		want := new(big.Rat)
		if _, ok := want.SetString(text); !ok {
			t.Fatalf("%q is not a rational", text)
		}
		for _, cell := range cells {
			cellRat := new(big.Rat).SetFrac(cell.BigInt(), den)
			if got, exp := sd.Order(cell), cellRat.Cmp(want); got != exp {
				t.Errorf("ScaledDecimal(%q at scale %d).Order(%s) = %d, want %d",
					text, scale, cell.String(), got, exp)
			}
		}
	}
}

// TestScaledDecimalOrderIsTransitiveAtTheBoundary walks the mixed order —
// stored values ordered against each other by Int128, stored values against
// constants by ScaledDecimal.Order, constants against each other exactly —
// and asserts transitivity over every triple. A wrapped constant broke it by
// reporting itself both above and below the same stored value depending on
// which stored value it was asked about.
func TestScaledDecimalOrderIsTransitiveAtTheBoundary(t *testing.T) {
	const scale = 4
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)

	type item struct {
		name string
		rat  *big.Rat
		cell *Int128       // set for a stored value
		sd   ScaledDecimal // set for a constant
	}
	var items []item
	for _, c := range []Int128{
		Int128Min, int128OrDie(t, "-99999999999999999999"), Int128From(-1),
		Int128From(0), Int128From(1), int128OrDie(t, "99999999999999999999"), Int128Max,
	} {
		cell := c
		items = append(items, item{
			name: "cell " + c.String(),
			rat:  new(big.Rat).SetFrac(c.BigInt(), den),
			cell: &cell,
		})
	}
	for _, text := range []string{
		"-99999999999999999999999999999999999999999", "-1234567.00005",
		"-0.00001", "0", "0.00001", "1234567.00005",
		"17014118346046923173168730371588.4105727",
		"17014118346046923173168730371588.4105728",
		"99999999999999999999999999999999999999999",
	} {
		sd, ok := DecimalTextAt(text, scale)
		if !ok {
			t.Fatalf("DecimalTextAt(%q) rejected a number", text)
		}
		r, ok := new(big.Rat).SetString(text)
		if !ok {
			t.Fatalf("%q is not a rational", text)
		}
		items = append(items, item{name: "const " + text, rat: r, sd: sd})
	}

	cmp := func(a, b item) int {
		switch {
		case a.cell != nil && b.cell != nil:
			return a.cell.Cmp(*b.cell)
		case a.cell != nil:
			return b.sd.Order(*a.cell)
		case b.cell != nil:
			return -a.sd.Order(*b.cell)
		}
		return a.rat.Cmp(b.rat)
	}

	for _, a := range items {
		for _, b := range items {
			if cmp(a, b) != -cmp(b, a) {
				t.Fatalf("antisymmetry: %s vs %s = %d, reversed %d",
					a.name, b.name, cmp(a, b), cmp(b, a))
			}
			if want := a.rat.Cmp(b.rat); cmp(a, b) != want {
				t.Fatalf("%s vs %s = %d, exact rationals say %d",
					a.name, b.name, cmp(a, b), want)
			}
			for _, c := range items {
				if cmp(a, b) <= 0 && cmp(b, c) <= 0 && cmp(a, c) > 0 {
					t.Fatalf("transitivity: %s <= %s <= %s but %s > %s",
						a.name, b.name, c.name, a.name, c.name)
				}
			}
		}
	}
}

// TestInt128FromBigSaturates pins the narrowing itself: the conversion every
// wide parse ends in must clamp, never wrap.
func TestInt128FromBigSaturates(t *testing.T) {
	for _, tc := range []struct {
		text string
		want Int128
	}{
		{"170141183460469231731687303715884105727", Int128Max},
		{"170141183460469231731687303715884105728", Int128Max},
		{"999999999999999999999999999999999999999999", Int128Max},
		{"-170141183460469231731687303715884105728", Int128Min},
		{"-170141183460469231731687303715884105729", Int128Min},
		{"-999999999999999999999999999999999999999999", Int128Min},
	} {
		b, ok := new(big.Int).SetString(tc.text, 10)
		if !ok {
			t.Fatalf("%q is not an integer", tc.text)
		}
		if got := int128FromBig(b); got != tc.want {
			t.Errorf("int128FromBig(%s) = %s, want %s", tc.text, got.String(), tc.want.String())
		}
	}
}

func int128OrDie(t *testing.T, digits string) Int128 {
	t.Helper()
	b, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		t.Fatalf("%q is not an integer", digits)
	}
	return int128FromBig(b)
}
