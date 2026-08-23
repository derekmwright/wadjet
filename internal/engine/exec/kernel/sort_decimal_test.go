package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// decVec builds a DECIMAL vector from unscaled int64 units at the given scale.
// nulls are physical indices.
func decVec(scale int, units []int64, nulls ...int) *batch.Vector {
	v := batch.NewVectorWithScale(batch.TypeDecimal, len(units), scale)
	for i, u := range units {
		v.DecimalData.Data[i] = batch.Int128From(u)
	}
	for _, i := range nulls {
		v.Nulls.SetNull(i)
	}
	return v
}

// TestResolveSortCompareDecimal is the regression test for #394: every
// resolver returned a comparator that reported every DECIMAL row EQUAL, so
// ORDER BY over a DECIMAL column was a stable no-op and a sort-merge join on
// a DECIMAL key matched every row against every row. The ordering asserted
// here is NUMERIC, which is what PostgreSQL's `numeric` means — and in
// particular 2.0002 sorts BEFORE 10.001, where the formatted-string path this
// engine also had ordered them the other way round.
func TestResolveSortCompareDecimal(t *testing.T) {
	// scale 4: 2.0002 -> 20002, 10.001 -> 100010, 0.0 -> 0, -3.5 -> -35000
	v := decVec(4, []int64{20002, 100010, 0, -35000})

	cases := []struct {
		name string
		a, b int
		want int
	}{
		{"2.0002 < 10.001 numerically, not lexicographically", 0, 1, -1},
		{"10.001 > 2.0002", 1, 0, 1},
		{"equal", 0, 0, 0},
		{"zero < positive", 2, 0, -1},
		{"negative < zero", 3, 2, -1},
		{"negative < positive", 3, 1, -1},
		{"positive > negative", 1, 3, 1},
	}

	for _, resolver := range []struct {
		name string
		fn   func(batch.TypeID) SortCompareKernel
	}{
		{"nulls-first", ResolveSortCompare},
		{"nulls-last", ResolveSortCompareNullsLast},
		{"no-nulls", ResolveSortCompareNoNulls},
	} {
		cmp := resolver.fn(batch.TypeDecimal)
		for _, tc := range cases {
			if got := cmp(v, tc.a, v, tc.b); got != tc.want {
				t.Errorf("%s/%s: compare(%d,%d) = %d, want %d",
					resolver.name, tc.name, tc.a, tc.b, got, tc.want)
			}
		}
	}
}

// TestResolveSortCompareDecimalNulls pins the null placement each resolver
// promises, the property #343 showed is not direction-relative.
func TestResolveSortCompareDecimalNulls(t *testing.T) {
	v := decVec(2, []int64{100, 0}, 1) // row 1 is NULL

	first := ResolveSortCompare(batch.TypeDecimal)
	if got := first(v, 1, v, 0); got != -1 {
		t.Errorf("nulls-first: NULL vs value = %d, want -1", got)
	}
	if got := first(v, 0, v, 1); got != 1 {
		t.Errorf("nulls-first: value vs NULL = %d, want 1", got)
	}
	if got := first(v, 1, v, 1); got != 0 {
		t.Errorf("nulls-first: NULL vs NULL = %d, want 0", got)
	}

	last := ResolveSortCompareNullsLast(batch.TypeDecimal)
	if got := last(v, 1, v, 0); got != 1 {
		t.Errorf("nulls-last: NULL vs value = %d, want 1", got)
	}
	if got := last(v, 0, v, 1); got != -1 {
		t.Errorf("nulls-last: value vs NULL = %d, want -1", got)
	}
	if got := last(v, 1, v, 1); got != 0 {
		t.Errorf("nulls-last: NULL vs NULL = %d, want 0", got)
	}
}

// TestCompareDecimalAtScaleMismatch covers the cross-scale arm, reachable
// where two separately declared DECIMAL columns meet (a sort-merge join key).
// 1.50 at scale 2 and 1.5000 at scale 4 are the same number.
func TestCompareDecimalAtScaleMismatch(t *testing.T) {
	a := decVec(2, []int64{150, 200})    // 1.50, 2.00
	b := decVec(4, []int64{15000, 5000}) // 1.5000, 0.5000

	if got := CompareDecimalAt(a, 0, b, 0); got != 0 {
		t.Errorf("1.50 vs 1.5000 = %d, want 0", got)
	}
	if got := CompareDecimalAt(a, 1, b, 0); got != 1 {
		t.Errorf("2.00 vs 1.5000 = %d, want 1", got)
	}
	if got := CompareDecimalAt(b, 1, a, 0); got != -1 {
		t.Errorf("0.5000 vs 1.50 = %d, want -1", got)
	}
}

// decVec128 builds a DECIMAL vector from raw Int128 units, for the values
// that do not fit in an int64.
func decVec128(scale int, units []batch.Int128) *batch.Vector {
	v := batch.NewVectorWithScale(batch.TypeDecimal, len(units), scale)
	copy(v.DecimalData.Data, units)
	return v
}

// TestCompareDecimalAtCrossScaleIsExact is the regression test for the
// float64 rescale this comparator used to do at unequal scales.
//
// SortMergeJoin uses this comparator for key EQUALITY, so an approximation is
// not a sort-order nicety — it is a spurious JOIN MATCH. 9007199254740993
// (scale 0) and 9007199254740992.0 (scale 1) differ by one unscaled unit at
// the common scale and are DIFFERENT numbers; both round to the same float64,
// which is why the old arm reported them equal.
func TestCompareDecimalAtCrossScaleIsExact(t *testing.T) {
	const twoP53Plus1 = 9007199254740993 // 2^53 + 1: not representable in float64
	a := decVec(0, []int64{twoP53Plus1})
	b := decVec(1, []int64{90071992547409920, 90071992547409930}) // 9007199254740992.0, 9007199254740993.0

	// The float64 rescale the fix replaces: proof the values it compared
	// really are indistinguishable, so this test is about the comparator and
	// not about the constants.
	if af, bf := float64(twoP53Plus1), float64(90071992547409920)/10; af != bf {
		t.Fatalf("the counterexample no longer collides in float64 (%v vs %v)", af, bf)
	}

	if got := CompareDecimalAt(a, 0, b, 0); got != 1 {
		t.Errorf("9007199254740993 vs 9007199254740992.0 = %d, want 1", got)
	}
	if got := CompareDecimalAt(b, 0, a, 0); got != -1 {
		t.Errorf("9007199254740992.0 vs 9007199254740993 = %d, want -1", got)
	}
	if got := CompareDecimalAt(a, 0, b, 1); got != 0 {
		t.Errorf("9007199254740993 vs 9007199254740993.0 = %d, want 0", got)
	}
}

// TestCompareDecimalAtCrossScaleOverflow covers the pair whose rescale does
// not fit in Int128 at all: the answer must still be exact, never a guess.
func TestCompareDecimalAtCrossScaleOverflow(t *testing.T) {
	// 2^126 at scale 0. Rescaling it by 10^20 overflows Int128, so the
	// comparison takes the big.Int path.
	big126 := batch.Int128{Hi: 1 << 62, Lo: 0}
	a := decVec128(0, []batch.Int128{big126, big126.Neg()})
	// scale 20: unscaled 1 is 1e-20, unscaled -1 is -1e-20.
	b := decVec128(20, []batch.Int128{batch.Int128From(1), batch.Int128From(-1)})

	cases := []struct {
		name         string
		av, bv       *batch.Vector
		ai, bi, want int
	}{
		{"huge positive > tiny positive", a, b, 0, 0, 1},
		{"tiny positive < huge positive", b, a, 0, 0, -1},
		{"huge negative < tiny negative", a, b, 1, 1, -1},
		{"tiny negative > huge negative", b, a, 1, 1, 1},
		{"huge positive > tiny negative", a, b, 0, 1, 1},
		{"huge negative < tiny positive", a, b, 1, 0, -1},
	}
	for _, tc := range cases {
		if got := CompareDecimalAt(tc.av, tc.ai, tc.bv, tc.bi); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}

	// And the exact tie the wide path has to see: 2^126 at scale 0 equals
	// 2^126 at scale 0 through the same code when only one side needs no
	// rescale — here a genuinely equal cross-scale pair inside Int128.
	c := decVec128(2, []batch.Int128{{Hi: 1 << 62, Lo: 0}})
	if got := CompareDecimalAt(a, 0, c, 0); got != 1 {
		t.Errorf("2^126 vs 2^126/100 = %d, want 1", got)
	}
}
