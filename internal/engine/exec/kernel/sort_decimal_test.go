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
