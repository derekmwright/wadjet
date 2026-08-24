package wadjet

import (
	"context"
	"testing"
)

// #476: a DECIMAL column compared against a column of another type routed
// through ColColFilter's row-at-a-time fallback, where the DECIMAL arrives as
// its RENDERED TEXT and the integer as an int64. compare() had no numeric
// reading of that pair and fell all the way through to a LEXICOGRAPHIC string
// comparison — "9" sorts above "10" — so the ORDERING operators were wrong
// while `=` and `<>` were right, which is why it took an oracle to see.
//
// Every expectation below is PostgreSQL's on the same fixture, verified
// against live postgres:17-alpine. `k` is the row index and every DECIMAL
// column is a large multiple of it, so the answers are also checkable by hand.
func TestDecimalAgainstOtherColumnTypesOrdersNumerically(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		// d_2 runs -4278767.13 .. 4278767.13 in steps of 43219.87 while k runs
		// 0..199, so the two cross exactly once and neither operator is
		// vacuous — the shape that tells a numeric comparison from a
		// lexicographic one.
		{"k >= d_2", 95},
		{"k > d_2", 95},
		{"k < d_2", 93},
		{"k <= d_2", 93},
		{"k = d_2", 0},
		{"k <> d_2", 188},
		// The same pair with the operands the other way round.
		{"d_2 >= k", 93},
		{"d_2 < k", 95},
		// Scale 4 and the 25-digit wide column, so the reading is not pinned
		// to one scale or to values an int64 could hold.
		{"k >= d_4", 96},
		{"k >= d_wide", 93},
		{"k < d_wide", 91},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			// The vectorized col-col filter and the row-at-a-time evaluator
			// must answer alike: a CASE wrapper is not vectorizable, so the
			// second form is evaluated row by row.
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE "+tc.pred, tc.want)
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE CASE WHEN "+tc.pred+
					" THEN 1 ELSE 0 END = 1", tc.want)
		})
	}
}

// #477: two DECIMAL columns share a TypeID, so ColColFilter skipped the
// mixed-type row fallback and looked for a vectorized kernel that
// ResolveColColFilterKernel had no arm to give it. Every operator FAILED the
// query outright. The row-at-a-time path did not fail — it answered, from the
// two RENDERED TEXTS, lexicographically, where "10.001" sorts below "2.0002".
//
// Every expectation is PostgreSQL's on the same fixture, verified against live
// postgres:17-alpine.
func TestDecimalColumnAgainstDecimalColumn(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		// Scale 2 against scale 4: the two columns cross, so no operator is
		// vacuous and equality is decided across scales.
		{"d_2 = d_4", 1},
		{"d_2 <> d_4", 179},
		{"d_2 < d_4", 89},
		{"d_2 <= d_4", 90},
		{"d_2 > d_4", 90},
		{"d_2 >= d_4", 91},
		// Scale 4 against the 25-digit scale-10 column, whose rescale is the
		// arm a float64 comparison could not carry.
		{"d_4 < d_wide", 87},
		{"d_4 >= d_wide", 89},
		{"d_2 < d_wide", 85},
		// A column against itself: every non-NULL row equals itself, no row is
		// distinct from itself, and the NULL rows appear in neither.
		{"d_2 = d_2", 188},
		{"d_2 <> d_2", 0},
		{"d_wide >= d_wide", 184},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE "+tc.pred, tc.want)
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE CASE WHEN "+tc.pred+
					" THEN 1 ELSE 0 END = 1", tc.want)
		})
	}
}
