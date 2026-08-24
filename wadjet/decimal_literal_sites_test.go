package wadjet

import (
	"context"
	"fmt"
	"testing"
)

// #465: #452 bound the exact literal text for `col op lit`, `col IN (...)` and
// `col BETWEEN a AND b`. Every other site that compares a DECIMAL column to a
// literal still went through the generic boxed path, where the column arrives
// as its rendered TEXT and the literal as the float64 the compiler built for
// arithmetic — a box that has already dropped everything past ~16 significant
// digits.
//
// The three sites, and what they answered before this landed:
//
//	CASE d WHEN lit         0 where PostgreSQL says 1
//	d IS DISTINCT FROM lit  200 where PostgreSQL says 199
//	GREATEST(d, lit) = lit  129 where PostgreSQL says 121 (its own fixture)
//
// Each case below is run against the literal that names a stored value exactly
// AND against one ONE UNIT OF THE LAST PLACE away from it. The pair is the
// whole test: a float64 renders both identically, so an implementation that
// still goes through one cannot answer both.
func TestDecimalLiteralAtEveryComparisonSite(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	c := declitCols[2] // d_wide: 25 significant digits, past int64 entirely
	target := c.text(declitPick(c, 150))
	// One unit of the last place above the target. Nothing holds it.
	ulp := target[:len(target)-1] + "1"
	if target[len(target)-1] != '0' {
		t.Fatalf("expected the target to end in 0 so the ulp is one above: %s", target)
	}

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		// Simple CASE, whose WHEN is an equality against the operand.
		{"case_exact", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE CASE d_wide WHEN %s THEN 1 ELSE 0 END = 1", target), 1},
		{"case_one_ulp_away", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE CASE d_wide WHEN %s THEN 1 ELSE 0 END = 1", ulp), 0},
		// IS [NOT] DISTINCT FROM, which is total over NULL and so counts the
		// NULL rows as distinct.
		{"is_distinct_from_exact", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE d_wide IS DISTINCT FROM %s", target), 199},
		{"is_distinct_from_one_ulp_away", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE d_wide IS DISTINCT FROM %s", ulp), 200},
		{"is_not_distinct_from_exact", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE d_wide IS NOT DISTINCT FROM %s", target), 1},
		// GREATEST / LEAST. PostgreSQL's ignore NULL arguments, so the NULL
		// rows answer with the literal and count.
		{"greatest_exact", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE GREATEST(d_wide, %s) = %s", target, target), 155},
		{"greatest_one_ulp_away", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE GREATEST(d_wide, %s) = %s", ulp, ulp), 155},
		{"least_exact", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE LEAST(d_wide, %s) = %s", target, target), 62},
		{"least_one_ulp_away", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM declit WHERE LEAST(d_wide, %s) = %s", ulp, ulp), 61},
		// A literal past the carrier reaches these sites too, and saturates
		// there as it does everywhere else (#462).
		{"greatest_past_carrier", "SELECT COUNT(*) AS n FROM declit WHERE GREATEST(d_2, 1e39) = 1e39", 200},
		// The narrow column, where every value IS a float64: these must keep
		// answering what they already did.
		{"case_narrow_column", "SELECT COUNT(*) AS n FROM declit WHERE CASE d_2 WHEN 1339815.97 THEN 1 ELSE 0 END = 1", 1},
		{"is_distinct_from_narrow_column", "SELECT COUNT(*) AS n FROM declit WHERE d_2 IS DISTINCT FROM 1339815.97", 199},
		{"greatest_narrow_column", "SELECT COUNT(*) AS n FROM declit WHERE GREATEST(d_2, 1339815.97) = 1339815.97", 136},
		{"least_narrow_column", "SELECT COUNT(*) AS n FROM declit WHERE LEAST(d_2, 1339815.97) = 1339815.97", 77},
	} {
		t.Run(tc.name, func(t *testing.T) {
			declitCheck(t, ctx, db, tc.sql, tc.want)
		})
	}
}

// GREATEST and LEAST now run through pickExtremum, which reads the argument
// EXPRESSIONS rather than only their boxes. The ordering rule is otherwise
// unchanged, and these are the controls that say so: numbers, strings (#333's
// fix, where ToFloat64 ranked every string equal) and NULL skipping.
func TestGreatestLeastKeepTheirOrdinaryOrdering(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)
	for _, tc := range []struct {
		sql  string
		want any
	}{
		{"SELECT GREATEST(3, 7, 5) AS n", int64(7)},
		{"SELECT LEAST(3, 7, 5) AS n", int64(3)},
		{"SELECT GREATEST(-2.5, -7.5) AS n", -2.5},
		{"SELECT LEAST('pear', 'apple', 'fig') AS n", "apple"},
		{"SELECT GREATEST('pear', 'apple', 'fig') AS n", "pear"},
		{"SELECT GREATEST(NULL, 4, NULL) AS n", int64(4)},
		{"SELECT LEAST(NULL, 4) AS n", int64(4)},
		{"SELECT GREATEST(NULL, NULL) AS n", nil},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			res, err := tmRun(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			got := res.Rows[0][res.Columns[0]]
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("%s = %#v, want %#v", tc.sql, got, tc.want)
			}
		})
	}
}
