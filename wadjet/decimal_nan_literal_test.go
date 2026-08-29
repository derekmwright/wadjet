package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #534 / ADR-0024 item 6: a DECIMAL column compared against 'NaN',
// 'Infinity' or '-Infinity'.
//
// PostgreSQL's `numeric` HAS all three — NaN greater than every non-NaN and
// equal only to itself, and since PostgreSQL 14 ±Infinity — so `WHERE d =
// 'NaN'` is a question it answers (with zero rows, for a column holding no
// NaN). Wadjet's carrier is a finite Int128 at a fixed scale with no bit
// pattern for any of them, so it REFUSED the query outright with 22P02
// "invalid input syntax for type numeric", which is PostgreSQL's answer for
// 'abc' and not for these.
//
// The fix is the mechanism the carrier already had: ScaledDecimal.Sat, the
// flag a finite literal too wide for Int128 already sets so that it orders
// past every value the column can hold (#462). NaN and Infinity saturate ABOVE
// and -Infinity BELOW, which is exactly where PostgreSQL's total order puts
// them relative to every value a DECIMAL column can hold. PostgreSQL's NaN >
// Infinity is not observable over a column that can hold neither.
//
// Every count below is live postgres:17-alpine (17.11) on the identical
// fixture, rebuilt there as numeric(9,2)/(18,4)/(38,10).

// TestDecimalComparedAgainstNaNAndTheInfinities is the per-shape table, run
// with the row-group prune both on and off (declitCheck): the prune converts a
// literal through kernel.StatsDomainValue and withholds for these three, so
// the two arms answering alike is what says the prune cannot delete a row
// group the filter would have kept.
func TestDecimalComparedAgainstNaNAndTheInfinities(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	// d_2 is NULL every 17th row of 200; d_4 every 23rd; d_wide every 13th.
	const (
		d2NonNull   = 188
		d4NonNull   = 191
		wideNonNull = 184
		allRows     = 200
	)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		// NaN is above every value the column holds, so it equals nothing and
		// every non-NULL row is below it.
		{"d_2 = 'NaN'", 0},
		{"d_2 <> 'NaN'", d2NonNull},
		{"d_2 < 'NaN'", d2NonNull},
		{"d_2 <= 'NaN'", d2NonNull},
		{"d_2 > 'NaN'", 0},
		{"d_2 >= 'NaN'", 0},
		// Infinity sits at the same end for a column that can hold neither.
		{"d_2 = 'Infinity'", 0},
		{"d_2 <= 'Infinity'", d2NonNull},
		{"d_2 < 'Infinity'", d2NonNull},
		{"d_2 > 'Infinity'", 0},
		// -Infinity is below every value.
		{"d_2 = '-Infinity'", 0},
		{"d_2 > '-Infinity'", d2NonNull},
		{"d_2 >= '-Infinity'", d2NonNull},
		{"d_2 < '-Infinity'", 0},
		{"d_2 BETWEEN '-Infinity' AND 'Infinity'", d2NonNull},
		{"d_2 BETWEEN 'Infinity' AND '-Infinity'", 0},
		// An IN list mixing a special with a real value keeps the real one:
		// 43219.87 is row 101's d_2. NOT IN over a special alone excludes
		// nothing, because nothing equals it.
		{"d_2 IN ('NaN', 43219.87)", 1},
		{"d_2 IN ('NaN')", 0},
		{"d_2 NOT IN ('NaN')", d2NonNull},
		{"d_2 NOT IN ('NaN', 43219.87)", d2NonNull - 1},
		{"d_2 IN ('-Infinity', 'Infinity')", 0},

		// Every spelling PostgreSQL's numeric input accepts, on the column
		// whose values a float64 cannot hold.
		{"d_wide < 'nan'", wideNonNull},
		{"d_wide < 'NAN'", wideNonNull},
		{"d_wide < '  NaN  '", wideNonNull},
		{"d_wide < 'inf'", wideNonNull},
		{"d_wide < 'Inf'", wideNonNull},
		{"d_wide < 'INFINITY'", wideNonNull},
		{"d_wide < '+Infinity'", wideNonNull},
		{"d_wide < '+inf'", wideNonNull},
		{"d_wide > '-inf'", wideNonNull},
		{"d_wide > '-Inf'", wideNonNull},
		{"d_wide > '-infinity'", wideNonNull},
		{"d_4 > '-Infinity'", d4NonNull},

		// The BOXED comparison sites (#465/#506), which reach the column as
		// its rendered TEXT and the literal as its own — the path that would
		// otherwise compare "12.75" against "NaN" lexicographically.
		{"CASE d_2 WHEN 'NaN' THEN 1 ELSE 0 END = 1", 0},
		{"CASE d_2 WHEN '-Infinity' THEN 1 ELSE 0 END = 1", 0},
		{"d_2 IS DISTINCT FROM 'NaN'", allRows},
		{"d_2 IS NOT DISTINCT FROM 'NaN'", 0},
		{"GREATEST(d_2, 'NaN') = 'NaN'", allRows},
		{"GREATEST(d_2, 'Infinity') = 'Infinity'", allRows},
		{"LEAST(d_2, '-Infinity') = '-Infinity'", allRows},

		// Through the row-at-a-time evaluator, which a CASE forces on both
		// operands.
		{"CASE WHEN d_2 < 'NaN' THEN 1 ELSE 0 END = 1", d2NonNull},
		{"CASE WHEN d_2 > '-Infinity' THEN 1 ELSE 0 END = 1", d2NonNull},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db, "SELECT COUNT(*) AS n FROM declit WHERE "+tc.pred, tc.want)
		})
	}
}

// TestDecimalNaNLiteralAnswersOverAnEmptyTable is the plan-time half. #517
// moved the non-numeric refusal to the binder, which is where #534 was found:
// the refusal reached a query over an EMPTY table, where before it silently
// answered zero rows. So the binder's accept-set has to widen with the
// runtime's, or a query PostgreSQL answers would be refused before a row
// existed — and the two predicates are one function
// (`expr.IsNumericLiteralText`) precisely so they cannot disagree.
func TestDecimalNaNLiteralAnswersOverAnEmptyTable(t *testing.T) {
	ctx := context.Background()
	empty := declitEmptyOpen(t)

	for _, pred := range []string{
		"d_2 = 'NaN'",
		"d_2 < 'NaN'",
		"d_2 <= 'Infinity'",
		"d_2 > '-Infinity'",
		"d_2 IS DISTINCT FROM 'NaN'",
		"CASE d_2 WHEN 'NaN' THEN 1 ELSE 0 END = 1",
		"GREATEST(d_2, 'NaN') = 'NaN'",
		"LEAST(d_2, '-inf') = '-inf'",
		"(d_2) = 'NaN'",
		"d_2 IN ('NaN', 1.5)",
		"d_2 BETWEEN '-Infinity' AND 'Infinity'",
	} {
		t.Run(pred, func(t *testing.T) {
			sql := "SELECT COUNT(*) AS n FROM declit WHERE " + pred
			res, err := tmRun(ctx, empty, sql)
			if err != nil {
				t.Fatalf("%s was refused: %v", sql, err)
			}
			if got, _ := tmAsInt64(res.Rows[0][res.Columns[0]]); got != 0 {
				t.Errorf("%s over an empty table = %d, want 0", sql, got)
			}
		})
	}

	// A conjunct no row survives is the other shape #517 closed: the refusal
	// must not come back for these literals there either.
	for _, sql := range []string{
		"SELECT COUNT(*) AS n FROM declit WHERE k > 100000 AND d_2 = 'NaN'",
		"SELECT COUNT(*) AS n FROM declit WHERE 1 = 0 AND d_2 = 'NaN'",
	} {
		if _, err := tmRun(ctx, declitOpen(t), sql); err != nil {
			t.Errorf("%s was refused: %v", sql, err)
		}
	}
}

// TestNonNumericDecimalLiteralIsStillRefused is the boundary: the accept-set
// widened by exactly the NaN/±Infinity spellings PostgreSQL's numeric input
// takes, and by nothing else. A partial spelling, a signed NaN and ordinary
// garbage are all still 22P02, at the binder and at both comparison paths.
func TestNonNumericDecimalLiteralIsStillRefused(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)
	empty := declitEmptyOpen(t)

	for _, lit := range []string{
		"abc",
		// PostgreSQL refuses a SIGNED NaN outright, and every prefix of the
		// infinities that is not 'inf' or 'infinity'.
		"+NaN", "-NaN", "NaN0", "Infin", "infinit", "infinityy", "- inf",
	} {
		for _, pred := range []string{
			"d_2 = '%s'",
			"d_2 IS DISTINCT FROM '%s'",
			"CASE d_2 WHEN '%s' THEN 1 ELSE 0 END = 1",
			"GREATEST(d_2, '%s') = '%s'",
		} {
			sql := "SELECT COUNT(*) AS n FROM declit WHERE " +
				strings.ReplaceAll(pred, "%s", lit)
			t.Run(sql, func(t *testing.T) {
				for _, arm := range []struct {
					name string
					db   *DB
				}{{"rows", db}, {"empty", empty}} {
					_, err := tmRun(ctx, arm.db, sql)
					if err == nil {
						t.Fatalf("%s: answered instead of refusing %q", arm.name, lit)
					}
					if !strings.Contains(err.Error(), "invalid input syntax for type numeric") {
						t.Errorf("%s: error = %v, want PostgreSQL's numeric input-syntax error", arm.name, err)
					}
					if got := sqlerr.StateOf(err); got != "22P02" {
						t.Errorf("%s: SQLSTATE = %q, want 22P02", arm.name, got)
					}
				}
			})
		}
	}
}

// TestUnaryMinusOverANaNLiteralIsStillRefused pins the one place the widening
// deliberately does NOT reach. `-'NaN'` is not a value in PostgreSQL either —
// an unknown-typed literal under unary minus is 42725 there, "operator is not
// unique: - unknown" — and negating the text would produce '-NaN', which
// PostgreSQL's numeric input refuses outright. So the fold keeps the narrow
// reader (kernel.FiniteDecimalText) and this stays the compile-time refusal
// #505 made it, exactly as `-'abc'` does. The SQLSTATE difference from
// PostgreSQL's 42725 is ADR-0012 item 5's recorded one, unchanged here.
func TestUnaryMinusOverANaNLiteralIsStillRefused(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, sql := range []string{
		"SELECT COUNT(*) AS n FROM declit WHERE d_2 = -'NaN'",
		"SELECT COUNT(*) AS n FROM declit WHERE d_2 = -'Infinity'",
		"SELECT -'NaN' AS v FROM declit",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := tmRun(ctx, db, sql); err == nil {
				t.Fatalf("%s answered instead of refusing", sql)
			}
		})
	}

	// The finite fold is untouched: row 99 holds d_2 = -43219.87.
	declitCheck(t, ctx, db,
		"SELECT COUNT(*) AS n FROM declit WHERE k = 99 AND d_2 = -'43219.87'", 1)
}

// TestFloatColumnsKeepTheirOwnNaNRule is the parity control for #534. FLOAT32
// and FLOAT64 DO hold NaN and the infinities, so their comparison against
// these literals is PostgreSQL's FLOAT order (ADR-0012 item 8) — NaN equal to
// itself and above every other value, -Infinity below every one — which is a
// different rule from the DECIMAL bound above, reached through different code
// (`kernel.CompareFloat64`, never `batch.DecimalBoundTextAt`). Nothing here
// may move when the DECIMAL accept-set widens.
//
// The type matrix holds no NaN in either float column, so PostgreSQL's answers
// over it are the same SHAPE as the decimal ones: `= 'NaN'` finds nothing and
// `< 'NaN'` finds every non-NULL row.
func TestFloatColumnsKeepTheirOwnNaNRule(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	// c_f64 is NULL every 41st row of 5000 and c_f32 every 37th.
	const (
		f64NonNull = 4879
		f32NonNull = 4865
	)

	// The ROW-AT-A-TIME path, which a CASE forces. It reads the quoted
	// literal as a float64 and orders it by PostgreSQL's float rule, so these
	// are PostgreSQL's own answers.
	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"CASE WHEN c_f64 = 'NaN' THEN 1 ELSE 0 END = 1", 0},
		{"CASE WHEN c_f64 < 'NaN' THEN 1 ELSE 0 END = 1", f64NonNull},
		{"CASE WHEN c_f64 > 'NaN' THEN 1 ELSE 0 END = 1", 0},
		{"CASE WHEN c_f64 > '-Infinity' THEN 1 ELSE 0 END = 1", f64NonNull},
		{"CASE WHEN c_f64 <= 'Infinity' THEN 1 ELSE 0 END = 1", f64NonNull},
		{"CASE WHEN c_f32 = 'NaN' THEN 1 ELSE 0 END = 1", 0},
		{"CASE WHEN c_f32 < 'NaN' THEN 1 ELSE 0 END = 1", f32NonNull},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			res, err := tmRun(ctx, db, "SELECT COUNT(*) AS n FROM typemx WHERE "+tc.pred)
			if err != nil {
				t.Fatalf("%s: %v", tc.pred, err)
			}
			if got, _ := tmAsInt64(res.Rows[0][res.Columns[0]]); got != tc.want {
				t.Errorf("%s = %d, want %d (live PostgreSQL 17's float rule)", tc.pred, got, tc.want)
			}
		})
	}

	// The BARE forms take the vectorized float kernel, whose constant
	// coercion still reads ANY string as 0.0 — the float arm of #536, tracked
	// as #646 and pinned in the pg-oracle corpus. They are asserted here at
	// the values they answered BEFORE #534, because the point of this test is
	// that the DECIMAL widening did not reach them. When #646 lands these
	// become PostgreSQL's answers above and this block fails — the same
	// ratchet the corpus pin uses, and deleting it is that fix's proof.
	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"c_f64 = 'NaN'", 1},          // the row holding 0.0, matched against 0.0
		{"c_f64 < 'NaN'", 0},          // asks `< 0.0` over a non-negative column
		{"c_f64 > '-Infinity'", 4878}, // asks `> 0.0`, so the 0.0 row drops out
		{"c_f32 < 'NaN'", 0},
	} {
		t.Run("pinned_"+tc.pred, func(t *testing.T) {
			res, err := tmRun(ctx, db, "SELECT COUNT(*) AS n FROM typemx WHERE "+tc.pred)
			if err != nil {
				t.Fatalf("%s: %v", tc.pred, err)
			}
			if got, _ := tmAsInt64(res.Rows[0][res.Columns[0]]); got != tc.want {
				t.Errorf("%s = %d, want %d — this arm is #536's, and #534 must not have moved it",
					tc.pred, got, tc.want)
			}
		})
	}
}

// TestStringColumnAgainstNaNIsStillATextComparison is the other control: a
// genuine STRING column compares its BYTES, whatever the literal looks like
// (ADR-0012 item 5's TEXT-against-a-number bullet). The DECIMAL widening is
// selected from the column's DECLARATION, so a text column must not acquire a
// numeric bound because its literal happens to spell one of the three.
func TestStringColumnAgainstNaNIsStillATextComparison(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	// c_str holds "s-%06d", so no row is 'NaN' and every non-NULL one sorts
	// ABOVE it ("N" < "s") — a text answer, not a bound.
	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"c_str = 'NaN'", 0},
		{"c_str < 'NaN'", 0},
		{"c_str > 'NaN'", 4884},
		{"CASE WHEN c_str > 'NaN' THEN 1 ELSE 0 END = 1", 4884},
		{"c_str IS NOT NULL", 4884},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			res, err := tmRun(ctx, db, "SELECT COUNT(*) AS n FROM typemx WHERE "+tc.pred)
			if err != nil {
				t.Fatalf("%s: %v", tc.pred, err)
			}
			if got, _ := tmAsInt64(res.Rows[0][res.Columns[0]]); got != tc.want {
				t.Errorf("%s = %d, want %d", tc.pred, got, tc.want)
			}
		})
	}
}
