package wadjet

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// fnegOpen is a float fixture with NEGATIVE values in it, which the type
// matrix does not have: c_f64 is float64(i)/3 and c_f32 is float32(i)/7, both
// non-negative on every row.
//
// The sign is what makes the test below able to fail. Every finite float
// renders starting with a digit or '-', and only a NEGATIVE rendering ("-5")
// sorts BELOW the "-Infinity" of a LEXICOGRAPHIC comparison — so over a
// non-negative column the correct float answer and the wrong text answer are
// the SAME row set for `> '-Infinity'`, and a gate written on one would gate
// nothing. It carries a DECIMAL column at the same values so the two rules can
// be asked side by side on identical numbers.
func fnegOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "fneg", schema, nil); err != nil {
		t.Fatal(err)
	}
	dec := func(v int64) parquet.Decimal128 {
		hi := int64(0)
		if v < 0 {
			hi = -1
		}
		return parquet.Decimal128{Hi: hi, Lo: uint64(v)}
	}
	rows := []map[string]any{
		{"k": int64(0), "f": float64(-5), "r": float32(-5), "d": dec(-500)},
		{"k": int64(1), "f": float64(-0.5), "r": float32(-0.5), "d": dec(-50)},
		{"k": int64(2), "f": float64(0), "r": float32(0), "d": dec(0)},
		{"k": int64(3), "f": float64(0.5), "r": float32(0.5), "d": dec(50)},
		{"k": int64(4), "f": float64(5), "r": float32(5), "d": dec(500)},
		{"k": int64(5)},
	}
	ing := db.NewIngester("fneg", schema, nil, ingest.Config{MaxBufferRows: 16})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestFloatColumnsKeepTheirOwnNaNRule is the parity control for #534, and it
// is a DIFFERENT rule from the DECIMAL one. FLOAT32 and FLOAT64 HOLD NaN and
// the infinities, so a comparison against one of them is an ordinary float
// comparison in PostgreSQL's float order (ADR-0012 item 8) — not the bound a
// DECIMAL column gets, which exists only because that carrier has no such
// value.
//
// The row-at-a-time path applies it through kernel.FloatSpecialText and
// kernel.CompareFloat64. It did NOT before this change: expr.
// decimalTextOrderFloat gated on batch.DecimalTextAt, which refuses all three,
// so boxedPair.order fell through to compare()'s LEXICOGRAPHIC string
// comparison — right on every non-negative column and wrong on this one.
//
// Every row set is live postgres:17-alpine (17.11) on the identical six rows.
func TestFloatColumnsKeepTheirOwnNaNRule(t *testing.T) {
	ctx := context.Background()
	db := fnegOpen(t)

	// The row-at-a-time path, which a CASE forces. -Infinity is the shape
	// that discriminates: rows 0 and 1 are negative and a text comparison
	// drops them.
	for _, tc := range []struct {
		pred string
		want []int64
	}{
		{"CASE WHEN f > '-Infinity' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN f < '-Infinity' THEN 1 ELSE 0 END = 1", nil},
		{"CASE WHEN f >= '-Infinity' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN f < 'NaN' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN f = 'NaN' THEN 1 ELSE 0 END = 1", nil},
		{"CASE WHEN f <= 'Infinity' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN f > 'Infinity' THEN 1 ELSE 0 END = 1", nil},
		// float4, the same shapes.
		{"CASE WHEN r > '-Infinity' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN r < 'NaN' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		// A SIGNED NaN is where the FLOAT grammar and the NUMERIC one part:
		// float8 reads '+NaN' and '-NaN' as NaN, numeric refuses both with
		// 22P02. So these ANSWER here and stay refused against a DECIMAL.
		{"CASE WHEN f < '+NaN' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN f < '-NaN' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN f < ' nan ' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		// The DECIMAL column at the SAME values takes the bound rule and
		// answers the same row set — the two rules agree over a column
		// holding no special, which is the whole of ADR-0024 item 6.
		{"d > '-Infinity'", []int64{0, 1, 2, 3, 4}},
		{"CASE WHEN d > '-Infinity' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2, 3, 4}},
		{"d < 'NaN'", []int64{0, 1, 2, 3, 4}},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			fnegRows(t, ctx, db, tc.pred, tc.want)
		})
	}

	// The BARE float forms take the vectorized float kernel, which still
	// reads ANY quoted constant as 0.0 — the float arm of #536, tracked as
	// #646 and pinned in the pg-oracle corpus (RealColumnGtNegInfinity).
	// They are asserted at the values they answer TODAY, because the point of
	// this block is that neither #534 nor the row-path fix above reached
	// them. When #646 lands these become the row sets above and this block
	// fails — the same ratchet the corpus pin uses, and deleting it is that
	// fix's proof.
	for _, tc := range []struct {
		pred string
		want []int64
	}{
		{"f > '-Infinity'", []int64{3, 4}}, // asks '> 0.0'
		{"f < 'NaN'", []int64{0, 1}},       // asks '< 0.0'
		{"r > '-Infinity'", []int64{3, 4}},
	} {
		t.Run("pinned_646_"+tc.pred, func(t *testing.T) {
			fnegRows(t, ctx, db, tc.pred, tc.want)
		})
	}

	// ARITHMETIC over a DECIMAL leaves the exact carrier for a float64 and
	// the result carries no declaration, so boxedPair can select no rule and
	// the comparison falls to compare()'s text rendering: `d + 0 >
	// '-Infinity'` drops the negative rows where PostgreSQL keeps them. That
	// is ADR-0012 item 6's recorded limit ("arithmetic over DECIMAL goes
	// through float64 before any comparison sees it") meeting #555's untyped
	// computed DECIMAL, not a rule this change decides; pinned here so it is
	// visible rather than assumed.
	t.Run("pinned_555_arithmetic_over_decimal", func(t *testing.T) {
		fnegRows(t, ctx, db, "d + 0 > '-Infinity'", []int64{2, 3, 4}) // PostgreSQL: 0 1 2 3 4
	})
}

// fnegRows asserts the exact k values a predicate selects over fneg, with the
// row-group prune both on and off.
func fnegRows(t *testing.T, ctx context.Context, db *DB, pred string, want []int64) {
	t.Helper()
	sql := "SELECT k FROM fneg WHERE " + pred + " ORDER BY k"
	for _, prune := range []bool{true, false} {
		prevStats := scan.StatsPrune.Set(prune)
		prevDict := scan.DictPrune.Set(prune)
		res, err := tmRun(ctx, db, sql)
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
		if err != nil {
			t.Fatalf("prune=%v: %s: %v", prune, sql, err)
		}
		got := make([]int64, 0, len(res.Rows))
		for _, r := range res.Rows {
			v, ok := tmAsInt64(r["k"])
			if !ok {
				t.Fatalf("prune=%v: k came back as %#v", prune, r["k"])
			}
			got = append(got, v)
		}
		if !slices.Equal(got, want) {
			t.Errorf("prune=%v: %s\n  got %v, want %v", prune, sql, got, want)
		}
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
