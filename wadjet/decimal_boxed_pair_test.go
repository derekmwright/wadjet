package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #506: `expr.bindDecimalCols` — the binding that compares two DECIMAL columns
// on their unscaled Int128s rather than on their rendered text — was wired up
// in `NewCmp` alone. The SAME two columns at a BOXED site (a simple `CASE a
// WHEN b`, `a IS DISTINCT FROM b`, `GREATEST`/`LEAST(a, b)`) reached
// `compare()` as two Go strings, indistinguishable from two ordinary STRING
// columns, and its two-strings fast path answered LEXICOGRAPHICALLY.
//
// The fixture is built so lexical and numeric order DISAGREE on every row that
// matters, rather than on one unlucky value:
//
//   - ±1 ULP at the WIDER scale. `12.75` against `12.7500` is the same number
//     and different text; against `12.7501` and `12.7499` it is one unit in
//     the last place either side. A lexicographic comparison calls all three
//     "less", because "12.75" is a prefix of each.
//   - A leading-digit trap. `2.00` against `10.0000`: "2.00" sorts ABOVE
//     "10.0000" as text and below it as a number.
//   - Zero and a negative at two scales, where the text differs and the value
//     does not.
//   - NULL on either side and on both, because GREATEST/LEAST SKIP NULL
//     arguments while `=` propagates it — the two rules that decide six of
//     these answers.
//
// Every expectation is what live postgres:17-alpine answers on the identical
// fixture, transcribed from its own per-row truth table.
const dpairRows = 9

func dpairOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "dpair", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Unscaled integers at the columns' declared scales: a is scale 2, b is
	// scale 4. Written as the writer's verbatim unscaled input (ADR-0018 §4)
	// so no float is involved in building the data either.
	type row struct {
		k     int64
		a, b  int64
		aNull bool
		bNull bool
	}
	src := []row{
		{k: 1, a: 1275, b: 127500},    // equal; "12.75" is a PREFIX of "12.7500"
		{k: 2, a: 1275, b: 127501},    // b greater by one ulp of scale 4
		{k: 3, a: 1275, b: 127499},    // b less by one ulp of scale 4
		{k: 4, a: -1, b: -100},        // equal and negative: -0.01 == -0.0100
		{k: 5, a: 200, b: 100000},     // 2.00 < 10.0000, and "2.00" > "10.0000"
		{k: 6, a: 0, b: 0},            // zero at two scales is one value
		{k: 7, aNull: true, b: 10000}, // NULL on the left
		{k: 8, a: 1275, bNull: true},  // NULL on the right
		{k: 9, aNull: true, bNull: true},
	}
	rows := make([]map[string]any, 0, len(src))
	for _, r := range src {
		m := map[string]any{"k": r.k}
		if !r.aNull {
			m["a"] = decimal128FromInt64(r.a)
		}
		if !r.bNull {
			m["b"] = decimal128FromInt64(r.b)
		}
		rows = append(rows, m)
	}
	ing := db.NewIngester("dpair", schema, nil, ingest.Config{MaxBufferRows: dpairRows + 1})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// decimal128FromInt64 is decimal128FromBigInt for a value an int64 holds,
// sign-extended into the high half the way two's complement requires.
func decimal128FromInt64(v int64) parquet.Decimal128 {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	return parquet.Decimal128{Hi: hi, Lo: uint64(v)}
}

func TestDecimalColumnPairAtBoxedSites(t *testing.T) {
	ctx := context.Background()
	db := dpairOpen(t)

	for _, tc := range []struct {
		name string
		pred string
		want int64
	}{
		// The control: bound in NewCmp since #477, and already correct.
		{name: "direct equality", pred: "a = b", want: 3},
		{name: "direct inequality", pred: "a <> b", want: 3},

		// #506's three sites, the same two columns.
		{name: "simple CASE", pred: "CASE a WHEN b THEN 1 ELSE 0 END = 1", want: 3},
		{name: "IS DISTINCT FROM", pred: "a IS DISTINCT FROM b", want: 5},
		{name: "IS NOT DISTINCT FROM", pred: "a IS NOT DISTINCT FROM b", want: 4},
		{name: "GREATEST against the wider column", pred: "GREATEST(a, b) = b", want: 6},
		{name: "LEAST against the narrower column", pred: "LEAST(a, b) = a", want: 6},

		// The extremum's own value, compared against a literal and against
		// zero: the answer depends on which argument pickExtremum picked, so
		// a lexicographic pick is visible here even where the two shapes
		// above happen to agree.
		{name: "GREATEST against a literal", pred: "GREATEST(a, b) = 12.75", want: 3},
		{name: "LEAST against zero", pred: "LEAST(a, b) > 0", want: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM dpair WHERE "+tc.pred, tc.want)
		})
	}
}

// TestDecimalColumnPairAtBoxedSitesOnDeclit is the same defect on the fixture
// #506 was filed against, whose columns cross once over 200 rows and carry
// values wider than a float64 and wider than an int64. It is the shape that
// says the fix is not pinned to the nine hand-picked values above.
//
// Every count is live postgres:17-alpine's on the identical fixture. Note
// GREATEST/LEAST's NULL SKIPPING is load-bearing in two of them: where `a` is
// NULL and `b` is not, `GREATEST(a, b)` is `b`, so `= b` is TRUE — 11 rows
// beyond the 90 where both are non-NULL and b is the greater.
func TestDecimalColumnPairAtBoxedSitesOnDeclit(t *testing.T) {
	ctx := context.Background()
	db := declitOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"d_2 = d_4", 1},
		{"CASE d_2 WHEN d_4 THEN 1 ELSE 0 END = 1", 1},
		{"GREATEST(d_2, d_4) = d_4", 101},
		{"LEAST(d_2, d_4) = d_2", 98},
		{"d_2 IS DISTINCT FROM d_4", 198},
		{"d_2 IS NOT DISTINCT FROM d_4", 2},
		// The 38-digit column, whose values no float64 and no int64 holds.
		{"CASE d_4 WHEN d_wide THEN 1 ELSE 0 END = 1", 1},
		{"GREATEST(d_2, d_wide) = d_wide", 97},
		{"d_4 IS DISTINCT FROM d_wide", 198},
		// A DECIMAL against a column of ANOTHER type at the same three sites
		// (#476's pair, which the same binding now answers from declarations
		// rather than by sniffing the box — see #504).
		{"GREATEST(k, d_2) = k", 107},
		{"k IS DISTINCT FROM d_2", 200},
		{"CASE k WHEN d_2 THEN 1 ELSE 0 END = 1", 0},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM declit WHERE "+tc.pred, tc.want)
		})
	}
}
