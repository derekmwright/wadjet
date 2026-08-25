package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #504: `compare()`'s mixed number/DECIMAL-text branch decided which reading
// to apply by SNIFFING the runtime box — any Go string that PARSED as a number
// was read numerically against the other side. Nothing in a box says whether
// it came from a DECIMAL column's rendered text or from a genuine STRING
// column whose value happens to look like a number, so a STRING column
// compared NUMERICALLY on the row-at-a-time path and as TEXT through the
// vectorized kernel: one predicate, two answers, decided by which lowering the
// query happened to take.
//
// The kernel's half was not "text" either. A non-string constant reached
// `kernel.toString` and came back as the EMPTY STRING, so `s = 1.5` compared
// every row against "": `=` selected nothing, `>` selected ALL five rows
// including the one holding "1.5", and `<` selected nothing.
//
// Both halves are fixed to the same rule, and the fixture is chosen so that
// rule is VISIBLE: "1.50" and "1.5" are the same number and different text,
// "10" and "9" sort the opposite way as text and as numbers, and "abc" is a
// number under neither reading.
//
// PostgreSQL cannot be asked this pair directly — it refuses `text = numeric`
// with 42883 "operator does not exist", an OVERLOAD RESOLUTION failure wadjet
// has no overload set to reproduce (ADR-0012 item 5). It CAN be asked the
// quoted form, and that is what pins these numbers: every expectation below is
// what live postgres:17-alpine answers for the same predicate with the literal
// QUOTED, over the same five rows in a `text COLLATE "C"` column — `s = '1.5'`
// 1, `s > '1.5'` 4, `s > '10'` 2, `s < '10'` 2, and so on, all thirteen. So
// wadjet's reading of the unquoted literal is PostgreSQL's reading of the
// quoted one, and the only divergence left is that PostgreSQL declines to
// resolve the unquoted spelling at all.
func strnumOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "strnum", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"k": int64(1), "s": "1.50"},
		{"k": int64(2), "s": "1.5"},
		{"k": int64(3), "s": "abc"},
		{"k": int64(4), "s": "10"},
		{"k": int64(5), "s": "9"},
	}
	ing := db.NewIngester("strnum", schema, nil, ingest.Config{MaxBufferRows: 16})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestTextColumnAgainstNumericLiteral(t *testing.T) {
	ctx := context.Background()
	db := strnumOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		// The reported shape. "1.50" and "1.5" are one number and two
		// strings, and the column is a STRING, so only "1.5" matches.
		{"s = 1.5", 1},
		{"s <> 1.5", 4},
		// The literal's exact SOURCE TEXT is the carrier (ADR-0012 item 6),
		// so `= 1.50` and `= 1.5` are different predicates — as `= '1.50'`
		// and `= '1.5'` already were.
		{"s = 1.50", 1},
		{"s = 1.500", 0},
		// Byte order, where '.' (0x2E) sorts below '0' (0x30): "1.50", "10",
		// "9" and "abc" are all above "1.5". A numeric reading would put
		// "1.50" level with it and "9" below "10".
		{"s > 1.5", 4},
		{"s >= 1.5", 5},
		{"s < 1.5", 0},
		{"s <= 1.5", 1},
		// Against "10": "abc" and "9" are above it and "1.50"/"1.5" below,
		// because '.' (0x2E) sorts under '0' (0x30). Numerically "9" would
		// be BELOW and "1.50" would not move.
		{"s > 10", 2},
		{"s < 10", 2},
		{"s = 10", 1},
		// A literal naming no number at all is still just text here: the
		// DECIMAL refusal (#463) is a rule about a DECIMAL column, and this
		// column is a STRING.
		{"s = 'abc'", 1},
		{"s > 'abc'", 0},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			// The vectorized kernel and the row-at-a-time evaluator must
			// answer alike — a CASE wrapper is not vectorizable, so the
			// second form is evaluated row by row. Their disagreement IS the
			// bug this test exists for, so both forms are asserted against
			// the same expectation rather than against each other.
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM strnum WHERE "+tc.pred, tc.want)
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM strnum WHERE CASE WHEN "+tc.pred+
					" THEN 1 ELSE 0 END = 1", tc.want)
		})
	}
}

// The three boxed comparison sites read the same rule from the same
// declaration, so a STRING column does not become numeric by being compared
// inside a CASE, an IS DISTINCT FROM or a GREATEST.
func TestTextColumnAgainstNumericLiteralAtBoxedSites(t *testing.T) {
	ctx := context.Background()
	db := strnumOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		// Only the row holding exactly "1.5".
		{"CASE s WHEN 1.5 THEN 1 ELSE 0 END = 1", 1},
		{"s IS DISTINCT FROM 1.5", 4},
		{"s IS NOT DISTINCT FROM 1.5", 1},
		// GREATEST picks by the same byte order: every value here is at or
		// above "1.5", so the extremum is the column on all five rows.
		{"GREATEST(s, 1.5) = s", 5},
		// LEAST picks the literal except on the row that equals it.
		{"LEAST(s, 1.5) = 1.5", 5},
		{"LEAST(s, 1.5) = s", 1},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM strnum WHERE "+tc.pred, tc.want)
		})
	}
}

// A BOOLEAN literal against a TEXT column is the same shape one type over, and
// the same requirement: PostgreSQL refuses the pair (`text = boolean` is
// 42883, verified live), so nothing outside the engine decides it — but one
// predicate still has to get ONE answer.
//
// It did not. kernel.toString returned the EMPTY STRING for a bool, so the
// vectorized filter compared every row against "" while the row-at-a-time
// path compared against "true" (fmt.Sprint). Rendering it the way
// PostgreSQL's own `boolean::text` does — "true"/"false", the single-letter
// 't' being psql's display and not the cast — makes the two agree and makes
// the answer explainable (#504 review, non-blocker c).
func TestTextColumnAgainstBooleanLiteral(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "strbool", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("strbool", schema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"k": int64(1), "s": "true"},
		{"k": int64(2), "s": "false"},
		{"k": int64(3), "s": "t"},
		{"k": int64(4), "s": ""},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// The row IDENTITY, not the count: this fixture holds both a "true" row
	// and an empty-string row on purpose, so a COUNT cannot tell "matched the
	// row spelled true" from "matched the row spelled nothing" — which is
	// exactly the confusion the empty-string rendering created.
	for _, tc := range []struct {
		pred string
		want int64 // the k of the single row that must match
	}{
		{"s = TRUE", 1},
		{"s = FALSE", 2},
		{"s = ''", 4},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			for _, sql := range []string{
				"SELECT k FROM strbool WHERE " + tc.pred,
				"SELECT k FROM strbool WHERE CASE WHEN " + tc.pred + " THEN 1 ELSE 0 END = 1",
			} {
				res, err := tmRun(ctx, db, sql)
				if err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
				if len(res.Rows) != 1 {
					t.Fatalf("%s\n  matched %d rows %v, want exactly row k=%d",
						sql, len(res.Rows), res.Rows, tc.want)
				}
				got, _ := tmAsInt64(res.Rows[0]["k"])
				if got != tc.want {
					t.Errorf("%s\n  matched row k=%d, want k=%d — the two paths must "+
						"match the SAME row, not merely the same number of them", sql, got, tc.want)
				}
			}
		})
	}
}
