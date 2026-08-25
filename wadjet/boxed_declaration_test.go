package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The review of #504's fix found that deleting `compare()`'s box-sniffing
// branch removed THREE readings and re-stated only one. This fixture holds the
// other two, plus the shapes whose declaration the classifier cannot read.
//
// `bx` is 12 rows: `k`/`i`/`f` are the row index in three numeric types, and
// `a` is a DECIMAL(9,2) whose values cross `k` and whose TEXT order disagrees
// with its numeric order ("9.00" above "10.00", "100.00" below "2.50").
func bxOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "i", Type: parquet.TypeInt32},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "bx", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Unscaled at scale 2; -1 marks the NULL row.
	dec := []int64{1000, 900, 250, -1, 0, 1100, 10000, 300, 2000, 100, 500, 800}
	rows := make([]map[string]any, len(dec))
	for k := range dec {
		m := map[string]any{"k": int64(k), "f": float64(k), "i": int32(k)}
		if dec[k] >= 0 {
			m["a"] = parquet.Decimal128{Lo: uint64(dec[k])}
		}
		rows[k] = m
	}
	ing := db.NewIngester("bx", schema, nil, ingest.Config{MaxBufferRows: 32})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestNumberColumnAgainstQuotedNumericLiteral: PostgreSQL types an
// unknown-typed literal FROM the operand it meets, so `k > '2'` over a BIGINT
// column is the integer comparison `k > 2`. That is the opposite direction
// from the TEXT-column rule in the same bullet of ADR-0012 item 5, and
// deleting the box-sniffing branch took it out along with the rest.
//
// It came back wrong in two ways at once, which is why the equality shapes
// are here beside the ordering ones: with the numeric reading gone, the
// comparison fell into `compare()`'s TEMPORAL branch, whose guard was "the
// string parsed, OR the number is zero" — true of ANY unparseable string
// against a zero. So `k = '2'` matched k=2 AND k=0.
//
// Every expectation is live postgres:17-alpine's over the same twelve rows.
func TestNumberColumnAgainstQuotedNumericLiteral(t *testing.T) {
	ctx := context.Background()
	db := bxOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"k > '2'", 9},
		{"k >= '2'", 10},
		{"k < '2'", 2},
		{"k <= '2'", 3},
		{"k = '2'", 1},
		{"k <> '2'", 11},
		// The zero row, which the temporal guard used to hand every
		// unparseable string.
		{"k = '0'", 1},
		{"k = 'abc'", 0},
		// INT32 and FLOAT64 take the same rule, the float one by float
		// comparison rather than exact.
		{"i > '2'", 9},
		{"f > '2'", 9},
		{"f = '2.0'", 1},
		// The list and range forms are the same comparison chained.
		{"k IN ('2','3')", 2},
		{"k BETWEEN '2' AND '4'", 3},
		// And the boxed sites.
		{"CASE k WHEN '2' THEN 1 ELSE 0 END = 1", 1},
		{"k IS DISTINCT FROM '2'", 11},
		{"GREATEST(k, '2') = k", 10},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			// The CASE wrapper forces the row-at-a-time evaluator, which is
			// the path this rule lives on. The BARE form takes the vectorized
			// filter, whose integer and float arms still read a quoted
			// constant as the type's ZERO — #536, pre-existing and pinned in
			// the pg-oracle corpus rather than asserted here.
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM bx WHERE CASE WHEN "+tc.pred+" THEN 1 ELSE 0 END = 1",
				tc.want)
		})
	}
}

// TestDecimalAgainstUnreadableDeclaration: a DECIMAL column boxes as its
// RENDERED TEXT, so a comparison against an operand whose declaration nothing
// can read — a scalar subquery, arithmetic, a CAST, a COALESCE — fell through
// to a lexicographic comparison. The binding used to require the OTHER side to
// be a number it could name; a PROVEN DECIMAL operand now applies against any
// other kind, and the numeric reading declines on its own when the other box
// is not a number (which is what keeps a genuine STRING column lexical).
//
// `a > k` and `a > k + 0` are the same question and are asserted together:
// one predicate with two answers was the visible symptom.
func TestDecimalAgainstUnreadableDeclaration(t *testing.T) {
	ctx := context.Background()
	db := bxOpen(t)

	for _, tc := range []struct {
		pred string
		want int64
	}{
		{"a > (SELECT MIN(f) FROM bx)", 10},
		{"a < (SELECT MAX(f) FROM bx)", 8},
		{"a > k", 6},
		{"a > k + 0", 6},
		{"a > CAST(k AS BIGINT)", 6},
		{"a > f", 6},
		{"a > i", 6},
		// A NULL alternative used to poison the operand's kind, and an
		// untyped literal one has to take the DECIMAL's type.
		{"COALESCE(a, NULL) = 10.00", 1},
		{"COALESCE(a, NULL) > 10.00", 3},
		// A quoted numeric literal against the DECIMAL column: exact, at the
		// boxed sites too.
		{"a = '10.00'", 1},
		{"CASE a WHEN '10.00' THEN 1 ELSE 0 END = 1", 1},
		{"a IS DISTINCT FROM '10.00'", 11},
	} {
		t.Run(tc.pred, func(t *testing.T) {
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM bx WHERE "+tc.pred, tc.want)
			declitCheck(t, ctx, db,
				"SELECT COUNT(*) AS n FROM bx WHERE CASE WHEN "+tc.pred+" THEN 1 ELSE 0 END = 1",
				tc.want)
		})
	}
}
