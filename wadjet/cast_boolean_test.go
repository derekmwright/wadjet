package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// cbOpen loads #592's fixture: one row per interesting truth value of every
// source type a CAST to BOOLEAN can meet.
//
// c is the reported column — a BIGINT holding 0, a positive, a negative and a
// NULL, which is exactly the SQLancer repro's table. The rest give the cast's
// other source types one row each of TRUE, FALSE and NULL, so a rule that
// silently reads the wrong arm shows up as a wrong ROW rather than a wrong
// count.
func cbOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c", Type: parquet.TypeInt64, Nullable: true},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeBool, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 0, Nullable: true},
		{Name: "dt", Type: parquet.TypeDate, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "cb", sch, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := []map[string]any{
		{"id": int64(1), "c": int64(0), "i": int32(0), "b": false, "s": "false", "f": float64(0), "d": "0", "dt": "2020-01-01"},
		{"id": int64(2), "c": int64(1), "i": int32(1), "b": true, "s": "true", "f": float64(1), "d": "1", "dt": "2020-01-02"},
		{"id": int64(3), "c": int64(2), "i": int32(2), "b": true, "s": "t", "f": float64(2), "d": "2", "dt": "2020-01-03"},
		{"id": int64(4), "c": int64(-1), "i": int32(-1), "b": nil, "s": "no", "f": float64(-1), "d": "-1", "dt": nil},
		{"id": int64(5), "c": nil, "i": nil, "b": false, "s": nil, "f": nil, "d": nil, "dt": "2020-01-05"},
	}
	ing := db.NewIngester("cb", sch, nil, ingest.Config{MaxBufferRows: 64, RowGroupSize: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

// cbIDs runs sql and returns the id column, in the order it came back.
func cbIDs(t *testing.T, db *DB, sql string) []int64 {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("query error: %v\n  SQL: %s", err, sql)
	}
	out := make([]int64, 0, len(res.Rows))
	for _, r := range res.Rows {
		v, ok := r["id"].(int64)
		if !ok {
			t.Fatalf("id came back as %T (%v)\n  SQL: %s", r["id"], r["id"], sql)
		}
		out = append(out, v)
	}
	return out
}

func cbSame(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBareBooleanPredicateAgreesWithTheProjection is #592's gate.
//
// `Cast.Eval` had no boolean arm at all, so `CAST(<x> AS BOOLEAN)` dropped its
// destination type and returned the operand unconverted. Three consumers then
// read that box by three different rules:
//
//	SELECT list   Vector.SetValue's TypeBool arm coerced an integer to `!= 0`,
//	              so the projection looked correct — by accident
//	NOT / IS NULL evalBoolNull's toBoolVal read the same truthiness, so those
//	              two were correct as well
//	WHERE         FilterPredicate's `v.(bool)` assertion FAILED and answered
//	              FALSE, so the bare predicate excluded EVERY ROW
//
// The three-way split is why the SQLancer TLP-WHERE oracle found this: its
// partition is those three readings of one predicate, and the arm that
// contributed nothing made it undercount by exactly the predicate's true rows.
// So the assertions below are not only "the WHERE clause is right" — they are
// "the WHERE clause, the projection and the negation are ONE reading".
//
// Every want is a live postgres:17-alpine transcript over these values, except
// where a note says PostgreSQL has no such cast at all.
func TestBareBooleanPredicateAgreesWithTheProjection(t *testing.T) {
	db := cbOpen(t)

	// The reported spellings. ids 2,3,4 hold 1, 2 and -1; id 1 holds 0 and
	// id 5 holds NULL.
	trueRows := []int64{2, 3, 4}
	for _, sql := range []string{
		`SELECT id FROM cb WHERE (c)::BOOLEAN ORDER BY id`,
		`SELECT id FROM cb WHERE (c)::BOOLEAN = TRUE ORDER BY id`,
		`SELECT id FROM cb WHERE CAST(c AS BOOLEAN) ORDER BY id`,
		`SELECT id FROM cb WHERE CAST(c AS BOOLEAN) IS TRUE ORDER BY id`,
		`SELECT id FROM cb WHERE (c)::BOOLEAN AND id > 0 ORDER BY id`,
		`SELECT id FROM cb WHERE id > 0 AND (c)::BOOLEAN ORDER BY id`,
		`SELECT id FROM (SELECT id, (c)::BOOLEAN AS bb FROM cb) sub WHERE bb ORDER BY id`,
		`SELECT id FROM cb WHERE CASE WHEN (c)::BOOLEAN THEN TRUE ELSE FALSE END ORDER BY id`,
	} {
		if got := cbIDs(t, db, sql); !cbSame(got, trueRows) {
			t.Errorf("bare boolean predicate answered %v, want %v (PostgreSQL 17)\n  SQL: %s", got, trueRows, sql)
		}
	}

	// The two spellings #592 reports as already CORRECT. They are the
	// evidence the three readings existed, so a fix that moved them moved too
	// much — and they are TLP-WHERE's other two arms.
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE NOT (c)::BOOLEAN ORDER BY id`); !cbSame(got, []int64{1}) {
		t.Errorf("NOT (c)::BOOLEAN answered %v, want [1]", got)
	}
	for _, sql := range []string{
		`SELECT id FROM cb WHERE (c)::BOOLEAN IS NULL ORDER BY id`,
		`SELECT id FROM cb WHERE (c)::BOOLEAN IS UNKNOWN ORDER BY id`,
	} {
		if got := cbIDs(t, db, sql); !cbSame(got, []int64{5}) {
			t.Errorf("%s answered %v, want [5]", sql, got)
		}
	}

	// TLP-WHERE's partition itself: the three arms together are the table,
	// once each. This is the assertion SQLancer failed
	// (ComparatorHelper.assumeResultSetsAreEqual), for any predicate.
	for _, pred := range []string{
		`(c)::BOOLEAN`, `CAST(c AS BOOLEAN)`, `CAST(i AS BOOLEAN)`, `b`,
		`COALESCE(b, FALSE)`, `(s)::BOOLEAN`, `NOT b`,
	} {
		sql := fmt.Sprintf(
			`SELECT id FROM cb WHERE %[1]s`+
				` UNION ALL SELECT id FROM cb WHERE NOT (%[1]s)`+
				` UNION ALL SELECT id FROM cb WHERE (%[1]s) IS NULL ORDER BY id`, pred)
		if got := cbIDs(t, db, sql); !cbSame(got, []int64{1, 2, 3, 4, 5}) {
			t.Errorf("TLP-WHERE partition over %s answered %v, want every row once", pred, got)
		}
	}

	// The SELECT list and the WHERE clause are one reading: project the cast
	// and filter on it, and the same rows must come back.
	proj := cbIDs(t, db, `SELECT id FROM (SELECT id, CAST(c AS BOOLEAN) AS bb FROM cb) s WHERE bb ORDER BY id`)
	filt := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(c AS BOOLEAN) ORDER BY id`)
	if !cbSame(proj, filt) {
		t.Errorf("the projection and the filter disagree about one cast: %v vs %v", proj, filt)
	}
}

// TestCastToBooleanTruthTable pins what each SOURCE TYPE answers.
//
// The rule is chosen from the operand's DECLARATION, never from the Go box a
// row produces (ADR-0012 item 8): a DECIMAL column and a STRING column both
// box as a Go string, and PostgreSQL answers them differently — 42846 against
// its boolean input function. Reading the box would give `DECIMAL(9,0)`
// holding 1 the answer TRUE where PostgreSQL refuses the cast outright, which
// is a wrong ROW rather than a wrong error code.
func TestCastToBooleanTruthTable(t *testing.T) {
	db := cbOpen(t)

	// INT64 / INT32: 0 is FALSE, everything else TRUE, NULL is NULL. This is
	// PostgreSQL's int4::bool rule, extended to the whole integer family as
	// a deliberate divergence (ADR-0012 item 5): PostgreSQL has no
	// int8::boolean cast, and refusing BIGINT while INTEGER answered would
	// split one rule across two column widths for no semantic reason.
	for _, col := range []string{"c", "i"} {
		if got := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(`+col+` AS BOOLEAN) ORDER BY id`); !cbSame(got, []int64{2, 3, 4}) {
			t.Errorf("CAST(%s AS BOOLEAN) answered %v, want [2 3 4]", col, got)
		}
	}
	// BOOL through the cast is the identity.
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(b AS BOOLEAN) ORDER BY id`); !cbSame(got, []int64{2, 3}) {
		t.Errorf("CAST(b AS BOOLEAN) answered %v, want [2 3]", got)
	}
	// STRING takes PostgreSQL's boolean input function: s holds "false",
	// "true", "t", "no", NULL.
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(s AS BOOLEAN) ORDER BY id`); !cbSame(got, []int64{2, 3}) {
		t.Errorf("CAST(s AS BOOLEAN) answered %v, want [2 3]", got)
	}
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE NOT CAST(s AS BOOLEAN) ORDER BY id`); !cbSame(got, []int64{1, 4}) {
		t.Errorf("NOT CAST(s AS BOOLEAN) answered %v, want [1 4]", got)
	}
	// A NULL literal cast to BOOLEAN is UNKNOWN, not an error and not TRUE.
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(NULL AS BOOLEAN) ORDER BY id`); len(got) != 0 {
		t.Errorf("CAST(NULL AS BOOLEAN) as a predicate answered %v, want no rows", got)
	}
	res, err := db.Query(context.Background(), `SELECT CAST(NULL AS BOOLEAN) AS b`)
	if err != nil {
		t.Fatalf("SELECT CAST(NULL AS BOOLEAN): %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["b"] != nil {
		t.Errorf("SELECT CAST(NULL AS BOOLEAN) answered %v, want a single NULL", res.Rows)
	}

	// The refusals. PostgreSQL has no cast from any of these to boolean and
	// raises 42846 cannot_coerce; verified live for float8, numeric, int8 and
	// int2. Wadjet keeps the integer family (above) and refuses the rest,
	// because a float's truthiness is a question PostgreSQL deliberately
	// declines to answer — and answering it silently is what let the SELECT
	// list and the WHERE clause disagree in the first place.
	for _, c := range []struct{ sql, want string }{
		{`SELECT id FROM cb WHERE CAST(f AS BOOLEAN)`, "double precision"},
		{`SELECT CAST(f AS BOOLEAN) AS x FROM cb`, "double precision"},
		{`SELECT id FROM cb WHERE CAST(d AS BOOLEAN)`, "numeric"},
		{`SELECT id FROM cb WHERE CAST(dt AS BOOLEAN)`, "date"},
		{`SELECT CAST(1.5 AS BOOLEAN) AS x`, "numeric"},
	} {
		_, err := db.Query(context.Background(), c.sql)
		if err == nil {
			t.Errorf("%s answered instead of refusing; PostgreSQL raises 42846", c.sql)
			continue
		}
		if got := sqlerr.StateOf(err); got != "42846" {
			t.Errorf("%s\n  SQLSTATE = %q, want 42846 — a client branches on this\n  err: %v", c.sql, got, err)
		}
		if want := "cannot cast type " + c.want + " to boolean"; err.Error() == "" || !contains(err.Error(), want) {
			t.Errorf("%s\n  message %q does not name the source type as %q", c.sql, err.Error(), want)
		}
	}

	// A CAST is not the only way to reach a BOOLEAN destination: a literal
	// integer is legal there, exactly as PostgreSQL's `1::int::bool` is.
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(1 AS BOOLEAN) ORDER BY id`); !cbSame(got, []int64{1, 2, 3, 4, 5}) {
		t.Errorf("CAST(1 AS BOOLEAN) answered %v, want every row", got)
	}
	if got := cbIDs(t, db, `SELECT id FROM cb WHERE CAST(0 AS BOOLEAN) ORDER BY id`); len(got) != 0 {
		t.Errorf("CAST(0 AS BOOLEAN) answered %v, want no rows", got)
	}
}

// TestCastTextToBooleanIsPostgresBoolin holds the STRING arm to
// `parse_bool_with_len` (src/backend/utils/adt/bool.c), which is the function
// PostgreSQL's `text::boolean` runs.
//
// The prefix rule is the part a hand-rolled reader gets wrong: PostgreSQL
// accepts any non-empty prefix of "true"/"false"/"yes"/"no", so `'tr'` and
// `'fals'` are values there, while `'o'` alone is an ERROR because it cannot
// choose between "on" and "off". Every entry is a live postgres:17-alpine
// transcript.
func TestCastTextToBooleanIsPostgresBoolin(t *testing.T) {
	ctx := context.Background()
	db := cbOpen(t)

	accept := map[string]bool{
		"t": true, "T": true, "tr": true, "tru": true, "true": true, "TRUE": true, "TrUe": true,
		"y": true, "ye": true, "yes": true, "YES": true,
		"on": true, "ON": true, "On": true,
		"1":        true,
		"  true  ": true, "\ttrue\n": true,
		"f": false, "F": false, "fa": false, "fal": false, "fals": false, "false": false, "FALSE": false,
		"n": false, "no": false, "nO": false,
		"of": false, "off": false, "Off": false,
		"0":   false,
		" f ": false,
	}
	for in, want := range accept {
		sql := fmt.Sprintf(`SELECT CAST(%s AS BOOLEAN) AS b`, quoteSQL(in))
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Errorf("%s: %v — PostgreSQL accepts this spelling", sql, err)
			continue
		}
		if len(res.Rows) != 1 || res.Rows[0]["b"] != want {
			t.Errorf("%s answered %v, want %v (PostgreSQL 17)", sql, res.Rows, want)
		}
	}

	// A string that names no boolean is a query ERROR, never a value and
	// never a match-nothing predicate — #463's rule for DECIMAL one type
	// family over. The message quotes the ORIGINAL input, untrimmed, as
	// PostgreSQL's does.
	for _, in := range []string{"garbage", "", "  ", "2", "01", "truex", "o", "ofx", "yess", "-1"} {
		sql := fmt.Sprintf(`SELECT CAST(%s AS BOOLEAN) AS b`, quoteSQL(in))
		_, err := db.Query(ctx, sql)
		if err == nil {
			t.Errorf("%s answered instead of refusing; PostgreSQL raises 22P02", sql)
			continue
		}
		if got := sqlerr.StateOf(err); got != "22P02" {
			t.Errorf("%s\n  SQLSTATE = %q, want 22P02 — a client branches on this\n  err: %v", sql, got, err)
		}
	}

	// The refusal reaches a WHERE clause too, over a COLUMN whose values are
	// not booleans: a match-nothing answer there deletes every row of a query
	// that cannot mean anything.
	if _, err := db.Query(ctx, `SELECT id FROM cb WHERE CAST('nope' AS BOOLEAN)`); err == nil {
		t.Error(`WHERE CAST('nope' AS BOOLEAN) answered instead of refusing`)
	}
}

// TestBareBooleanInHavingIsEvaluated is the HAVING half of #592's sweep.
//
// `logical.rewriteExpr` assumed EVERY function call in a HAVING clause was an
// aggregate standing for a column the aggregate produced, and rewrote it to a
// ColRef named after the function's rendered text. So `HAVING COALESCE(f,
// FALSE)` became a reference to a column literally named
// "coalesce(f, false)", which no batch has: as a bare predicate the filter
// silently admitted NO GROUP, and with a comparison around it the query failed
// with `filter column "coalesce(f, false)" does not exist in the input
// schema`. Only a HAVING naming no aggregate at all reached that fallback —
// one that does takes ReplaceAllAggregates, which has always asked
// IsAggregate.
func TestBareBooleanInHavingIsEvaluated(t *testing.T) {
	db := cbOpen(t)
	ctx := context.Background()

	// b groups into TRUE (ids 2,3), FALSE (ids 1,5) and NULL (id 4).
	cases := []struct {
		sql  string
		want []int64 // the COUNT(*) per surviving group, ordered by b
	}{
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING b ORDER BY b`, []int64{2}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING NOT b ORDER BY b`, []int64{2}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING COALESCE(b, FALSE) ORDER BY b`, []int64{2}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING COALESCE(b, TRUE) ORDER BY b`, []int64{2, 1}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING NOT COALESCE(b, FALSE) ORDER BY b`, []int64{2, 1}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING b IS UNKNOWN ORDER BY b`, []int64{1}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING COALESCE(b, FALSE) = TRUE ORDER BY b`, []int64{2}},
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING CAST(b AS BOOLEAN) ORDER BY b`, []int64{2}},
		// An aggregate inside a scalar call still resolves to the aggregate's
		// output column: the walk into a non-aggregate's ARGUMENTS is what
		// keeps that working.
		{`SELECT b, COUNT(*) AS n FROM cb GROUP BY b HAVING ABS(COUNT(*)) > 1 ORDER BY b`, []int64{2, 2}},
	}
	for _, c := range cases {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("query error: %v\n  SQL: %s", err, c.sql)
			continue
		}
		got := make([]int64, 0, len(res.Rows))
		for _, r := range res.Rows {
			n, ok := r["n"].(int64)
			if !ok {
				t.Fatalf("n came back as %T\n  SQL: %s", r["n"], c.sql)
			}
			got = append(got, n)
		}
		if !cbSame(got, c.want) {
			t.Errorf("HAVING answered group counts %v, want %v (PostgreSQL 17)\n  SQL: %s", got, c.want, c.sql)
		}
	}
}

// contains is strings.Contains, spelled locally so this file needs no import
// for one call.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// quoteSQL renders s as a SQL string literal.
func quoteSQL(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	return string(append(out, '\''))
}
