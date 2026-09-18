// SPDX-License-Identifier: MIT

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

// The round-1 review's B1, with its values.
//
// This grammar rewrites the SQL-standard TRIM spellings into a TWO-argument
// call — `TRIM(BOTH c FROM s)` is `trim(s, c)` — and the arity table declared
// those three names as taking one argument, so every such statement was
// `42883 function trim(text, unknown) does not exist`. Three of the same cells
// were WRONG before that in the other direction: the second argument was read
// and then dropped, so `LTRIM('007','0')` answered `007` where PostgreSQL
// answers `7`.
//
// PostgreSQL's second argument is a SET of characters, not a prefix —
// `TRIM(BOTH 'ab' FROM 'baXab')` is `X` and `TRIM('xyx','xy')` is the empty
// string, measured on 17.11 — which is what `btrim`/`ltrim`/`rtrim` take
// there. Every expectation below is that server's answer.
func TestTheGrammarsTrimSpellingsAnswerPostgresValues(t *testing.T) {
	ctx := context.Background()
	db := grtOpen(t)

	for _, c := range []struct {
		name, expr string
		want       any
		pg         string
	}{
		// The whitespace spellings: `s` is `'  padded  '`.
		{"trim_plain", `TRIM(s)`, "padded", `padded`},
		{"trim_both_from", `TRIM(BOTH ' ' FROM s)`, "padded", `padded`},
		{"trim_leading_from", `TRIM(LEADING ' ' FROM s)`, "padded  ", `'padded  '`},
		{"trim_trailing_from", `TRIM(TRAILING ' ' FROM s)`, "  padded", `'  padded'`},
		{"trim_both_no_char", `TRIM(BOTH FROM s)`, "padded", `padded`},
		{"trim_leading_no_char", `TRIM(LEADING FROM s)`, "padded  ", `'padded  '`},
		{"trim_trailing_no_char", `TRIM(TRAILING FROM s)`, "  padded", `'  padded'`},
		{"trim_two_args", `TRIM(s, ' ')`, "padded", `padded`},
		{"ltrim_one_arg", `LTRIM(s)`, "padded  ", `'padded  '`},
		{"rtrim_one_arg", `RTRIM(s)`, "  padded", `'  padded'`},
		// The CUTSET spellings: `z` is `'007'`. These are the three the engine
		// answered WRONG before this arc — the cutset was read and dropped.
		{"trim_leading_zero", `TRIM(LEADING '0' FROM z)`, "7", `7`},
		{"trim_trailing_zero", `TRIM(TRAILING '0' FROM z)`, "007", `007`},
		{"trim_both_zero", `TRIM(BOTH '0' FROM z)`, "7", `7`},
		{"ltrim_cutset", `LTRIM(z, '0')`, "7", `7`},
		{"rtrim_cutset", `RTRIM(z, '0')`, "007", `007`},
		{"trim_cutset", `TRIM(z, '0')`, "7", `7`},
		// The cutset is a SET, not a prefix — the cell that separates the two
		// readings, and the one a "strip this prefix" implementation fails.
		{"cutset_is_a_set", `TRIM(BOTH 'ab' FROM w)`, "X", `X — 'baXab' with the SET {a,b} trimmed`},
		{"cutset_consumes_everything", `TRIM(y, 'xy')`, "", `the empty string`},
		{"cutset_empty_trims_nothing", `LTRIM(z, '')`, "007", `007`},
		// A NULL on either side is NULL, as it is on the server.
		{"null_source", `TRIM(BOTH ' ' FROM n)`, nil, `NULL`},
		{"null_cutset", `TRIM(z, n)`, nil, `NULL`},
		// A COLUMN cutset takes the vectorized kernel's per-row path, which is
		// the arm a constant cutset does not reach.
		{"column_cutset", `TRIM(z, z)`, "", `TRIM('007','007') is the empty string`},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT %s AS v FROM grt WHERE k = 1`, c.expr)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%v — PostgreSQL 17.11 answers %s\n  SQL: %s", err, c.pg, sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1\n  SQL: %s", len(res.Rows), sql)
			}
			if got := res.Rows[0]["v"]; got != c.want {
				t.Errorf("= %#v, want %#v (PostgreSQL 17.11: %s)\n  SQL: %s", got, c.want, c.pg, sql)
			}
		})
	}

	// The OTHER rewrites in the same family, so the fix for one spelling is
	// not the only thing holding the table honest. Each is a call the query
	// did not write as one.
	for _, c := range []struct {
		name, expr string
		want       any
		pg         string
	}{
		{"position_in", `POSITION('d' IN s)`, int32(5), `5 — one-based, over '  padded  '`},
		{"extract_year", `EXTRACT(YEAR FROM ts)`, 2024.0, `2024`},
		{"extract_month", `EXTRACT(MONTH FROM ts)`, 1.0, `1`},
		{"extract_dow", `EXTRACT(DOW FROM ts)`, 1.0, `1`},
		{"ilike", `CASE WHEN s ILIKE '%PADDED%' THEN 1 ELSE 0 END`, int32(1), `true`},
		{"similar_to", `CASE WHEN z SIMILAR TO '0+7' THEN 1 ELSE 0 END`, int32(1), `true`},
	} {
		t.Run("family/"+c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT %s AS v FROM grt WHERE k = 1`, c.expr)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%v — PostgreSQL 17.11 answers %s\n  SQL: %s", err, c.pg, sql)
			}
			if got := res.Rows[0]["v"]; got != c.want {
				t.Errorf("= %#v, want %#v (PostgreSQL 17.11: %s)\n  SQL: %s", got, c.want, c.pg, sql)
			}
		})
	}

	// And the arity refusal is STILL there for a count the rewrite cannot
	// produce, so widening the three rows did not widen them to everything.
	for _, sql := range []string{
		`SELECT TRIM('a','b','c') AS v FROM grt WHERE k = 1`,
		`SELECT LTRIM('a','b','c') AS v FROM grt WHERE k = 1`,
		`SELECT RTRIM('a','b','c') AS v FROM grt WHERE k = 1`,
	} {
		t.Run("three_arguments_is_still_42883", func(t *testing.T) {
			_, err := db.Query(ctx, sql)
			if err == nil {
				t.Fatalf("answered; PostgreSQL 17.11 has no three-argument btrim either\n  SQL: %s", sql)
			}
			if got := sqlerr.StateOf(err); got != "42883" {
				t.Errorf("SQLSTATE %q, want 42883\n  err: %v\n  SQL: %s", got, err, sql)
			}
		})
	}
}

func grtOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "z", Type: parquet.TypeString, Nullable: true},
		{Name: "w", Type: parquet.TypeString, Nullable: true},
		{Name: "y", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeString, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "grt", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("grt", sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{
		"k": int64(1), "s": "  padded  ", "z": "007", "w": "baXab", "y": "xyx",
		"n": nil, "ts": "2024-01-15 10:30:00",
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
