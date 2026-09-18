// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1141: a CAST to an integer type reads its operand with the cast the
// OPERAND'S DECLARATION selects, and PostgreSQL has two of them.
//
//	'2.5'::integer        22P02 — int4in has no fractional part
//	numeric_col::integer  3     — numeric→int rounds half away from zero
//	float_col::integer    2     — float8→int rounds half to EVEN (rint)
//
// castDecimalToInt reads the BOX, and a text box and a DECIMAL box are the
// same Go string in this engine, so with it first every quoted fractional
// literal took the numeric cast's rounding: `CAST('2.5' AS INTEGER)` answered
// 3 where the server raises, and `INSERT … SELECT '2.5'` put that 3 at rest.
//
// Every expectation below is live PostgreSQL 17.11 over the same text, except
// the PORT and PROTOCOL rows — those two types are wadjet's own and the
// specification is arc NT's one-grammar rule (docs/data-types.md).
func TestACastToAnIntegerTypeReadsItsOperandsOwnGrammar(t *testing.T) {
	ctx := context.Background()
	db := ctiOpen(t)

	// The TEXT grammar, at every integer width. `want` is the value; `state`
	// the SQLSTATE when there is no value.
	var forms []string
	cells := []struct {
		form  string
		dest  string
		want  int64
		state string
		// pg is PostgreSQL 17.11's verbatim answer, recorded beside the cell
		// so the expectation is auditable without the server.
		pg string
	}{
		// FRACTIONAL TEXT — the defect. int4in reads a whole number or
		// nothing; there is no rounding on this side of the cast.
		{form: `'2.5'`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: "2.5"`},
		{form: `'2.0'`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: "2.0"`},
		{form: `'26.7'`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: "26.7"`},
		{form: `'-0.4'`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: "-0.4"`},
		{form: `'2.5'`, dest: "BIGINT", state: "22P02", pg: `22P02 invalid input syntax for type bigint: "2.5"`},
		{form: `'2.5'`, dest: "SMALLINT", state: "22P02", pg: `22P02 invalid input syntax for type smallint: "2.5"`},
		{form: `'2.5'`, dest: "INT32", state: "22P02", pg: `int4 under another name`},
		// INT64 is BIGINT's wadjet spelling, and it matched no label in the
		// cast's own switch until this arc: the projection allocated an INT64
		// vector while the cast answered the operand UNTOUCHED, so a string
		// reached the store and died on the #361 silent-write guard — a
		// message about a vector where the answer is 22P02. Found by
		// re-running arc NT's review probes at this arc's tip.
		{form: `'2.5'`, dest: "INT64", state: "22P02", pg: `bigint under another name`},
		{form: `'12'`, dest: "INT64", want: 12, pg: `12`},
		{form: `'9223372036854775808'`, dest: "INT64", state: "22003", pg: `22003, and the message says bigint`},
		// EXPONENT text. `'1e3'` is a float8 spelling, not an int4 one.
		{form: `'1e3'`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: "1e3"`},
		{form: `'1e3'`, dest: "BIGINT", state: "22P02", pg: `22P02 invalid input syntax for type bigint: "1e3"`},
		// WHOLE NUMBERS still convert, radix prefixes and underscores
		// included — PostgreSQL 16+ reads all of these (#634).
		{form: `'12'`, dest: "INTEGER", want: 12, pg: `12`},
		{form: `'+12'`, dest: "INTEGER", want: 12, pg: `12`},
		{form: `'-12'`, dest: "BIGINT", want: -12, pg: `-12`},
		{form: `' 12 '`, dest: "INTEGER", want: 12, pg: `12 — C isspace is trimmed at both ends`},
		{form: `'0x10'`, dest: "INTEGER", want: 16, pg: `16`},
		{form: `'0o17'`, dest: "INTEGER", want: 15, pg: `15`},
		{form: `'0b101'`, dest: "INTEGER", want: 5, pg: `5`},
		{form: `'1_000'`, dest: "INTEGER", want: 1000, pg: `1000`},
		{form: `'017'`, dest: "INTEGER", want: 17, pg: `17 — decimal, never octal`},
		// NON-NUMBERS and the edges.
		{form: `''`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: ""`},
		{form: `'  '`, dest: "INTEGER", state: "22P02", pg: `22P02 for the all-whitespace string`},
		{form: `'12abc'`, dest: "INTEGER", state: "22P02", pg: `22P02 invalid input syntax for type integer: "12abc"`},
		{form: `'abc'`, dest: "SMALLINT", state: "22P02", pg: `22P02 invalid input syntax for type smallint: "abc"`},
		// RANGE is the destination's own, and a different SQLSTATE.
		{form: `'2147483648'`, dest: "INTEGER", state: "22003", pg: `22003 value "2147483648" is out of range for type integer`},
		{form: `'2147483648'`, dest: "BIGINT", want: 2147483648, pg: `2147483648`},
		{form: `'99999'`, dest: "SMALLINT", state: "22003", pg: `22003 value "99999" is out of range for type smallint`},
		{form: `'9223372036854775808'`, dest: "BIGINT", state: "22003", pg: `22003 value "…" is out of range for type bigint`},
		// PORT and PROTOCOL read their OWN text form, which is narrower than
		// int4's and includes PROTOCOL's IANA name (arc NT, #986). Fractional
		// text is refused there for the same reason it is here.
		{form: `'2.5'`, dest: "PORT", state: "22P02", pg: `wadjet's own type: PORT reads decimal digits`},
		{form: `'443'`, dest: "PORT", want: 443, pg: `wadjet's own type`},
		{form: `'0x1bb'`, dest: "PORT", state: "22P02", pg: `wadjet's own type: PORT has no radix prefix`},
		{form: `'udp'`, dest: "PROTOCOL", want: 17, pg: `wadjet's own type: the IANA name`},
		{form: `'2.5'`, dest: "PROTOCOL", state: "22P02", pg: `wadjet's own type`},
	}
	for _, c := range cells {
		forms = append(forms, strings.Trim(c.form, "'"))
	}
	ctiLoadForms(t, ctx, db, forms)
	for i, c := range cells {
		t.Run(fmt.Sprintf("text/%s_as_%s", strings.Trim(c.form, "'"), c.dest), func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT CAST(%s AS %s) AS v FROM cti WHERE k = 1`, c.form, c.dest)
			ctiCheck(t, ctx, db, sql, c.want, c.state, c.pg)
			// The SAME text through a STRING COLUMN. PostgreSQL types a text
			// column's cast with the same input function it gives an unknown
			// literal, and this engine used to route the two differently
			// because it read the BOX: a STRING column and a DECIMAL column
			// arrive here as the same Go string, so only the DECLARATION can
			// separate them.
			col := fmt.Sprintf(`SELECT CAST(t AS %s) AS v FROM ctitext WHERE k = %d`, c.dest, i)
			ctiCheck(t, ctx, db, col, c.want, c.state, c.pg)
		})
	}

	// The OTHER cast, and the one the first must not steal: a value that is a
	// NUMBER already rounds, and the two numeric sources round DIFFERENTLY on
	// the same server (#768, #373).
	for _, c := range []struct {
		name, expr string
		want       int64
		pg         string
	}{
		{"decimal_column_rounds_half_away", `CAST(d AS INTEGER)`, 3, `numeric 2.5 -> 3`},
		{"decimal_column_rounds_half_away_negative", `CAST(dn AS INTEGER)`, -3, `numeric -2.5 -> -3`},
		{"decimal_column_rounds_half_away_35", `CAST(d35 AS INTEGER)`, 4, `numeric 3.5 -> 4`},
		{"float_column_rounds_half_to_even", `CAST(f AS INTEGER)`, 2, `float8 2.5 -> 2 (rint)`},
		{"float_column_rounds_half_to_even_35", `CAST(f35 AS INTEGER)`, 4, `float8 3.5 -> 4 (rint)`},
		{"numeric_literal_rounds_half_away", `CAST(2.5 AS INTEGER)`, 3, `CAST(2.5 AS int) -> 3`},
		{"numeric_literal_rounds_half_away_negative", `CAST(-2.5 AS INTEGER)`, -3, `CAST(-2.5 AS int) -> -3`},
		{"decimal_cast_of_text_then_int_rounds", `CAST(CAST('2.5' AS DECIMAL(4,1)) AS INTEGER)`, 3, `('2.5'::numeric)::int -> 3`},
		{"decimal_column_to_int64_rounds", `CAST(d AS INT64)`, 3, `numeric 2.5 -> 3 at bigint too`},
		{"numeric_literal_to_int64_rounds", `CAST(2.5 AS INT64)`, 3, `CAST(2.5 AS bigint) -> 3`},
		{"integer_column_passes_through", `CAST(i AS INTEGER)`, 7, `7`},
		// A DERIVED text value takes the TEXT cast, because its declaration is
		// text: PostgreSQL's `(s||'')::integer` and `substr(s,1,3)::integer`
		// are both 22P02 over '2.5'.
		{"text_function_result_is_text", `CAST(UPPER(s) AS INTEGER)`, 0, ``},
		{"cast_to_string_then_int_is_text", `CAST(CAST(d AS STRING) AS INTEGER)`, 0, ``},
	} {
		t.Run("number/"+c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT %s AS v FROM cti WHERE k = 1`, c.expr)
			if c.pg == "" {
				ctiCheck(t, ctx, db, sql, 0, "22P02", `22P02 — a derived TEXT value takes int4in, not the numeric cast`)
				return
			}
			ctiCheck(t, ctx, db, sql, c.want, "", c.pg)
		})
	}
}

// TestACastToAnIntegerTypeReadsTheSameGrammarAtEveryDoor is the same rule at
// the doors a value can ENTER an integer column through. A cast that answers 3
// in a SELECT and writes 3 through INSERT … SELECT while the writer's own text
// door refuses '2.5' is the split #1141 was filed for.
func TestACastToAnIntegerTypeReadsTheSameGrammarAtEveryDoor(t *testing.T) {
	ctx := context.Background()
	for _, door := range []struct {
		name string
		sql  func(form string) string
	}{
		{"select", func(f string) string { return fmt.Sprintf(`SELECT CAST(%s AS INTEGER) AS v FROM cti WHERE k = 1`, f) }},
		{"where", func(f string) string {
			return fmt.Sprintf(`SELECT k AS v FROM cti WHERE i = CAST(%s AS INTEGER)`, f)
		}},
		{"group-by", func(f string) string {
			return fmt.Sprintf(`SELECT CAST(%s AS INTEGER) AS v FROM cti GROUP BY CAST(%s AS INTEGER)`, f, f)
		}},
		{"order-by", func(f string) string {
			return fmt.Sprintf(`SELECT k AS v FROM cti ORDER BY CAST(%s AS INTEGER)`, f)
		}},
		{"having", func(f string) string {
			return fmt.Sprintf(`SELECT k AS v FROM cti GROUP BY k HAVING MAX(i) = CAST(%s AS INTEGER)`, f)
		}},
		{"insert-select", func(f string) string {
			return fmt.Sprintf(`INSERT INTO ctisink (v) SELECT CAST(%s AS INTEGER) FROM cti WHERE k = 1`, f)
		}},
		{"ctas", func(f string) string {
			return fmt.Sprintf(`CREATE TABLE ctictas AS SELECT CAST(%s AS INTEGER) AS v FROM cti WHERE k = 1`, f)
		}},
	} {
		t.Run(door.name, func(t *testing.T) {
			db := ctiOpen(t)
			if _, err := db.Query(ctx, `CREATE TABLE ctisink (v INT64)`); err != nil {
				t.Fatalf("create sink: %v", err)
			}
			sql := door.sql(`'2.5'`)
			var err error
			if strings.HasPrefix(sql, "INSERT") {
				_, err = db.Execute(ctx, sql)
			} else {
				_, err = db.Query(ctx, sql)
			}
			if err == nil {
				t.Fatalf("%s door ANSWERED CAST('2.5' AS INTEGER); PostgreSQL 17.11 raises "+
					"22P02 invalid input syntax for type integer: \"2.5\" in every position\n  SQL: %s",
					door.name, sql)
			}
			if got := sqlerr.StateOf(err); got != "22P02" {
				t.Errorf("%s door: SQLSTATE %q, want 22P02\n  err: %v\n  SQL: %s", door.name, got, err, sql)
			}
			// And the well-formed spelling still passes through the same door.
			ok := door.sql(`'12'`)
			if strings.HasPrefix(ok, "INSERT") {
				_, err = db.Execute(ctx, ok)
			} else if strings.HasPrefix(ok, "CREATE") {
				_, err = db.Query(ctx, strings.Replace(ok, "ctictas", "ctictas2", 1))
			} else {
				_, err = db.Query(ctx, ok)
			}
			if err != nil {
				t.Errorf("%s door refused CAST('12' AS INTEGER), which PostgreSQL answers 12: %v\n  SQL: %s",
					door.name, err, ok)
			}
		})
	}
}

// ctiLoadForms puts each form's TEXT into a STRING column, one row per form,
// so the same characters can be cast from a COLUMN as well as from a literal.
// The two must answer the same thing: both declare text, and PostgreSQL gives
// both int4in.
func ctiLoadForms(t *testing.T, ctx context.Context, db *DB, forms []string) {
	t.Helper()
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "t", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ctitext", sc, nil); err != nil {
		t.Fatalf("create ctitext: %v", err)
	}
	rows := make([]map[string]any, 0, len(forms))
	for i, f := range forms {
		rows = append(rows, map[string]any{"k": int64(i), "t": f})
	}
	ing := db.NewIngester("ctitext", sc, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest ctitext: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush ctitext: %v", err)
	}
}

func ctiCheck(t *testing.T, ctx context.Context, db *DB, sql string, want int64, state, pg string) {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if state != "" {
		if err == nil {
			t.Fatalf("answered %v; PostgreSQL 17.11 says %s\n  SQL: %s", res.Rows, pg, sql)
		}
		if got := sqlerr.StateOf(err); got != state {
			t.Errorf("SQLSTATE %q, want %q\n  err: %v\n  PostgreSQL 17.11: %s\n  SQL: %s", got, state, err, pg, sql)
		}
		return
	}
	if err != nil {
		t.Fatalf("%v\n  PostgreSQL 17.11 answers %s\n  SQL: %s", err, pg, sql)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%d rows, want 1\n  SQL: %s", len(res.Rows), sql)
	}
	switch got := res.Rows[0]["v"].(type) {
	case int64:
		if got != want {
			t.Errorf("= %d, want %d (PostgreSQL 17.11: %s)\n  SQL: %s", got, want, pg, sql)
		}
	case int32:
		if int64(got) != want {
			t.Errorf("= %d, want %d (PostgreSQL 17.11: %s)\n  SQL: %s", got, want, pg, sql)
		}
	default:
		t.Errorf("= %#v (%T), want %d (PostgreSQL 17.11: %s)\n  SQL: %s",
			res.Rows[0]["v"], res.Rows[0]["v"], want, pg, sql)
	}
}

func ctiOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "dn", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "d35", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "f35", Type: parquet.TypeFloat64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "cti", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("cti", sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{
		"k": int64(1), "i": int64(7), "s": "2.5",
		"d": "2.50", "dn": "-2.50", "d35": "3.50", "f": 2.5, "f35": 3.5,
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
