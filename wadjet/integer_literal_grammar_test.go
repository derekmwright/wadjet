package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// PostgreSQL's integer literal grammar, in the LEXER and in the CAST door
// (#634), and its error messages byte for byte (#638).
//
// PostgreSQL 16+ reads `0x`/`0o`/`0b` radix prefixes and `_` digit separators
// in both places, and reads a leading zero as DECIMAL — `'017'` is seventeen,
// not fifteen. Measured live on postgres:17-alpine:
//
//	SELECT 0x1A, 0o17, 0b101, 1_000, 007   →  26, 15, 5, 1000, 7
//	'0x1A'::integer 26   '1_000'::integer 1000   '017'::integer 17
//	'0x80000000'::integer  22003   '_100' '100_' '1__0' '0x'  22P02
//
// The comparison kernels already read it (the parser landed with #536's
// follow-up); the LEXER refused every radix form as a syntax error and the
// CAST door read base 10 only.
func TestIntegerLiteralGrammar(t *testing.T) {
	ctx := context.Background()
	db := intLitDB(t, ctx)
	for _, tc := range []struct {
		name  string
		sql   string
		rows  []string
		state string
		msg   string
	}{
		// The LEXER: an unquoted literal.
		{name: "hex", sql: "SELECT 0x1A AS v", rows: []string{"[26]"}},
		{name: "hex upper prefix", sql: "SELECT 0X1a AS v", rows: []string{"[26]"}},
		{name: "octal", sql: "SELECT 0o17 AS v", rows: []string{"[15]"}},
		{name: "binary", sql: "SELECT 0b101 AS v", rows: []string{"[5]"}},
		{name: "underscore", sql: "SELECT 1_000 AS v", rows: []string{"[1000]"}},
		{name: "underscore repeated", sql: "SELECT 1_0_0 AS v", rows: []string{"[100]"}},
		{name: "underscore after a radix prefix", sql: "SELECT 0x_1A AS v", rows: []string{"[26]"}},
		{name: "leading zero is DECIMAL", sql: "SELECT 017 AS v", rows: []string{"[17]"}},
		{name: "hex in arithmetic", sql: "SELECT 0x1A + 1 AS v", rows: []string{"[27]"}},
		{name: "hex in a predicate", sql: "SELECT id FROM lit WHERE k = 0x1A", rows: []string{"[1]"}},
		{name: "underscores in a decimal literal", sql: "SELECT 1_000.5 AS v", rows: []string{"[1000.5]"}},
		// A radix literal has no width of its own. PostgreSQL promotes one
		// past bigint to `numeric` and ANSWERS it (measured: `\gdesc` says
		// numeric for 0x8000000000000000 and bigint for 0x7fffffffffffffff),
		// so refusing the overflow would reject input PostgreSQL accepts —
		// which is what this lexer did before it rendered the value through
		// math/big. Past bigint the two spellings are compared to EACH OTHER
		// instead (TestARadixLiteralMeansItsDecimalSpelling), because this
		// engine promotes any literal past int64 to float64 and that is a
		// numeric question, not an identifier one.
		{name: "at the bigint boundary", sql: "SELECT 0x7fffffffffffffff AS v",
			rows: []string{"[9223372036854775807]"}},
		// The other side of the same boundary: the CAST door reads the value
		// INTO a type, so there the overflow IS 22003, as PostgreSQL says.
		{name: "the CAST door still refuses the overflow",
			sql:   "SELECT CAST('0x8000000000000000' AS BIGINT) AS v",
			state: "22003", msg: `value "0x8000000000000000" is out of range for type bigint`},
		// PostgreSQL refuses a numeric literal that runs into an identifier
		// character — 42601 `trailing junk after numeric literal`. This lexer
		// stopped the number at the underscore and handed the rest to the
		// parser as an implicit ALIAS, so `SELECT 100_` answered 100 under a
		// column called `_`, and `1__0` under `__0` — the shape of the
		// reserved hidden-slot namespace.
		{name: "a trailing underscore is junk, not an alias", sql: "SELECT 100_",
			state: "42601", msg: "trailing junk after numeric literal"},
		{name: "two underscores together are junk", sql: "SELECT 1__0",
			state: "42601", msg: "trailing junk after numeric literal"},
		{name: "a separator with nothing after it", sql: "SELECT 1_000_",
			state: "42601", msg: "trailing junk after numeric literal"},
		{name: "ctl: a real alias with no space", sql: "SELECT 1 x", rows: []string{"[1]"}},
		{name: "ctl: a real alias with AS", sql: "SELECT 100 AS x", rows: []string{"[100]"}},

		// The CAST door: a quoted literal read as an integer.
		{name: "cast hex", sql: "SELECT CAST('0x1A' AS BIGINT) AS v", rows: []string{"[26]"}},
		{name: "cast octal", sql: "SELECT CAST('0o17' AS BIGINT) AS v", rows: []string{"[15]"}},
		{name: "cast binary", sql: "SELECT CAST('0b101' AS BIGINT) AS v", rows: []string{"[5]"}},
		{name: "cast underscore", sql: "SELECT CAST('1_000' AS BIGINT) AS v", rows: []string{"[1000]"}},
		{name: "cast leading zero", sql: "SELECT CAST('017' AS BIGINT) AS v", rows: []string{"[17]"}},
		{name: "cast signed hex", sql: "SELECT CAST('-0x1A' AS BIGINT) AS v", rows: []string{"[-26]"}},
		{name: "cast a fraction still ROUNDS", sql: "SELECT CAST('26.7' AS BIGINT) AS v",
			rows: []string{"[27]"}},
		{name: "cast a non-number", sql: "SELECT CAST('abc' AS BIGINT) AS v",
			state: "22P02", msg: `invalid input syntax for type bigint: "abc"`},

		// The PREDICATE door, which already read the grammar — controls that
		// it still does, and the 22003/22P02 split.
		{name: "predicate hex", sql: "SELECT id FROM lit WHERE k = '0x1A'", rows: []string{"[1]"}},
		{name: "predicate underscore", sql: "SELECT id FROM lit WHERE k = '1_000'", rows: []string{"[4]"}},
		{name: "predicate whitespace-padded", sql: "SELECT id FROM lit WHERE k = ' 0x1a '",
			rows: []string{"[1]"}},
		{name: "predicate IN list", sql: "SELECT id FROM lit WHERE k IN ('0x1A', '0o17') ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "an underscore may not be first", sql: "SELECT id FROM lit WHERE k = '_100'",
			state: "22P02", msg: `invalid input syntax for type bigint: "_100"`},
		{name: "an underscore may not be last", sql: "SELECT id FROM lit WHERE k = '100_'",
			state: "22P02", msg: `invalid input syntax for type bigint: "100_"`},
		{name: "two underscores together", sql: "SELECT id FROM lit WHERE k = '1__0'",
			state: "22P02", msg: `invalid input syntax for type bigint: "1__0"`},
		{name: "a radix prefix with no digits", sql: "SELECT id FROM lit WHERE k = '0x'",
			state: "22P02", msg: `invalid input syntax for type bigint: "0x"`},
		{name: "out of range for the narrower type", sql: "SELECT id FROM lit WHERE j = '0x80000000'",
			state: "22003", msg: `value "0x80000000" is out of range for type integer`},

		// #638: the message carries the offending text BYTE for byte, the way
		// PostgreSQL writes it. Go's %q escaped every non-ASCII byte, so a
		// literal holding a NBSP came out as " 42" — text nobody typed.
		{name: "a non-ASCII byte is not escaped",
			sql:   "SELECT id FROM lit WHERE k = '\u00a042'",
			state: "22P02", msg: "invalid input syntax for type bigint: \"\u00a042\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if tc.state != "" {
				if err == nil {
					t.Fatalf("%s: answered %d rows; want SQLSTATE %s", tc.sql, len(res.Rows), tc.state)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Fatalf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
				if tc.msg != "" && !containsSub(err.Error(), tc.msg) {
					t.Errorf("%s: message %q does not carry PostgreSQL's %q", tc.sql, err, tc.msg)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if len(res.Rows) != len(tc.rows) {
				t.Fatalf("%d rows, want %d\n  SQL: %s", len(res.Rows), len(tc.rows), tc.sql)
			}
			for i, w := range tc.rows {
				if got := fmt.Sprint(res.Cells(i)); got != w {
					t.Errorf("row %d = %s, want %s\n  SQL: %s", i, got, w, tc.sql)
				}
			}
		})
	}
}

// A radix literal past bigint means what its DECIMAL spelling means.
//
// PostgreSQL answers the exact number under `numeric` for both spellings.
// This engine promotes a literal past int64 to float64 for BOTH — a residual
// of its own (a numeric question, not an identifier one), and the property
// #634 owns is that the two spellings cannot disagree: rendering the radix
// value through math/big is what makes `0x8000000000000000` and
// `9223372036854775808` one number instead of one number and one 42601.
func TestARadixLiteralMeansItsDecimalSpelling(t *testing.T) {
	ctx := context.Background()
	db := intLitDB(t, ctx)
	for _, tc := range []struct{ radix, decimal string }{
		{"SELECT 0x7fffffffffffffff AS v", "SELECT 9223372036854775807 AS v"},
		{"SELECT 0x8000000000000000 AS v", "SELECT 9223372036854775808 AS v"},
		{"SELECT 0xffffffffffffffffff AS v", "SELECT 4722366482869645213695 AS v"},
		{"SELECT 0b1111111111111111111111111111111111111111111111111111111111111111 AS v",
			"SELECT 18446744073709551615 AS v"},
		{"SELECT 0o777777777777777777777 AS v", "SELECT 9223372036854775807 AS v"},
	} {
		t.Run(tc.radix, func(t *testing.T) {
			a, err := db.Query(ctx, tc.radix)
			if err != nil {
				t.Fatalf("%s: %v", tc.radix, err)
			}
			b, err := db.Query(ctx, tc.decimal)
			if err != nil {
				t.Fatalf("%s: %v", tc.decimal, err)
			}
			got, want := fmt.Sprint(a.Cells(0)), fmt.Sprint(b.Cells(0))
			if got != want {
				t.Errorf("%s answered %s; the same number written in decimal answers %s",
					tc.radix, got, want)
			}
		})
	}
}

func intLitDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE lit (id INT64, k INT64, j INT32)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx,
		"INSERT INTO lit VALUES (1,26,26),(2,15,15),(3,5,5),(4,1000,1000),(5,7,7),(6,17,17)"); err != nil {
		t.Fatal(err)
	}
	return db
}
