package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A non-boolean expression is not a predicate (#599).
//
// PostgreSQL 17, measured live over `cb(id bigint, c bigint, s varchar,
// f double precision, n numeric, b boolean)`: every non-boolean argument to
// WHERE / NOT / AND / OR / HAVING / JOIN-ON / CASE-WHEN is SQLSTATE 42804,
// with the SITE and the TYPE in the message. An UNKNOWN-typed literal is not
// a type error there — it is coerced through the boolean input function, so
// `WHERE 'true'` answers every row, `WHERE NULL` answers none, and
// `WHERE 'abc'` is 22P02.
//
// Wadjet had no type check at all, and its two truth-value collapses did not
// agree: `WHERE c` was 0 rows (a failed `v.(bool)` assertion read as FALSE)
// and `WHERE NOT c` was the row holding 0 (C truthiness). Not complements of
// each other under any reading, and both silent.
func TestNonBooleanPredicateIsATypeError(t *testing.T) {
	ctx := context.Background()
	db := boolCtxDB(t, ctx)
	for _, tc := range []struct {
		name  string
		sql   string
		state string
		msg   string
		rows  []string
	}{
		{name: "a bigint column in WHERE", sql: "SELECT id FROM cb WHERE c",
			state: "42804", msg: "argument of WHERE must be type boolean, not type bigint"},
		{name: "the same under NOT", sql: "SELECT id FROM cb WHERE NOT c",
			state: "42804", msg: "argument of NOT must be type boolean, not type bigint"},
		{name: "an integer literal", sql: "SELECT id FROM cb WHERE 1",
			state: "42804", msg: "argument of WHERE must be type boolean, not type integer"},
		{name: "a decimal literal", sql: "SELECT id FROM cb WHERE 1.5",
			state: "42804", msg: "argument of WHERE must be type boolean, not type numeric"},
		{name: "a varchar column", sql: "SELECT id FROM cb WHERE s",
			state: "42804", msg: "argument of WHERE must be type boolean, not type character varying"},
		{name: "a double column", sql: "SELECT id FROM cb WHERE f",
			state: "42804", msg: "argument of WHERE must be type boolean, not type double precision"},
		{name: "a numeric column", sql: "SELECT id FROM cb WHERE n",
			state: "42804", msg: "argument of WHERE must be type boolean, not type numeric"},
		{name: "arithmetic over one", sql: "SELECT id FROM cb WHERE c + 1",
			state: "42804", msg: "argument of WHERE must be type boolean, not type bigint"},
		{name: "under AND", sql: "SELECT id FROM cb WHERE c AND b",
			state: "42804", msg: "argument of AND must be type boolean, not type bigint"},
		{name: "under OR", sql: "SELECT id FROM cb WHERE b OR c",
			state: "42804", msg: "argument of OR must be type boolean, not type bigint"},
		{name: "in HAVING", sql: "SELECT id FROM cb GROUP BY id HAVING COUNT(*)",
			state: "42804", msg: "argument of HAVING must be type boolean, not type bigint"},
		{name: "in a JOIN condition", sql: "SELECT a.id FROM cb a JOIN cb z ON a.c",
			state: "42804", msg: "argument of JOIN/ON must be type boolean, not type bigint"},
		{name: "in a searched CASE's WHEN", sql: "SELECT CASE WHEN 1 THEN 'x' END AS c FROM cb",
			state: "42804", msg: "argument of CASE/WHEN must be type boolean, not type integer"},
		{name: "in a searched CASE's WHEN inside WHERE",
			sql:   "SELECT id FROM cb WHERE CASE WHEN c THEN true ELSE false END",
			state: "42804", msg: "argument of CASE/WHEN must be type boolean, not type bigint"},
		{name: "a string naming no boolean", sql: "SELECT id FROM cb WHERE 'abc'",
			state: "22P02", msg: `invalid input syntax for type boolean: "abc"`},

		// The UNKNOWN-typed literal half: PostgreSQL coerces, so these ANSWER.
		{name: "the literal true", sql: "SELECT id FROM cb WHERE 'true' ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "a prefix of true", sql: "SELECT id FROM cb WHERE 'tr' ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "yes", sql: "SELECT id FROM cb WHERE 'yes' ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "on", sql: "SELECT id FROM cb WHERE 'on' ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "the digit one, whitespace-padded", sql: "SELECT id FROM cb WHERE ' 1 ' ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "false", sql: "SELECT id FROM cb WHERE 'false'", rows: nil},
		{name: "NULL", sql: "SELECT id FROM cb WHERE NULL", rows: nil},

		// The BOUNDARY: everything that IS boolean keeps working.
		{name: "ctl: a bool column", sql: "SELECT id FROM cb WHERE b ORDER BY id",
			rows: []string{"[2]"}},
		{name: "ctl: NOT a bool column", sql: "SELECT id FROM cb WHERE NOT b ORDER BY id",
			rows: []string{"[1]"}},
		{name: "ctl: a comparison", sql: "SELECT id FROM cb WHERE c > 0 ORDER BY id",
			rows: []string{"[2]"}},
		// CAST(<bigint> AS BOOLEAN) is 42846 in PostgreSQL and answers here —
		// ADR-0012's recorded #592 divergence, an extension of the int4 rule
		// to the whole integer family. It is a control for THIS gate: a CAST
		// produces a real boolean, so the boolean-context check must not
		// refuse it.
		{name: "ctl: a CAST to boolean", sql: "SELECT id FROM cb WHERE CAST(c AS BOOLEAN) ORDER BY id",
			rows: []string{"[2]"}},
		{name: "ctl: IS NULL", sql: "SELECT id FROM cb WHERE c IS NOT NULL ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "ctl: IN", sql: "SELECT id FROM cb WHERE c IN (0, 1) ORDER BY id",
			rows: []string{"[1]", "[2]"}},
		{name: "ctl: a simple CASE's WHEN is a VALUE, not a predicate",
			sql:  "SELECT CASE c WHEN 1 THEN 'x' ELSE 'y' END AS r FROM cb ORDER BY 1",
			rows: []string{"[x]", "[y]"}},
		{name: "ctl: a searched CASE over a comparison",
			sql:  "SELECT CASE WHEN c > 0 THEN 'x' ELSE 'y' END AS r FROM cb ORDER BY 1",
			rows: []string{"[x]", "[y]"}},
		{name: "ctl: a bool column in a JOIN condition",
			sql:  "SELECT a.id FROM cb a JOIN cb z ON a.b = z.b ORDER BY 1",
			rows: []string{"[1]", "[2]"}},
		{name: "ctl: a real HAVING", sql: "SELECT id FROM cb GROUP BY id HAVING COUNT(*) > 0 ORDER BY id",
			rows: []string{"[1]", "[2]"}},
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

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func boolCtxDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx,
		"CREATE TABLE cb (id INT64, c INT64, s STRING, f FLOAT64, n DECIMAL(9,2), b BOOL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx,
		"INSERT INTO cb VALUES (1, 0, 'false', 0, 0, false), (2, 1, 'true', 1, 1, true)"); err != nil {
		t.Fatal(err)
	}
	return db
}
