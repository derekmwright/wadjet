// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInsertValuesAcceptsExpressions is #1252's own repro plus the
// coverage table the issue's fix asks for: a bare literal, a typed literal,
// a CAST, arithmetic, now()/CURRENT_DATE, NULL, DEFAULT, a string into
// DATE/TIMESTAMP/INT/DECIMAL/IPv4/UUID, a type MISMATCH (42804), a column
// reference (42703) and a subquery (0A000) — every VALUES cell shape,
// evaluated through the SAME expression compiler SELECT uses
// (assignInsertValue in dml.go, no second evaluator).
//
// PostgreSQL 17.11, measured live (postgres:17.11-alpine), is the authority
// for every SQLSTATE here (ADR-0012): a bare unquoted name in a VALUES cell
// is an identifier PostgreSQL has no FROM to resolve — 42703, never a
// malformed literal — and a scalar subquery is a constant PostgreSQL DOES
// accept in VALUES; this engine has no query environment to run one against
// at this seam and says so with 0A000 rather than silently answering NULL.
func TestInsertValuesAcceptsExpressions(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "b", Type: parquet.TypeBool, Nullable: true},
		{Name: "arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
	}}
	if err := db.CreateTable(ctx, "cov", schema, nil); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		sql   string
		state string // "" = must succeed
	}{
		{"bare literal", `INSERT INTO cov (id, n) VALUES (1, 5)`, ""},
		{"typed literal timestamp", `INSERT INTO cov (id, ts) VALUES (2, TIMESTAMP '2026-01-01 00:00:00')`, ""},
		{"typed literal date", `INSERT INTO cov (id, d) VALUES (3, DATE '2026-01-01')`, ""},
		{"cast", `INSERT INTO cov (id, n) VALUES (4, CAST('7' AS BIGINT))`, ""},
		{"arithmetic", `INSERT INTO cov (id, n) VALUES (5, 2 + 3)`, ""},
		{"now", `INSERT INTO cov (id, ts) VALUES (6, now())`, ""},
		{"current_date", `INSERT INTO cov (id, d) VALUES (7, CURRENT_DATE)`, ""},
		{"null", `INSERT INTO cov (id, n) VALUES (8, NULL)`, ""},
		{"default", `INSERT INTO cov (id, n) VALUES (9, DEFAULT)`, ""},
		{"string into date", `INSERT INTO cov (id, d) VALUES (10, '2026-02-02')`, ""},
		{"string into timestamp", `INSERT INTO cov (id, ts) VALUES (11, '2026-02-02 03:04:05')`, ""},
		{"string into int", `INSERT INTO cov (id, n) VALUES (12, '42')`, ""},
		{"string into decimal", `INSERT INTO cov (id, dec) VALUES (13, '12.50')`, ""},
		{"string into ipv4", `INSERT INTO cov (id, ip) VALUES (14, '10.0.0.1')`, ""},
		{"string into uuid", `INSERT INTO cov (id, u) VALUES (15, '11111111-1111-1111-1111-111111111111')`, ""},
		{"cast into date", `INSERT INTO cov (id, d) VALUES (16, CAST('2026-03-03' AS DATE))`, ""},
		{"cast into timestamp", `INSERT INTO cov (id, ts) VALUES (17, CAST('2026-03-03 01:02:03' AS TIMESTAMP))`, ""},
		{"mismatch: bool literal into an integer column", `INSERT INTO cov (id, n) VALUES (18, TRUE)`, "42804"},
		{"a column reference — VALUES has no FROM", `INSERT INTO cov (id, n) VALUES (19, id)`, "42703"},
		{"a subquery — accepted by PostgreSQL, refused here", `INSERT INTO cov (id, n) VALUES (20, (SELECT 1))`, "0A000"},
		// B4 (round-2 review): the bracket-depth revert cell the notes
		// claimed had no test. Before the `[`/`]` depth-tracking fix,
		// parseValuesRow's top-level-comma split did not track `[`/`]` at
		// all, so the comma INSIDE the ARRAY constructor was read as the
		// row's own value separator and this single-value row mis-split
		// into 3 raw values for 1 target column (42601 arity mismatch, or —
		// see TestInsertValuesArrayBracketWrongSplitStillRefuses below —
		// silent garbage on a row shaped to make the wrong split's COUNT
		// coincidentally match).
		{"array literal — the bracket-depth fix", `INSERT INTO cov (id, arr) VALUES (21, ARRAY[1, 2, 3])`, ""},
		// P2 (round-2 review): a computed INTEGER expression into BOOL,
		// IPv4 or UUID answered with no SQLSTATE at all — `assignEvaluatedValue`
		// had no BOOL arm and let a non-string box through the network/UUID
		// arm unchecked, reaching ingest.checkType's raw "expected bool, got
		// int64" / "expected string, got int64". PostgreSQL: 42804 for all
		// three, the same declared-source-type table B1's DATE/TIMESTAMP
		// cells use.
		{"P2: integer expression into BOOL", `INSERT INTO cov (id, b) VALUES (22, 1 + 0)`, "42804"},
		{"P2: integer expression into IPv4", `INSERT INTO cov (id, ip) VALUES (23, 1 + 1)`, "42804"},
		{"P2: integer expression into UUID", `INSERT INTO cov (id, u) VALUES (24, 1 + 1)`, "42804"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Execute(ctx, tc.sql)
			if tc.state == "" {
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("%s: want SQLSTATE %s, got success", tc.sql, tc.state)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Fatalf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
			}
		})
	}
}

// TestInsertValuesBoolLiteralMismatchMatchesPostgres pins the exact
// SENTENCE, not only the SQLSTATE — PostgreSQL's 42804 names the column's
// OWN type ("bigint"), not this engine's internal TypeID spelling
// ("INT64"), which datatypeMismatch (dml.go) used to print before this fix
// routed it through physical.PgTypeName.
func TestInsertValuesBoolLiteralMismatchMatchesPostgres(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "ev", schema, nil); err != nil {
		t.Fatal(err)
	}
	_, err = db.Execute(ctx, "INSERT INTO ev VALUES (TRUE)")
	if err == nil {
		t.Fatal("VALUES (TRUE) into a bigint column: want an error, got none")
	}
	want := `column "n" is of type bigint but expression is of type boolean`
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
	if got := sqlerr.StateOf(err); got != "42804" {
		t.Errorf("SQLSTATE %q, want 42804", got)
	}
}

// TestInsertValuesDefaultIsNullOnEveryColumn is #1252's DEFAULT cell: no
// column this catalog describes ever carries an explicit default
// (parquet.Column has no such field), so DEFAULT resolves to NULL uniformly
// — PostgreSQL's own rule for a column with none — across every type this
// engine has, not only the ones the coverage table above exercises.
func TestInsertValuesDefaultIsNullOnEveryColumn(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "b", Type: parquet.TypeBool, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 5, Scale: 1, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "alld", schema, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO alld VALUES (DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)"); err != nil {
		t.Fatalf("VALUES (DEFAULT, ...): %v", err)
	}
	res, err := db.Query(ctx, "SELECT b, n, f, s, ts, d, dec FROM alld")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	for col, v := range res.Rows[0] {
		if v != nil {
			t.Errorf("column %q = %#v, want NULL", col, v)
		}
	}
}

// TestInsertValuesArrayBracketWrongSplitStillRefuses is round-2 review B4's
// attempt at the discriminating fixture the original notes' filing candidate
// 4 asked for: a row shaped so the PRE-FIX split bug's WRONG value count
// would equal the target column count, so the arity check alone could not
// catch it and the row would store a plausible-looking but WRONG value
// instead of refusing.
//
// `ARRAY[1,2][1]` has exactly one comma inside its brackets, so — measured
// by hand in a scratch copy of this tip with ONLY the `[`/`]` depth-tracking
// arms of parseValuesRow reverted — the pre-fix split DOES turn this row's
// 2 real values (`ARRAY[1,2][1]`, `5`) into 3 raw pieces ("ARRAY [ 1",
// "2 ] [ 1 ]", "5"), matching the 3-column target (id, a, b) by coincidence
// and slipping past the arity check exactly as the filing candidate
// predicted. But it does NOT silently store garbage: every fragment a
// bracket-internal split produces carries an UNBALANCED bracket (one side
// opens without closing, the other closes without opening), and
// `plansql.ParseExpressionComplete` refuses that fragment as its own syntax
// error (42601) before it ever reaches a target column — reverted, this row
// fails with "row 0, column \"id\": parsing \"ARRAY [ 1\": ..." instead of
// the arity message below, but it still fails. This is a STRUCTURAL
// property of the grammar (brackets are used ONLY for an ARRAY literal or a
// subscript, both requiring balance), not a property of this one fixture:
// the "coincidentally-matching-count, plausible-wrong-value" shape the
// filing candidate worried about does not appear to be reachable through
// `[`/`]` in this grammar. Kept as a regression test for the CORRECT arity
// refusal at this tip; the SQLSTATE is 42601 either way, so it does not
// discriminate reverted-vs-fixed on its own — TestInsertValuesAcceptsExpressions's
// "array literal" case (the single-value ARRAY[1, 2, 3] row) is the cell
// that actually fails when the depth-tracking arms are reverted.
func TestInsertValuesArrayBracketWrongSplitStillRefuses(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "vab", schema, nil); err != nil {
		t.Fatal(err)
	}
	_, err = db.Execute(ctx, "INSERT INTO vab (id, a, b) VALUES (ARRAY[1,2][1], 5)")
	if err == nil {
		t.Fatal("2 values for 3 columns (id, a, b) succeeded; " +
			"want a 42601 arity refusal, not a silent 3-way mis-split")
	}
	if got := sqlerr.StateOf(err); got != "42601" {
		t.Errorf("SQLSTATE %q, want 42601 (err: %v)", got, err)
	}
	res, qerr := db.Query(ctx, "SELECT id, a, b FROM vab")
	if qerr != nil {
		t.Fatal(qerr)
	}
	if len(res.Rows) != 0 {
		t.Errorf("row was stored despite the refusal: %#v", res.Rows)
	}
}
