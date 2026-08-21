package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestValuesListInFromEngine is the #374 engine-level regression for VALUES
// as a table source: FROM (VALUES ...) desugars to a UNION ALL of SELECTs
// (internal/planner/sql), and this proves the desugared form actually
// executes and answers correctly, not just parses.
func TestValuesListInFromEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	res, err := db.Query(ctx, `SELECT v FROM (VALUES ('b'), ('a'), ('c')) AS t(v) ORDER BY v`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("got %d rows, want 3: %+v", len(res.Rows), res.Rows)
	}
	want := []string{"a", "b", "c"}
	for i, w := range want {
		if got := res.Rows[i]["v"]; got != w {
			t.Errorf("row %d: v = %v, want %v", i, got, w)
		}
	}

	// Multi-column, no alias list: PostgreSQL's default column1/column2
	// names.
	res2, err := db.Query(ctx, `SELECT column1, column2 FROM (VALUES (1, 'x'), (2, 'y')) v ORDER BY column1`)
	if err != nil {
		t.Fatalf("multi-column query: %v", err)
	}
	if len(res2.Rows) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(res2.Rows), res2.Rows)
	}
	if res2.Rows[0]["column2"] != "x" || res2.Rows[1]["column2"] != "y" {
		t.Errorf("column2 values = %v, %v, want x, y", res2.Rows[0]["column2"], res2.Rows[1]["column2"])
	}

	// VALUES joins like any other derived table.
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "ids", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("ids", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	res3, err := db.Query(ctx, `SELECT ids.id, label FROM ids JOIN (VALUES (1, 'one'), (2, 'two')) AS l(id, label) ON ids.id = l.id ORDER BY ids.id`)
	if err != nil {
		t.Fatalf("join query: %v", err)
	}
	if len(res3.Rows) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(res3.Rows), res3.Rows)
	}
	if res3.Rows[0]["label"] != "one" || res3.Rows[1]["label"] != "two" {
		t.Errorf("labels = %v, %v, want one, two", res3.Rows[0]["label"], res3.Rows[1]["label"])
	}
}

// TestTwoWordCastTypesEngine is the #374 engine-level regression for
// two-word CAST type names: double precision and character varying must
// produce the same values as this engine's existing single-word spellings,
// over both literals and columns.
func TestTwoWordCastTypesEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, []map[string]any{{"n": int64(3), "s": "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, `SELECT
		CAST(n AS double precision) AS a,
		CAST(n AS double) AS a2,
		CAST(s AS character varying) AS b,
		CAST(s AS varchar) AS b2,
		n::double precision AS c
		FROM t`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	row := res.Rows[0]
	if row["a"] != row["a2"] {
		t.Errorf("double precision (%v) != double (%v)", row["a"], row["a2"])
	}
	if av, ok := row["a"].(float64); !ok || av != 3.0 {
		t.Errorf("a = %v (%T), want float64(3)", row["a"], row["a"])
	}
	if row["b"] != row["b2"] {
		t.Errorf("character varying (%v) != varchar (%v)", row["b"], row["b2"])
	}
	if row["b"] != "x" {
		t.Errorf("b = %v, want \"x\"", row["b"])
	}
	if row["c"] != row["a"] {
		t.Errorf(":: postfix double precision (%v) != CAST(... AS double precision) (%v)", row["c"], row["a"])
	}
}

// TestIsDistinctFromEngine is the #374 engine-level regression for
// IS [NOT] DISTINCT FROM: NULL-safe (in)equality over both literals and a
// nullable column, pinning that NULL IS DISTINCT FROM NULL is FALSE, never
// NULL/UNKNOWN.
func TestIsDistinctFromEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	res, err := db.Query(ctx, `SELECT
		(NULL IS DISTINCT FROM NULL) AS a,
		(NULL IS NOT DISTINCT FROM NULL) AS b,
		(1 IS DISTINCT FROM NULL) AS c,
		(1 IS DISTINCT FROM 1) AS d,
		(1 IS DISTINCT FROM 2) AS e,
		(1 IS NOT DISTINCT FROM 1) AS f`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	row := res.Rows[0]
	want := map[string]bool{"a": false, "b": true, "c": true, "d": false, "e": true, "f": true}
	for col, w := range want {
		if row[col] != w {
			t.Errorf("%s = %v, want %v", col, row[col], w)
		}
	}

	// Over a nullable column: two rows with equal non-NULL values, one row
	// with NULL against a non-NULL value, and one row NULL against NULL —
	// the shape a WHERE clause distinguishes real equality tests from.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "pairs", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("pairs", schema, nil, ingest.Config{MaxBufferRows: 100})
	rows := []map[string]any{
		{"id": int64(1), "a": int64(5), "b": int64(5)}, // equal, not distinct
		{"id": int64(2), "a": int64(5), "b": int64(6)}, // different, distinct
		{"id": int64(3), "a": nil, "b": int64(5)},      // NULL vs value, distinct
		{"id": int64(4), "a": nil, "b": nil},           // NULL vs NULL, NOT distinct
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	res2, err := db.Query(ctx, `SELECT id FROM pairs WHERE a IS DISTINCT FROM b ORDER BY id`)
	if err != nil {
		t.Fatalf("where query: %v", err)
	}
	if len(res2.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (ids 2, 3): %+v", len(res2.Rows), res2.Rows)
	}
	if res2.Rows[0]["id"] != int64(2) || res2.Rows[1]["id"] != int64(3) {
		t.Errorf("ids = %v, %v, want 2, 3", res2.Rows[0]["id"], res2.Rows[1]["id"])
	}
}

// TestPositionFunctionEngine is the #374 engine-level regression for
// POSITION(needle IN haystack): 1-based match position, 0 for no match,
// over both literals and columns.
func TestPositionFunctionEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	res, err := db.Query(ctx, `SELECT
		POSITION('cd' IN 'abcdef') AS found,
		POSITION('zz' IN 'abcdef') AS missing,
		POSITION('a' IN 'abcdef') AS at_start`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	row := res.Rows[0]
	wantFloat := map[string]float64{"found": 3, "missing": 0, "at_start": 1}
	for col, w := range wantFloat {
		got, ok := row[col].(float64)
		if !ok || got != w {
			t.Errorf("%s = %v (%T), want %v", col, row[col], row[col], w)
		}
	}

	schema := parquet.Schema{Columns: []parquet.Column{{Name: "s", Type: parquet.TypeString}}}
	if err := db.CreateTable(ctx, "strs", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("strs", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, []map[string]any{{"s": "hello world"}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	res2, err := db.Query(ctx, `SELECT POSITION('world' IN s) AS p FROM strs`)
	if err != nil {
		t.Fatalf("column query: %v", err)
	}
	if got, ok := res2.Rows[0]["p"].(float64); !ok || got != 7 {
		t.Errorf("POSITION('world' IN s) = %v, want 7", res2.Rows[0]["p"])
	}
}

// TestReplaceFunctionEngine is the #382 engine-level regression: REPLACE(...)
// now parses as a function call (the same keyword-dispatch gap #371 fixed
// for EVERY), and fnReplace/vecReplace — already implemented and registered
// — answer correctly over both literals and columns. Also exercises
// POSITION and REPLACE in the same statement, the exact shape the oracle's
// PositionAndReplace entry reproduces.
func TestReplaceFunctionEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	res, err := db.Query(ctx, `SELECT
		REPLACE('abcabc', 'b', 'X') AS r,
		REPLACE('abcabc', 'z', 'X') AS no_match,
		POSITION('cd' IN 'abcdef') AS p,
		POSITION('zz' IN 'abcdef') AS missing`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	row := res.Rows[0]
	if row["r"] != "aXcaXc" {
		t.Errorf("r = %v, want aXcaXc", row["r"])
	}
	if row["no_match"] != "abcabc" {
		t.Errorf("no_match = %v, want abcabc (unchanged)", row["no_match"])
	}
	wantFloat := map[string]float64{"p": 3, "missing": 0}
	for col, w := range wantFloat {
		got, ok := row[col].(float64)
		if !ok || got != w {
			t.Errorf("%s = %v (%T), want %v", col, row[col], row[col], w)
		}
	}

	schema := parquet.Schema{Columns: []parquet.Column{{Name: "s", Type: parquet.TypeString}}}
	if err := db.CreateTable(ctx, "replace_strs", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("replace_strs", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, []map[string]any{{"s": "hello world"}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	res2, err := db.Query(ctx, `SELECT REPLACE(s, 'world', 'there') AS r FROM replace_strs`)
	if err != nil {
		t.Fatalf("column query: %v", err)
	}
	if res2.Rows[0]["r"] != "hello there" {
		t.Errorf("REPLACE(s, 'world', 'there') = %v, want %q", res2.Rows[0]["r"], "hello there")
	}
}
