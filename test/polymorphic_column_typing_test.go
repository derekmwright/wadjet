package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A string column passed through a polymorphic function came back as the
// integer 0 on every row, with no error (#333):
//
//	SELECT COALESCE(n_name, n_comment) FROM nation   -- 0, 0, 0 …
//
// coalesce, nullif, greatest and least declare RetSameAsArg(TypeFloat64), and
// nothing in those expressions decided a type — a bare column reference
// reported "undecided" by design, since its type came from the input schema at
// runtime. So the numeric fallback stood, the projection allocated a Float64
// vector, and every string write into it was dropped. ifnull was the only one
// that worked, because it alone declares RetSameAsArg(TypeString, 0, 1) — the
// tell that this was about the CHOICE of fallback rather than the fallback
// itself.
//
// Flipping the declarations to String would only move the corruption to the
// numeric side, which is why the fix is that a bare column reference now
// decides its type from the catalog (logical.Node.ScanColTypes) and these
// declarations never reach a fallback at all. The numeric half of this test is
// that constraint, held.
func TestPolymorphicFunctionsOverColumns(t *testing.T) {
	ctx := context.Background()
	db := openPolyTypingDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		// The four broken shapes.
		{"nullif over a string column", "SELECT NULLIF(n_name, 'ALGERIA') AS x FROM nation ORDER BY n_nationkey",
			[]any{nil, "ARGENTINA", "BRAZIL"}},
		{"single-argument coalesce", "SELECT COALESCE(n_name) AS x FROM nation ORDER BY n_nationkey",
			[]any{"ALGERIA", "ARGENTINA", "BRAZIL"}},
		{"coalesce over two string columns", "SELECT COALESCE(n_name, n_comment) AS x FROM nation ORDER BY n_nationkey",
			[]any{"ALGERIA", "ARGENTINA", "BRAZIL"}},
		{"greatest over two string columns", "SELECT GREATEST(n_name, n_comment) AS x FROM nation ORDER BY n_nationkey",
			[]any{"cc", "cc", "cc"}},
		{"least over two string columns", "SELECT LEAST(n_name, n_comment) AS x FROM nation ORDER BY n_nationkey",
			[]any{"ALGERIA", "ARGENTINA", "BRAZIL"}},
		// The control that was already correct and must stay correct.
		{"ifnull, the control", "SELECT IFNULL(n_name, n_comment) AS x FROM nation ORDER BY n_nationkey",
			[]any{"ALGERIA", "ARGENTINA", "BRAZIL"}},

		// The numeric constraint: NULLIF(int_col, 1) must stay numeric. This
		// is exactly why the declarations were not flipped to String.
		{"nullif over an int column", "SELECT NULLIF(n_nationkey, 1) AS x FROM nation ORDER BY n_nationkey",
			[]any{int64(0), nil, int64(2)}},
		{"coalesce over an int column", "SELECT COALESCE(n_nationkey, 0) AS x FROM nation ORDER BY n_nationkey",
			[]any{int64(0), int64(1), int64(2)}},
		{"greatest over two int columns", "SELECT GREATEST(n_nationkey, n_regionkey) AS x FROM nation ORDER BY n_nationkey",
			[]any{int64(10), int64(11), int64(12)}},
		// A float column must not be demoted to the int the literal decides:
		// COALESCE(n_ratio, 0) returned 1, 2, 3 before this change because
		// the literal 0 was the first argument that decided anything.
		{"coalesce over a float column", "SELECT COALESCE(n_ratio, 0) AS x FROM nation ORDER BY n_nationkey",
			[]any{1.5, 2.5, 3.5}},
		{"greatest over a float and an int column", "SELECT GREATEST(n_ratio, n_nationkey) AS x FROM nation ORDER BY n_nationkey",
			[]any{1.5, 2.5, 3.5}},

		// Mixed types, where the SQL answer is genuinely debatable. Wadjet
		// takes the FIRST argument that decides — the column here, since a
		// literal is consulted only after it. That is DuckDB's answer too:
		// SELECT typeof(COALESCE(42, 'text')) is INTEGER, the string cast to
		// the column's type rather than the column widened to text.
		{"int column beside a string literal follows the column",
			"SELECT COALESCE(n_nationkey, 'text') AS x FROM nation ORDER BY n_nationkey",
			[]any{int64(0), int64(1), int64(2)}},
		{"string column beside an int literal follows the column",
			"SELECT COALESCE(n_name, 0) AS x FROM nation ORDER BY n_nationkey",
			[]any{"ALGERIA", "ARGENTINA", "BRAZIL"}},

		// #331's nested shapes still resolve, and now resolve without
		// needing a literal beside the nested call to rescue them.
		{"nested nullif over a string column",
			"SELECT COALESCE(NULLIF(n_name, 'ALGERIA'), n_comment) AS x FROM nation ORDER BY n_nationkey",
			[]any{"cc", "ARGENTINA", "BRAZIL"}},
		{"nested nullif over an int column",
			"SELECT COALESCE(NULLIF(n_nationkey, 0), n_regionkey) AS x FROM nation ORDER BY n_nationkey",
			[]any{int64(10), int64(1), int64(2)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

// A column the planner cannot resolve honestly must keep answering
// "undecided", so today's fallback still applies. #331's machinery propagates
// a decision as fact, so a wrong confident answer is worse than a guess: it
// would be believed by every enclosing call.
//
// The dangerous case is a name that IS in some scan below but does not mean
// what that scan means by it. Descending past the projection that rebinds it
// would type these outputs from the catalog's nation.n_name (a string) for
// values that arrive as int64 — the same silent drop as #333, pointing the
// other way.
func TestPolymorphicFunctionsOverUnresolvableColumns(t *testing.T) {
	ctx := context.Background()
	db := openPolyTypingDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{
			name: "a derived table rebinds a scan column's name to another type",
			sql: `SELECT COALESCE(n_name, 0) AS x
			      FROM (SELECT n_nationkey AS n_name FROM nation) t ORDER BY x`,
			want: []any{int64(0), int64(1), int64(2)},
		},
		{
			name: "a derived table rebinds an int column's name to a string",
			sql: `SELECT COALESCE(n_nationkey, 'zzz') AS x
			      FROM (SELECT n_name AS n_nationkey FROM nation) t ORDER BY x`,
			want: []any{"ALGERIA", "ARGENTINA", "BRAZIL"},
		},
		{
			// No scan carries "max(n_nationkey)", so the fallback stands and
			// the output is the declared Float64 — the residual this change
			// deliberately does not reach, pinned so it cannot become a wrong
			// CONFIDENT answer without a test noticing.
			name: "an aggregate output is not a scan column",
			sql:  `SELECT COALESCE(MAX(n_nationkey), -1) AS x FROM nation`,
			want: []any{float64(2)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

// Two scans that carry the same column name at different types cannot answer
// for it, so the name is dropped from the map and the fallback stands —
// picking a side would type one of the two joined columns wrong. Columns only
// one side carries are unaffected and still resolve.
func TestPolymorphicFunctionsOverAJoinWithConflictingColumnTypes(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	left := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "shared", Type: parquet.TypeString},
		{Name: "only_left", Type: parquet.TypeString},
	}}
	right := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "shared", Type: parquet.TypeInt64},
	}}
	ingestRows(t, ctx, db, "lft", left, []map[string]any{
		{"k": int64(1), "shared": "sss", "only_left": "aaa"},
	})
	ingestRows(t, ctx, db, "rgt", right, []map[string]any{
		{"k": int64(1), "shared": int64(7)},
	})

	// only_left is carried by one side alone, so it resolves and the string
	// survives — the fix working across a join.
	assertColumn(t, ctx, db,
		"SELECT COALESCE(only_left, 'fallback') AS x FROM lft JOIN rgt ON lft.k = rgt.k",
		"x", []any{"aaa"})

	// shared is carried at two different types, so nothing decides it and the
	// numeric fallback answers — the honest outcome, not a wrong claim. The
	// literal beside it is what makes the answer right anyway.
	assertColumn(t, ctx, db,
		"SELECT COALESCE(lft.shared, 'fallback') AS x FROM lft JOIN rgt ON lft.k = rgt.k",
		"x", []any{"sss"})
}

func openPolyTypingDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "n_nationkey", Type: parquet.TypeInt64},
		{Name: "n_name", Type: parquet.TypeString},
		{Name: "n_regionkey", Type: parquet.TypeInt64},
		{Name: "n_comment", Type: parquet.TypeString},
		{Name: "n_ratio", Type: parquet.TypeFloat64},
	}}
	ingestRows(t, ctx, db, "nation", schema, []map[string]any{
		{"n_nationkey": int64(0), "n_name": "ALGERIA", "n_regionkey": int64(10), "n_comment": "cc", "n_ratio": 1.5},
		{"n_nationkey": int64(1), "n_name": "ARGENTINA", "n_regionkey": int64(11), "n_comment": "cc", "n_ratio": 2.5},
		{"n_nationkey": int64(2), "n_name": "BRAZIL", "n_regionkey": int64(12), "n_comment": "cc", "n_ratio": 3.5},
	})
	return db
}

func ingestRows(t *testing.T, ctx context.Context, db *wadjet.DB, table string, schema parquet.Schema, rows []map[string]any) {
	t.Helper()
	if err := db.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 20})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
}

// assertColumn compares one output column value by value AND type: the defect
// this file is about produced the right number of rows with the wrong type,
// so an assertion that only compared rendered text would have passed through
// the whole bug.
func assertColumn(t *testing.T, ctx context.Context, db *wadjet.DB, sql, col string, want []any) {
	t.Helper()
	r, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(r.Rows) != len(want) {
		t.Fatalf("%s: got %d rows, want %d: %v", sql, len(r.Rows), len(want), r.Rows)
	}
	for i, w := range want {
		got := r.Rows[i][col]
		if got != w {
			t.Errorf("%s\n  row %d: %v (%T), want %v (%T)", sql, i, got, got, w, w)
		}
	}
}
