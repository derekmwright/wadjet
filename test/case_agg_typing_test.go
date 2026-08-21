package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// MIN/MAX over a string CASE expression returned the integer 0 (#372).
//
// The four-column control from the issue localizes the trigger: the bare
// column, LOWER(col) and col || 'x' are all correct, because a column
// reference resolves through the catalog and function/concat declarations
// carry string return types. A CASE declared NOTHING — nodeDeclaredType had
// no CaseNode arm — so the pre-aggregate projection fell back to Float64,
// every string branch value was dropped in silence, and the aggregate's
// min was the vector's untouched 0. A PROJECTED string CASE was already
// correct (exec.Project re-types from the computed value), which is what
// pins the defect to the aggregate's view. Found by the PostgreSQL
// differential oracle (StringMinMaxCollation, MinMaxOverStringExpression).
//
// The fix resolves a CASE's declared type from its THEN/ELSE branches, the
// same recursion a projection's type resolution uses.
func TestMinMaxOverStringCase(t *testing.T) {
	ctx := context.Background()
	db := openCaseAggDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		// The issue's four-column control, one column at a time so a single
		// failure names its shape.
		{"bare column control",
			"SELECT MIN(s) AS x FROM c", []any{"alpha"}},
		{"function argument control",
			"SELECT MIN(LOWER(s)) AS x FROM c", []any{"alpha"}},
		{"concat control",
			"SELECT MIN(s || 'x') AS x FROM c", []any{"alphax"}},
		{"min over a degenerate string CASE",
			"SELECT MIN(CASE WHEN g = 0 THEN s ELSE s END) AS x FROM c",
			[]any{"alpha"}},
		{"max over a degenerate string CASE",
			"SELECT MAX(CASE WHEN g = 0 THEN s ELSE s END) AS x FROM c",
			[]any{"delta"}},

		// A CASE whose branches genuinely differ, including one built from
		// a concat — the first branch whose type resolves decides.
		{"min over a mixed-branch string CASE",
			"SELECT MIN(CASE WHEN g = 0 THEN LOWER(s) ELSE '_' || s END) AS x FROM c",
			[]any{"_bravo"}},
		{"max over a CASE with a literal branch",
			"SELECT MAX(CASE WHEN g = 0 THEN s ELSE 'zzz' END) AS x FROM c",
			[]any{"zzz"}},

		// No ELSE: the missing branch is NULL, which decides nothing and
		// must not undo the string resolution; the NULL rows are skipped.
		{"min over a string CASE without ELSE",
			"SELECT MIN(CASE WHEN g = 0 THEN s END) AS x FROM c",
			[]any{"alpha"}},

		// Grouped, so per-group accumulators see the projected vector.
		{"grouped min over a string CASE",
			"SELECT g, MIN(CASE WHEN k > 0 THEN s ELSE s END) AS x FROM c GROUP BY g ORDER BY g",
			[]any{"alpha", "bravo"}},

		// A numeric CASE must keep resolving numeric: the branch walk must
		// not break what the Float64 fallback got right.
		{"min over a numeric CASE",
			"SELECT MIN(CASE WHEN g = 0 THEN v ELSE v + 100 END) AS x FROM c",
			[]any{int64(10)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

func openCaseAggDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}}
	ingestRows(t, ctx, db, "c", schema, []map[string]any{
		{"k": int64(1), "g": int64(0), "s": "alpha", "v": int64(10)},
		{"k": int64(2), "g": int64(1), "s": "bravo", "v": int64(20)},
		{"k": int64(3), "g": int64(0), "s": "charlie", "v": int64(30)},
		{"k": int64(4), "g": int64(1), "s": "delta", "v": int64(40)},
	})
	return db
}
