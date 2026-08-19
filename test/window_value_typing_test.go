package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A window VALUE function over a string column came back as the integer 0 on
// every row, with no error (#345):
//
//	SELECT n_name, LAG(n_name)         OVER (ORDER BY n_nationkey) FROM nation
//	-- NULL, 0, 0 …
//	SELECT n_name, FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey) FROM nation
//	-- 0, 0, 0 …
//
// LAG, LEAD, FIRST_VALUE, LAST_VALUE and NTH_VALUE return a value taken FROM
// their input column rather than computing one, so their output type is that
// column's type. The physical planner declared all five float64 from a
// hand-maintained name list, exec.Window allocated the output vector at the
// declared type, and Vector.SetValue dropped every value the vector could not
// hold — leaving the slot at 0 and marking it valid, so the row rendered as
// the integer 0. Unlike exec.Project, exec.Window had no runtime correction,
// so a wrong declaration was final. This is #333's signature inside the window
// operator.
//
// The fix is the shape #329 and #333 both used: the name list keeps only what
// is genuinely input-independent (the rank family, and the aggregate window
// functions that finalize to a fixed type), and the value functions resolve
// from the input column — declining, and keeping the old float64, wherever the
// column cannot be resolved. exec.Window additionally re-types from the vector
// it will read, so a declaration that still arrives wrong is corrected instead
// of corrupting the column.
//
// The numeric control is not a formality: Float64.SetValue has no int32 case
// either, so LAG over an INT32 column dropped its writes exactly as a string
// did. DATE and TIMESTAMP are here because their RENDERING is what the
// declared type buys — a date typed float64 is not a mis-scaled date, it is 0.
func TestWindowValueFunctionsOverTypedColumns(t *testing.T) {
	ctx := context.Background()
	db := openWindowTypingDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		// ---- STRING: the reported shape, all five value functions. ----
		{"lag over a string column",
			"SELECT LAG(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{nil, "alpha", "bravo", "charlie"}},
		{"lead over a string column",
			"SELECT LEAD(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"bravo", "charlie", "delta", nil}},
		{"first_value over a string column",
			"SELECT FIRST_VALUE(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"alpha", "alpha", "alpha", "alpha"}},
		{"last_value over a string column",
			"SELECT LAST_VALUE(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"alpha", "bravo", "charlie", "delta"}},
		// No explicit frame, so the frame is the SQL default: RANGE BETWEEN
		// UNBOUNDED PRECEDING AND CURRENT ROW. Row 0 sees one row and has no
		// second one. This case wanted "bravo" on every row while the engine
		// evaluated NTH_VALUE over the whole partition whatever frame it was
		// given (#350); the explicit whole-partition spelling below is the
		// one that means every row (and DuckDB agrees on both — see
		// benchmarks/tpch's WindowNthValueFrames).
		{"nth_value over a string column",
			"SELECT NTH_VALUE(s, 2) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{nil, "bravo", "bravo", "bravo"}},

		// ---- The extra arguments share the column's argument string. ----
		// Column pruning read that whole string as the column name, so "s"
		// was never marked required, the scan dropped it, and the operator
		// resolved no input vector at all. These four SELECT the column
		// nowhere else, which is what makes the pruning decision bite.
		{"lag with an offset and a string default",
			"SELECT LAG(s, 2, 'none') OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"none", "none", "alpha", "bravo"}},
		{"lead with an offset",
			"SELECT LEAD(s, 2) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"charlie", "delta", nil, nil}},
		{"lag with a float default, which reads as a qualified name",
			"SELECT LAG(f, 1, 0.5) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{0.5, 1.5, 2.5, 3.5}},
		{"ntile takes a bucket count, not a column",
			"SELECT NTILE(2) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(1), int64(2), int64(2)}},

		// ---- Explicit frames: where window bugs hide. ----
		{"nth_value with an explicit whole-partition frame",
			`SELECT NTH_VALUE(s, 3) OVER (ORDER BY k
			   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS x
			 FROM w ORDER BY k`,
			[]any{"charlie", "charlie", "charlie", "charlie"}},
		{"nth_value past the end of the frame is NULL, not zero",
			`SELECT NTH_VALUE(s, 9) OVER (ORDER BY k
			   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS x
			 FROM w ORDER BY k`,
			[]any{nil, nil, nil, nil}},
		{"last_value with an explicit frame, per partition",
			`SELECT LAST_VALUE(s) OVER (PARTITION BY g ORDER BY k
			   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS x
			 FROM w ORDER BY k`,
			[]any{"alpha", "bravo", "charlie", "delta"}},

		// ---- The numeric control. ----
		{"lag over an INT32 column",
			"SELECT LAG(i32) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{nil, int32(10), int32(20), int32(30)}},
		{"first_value over an INT64 column",
			"SELECT FIRST_VALUE(k) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(1), int64(1), int64(1)}},
		{"lag over a FLOAT64 column, which was already correct",
			"SELECT LAG(f) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{nil, 1.5, 2.5, 3.5}},

		// ---- DATE and TIMESTAMP: the declared type is the rendering. ----
		{"first_value over a DATE column",
			"SELECT FIRST_VALUE(d) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"2026-03-18", "2026-03-18", "2026-03-18", "2026-03-18"}},
		{"lag over a DATE column",
			"SELECT LAG(d) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{nil, "2026-03-18", "2026-03-19", "2026-03-20"}},
		// A TIMESTAMP reads back as its epoch milliseconds, so the tell here
		// is the Go TYPE: declared float64 the same value came back as
		// float64(1.7738784e+12), which pgwire and every cast downstream then
		// render as a float rather than a timestamp.
		{"lead over a TIMESTAMP column",
			"SELECT LEAD(ts) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1773878400000), int64(1773964800000), int64(1774051200000), nil}},

		// ---- PARTITION BY, so the value is lifted per partition. ----
		{"first_value over a string column, partitioned",
			"SELECT FIRST_VALUE(s) OVER (PARTITION BY g ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"alpha", "bravo", "alpha", "bravo"}},

		// ---- The rank family: input-independent, and must stay untouched. ----
		{"row_number is unaffected",
			"SELECT ROW_NUMBER() OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(2), int64(3), int64(4)}},
		{"rank is unaffected",
			"SELECT RANK() OVER (ORDER BY g) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(3), int64(1), int64(3)}},
		{"dense_rank is unaffected",
			"SELECT DENSE_RANK() OVER (ORDER BY g) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(2), int64(1), int64(2)}},
		{"count over a string column stays int64",
			"SELECT COUNT(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(2), int64(3), int64(4)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

// A value function whose argument the planner cannot resolve keeps the old
// float64 declaration — declining is deliberate, since nothing downstream
// corrects a declaration and a confidently wrong one is worse than the guess
// it replaces. exec.Window's re-typing is what still gets these right at
// runtime, which is the whole reason it exists: it covers exactly the cases
// the planner had to decline.
func TestWindowValueFunctionsOverUnresolvableArguments(t *testing.T) {
	ctx := context.Background()
	db := openWindowTypingDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{
			// A derived table rebinds a scan column's name to another type.
			// inputColTypes stops at the projection rather than answering
			// with nation's meaning of the name, so the planner declines —
			// and the operator resolves it from the vector it actually reads.
			name: "a derived table rebinds a column name to another type",
			sql: `SELECT LAG(s) OVER (ORDER BY t.k) AS x
			      FROM (SELECT k, i32 AS s FROM w) t ORDER BY t.k`,
			want: []any{nil, int32(10), int32(20), int32(30)},
		},
		{
			// A COMPUTED argument is not a column at all, and nothing below
			// the window materializes it — so the operator finds no input
			// vector and the column comes back NULL. That is a separate,
			// pre-existing gap (window arguments must be column references);
			// it is pinned here because the shape used to nil-dereference and
			// take the process down with it, and NULL is the floor this must
			// not fall below again.
			name: "a computed argument is unsupported, and must not panic",
			sql:  `SELECT FIRST_VALUE(UPPER(s)) OVER (ORDER BY k) AS x FROM w ORDER BY k`,
			want: []any{nil, nil, nil, nil},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}

func openWindowTypingDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "i32", Type: parquet.TypeInt32},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
	}}
	// TIMESTAMP is written as epoch milliseconds (the vector's storage); the
	// ingester does not parse a timestamp string, which is a separate gap.
	ingestRows(t, ctx, db, "w", schema, []map[string]any{
		{"k": int64(1), "g": int64(0), "s": "alpha", "i32": int32(10), "f": 1.5, "d": "2026-03-18", "ts": int64(1773792000000)},
		{"k": int64(2), "g": int64(1), "s": "bravo", "i32": int32(20), "f": 2.5, "d": "2026-03-19", "ts": int64(1773878400000)},
		{"k": int64(3), "g": int64(0), "s": "charlie", "i32": int32(30), "f": 3.5, "d": "2026-03-20", "ts": int64(1773964800000)},
		{"k": int64(4), "g": int64(1), "s": "delta", "i32": int32(40), "f": 4.5, "d": "2026-03-21", "ts": int64(1774051200000)},
	})
	return db
}
