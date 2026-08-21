package test

import (
	"context"
	"testing"
)

// MIN/MAX over a window were the last input-dependent entries answered from
// the planner's fixed name list, and so were declared float64 whatever
// their input (#361, deliberately left by #345). Over an INT32 column the
// compute read int32 values, Vector.SetValue had no int32 case for a
// Float64 destination, and every write vanished: the column answered 0 on
// every row, marked valid. Strings and dates had the same silent-zero
// symptom for the same reason.
//
// The fix is the one the five value functions already got in #345: resolve
// the output type from the input column — in the planner where the catalog
// can answer, and again in exec.Window from the vector it actually reads
// (retypeValueColumns), so a declaration that arrives wrong is corrected
// instead of final. MIN/MAX carry values through compareAny/SetValue in
// every path (in-memory columnar, the row-map walker, the empty-
// PARTITION-BY streamer, frames), so the retype is the whole fix.
//
// The window corpus entries for MIN/MAX use a FLOAT column deliberately;
// these fixtures use the narrow-int, string and date columns that reach
// the drop. Reuses openWindowTypingDB (window_value_typing_test.go).
func TestWindowMinMaxOverTypedColumns(t *testing.T) {
	ctx := context.Background()
	db := openWindowTypingDB(t, ctx)

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		// ---- INT32: the reported shape. ----
		{"max over an INT32 column, running",
			"SELECT MAX(i32) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int32(10), int32(20), int32(30), int32(40)}},
		{"min over an INT32 column, running",
			"SELECT MIN(i32) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int32(10), int32(10), int32(10), int32(10)}},
		{"max over an INT32 column, partitioned",
			"SELECT MAX(i32) OVER (PARTITION BY g ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int32(10), int32(20), int32(30), int32(40)}},
		{"min over an INT32 column, whole partition",
			`SELECT MIN(i32) OVER (ORDER BY k
			   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS x
			 FROM w ORDER BY k`,
			[]any{int32(10), int32(10), int32(10), int32(10)}},
		{"min over an INT32 column with a sliding frame",
			`SELECT MIN(i32) OVER (ORDER BY k
			   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x
			 FROM w ORDER BY k`,
			[]any{int32(10), int32(10), int32(20), int32(30)}},
		{"max over an INT32 column without ORDER BY",
			"SELECT MAX(i32) OVER () AS x FROM w ORDER BY k",
			[]any{int32(40), int32(40), int32(40), int32(40)}},

		// ---- INT64. ----
		{"max over an INT64 column, running",
			"SELECT MAX(k) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(2), int64(3), int64(4)}},

		// ---- STRING: #345's symptom through MIN/MAX. ----
		{"min over a string column, running",
			"SELECT MIN(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"alpha", "alpha", "alpha", "alpha"}},
		{"max over a string column, running",
			"SELECT MAX(s) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"alpha", "bravo", "charlie", "delta"}},
		{"max over a string column, partitioned",
			"SELECT MAX(s) OVER (PARTITION BY g ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"alpha", "bravo", "charlie", "delta"}},
		{"min over a string column with an explicit frame",
			`SELECT MIN(s) OVER (ORDER BY k
			   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS x
			 FROM w ORDER BY k`,
			[]any{"alpha", "alpha", "alpha", "alpha"}},

		// ---- DATE and TIMESTAMP: the declared type is the rendering. ----
		{"min over a DATE column",
			"SELECT MIN(d) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"2026-03-18", "2026-03-18", "2026-03-18", "2026-03-18"}},
		{"max over a DATE column",
			"SELECT MAX(d) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{"2026-03-18", "2026-03-19", "2026-03-20", "2026-03-21"}},
		{"max over a TIMESTAMP column",
			"SELECT MAX(ts) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1773792000000), int64(1773878400000), int64(1773964800000), int64(1774051200000)}},

		// ---- The float control: already correct, must stay so. ----
		{"min over a FLOAT64 column",
			"SELECT MIN(f) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{1.5, 1.5, 1.5, 1.5}},
		{"max over a FLOAT64 column, whole partition",
			`SELECT MAX(f) OVER (ORDER BY k
			   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS x
			 FROM w ORDER BY k`,
			[]any{4.5, 4.5, 4.5, 4.5}},

		// ---- The aggregate window functions that finalize to a fixed
		// type must keep their name-list answer. ----
		{"sum over an INT32 column stays float64",
			"SELECT SUM(i32) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{10.0, 30.0, 60.0, 100.0}},
		{"count over an INT32 column stays int64",
			"SELECT COUNT(i32) OVER (ORDER BY k) AS x FROM w ORDER BY k",
			[]any{int64(1), int64(2), int64(3), int64(4)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertColumn(t, ctx, db, tc.sql, "x", tc.want)
		})
	}
}
