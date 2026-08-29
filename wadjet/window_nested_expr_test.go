package wadjet

import (
	"context"
	"fmt"
	"testing"
)

// #610: a window function nested inside a larger expression —
// SUM(x) OVER (...) + 1, SUM(x) OVER (...) * 2, COALESCE(LAG(x) OVER (...), 0),
// a window call inside a CASE branch — silently discarded the window
// computation. The parser accepted the syntax but only wired a window into the
// plan when it was the ENTIRE select expression, so the outer arithmetic ran
// over nothing and the answer was a plausible-looking wrong value (every row
// of `SUM(c_i32) OVER (...) + 1` came back as 1).
//
// The fixture is the type-matrix table (id, g = i%3, c_i32 = i*3 for i<28).
// For id < 5, c_i32 is 0,3,6,9,12 and non-null, so the numbers are exact.

// TestNestedWindowExpressionExactValues pins the issue's own reproduction to
// the values PostgreSQL returns: the running sum plus one, not the bare sum,
// not NULL, not the window discarded.
func TestNestedWindowExpressionExactValues(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	// Running SUM(c_i32) OVER (ORDER BY id) for id 0..4 is 0,3,9,18,30; +1 is
	// 1,4,10,19,31 and *2 is 0,6,18,36,60.
	cases := []struct {
		name string
		proj string
		want []int64
	}{
		{"plus_one", "SUM(c_i32) OVER (ORDER BY id) + 1", []int64{1, 4, 10, 19, 31}},
		{"times_two", "SUM(c_i32) OVER (ORDER BY id) * 2", []int64{0, 6, 18, 36, 60}},
		{"one_plus_window", "1 + SUM(c_i32) OVER (ORDER BY id)", []int64{1, 4, 10, 19, 31}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(
				"SELECT id, %s AS w FROM mbtypes WHERE id < 5 ORDER BY id", tc.proj)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("query failed: %v\n  SQL: %s", err, sql)
			}
			if len(res.Rows) != len(tc.want) {
				t.Fatalf("%s: got %d rows, want %d\n  SQL: %s",
					tc.name, len(res.Rows), len(tc.want), sql)
			}
			for i, r := range res.Rows {
				got := wpkInt(t, r["w"])
				if got != tc.want[i] {
					t.Fatalf("%s: row %d (id=%v) w=%d, want %d — the window computation "+
						"was dropped from the nested expression (#610)\n  SQL: %s",
						tc.name, i, r["id"], got, tc.want[i], sql)
				}
			}
		})
	}
}

// TestNestedWindowExpressionMatchesBareWindow checks the general property
// without hardcoded numbers: the nested form must equal the bare window with
// the outer operation applied in Go, over the whole fixture and every
// partition. This is the shape the differential/type-matrix gates rely on.
func TestNestedWindowExpressionMatchesBareWindow(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	bare := func(over string) []int64 {
		sql := fmt.Sprintf(
			"SELECT id, SUM(c_i32) OVER (%s) AS w FROM mbtypes WHERE id < 200 ORDER BY id", over)
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("bare query failed: %v\n  SQL: %s", err, sql)
		}
		out := make([]int64, len(res.Rows))
		for i, r := range res.Rows {
			out[i] = wpkInt(t, r["w"])
		}
		return out
	}
	nested := func(proj, over string) []int64 {
		sql := fmt.Sprintf(
			"SELECT id, (%s) AS w FROM mbtypes WHERE id < 200 ORDER BY id",
			fmt.Sprintf(proj, over))
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("nested query failed: %v\n  SQL: %s", err, sql)
		}
		out := make([]int64, len(res.Rows))
		for i, r := range res.Rows {
			out[i] = wpkInt(t, r["w"])
		}
		return out
	}

	over := "PARTITION BY g ORDER BY id"
	base := bare(over)

	// + 1
	plusRows := nested("SUM(c_i32) OVER (%s) + 1", over)
	if len(plusRows) != len(base) {
		t.Fatalf("+1: %d rows, bare has %d", len(plusRows), len(base))
	}
	distinct := map[int64]bool{}
	for i := range base {
		if plusRows[i] != base[i]+1 {
			t.Fatalf("+1: row %d = %d, bare window + 1 = %d (#610)", i, plusRows[i], base[i]+1)
		}
		distinct[plusRows[i]] = true
	}
	if len(distinct) <= 1 {
		t.Fatalf("+1: window column is constant — the window ran over nothing (#610)")
	}

	// * 2
	timesRows := nested("SUM(c_i32) OVER (%s) * 2", over)
	for i := range base {
		if timesRows[i] != base[i]*2 {
			t.Fatalf("*2: row %d = %d, bare window * 2 = %d (#610)", i, timesRows[i], base[i]*2)
		}
	}
}

// TestNestedWindowInCaseSingleProcess covers a window call inside a CASE
// branch, whose outer result is a STRING. The single-process pipeline
// evaluates it (the DAG's gather-time evaluator is numeric-only — a
// pre-existing limitation noted in coordinator.TestWindowNestedExprTwoPath).
func TestNestedWindowInCaseSingleProcess(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	// g = id % 3, so for id 0..6 the per-partition ROW_NUMBER is 1,1,1,2,2,2,3.
	sql := "SELECT id, CASE WHEN ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) = 1 " +
		"THEN 'first' ELSE 'rest' END AS w FROM mbtypes WHERE id < 7 ORDER BY id"
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v\n  SQL: %s", err, sql)
	}
	want := []string{"first", "first", "first", "rest", "rest", "rest", "rest"}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, r := range res.Rows {
		got := fmt.Sprint(r["w"])
		if got != want[i] {
			t.Fatalf("row %d (id=%v) w=%q, want %q — the window inside CASE was dropped (#610)",
				i, r["id"], got, want[i])
		}
	}
}

// TestNestedWindowInCoalesce covers a function wrapper: COALESCE(LAG(...), 0).
// The first row of each ordering has no predecessor, so LAG is NULL and
// COALESCE must fold it to 0 — the bare LAG's own answer with NULL replaced.
func TestNestedWindowInCoalesce(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	sql := "SELECT id, COALESCE(LAG(c_i32) OVER (ORDER BY id), -7) AS w " +
		"FROM mbtypes WHERE id < 5 ORDER BY id"
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v\n  SQL: %s", err, sql)
	}
	// LAG(c_i32) over id 0..4 = NULL,0,3,6,9 → COALESCE(...,-7) = -7,0,3,6,9.
	want := []int64{-7, 0, 3, 6, 9}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, r := range res.Rows {
		got := wpkInt(t, r["w"])
		if got != want[i] {
			t.Fatalf("row %d (id=%v) w=%d, want %d — COALESCE(LAG(...)) dropped the window (#610)",
				i, r["id"], got, want[i])
		}
	}
}
