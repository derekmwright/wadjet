package tpch

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestTrailingInputAndPagination is #337's table, run end to end on both
// execution paths with the row VALUES and their SEQUENCE asserted — not
// merely that the query returned without an error, which every one of these
// already did while returning the wrong rows.
//
// Three faces of one defect, all verified on main before the fix:
//
//	SELECT COUNT(*) ... WHERE n_regionkey = 1 GARBAGE TOKENS HERE  → 5
//	SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 5             → 25 rows
//	SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 5 LIMIT 3     → 25 rows
//	... UNION ALL ... ORDER BY 1                                   → unsorted
//	... LIMIT 5 UNION SELECT ...                                   → left arm
//	SELECT COUNT(*) FROM nation NATURAL JOIN region                → 25
//
// Returning the whole table is the worst failure a paginating client can be
// handed, because page one still looks right and only the totals are wrong.
func TestTrailingInputAndPagination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	rows := duckdbFixtureRows(t)
	embedded := ingestDuckDBFixture(t, ctx, rows) // arm A: single-process
	_, dag := setupCluster(t, ctx, rows)          // arm B: stage DAG

	// nation's keys are 0..24 and region's are 0..4 at SF0.01.
	all25 := seq(0, 25)

	for _, tc := range paginationCases() {
		t.Run(tc.name, func(t *testing.T) {
			aRows, aCols, aErr := runWadjet(ctx, embedded, tc.sql)
			checkPagination(t, armLocal, tc, aRows, aCols, aErr, all25)

			bRows, bCols, bErr := runArm(t, ctx, dag, tc.sql)
			checkPagination(t, armDAG, tc, bRows, bCols, bErr, all25)
		})
	}
}

// paginationCase is one statement plus the answer it must give.
type paginationCase struct {
	name string
	sql  string
	// wantErr, when set, is a substring the error must contain — the token
	// where parsing stopped. An empty result is NOT an acceptable substitute:
	// a client can act on a syntax error and cannot act on silence.
	wantErr string
	// col is the column whose values are compared; want is those values in
	// the order the statement asked for. A nil want with no wantErr means
	// "no rows", which is a real answer and not the same as "all of them".
	col  string
	want []int
	// dagDefect pins the stage DAG's known disagreement, which predates this
	// fix and is not a parser question: walkStages emits no merge stage for
	// UNION / INTERSECT / EXCEPT (physical/plan.go, "each side runs
	// independently; merge results at the end" — nothing merges), so a set
	// operation on the DAG returns one arm's raw scan, unprojected. Arm A
	// stays fully gated. The arm is still compared here, so this subtest
	// FAILS the moment the DAG starts agreeing and the pin has to go.
	dagDefect string
}

func paginationCases() []paginationCase {
	return []paginationCase{
		// --- the issue's table -------------------------------------------
		{
			name:    "TrailingGarbage",
			sql:     "SELECT COUNT(*) FROM nation WHERE n_regionkey = 1 GARBAGE TOKENS HERE",
			wantErr: "GARBAGE",
		},
		{
			name: "OffsetAlone",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 5",
			col:  "n_nationkey", want: seq(5, 25),
		},
		{
			name: "OffsetThenLimit",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 5 LIMIT 3",
			col:  "n_nationkey", want: []int{5, 6, 7},
		},
		{
			name: "UnionAllOrderBy",
			sql:  "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1",
			col:  "r_regionkey", want: []int{0, 0, 1, 1, 2, 2, 3, 3, 4, 4},
			dagDefect: "the stage DAG emits no merge stage for a set operation",
		},
		{
			// PostgreSQL and DuckDB both reject this: an arm's own LIMIT
			// needs parentheses. Wadjet used to run the left arm alone.
			name:    "LimitBeforeUnion",
			sql:     "SELECT r_regionkey FROM region LIMIT 5 UNION SELECT r_regionkey FROM region",
			wantErr: "UNION",
		},
		{
			name:    "NaturalJoin",
			sql:     "SELECT COUNT(*) FROM nation NATURAL JOIN region",
			wantErr: "NATURAL JOIN is not supported",
		},

		// --- pagination, every spelling ----------------------------------
		{
			name: "LimitThenOffset",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 LIMIT 3 OFFSET 5",
			col:  "n_nationkey", want: []int{5, 6, 7},
		},
		{
			name: "OffsetRowsThenLimit",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 5 ROWS LIMIT 3",
			col:  "n_nationkey", want: []int{5, 6, 7},
		},
		{
			// The page after the last one is empty. Returning the table is
			// what the bug did, and it is the failure that looks like success.
			name: "OffsetPastTheEnd",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 100",
			col:  "n_nationkey", want: nil,
		},
		{
			name: "OffsetPastTheEndWithLimit",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 100 LIMIT 5",
			col:  "n_nationkey", want: nil,
		},
		{
			// OFFSET 0 skips nothing — the control that says the fix is not
			// simply "drop rows".
			name: "OffsetZero",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 0",
			col:  "n_nationkey", want: seq(0, 25),
		},
		{
			name: "LastPage",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 OFFSET 23 LIMIT 10",
			col:  "n_nationkey", want: []int{23, 24},
		},
		{
			// Walking the whole table one page at a time must visit every
			// row exactly once; a dropped OFFSET repeats page one forever.
			name: "PageTwo",
			sql:  "SELECT n_nationkey FROM nation ORDER BY 1 LIMIT 10 OFFSET 10",
			col:  "n_nationkey", want: seq(10, 20),
		},
		{
			name: "OffsetOverJoin",
			sql: "SELECT n_nationkey FROM nation JOIN region ON n_regionkey = r_regionkey " +
				"ORDER BY n_nationkey OFFSET 20",
			col: "n_nationkey", want: seq(20, 25),
		},
		{
			name: "OffsetOverGroupBy",
			sql:  "SELECT n_regionkey FROM nation GROUP BY n_regionkey ORDER BY 1 OFFSET 3",
			col:  "n_regionkey", want: []int{3, 4},
		},
		{
			name: "OffsetOverDistinct",
			sql:  "SELECT DISTINCT n_regionkey FROM nation ORDER BY 1 OFFSET 2 LIMIT 2",
			col:  "n_regionkey", want: []int{2, 3},
		},

		// --- set operations, ORDER BY over the WHOLE result ---------------
		{
			name: "UnionAllOrderByLimit",
			sql:  "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1 LIMIT 3",
			col:  "r_regionkey", want: []int{0, 0, 1},
			dagDefect: "the stage DAG emits no merge stage for a set operation",
		},
		{
			name: "UnionOrderBy",
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION " +
				"SELECT n_regionkey FROM nation WHERE n_nationkey >= 5 ORDER BY 1",
			col: "n_regionkey", want: []int{0, 1, 2, 3, 4},
			dagDefect: "the stage DAG emits no merge stage for a set operation",
		},
		{
			name: "UnionOrderByLimit",
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION " +
				"SELECT n_regionkey FROM nation WHERE n_nationkey >= 5 ORDER BY 1 LIMIT 2",
			col: "n_regionkey", want: []int{0, 1},
			dagDefect: "the stage DAG emits no merge stage for a set operation",
		},
		{
			name: "UnionAllOrderByDesc",
			sql:  "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1 DESC",
			col:  "r_regionkey", want: []int{4, 4, 3, 3, 2, 2, 1, 1, 0, 0},
			dagDefect: "the stage DAG emits no merge stage for a set operation",
		},
		{
			name: "UnionAllOrderByOffset",
			sql: "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region " +
				"ORDER BY 1 OFFSET 6",
			col: "r_regionkey", want: []int{3, 3, 4, 4},
			dagDefect: "the stage DAG emits no merge stage for a set operation",
		},
	}
}

// checkPagination holds one arm's answer against the case.
func checkPagination(t *testing.T, arm string, tc paginationCase, rows []map[string]any, cols []string, err error, all25 []int) {
	t.Helper()

	if tc.wantErr != "" {
		if err == nil {
			t.Errorf("arm %s: %q returned %d rows instead of erroring — the clause was discarded in silence",
				arm, tc.sql, len(rows))
			return
		}
		if !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("arm %s: error does not name %q:\n  %v", arm, tc.wantErr, err)
		}
		return
	}

	if tc.dagDefect != "" && arm == armDAG {
		// Not gated, but not ignored: the moment the DAG agrees, the pin is
		// stale and this fails until it is deleted. Agreement means the whole
		// answer — the DAG's set-operation output carries the arm's every
		// column, so the SHAPE has to match too or a two-value comparison can
		// coincide with the wrong rows.
		if err == nil && len(cols) == 1 && strings.EqualFold(cols[0], tc.col) &&
			intsEqual(intColumn(rows, tc.col), tc.want) {
			t.Errorf("arm %s now agrees on %q, so the pinned defect is FIXED:\n  %s\n"+
				"Delete the dagDefect field on %s so the arm is gated again.",
				arm, tc.sql, tc.dagDefect, tc.name)
		} else {
			t.Logf("known divergence, NOT gated on arm %s: %s", arm, tc.dagDefect)
		}
		return
	}

	if err != nil {
		t.Errorf("arm %s: %q failed: %v", arm, tc.sql, err)
		return
	}
	got := intColumn(rows, tc.col)
	if intsEqual(got, tc.want) {
		return
	}
	// Name the failure mode the bug produced, so a regression reads as
	// itself rather than as a generic mismatch.
	if intsEqual(got, all25) && len(tc.want) != len(all25) {
		t.Errorf("arm %s: %q returned the whole table (%d rows) — the clause was discarded\n  want %v",
			arm, tc.sql, len(got), tc.want)
		return
	}
	t.Errorf("arm %s: %q\n  got  %v\n  want %v", arm, tc.sql, got, tc.want)
}

// intColumn reads one integer column out of a result, in row order.
func intColumn(rows []map[string]any, col string) []int {
	if len(rows) == 0 {
		return nil
	}
	out := make([]int, 0, len(rows))
	for _, r := range rows {
		out = append(out, int(cellNum(r, col)))
	}
	return out
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// seq returns lo, lo+1, ... hi-1.
func seq(lo, hi int) []int {
	if hi <= lo {
		return nil
	}
	out := make([]int, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, i)
	}
	return out
}
