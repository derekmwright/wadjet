package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// This file gates #521: CAST(<col> AS STRING) must render the same text the
// column's own projection does — the claim ADR-0012 item 11 makes for LIKE
// and every scalar function argument, which CAST itself did not keep for
// two types.
//
// DATE: Cast.Eval's string-family case read a DATE operand through
// ColRef.Eval's raw epoch-day int32 fast path instead of the column's own
// rendering, so `CAST(c_date AS STRING)` answered the epoch day ("15007")
// where the projection and LIKE (both already fixed, #497) answer the date
// ("2011-02-02"). Verified against live PostgreSQL 17: `date '2011-02-02'::
// text` answers "2011-02-02".
//
// FLOAT32: the same operand-boxing gap, found while fixing DATE.
// ColRef.Eval widens a FLOAT32 column to float64, so
// `CAST(c_f32 AS STRING)` printed the float64-widened digits
// ("0.1428571492433548") instead of the float32-shortest-round-trip form
// ("0.14285715") the projection and LIKE use. Verified against live
// PostgreSQL 17: a `real` column holding 1.0::real/7::real answers
// "0.14285715" for `::text`.
//
// Both are now fixed through one resolver (expr.boxedTextOperand), shared
// with LIKE's operand rendering instead of a second, narrower copy
// (networkOperand, IPv4/MAC only) — the two-implementation drift ADR-0012
// calls out elsewhere (CidrSortKey, appendColumnValue) for exactly this
// class of bug.

// TestCastStringMatchesProjectionForDateAndFloat32 pins the two fixed types
// to literal values, so a CAST implementation and a comparator that agreed
// on the same wrong answer would still fail the test.
func TestCastStringMatchesProjectionForDateAndFloat32(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	cases := []struct {
		name string
		sql  string
		want any
	}{
		{"date", fmt.Sprintf("SELECT CAST(c_date AS STRING) AS v FROM %s WHERE id = 1", typematrix.Table), "2011-02-02"},
		{"float32", fmt.Sprintf("SELECT CAST(c_f32 AS STRING) AS v FROM %s WHERE id = 7", typematrix.Table), "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmRun(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(res.Rows))
			}
			got := res.Rows[0]["v"]
			if got != tc.want {
				t.Errorf("%s = %#v, want %#v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestCastStringAgreesWithProjectionAcrossFixture sweeps every row of the
// type-matrix fixture (multiple scan batches — the vectorized EvalVec path
// falls back to per-row Eval for every scalar function including CAST, so
// this also exercises the batch boundary) and checks CAST(<col> AS STRING)
// against the same row's plain projection, for both fixed types.
func TestCastStringAgreesWithProjectionAcrossFixture(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	for _, col := range []string{"c_date", "c_f32"} {
		t.Run(col, func(t *testing.T) {
			res, err := tmRun(ctx, db, fmt.Sprintf(
				"SELECT id, %s AS v, CAST(%s AS STRING) AS s FROM %s WHERE %s IS NOT NULL ORDER BY id",
				col, col, typematrix.Table, col))
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(res.Rows) == 0 {
				t.Fatalf("no non-NULL rows for %s in the fixture", col)
			}
			mismatches := 0
			for _, r := range res.Rows {
				want := fmt.Sprint(r["v"])
				got, _ := r["s"].(string)
				if got != want {
					mismatches++
					if mismatches <= 5 {
						t.Errorf("id %v: CAST(%s AS STRING) = %q, projection renders %q", r["id"], col, got, want)
					}
				}
			}
			if mismatches > 5 {
				t.Errorf("... and %d more mismatches", mismatches-5)
			}
		})
	}
}
