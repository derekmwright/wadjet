package wadjet

import (
	"context"
	"testing"
)

// TestLimitZero is the single-process-path regression suite for #481:
// `SELECT id, c_dec FROM mbtypes ORDER BY id LIMIT 0` returned all 5000
// rows instead of 0. The root cause was a sentinel collision — 0 doubled
// as both "a real LIMIT 0" and "no limit at all" across exec.Sort.Limit,
// sortSourceAdapter's Top-K guard, and the coordinator's MergeInfo.KeepRows
// — so the broken shape was specifically `ORDER BY ... LIMIT 0` with no
// OFFSET; plain `LIMIT 0` (no ORDER BY) and `LIMIT 0 OFFSET n` already
// worked because they never touched the Sort/Top-K "0 = no limit"
// convention. This table pins every shape from the issue's own repro plus
// the ones the fix reaches (subqueries, derived tables).
func TestLimitZero(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	cases := []struct {
		name string
		sql  string
	}{
		{"order_by_limit_zero", "SELECT id, c_dec FROM mbtypes ORDER BY id LIMIT 0"},
		{"order_by_desc_limit_zero", "SELECT id, c_dec FROM mbtypes ORDER BY id DESC LIMIT 0"},
		{"plain_limit_zero_no_order_by", "SELECT id FROM mbtypes LIMIT 0"},
		{"limit_zero_offset_n", "SELECT id FROM mbtypes ORDER BY id LIMIT 0 OFFSET 10"},
		{"plain_limit_zero_offset_n_no_order_by", "SELECT id FROM mbtypes LIMIT 0 OFFSET 10"},
		{"limit_zero_multi_key_sort", "SELECT id, g, c_dec FROM mbtypes ORDER BY g, id DESC LIMIT 0"},
		{"limit_zero_where_and_order_by", "SELECT id FROM mbtypes WHERE id > 10 ORDER BY id LIMIT 0"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query error: %v", err)
			}
			if len(res.Rows) != 0 {
				t.Fatalf("%s: got %d rows, want 0", tc.sql, len(res.Rows))
			}
		})
	}
}

// TestLimitZero_DerivedTableOrderByCount pins the derived-table shape's
// VALUE: `SELECT COUNT(*) FROM (... ORDER BY ... LIMIT 0) u` always returns
// exactly one row (a scalar aggregate has no GROUP BY), so the assertion
// that matters is the COUNT value, not the row count. An `ORDER BY` inside
// the derived table produces a sort/merge_sort stage on the DAG path, and
// #481's fix reaches that stage's Limit/HasLimit exactly the same way it
// reaches a top-level `ORDER BY ... LIMIT 0` — unlike a bare (no ORDER BY)
// derived-table LIMIT, which is #478's separate, still-open bug (the DAG
// emits no stage at all for it) and is deliberately not exercised here.
func TestLimitZero_DerivedTableOrderByCount(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	res, err := db.Query(ctx, "SELECT COUNT(*) AS c FROM (SELECT id FROM mbtypes ORDER BY id LIMIT 0) u")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected exactly 1 row (scalar COUNT), got %d", len(res.Rows))
	}
	if got := res.Rows[0]["c"]; got != int64(0) {
		t.Errorf("COUNT(*) over a derived table's ORDER BY ... LIMIT 0 = %v, want 0", got)
	}
}

// TestLimitZero_ScalarSubqueryReturnsNull pins the scalar-subquery shape's
// VALUE, not just its row count: `(SELECT ... LIMIT 0)` as a scalar
// subquery must yield SQL NULL for the enclosing row (PostgreSQL's rule for
// a scalar subquery with zero rows), not an error or a stale value.
func TestLimitZero_ScalarSubqueryReturnsNull(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	res, err := db.Query(ctx, "SELECT (SELECT id FROM mbtypes ORDER BY id LIMIT 0) AS sub")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected exactly 1 row (the outer scalar row), got %d", len(res.Rows))
	}
	if res.Rows[0]["sub"] != nil {
		t.Errorf("scalar subquery over LIMIT 0 = %v, want NULL", res.Rows[0]["sub"])
	}
}

// TestLimitZero_NonZeroStillWorks guards against an off-by-one in the
// LIMIT-0 fix: a real, positive LIMIT over the same ORDER BY must still
// return exactly that many rows, correctly sorted.
func TestLimitZero_NonZeroStillWorks(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	res, err := db.Query(ctx, "SELECT id FROM mbtypes ORDER BY id LIMIT 5")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(res.Rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(res.Rows))
	}
	var prev int64 = -1
	for i, row := range res.Rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("row %d: id is %T, want int64", i, row["id"])
		}
		if id <= prev {
			t.Fatalf("row %d: id=%d not ascending after %d", i, id, prev)
		}
		prev = id
	}
}
