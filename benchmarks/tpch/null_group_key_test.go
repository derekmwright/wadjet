package tpch

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/wadjet"
)

// #338: GROUP BY over a NULL key reported the group once per parallel
// partial instead of once.
//
//	SELECT o.o_orderstatus AS s, COUNT(*) AS c
//	FROM customer c LEFT JOIN orders o
//	  ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0
//	GROUP BY o.o_orderstatus
//
// answered with FOUR rows of (NULL, 375) where the answer is ONE row of
// (NULL, 1500). 4 × 375 = 1500 exactly: no row was lost, the group was
// split across partial states that were never merged into one another —
// which is why every row-count check, every total, and the whole SF0.01
// correctness suite stayed green through it.
//
// Two independent defects produce that, and both are fixed here:
//
//   - the in-memory merge could not match a NULL key to another NULL key.
//     The single-string GROUP BY path keeps its NULL group out of the string
//     hash table on purpose (a real one-byte "\x01" key would collide with
//     the sentinel), so the merge's table probe never matched it and each
//     merge appended one more NULL group.
//   - partitioned aggregation gives each worker a disjoint slice of the key
//     space, and its merge ADOPTS each worker's state whole on that promise.
//     A batch whose key the router cannot hash — here the group column is
//     not in the join's output at all — is instead consumed by whichever
//     worker pulled it, so the same key lands in several workers and every
//     group in such a batch was emitted once per worker.
//
// Input size is the whole reason this hid: partials only exist once a table
// is big enough to fan out. customer (1500 rows) shows it; nation (25) and
// supplier (100) do not, and the small-input controls at the bottom pin that
// those shapes were and stay correct.
func TestNullGroupKeyNotSplitAcrossPartials(t *testing.T) {
	ctx := context.Background()
	db := setupTPCH(t, SF001)

	// nullKeyJoin is the reduction: the ON clause's o_orderkey < 0 matches no
	// order at all, so every customer row survives the LEFT JOIN with the
	// whole right side NULL.
	nullKeyJoin := func(selectList, tail string) string {
		return "SELECT " + selectList + " FROM customer c LEFT JOIN orders o" +
			" ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0 " + tail
	}

	t.Run("issue repro: one NULL group of 1500", func(t *testing.T) {
		rows := query(t, ctx, db, nullKeyJoin("o.o_orderstatus AS s, COUNT(*) AS c", "GROUP BY o.o_orderstatus"))
		if len(rows) != 1 {
			t.Fatalf("got %d rows, want 1 — every key is NULL and SQL groups all NULLs together: %v", len(rows), rows)
		}
		if rows[0]["s"] != nil {
			t.Errorf("s = %v, want NULL", rows[0]["s"])
		}
		// Assert the VALUE, not just the row count: the failure preserved
		// the total (4 × 375), so only the per-group count names it.
		if got := rows[0]["c"]; got != int64(1500) {
			t.Errorf("COUNT(*) = %v, want 1500 (partial groups not merged)", got)
		}
	})

	t.Run("int group key", func(t *testing.T) {
		rows := query(t, ctx, db, nullKeyJoin("o.o_shippriority AS s, COUNT(*) AS c", "GROUP BY o.o_shippriority"))
		if len(rows) != 1 || rows[0]["s"] != nil || rows[0]["c"] != int64(1500) {
			t.Fatalf("got %v, want one row of (NULL, 1500)", rows)
		}
	})

	t.Run("multi-column key, every column NULL", func(t *testing.T) {
		rows := query(t, ctx, db, nullKeyJoin(
			"o.o_orderstatus AS s, o.o_orderpriority AS p, COUNT(*) AS c",
			"GROUP BY o.o_orderstatus, o.o_orderpriority"))
		if len(rows) != 1 {
			t.Fatalf("got %d rows, want 1: %v", len(rows), rows)
		}
		if rows[0]["s"] != nil || rows[0]["p"] != nil {
			t.Errorf("keys = (%v, %v), want (NULL, NULL)", rows[0]["s"], rows[0]["p"])
		}
		if got := rows[0]["c"]; got != int64(1500) {
			t.Errorf("COUNT(*) = %v, want 1500", got)
		}
	})

	// The case a fix narrowed to "the key is entirely NULL" would miss: one
	// key column carries a real value and the other is NULL, so the answer is
	// one row per market segment and the NULL half must not split any of them.
	t.Run("multi-column key, one column NULL", func(t *testing.T) {
		rows := query(t, ctx, db, nullKeyJoin(
			"c.c_mktsegment AS m, o.o_orderstatus AS s, COUNT(*) AS c",
			"GROUP BY c.c_mktsegment, o.o_orderstatus"))
		if len(rows) != 5 {
			t.Fatalf("got %d rows, want 5 (one per market segment): %v", len(rows), rows)
		}
		seen := map[string]bool{}
		total := int64(0)
		for _, r := range rows {
			if r["s"] != nil {
				t.Errorf("s = %v, want NULL", r["s"])
			}
			seg, _ := r["m"].(string)
			if seen[seg] {
				t.Errorf("market segment %q reported twice — its (segment, NULL) group was split across partials", seg)
			}
			seen[seg] = true
			total += r["c"].(int64)
		}
		if total != 1500 {
			t.Errorf("counts total %d, want 1500", total)
		}
	})

	// DISTINCT is the same rule on a different code path: it plans as a
	// GROUP BY over every output column (GroupByAll), which partitioned
	// aggregation deliberately never takes — so here the merge alone decides.
	t.Run("distinct", func(t *testing.T) {
		rows := query(t, ctx, db, nullKeyJoin("DISTINCT o.o_orderstatus AS s", ""))
		if len(rows) != 1 {
			t.Fatalf("DISTINCT over an all-NULL column returned %d rows, want 1: %v", len(rows), rows)
		}
		if rows[0]["s"] != nil {
			t.Errorf("s = %v, want NULL", rows[0]["s"])
		}
	})

	// A partially matching join, so the NULL group is aggregated alongside
	// real ones. Both halves have to survive: one NULL row, and the real
	// groups unsplit.
	t.Run("null group beside real groups", func(t *testing.T) {
		rows := query(t, ctx, db, `SELECT o.o_orderstatus AS s, COUNT(*) AS c FROM customer c
			LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 5
			GROUP BY o.o_orderstatus`)
		nulls, total, seen := 0, int64(0), map[string]bool{}
		for _, r := range rows {
			if r["s"] == nil {
				nulls++
				if got := r["c"]; got != int64(1496) {
					t.Errorf("NULL group COUNT(*) = %v, want 1496", got)
				}
			} else {
				s := r["s"].(string)
				if seen[s] {
					t.Errorf("group %q reported twice", s)
				}
				seen[s] = true
			}
			total += r["c"].(int64)
		}
		if nulls != 1 {
			t.Errorf("the NULL group appeared %d times, want exactly 1: %v", nulls, rows)
		}
		if total != 1500 {
			t.Errorf("counts total %d, want 1500", total)
		}
	})

	// Small-input controls. These shapes were CORRECT before the fix — an
	// input this small never fans out into parallel partials, so there were
	// no partials to leave unmerged — and they must stay correct: a fix that
	// changed small-table behaviour would be changing something other than
	// the merge.
	t.Run("small input control: nation (25 rows)", func(t *testing.T) {
		rows := query(t, ctx, db, `SELECT r.r_name AS rn, COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0
			GROUP BY r.r_name`)
		if len(rows) != 1 || rows[0]["rn"] != nil || rows[0]["c"] != int64(25) {
			t.Fatalf("got %v, want one row of (NULL, 25)", rows)
		}
	})

	t.Run("small input control: supplier (100 rows)", func(t *testing.T) {
		rows := query(t, ctx, db, `SELECT n.n_name AS nn, COUNT(*) AS c FROM supplier s
			LEFT JOIN nation n ON s.s_nationkey = n.n_nationkey AND n.n_nationkey < 0
			GROUP BY n.n_name`)
		if len(rows) != 1 || rows[0]["nn"] != nil || rows[0]["c"] != int64(100) {
			t.Fatalf("got %v, want one row of (NULL, 100)", rows)
		}
	})

	// Control on the other side: the same GROUP BY with no NULL anywhere.
	// Three statuses over 15000 orders, which is plenty of partials — so a
	// merge that duplicated groups in general (rather than only NULL ones)
	// would show up here.
	t.Run("control: no NULLs, same scale", func(t *testing.T) {
		rows := query(t, ctx, db, "SELECT o_orderstatus AS s, COUNT(*) AS c FROM orders GROUP BY o_orderstatus")
		if len(rows) != 3 {
			t.Fatalf("got %d rows, want 3 (F, O, P): %v", len(rows), rows)
		}
		total := int64(0)
		for _, r := range rows {
			total += r["c"].(int64)
		}
		if total != 15000 {
			t.Errorf("counts total %d, want 15000", total)
		}
	})
}

func query(t *testing.T, ctx context.Context, db *wadjet.DB, sql string) []map[string]any {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query failed: %v\n  SQL: %s", err, sql)
	}
	return res.Rows
}
