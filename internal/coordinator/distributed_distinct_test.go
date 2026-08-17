package coordinator

import (
	"fmt"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// Regression tests for two distributed wrong-answer classes found by the
// #288 differential's extended campaign (seeds 154/156/194), pinned with
// literal SQL and concrete expected values.

// COUNT(DISTINCT x) degenerated to COUNT(x) on every distributed path
// (#291): walkStages dropped logical.AggExpr.Distinct, and the two-phase
// partial/merge shape can't carry it anyway (per-task distinct counts
// don't sum). Distinct aggregates now dispatch as a single
// RawInputAggregate final over raw rows, with the canonical
// "count_distinct" Func string the worker already maps, and
// dispatchFinalAggregateFanout refuses to re-split raw finals.
func TestDistributedCountDistinct(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	cases := []struct {
		sql  string
		col  string
		want int64
	}{
		// #291 minimal repro: was 60000 (total join rows).
		{"SELECT COUNT(DISTINCT l_linestatus) AS d FROM supplier JOIN lineitem ON l_suppkey = s_suppkey", "d", 2},
		// #291 second repro: was 8000 (total partsupp rows).
		{"SELECT COUNT(DISTINCT p_name) AS d FROM partsupp JOIN part ON ps_partkey = p_partkey", "d", 1997},
		// Fused-scan path (no join) — was also wrong (60000).
		{"SELECT COUNT(DISTINCT l_linestatus) AS d FROM lineitem", "d", 2},
	}
	for _, tc := range cases {
		res, err := coord.ExecuteSQL(ctx, tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if res.Error != "" {
			t.Fatalf("%s: %s", tc.sql, res.Error)
		}
		rows := mustRows(t, res)
		if len(rows) != 1 {
			t.Fatalf("%s: got %d rows, want 1", tc.sql, len(rows))
		}
		if got := fmt.Sprint(rows[0][tc.col]); got != fmt.Sprint(tc.want) {
			t.Errorf("%s: %s = %s, want %d", tc.sql, tc.col, got, tc.want)
		}
	}

	// Mixed distinct + plain aggregates in one grouped query: the raw
	// final must keep both exact (the partial-dedup alternative would
	// corrupt COUNT(*)/MIN).
	res, err := coord.ExecuteSQL(ctx,
		"SELECT l_returnflag, COUNT(DISTINCT l_suppkey) AS d, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag")
	if err != nil {
		t.Fatal(err)
	}
	rows := mustRows(t, res)
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(rows))
	}
	var totalC int64
	for _, r := range rows {
		if got := fmt.Sprint(r["d"]); got != "100" {
			t.Errorf("group %v: d = %s, want 100 (all suppliers)", r["l_returnflag"], got)
		}
		if c, ok := r["c"].(int64); ok {
			totalC += c
		}
	}
	if totalC != 60000 {
		t.Errorf("COUNT(*) sum across groups = %d, want 60000", totalC)
	}
}

// A scalar-subquery producer whose input is empty (all rows pruned) wrote
// no output files, and scalar substitution failed the whole query (#292)
// — standalone returns the correct result (COUNT over empty = 0). The
// worker's empty-source fragment short-circuit now exempts scalar
// aggregates, which must emit their identity row over zero input.
func TestDistributedEmptyScalarSubquery(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed test skipped in -short mode")
	}
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)

	res, err := coord.ExecuteSQL(ctx,
		"SELECT MIN(p_retailprice) AS a1 FROM orders JOIN lineitem ON l_orderkey = o_orderkey JOIN part ON l_partkey = p_partkey "+
			"HAVING COUNT(*) > (SELECT COUNT(*) * 0.3 FROM orders JOIN lineitem ON l_orderkey = o_orderkey JOIN part ON l_partkey = p_partkey "+
			"WHERE l_partkey BETWEEN 1999 AND 500 AND l_partkey <> 500)")
	if err != nil {
		t.Fatalf("query failed (was: producer stage emitted no output files): %v", err)
	}
	if res.Error != "" {
		t.Fatalf("query failed (was: producer stage emitted no output files): %s", res.Error)
	}
	rows := mustRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 (empty-subquery threshold 0 always passes)", len(rows))
	}
	v, ok := rows[0]["a1"].(float64)
	if !ok || math.Abs(v-900.2) > 0.01 {
		t.Errorf("a1 = %v, want ~900.2 (deterministic SF0.01 MIN)", rows[0]["a1"])
	}

	// Non-COUNT producer over empty input (#288 seed 237): SUM over zero
	// rows is NULL, the HAVING comparison against NULL is unknown, and the
	// query returns zero rows — same as standalone. Substituted as the
	// null literal at scalar extraction (COUNT producers instead emit
	// their identity row worker-side).
	res, err = coord.ExecuteSQL(ctx,
		"SELECT SUM(l_orderkey) AS a0 FROM part JOIN lineitem ON l_partkey = p_partkey "+
			"HAVING SUM(l_orderkey) > (SELECT SUM(l_orderkey) * 0.3 FROM part JOIN lineitem ON l_partkey = p_partkey "+
			"WHERE p_retailprice BETWEEN 1500.0 AND 950.0)")
	if err != nil {
		t.Fatalf("SUM-threshold query failed (was: producer stage emitted no output files): %v", err)
	}
	if res.Error != "" {
		t.Fatalf("SUM-threshold query failed (was: producer stage emitted no output files): %s", res.Error)
	}
	if rows := mustRows(t, res); len(rows) != 0 {
		t.Errorf("got %d rows, want 0 (comparison against NULL threshold is unknown)", len(rows))
	}
}
