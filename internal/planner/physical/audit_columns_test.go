package physical

import (
	"sort"
	"strings"
	"testing"
)

// TestAuditStageColumns dumps every stage's Columns slice for Q21/Q18 and
// flags any planner-level leak of lineitem columns the query doesn't use.
// Used during the 2026-05-03 column-pruning audit.
//
// Finding: planner-level Columns are well-pruned (no l_comment leaks at
// stage boundaries). The real bytes-through-pipeline leak is at the worker
// runtime, NOT at the planner — distributed hash_join workers don't propagate
// stage.Columns into the probe's OutputFilter, so the join emits the FULL
// union of build+probe schemas regardless of what's needed downstream. See
// project_perf_pass_planner_column_pruning_2026-05-03.md for the full audit.
//
// Run:
//   go test -v -run TestAuditStageColumns ./internal/planner/physical/
func TestAuditStageColumns(t *testing.T) {
	const q18 = `SELECT
		c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
		SUM(l_quantity) as total_qty
	FROM customer
	JOIN orders ON c_custkey = o_custkey
	JOIN lineitem ON o_orderkey = l_orderkey
	WHERE o_orderkey IN (
		SELECT l_orderkey
		FROM lineitem
		GROUP BY l_orderkey
		HAVING SUM(l_quantity) > 300
	)
	GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
	ORDER BY o_totalprice DESC, o_orderdate
	LIMIT 100`

	cases := []struct {
		name     string
		sql      string
		liNeeded []string // lineitem cols actually referenced
	}{
		{"Q21", tpchPlanQueries["Q21"], []string{"l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"}},
		{"Q18", q18, []string{"l_orderkey", "l_quantity"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auditQueryWithSQL(t, tc.name, tc.sql, tc.liNeeded)
		})
	}
}

func auditQueryWithSQL(t *testing.T, queryKey, sql string, liNeeded []string) {
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStages(t, cat, ctx, sql, 3)

	neededSet := make(map[string]bool, len(liNeeded))
	for _, c := range liNeeded {
		neededSet[c] = true
	}

	// Lineitem columns the query does NOT reference.
	allLineitem := []string{
		"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
		"l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
		"l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
		"l_shipmode", "l_comment",
	}
	leakSet := make(map[string]bool, len(allLineitem))
	for _, c := range allLineitem {
		if !neededSet[c] {
			leakSet[c] = true
		}
	}

	t.Logf("=== %s STAGE COLUMNS AUDIT (%d stages) ===", queryKey, len(stages))
	t.Logf("%s needs lineitem cols: %v", queryKey, liNeeded)
	t.Logf("")

	leakCount := 0
	for _, s := range stages {
		colsCopy := append([]string(nil), s.Columns...)
		sort.Strings(colsCopy)
		desc := s.Type
		if s.TableName != "" {
			desc += " table=" + s.TableName
		}
		if s.ScanAlias != "" && s.ScanAlias != s.TableName {
			desc += " alias=" + s.ScanAlias
		}
		if s.Exchange != nil && len(s.Exchange.Keys) > 0 {
			desc += " shuffleKeys=" + strings.Join(s.Exchange.Keys, ",")
		}
		t.Logf("STAGE %-22s [%s]", s.ID, desc)
		if len(colsCopy) == 0 {
			t.Logf("  Columns: <nil> (SELECT *)")
		} else {
			t.Logf("  Columns (%d): %v", len(colsCopy), colsCopy)
		}

		// Flag leaks.
		var leaks []string
		for _, c := range colsCopy {
			if leakSet[c] {
				leaks = append(leaks, c)
			}
		}
		if len(leaks) > 0 {
			leakCount++
			t.Logf("  *** LEAK: stage carries %d lineitem cols %s doesn't use: %v", len(leaks), queryKey, leaks)
		}
		t.Logf("")
	}
	t.Logf("=== %s SUMMARY: %d stages with leaked lineitem cols ===", queryKey, leakCount)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
