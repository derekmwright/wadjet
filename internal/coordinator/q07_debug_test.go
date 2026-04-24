package coordinator

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

// TestQ07_SF01_DumpRows is an investigation aid for the Q07 SF0.1 native-DAG
// 1-row drift. It runs Q07 on both legacy and native-DAG paths and prints
// the full row sets sorted by group key so the diff can be inspected by eye.
// Skipped in -short. Not a correctness gate — TestTPCHNativeDAG_SF01 owns
// the gate, this file just helps debug.
func TestQ07_SF01_DumpRows(t *testing.T) {
	if testing.Short() {
		t.Skip("Q07 debug — skipped in -short")
	}

	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cat := coord.catalog

	const chunkSize = 100_000
	tableChunks := make(map[string][][]map[string]any)
	if err := tpch.GenerateChunked(tpch.SF01, chunkSize, func(table string, rows []map[string]any) error {
		cp := make([]map[string]any, len(rows))
		copy(cp, rows)
		tableChunks[table] = append(tableChunks[table], cp)
		return nil
	}); err != nil {
		t.Fatalf("GenerateChunked: %v", err)
	}
	for _, table := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
		ingestTPCHTableChunked(t, ctx, store, cat, table, tpch.AllTables[table], tableChunks[table])
		tableChunks[table] = nil
		runtime.GC()
	}

	dump := func(label, sql string, useNative bool, maxRows int) []string {
		coord.UseNativeDAG = useNative
		defer func() { coord.UseNativeDAG = false }()
		res, err := coord.ExecuteSQL(ctx, sql)
		if err != nil {
			t.Fatalf("%s ExecuteSQL: %v", label, err)
		}
		var rendered []string
		for _, row := range res.Rows() {
			rendered = append(rendered, fmt.Sprintf("%v", row))
		}
		sort.Strings(rendered)
		fmt.Printf("=== %s | rows=%d cols=%v ===\n", label, len(rendered), res.Columns)
		shown := rendered
		if maxRows > 0 && len(shown) > maxRows {
			shown = shown[:maxRows]
		}
		for _, r := range shown {
			fmt.Printf("  %s\n", r)
		}
		if len(shown) < len(rendered) {
			fmt.Printf("  ... (%d more)\n", len(rendered)-len(shown))
		}
		return rendered
	}

	q07 := tpch.TPCHQueries[7].SQL
	legacy := dump("Q07_LEGACY", q07, false, 0)
	native := dump("Q07_NATIVE", q07, true, 0)

	aliasSQL := `SELECT n_name AS my_nation FROM nation WHERE n_name = 'FRANCE'`
	dump("ALIAS_LEGACY", aliasSQL, false, 5)
	dump("ALIAS_NATIVE", aliasSQL, true, 5)

	selfSQL := `SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation
		FROM nation n1
		JOIN nation n2 ON n1.n_nationkey != n2.n_nationkey
		WHERE n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'`
	dump("SELFJOIN_LEGACY", selfSQL, false, 5)
	dump("SELFJOIN_NATIVE", selfSQL, true, 5)

	// Q07 with explicit projection of the raw qualified columns — drops the
	// AS aliases so we can see whether the bug is purely the alias layer
	// or the underlying column also goes missing.
	q07Raw := `SELECT
			n1.n_name,
			n2.n_name,
			SUBSTR(l_shipdate, 1, 4),
			SUM(l_extendedprice * (1 - l_discount)) AS revenue
		FROM supplier
		JOIN lineitem ON s_suppkey = l_suppkey
		JOIN orders ON o_orderkey = l_orderkey
		JOIN customer ON c_custkey = o_custkey
		JOIN nation n1 ON s_nationkey = n1.n_nationkey
		JOIN nation n2 ON c_nationkey = n2.n_nationkey
		WHERE n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'
			AND l_shipdate >= '1995-01-01' AND l_shipdate <= '1996-12-31'
		GROUP BY n1.n_name, n2.n_name, SUBSTR(l_shipdate, 1, 4)`
	dump("Q07RAW_LEGACY", q07Raw, false, 0)
	dump("Q07RAW_NATIVE", q07Raw, true, 0)

	// Q08 also has nation n1 + nation n2. Does Bug B (n1 column nulled)
	// reproduce there? If yes, same root cause; if no, Q07's join chain
	// is the trigger.
	q08 := tpch.TPCHQueries[8].SQL
	dump("Q08_LEGACY", q08, false, 0)
	dump("Q08_NATIVE", q08, true, 0)

	// Symmetric diff to highlight which rows are unique to each side.
	in := func(s []string, k string) bool {
		for _, x := range s {
			if x == k {
				return true
			}
		}
		return false
	}
	t.Logf("--- ROWS ONLY IN NATIVE-DAG ---")
	for _, r := range native {
		if !in(legacy, r) {
			t.Logf("  + %s", r)
		}
	}
	t.Logf("--- ROWS ONLY IN LEGACY ---")
	for _, r := range legacy {
		if !in(native, r) {
			t.Logf("  - %s", r)
		}
	}
}
