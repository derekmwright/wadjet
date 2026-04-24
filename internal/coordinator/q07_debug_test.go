package coordinator

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// dumpStages prints the native-DAG stage list for a query without executing.
// Useful for spotting plan-shape issues that produce wrong runtime output.
func dumpStages(t *testing.T, c *Coordinator, ctx context.Context, label, sql string) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Logf("%s: parse: %v", label, err)
		return
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Logf("%s: extract: %v", label, err)
		return
	}
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Logf("%s: build logical: %v", label, err)
		return
	}
	planner := physical.NewPlanner(c.catalog)
	planner.WorkerCount = c.workers.Count()
	planner.UseEnsureDistribution = true
	scanAnnotator := func(plan *logical.Node) { planner.AnnotateScanColumns(ctx, plan) }
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Logf("%s: PlanDistributed: %v", label, err)
		return
	}
	fmt.Printf("=== %s | %d stages ===\n", label, len(stages))
	for _, s := range stages {
		var bits []string
		bits = append(bits, fmt.Sprintf("id=%s type=%s tasks=%d", s.ID, s.Type, s.Tasks))
		if len(s.Dependencies) > 0 {
			bits = append(bits, fmt.Sprintf("deps=%v", s.Dependencies))
		}
		if s.JoinType != "" {
			bits = append(bits, fmt.Sprintf("joinType=%s left=%v right=%v", s.JoinType, s.JoinLeftKeys, s.JoinRightKeys))
		}
		if s.BuildTableAlias != "" {
			bits = append(bits, fmt.Sprintf("buildAlias=%s", s.BuildTableAlias))
		}
		if len(s.GroupByCols) > 0 {
			bits = append(bits, fmt.Sprintf("groupBy=%v", s.GroupByCols))
		}
		if len(s.AggSpecs) > 0 {
			var aggs []string
			for _, a := range s.AggSpecs {
				aggs = append(aggs, fmt.Sprintf("%s(%s)->%s", a.Func, a.InputCol, a.OutputCol))
			}
			bits = append(bits, fmt.Sprintf("agg=[%s]", strings.Join(aggs, ",")))
		}
		if len(s.FilterExprs) > 0 {
			bits = append(bits, fmt.Sprintf("filters=%v", s.FilterExprs))
		}
		if len(s.FusedAggSpecs) > 0 {
			bits = append(bits, fmt.Sprintf("fusedAgg=%d", len(s.FusedAggSpecs)))
		}
		if s.TableName != "" {
			bits = append(bits, fmt.Sprintf("table=%s", s.TableName))
		}
		fmt.Printf("  %s\n", strings.Join(bits, " "))
	}
}

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

	q08 := tpch.TPCHQueries[8].SQL
	dump("Q08_LEGACY", q08, false, 0)
	dump("Q08_NATIVE", q08, true, 0)

	// Probe: simple GROUP BY, no joins. Does Bug C (extra empty group row)
	// reproduce on a single-table aggregate?
	groupSQL := `SELECT n_name, COUNT(*) AS c FROM nation GROUP BY n_name`
	dump("GROUP_LEGACY", groupSQL, false, 30)
	dump("GROUP_NATIVE", groupSQL, true, 30)

	// Probe: GROUP BY with a WHERE that filters out all nations on some
	// workers. If Bug C is caused by workers that produce zero rows still
	// emitting an empty placeholder, this should reproduce.
	groupFilterSQL := `SELECT n_name, COUNT(*) AS c FROM nation WHERE n_regionkey = 3 GROUP BY n_name`
	dump("GROUPFILT_LEGACY", groupFilterSQL, false, 30)
	dump("GROUPFILT_NATIVE", groupFilterSQL, true, 30)

	// Probe: Q07 minus the SELF-JOIN. One nation join only. If Bug C
	// (extra empty row) repros here, the self-join isn't the trigger.
	noSelfSQL := `SELECT n_name AS supp_nation, SUBSTR(l_shipdate, 1, 4) AS l_year,
			SUM(l_extendedprice * (1 - l_discount)) AS revenue
		FROM supplier
		JOIN lineitem ON s_suppkey = l_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name IN ('FRANCE','GERMANY')
		AND l_shipdate >= '1995-01-01' AND l_shipdate <= '1996-12-31'
		GROUP BY n_name, SUBSTR(l_shipdate, 1, 4)`
	dump("NOSELF_LEGACY", noSelfSQL, false, 0)
	dump("NOSELF_NATIVE", noSelfSQL, true, 0)

	// Q07 self-join WITHOUT the OR-WHERE. Just one nation pair direction.
	// If Bug C (extra empty row) disappears here, the OR disjunction is
	// the trigger. Q07RAW is similar but no GROUP BY OR — let's also
	// reuse the full Q07 GROUP BY with AND-only WHERE.
	q07ANDOnly := `SELECT
			n1.n_name AS supp_nation, n2.n_name AS cust_nation,
			SUBSTR(l_shipdate, 1, 4) AS l_year,
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
	dump("Q07AND_LEGACY", q07ANDOnly, false, 0)
	dump("Q07AND_NATIVE", q07ANDOnly, true, 0)

	// Q07 with the OR-WHERE but NO self-join. If Bug C still appears,
	// it's the OR-disjunction over join columns, not the self-join.
	q07ORNoSelf := `SELECT n_name AS supp_nation, SUBSTR(l_shipdate, 1, 4) AS l_year,
			SUM(l_extendedprice * (1 - l_discount)) AS revenue
		FROM supplier
		JOIN lineitem ON s_suppkey = l_suppkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE (n_name = 'FRANCE' OR n_name = 'GERMANY')
		AND l_shipdate >= '1995-01-01' AND l_shipdate <= '1996-12-31'
		GROUP BY n_name, SUBSTR(l_shipdate, 1, 4)`
	dump("Q07OR_NOSELF_LEGACY", q07ORNoSelf, false, 0)
	dump("Q07OR_NOSELF_NATIVE", q07ORNoSelf, true, 0)

	// Dump native-DAG stages for Q07 so we can inspect the plan shape.
	dumpStages(t, coord, ctx, "Q07_STAGES", q07)
	dumpStages(t, coord, ctx, "Q07AND_STAGES", q07ANDOnly)

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
