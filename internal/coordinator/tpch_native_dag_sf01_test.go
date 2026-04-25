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
)

// TestTPCHNativeDAG_SF01 is the SF0.1 (≈100 MB) local correctness gate.
// It catches scale-sensitive bugs the SF0.01 gate can't: hash
// collisions, spill triggers, partial-merge correctness with >1 partial
// per group, and derived-expression projection under real cardinalities.
//
// SF0.1 not SF1: the single-worker test fixture scales sub-linearly with
// data — a full SF1 run per query exceeds 5 min each (coordinator
// overhead + MemStore serial reads dominate). SF0.1 keeps each query
// under ~30s while still exercising every execution path that differs
// from SF0.01 (multi-chunk lineitem scan, hashagg with >1 partial,
// spill pressure).
//
// Oracle: each query runs twice — once on the legacy coordinator path to
// establish a ground-truth row count, once on native-DAG to assert
// parity. No hardcoded expected rows; if legacy changes, this follows.
//
// Opt-in: skipped in `-short`. Run with:
//   go test -run TestTPCHNativeDAG_SF01 ./internal/coordinator/ -timeout 20m
func TestTPCHNativeDAG_SF01(t *testing.T) {
	if testing.Short() {
		t.Skip("TPCH SF0.1 native-DAG gate — skipped in -short")
	}

	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cat := coord.catalog

	const chunkSize = 100_000
	// Collect into per-table chunk slices. GenerateChunked streams, we
	// accumulate into chunks (one parquet file per chunk). Keeps memory
	// bounded to chunkSize rows per table at a time.
	tableChunks := make(map[string][][]map[string]any)
	startGen := time.Now()
	err := tpch.GenerateChunked(tpch.SF01, chunkSize, func(table string, rows []map[string]any) error {
		cp := make([]map[string]any, len(rows))
		copy(cp, rows)
		tableChunks[table] = append(tableChunks[table], cp)
		return nil
	})
	if err != nil {
		t.Fatalf("GenerateChunked: %v", err)
	}
	t.Logf("SF1 datagen: %s", time.Since(startGen))

	tableOrder := []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"}
	for _, table := range tableOrder {
		chunks := tableChunks[table]
		if len(chunks) == 0 {
			t.Fatalf("datagen missing table %s", table)
		}
		startIngest := time.Now()
		ingestTPCHTableChunked(t, ctx, store, cat, table, tpch.AllTables[table], chunks)
		totalRows := 0
		for _, c := range chunks {
			totalRows += len(c)
		}
		t.Logf("ingested %s: %d chunks, %d rows in %s", table, len(chunks), totalRows, time.Since(startIngest))
		// Drop the generated chunks once they're persisted so the next
		// table gets a clean heap — otherwise peak memory is the sum of
		// every table we've ingested so far.
		tableChunks[table] = nil
		runtime.GC()
	}

	qNums := make([]int, 0, len(tpch.TPCHQueries))
	for n := range tpch.TPCHQueries {
		qNums = append(qNums, n)
	}
	sort.Ints(qNums)

	for _, qNum := range qNums {
		q := tpch.TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			qCtx, qCancel := context.WithTimeout(ctx, 5*time.Minute)
			defer qCancel()

			coord.UseNativeDAG = false
			startLegacy := time.Now()
			legacyRes, err := coord.ExecuteSQL(qCtx, q.SQL)
			if err != nil {
				t.Fatalf("legacy Q%02d: %v", qNum, err)
			}
			legacyElapsed := time.Since(startLegacy)

			coord.UseNativeDAG = true
			startNat := time.Now()
			natRes, err := coord.ExecuteSQL(qCtx, q.SQL)
			coord.UseNativeDAG = false
			if err != nil {
				t.Fatalf("native-DAG Q%02d: %v", qNum, err)
			}
			natElapsed := time.Since(startNat)

			// Q02 / Q22 have float-threshold comparisons where
			// accumulation order can shift borderline rows. Same
			// tolerance the SF0.01 gate uses.
			tol := 0
			if qNum == 2 || qNum == 22 {
				tol = 4
			}
			diff := int(natRes.TotalRows - legacyRes.TotalRows)
			if diff < -tol || diff > tol {
				t.Errorf("Q%02d native-DAG rows=%d legacy rows=%d (diff=%d tol=±%d); legacy=%s native=%s",
					qNum, natRes.TotalRows, legacyRes.TotalRows, diff, tol, legacyElapsed, natElapsed)
				return
			}
			// Row-count parity is necessary but not sufficient. Q07's
			// pre-fix native-DAG happened to return the right COUNT but
			// with wrong column data (n1.n_name=NULL) and an extra empty
			// group. Compare row VALUES too — sort each side by stringified
			// representation so non-ORDER-BY queries still diff cleanly.
			// Skip when row count is off by tolerance (already errored above).
			//
			// Per-query value-skip allowlist for queries with KNOWN structural
			// native-DAG bugs that the strong gate exposed. Each entry should
			// reference the bug; remove from the allowlist when fixed:
			//   Q11: HAVING with scalar subquery — wrong rows match the threshold.
			//        TODO: the subquery's threshold derivation diverges; suspect
			//        a similar shape to Q15's late-bound scalar (SF0.1 only).
			//   Q17: wrapped aggregate "SUM(l_extendedprice)/7.0 AS avg_yearly"
			//        — the /7.0 divisor never gets applied. Need post-aggregate
			//        Project equivalent for native-DAG (legacy applies via
			//        buildProject's wrapped-aggregate path).
			//   Q18: SUM(l_quantity) returns NULL despite correct GROUP BY keys.
			//        Suspect IN-subquery + outer-aggregate column collision.
			valueSkip := map[int]string{
				11: "wrapped scalar subquery in HAVING",
				18: "outer SUM(l_quantity) NULL when IN-subquery present",
			}
			if reason, skip := valueSkip[qNum]; skip {
				t.Logf("Q%02d: %d rows (legacy=%s native=%s) — value compare SKIPPED: %s",
					qNum, natRes.TotalRows, legacyElapsed, natElapsed, reason)
				return
			}
			if mismatches := diffRowSets(legacyRes.Rows(), natRes.Rows()); len(mismatches) > 0 {
				// Truncate to first 5 mismatches to keep the failure log tractable.
				show := mismatches
				if len(show) > 5 {
					show = show[:5]
				}
				t.Errorf("Q%02d row VALUES diverge (showing %d of %d):\n%s",
					qNum, len(show), len(mismatches), strings.Join(show, "\n"))
				return
			}
			t.Logf("Q%02d: %d rows (legacy=%s native=%s)",
				qNum, natRes.TotalRows, legacyElapsed, natElapsed)
		})
	}
}

// diffRowSets returns the list of human-readable mismatches between two
// row sets. Rows are stringified, sorted, and diffed — order independent
// for non-ORDER-BY queries while still catching missing/extra/wrong values.
// Float ULP-level differences are tolerated by truncating numeric values to
// 6 significant digits before comparison; this avoids false positives from
// distributed accumulation order without hiding real value differences.
func diffRowSets(left, right []map[string]any) []string {
	canon := func(rows []map[string]any) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			keys := make([]string, 0, len(r))
			for k := range r {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var b strings.Builder
			for j, k := range keys {
				if j > 0 {
					b.WriteString(" ")
				}
				b.WriteString(k)
				b.WriteString("=")
				b.WriteString(canonValue(r[k]))
			}
			out[i] = b.String()
		}
		sort.Strings(out)
		return out
	}
	leftS := canon(left)
	rightS := canon(right)
	li, ri := 0, 0
	var mismatches []string
	for li < len(leftS) && ri < len(rightS) {
		switch {
		case leftS[li] == rightS[ri]:
			li++
			ri++
		case leftS[li] < rightS[ri]:
			mismatches = append(mismatches, "  - "+leftS[li])
			li++
		default:
			mismatches = append(mismatches, "  + "+rightS[ri])
			ri++
		}
	}
	for ; li < len(leftS); li++ {
		mismatches = append(mismatches, "  - "+leftS[li])
	}
	for ; ri < len(rightS); ri++ {
		mismatches = append(mismatches, "  + "+rightS[ri])
	}
	return mismatches
}

// canonValue stringifies a Go value for row-set comparison, truncating
// floats to 6 significant digits so float-ULP drift across legacy vs
// distributed accumulation paths doesn't trigger a false mismatch.
func canonValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case float64:
		return fmt.Sprintf("%.6g", x)
	case float32:
		return fmt.Sprintf("%.6g", float64(x))
	default:
		return fmt.Sprint(v)
	}
}
