package coordinator

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

var updateSF01Golden = flag.Bool("update-sf01-golden", false, "update TPCH SF0.1 native-DAG golden snapshots")

// TestTPCHNativeDAG_SF01 is the SF0.1 (≈100 MB) local correctness gate
// for the native-DAG executor. Each Q01-Q22 result is compared against a
// checked-in golden file. The test is the OUTPUT contract for native-DAG
// at this scale — if results drift, either the golden was wrong (regen
// with -update-sf01-golden) or the engine introduced a regression.
//
// Oracle: golden snapshots in testdata/sf01_native_dag/*.golden. Native-
// DAG output is canonicalized (sorted rows, 6-sig-digit float
// truncation) so float ULP drift across runs doesn't cause spurious
// diffs while real value differences still surface.
//
// Why golden, not cross-path: the previous design ran each query
// through both legacy and native-DAG and compared row sets. That worked
// until distributed-vs-single-process float reduction order produced
// ULP-different per-group SUMs at SF0.1 scale (Q11 root cause), making
// cross-path comparison fundamentally fragile. Golden snapshots assert
// against a fixed expected output instead — drift surfaces a real
// regression rather than a "two paths reduce in different order" wash.
//
// SF0.1 not SF1: the single-worker test fixture scales sub-linearly with
// data — a full SF1 run per query exceeds 5 min each (coordinator
// overhead + MemStore serial reads dominate). SF0.1 keeps each query
// under ~30s while still exercising every execution path that differs
// from SF0.01.
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

			start := time.Now()
			res, err := coord.ExecuteSQL(qCtx, q.SQL)
			if err != nil {
				t.Fatalf("native-DAG Q%02d: %v", qNum, err)
			}
			elapsed := time.Since(start)

			snap := canonResultSnapshot(res.Rows())
			goldenPath := filepath.Join("testdata", "sf01_native_dag", fmt.Sprintf("q%02d.golden", qNum))
			if *updateSF01Golden {
				if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
					t.Fatalf("mkdir %s: %v", goldenPath, err)
				}
				if err := os.WriteFile(goldenPath, []byte(snap), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				t.Logf("Q%02d: %d rows in %s — wrote golden", qNum, res.TotalRows, elapsed)
				return
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden %s: %v (regen with -update-sf01-golden)", goldenPath, err)
			}
			if snap != string(want) {
				diff := snapshotDiff(string(want), snap)
				t.Errorf("Q%02d snapshot diff (rows=%d, %s):\n%s\n(regen with -update-sf01-golden if intentional)",
					qNum, res.TotalRows, elapsed, diff)
				return
			}
			t.Logf("Q%02d: %d rows in %s", qNum, res.TotalRows, elapsed)
		})
	}
}

// canonResultSnapshot returns a stable string representation of a row set
// suitable for golden comparison. Rows are stringified with sorted keys,
// floats truncated to 6 significant digits to absorb cross-run ULP drift,
// and the rows are sorted alphabetically so query result order doesn't
// matter for non-ORDER-BY queries.
func canonResultSnapshot(rows []map[string]any) string {
	canon := make([]string, len(rows))
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
		canon[i] = b.String()
	}
	sort.Strings(canon)
	return strings.Join(canon, "\n") + "\n"
}

// snapshotDiff renders a small unified-style diff between two snapshot
// strings, truncated to keep failure logs tractable.
func snapshotDiff(want, got string) string {
	wantLines := strings.Split(strings.TrimRight(want, "\n"), "\n")
	gotLines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	wantSet := make(map[string]bool, len(wantLines))
	for _, l := range wantLines {
		wantSet[l] = true
	}
	gotSet := make(map[string]bool, len(gotLines))
	for _, l := range gotLines {
		gotSet[l] = true
	}
	var diff []string
	for _, l := range wantLines {
		if !gotSet[l] {
			diff = append(diff, "  - "+l)
		}
	}
	for _, l := range gotLines {
		if !wantSet[l] {
			diff = append(diff, "  + "+l)
		}
	}
	if len(diff) > 10 {
		diff = append(diff[:10], fmt.Sprintf("  ... (%d more)", len(diff)-10))
	}
	return strings.Join(diff, "\n")
}

// canonValue stringifies a Go value for snapshot comparison, truncating
// floats to 6 significant digits so distributed accumulation-order ULP
// drift doesn't trigger a false mismatch. Used by canonResultSnapshot.
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
