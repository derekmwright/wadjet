package harness

import (
	"path/filepath"
	"testing"
)

// The local/small gate lost its default baseline when the SF100 file
// became a row-count oracle (da5301a): comparing SF0.01-fixture row
// counts against SF100 counts fails every SF-scaled query, so the gate
// only "passed" when run with --no-compare. baseline-local-small.json is
// the local fixture's own oracle; this locks its shape so the gate stays
// trustworthy.
func TestLocalSmallBaselineIntegrity(t *testing.T) {
	path := filepath.Join("..", "..", "benchmarks", "tpch", "baseline-local-small.json")
	bf, err := LoadBaseline(path)
	if err != nil {
		t.Fatalf("loading local-small baseline: %v", err)
	}
	if len(bf.Queries) != 22 {
		t.Fatalf("want 22 gated queries, got %d", len(bf.Queries))
	}
	if _, ok := bf.ProjectionFactors["small_slice"]; !ok {
		t.Fatal("missing small_slice projection factors — Project() errors skip Compare entirely, silently disabling the gate")
	}
	for name, qb := range bf.Queries {
		// q18 legitimately returns 0 rows on this fixture (max per-order
		// sum(l_quantity)=295 < 300, DuckDB-validated 2026-08-06); the
		// RowCount==0 guard in Compare skips it, so its correctness gate
		// is its (empty) value-sig only. Everything else must gate rows.
		if name != "q18" && qb.RowCount == 0 {
			t.Errorf("%s: row_count 0 — this query would never be gated", name)
		}
		// q18: no rows to sign; q20: all-string output has no numeric
		// columns to sign. All other queries must carry a value-sig.
		if name != "q18" && name != "q20" && qb.ValueSig == "" {
			t.Errorf("%s: missing value_sig", name)
		}
		if qb.WallMsP50 != 0 || qb.PeakHeapMB != 0 {
			t.Errorf("%s: walls/heap must stay zero — this file is a correctness oracle, not a perf baseline", name)
		}
	}
}
