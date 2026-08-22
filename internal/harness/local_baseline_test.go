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

// TestLocalLargeBaselineIntegrity mirrors TestLocalSmallBaselineIntegrity
// for baseline-local-large.json. cmd/tpch-harness/main.go had no
// --mode=local --slice=large branch in its default-baseline selection
// until this file existed, and fell through to
// benchmarks/tpch/baseline-sf100.json — comparing SF0.01 local/large row
// counts against SF100 row counts and failing every SF-scaled query. This
// locks the large file's shape the same way the small file's is locked.
func TestLocalLargeBaselineIntegrity(t *testing.T) {
	path := filepath.Join("..", "..", "benchmarks", "tpch", "baseline-local-large.json")
	bf, err := LoadBaseline(path)
	if err != nil {
		t.Fatalf("loading local-large baseline: %v", err)
	}
	if len(bf.Queries) != 22 {
		t.Fatalf("want 22 gated queries, got %d", len(bf.Queries))
	}
	if _, ok := bf.ProjectionFactors["large_slice"]; !ok {
		t.Fatal("missing large_slice projection factors — Project() errors skip Compare entirely, silently disabling the gate")
	}
	for name, qb := range bf.Queries {
		// q18 legitimately returns 0 rows on this fixture, for the same
		// reason as local/small: the large slice's loadSampleData path
		// generates the identical SF0.01 seed-42M datagen fixture (Slice
		// is not consulted by data generation).
		if name != "q18" && qb.RowCount == 0 {
			t.Errorf("%s: row_count 0 — this query would never be gated", name)
		}
		if name != "q18" && name != "q20" && qb.ValueSig == "" {
			t.Errorf("%s: missing value_sig", name)
		}
		if qb.WallMsP50 != 0 || qb.PeakHeapMB != 0 {
			t.Errorf("%s: walls/heap must stay zero — this file is a correctness oracle, not a perf baseline", name)
		}
	}
}

// TestLocalBaselinesAgreeOnRowsAndSigs guards the fact baseline-local-large.json
// depends on: the large slice loads the identical SF0.01 seed-42M fixture as
// the small slice (SliceConfig is not consulted by loadSampleData — only
// GoMemLimit and --memory-budget/--shared-pool-budget differ between the two
// slices), so every row_count/row_checksum/value_sig entry must match
// byte-for-byte between the two baseline files. If a future change makes the
// slices load different data, this test is the tripwire: it means
// baseline-local-large.json can no longer be a copy of baseline-local-small.json
// and needs its own independently-captured, independently-validated numbers.
func TestLocalBaselinesAgreeOnRowsAndSigs(t *testing.T) {
	smallPath := filepath.Join("..", "..", "benchmarks", "tpch", "baseline-local-small.json")
	largePath := filepath.Join("..", "..", "benchmarks", "tpch", "baseline-local-large.json")
	small, err := LoadBaseline(smallPath)
	if err != nil {
		t.Fatalf("loading local-small baseline: %v", err)
	}
	large, err := LoadBaseline(largePath)
	if err != nil {
		t.Fatalf("loading local-large baseline: %v", err)
	}
	if len(small.Queries) != len(large.Queries) {
		t.Fatalf("query count mismatch: small=%d large=%d", len(small.Queries), len(large.Queries))
	}
	for name, sq := range small.Queries {
		lq, ok := large.Queries[name]
		if !ok {
			t.Errorf("%s: present in baseline-local-small.json, missing from baseline-local-large.json", name)
			continue
		}
		if sq.RowCount != lq.RowCount {
			t.Errorf("%s: row_count mismatch: small=%d large=%d", name, sq.RowCount, lq.RowCount)
		}
		if sq.RowChecksum != lq.RowChecksum {
			t.Errorf("%s: row_checksum mismatch: small=%q large=%q", name, sq.RowChecksum, lq.RowChecksum)
		}
		if sq.ValueSig != lq.ValueSig {
			t.Errorf("%s: value_sig mismatch: small=%q large=%q", name, sq.ValueSig, lq.ValueSig)
		}
	}
}
