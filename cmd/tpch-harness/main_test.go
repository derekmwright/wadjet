package main

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/harness"
)

// TestDefaultBaselinePath covers every (mode, slice, scaleFactor)
// combination the --baseline flag's default depends on. Regression test for
// the bug where --mode=local --slice=large at the default scale factor fell
// through to benchmarks/tpch/baseline-sf100.json (an SF100 row-count
// oracle) instead of getting its own local fixture baseline — comparing
// SF0.01 local results against SF100 row counts failed every SF-scaled
// query (q01, q02, q09, q11, q16, q18, q20, q21 diverge; q03-q08, q10,
// q12-q15, q17, q19, q22 happen to share small enough counts to not show
// it, which is why the failure looked partial rather than total).
func TestDefaultBaselinePath(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		slice       string
		scaleFactor float64
		want        string
	}{
		{"local/small default scale", "local", "small", 0, "benchmarks/tpch/baseline-local-small.json"},
		{"local/large default scale", "local", "large", 0, "benchmarks/tpch/baseline-local-large.json"},
		// Non-default scale factors aren't covered by either local fixture
		// (both are DuckDB-validated only at SF0.01) — fall through to the
		// SF100 oracle, same as before this fix.
		{"local/small scaled", "local", "small", 0.1, "benchmarks/tpch/baseline-sf100.json"},
		{"local/large scaled", "local", "large", 1, "benchmarks/tpch/baseline-sf100.json"},
		{"local/small SF10", "local", "small", 10, "benchmarks/tpch/baseline-sf100.json"},
		// Golden mode always uses the SF100 oracle regardless of slice
		// (Slice is local-mode-only; golden runs against a live cluster
		// sized independently of any --slice flag).
		{"golden default scale", "golden", "", 0, "benchmarks/tpch/baseline-sf100.json"},
		{"golden scaled", "golden", "", 1, "benchmarks/tpch/baseline-sf100.json"},
		// An unrecognized slice value (e.g. a future addition, or a typo)
		// must not silently fall into the small/large branches — it should
		// fall through to the SF100 default like any other unmatched case.
		{"local unknown slice", "local", "medium", 0, "benchmarks/tpch/baseline-sf100.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := defaultBaselinePath(tt.mode, tt.slice, tt.scaleFactor)
			if got != tt.want {
				t.Errorf("defaultBaselinePath(%q, %q, %v) = %q, want %q",
					tt.mode, tt.slice, tt.scaleFactor, got, tt.want)
			}
		})
	}
}

// TestFormatRegressionShowsRealDrift is a regression test for the cosmetic
// bug where every regression line printed "drift=0.0% (tol=0%)" regardless
// of the metric or how far off the value was — baseline.Compare's row_count
// branch left QueryDelta.DriftPct at its zero value, and metrics with no
// percentage meaning at all (row_checksum, spill_paths_exercised) were
// forced through the same "drift=X%" format. A row_count mismatch now
// carries a real percentage; the non-percentage metrics get a PASS/REGRESS
// line instead of a fabricated 0.0%.
func TestFormatRegressionShowsRealDrift(t *testing.T) {
	rowCount := harness.QueryDelta{
		Query: "q18", Metric: "row_count", Status: "REGRESS",
		Baseline: 6, Projected: 0, DriftPct: -100,
	}
	if got := formatRegression(rowCount); !strings.Contains(got, "drift=-100.0%") {
		t.Errorf("row_count: want a real drift percentage, got %q", got)
	}

	for _, metric := range []string{"row_checksum", "value_signature", "value_sig", "spill_paths_exercised"} {
		d := harness.QueryDelta{Query: "<run>", Metric: metric, Status: "REGRESS"}
		got := formatRegression(d)
		if strings.Contains(got, "drift=") {
			t.Errorf("%s: non-percentage metric should not print a fabricated drift, got %q", metric, got)
		}
		if !strings.Contains(got, "REGRESS") {
			t.Errorf("%s: want the status in the line, got %q", metric, got)
		}
	}

	withDetail := harness.QueryDelta{
		Query: "<run>", Metric: "spill_paths_exercised", Status: "REGRESS",
		Detail: "no task's tracked memory saturated its budget",
	}
	if got := formatRegression(withDetail); !strings.Contains(got, withDetail.Detail) {
		t.Errorf("want Detail included in the line, got %q", got)
	}
}
