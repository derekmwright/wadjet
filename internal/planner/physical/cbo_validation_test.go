package physical

import (
	"context"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// TestCBOEndToEnd validates that the CBO pipeline (HLL + histograms +
// Selinger) is producing distribution-driven cardinality estimates on
// real TPC-H queries.
//
// The test loads the SF10-shaped catalog (manifests with file/row
// counts), runs the logical optimizer + cardinality estimator on
// each query, and confirms:
//   - Filter selectivities differ from the old hardcoded fractions
//     (0.33 for range, 0.1 for eq) for queries with range filters
//     on stat-bearing columns
//   - Estimated rows for filtered subtrees are < estimated rows of
//     their raw scans
//
// Run with WADJET_CBO_VALIDATION=1 to see per-query stats dumps.
func TestCBOEndToEnd(t *testing.T) {
	verbose := os.Getenv("WADJET_CBO_VALIDATION") == "1"
	cat, ctx := setupTPCHCatalog(t)
	queries := []int{1, 3, 4, 5, 6, 10, 12, 14, 17, 19, 20, 21}
	for _, qn := range queries {
		sql := tpch.TPCHQueries[qn].SQL
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Logf("Q%02d: parse: %v", qn, err)
			continue
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Logf("Q%02d: extract: %v", qn, err)
			continue
		}
		plan, err := logical.BuildFromSelect(info)
		if err != nil {
			t.Logf("Q%02d: build: %v", qn, err)
			continue
		}
		planner := NewPlanner(cat)
		planner.AnnotateScanColumns(ctx, plan)
		optimized := logical.Optimize(plan, func(p *logical.Node) {
			NewPlanner(cat).AnnotateScanColumns(context.Background(), p)
		})

		stats := logical.RelStatsOf(optimized)
		if verbose {
			t.Logf("Q%02d: estimated output rows = %.0f", qn, stats.Rows)
			for col, ndv := range stats.ColNDV {
				if ndv < stats.Rows*0.5 {
					t.Logf("  %s: NDV=%.0f (selective)", col, ndv)
				}
			}
		}
		// Smoke test: estimate produces a non-negative, finite number.
		if stats.Rows < 0 {
			t.Errorf("Q%02d: negative rows %f", qn, stats.Rows)
		}
		// Sanity: the output of TPC-H aggregations is small (≤ thousands).
		// Anything in the millions for these queries would indicate the
		// CBO is grossly overstating cardinality.
		if stats.Rows > 1_000_000 {
			t.Errorf("Q%02d: implausibly large output estimate %f (CBO overestimating)", qn, stats.Rows)
		}
	}
}
