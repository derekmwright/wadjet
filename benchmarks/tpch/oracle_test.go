package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestTPCHOptimizationInvariance is the kill-switch differential oracle
// (#287) over the TPC-H corpus at SF0.01: all 22 queries run with every
// registered optimization enabled, then once per kill switch with just
// that optimization disabled, then with all disabled — results must be
// identical. A failing subtest names the diverging optimization.
func TestTPCHOptimizationInvariance(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	queries := make([]oracle.Query, 0, len(queryNums))
	for _, qNum := range queryNums {
		q := oracle.Query{
			Name: fmt.Sprintf("Q%02d", qNum),
			SQL:  TPCHQueries[qNum].SQL,
		}
		// Q02/Q22: float-threshold row membership shifts with accumulation
		// order between two correct runs (same tolerance as TestTPCHQueries).
		if qNum == 2 || qNum == 22 {
			q.CountOnly = true
			q.Tolerance = 4
		}
		queries = append(queries, q)
	}

	pushdownsBefore := physical.ScanFilterPushdowns.Load()
	oracle.RunDifferential(ctx, t, oracle.ExpandLimits(queries), func(ctx context.Context, sql string) (*oracle.Result, error) {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return &oracle.Result{Columns: res.Columns, Rows: res.Rows}, nil
	})
	// Engagement check: the corpus must actually exercise scan-filter
	// pushdown, or the scan-filter arm proves nothing.
	if physical.ScanFilterPushdowns.Load() == pushdownsBefore {
		t.Error("scan-filter pushdown never engaged across the corpus — oracle arm is dormant")
	}
}
