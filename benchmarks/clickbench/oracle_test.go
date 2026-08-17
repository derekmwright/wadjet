package clickbench

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestHitsOptimizationInvariance is the kill-switch differential oracle
// (#287) over the ClickBench corpus: all 43 queries run against the local
// hits part (WADJET_HITS_PART) with every registered optimization enabled,
// then once per kill switch with just that optimization disabled, then
// with all disabled — results must be identical. A failing subtest names
// the diverging optimization.
//
// This is the corpus that would have localized both recent near-misses
// instantly: the stats-compare date coercion bug (stats-prune) and the
// nested-fallback pushed-predicate drop (scan-filter).
func TestHitsOptimizationInvariance(t *testing.T) {
	ctx := context.Background()
	db, _ := openHitsDB(t, ctx)

	raw := loadHitsQueries(t)
	queries := make([]oracle.Query, len(raw))
	for i, sql := range raw {
		queries[i] = oracle.Query{Name: fmt.Sprintf("Q%02d", i+1), SQL: sql}
	}

	metaCountBefore := physical.MetadataCountsPlanned.Load()
	metaMinMaxBefore := physical.MetadataMinMaxPlanned.Load()
	oracle.RunDifferential(ctx, t, oracle.ExpandLimits(queries), func(ctx context.Context, sql string) (*oracle.Result, error) {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		return &oracle.Result{Columns: res.Columns, Rows: res.Rows}, nil
	})
	// Engagement check: Q01 is a bare COUNT(*), so the metadata-count
	// rewrite must fire somewhere in the run or its oracle arm is dormant.
	if physical.MetadataCountsPlanned.Load() == metaCountBefore {
		t.Error("metadata COUNT(*) rewrite never engaged across the corpus — oracle arm is dormant")
	}
	// Q07 is `SELECT MIN(EventDate), MAX(EventDate) FROM hits`, so the
	// statistics-answered MIN/MAX rewrite must fire too. This assertion is
	// load-bearing: the rewrite declines silently (falling back to the scan)
	// whenever a file's schema does not let it trust the footer, and a
	// dormant arm proves nothing about a live optimization.
	if physical.MetadataMinMaxPlanned.Load() == metaMinMaxBefore {
		t.Error("metadata MIN/MAX rewrite never engaged across the corpus — oracle arm is dormant")
	}
}
