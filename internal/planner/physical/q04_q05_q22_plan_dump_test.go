package physical

import (
	"fmt"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

// Spot-check whether the SF10 A/B "wins" actually had dynamic-filter
// annotations attached (the win hypothesis) vs no annotations (in which
// case the wall delta was noise like Q21's).
func TestDynamicFilterAnnotationsPerQuery(t *testing.T) {
	if os.Getenv("WADJET_DF_COVERAGE") != "1" {
		t.Skip("set WADJET_DF_COVERAGE=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	checks := []int{4, 5, 8, 17, 18, 20, 21, 22}
	for _, qn := range checks {
		sql := tpch.TPCHQueries[qn].SQL
		stages := sqlToStagesWithDynamicFilters(t, cat, ctx, sql, 3, 0)
		emits, consumes := 0, 0
		var detail []string
		for _, s := range stages {
			for _, e := range s.EmitDynamicFilters {
				emits++
				detail = append(detail, fmt.Sprintf("EMIT %s on %s.%s", e.FilterID, s.ID, e.KeyColumn))
			}
			for _, c := range s.ConsumeDynamicFilters {
				consumes++
				detail = append(detail, fmt.Sprintf("CONS %s on %s.%s (from %s)", c.FilterID, s.ID, c.TargetColumn, c.SourceStageID))
			}
		}
		marker := " "
		if emits == 0 && consumes == 0 {
			marker = "🚫"
		} else if emits > 0 && consumes > 0 {
			marker = "✅"
		}
		t.Logf("Q%02d %s %d emits, %d consumes %v", qn, marker, emits, consumes, detail)
	}
}
