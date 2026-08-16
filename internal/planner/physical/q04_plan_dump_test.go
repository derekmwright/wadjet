package physical

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
)

func TestQ04PlanDump(t *testing.T) {
	if os.Getenv("WADJET_Q04_PLAN_DUMP") != "1" {
		t.Skip("set WADJET_Q04_PLAN_DUMP=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, tpch.TPCHQueries[4].SQL, 3, 0)
	t.Logf("Q04 produced %d stages", len(stages))
	for i, s := range stages {
		var b strings.Builder
		fmt.Fprintf(&b, "[%02d] %s type=%s tasks=%d", i, s.ID, s.Type, s.Tasks)
		if len(s.Dependencies) > 0 {
			fmt.Fprintf(&b, " deps=%v", s.Dependencies)
		}
		if s.TableName != "" {
			fmt.Fprintf(&b, " table=%s", s.TableName)
		}
		if len(s.FilterExprs) > 0 {
			fmt.Fprintf(&b, " filters=%v", s.FilterExprs)
		}
		if s.JoinType != "" {
			fmt.Fprintf(&b, " join=%s lkeys=%v rkeys=%v", s.JoinType, s.JoinLeftKeys, s.JoinRightKeys)
		}
		if len(s.FusedJoins) > 0 {
			for j, fj := range s.FusedJoins {
				fmt.Fprintf(&b, " fused[%d]=(%s lkeys=%v rkeys=%v build=%s)", j, fj.JoinType, fj.JoinLeftKeys, fj.JoinRightKeys, fj.BuildDepStage)
			}
		}
		if s.EstimatedRows > 0 {
			fmt.Fprintf(&b, " est_rows=%d", s.EstimatedRows)
		}
		t.Log(b.String())
	}
}
