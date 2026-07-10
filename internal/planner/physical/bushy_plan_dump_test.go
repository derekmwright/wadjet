package physical

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
)

func TestBushyPlanDump(t *testing.T) {
	if os.Getenv("WADJET_BUSHY_PLAN_DUMP") != "1" {
		t.Skip("set WADJET_BUSHY_PLAN_DUMP=1 to enable")
	}
	logical.BushyJoinReorder.Store(true)
	defer logical.BushyJoinReorder.Store(false)
	cat, ctx := setupTPCHCatalog(t)
	for _, qn := range []int{2, 7, 8} {
		sql := tpch.TPCHQueries[qn].SQL
		stages := sqlToStages(t, cat, ctx, sql, 3)
		t.Logf("=== Q%02d: %d stages ===", qn, len(stages))
		for i, s := range stages {
			var b strings.Builder
			fmt.Fprintf(&b, "[%02d] %s type=%s", i, s.ID, s.Type)
			if s.TableName != "" {
				fmt.Fprintf(&b, " table=%s", s.TableName)
			}
			if s.ScanAlias != "" {
				fmt.Fprintf(&b, " alias=%s", s.ScanAlias)
			}
			if s.JoinType != "" {
				fmt.Fprintf(&b, " join=%s lk=%v rk=%v lt=%s rt=%s balias=%s qual=%v origins=%v",
					s.JoinType, s.JoinLeftKeys, s.JoinRightKeys, s.LeftDepStage, s.RightDepStage,
					s.BuildTableAlias, s.QualifyAllBuildCols, s.BuildColOrigins)
			}
			if len(s.FusedJoins) > 0 {
				for _, fj := range s.FusedJoins {
					fmt.Fprintf(&b, " fused={%s lk=%v rk=%v balias=%s origins=%v}", fj.JoinType, fj.JoinLeftKeys, fj.JoinRightKeys, fj.BuildTableAlias, fj.BuildColOrigins)
				}
			}
			if s.Exchange != nil {
				fmt.Fprintf(&b, " exch_keys=%v", s.Exchange.Keys)
			}
			if len(s.Columns) > 0 && (s.JoinType != "" || s.Exchange != nil) {
				fmt.Fprintf(&b, " cols=%v", s.Columns)
			}
			t.Log(b.String())
		}
	}
}
