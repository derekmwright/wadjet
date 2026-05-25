package physical

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

// TestQ07PlanDump prints the full physical plan for Q07. Run with
//
//	WADJET_Q07_PLAN_DUMP=1 go test -run TestQ07PlanDump -v ./internal/planner/physical/
//
// to inspect the plan during the Q07 SF10 stall investigation.
func TestQ07PlanDump(t *testing.T) {
	if os.Getenv("WADJET_Q07_PLAN_DUMP") != "1" {
		t.Skip("set WADJET_Q07_PLAN_DUMP=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	sql := tpch.TPCHQueries[7].SQL
	stages := sqlToStages(t, cat, ctx, sql, 3)
	t.Logf("Q07 produced %d stages (worker_count=3, SF10-shaped catalog)", len(stages))
	for i, s := range stages {
		var b strings.Builder
		fmt.Fprintf(&b, "[%02d] %s type=%s tasks=%d", i, s.ID, s.Type, s.Tasks)
		if len(s.Dependencies) > 0 {
			fmt.Fprintf(&b, " deps=%v", s.Dependencies)
		}
		if s.TableName != "" {
			fmt.Fprintf(&b, " table=%s", s.TableName)
		}
		if s.ScanAlias != "" {
			fmt.Fprintf(&b, " alias=%s", s.ScanAlias)
		}
		if len(s.FilterExprs) > 0 {
			fmt.Fprintf(&b, " filters=%v", s.FilterExprs)
		}
		if s.JoinType != "" {
			fmt.Fprintf(&b, " join=%s left_keys=%v right_keys=%v lt=%s rt=%s",
				s.JoinType, s.JoinLeftKeys, s.JoinRightKeys, s.LeftDepStage, s.RightDepStage)
		}
		if len(s.FusedJoins) > 0 {
			fmt.Fprintf(&b, " fused_joins=%d", len(s.FusedJoins))
		}
		if len(s.GroupByCols) > 0 {
			fmt.Fprintf(&b, " group_by=%v", s.GroupByCols)
		}
		if len(s.AggSpecs) > 0 {
			specs := make([]string, len(s.AggSpecs))
			for j, a := range s.AggSpecs {
				in := a.InputCol
				if a.InputExpr != "" {
					in = a.InputExpr
				}
				specs[j] = fmt.Sprintf("%s(%s)->%s", a.Func, in, a.OutputCol)
			}
			fmt.Fprintf(&b, " aggs=[%s]", strings.Join(specs, ","))
		}
		if s.Exchange != nil {
			fmt.Fprintf(&b, " exchange={count=%d keys=%v}", s.Exchange.Count, s.Exchange.Keys)
		}
		if s.EstimatedRows > 0 {
			fmt.Fprintf(&b, " est_rows=%d", s.EstimatedRows)
		}
		t.Log(b.String())
	}
}
