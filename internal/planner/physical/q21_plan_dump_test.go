package physical

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

// TestQ21PlanDump emits the Q21 stage DAG plus all Emit/Consume
// annotations under dynamic-filters=on. Run with
//
//	WADJET_Q21_PLAN_DUMP=1 go test -run TestQ21PlanDump -v ./internal/planner/physical/
//
// Used to investigate the +7.7% SF10 regression observed in the
// 2026-05-25 A/B (118.78s off → 127.92s on).
func TestQ21PlanDump(t *testing.T) {
	if os.Getenv("WADJET_Q21_PLAN_DUMP") != "1" {
		t.Skip("set WADJET_Q21_PLAN_DUMP=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	sql := tpch.TPCHQueries[21].SQL
	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, sql, 3, 0)
	t.Logf("Q21 produced %d stages (worker_count=3, SF10-shaped catalog, dynamic-filters=on)", len(stages))

	emitCount, consumeCount := 0, 0
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
			fmt.Fprintf(&b, " join=%s lkeys=%v rkeys=%v lt=%s rt=%s",
				s.JoinType, s.JoinLeftKeys, s.JoinRightKeys, s.LeftDepStage, s.RightDepStage)
		}
		if len(s.FusedJoins) > 0 {
			fmt.Fprintf(&b, " fused_joins=%d", len(s.FusedJoins))
		}
		if s.Exchange != nil {
			fmt.Fprintf(&b, " exchange={count=%d keys=%v}", s.Exchange.Count, s.Exchange.Keys)
		}
		if s.EstimatedRows > 0 {
			fmt.Fprintf(&b, " est_rows=%d", s.EstimatedRows)
		}
		if len(s.EmitDynamicFilters) > 0 {
			emitCount += len(s.EmitDynamicFilters)
			cols := make([]string, len(s.EmitDynamicFilters))
			for j, e := range s.EmitDynamicFilters {
				cols[j] = fmt.Sprintf("%s/%s(bits=%d,id=%s)", e.KeyColumn, e.KeyType, e.BloomBits, e.FilterID)
			}
			fmt.Fprintf(&b, " 🔵EMIT=[%s]", strings.Join(cols, ","))
		}
		if len(s.ConsumeDynamicFilters) > 0 {
			consumeCount += len(s.ConsumeDynamicFilters)
			cols := make([]string, len(s.ConsumeDynamicFilters))
			for j, c := range s.ConsumeDynamicFilters {
				cols[j] = fmt.Sprintf("%s←%s/%s", c.TargetColumn, c.SourceStageID, c.FilterID)
			}
			fmt.Fprintf(&b, " 🟢CONS=[%s]", strings.Join(cols, ","))
		}
		t.Log(b.String())
	}
	t.Logf("Q21 totals: %d emits, %d consumes", emitCount, consumeCount)
}
