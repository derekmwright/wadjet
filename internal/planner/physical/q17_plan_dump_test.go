package physical

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestQ17PlanDump prints the full physical plan for Q17 against a SF10-shaped
// catalog (mirrors plan_tpch_test.go file-counts). Run with
//
//	WADJET_Q17_PLAN_DUMP=1 go test -run TestQ17PlanDump -v ./internal/planner/physical/
//
// to inspect the plan during the SF100 hang investigation. Skipped by
// default to keep `go test ./internal/planner/physical/` quiet.
func TestQ17PlanDump(t *testing.T) {
	if os.Getenv("WADJET_Q17_PLAN_DUMP") != "1" {
		t.Skip("set WADJET_Q17_PLAN_DUMP=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT
		SUM(l_extendedprice) / 7.0 as avg_yearly
	FROM lineitem
	JOIN part ON p_partkey = l_partkey
	WHERE p_brand = 'Brand#23'
		AND p_container = 'MED BOX'
		AND l_quantity < (
			SELECT 0.2 * AVG(l_quantity)
			FROM lineitem
			WHERE l_partkey = p_partkey
		)`

	stages := sqlToStages(t, cat, ctx, sql, 3)

	t.Logf("Q17 produced %d stages (worker_count=3, SF10-shaped catalog)", len(stages))
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
		if len(s.Columns) > 0 {
			fmt.Fprintf(&b, " cols=%v", s.Columns)
		}
		if len(s.FilterExprs) > 0 {
			fmt.Fprintf(&b, " filters=%v", s.FilterExprs)
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
		if len(s.FusedAggGroupBy) > 0 {
			fmt.Fprintf(&b, " fused_group_by=%v", s.FusedAggGroupBy)
		}
		if len(s.FusedAggSpecs) > 0 {
			specs := make([]string, len(s.FusedAggSpecs))
			for j, a := range s.FusedAggSpecs {
				in := a.InputCol
				if a.InputExpr != "" {
					in = a.InputExpr
				}
				specs[j] = fmt.Sprintf("%s(%s)->%s", a.Func, in, a.OutputCol)
			}
			fmt.Fprintf(&b, " fused_aggs=[%s]", strings.Join(specs, ","))
		}
		if s.JoinType != "" {
			fmt.Fprintf(&b, " join=%s left_keys=%v right_keys=%v left_dep=%s right_dep=%s",
				s.JoinType, s.JoinLeftKeys, s.JoinRightKeys, s.LeftDepStage, s.RightDepStage)
			if s.JoinPartitionCount > 0 {
				fmt.Fprintf(&b, " join_partitions=%d", s.JoinPartitionCount)
			}
		}
		if len(s.FusedJoins) > 0 {
			specs := make([]string, len(s.FusedJoins))
			for j, fj := range s.FusedJoins {
				specs[j] = fmt.Sprintf("%s %v=%v build_dep=%s build_alias=%s",
					fj.JoinType, fj.JoinLeftKeys, fj.JoinRightKeys, fj.BuildDepStage, fj.BuildTableAlias)
			}
			fmt.Fprintf(&b, " fused_joins=[%s]", strings.Join(specs, ";"))
		}
		if s.ProbeSplitAlias != "" {
			fmt.Fprintf(&b, " probe_split=%s files=%d", s.ProbeSplitAlias, len(s.ProbeSplitFiles))
		}
		if s.Exchange != nil {
			fmt.Fprintf(&b, " exchange={count=%d keys=%v}", s.Exchange.Count, s.Exchange.Keys)
		}
		if s.MergeGroupCount > 0 {
			fmt.Fprintf(&b, " merge_group=%d/%d", s.MergeGroup, s.MergeGroupCount)
		}
		if s.EstimatedRows > 0 {
			fmt.Fprintf(&b, " est_rows=%d", s.EstimatedRows)
		}
		if s.EstimatedBytes > 0 {
			fmt.Fprintf(&b, " est_bytes=%d", s.EstimatedBytes)
		}
		t.Log(b.String())
	}
}
