package physical

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestQ18PlanDump prints the full physical plan for Q18 against a SF10-shaped
// catalog (mirrors plan_tpch_test.go file-counts). Run with
//
//	WADJET_Q18_PLAN_DUMP=1 go test -run TestQ18PlanDump -v ./internal/planner/physical/
//
// Used to identify which operator pins the heap on Q18 SF100 mc=3 (failure
// shape: worker reap from heap pin → 30m timeout). HashAggregate's spill
// paths are now partial-drain (PRs #93/#94); remaining suspects are
// HashJoin build at the 150M-orderkey shape, a non-int-keyed aggregate
// path, or join output materialization. Skipped by default.
func TestQ18PlanDump(t *testing.T) {
	if os.Getenv("WADJET_Q18_PLAN_DUMP") != "1" {
		t.Skip("set WADJET_Q18_PLAN_DUMP=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT
		c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
		SUM(l_quantity) as total_qty
	FROM customer
	JOIN orders ON c_custkey = o_custkey
	JOIN lineitem ON o_orderkey = l_orderkey
	WHERE o_orderkey IN (
		SELECT l_orderkey
		FROM lineitem
		GROUP BY l_orderkey
		HAVING SUM(l_quantity) > 300
	)
	GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
	ORDER BY o_totalprice DESC, o_orderdate
	LIMIT 100`

	stages := sqlToStages(t, cat, ctx, sql, 3)

	t.Logf("Q18 produced %d stages (worker_count=3, SF10-shaped catalog)", len(stages))
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
