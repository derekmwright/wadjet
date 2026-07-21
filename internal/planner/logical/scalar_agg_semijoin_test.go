package logical

import (
	"testing"
)

// buildDecorrelatedShape constructs the post-pushdown shape that
// decorrelateScalarSubqueries + pushdownPredicates produce for Q17:
//
//	Join(left, ScalarDecorrelated) ON p_partkey = l_partkey
//	├── Join(inner) ON p_partkey = l_partkey
//	│   ├── Scan lineitem
//	│   └── Filter(p_brand=...) → Scan part      ← key-source branch
//	└── Aggregate group_by=[l_partkey] avg(l_quantity)
//	    └── Scan lineitem                        ← reduction target
func buildDecorrelatedShape(partFiltered bool) *Node {
	lineitemCols := []string{"l_partkey", "l_quantity", "l_extendedprice"}
	partCols := []string{"p_partkey", "p_brand", "p_container"}

	outerLineitem := NewScan("lineitem", "")
	outerLineitem.ScanColumns = lineitemCols
	outerLineitem.ScanRowEstimate = 6_000_000
	partScan := NewScan("part", "")
	partScan.ScanColumns = partCols
	partScan.ScanRowEstimate = 200_000
	var partBranch *Node = partScan
	if partFiltered {
		partBranch = NewFilter(partScan, []Predicate{{Raw: "p_brand = 'Brand#23'"}})
	}
	outer := NewJoin(outerLineitem, partBranch, "inner", "p_partkey = l_partkey")

	aggLineitem := NewScan("lineitem", "")
	aggLineitem.ScanColumns = lineitemCols
	aggLineitem.ScanRowEstimate = 6_000_000
	agg := NewAggregate(aggLineitem, []string{"l_partkey"},
		[]AggExpr{{Func: "avg", InputCol: "l_quantity", OutputCol: "__scalar_0"}})

	join := &Node{
		Type:               NodeJoin,
		JoinType:           "left",
		JoinCond:           "p_partkey = l_partkey",
		Children:           []*Node{outer, agg},
		ScalarDecorrelated: true,
	}
	return join
}

func TestReduceDecorrelatedScalarAggs_InsertsSemijoin(t *testing.T) {
	plan := buildDecorrelatedShape(true)
	reduceDecorrelatedScalarAggs(plan)

	agg := plan.Children[1]
	semi := agg.Children[0]
	if semi.Type != NodeJoin || semi.JoinType != "semi" {
		t.Fatalf("aggregate input: want semi join, got %v/%s", semi.Type, semi.JoinType)
	}
	if semi.JoinCond != "l_partkey = p_partkey" {
		t.Errorf("semi cond: want %q got %q", "l_partkey = p_partkey", semi.JoinCond)
	}
	if semi.Children[0].Type != NodeScan || semi.Children[0].TableName != "lineitem" {
		t.Errorf("semi probe: want lineitem scan, got %v %s", semi.Children[0].Type, semi.Children[0].TableName)
	}
	build := semi.Children[1]
	if build.Type != NodeDistinct {
		t.Fatalf("semi build: want Distinct root, got %v", build.Type)
	}
	proj := build.Children[0]
	if proj.Type != NodeProject || len(proj.Projections) != 1 || proj.Projections[0].Column != "p_partkey" {
		t.Fatalf("semi build: want Project[p_partkey], got %+v", proj)
	}
	if proj.Children[0].Type != NodeFilter {
		t.Fatalf("semi build: want cloned Filter→Scan branch, got %v", proj.Children[0].Type)
	}
	// The clone must be a COPY — mutating it must not corrupt the outer
	// branch (later passes mutate predicate/column slices in place).
	clonedScan := proj.Children[0].Children[0]
	outerScan := plan.Children[0].Children[1].Children[0]
	if clonedScan == outerScan {
		t.Error("key-source branch was shared, not cloned")
	}
}

// TestReduceDecorrelatedScalarAggs_SemiBelowFilter is the regression test
// for the Q02 row-loss bug: when the aggregate input carries a Filter (the
// subquery's own residual predicates, e.g. r_name='EUROPE' above its join
// tree), the semijoin must insert BELOW that Filter. Above it, walkStages'
// broadcast-chain fusion detaches the Filter from its stage and its
// predicates are dropped — Q02's inner min() was computed over all regions
// and the harness lost 3 of 5 result rows.
func TestReduceDecorrelatedScalarAggs_SemiBelowFilter(t *testing.T) {
	plan := buildDecorrelatedShape(true)
	agg := plan.Children[1]
	residual := NewFilter(agg.Children[0], []Predicate{{Raw: "r_name = 'EUROPE'"}})
	agg.Children[0] = residual

	reduceDecorrelatedScalarAggs(plan)

	if agg.Children[0] != residual {
		t.Fatalf("filter must stay the aggregate's direct child; got %v", agg.Children[0].Type)
	}
	semi := residual.Children[0]
	if semi.Type != NodeJoin || semi.JoinType != "semi" {
		t.Fatalf("want semi join below the residual filter, got %v/%s", semi.Type, semi.JoinType)
	}
}

func TestReduceDecorrelatedScalarAggs_Gates(t *testing.T) {
	t.Run("unfiltered_branch_no_reduction", func(t *testing.T) {
		plan := buildDecorrelatedShape(false)
		reduceDecorrelatedScalarAggs(plan)
		if got := plan.Children[1].Children[0].Type; got != NodeScan {
			t.Errorf("unfiltered key source must not reduce; agg input became %v", got)
		}
	})
	t.Run("same_name_keys_no_reduction", func(t *testing.T) {
		plan := buildDecorrelatedShape(true)
		plan.JoinCond = "l_partkey = l_partkey"
		reduceDecorrelatedScalarAggs(plan)
		if got := plan.Children[1].Children[0].Type; got != NodeScan {
			t.Errorf("ambiguous same-name keys must not reduce; agg input became %v", got)
		}
	})
	t.Run("kill_switch", func(t *testing.T) {
		old := ScalarAggSemijoin.Load()
		ScalarAggSemijoin.Store(false)
		defer ScalarAggSemijoin.Store(old)
		plan := buildDecorrelatedShape(true)
		reduceDecorrelatedScalarAggs(plan)
		if got := plan.Children[1].Children[0].Type; got != NodeScan {
			t.Errorf("kill switch off must not reduce; agg input became %v", got)
		}
	})
	t.Run("huge_key_source_no_reduction", func(t *testing.T) {
		plan := buildDecorrelatedShape(true)
		// Inflate the key-source scan far beyond the broadcastable cap;
		// even filtered, the estimate stays above it.
		plan.Children[0].Children[1].Children[0].ScanRowEstimate = 2_000_000_000
		reduceDecorrelatedScalarAggs(plan)
		if got := plan.Children[1].Children[0].Type; got != NodeScan {
			t.Errorf("un-broadcastable key source must not reduce; agg input became %v", got)
		}
	})
	t.Run("no_stats_no_reduction", func(t *testing.T) {
		plan := buildDecorrelatedShape(true)
		plan.Children[0].Children[1].Children[0].ScanRowEstimate = 0
		reduceDecorrelatedScalarAggs(plan)
		if got := plan.Children[1].Children[0].Type; got != NodeScan {
			t.Errorf("stat-less key source must not reduce; agg input became %v", got)
		}
	})
	t.Run("unmarked_join_no_reduction", func(t *testing.T) {
		plan := buildDecorrelatedShape(true)
		plan.ScalarDecorrelated = false
		reduceDecorrelatedScalarAggs(plan)
		if got := plan.Children[1].Children[0].Type; got != NodeScan {
			t.Errorf("unmarked join must not reduce; agg input became %v", got)
		}
	})
}
