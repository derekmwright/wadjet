package physical

import (
	"reflect"
	"testing"
)

// Q18-shaped fixture: one raw exchange on (l_orderkey) carrying
// (l_orderkey, l_quantity), consumed by BOTH a grouped final_aggregate
// (the HAVING subquery, via the agg-over-exchange rewire) and a hash
// join whose sole dependent re-aggregates the same column. This is the
// exact SF100 shape that ships 600M raw rows; eligibility must mark it.
func q18Stages() []Stage {
	return []Stage{
		{ID: "scan-orders", Type: StageScan},
		{
			ID:   "rp",
			Type: StageExchangeRepartition,
			Exchange: &ExchangeStage{
				Keys:  []string{"l_orderkey"},
				Count: 24,
			},
			Columns:      []string{"l_orderkey", "l_quantity"},
			Dependencies: []string{"scan-li"},
		},
		{
			ID:           "fa-inner",
			Type:         "final_aggregate",
			GroupByCols:  []string{"l_orderkey"},
			AggSpecs:     []AggSpec{{Func: "SUM", InputCol: "l_quantity", OutputCol: "__having_0"}},
			FilterExprs:  []string{"__having_0 > 300"},
			Dependencies: []string{"rp"},
		},
		{
			ID:            "join",
			Type:          StageHashJoin,
			LeftDepStage:  "rp",
			RightDepStage: "scan-orders",
			JoinLeftKeys:  []string{"l_orderkey"},
			JoinRightKeys: []string{"o_orderkey"},
			Dependencies:  []string{"rp", "scan-orders", "fa-inner"},
			ChainedJoins: []ChainedJoinSpec{{
				JoinType:      "inner",
				JoinLeftKeys:  []string{"o_custkey"},
				JoinRightKeys: []string{"c_custkey"},
				BuildDepStage: "scan-cust",
			}},
		},
		{
			ID:           "fa-outer",
			Type:         "final_aggregate",
			GroupByCols:  []string{"c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"},
			AggSpecs:     []AggSpec{{Func: "sum", InputCol: "l_quantity", OutputCol: "sum_qty"}},
			Dependencies: []string{"join"},
		},
	}
}

func exchangeByID(t *testing.T, stages []Stage, id string) *ExchangeStage {
	t.Helper()
	for i := range stages {
		if stages[i].ID == id {
			return stages[i].Exchange
		}
	}
	t.Fatalf("stage %s not found", id)
	return nil
}

func TestMarkExchangePartialAgg_Q18ShapeMarks(t *testing.T) {
	stages := q18Stages()
	markExchangePartialAgg(stages)
	ex := exchangeByID(t, stages, "rp")
	if !reflect.DeepEqual(ex.PartialAggGroupBy, []string{"l_orderkey"}) {
		t.Fatalf("PartialAggGroupBy = %v, want [l_orderkey]", ex.PartialAggGroupBy)
	}
	want := []AggSpec{{Func: "sum", InputCol: "l_quantity", OutputCol: "l_quantity"}}
	if !reflect.DeepEqual(ex.PartialAggSpecs, want) {
		t.Fatalf("PartialAggSpecs = %v, want %v (name-preserving)", ex.PartialAggSpecs, want)
	}
}

func TestMarkExchangePartialAgg_KillSwitch(t *testing.T) {
	exchangePartialAggEnabled = false
	defer func() { exchangePartialAggEnabled = true }()
	stages := q18Stages()
	markExchangePartialAgg(stages)
	if ex := exchangeByID(t, stages, "rp"); ex.PartialAggSpecs != nil {
		t.Fatal("kill switch must leave exchanges unmarked")
	}
}

func TestMarkExchangePartialAgg_IneligibleShapes(t *testing.T) {
	mutate := map[string]func([]Stage){
		"count spec on payload": func(s []Stage) {
			s[2].AggSpecs = []AggSpec{{Func: "count", InputCol: "l_quantity", OutputCol: "c"}}
		},
		"avg spec on payload": func(s []Stage) {
			s[2].AggSpecs = []AggSpec{{Func: "avg", InputCol: "l_quantity", OutputCol: "a"}}
		},
		"derived input expression": func(s []Stage) {
			s[2].AggSpecs[0].InputExpr = "l_quantity * 2"
		},
		"conflicting funcs across consumers": func(s []Stage) {
			s[4].AggSpecs = []AggSpec{{Func: "max", InputCol: "l_quantity", OutputCol: "m"}}
		},
		"join dependent counts rows": func(s []Stage) {
			s[4].AggSpecs = append(s[4].AggSpecs, AggSpec{Func: "count", InputCol: "", OutputCol: "n"})
		},
		"join dependent aggregates uncovered column": func(s []Stage) {
			s[4].AggSpecs = append(s[4].AggSpecs, AggSpec{Func: "sum", InputCol: "o_totalprice", OutputCol: "t"})
		},
		"join keys on aggregated column": func(s []Stage) {
			s[3].JoinLeftKeys = []string{"l_quantity"}
		},
		"join filter references aggregated column": func(s []Stage) {
			s[3].JoinFilter = "l_quantity > 10"
		},
		"chained join keys on aggregated column": func(s []Stage) {
			s[3].ChainedJoins[0].JoinLeftKeys = []string{"l_quantity"}
		},
		"partition key aggregated": func(s []Stage) {
			s[1].Exchange.Keys = []string{"l_quantity"}
		},
		"computed cols on exchange": func(s []Stage) {
			s[1].Exchange.ComputedCols = []ComputedCol{{Name: "f", Expr: "1"}}
		},
		"non-join non-agg consumer": func(s []Stage) {
			s[4].Type = StageWindow
			s[4].Dependencies = []string{"rp"}
		},
		"join output goes to gather (no dependents)": func(s []Stage) {
			s[4].Dependencies = nil
		},
		"grouped agg groups by aggregated column": func(s []Stage) {
			s[2].GroupByCols = []string{"l_orderkey", "l_quantity"}
		},
	}
	for name, fn := range mutate {
		stages := q18Stages()
		fn(stages)
		markExchangePartialAgg(stages)
		if ex := exchangeByID(t, stages, "rp"); ex.PartialAggSpecs != nil {
			t.Errorf("%s: exchange must be unmarked, got specs %v", name, ex.PartialAggSpecs)
		}
	}
}

// Post-semi-pushdown Q18 shape: the lineitem exchange's join consumer
// feeds ANOTHER join (customer) before the terminal grouped aggregate.
// The dependent walk must recurse through the intermediate join.
func TestMarkExchangePartialAgg_JoinChainDependentMarks(t *testing.T) {
	stages := q18Stages()
	// Splice a customer join between the lineitem join and the outer
	// grouped final: join → join2 → fa-outer.
	stages = append(stages, Stage{
		ID:            "join2",
		Type:          StageHashJoin,
		LeftDepStage:  "join",
		RightDepStage: "scan-cust",
		JoinLeftKeys:  []string{"o_custkey"},
		JoinRightKeys: []string{"c_custkey"},
		Dependencies:  []string{"join", "scan-cust"},
	})
	for i := range stages {
		if stages[i].ID == "fa-outer" {
			stages[i].Dependencies = []string{"join2"}
		}
	}
	markExchangePartialAgg(stages)
	ex := exchangeByID(t, stages, "rp")
	want := []AggSpec{{Func: "sum", InputCol: "l_quantity", OutputCol: "l_quantity"}}
	if !reflect.DeepEqual(ex.PartialAggSpecs, want) {
		t.Fatalf("PartialAggSpecs = %v, want %v (recurse through join2)", ex.PartialAggSpecs, want)
	}
}

// The recursion must still reject a chain whose intermediate join keys
// on the aggregated column or whose terminal counts rows.
func TestMarkExchangePartialAgg_JoinChainDependentRejects(t *testing.T) {
	mutate := map[string]func(s []Stage){
		"intermediate join keys on agg col": func(s []Stage) {
			s[len(s)-1].JoinLeftKeys = []string{"l_quantity"}
		},
		"terminal counts rows": func(s []Stage) {
			for i := range s {
				if s[i].ID == "fa-outer" {
					s[i].AggSpecs = append(s[i].AggSpecs, AggSpec{Func: "count", InputCol: "", OutputCol: "n"})
				}
			}
		},
	}
	for name, fn := range mutate {
		stages := q18Stages()
		stages = append(stages, Stage{
			ID:            "join2",
			Type:          StageHashJoin,
			LeftDepStage:  "join",
			RightDepStage: "scan-cust",
			JoinLeftKeys:  []string{"o_custkey"},
			JoinRightKeys: []string{"c_custkey"},
			Dependencies:  []string{"join", "scan-cust"},
		})
		for i := range stages {
			if stages[i].ID == "fa-outer" {
				stages[i].Dependencies = []string{"join2"}
			}
		}
		fn(stages)
		markExchangePartialAgg(stages)
		if ex := exchangeByID(t, stages, "rp"); ex.PartialAggSpecs != nil {
			t.Errorf("%s: exchange must be unmarked", name)
		}
	}
}

func TestMarkExchangePartialAgg_SoleAggConsumerMarks(t *testing.T) {
	// Single grouped-final consumer, no join — the simplest eligible shape.
	stages := []Stage{
		{
			ID:   "rp",
			Type: StageExchangeRepartition,
			Exchange: &ExchangeStage{
				Keys: []string{"g"},
			},
			Columns:      []string{"g", "v"},
			Dependencies: []string{"src"},
		},
		{
			ID:           "fa",
			Type:         "final_aggregate",
			GroupByCols:  []string{"g"},
			AggSpecs:     []AggSpec{{Func: "min", InputCol: "v", OutputCol: "mv"}},
			Dependencies: []string{"rp"},
		},
	}
	markExchangePartialAgg(stages)
	ex := exchangeByID(t, stages, "rp")
	want := []AggSpec{{Func: "min", InputCol: "v", OutputCol: "v"}}
	if !reflect.DeepEqual(ex.PartialAggSpecs, want) {
		t.Fatalf("PartialAggSpecs = %v, want %v", ex.PartialAggSpecs, want)
	}
}
