package logical

import "testing"

func aggNode(groupBy []string, aggs []AggExpr) *Node {
	scan := NewScan("t", "")
	return NewAggregate(scan, groupBy, aggs)
}

func TestTwoLevelDistinctRewrite(t *testing.T) {
	n := aggNode([]string{"k"}, []AggExpr{
		{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
		{Func: "sum", InputCol: "v", OutputCol: "s"},
		{Func: "count", OutputCol: "c"},
		{Func: "min", InputCol: "v", OutputCol: "m"},
	})
	out := rewriteCountDistinctTwoLevel(n)
	if out.Type != NodeAggregate || len(out.Children) != 1 || out.Children[0].Type != NodeAggregate {
		t.Fatalf("expected two stacked aggregates, got %v over %v", out.Type, out.Children[0].Type)
	}
	inner := out.Children[0]
	if got := inner.GroupBy; len(got) != 2 || got[0] != "k" || got[1] != "x" {
		t.Fatalf("inner GroupBy = %v, want [k x]", got)
	}
	for _, a := range inner.AggExprs {
		if a.Distinct {
			t.Fatal("inner level must carry no distinct aggregates")
		}
	}
	if len(out.GroupBy) != 1 || out.GroupBy[0] != "k" {
		t.Fatalf("outer GroupBy = %v, want [k]", out.GroupBy)
	}
	byOut := map[string]AggExpr{}
	for _, a := range out.AggExprs {
		if a.Distinct {
			t.Fatal("outer level must carry no distinct aggregates")
		}
		byOut[a.OutputCol] = a
	}
	if a := byOut["d"]; a.Func != "count" || a.InputCol != "x" {
		t.Fatalf("distinct became %+v, want count(x)", a)
	}
	if a := byOut["s"]; a.Func != "sum" {
		t.Fatalf("sum recombine = %+v", a)
	}
	if a := byOut["c"]; a.Func != "sum" {
		t.Fatalf("count recombine = %+v, want sum of partial counts", a)
	}
	if a := byOut["m"]; a.Func != "min" {
		t.Fatalf("min recombine = %+v", a)
	}
}

func TestTwoLevelDistinctGuards(t *testing.T) {
	cases := map[string]*Node{
		"multi-distinct": aggNode([]string{"k"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d1", Distinct: true},
			{Func: "count", InputCol: "y", OutputCol: "d2", Distinct: true},
		}),
		"avg alongside": aggNode([]string{"k"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
			{Func: "avg", InputCol: "v", OutputCol: "a"},
		}),
		"distinct col is group key": aggNode([]string{"x"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
		}),
	}
	for name, n := range cases {
		if out := rewriteCountDistinctTwoLevel(n); out.Children[0].Type == NodeAggregate {
			t.Errorf("%s: rewrote but must fall through", name)
		}
	}
}

func TestTwoLevelDistinctKillSwitch(t *testing.T) {
	prev := twoLevelDistinctToggle.Set(false)
	defer twoLevelDistinctToggle.Set(prev)
	n := aggNode(nil, []AggExpr{{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true}})
	if out := rewriteCountDistinctTwoLevel(n); out.Children[0].Type == NodeAggregate {
		t.Fatal("kill switch off but rewrite fired")
	}
}
