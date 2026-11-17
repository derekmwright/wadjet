// SPDX-License-Identifier: MIT

package logical

import "testing"

func aggNode(groupBy []string, aggs []AggExpr) *Node {
	scan := NewScan("t", "")
	// The cost gate requires int-typed key knowledge for grouped rewrites.
	scan.ScanIntCols = map[string]bool{"k": true, "x": true, "v": true}
	return NewAggregate(scan, groupBy, aggs)
}

func TestTwoLevelDistinctRewrite(t *testing.T) {
	// Decomposed partials alongside the distinct fire on the GLOBAL form
	// (the grouped form is gated to a lone distinct — Q10 metal lesson).
	n := aggNode(nil, []AggExpr{
		{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
		{Func: "sum", InputCol: "v", OutputCol: "s"},
		{Func: "count", OutputCol: "c"},
		{Func: "min", InputCol: "v", OutputCol: "m"},
	})
	out := rewriteCountDistinctTwoLevel(n)
	// Ungrouped, the summed COUNT partial is finalized through
	// COALESCE(…, 0): over zero rows level 1 has no groups and the SUM
	// answers NULL where COUNT answers 0 (#1584). Level 2 publishes every
	// aggregate under a slot the rewrite mints, and the projection publishes
	// each under the user's name, in order (#1696).
	if out.Type != NodeProject || !out.PreservesAggOutputs || len(out.Children) != 1 {
		t.Fatalf("expected a finalization Project, got %v", out.Type)
	}
	wantProj := []struct{ alias, column, expr string }{
		{"d", "__tl_distinct_0", ""},
		{"s", "__tl_sum_1", ""},
		{"c", "", "coalesce(__tl_count_2, 0)"},
		{"m", "__tl_min_3", ""},
	}
	if len(out.Projections) != len(wantProj) {
		t.Fatalf("projections = %+v", out.Projections)
	}
	for i, w := range wantProj {
		p := out.Projections[i]
		if p.Alias != w.alias || p.Column != w.column || p.Expr != w.expr {
			t.Fatalf("projection %d = %+v, want alias %s column %q expr %q", i, p, w.alias, w.column, w.expr)
		}
	}
	out = out.Children[0]
	if out.Type != NodeAggregate || len(out.Children) != 1 || out.Children[0].Type != NodeAggregate {
		t.Fatalf("expected two stacked aggregates, got %v over %v", out.Type, out.Children[0].Type)
	}
	inner := out.Children[0]
	if got := inner.GroupBy; len(got) != 1 || got[0] != "x" {
		t.Fatalf("inner GroupBy = %v, want [x]", got)
	}
	for _, a := range inner.AggExprs {
		if a.Distinct {
			t.Fatal("inner level must carry no distinct aggregates")
		}
	}
	if len(out.GroupBy) != 0 {
		t.Fatalf("outer GroupBy = %v, want []", out.GroupBy)
	}
	byOut := map[string]AggExpr{}
	for _, a := range out.AggExprs {
		if a.Distinct {
			t.Fatal("outer level must carry no distinct aggregates")
		}
		byOut[a.OutputCol] = a
	}
	if a := byOut["__tl_distinct_0"]; a.Func != "count" || a.InputCol != "x" {
		t.Fatalf("distinct became %+v, want count(x)", a)
	}
	// The recombining SUMs are marked, so they declare their partial's type
	// (bigint for a COUNT) rather than SUM-over-bigint's numeric (#1696).
	if a := byOut["__tl_sum_1"]; a.Func != "sum" || !a.Recombines {
		t.Fatalf("sum recombine = %+v", a)
	}
	if a := byOut["__tl_count_2"]; a.Func != "sum" || !a.Recombines {
		t.Fatalf("count recombine = %+v, want a recombining sum of partial counts", a)
	}
	if a := byOut["__tl_min_3"]; a.Func != "min" {
		t.Fatalf("min recombine = %+v", a)
	}
}

func TestTwoLevelDistinctGuards(t *testing.T) {
	cases := map[string]*Node{
		"multi-distinct": aggNode([]string{"k"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d1", Distinct: true},
			{Func: "count", InputCol: "y", OutputCol: "d2", Distinct: true},
		}),
		"non-decomposable alongside": aggNode([]string{"k"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
			{Func: "approx_distinct", InputCol: "v", OutputCol: "a"},
		}),
		"grouped with extras (Q10 gate)": aggNode([]string{"k"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
			{Func: "sum", InputCol: "v", OutputCol: "s"},
		}),
		"distinct col is group key": aggNode([]string{"x"}, []AggExpr{
			{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
		}),
		"string group key (cost gate)": func() *Node {
			n := aggNode([]string{"s"}, []AggExpr{
				{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
			})
			return n
		}(),
		"no type annotation (cost gate)": func() *Node {
			scan := NewScan("t", "")
			return NewAggregate(scan, []string{"k"}, []AggExpr{
				{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
			})
		}(),
	}
	for name, n := range cases {
		if out := rewriteCountDistinctTwoLevel(n); out.Children[0].Type == NodeAggregate {
			t.Errorf("%s: rewrote but must fall through", name)
		}
	}
}

func TestTwoLevelDistinctGlobalNeedsNoTypes(t *testing.T) {
	scan := NewScan("t", "")
	n := NewAggregate(scan, nil, []AggExpr{
		{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
	})
	if out := rewriteCountDistinctTwoLevel(n); out.Children[0].Type != NodeAggregate {
		t.Fatal("global distinct must rewrite regardless of type annotations")
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

func TestTwoLevelDistinctGroupedLoneFires(t *testing.T) {
	n := aggNode([]string{"k"}, []AggExpr{
		{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
	})
	out := rewriteCountDistinctTwoLevel(n)
	if out.Type != NodeAggregate || out.Children[0].Type != NodeAggregate {
		t.Fatalf("grouped lone distinct must rewrite, got %v over %v", out.Type, out.Children[0].Type)
	}
	if got := out.Children[0].GroupBy; len(got) != 2 || got[0] != "k" || got[1] != "x" {
		t.Fatalf("inner GroupBy = %v, want [k x]", got)
	}
}

func TestTwoLevelDistinctAvgRewrite(t *testing.T) {
	// AVG decomposition fires on the global form.
	n := aggNode(nil, []AggExpr{
		{Func: "count", InputCol: "x", OutputCol: "d", Distinct: true},
		{Func: "avg", InputCol: "v", OutputCol: "a"},
	})
	out := rewriteCountDistinctTwoLevel(n)
	if out.Type != NodeProject {
		t.Fatalf("AVG rewrite must add a finalization projection, got %v", out.Type)
	}
	agg := out.Children[0]
	if agg.Type != NodeAggregate || agg.Children[0].Type != NodeAggregate {
		t.Fatalf("expected Project over two stacked aggregates")
	}
	inner := agg.Children[0]
	innerFuncs := map[string]string{}
	for _, a := range inner.AggExprs {
		innerFuncs[a.OutputCol] = a.Func
	}
	if innerFuncs["__tl_avgsum_1"] != "sum" || innerFuncs["__tl_avgcnt_1"] != "count" {
		t.Fatalf("inner AVG partials = %v, want sum+count", innerFuncs)
	}
	outerFuncs := map[string]string{}
	for _, a := range agg.AggExprs {
		outerFuncs[a.OutputCol] = a.Func
	}
	if outerFuncs["__tl_avgsum_1"] != "sum" || outerFuncs["__tl_avgcnt_1"] != "sum" {
		t.Fatalf("outer AVG partials = %v, want sum+sum", outerFuncs)
	}
	if len(out.Projections) != 2 {
		t.Fatalf("projections = %+v, want [d a]", out.Projections)
	}
	if p := out.Projections[0]; p.Column != "__tl_distinct_0" || p.Alias != "d" {
		t.Fatalf("proj[0] = %+v, want the distinct slot published as d", p)
	}
	if p := out.Projections[1]; p.Alias != "a" || p.ASTExpr == nil {
		t.Fatalf("proj[1] = %+v, want division aliased to a", p)
	}
}
