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
	// COALESCE(c, 0): over zero rows level 1 has no groups and the SUM
	// answers NULL where COUNT answers 0 (#1584). Every other output passes
	// through by name.
	if out.Type != NodeProject || !out.PreservesAggOutputs || len(out.Children) != 1 {
		t.Fatalf("expected a finalization Project, got %v", out.Type)
	}
	for _, p := range out.Projections {
		if p.Alias == "c" {
			if p.Expr != "coalesce(c, 0)" {
				t.Fatalf("count finalization = %q, want coalesce(c, 0)", p.Expr)
			}
		} else if p.Column != p.Alias || p.Expr != "" {
			t.Fatalf("projection %+v must pass its column through", p)
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
	if a := byOut["d"]; a.Func != "count" || a.InputCol != "x" {
		t.Fatalf("distinct became %+v, want count(x)", a)
	}
	// The recombining SUMs are marked, so they declare their partial's type
	// (bigint for a COUNT) rather than SUM-over-bigint's numeric (#1696).
	if a := byOut["s"]; a.Func != "sum" || !a.Recombines {
		t.Fatalf("sum recombine = %+v", a)
	}
	if a := byOut["c"]; a.Func != "sum" || !a.Recombines {
		t.Fatalf("count recombine = %+v, want a recombining sum of partial counts", a)
	}
	if a := byOut["m"]; a.Func != "min" || a.Recombines {
		t.Fatalf("min recombine = %+v", a)
	}
}

// Where two outputs share a name and the rewrite builds its finalizing
// projection, the projection reads level 2 by that name and so reads the
// first column for both. The recombined aggregates keep SUM's own
// declaration there: declared bigint beside the bigint distinct count, the
// single-process refusal (#361 store guard) became the COUNT answered twice
// (#1696). Without a finalizing projection, or without a shared name, they
// are marked.
func TestTwoLevelDistinctSharedNameKeepsDeclaration(t *testing.T) {
	outerAggs := func(out *Node) []AggExpr {
		t.Helper()
		if out.Type == NodeProject {
			out = out.Children[0]
		}
		if out.Type != NodeAggregate || len(out.Children) != 1 || out.Children[0].Type != NodeAggregate {
			t.Fatalf("expected the two-level rewrite, got %v", out.Type)
		}
		return out.AggExprs
	}
	for _, tc := range []struct {
		name string
		aggs []AggExpr
		want bool // the recombining SUM is marked
	}{
		{"count shares the distinct's name", []AggExpr{
			{Func: "count", OutputCol: "a"},
			{Func: "count", InputCol: "x", OutputCol: "A", Distinct: true},
		}, false},
		{"count beside a shared pair", []AggExpr{
			{Func: "count", OutputCol: "c"},
			{Func: "min", InputCol: "v", OutputCol: "a"},
			{Func: "count", InputCol: "x", OutputCol: "a", Distinct: true},
		}, false},
		{"sum shares the name, no finalization", []AggExpr{
			{Func: "sum", InputCol: "v", OutputCol: "a"},
			{Func: "count", InputCol: "x", OutputCol: "a", Distinct: true},
		}, true},
		{"count, distinct names", []AggExpr{
			{Func: "count", OutputCol: "a"},
			{Func: "count", InputCol: "x", OutputCol: "b", Distinct: true},
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, a := range outerAggs(rewriteCountDistinctTwoLevel(aggNode(nil, tc.aggs))) {
				if a.Func == "sum" && a.Recombines != tc.want {
					t.Fatalf("recombining %+v: Recombines = %v, want %v", a, a.Recombines, tc.want)
				}
			}
		})
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
	if p := out.Projections[0]; p.Column != "d" {
		t.Fatalf("proj[0] = %+v, want d passthrough", p)
	}
	if p := out.Projections[1]; p.Alias != "a" || p.ASTExpr == nil {
		t.Fatalf("proj[1] = %+v, want division aliased to a", p)
	}
}
