package logical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/optswitch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// rewriteCountDistinctTwoLevel rewrites an aggregate containing exactly one
// COUNT(DISTINCT x) into two stacked aggregates, so the distinct set rides
// the typed GROUP BY fast paths instead of per-group distinct-set maps:
//
//	Aggregate{GroupBy: K, [COUNT(DISTINCT x), SUM(a), COUNT(*), ...]}
//	⇒ Aggregate{GroupBy: K, [COUNT(x), SUM(__tl_sum0), SUM(__tl_cnt1), ...]}
//	    → Aggregate{GroupBy: K+[x], [SUM(a) AS __tl_sum0, COUNT(*) AS __tl_cnt1, ...]}
//
// Level 1 groups by (K, x): one row per distinct (key, x) pair, with the
// other aggregates decomposed into re-aggregable partials. Level 2 groups
// by K: COUNT(x) over level-1 rows IS the distinct count (COUNT skips the
// NULL-x row, matching COUNT(DISTINCT) semantics), and the partials
// recombine (COUNT→SUM of counts, SUM→SUM of sums, MIN/MAX pass through).
//
// Why: profile-attributed (2026-08-17 c6a telemetry). The distinct-set
// path pays per-row map inserts into per-group Go maps; both levels of
// the rewritten form land on the typed SoA aggregation paths and the
// off-heap arena. As a structural bonus, rewritten shapes no longer carry
// AggExpr.Distinct, so distributed plans take the ordinary parallel
// two-phase aggregate stages instead of the #291 single-task
// RawInputAggregate route (#294's ask, delivered by construction).
//
// Scope (v1):
//   - Exactly one distinct aggregate; it must be COUNT(DISTINCT col) over
//     a bare column (approx_distinct keeps its sketch path; expressions
//     and multi-distinct fall through).
//   - Every other aggregate is a decomposable simple: COUNT(*)/COUNT(col),
//     SUM, MIN, MAX, AVG over bare columns or expressions (the expression
//     evaluates at level 1, exactly as it would have in the original).
//     AVG decomposes into SUM+COUNT partials with a division projection
//     above level 2 (ClickBench Q10's AVG(ResolutionWidth)).
//   - No grouping sets.
var twoLevelDistinctToggle = optswitch.Register("two-level-distinct", "WADJET_TWO_LEVEL_DISTINCT",
	"rewrite COUNT(DISTINCT x) aggregates into two stacked GROUP BYs on the typed fast paths")

func rewriteCountDistinctTwoLevel(n *Node) *Node {
	if n == nil {
		return n
	}
	for i, c := range n.Children {
		n.Children[i] = rewriteCountDistinctTwoLevel(c)
	}
	if n.Type != NodeAggregate || !twoLevelDistinctToggle.On() {
		return n
	}
	if len(n.GroupingSets) > 0 || len(n.Children) != 1 {
		return n
	}

	distinctIdx := -1
	for i, a := range n.AggExprs {
		if !a.Distinct {
			continue
		}
		if distinctIdx >= 0 {
			return n // multi-distinct: can't share one level-1 key
		}
		if !strings.EqualFold(a.Func, "count") || a.InputCol == "" {
			return n
		}
		if a.InputExpr != nil {
			if _, bare := a.InputExpr.(*plansql.ColRef); !bare {
				return n
			}
		}
		distinctIdx = i
	}
	if distinctIdx < 0 {
		return n
	}
	for i, a := range n.AggExprs {
		if i == distinctIdx {
			continue
		}
		switch strings.ToLower(a.Func) {
		case "count", "sum", "min", "max", "avg":
		default:
			return n
		}
	}
	x := n.AggExprs[distinctIdx].InputCol
	// Cost gate (metal-validated 2026-08-17): the rewrite pays when level 1
	// lands on a typed aggregation path — no group keys (single-column
	// GROUP BY x fast paths; ClickBench Q6 −42% hot) or an all-integer
	// (K ∪ x) key set (dual/multi-int; Q9 −29%). A string group key pushes
	// level 1 onto the multi-column generic path at pair cardinality and
	// LOSES (Q14 +87% before this gate) — those shapes keep the
	// distinct-set path.
	if len(n.GroupBy) > 0 {
		intCols := scanIntCols(n.Children[0])
		if intCols == nil || !intCols[strings.ToLower(x)] {
			return n
		}
		for _, k := range n.GroupBy {
			if !intCols[strings.ToLower(k)] {
				return n
			}
		}
	}
	for _, k := range n.GroupBy {
		if strings.EqualFold(k, x) {
			// COUNT(DISTINCT k) with k a group key is a degenerate 0/1;
			// the rewrite is still correct but the level-1 key set equals
			// K — no win, extra plumbing. Leave it alone.
			return n
		}
	}

	// Level 1: GROUP BY K + x, partials for the non-distinct aggregates.
	// AVG decomposes into SUM+COUNT partials at both levels and a division
	// projection above level 2 (avgDivs); everything else re-aggregates
	// with a single partial.
	type avgDiv struct{ out, sum, cnt string }
	var avgDivs []avgDiv
	innerGroupBy := append(append([]string(nil), n.GroupBy...), x)
	innerAggs := make([]AggExpr, 0, len(n.AggExprs)-1)
	outerAggs := make([]AggExpr, 0, len(n.AggExprs))
	for i, a := range n.AggExprs {
		if i == distinctIdx {
			// Level 2 counts the distinct column itself over level-1 rows.
			outerAggs = append(outerAggs, AggExpr{
				Func:      "count",
				InputCol:  x,
				OutputCol: a.OutputCol,
			})
			continue
		}
		if strings.EqualFold(a.Func, "avg") {
			sumP := fmt.Sprintf("__tl_avgsum_%d", i)
			cntP := fmt.Sprintf("__tl_avgcnt_%d", i)
			innerSum, innerCnt := a, a
			innerSum.Func, innerSum.OutputCol = "sum", sumP
			innerCnt.Func, innerCnt.OutputCol = "count", cntP
			innerAggs = append(innerAggs, innerSum, innerCnt)
			outerAggs = append(outerAggs,
				AggExpr{Func: "sum", InputCol: sumP, OutputCol: sumP},
				AggExpr{Func: "sum", InputCol: cntP, OutputCol: cntP})
			avgDivs = append(avgDivs, avgDiv{out: a.OutputCol, sum: sumP, cnt: cntP})
			continue
		}
		partial := fmt.Sprintf("__tl_%s_%d", strings.ToLower(a.Func), i)
		inner := a
		inner.OutputCol = partial
		innerAggs = append(innerAggs, inner)
		outer := AggExpr{InputCol: partial, OutputCol: a.OutputCol}
		switch strings.ToLower(a.Func) {
		case "count":
			outer.Func = "sum" // sum of per-(K,x) counts
		case "sum":
			outer.Func = "sum"
		case "min":
			outer.Func = "min"
		case "max":
			outer.Func = "max"
		}
		outerAggs = append(outerAggs, outer)
	}

	inner := NewAggregate(n.Children[0], innerGroupBy, innerAggs)
	// Group-by-expression ASTs stay aligned with GroupBy: K's exprs carry
	// over and x (a bare column) gets a ColRef entry. When the original
	// carried none, both levels stay plain-column.
	if n.GroupByExprs != nil {
		exprs := append([]plansql.Node(nil), n.GroupByExprs...)
		exprs = append(exprs, &plansql.ColRef{Column: x})
		inner.GroupByExprs = exprs
	}
	outer := NewAggregate(inner, append([]string(nil), n.GroupBy...), outerAggs)
	outer.GroupByExprs = n.GroupByExprs
	if len(avgDivs) == 0 {
		return outer
	}
	// AVG finalization: project sum/count into the original output name,
	// passing every other output column through by name in the original
	// order (group keys, then aggregates). Division by a zero count reads
	// NULL — AVG over an all-NULL group (see expr arithDiv).
	div := make(map[string]avgDiv, len(avgDivs))
	for _, d := range avgDivs {
		div[d.out] = d
	}
	projs := make([]Projection, 0, len(n.GroupBy)+len(n.AggExprs))
	for _, k := range n.GroupBy {
		projs = append(projs, Projection{Column: k, Alias: k})
	}
	for _, a := range n.AggExprs {
		if d, ok := div[a.OutputCol]; ok {
			projs = append(projs, Projection{
				Alias: d.out,
				Expr:  fmt.Sprintf("%s / %s", d.sum, d.cnt),
				ASTExpr: &plansql.BinaryOp{
					Left:  &plansql.ColRef{Column: d.sum},
					Op:    "/",
					Right: &plansql.ColRef{Column: d.cnt},
				},
			})
			continue
		}
		projs = append(projs, Projection{Column: a.OutputCol, Alias: a.OutputCol})
	}
	fin := NewProject(outer, projs)
	fin.PreservesAggOutputs = true
	return fin
}

// scanIntCols walks through single-child passthrough nodes to the scan
// feeding an aggregate and returns its integer-typed column set, or nil
// when the input shape (joins, subqueries) hides the types.
func scanIntCols(n *Node) map[string]bool {
	for n != nil {
		switch n.Type {
		case NodeScan:
			return n.ScanIntCols
		case NodeFilter, NodeProject, NodeLimit, NodeSort:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}
