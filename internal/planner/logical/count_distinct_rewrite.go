// SPDX-License-Identifier: MIT

package logical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/optswitch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Rewrite exactly one COUNT(DISTINCT bare_col) into groups (K,x), then K:
// COUNT(x) skips the NULL-x row; COUNT partials sum, SUM partials sum, MIN/MAX pass through.
// Other aggregates must be simple decomposable COUNT(*), COUNT(col), SUM, MIN, MAX or AVG;
// arguments may be expressions evaluated at level 1. AVG uses SUM+COUNT partials
// and a division projection above level 2. No grouping sets.
// Approx-distinct keeps its sketch; computed distinct arguments and multi-distinct decline.
// Removing AggExpr.Distinct uses typed GROUP BY and ordinary distributed two-phase
// stages instead of the #291 single-task RawInputAggregate route (#294).
// See docs/internals/count-distinct-two-level-rewrite.md for the design.
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
		// Extra-aggregate gate (metal-validated 2026-08-17): decomposed
		// partials multiply level-1 state per (K, x) pair group. At the
		// same pair cardinality, the bare grouped distinct wins big
		// (ClickBench Q9 −31% hot) while four decomposed partials lose
		// bigger (Q10 +54%) — the accumulator traffic over tens of
		// millions of pair groups swamps the distinct-set saving. Until
		// NDV stats can bound pair cardinality at plan time, the grouped
		// rewrite fires only for a lone COUNT(DISTINCT). The global form
		// keeps full decomposition (level 1 is single-key; Q6 −40%).
		if len(n.AggExprs) > 1 {
			return n
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
	// projection above level 2; everything else re-aggregates with a single
	// partial.
	//
	// fins[i] is how the finalization projection, when one is built,
	// publishes original aggregate i: the level-2 output it reads, and whether
	// it is AVG's division or a COUNT's coalesce. Indexed by POSITION, never
	// by output name: two aggregates may share one (`COUNT(*) AS a,
	// COUNT(DISTINCT x) AS a`).
	type finItem struct {
		slot     string // the level-2 output this aggregate reads (AVG: its SUM leg)
		cnt      string // AVG only: the level-2 COUNT leg
		avg      bool
		zeroable bool // a non-distinct COUNT, re-summed at level 2 (#1584)
	}
	fins := make([]finItem, len(n.AggExprs))
	// outerIdx[i] is aggregate i's index in outerAggs (AVG: its SUM leg).
	outerIdx := make([]int, len(n.AggExprs))
	hasAvg, hasCount := false, false
	innerGroupBy := append(append([]string(nil), n.GroupBy...), x)
	innerAggs := make([]AggExpr, 0, len(n.AggExprs)-1)
	outerAggs := make([]AggExpr, 0, len(n.AggExprs))
	for i, a := range n.AggExprs {
		outerIdx[i] = len(outerAggs)
		if i == distinctIdx {
			// Level 2 counts the distinct column itself over level-1 rows.
			outerAggs = append(outerAggs, AggExpr{
				Func:      "count",
				InputCol:  x,
				OutputCol: a.OutputCol,
			})
			fins[i] = finItem{slot: fmt.Sprintf("%sdistinct_%d", plansql.SlotTwoLevel, i)}
			continue
		}
		if strings.EqualFold(a.Func, "avg") {
			sumP := fmt.Sprintf("%savgsum_%d", plansql.SlotTwoLevel, i)
			cntP := fmt.Sprintf("%savgcnt_%d", plansql.SlotTwoLevel, i)
			innerSum, innerCnt := a, a
			innerSum.Func, innerSum.OutputCol = "sum", sumP
			innerCnt.Func, innerCnt.OutputCol = "count", cntP
			innerAggs = append(innerAggs, innerSum, innerCnt)
			outerAggs = append(outerAggs,
				AggExpr{Func: "sum", InputCol: sumP, OutputCol: sumP},
				AggExpr{Func: "sum", InputCol: cntP, OutputCol: cntP})
			fins[i] = finItem{slot: sumP, cnt: cntP, avg: true}
			hasAvg = true
			continue
		}
		partial := fmt.Sprintf("%s%s_%d", plansql.SlotTwoLevel, strings.ToLower(a.Func), i)
		inner := a
		inner.OutputCol = partial
		innerAggs = append(innerAggs, inner)
		outer := AggExpr{InputCol: partial, OutputCol: a.OutputCol}
		fins[i] = finItem{slot: partial}
		// The recombining SUM declares what the ORIGINAL aggregate declares,
		// which is its partial's own type: bigint for a COUNT and a SUM(int4),
		// numeric for a SUM(int8) or SUM(numeric), the float for a float SUM.
		// Declared as a SUM over its bigint input, it said numeric where the
		// original and PostgreSQL say bigint (#1696). AVG's two legs are not
		// marked: its division declares the AVG.
		switch strings.ToLower(a.Func) {
		case "count":
			outer.Func = "sum" // sum of per-(K,x) counts
			outer.Recombines = true
			fins[i].zeroable = true
			hasCount = true
		case "sum":
			outer.Func = "sum"
			outer.Recombines = true
		case "min":
			outer.Func = "min"
		case "max":
			outer.Func = "max"
		}
		outerAggs = append(outerAggs, outer)
	}
	// Grouped, a group exists only when a row does, so the SUM of COUNT
	// partials is never over zero rows; only the ungrouped form needs the 0.
	zeroCounts := len(n.GroupBy) == 0 && hasCount
	finalize := hasAvg || zeroCounts
	if finalize {
		// The finalization projection reads level 2 by NAME, so every name it
		// reads must be one level 2 publishes exactly once. The user's names
		// need not be: two aggregates may share an alias, and a COUNT's
		// coalesce used to be published under the very name of the SUM it
		// wraps. So level 2 publishes each aggregate under a slot only this
		// rewrite mints (AVG's legs always did), and the projection alone
		// publishes the user's names. Read by a shared name, `COUNT(*) AS a,
		// COUNT(DISTINCT x) AS a` answered the COUNT twice (#1696).
		for i := range n.AggExprs {
			if !fins[i].avg {
				outerAggs[outerIdx[i]].OutputCol = fins[i].slot
			}
		}
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
	if !finalize {
		return outer
	}
	// Finalization: AVG's sum/count divided, and, ungrouped, a COUNT's summed
	// partials through COALESCE(…, 0); every other aggregate's slot is
	// published under its original name. The original order holds (group
	// keys, then aggregates). Division by a zero count reads NULL — AVG over
	// an all-NULL group (see expr arithDiv).
	projs := make([]Projection, 0, len(n.GroupBy)+len(n.AggExprs))
	for _, k := range n.GroupBy {
		projs = append(projs, Projection{Column: k, Alias: k})
	}
	for i, a := range n.AggExprs {
		f := fins[i]
		switch {
		case f.avg:
			projs = append(projs, Projection{
				Alias: a.OutputCol,
				Expr:  fmt.Sprintf("%s / %s", f.slot, f.cnt),
				ASTExpr: &plansql.BinaryOp{
					Left:  &plansql.ColRef{Column: f.slot},
					Op:    "/",
					Right: &plansql.ColRef{Column: f.cnt},
				},
			})
		case f.zeroable && zeroCounts:
			projs = append(projs, Projection{
				Alias: a.OutputCol,
				Expr:  fmt.Sprintf("coalesce(%s, 0)", f.slot),
				ASTExpr: &plansql.FuncCallNode{
					Name: "coalesce",
					Args: []plansql.Node{
						&plansql.ColRef{Column: f.slot},
						&plansql.Lit{Value: "0", Kind: plansql.LitNumber},
					},
				},
			})
		default:
			projs = append(projs, Projection{Column: f.slot, Alias: a.OutputCol})
		}
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
