package logical

import (
	"testing"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// findFirstNode returns the first node (pre-order) matching pred.
func findFirstNode(n *Node, pred func(*Node) bool) *Node {
	if n == nil {
		return nil
	}
	if pred(n) {
		return n
	}
	for _, c := range n.Children {
		if found := findFirstNode(c, pred); found != nil {
			return found
		}
	}
	return nil
}

// TestFilteredExistsDecorrelation_FlipsOpAndSkipsDedup covers two logical-
// layer bugs on decorrelated EXISTS with a non-equality correlated condition
// (fixed 2026-07-06):
//
//  1. extractCorrelatedCols normalizes the condition to probe-left form; when
//     the OUTER column was written on the right ("o_totalprice > c_acctbal"),
//     the operator must flip ("c_acctbal < o_totalprice") or the comparison
//     inverts.
//  2. dedupSemiAntiBuildSide must skip filtered semi/anti joins — its
//     Project(keys)→Distinct wrapper drops the filter's build-side column
//     (o_totalprice), making the probe-time filter reject every row.
func TestFilteredExistsDecorrelation_FlipsOpAndSkipsDedup(t *testing.T) {
	q := `SELECT c_name FROM customer
		WHERE EXISTS (
			SELECT 1 FROM orders
			WHERE o_custkey = c_custkey AND o_totalprice > c_acctbal)`

	parsed, err := plansql.Parse(q)
	if err != nil {
		t.Fatal("Parse error:", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal("Extract error:", err)
	}
	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatal("Build error:", err)
	}
	annotateScanColumnsForTest(plan)
	optimized := Optimize(plan, annotateScanColumnsForTest)

	semi := findFirstNode(optimized, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "semi"
	})
	if semi == nil {
		t.Fatal("no semi join found — EXISTS decorrelation did not fire")
	}
	if semi.JoinFilter != "c_acctbal < o_totalprice" {
		t.Fatalf("JoinFilter = %q, want %q (probe-left with flipped operator)",
			semi.JoinFilter, "c_acctbal < o_totalprice")
	}
	// The dedup wrapper is Distinct at insertion and may be rewritten to a
	// GroupBy aggregate by rewriteDistinctAsGroupBy — reject both.
	if dist := findFirstNode(semi.Children[1], func(n *Node) bool {
		return n.Type == NodeDistinct || n.Type == NodeAggregate
	}); dist != nil {
		t.Fatal("filtered semi join's build side was wrapped in a key-only dedup — it drops the JoinFilter's build column")
	}
	// The unfiltered shape must still dedup (the guard is filter-scoped).
	q2 := `SELECT c_name FROM customer
		WHERE EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)`
	parsed2, err := plansql.Parse(q2)
	if err != nil {
		t.Fatal("Parse error:", err)
	}
	info2, err := plansql.ExtractSelect(parsed2)
	if err != nil {
		t.Fatal("Extract error:", err)
	}
	plan2, err := BuildFromSelect(info2)
	if err != nil {
		t.Fatal("Build error:", err)
	}
	annotateScanColumnsForTest(plan2)
	optimized2 := Optimize(plan2, annotateScanColumnsForTest)
	semi2 := findFirstNode(optimized2, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "semi"
	})
	if semi2 == nil {
		t.Fatal("no semi join found for unfiltered EXISTS")
	}
	if dist := findFirstNode(semi2.Children[1], func(n *Node) bool {
		return n.Type == NodeDistinct || n.Type == NodeAggregate
	}); dist == nil {
		t.Fatal("unfiltered semi join build side lost its key dedup — the JoinFilter guard is over-broad")
	}
}
