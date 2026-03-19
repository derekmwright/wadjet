package logical

import (
	"strings"
	"testing"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

func TestExtractPartitionFilters_Equality(t *testing.T) {
	// Filter(year = '2026') → Scan(events)
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Raw: "year = '2026'"},
	})

	result := extractPartitionFilters(filter)

	// The filter should still exist (we don't remove it)
	if result.Type != NodeFilter {
		t.Fatalf("expected filter node, got %s", result.Type)
	}

	// The scan should now have partition filter
	scanNode := result.Children[0]
	if scanNode.Type != NodeScan {
		t.Fatalf("expected scan node, got %s", scanNode.Type)
	}
	if len(scanNode.PartitionFilter) == 0 {
		t.Fatal("expected partition filter to be extracted")
	}
	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %s", scanNode.PartitionFilter["year"])
	}
}

func TestExtractPartitionFilters_MultipleKeys(t *testing.T) {
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Raw: "year = '2026' AND month = '03' AND day = '15'"},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if scanNode == nil {
		t.Fatal("expected to find scan node")
	}

	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %s", scanNode.PartitionFilter["year"])
	}
	if scanNode.PartitionFilter["month"] != "03" {
		t.Fatalf("expected month=03, got %s", scanNode.PartitionFilter["month"])
	}
	if scanNode.PartitionFilter["day"] != "15" {
		t.Fatalf("expected day=15, got %s", scanNode.PartitionFilter["day"])
	}
}

func TestExtractPartitionFilters_NonPartitionKey(t *testing.T) {
	// Non-partition key columns should not be extracted
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Raw: "status = 'active'"},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if len(scanNode.PartitionFilter) != 0 {
		t.Fatalf("expected no partition filter, got %v", scanNode.PartitionFilter)
	}
}

func TestExtractPartitionFilters_MixedKeys(t *testing.T) {
	// Mix of partition and non-partition keys
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Raw: "year = '2026' AND status = 'active'"},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)

	// Only year should be extracted
	if len(scanNode.PartitionFilter) != 1 {
		t.Fatalf("expected 1 partition filter, got %d", len(scanNode.PartitionFilter))
	}
	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %s", scanNode.PartitionFilter["year"])
	}
}

func TestExtractPartitionFilters_WithAST(t *testing.T) {
	// Test with AST-based predicate
	scan := NewScan("events", "")

	// Build: year = '2026' AST expression using our types
	astExpr := &plansql.CmpExpr{
		Op:    "=",
		Left:  &plansql.ColRef{Column: "year"},
		Right: &plansql.Lit{Kind: plansql.LitString, Value: "2026"},
	}

	filter := NewFilter(scan, []Predicate{
		{ASTExpr: astExpr},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %v", scanNode.PartitionFilter)
	}
}

func TestExtractPartitionFilters_ASTWithAnd(t *testing.T) {
	scan := NewScan("events", "")

	astExpr := &plansql.AndNode{
		Left: &plansql.CmpExpr{
			Op:    "=",
			Left:  &plansql.ColRef{Column: "year"},
			Right: &plansql.Lit{Kind: plansql.LitString, Value: "2026"},
		},
		Right: &plansql.CmpExpr{
			Op:    "=",
			Left:  &plansql.ColRef{Column: "month"},
			Right: &plansql.Lit{Kind: plansql.LitString, Value: "03"},
		},
	}

	filter := NewFilter(scan, []Predicate{
		{ASTExpr: astExpr},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %v", scanNode.PartitionFilter)
	}
	if scanNode.PartitionFilter["month"] != "03" {
		t.Fatalf("expected month=03, got %v", scanNode.PartitionFilter)
	}
}

func TestExtractPartitionFilters_IgnoresNonEquality(t *testing.T) {
	// Inequality predicates on partition keys should not be extracted
	scan := NewScan("events", "")

	astExpr := &plansql.CmpExpr{
		Op:    ">",
		Left:  &plansql.ColRef{Column: "year"},
		Right: &plansql.Lit{Kind: plansql.LitString, Value: "2025"},
	}

	filter := NewFilter(scan, []Predicate{
		{ASTExpr: astExpr},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if len(scanNode.PartitionFilter) != 0 {
		t.Fatalf("expected no partition filter for >, got %v", scanNode.PartitionFilter)
	}
}

func TestExtractPartitionFilters_ThroughProject(t *testing.T) {
	// Filter → Project → Scan
	scan := NewScan("events", "")
	proj := NewProject(scan, []Projection{{Column: "name", Alias: "name"}})
	filter := NewFilter(proj, []Predicate{
		{Raw: "year = '2026'"},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if scanNode == nil {
		t.Fatal("expected to find scan node")
	}
	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %v", scanNode.PartitionFilter)
	}
}

func TestOptimize_PartitionExtraction(t *testing.T) {
	// Full optimizer pipeline
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Raw: "year = '2026' AND month = '03'"},
	})

	result := Optimize(filter)
	scanNode := findNodeType(result, NodeScan)
	if scanNode == nil {
		t.Fatal("expected to find scan node")
	}
	if scanNode.PartitionFilter["year"] != "2026" {
		t.Fatalf("expected year=2026, got %v", scanNode.PartitionFilter)
	}
	if scanNode.PartitionFilter["month"] != "03" {
		t.Fatalf("expected month=03, got %v", scanNode.PartitionFilter)
	}
}

func TestReorderJoins_LargestFirst(t *testing.T) {
	// Three-way join: A (large, no filter) JOIN B (filtered) JOIN C (no filter)
	// Greedy reorder starts with the most expensive (largest) table as the
	// initial probe (left) side, avoiding hashing large tables into build side.
	scanA := NewScan("big_table", "a")
	scanB := NewScan("small_table", "b")
	scanB.ScanPredicates = []Predicate{{Column: "status", Op: "=", Value: "active"}}
	scanC := NewScan("medium_table", "c")

	// Original: (A JOIN B) JOIN C
	join1 := NewJoin(scanA, scanB, "inner", "a.id = b.id")
	join2 := NewJoin(join1, scanC, "inner", "b.id = c.id")

	result := reorderJoins(join2)

	if result.Type != NodeJoin {
		t.Fatalf("expected join, got %s", result.Type)
	}
	// A (most expensive, no filter) should be the leftmost leaf (probe side)
	leftmost := result
	for leftmost.Type == NodeJoin {
		leftmost = leftmost.Children[0]
	}
	if leftmost.TableName != "big_table" {
		t.Errorf("expected largest table 'big_table' as leftmost, got %q", leftmost.TableName)
	}
}

func TestReorderJoins_SkipsOuterJoins(t *testing.T) {
	// LEFT JOIN order is semantically significant — should not be reordered
	scanA := NewScan("t1", "a")
	scanB := NewScan("t2", "b")
	scanB.ScanPredicates = []Predicate{{Column: "x", Op: "=", Value: 1}}
	scanC := NewScan("t3", "c")

	join1 := NewJoin(scanA, scanB, "left", "a.id = b.id")
	join2 := NewJoin(join1, scanC, "inner", "b.id = c.id")

	result := reorderJoins(join2)

	if result.Type != NodeJoin {
		t.Fatalf("expected join, got %s", result.Type)
	}
	// LEFT JOIN is not flattenable, so the inner join on top is only 2-way — no reorder
	left := result.Children[0]
	if left.Type != NodeJoin || left.JoinType != "left" {
		t.Errorf("expected left join preserved, got type=%s joinType=%s", left.Type, left.JoinType)
	}
}

func TestReorderJoins_TwoTableEqualCost(t *testing.T) {
	scanA := NewScan("t1", "")
	scanB := NewScan("t2", "")
	join := NewJoin(scanA, scanB, "inner", "t1.id = t2.id")

	result := reorderJoins(join)
	if result.Type != NodeJoin {
		t.Fatalf("expected join, got %s", result.Type)
	}
	// Equal cost: should not swap
	if result.Children[0].TableName != "t1" || result.Children[1].TableName != "t2" {
		t.Error("equal-cost two-way join should not be reordered")
	}
}

func TestReorderJoins_TwoTableSwap(t *testing.T) {
	// Small table (left) should be swapped to build (right) side
	scanSmall := NewScan("small", "")
	scanSmall.ScanRowEstimate = 1000 // 1K rows → cost 1

	scanLarge := NewScan("large", "")
	scanLarge.ScanRowEstimate = 1000000 // 1M rows → cost 1000

	join := NewJoin(scanSmall, scanLarge, "inner", "small.id = large.id")

	result := reorderJoins(join)
	if result.Type != NodeJoin {
		t.Fatalf("expected join, got %s", result.Type)
	}
	// Small table should be swapped to build (right) side
	if result.Children[0].TableName != "large" {
		t.Errorf("expected probe (left) = large, got %s", result.Children[0].TableName)
	}
	if result.Children[1].TableName != "small" {
		t.Errorf("expected build (right) = small, got %s", result.Children[1].TableName)
	}
}

func TestReorderJoins_FilterPreference(t *testing.T) {
	// In a multi-way join with probe = largest table, filtered relations
	// should be preferred as build sides (even if more expensive than
	// unfiltered alternatives) because their bloom filters reduce probe volume.
	//
	// Setup: lineitem (6M probe) joins supplier (10K), part+filter (200K).
	// Without filter preference: supplier (cost 10) chosen first.
	// With filter preference: part+filter chosen first.
	lineitem := NewScan("lineitem", "")
	lineitem.ScanRowEstimate = 6000000
	lineitem.ScanColumns = []string{"l_partkey", "l_suppkey"}

	supplier := NewScan("supplier", "")
	supplier.ScanRowEstimate = 10000
	supplier.ScanColumns = []string{"s_suppkey"}

	partScan := NewScan("part", "")
	partScan.ScanRowEstimate = 200000
	partScan.ScanColumns = []string{"p_partkey", "p_name"}

	partFiltered := NewFilter(partScan, []Predicate{{Column: "p_name", Op: "LIKE", Value: "%green%"}})

	// lineitem JOIN supplier ON l_suppkey = s_suppkey
	j1 := NewJoin(lineitem, supplier, "inner", "l_suppkey = s_suppkey")
	// ... JOIN part ON l_partkey = p_partkey (with filter on part)
	j2 := NewJoin(j1, partFiltered, "inner", "l_partkey = p_partkey")

	result := reorderJoins(j2)

	// After reorder: lineitem should be probe (left-most).
	// The first build (right child of innermost join) should be part+filter,
	// not supplier, because part has a selective filter.
	innerJoin := result
	for innerJoin.Type == NodeJoin && innerJoin.Children[0].Type == NodeJoin {
		innerJoin = innerJoin.Children[0]
	}
	// innerJoin is the innermost join: probe(lineitem) JOIN build(first_choice)
	rightChild := innerJoin.Children[1]
	// The right child should be the part+filter subtree
	if rightChild.Type == NodeFilter {
		// Good — part+filter was chosen first
		if rightChild.Children[0].TableName != "part" {
			t.Errorf("expected part as first build (under filter), got %s", rightChild.Children[0].TableName)
		}
	} else if rightChild.Type == NodeScan && rightChild.TableName == "part" {
		// Also acceptable
	} else {
		t.Errorf("expected part+filter as first build side, got type=%s table=%s", rightChild.Type, rightChild.TableName)
	}
}

func TestExtractCommonORPredicates_Basic(t *testing.T) {
	// (a = 1 AND c = 3) OR (b = 2 AND c = 3) → common: c = 3, remaining: (a = 1 OR b = 2)
	scan := NewScan("t1", "")
	orExpr := &plansql.OrNode{
		Left: &plansql.AndNode{
			Left:  &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "a"}, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "1"}},
			Right: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "c"}, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "3"}},
		},
		Right: &plansql.AndNode{
			Left:  &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "b"}, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "2"}},
			Right: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "c"}, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "3"}},
		},
	}
	filter := NewFilter(scan, []Predicate{{ASTExpr: orExpr, Raw: orExpr.String()}})

	result := extractCommonORPredicates(filter)
	if result.Type != NodeFilter {
		t.Fatalf("expected filter, got %s", result.Type)
	}
	// Should have 2 predicates: common (c = 3) + simplified OR (a = 1 OR b = 2)
	if len(result.Predicates) != 2 {
		t.Fatalf("expected 2 predicates, got %d: %v", len(result.Predicates), result.Predicates)
	}

	// Check that one predicate is the common term
	foundCommon := false
	foundOR := false
	for _, p := range result.Predicates {
		s := strings.ToLower(p.Raw)
		if strings.Contains(s, "c = 3") && !strings.Contains(s, "or") {
			foundCommon = true
		}
		if strings.Contains(s, "or") {
			foundOR = true
		}
	}
	if !foundCommon {
		t.Errorf("expected common predicate c = 3, got predicates: %v", result.Predicates)
	}
	if !foundOR {
		t.Errorf("expected simplified OR predicate, got predicates: %v", result.Predicates)
	}
}

func TestExtractCommonORPredicates_ThreeBranches(t *testing.T) {
	// (a AND c1 AND c2) OR (b AND c1 AND c2) OR (d AND c1 AND c2)
	// Should extract c1 and c2
	c1 := &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "mode"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "AIR"}}
	c2 := &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "instruct"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "DELIVER"}}

	branch1 := &plansql.AndNode{
		Left: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "brand"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "A"}},
		Right: &plansql.AndNode{Left: c1, Right: c2},
	}
	branch2 := &plansql.AndNode{
		Left: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "brand"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "B"}},
		Right: &plansql.AndNode{Left: c1, Right: c2},
	}
	branch3 := &plansql.AndNode{
		Left: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "brand"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "C"}},
		Right: &plansql.AndNode{Left: c1, Right: c2},
	}

	orExpr := &plansql.OrNode{
		Left:  branch1,
		Right: &plansql.OrNode{Left: branch2, Right: branch3},
	}

	scan := NewScan("t1", "")
	filter := NewFilter(scan, []Predicate{{ASTExpr: orExpr, Raw: orExpr.String()}})

	result := extractCommonORPredicates(filter)

	// Should have 3 predicates: c1, c2, and the simplified OR
	if len(result.Predicates) != 3 {
		t.Fatalf("expected 3 predicates, got %d: %v", len(result.Predicates), result.Predicates)
	}
}

func TestExtractCommonORPredicates_NoCommon(t *testing.T) {
	// (a = 1) OR (b = 2) → no common terms, should return unchanged
	orExpr := &plansql.OrNode{
		Left:  &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "a"}, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "1"}},
		Right: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Column: "b"}, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "2"}},
	}

	scan := NewScan("t1", "")
	filter := NewFilter(scan, []Predicate{{ASTExpr: orExpr, Raw: orExpr.String()}})

	result := extractCommonORPredicates(filter)
	if len(result.Predicates) != 1 {
		t.Fatalf("expected 1 predicate (unchanged), got %d", len(result.Predicates))
	}
}

func TestExtractColumnValueSetsFromOR(t *testing.T) {
	// Q07 pattern: (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
	//           OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
	// Should extract: n1.n_name IN ('FRANCE', 'GERMANY'), n2.n_name IN ('FRANCE', 'GERMANY')
	branch1 := &plansql.AndNode{
		Left:  &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Table: "n1", Column: "n_name"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "FRANCE"}},
		Right: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Table: "n2", Column: "n_name"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "GERMANY"}},
	}
	branch2 := &plansql.AndNode{
		Left:  &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Table: "n1", Column: "n_name"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "GERMANY"}},
		Right: &plansql.CmpExpr{Op: "=", Left: &plansql.ColRef{Table: "n2", Column: "n_name"}, Right: &plansql.Lit{Kind: plansql.LitString, Value: "FRANCE"}},
	}
	orExpr := &plansql.OrNode{Left: branch1, Right: branch2}

	scan := NewScan("t1", "")
	filter := NewFilter(scan, []Predicate{{ASTExpr: orExpr, Raw: orExpr.String()}})

	result := extractCommonORPredicates(filter)
	// Should have 3 predicates: original OR + two IN predicates
	if len(result.Predicates) != 3 {
		t.Fatalf("expected 3 predicates, got %d: %v", len(result.Predicates), result.Predicates)
	}

	// Check that we have both IN predicates
	foundN1In := false
	foundN2In := false
	for _, p := range result.Predicates {
		raw := strings.ToLower(p.Raw)
		if strings.Contains(raw, "n1.n_name") && strings.Contains(raw, "in") {
			foundN1In = true
		}
		if strings.Contains(raw, "n2.n_name") && strings.Contains(raw, "in") {
			foundN2In = true
		}
	}
	if !foundN1In {
		t.Errorf("expected n1.n_name IN predicate, got: %v", result.Predicates)
	}
	if !foundN2In {
		t.Errorf("expected n2.n_name IN predicate, got: %v", result.Predicates)
	}
}

func TestEstimateRelCost_JoinSubtree(t *testing.T) {
	// Join subtree cost should be estimated from children, not flat 200
	small := NewScan("nation", "")
	small.ScanRowEstimate = 25
	large := NewScan("lineitem", "")
	large.ScanRowEstimate = 6000000
	join := NewJoin(small, large, "inner", "n_nationkey = l_nationkey")

	cost := estimateRelCost(join)
	largeCost := estimateRelCost(large)
	// Join should reflect the larger child, not a fixed 200
	if cost < largeCost {
		t.Errorf("join cost %d should be >= large child cost %d", cost, largeCost)
	}
}

func TestEstimateRelCost_SemiAntiJoin(t *testing.T) {
	// Semi/anti join reduces cardinality — should be cheaper than left child
	outer := NewScan("orders", "")
	outer.ScanRowEstimate = 1500000
	inner := NewScan("lineitem", "")
	inner.ScanRowEstimate = 6000000
	semi := NewJoin(outer, inner, "semi", "o_orderkey = l_orderkey")

	semiCost := estimateRelCost(semi)
	outerCost := estimateRelCost(outer)
	if semiCost >= outerCost {
		t.Errorf("semi join cost %d should be < outer cost %d", semiCost, outerCost)
	}
}

func TestEstimateRelCost_AggregateReduction(t *testing.T) {
	// Aggregate should dramatically reduce cost relative to input
	scan := NewScan("lineitem", "")
	scan.ScanRowEstimate = 6000000
	agg := NewAggregate(scan, []string{"l_orderkey"}, nil)

	aggCost := estimateRelCost(agg)
	scanCost := estimateRelCost(scan)
	if aggCost > scanCost/5 {
		t.Errorf("aggregate cost %d should be much less than scan cost %d", aggCost, scanCost)
	}
}

func TestDecorrelateExists_SingleTable(t *testing.T) {
	// EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)
	// should become a semi join
	outerScan := NewScan("orders", "")
	outerScan.ScanColumns = []string{"o_orderkey", "o_orderstatus"}

	existsNode := &plansql.ExistsNode{
		Not: false,
		SQL: "SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey",
	}
	filter := NewFilter(outerScan, []Predicate{
		{Raw: "EXISTS(...)", ASTExpr: existsNode},
	})

	result := decorrelateExists(filter)

	if result.Type != NodeJoin {
		t.Fatalf("expected semi join, got %s", result.Type)
	}
	if result.JoinType != "semi" {
		t.Fatalf("expected semi join type, got %q", result.JoinType)
	}
	if !strings.Contains(result.JoinCond, "o_orderkey") {
		t.Fatalf("expected join condition on o_orderkey, got %q", result.JoinCond)
	}
}

func TestDecorrelateExists_NotExists(t *testing.T) {
	// NOT EXISTS should become an anti join
	outerScan := NewScan("customer", "c")
	outerScan.ScanColumns = []string{"c_custkey"}

	existsNode := &plansql.ExistsNode{
		Not: true,
		SQL: "SELECT 1 FROM orders WHERE o_custkey = c_custkey",
	}
	filter := NewFilter(outerScan, []Predicate{
		{Raw: "NOT EXISTS(...)", ASTExpr: existsNode},
	})

	result := decorrelateExists(filter)

	if result.Type != NodeJoin {
		t.Fatalf("expected anti join, got %s", result.Type)
	}
	if result.JoinType != "anti" {
		t.Fatalf("expected anti join type, got %q", result.JoinType)
	}
}

func TestDecorrelateExists_MultiTableSubquery(t *testing.T) {
	// EXISTS with JOINs in the subquery should now decorrelate
	// EXISTS (SELECT 1 FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey
	//         WHERE ps_partkey = p_partkey AND s_nationkey = 10)
	outerScan := NewScan("part", "")
	outerScan.ScanColumns = []string{"p_partkey", "p_name"}

	existsNode := &plansql.ExistsNode{
		Not: false,
		SQL: "SELECT 1 FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey WHERE ps_partkey = p_partkey AND s_nationkey = 10",
	}
	filter := NewFilter(outerScan, []Predicate{
		{Raw: "EXISTS(...)", ASTExpr: existsNode},
	})

	result := decorrelateExists(filter)

	if result.Type != NodeJoin {
		t.Fatalf("expected semi join for multi-table EXISTS, got %s", result.Type)
	}
	if result.JoinType != "semi" {
		t.Fatalf("expected semi join type, got %q", result.JoinType)
	}
	if !strings.Contains(result.JoinCond, "p_partkey") {
		t.Fatalf("expected join condition on p_partkey, got %q", result.JoinCond)
	}
	// Inner plan should contain a join (partsupp JOIN supplier)
	innerPlan := result.Children[1]
	foundInnerJoin := false
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeJoin {
			foundInnerJoin = true
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(innerPlan)
	if !foundInnerJoin {
		t.Fatal("expected inner plan to contain a join node for multi-table subquery")
	}
}

func TestDecorrelateExists_MultiTableNotExists(t *testing.T) {
	// NOT EXISTS with JOINs → anti join with inner join tree
	outerScan := NewScan("orders", "o")
	outerScan.ScanColumns = []string{"o_orderkey", "o_custkey"}

	existsNode := &plansql.ExistsNode{
		Not: true,
		SQL: "SELECT 1 FROM lineitem JOIN partsupp ON l_partkey = ps_partkey WHERE l_orderkey = o_orderkey AND ps_availqty > 0",
	}
	filter := NewFilter(outerScan, []Predicate{
		{Raw: "NOT EXISTS(...)", ASTExpr: existsNode},
	})

	result := decorrelateExists(filter)

	if result.Type != NodeJoin {
		t.Fatalf("expected anti join, got %s", result.Type)
	}
	if result.JoinType != "anti" {
		t.Fatalf("expected anti join type, got %q", result.JoinType)
	}
}
