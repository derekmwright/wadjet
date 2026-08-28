package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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

func TestReorderJoins_CBO_FactTableProbes(t *testing.T) {
	// Three-way join: A (1M) JOIN B (1K filtered) JOIN C (100K)
	// In a left-deep hash join tree, the leftmost relation is the initial probe
	// that streams through all subsequent hash tables. The CBO should keep the
	// largest table (fact table) as the probe side and build smaller tables
	// (dimensions) into hash tables, minimizing build cost and memory.
	scanA := NewScan("big_table", "a")
	scanA.ScanRowEstimate = 1000000
	scanA.ScanColumns = []string{"id"}

	scanB := NewScan("small_table", "b")
	scanB.ScanRowEstimate = 1000
	scanB.ScanColumns = []string{"id", "status"}
	scanB.ScanPredicates = []Predicate{{Column: "status", Op: "=", Value: "active"}}

	scanC := NewScan("medium_table", "c")
	scanC.ScanRowEstimate = 100000
	scanC.ScanColumns = []string{"id"}

	// Original: (A JOIN B) JOIN C
	join1 := NewJoin(scanA, scanB, "inner", "a.id = b.id")
	join2 := NewJoin(join1, scanC, "inner", "b.id = c.id")

	result := reorderJoins(join2)

	if result.Type != NodeJoin {
		t.Fatalf("expected join, got %s", result.Type)
	}
	// Verify all three tables are in the plan
	tables := collectAllTables(result)
	if !tables["big_table"] || !tables["small_table"] || !tables["medium_table"] {
		t.Errorf("missing tables in result, got: %v", tables)
	}
	// The largest table should be the leftmost leaf (probe side) since
	// building it into a hash table would be very expensive.
	leftmost := result
	for leftmost.Type == NodeJoin {
		leftmost = leftmost.Children[0]
	}
	if leftmost.TableName != "big_table" {
		t.Errorf("CBO should keep fact table 'big_table' as probe (leftmost), got %q", leftmost.TableName)
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

func TestReorderJoins_CBO_AvoidsBuildingFactTable(t *testing.T) {
	// CBO should avoid building the large fact table into a hash table.
	// lineitem (6M) should be the probe (leftmost), with supplier (10K)
	// and part+filter (~20K) as build sides.
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

	// Verify all tables present
	tables := collectAllTables(result)
	if !tables["lineitem"] || !tables["supplier"] || !tables["part"] {
		t.Errorf("missing tables in result, got: %v", tables)
	}
	// lineitem (6M) should be leftmost (probe side) — building it would be
	// extremely expensive (memory + cache misses).
	leftmost := result
	for leftmost.Type == NodeJoin {
		leftmost = leftmost.Children[0]
	}
	if leftmost.TableName != "lineitem" {
		t.Errorf("CBO should keep fact table 'lineitem' as probe (leftmost), got %q", leftmost.TableName)
	}
	// Neither build side should be lineitem
	var checkBuilds func(n *Node)
	checkBuilds = func(n *Node) {
		if n == nil || n.Type != NodeJoin {
			return
		}
		right := n.Children[1]
		if right.Type == NodeScan && right.TableName == "lineitem" {
			t.Error("lineitem should not be a build side (right child)")
		}
		checkBuilds(n.Children[0])
	}
	checkBuilds(result)
}

// collectAllTables returns all table names in a plan tree.
func collectAllTables(n *Node) map[string]bool {
	result := make(map[string]bool)
	var walk func(*Node)
	walk = func(node *Node) {
		if node == nil {
			return
		}
		if node.Type == NodeScan && node.TableName != "" {
			result[node.TableName] = true
		}
		for _, child := range node.Children {
			walk(child)
		}
	}
	walk(n)
	return result
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

	result := decorrelateExists(filter, nil)

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

	result := decorrelateExists(filter, nil)

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

	result := decorrelateExists(filter, nil)

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

	result := decorrelateExists(filter, nil)

	if result.Type != NodeJoin {
		t.Fatalf("expected anti join, got %s", result.Type)
	}
	if result.JoinType != "anti" {
		t.Fatalf("expected anti join type, got %q", result.JoinType)
	}
}

// Regression test for the CTE materialization fence (issue #127 follow-on,
// pre-existing wrong-results): pushdownPredicates swapped Filter-above-
// Project unconditionally, so an outer WHERE over a CTE reference was
// pushed BELOW the CTEName-tagged subtree root. The physical planner then
// substituted the cached (unfiltered) CTE result for the whole tagged
// subtree, silently dropping the predicate — `WITH m AS (...) SELECT ...
// FROM m WHERE grp = 'x'` returned every CTE row. Predicates must stay
// above the fence.
func TestPushdownStopsAtCTEFence(t *testing.T) {
	scan := NewScan("metrics", "")
	cteRoot := NewProject(scan, []Projection{{Column: "grp", Expr: "grp"}})
	cteRoot.CTEName = "m"
	filter := NewFilter(cteRoot, []Predicate{{Column: "grp", Op: "=", Value: "'g1'"}})

	got := pushdownPredicates(filter)
	if got.Type != NodeFilter {
		t.Fatalf("filter was pushed through the CTE fence: root = %s", got.Type)
	}
	if len(got.Children) != 1 || got.Children[0].CTEName != "m" {
		t.Fatalf("expected Filter(CTE-tagged Project), got %s over %v", got.Type, got.Children)
	}

	// Control: without the tag, the swap must still happen (the whole
	// point of the pushdown).
	scan2 := NewScan("metrics", "")
	proj2 := NewProject(scan2, []Projection{{Column: "grp", Expr: "grp"}})
	filter2 := NewFilter(proj2, []Predicate{{Column: "grp", Op: "=", Value: "'g1'"}})
	got2 := pushdownPredicates(filter2)
	if got2.Type != NodeProject || got2.Children[0].Type != NodeFilter {
		t.Fatalf("untagged Filter-Project swap broken: root = %s", got2.Type)
	}
}

// Regression for #584: an UNQUALIFIED conjunct of the OUTER WHERE, sitting
// above a semi/anti join produced by decorrelating a correlated EXISTS, must
// be attributed to the PROBE (left) side and never pushed onto the build
// (right) side. A self-EXISTS decorrelates to `orders t0` over `orders sub`,
// both carrying `o_totalprice`; the merged column map used to resolve the bare
// `o_totalprice` to the build relation and push the predicate onto the
// subquery's scan, silently filtering the wrong relation. A semi/anti join
// emits only the probe side's columns, so a predicate above it can only mean
// the probe.
func TestPushFilterThroughSemiJoinAttributesUnqualifiedToProbe(t *testing.T) {
	for _, jt := range []string{"semi", "anti"} {
		for _, spelling := range []string{"o_totalprice < 1000", "t0.o_totalprice < 1000"} {
			t.Run(jt+"/"+spelling, func(t *testing.T) {
				left := NewScan("orders", "t0")
				left.ScanColumns = []string{"o_totalprice", "o_clerk"}
				right := NewScan("orders", "sub")
				right.ScanColumns = []string{"o_totalprice", "o_clerk"}
				join := NewJoin(left, right, jt, "o_clerk = o_clerk")
				filter := NewFilter(join, []Predicate{{Raw: spelling, ASTExpr: tryParseExpr(spelling)}})

				got := pushdownPredicates(filter)

				if got.Type != NodeJoin || got.JoinType != jt {
					t.Fatalf("expected the predicate to push through the %s join, got root %s/%q", jt, got.Type, got.JoinType)
				}
				// Probe side must now carry the predicate.
				if got.Children[0].Type != NodeFilter {
					t.Fatalf("outer conjunct was not pushed onto the probe side: probe root = %s", got.Children[0].Type)
				}
				// Build side (the decorrelated subquery scan) must be untouched.
				if got.Children[1].Type != NodeScan {
					t.Fatalf("outer conjunct was mis-attributed to the build (subquery) side: build root = %s (#584)", got.Children[1].Type)
				}
			})
		}
	}
}

// Control for #584: over an INNER join, a genuinely build-side predicate still
// pushes to the build side — the fix must not over-correct semi/anti behavior
// onto ordinary joins.
func TestPushFilterThroughInnerJoinStillPushesRight(t *testing.T) {
	left := NewScan("orders", "o")
	left.ScanColumns = []string{"o_orderkey", "o_custkey"}
	right := NewScan("customer", "c")
	right.ScanColumns = []string{"c_custkey", "c_acctbal"}
	join := NewJoin(left, right, "inner", "o_custkey = c_custkey")
	filter := NewFilter(join, []Predicate{{Raw: "c_acctbal > 0", ASTExpr: tryParseExpr("c_acctbal > 0")}})

	got := pushdownPredicates(filter)
	if got.Type != NodeJoin {
		t.Fatalf("expected predicate to push through the inner join, got root %s", got.Type)
	}
	if got.Children[1].Type != NodeFilter {
		t.Fatalf("build-side predicate was not pushed onto the build side: build root = %s", got.Children[1].Type)
	}
}
