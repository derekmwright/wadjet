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
