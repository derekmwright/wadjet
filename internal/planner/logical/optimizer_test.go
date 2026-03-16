package logical

import (
	"testing"

	plansql "github.com/derekmwright/caelum/internal/planner/sql"
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

func TestReorderJoins_SmallestFirst(t *testing.T) {
	// Three-way join: A (large, no filter) JOIN B (filtered) JOIN C (no filter)
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
	// B (filtered, cheapest) should be the leftmost leaf
	leftmost := result
	for leftmost.Type == NodeJoin {
		leftmost = leftmost.Children[0]
	}
	if leftmost.TableName != "small_table" {
		t.Errorf("expected filtered table 'small_table' as leftmost, got %q", leftmost.TableName)
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

func TestReorderJoins_TwoTableNoop(t *testing.T) {
	scanA := NewScan("t1", "")
	scanB := NewScan("t2", "")
	join := NewJoin(scanA, scanB, "inner", "t1.id = t2.id")

	result := reorderJoins(join)
	if result.Type != NodeJoin {
		t.Fatalf("expected join, got %s", result.Type)
	}
	if result.Children[0].TableName != "t1" || result.Children[1].TableName != "t2" {
		t.Error("two-way join should not be reordered")
	}
}
