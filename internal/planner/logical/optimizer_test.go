package logical

import (
	"testing"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
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

	// Build: year = '2026' AST expression
	astExpr := &sqlparser.ComparisonExpr{
		Operator: "=",
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("year")},
		Right:    &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("2026")},
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

	astExpr := &sqlparser.AndExpr{
		Left: &sqlparser.ComparisonExpr{
			Operator: "=",
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("year")},
			Right:    &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("2026")},
		},
		Right: &sqlparser.ComparisonExpr{
			Operator: "=",
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("month")},
			Right:    &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("03")},
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

	astExpr := &sqlparser.ComparisonExpr{
		Operator: ">",
		Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("year")},
		Right:    &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("2025")},
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
