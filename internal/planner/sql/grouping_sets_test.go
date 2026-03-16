package sql

import (
	"testing"
)

func TestParseGroupingSets(t *testing.T) {
	sql := "SELECT region, product, SUM(sales) FROM t GROUP BY GROUPING SETS ((region, product), (region), ())"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.GroupingSets) != 3 {
		t.Fatalf("expected 3 grouping sets, got %d", len(info.GroupingSets))
	}
	// Set 0: (region, product)
	if len(info.GroupingSets[0]) != 2 {
		t.Fatalf("set 0: expected 2 columns, got %d", len(info.GroupingSets[0]))
	}
	// Set 1: (region)
	if len(info.GroupingSets[1]) != 1 {
		t.Fatalf("set 1: expected 1 column, got %d", len(info.GroupingSets[1]))
	}
	// Set 2: () — empty set (grand total)
	if len(info.GroupingSets[2]) != 0 {
		t.Fatalf("set 2: expected 0 columns, got %d", len(info.GroupingSets[2]))
	}

	// GroupBy should contain all referenced columns
	if len(info.GroupBy) < 2 {
		t.Fatalf("expected at least 2 GroupBy columns, got %d", len(info.GroupBy))
	}
}

func TestParseCube(t *testing.T) {
	sql := "SELECT a, b, SUM(c) FROM t GROUP BY CUBE(a, b)"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	// CUBE(a, b) = 4 sets: (a,b), (a), (b), ()
	if len(info.GroupingSets) != 4 {
		t.Fatalf("expected 4 grouping sets for CUBE(a,b), got %d", len(info.GroupingSets))
	}

	// Verify we have all the expected sets
	counts := map[int]int{} // length → count
	for _, s := range info.GroupingSets {
		counts[len(s)]++
	}
	if counts[2] != 1 || counts[1] != 2 || counts[0] != 1 {
		t.Fatalf("unexpected set distribution: %v (sets: %v)", counts, info.GroupingSets)
	}
}

func TestParseRollup(t *testing.T) {
	sql := "SELECT year, month, day, SUM(sales) FROM t GROUP BY ROLLUP(year, month, day)"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	// ROLLUP(year, month, day) = 4 sets: (year,month,day), (year,month), (year), ()
	if len(info.GroupingSets) != 4 {
		t.Fatalf("expected 4 grouping sets for ROLLUP(year,month,day), got %d", len(info.GroupingSets))
	}

	// Verify hierarchy: longest to shortest
	for i := 0; i < len(info.GroupingSets)-1; i++ {
		if len(info.GroupingSets[i]) < len(info.GroupingSets[i+1]) {
			t.Fatalf("ROLLUP sets should decrease in length: set %d has %d cols, set %d has %d cols",
				i, len(info.GroupingSets[i]), i+1, len(info.GroupingSets[i+1]))
		}
	}

	// Last set should be empty (grand total)
	if len(info.GroupingSets[len(info.GroupingSets)-1]) != 0 {
		t.Fatalf("last ROLLUP set should be empty")
	}
}

func TestExpandCube(t *testing.T) {
	sets := expandCube([]string{"a", "b", "c"})
	// 2^3 = 8 sets
	if len(sets) != 8 {
		t.Fatalf("expected 8 sets for CUBE(a,b,c), got %d", len(sets))
	}
}

func TestExpandRollup(t *testing.T) {
	sets := expandRollup([]string{"a", "b", "c"})
	// 4 sets: (a,b,c), (a,b), (a), ()
	if len(sets) != 4 {
		t.Fatalf("expected 4 sets for ROLLUP(a,b,c), got %d", len(sets))
	}
	if len(sets[0]) != 3 {
		t.Fatalf("first set should have 3 cols")
	}
	if len(sets[3]) != 0 {
		t.Fatalf("last set should be empty")
	}
}

func TestParseSimpleGroupByUnchanged(t *testing.T) {
	// Ensure simple GROUP BY still works
	sql := "SELECT department, COUNT(*) FROM employees GROUP BY department"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.GroupBy) != 1 || info.GroupBy[0] != "department" {
		t.Fatalf("expected simple GROUP BY department, got %v", info.GroupBy)
	}
	if info.GroupingSets != nil {
		t.Fatal("expected nil GroupingSets for simple GROUP BY")
	}
}
