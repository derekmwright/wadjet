package oracle

import "testing"

func res(cols []string, rows ...map[string]any) *Result {
	return &Result{Columns: cols, Rows: rows}
}

func row(pairs ...any) map[string]any {
	m := map[string]any{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i].(string)] = pairs[i+1]
	}
	return m
}

// TestCompareUnorderedIgnoresRowOrder: without an ORDER BY the row sequence is
// not part of the answer, so a reordered result must compare equal — otherwise
// the harness reports a divergence SQL never promised.
func TestCompareUnorderedIgnoresRowOrder(t *testing.T) {
	a := res([]string{"k"}, row("k", 1), row("k", 2), row("k", 3))
	b := res([]string{"k"}, row("k", 3), row("k", 1), row("k", 2))
	if d := Compare(a, b, CompareSpec{Mode: CmpUnordered}); d != "" {
		t.Errorf("reordered rows reported as a divergence: %s", d)
	}
	c := res([]string{"k"}, row("k", 3), row("k", 1), row("k", 9))
	if d := Compare(a, c, CompareSpec{Mode: CmpUnordered}); d == "" {
		t.Error("a changed VALUE was not caught under CmpUnordered")
	}
}

func TestCompareOrderedCatchesRowOrder(t *testing.T) {
	a := res([]string{"k"}, row("k", 1), row("k", 2), row("k", 3))
	b := res([]string{"k"}, row("k", 3), row("k", 1), row("k", 2))
	if d := Compare(a, b, CompareSpec{Mode: CmpOrdered}); d == "" {
		t.Error("row order divergence not caught under CmpOrdered")
	}
}

func TestCompareCountAndLimit(t *testing.T) {
	a := res([]string{"k"}, row("k", 1), row("k", 2))
	b := res([]string{"k"}, row("k", 7), row("k", 8))
	if d := Compare(a, b, CompareSpec{Mode: CmpCount}); d != "" {
		t.Errorf("equal counts reported as a divergence: %s", d)
	}
	c := res([]string{"k"}, row("k", 1))
	if d := Compare(a, c, CompareSpec{Mode: CmpCount}); d == "" {
		t.Error("differing row counts not caught")
	}
	over := res([]string{"k"}, row("k", 1), row("k", 2), row("k", 3))
	if d := Compare(a, over, CompareSpec{Mode: CmpCount, Limit: 2}); d == "" {
		t.Error("a result exceeding its LIMIT was not caught")
	}
}

// TestCompareOrderKeysIsTieImmune is the reason the key-sequence comparison
// exists: with ties, which row of a tie group comes first is arbitrary, but
// the SEQUENCE OF KEY VALUES is not. A dropped ORDER BY must still be caught.
func TestCompareOrderKeysIsTieImmune(t *testing.T) {
	spec := CompareSpec{Mode: CmpUnordered, OrderKeys: []OrderKey{{Alias: "g"}}}
	// Same keys in the same sequence; tied rows swapped.
	a := res([]string{"g", "v"}, row("g", 1, "v", "x"), row("g", 1, "v", "y"), row("g", 2, "v", "z"))
	b := res([]string{"g", "v"}, row("g", 1, "v", "y"), row("g", 1, "v", "x"), row("g", 2, "v", "z"))
	if d := Compare(a, b, spec); d != "" {
		t.Errorf("tie order reported as a divergence: %s", d)
	}
	// Key sequence genuinely different: the ordering was not honoured.
	c := res([]string{"g", "v"}, row("g", 2, "v", "z"), row("g", 1, "v", "x"), row("g", 1, "v", "y"))
	if d := Compare(a, c, spec); d == "" {
		t.Error("a dropped ORDER BY was not caught by the key-sequence comparison")
	}
}

func TestCompareMissingColumn(t *testing.T) {
	a := res([]string{"k", "v"}, row("k", 1, "v", 2))
	b := res([]string{"k"}, row("k", 1))
	if d := Compare(a, b, CompareSpec{Mode: CmpUnordered}); d == "" {
		t.Error("a result missing a requested column was not caught")
	}
}

func TestCheckOrder(t *testing.T) {
	keys := []OrderKey{{Alias: "k"}}
	sorted := res([]string{"k"}, row("k", 1), row("k", 2), row("k", 2), row("k", 5))
	if d := CheckOrder(sorted, keys); d != "" {
		t.Errorf("sorted rows reported as unsorted: %s", d)
	}
	unsorted := res([]string{"k"}, row("k", 5), row("k", 1))
	if d := CheckOrder(unsorted, keys); d == "" {
		t.Error("unsorted rows passed the ordering check")
	}

	desc := []OrderKey{{Alias: "k", Desc: true}}
	if d := CheckOrder(res([]string{"k"}, row("k", 5), row("k", 2), row("k", 2)), desc); d != "" {
		t.Errorf("descending rows reported as unsorted: %s", d)
	}
	if d := CheckOrder(res([]string{"k"}, row("k", 2), row("k", 5)), desc); d == "" {
		t.Error("ascending rows passed a DESC ordering check")
	}

	// PostgreSQL's NULL placement, which is what wadjet implements and what
	// the DuckDB arm configures the reference engine for: NULLS LAST on ASC,
	// NULLS FIRST on DESC. This checker used to place NULLs last in both
	// directions, so a correct DESC result was reported as unsorted; TPC-H has
	// no NULLs at all, so nothing exercised it until a fixture with nullable
	// sort keys arrived.
	if d := CheckOrder(res([]string{"k"}, row("k", 1), row("k", nil)), keys); d != "" {
		t.Errorf("trailing NULL reported as out of order (ASC, NULLS LAST): %s", d)
	}
	if d := CheckOrder(res([]string{"k"}, row("k", nil), row("k", 1)), keys); d == "" {
		t.Error("leading NULL passed the ASC ordering check (ASC is NULLS LAST)")
	}
	if d := CheckOrder(res([]string{"k"}, row("k", nil), row("k", 5)), desc); d != "" {
		t.Errorf("leading NULL reported as out of order (DESC, NULLS FIRST): %s", d)
	}
	if d := CheckOrder(res([]string{"k"}, row("k", 5), row("k", nil)), desc); d == "" {
		t.Error("trailing NULL passed the DESC ordering check (DESC is NULLS FIRST)")
	}

	// Multi-key: the second key only decides ties on the first.
	multi := []OrderKey{{Alias: "a"}, {Alias: "b", Desc: true}}
	ok := res([]string{"a", "b"}, row("a", 1, "b", 9), row("a", 1, "b", 3), row("a", 2, "b", 1))
	if d := CheckOrder(ok, multi); d != "" {
		t.Errorf("correctly ordered multi-key rows rejected: %s", d)
	}
	bad := res([]string{"a", "b"}, row("a", 1, "b", 3), row("a", 1, "b", 9))
	if d := CheckOrder(bad, multi); d == "" {
		t.Error("a broken secondary DESC key passed the ordering check")
	}

	// An unprojected key cannot be checked; that must not fail the result.
	if d := CheckOrder(sorted, []OrderKey{{Alias: "absent"}}); d != "" {
		t.Errorf("unprojected key produced an ordering failure: %s", d)
	}
}

// TestCheckOrderToleratesULPDrift: a float sort key computed slightly
// differently by two correct engines must not read as an ordering failure.
func TestCheckOrderToleratesULPDrift(t *testing.T) {
	r := res([]string{"k"}, row("k", 1.0000000000000002), row("k", 1.0))
	if d := CheckOrder(r, []OrderKey{{Alias: "k"}}); d != "" {
		t.Errorf("last-ULP drift reported as an ordering failure: %s", d)
	}
}

func TestParseCell(t *testing.T) {
	if got := ParseCell("<NULL>", 1.0); got != nil {
		t.Errorf("<NULL> parsed as %#v, want nil", got)
	}
	if got := ParseCell("", int64(1)); got != nil {
		t.Errorf("empty cell parsed as %#v, want nil", got)
	}
	if got := ParseCell("2.5", 1.0); got != 2.5 {
		t.Errorf("float cell parsed as %#v", got)
	}
	if got := ParseCell("7", int64(1)); got != int64(7) {
		t.Errorf("int cell parsed as %#v", got)
	}
	// An integer column the other engine renders with a decimal point still
	// has to compare equal, so it becomes a float rather than a string.
	if got := ParseCell("7.0", int64(1)); got != 7.0 {
		t.Errorf("int cell rendered as 7.0 parsed as %#v", got)
	}
	if got := ParseCell("ALGERIA", "x"); got != "ALGERIA" {
		t.Errorf("text cell parsed as %#v", got)
	}
	// No reference type: keep the raw text rather than guessing.
	if got := ParseCell("1996-01-02", nil); got != "1996-01-02" {
		t.Errorf("untyped cell parsed as %#v", got)
	}
}
