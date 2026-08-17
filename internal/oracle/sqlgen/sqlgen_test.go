package sqlgen

import "testing"

func testSchema() *Schema {
	return &Schema{
		Tables: []Table{
			{Name: "t1", Cols: []Column{
				{Name: "t1_key", Kind: KindInt, Lits: []string{"1", "5", "9"}},
				{Name: "t1_val", Kind: KindFloat, Lits: []string{"0.5", "2.5"}},
				{Name: "t1_name", Kind: KindString, Lits: []string{"'alpha'", "'beta'"}},
			}},
			{Name: "t2", Cols: []Column{
				{Name: "t2_key", Kind: KindInt, Lits: []string{"1", "3"}},
				{Name: "t2_tag", Kind: KindString},
			}},
		},
		Edges: []Edge{{LeftTable: "t1", LeftCol: "t1_key", RightTable: "t2", RightCol: "t2_key"}},
	}
}

func TestGeneratorDeterministic(t *testing.T) {
	s := testSchema()
	for seed := int64(0); seed < 50; seed++ {
		a := New(seed, s).Query().SQL()
		b := New(seed, s).Query().SQL()
		if a != b {
			t.Fatalf("seed %d not deterministic:\n%s\n%s", seed, a, b)
		}
		if a == "" {
			t.Fatalf("seed %d produced empty SQL", seed)
		}
	}
}

func TestGeneratorCoversTargetShapes(t *testing.T) {
	s := testSchema()
	var aggFree, distinct, havingSub, joined int
	for seed := int64(0); seed < 300; seed++ {
		q := New(seed, s).Query()
		if len(q.GroupBy) > 0 && len(q.Select) == len(q.GroupBy) {
			aggFree++
		}
		if q.Distinct {
			distinct++
		}
		if len(q.Having) > 0 && len(q.Having) > 40 { // subquery form is long
			havingSub++
		}
		if len(q.Tables) > 1 {
			joined++
		}
	}
	if aggFree == 0 || distinct == 0 || havingSub == 0 || joined == 0 {
		t.Fatalf("historical-breaker shapes not covered in 300 seeds: aggFree=%d distinct=%d havingSub=%d joined=%d",
			aggFree, distinct, havingSub, joined)
	}
}

func TestShrinkReducesToMinimal(t *testing.T) {
	s := testSchema()
	// Find a reasonably big query.
	var q *Query
	for seed := int64(0); ; seed++ {
		q = New(seed, s).Query()
		if len(q.Where) >= 2 && len(q.Select) >= 2 {
			break
		}
	}
	// Failure predicate: "still selects t1_key somewhere". Shrink should
	// strip everything not needed to keep that true.
	fails := func(c *Query) bool {
		for _, sItem := range c.Select {
			if sItem == "t1_key" {
				return true
			}
		}
		return false
	}
	if !fails(q) {
		// Force the condition into the query so the walk has something
		// to preserve.
		q.Select = append(q.Select, "t1_key")
	}
	min := Shrink(s, q, fails)
	if !fails(min) {
		t.Fatal("shrunk query no longer fails")
	}
	if len(min.Select) != 1 || len(min.Where) != 0 || min.Having != "" || min.Distinct || min.Limit != 0 {
		t.Fatalf("not minimal: %s", min.SQL())
	}
}
