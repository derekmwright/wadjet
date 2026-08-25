package physical

import "testing"

// F1: an IN-subquery whose FROM reads a RECURSIVE CTE has no set-producer
// lowering — buildSubqueryPipeline reads it as ZERO rows with no error, which
// materializeInSubquery would inline as a genuine empty set (IN → 0, NOT IN →
// every row). sqlReadsRecursiveCTE is what makes the materializer REFUSE and
// route local. It must see the recursion through a derived wrapper.
func TestSqlReadsRecursiveCTE(t *testing.T) {
	rec := map[string]bool{"r": true}
	cases := []struct {
		name string
		sql  string
		want bool
	}{
		{"direct", "SELECT r.x FROM r", true},
		{"aliased", "SELECT y.x FROM r y", true},
		{"derived wrapper", "SELECT y.x FROM (SELECT x FROM r) y", true},
		{"two-level wrapper", "SELECT z.x FROM (SELECT x FROM (SELECT x FROM r) y) z", true},
		{"joined", "SELECT r.x FROM r JOIN t ON t.k = r.x", true},
		{"non-recursive base", "SELECT n FROM mk_inner", false},
		{"non-recursive derived", "SELECT y.n FROM (SELECT n FROM mk_inner) y", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sqlReadsRecursiveCTE(c.sql, rec); got != c.want {
				t.Fatalf("sqlReadsRecursiveCTE(%q) = %v, want %v", c.sql, got, c.want)
			}
		})
	}
	self := "WITH RECURSIVE s(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM s WHERE x<3) SELECT s.x FROM s"
	if !sqlReadsRecursiveCTE(self, map[string]bool{}) {
		t.Fatalf("a subquery's own WITH RECURSIVE was not detected")
	}
}
