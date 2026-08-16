package logical

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

func buildPlan(t *testing.T, sql string) *Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return plan
}

func hasNodeType(n *Node, nt NodeType) bool {
	if n == nil {
		return false
	}
	if n.Type == nt {
		return true
	}
	for _, c := range n.Children {
		if hasNodeType(c, nt) {
			return true
		}
	}
	return false
}

func findFirst(n *Node, nt NodeType) *Node {
	if n == nil {
		return nil
	}
	if n.Type == nt {
		return n
	}
	for _, c := range n.Children {
		if f := findFirst(c, nt); f != nil {
			return f
		}
	}
	return nil
}

func TestRewriteDistinctAsGroupBy(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		rewritten bool
		groupBy   []string
	}{
		{"single column", "SELECT DISTINCT a FROM t", true, []string{"a"}},
		{"two columns", "SELECT DISTINCT a, b FROM t", true, []string{"a", "b"}},
		{"aliased column", "SELECT DISTINCT a AS x FROM t", true, []string{"a"}},
		{"order by", "SELECT DISTINCT a FROM t ORDER BY a", true, []string{"a"}},
		{"order by limit", "SELECT DISTINCT a FROM t ORDER BY a LIMIT 5", true, []string{"a"}},
		{"duplicate column deduped", "SELECT DISTINCT a, a FROM t", true, []string{"a"}},
		{"expression", "SELECT DISTINCT a, b + c FROM t", true, []string{"a", "b + c"}},
		{"function expression", "SELECT DISTINCT lower(a) AS x FROM t", true, []string{"lower(a)"}},
		{"star falls back", "SELECT DISTINCT * FROM t", false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := rewriteDistinctAsGroupBy(buildPlan(t, tc.sql))
			if hasNodeType(plan, NodeDistinct) == tc.rewritten {
				t.Fatalf("distinct present=%v, want rewritten=%v\n%s",
					!tc.rewritten, tc.rewritten, plan.PrettyPrint(0))
			}
			if !tc.rewritten {
				return
			}
			agg := findFirst(plan, NodeAggregate)
			if agg == nil {
				t.Fatalf("no aggregate node after rewrite\n%s", plan.PrettyPrint(0))
			}
			if len(agg.AggExprs) != 0 {
				t.Fatalf("expected aggregate-free GROUP BY, got %d agg exprs", len(agg.AggExprs))
			}
			if len(agg.GroupBy) != len(tc.groupBy) {
				t.Fatalf("group by = %v, want %v", agg.GroupBy, tc.groupBy)
			}
			for i := range tc.groupBy {
				if agg.GroupBy[i] != tc.groupBy[i] {
					t.Fatalf("group by = %v, want %v", agg.GroupBy, tc.groupBy)
				}
			}
			// The Project must sit ABOVE the aggregate so output naming
			// (aliases) resolves against the aggregate's group columns.
			proj := findFirst(plan, NodeProject)
			if proj == nil || findFirst(proj, NodeAggregate) == nil {
				t.Fatalf("project must be an ancestor of the aggregate\n%s", plan.PrettyPrint(0))
			}
		})
	}
}

// Semi/anti build-side dedup Distincts sit under joins — the rewrite must
// not touch them (they are not on the root path).
func TestRewriteDistinctAsGroupBy_LeavesSemiAntiDedupAlone(t *testing.T) {
	child := NewScan("s", "")
	proj := NewProject(child, []Projection{{Expr: "k", Column: "k"}})
	dedup := NewDistinct(proj)
	join := &Node{Type: NodeJoin, JoinType: "semi", Children: []*Node{NewScan("t", ""), dedup}}
	root := NewProject(join, []Projection{{Expr: "a", Column: "a"}})

	out := rewriteDistinctAsGroupBy(root)
	if !hasNodeType(out, NodeDistinct) {
		t.Fatalf("semi/anti build-side Distinct was rewritten\n%s", out.PrettyPrint(0))
	}
}

// The Distinct node can be the plan root and carry CTE definitions; the
// rewrite must transfer them to the new root.
func TestRewriteDistinctAsGroupBy_PreservesCTEs(t *testing.T) {
	plan := buildPlan(t, "WITH c AS (SELECT a FROM t) SELECT DISTINCT a FROM c")
	if plan.CTEs == nil {
		t.Skip("builder did not attach CTEs at root for this shape")
	}
	out := rewriteDistinctAsGroupBy(plan)
	if out.CTEs == nil {
		t.Fatal("CTEs lost in rewrite")
	}
	if hasNodeType(out, NodeDistinct) {
		t.Fatalf("distinct not rewritten\n%s", out.PrettyPrint(0))
	}
}
