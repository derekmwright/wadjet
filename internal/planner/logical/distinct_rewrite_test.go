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

// Semi/anti build-side dedup Distincts are planner-inserted and carry no
// user-visible semantics; the physical planner has dedicated handling for the
// Distinct(Project) shape. The rewrite must leave a MARKED one alone.
//
// Position is no longer what protects them: the rewrite runs over the whole
// tree (#466), because a user DISTINCT under a join or an aggregate has to be
// executed too. Only BuildSideDedup separates the two now.
func TestRewriteDistinctAsGroupBy_LeavesSemiAntiDedupAlone(t *testing.T) {
	child := NewScan("s", "")
	proj := NewProject(child, []Projection{{Expr: "k", Column: "k"}})
	dedup := NewDistinct(proj)
	dedup.BuildSideDedup = true
	join := &Node{Type: NodeJoin, JoinType: "semi", Children: []*Node{NewScan("t", ""), dedup}}
	root := NewProject(join, []Projection{{Expr: "a", Column: "a"}})

	out := rewriteDistinctAsGroupBy(root)
	if !hasNodeType(out, NodeDistinct) {
		t.Fatalf("semi/anti build-side Distinct was rewritten\n%s", out.PrettyPrint(0))
	}
}

// The marker is the whole of that protection, so the pass that inserts the
// Distinct must set it. Losing the field silently would turn every semi/anti
// build dedup into an aggregate and change the physical plan for TPC-H
// Q04/Q21 without any test noticing.
func TestDedupSemiAntiBuildSideMarksWhatItInserts(t *testing.T) {
	right := NewScan("lineitem", "")
	right.ScanColumns = []string{"l_orderkey"}
	left := NewScan("orders", "")
	left.ScanColumns = []string{"o_orderkey"}
	join := &Node{
		Type:     NodeJoin,
		JoinType: "semi",
		JoinCond: "o_orderkey = l_orderkey",
		Children: []*Node{left, right},
	}
	out := dedupSemiAntiBuildSide(join)

	d := findFirst(out, NodeDistinct)
	if d == nil {
		t.Fatalf("no build-side dedup was inserted\n%s", out.PrettyPrint(0))
	}
	if !d.BuildSideDedup {
		t.Fatal("the inserted build-side dedup is not marked BuildSideDedup —" +
			" rewriteDistinctAsGroupBy will rewrite it into an aggregate")
	}
}

// A user DISTINCT that is NOT on the root path must be rewritten: the DAG
// emits no stage for a Distinct node, so anything left un-rewritten and
// un-marked is a DISTINCT nobody applies (#466).
func TestRewriteDistinctAsGroupBy_RewritesOffTheRootPath(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		groupBy []string
	}{
		{"under an aggregate", "SELECT COUNT(*) AS c FROM (SELECT DISTINCT a FROM t) u", []string{"a"}},
		{"under an aggregate, renamed", "SELECT SUM(k) AS s FROM (SELECT DISTINCT a AS k FROM t) u", []string{"a"}},
		{"several columns", "SELECT COUNT(*) AS c FROM (SELECT DISTINCT a, b FROM t) u", []string{"a", "b"}},
		{"under a projection", "SELECT a FROM (SELECT DISTINCT a FROM t) u", []string{"a"}},
		{"under a join", "SELECT COUNT(*) AS c FROM (SELECT DISTINCT a FROM t) u JOIN s ON u.a = s.a", []string{"a"}},
		{"grouped over a derived distinct",
			"SELECT k, COUNT(*) AS c FROM (SELECT DISTINCT a AS k, b FROM t) u GROUP BY k", []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := rewriteDistinctAsGroupBy(buildPlan(t, tc.sql))
			if hasNodeType(plan, NodeDistinct) {
				t.Fatalf("DISTINCT survived the rewrite — the DAG emits no stage for it,"+
					" so it would be dropped\n%s", plan.PrettyPrint(0))
			}
			agg := findDistinctGroupBy(plan, tc.groupBy)
			if agg == nil {
				t.Fatalf("no aggregate-free GROUP BY on %v\n%s", tc.groupBy, plan.PrettyPrint(0))
			}
		})
	}
}

// findDistinctGroupBy locates the aggregate-free GROUP BY a DISTINCT lowers
// to: no aggregate functions, grouping on exactly the given keys.
func findDistinctGroupBy(n *Node, keys []string) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeAggregate && len(n.AggExprs) == 0 && len(n.GroupBy) == len(keys) {
		match := true
		for i := range keys {
			if n.GroupBy[i] != keys[i] {
				match = false
				break
			}
		}
		if match {
			return n
		}
	}
	for _, c := range n.Children {
		if f := findDistinctGroupBy(c, keys); f != nil {
			return f
		}
	}
	return nil
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
