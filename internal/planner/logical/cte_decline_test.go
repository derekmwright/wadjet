package logical

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A CTE or a DERIVED TABLE feeding a correlated subquery's FROM is BUILT into
// the semi/anti join's build side, not declined (#852).
//
// It used to be declined, and for a reason that was true at the time: the
// rewrites assembled that side out of `NewScan(info.Tables[0].Name)`, and
// neither a CTE name nor a derived table's SQL text is a name a Scan can hold
// — the build side became a scan of a table the catalog has never heard of,
// which yields zero batches, so IN answered 0 and NOT IN every row in silence
// (#535, #581, #571). Declining made those queries right and SLOW: the
// subquery stayed a per-row predicate and the re-run read the whole inner
// relation once per outer row.
//
// buildFromClause is the builder's own FROM assembly, so the build side is now
// the subquery's own plan and the decline is unnecessary. This test is the
// decline's inverse: the shapes it used to refuse must now produce a semi/anti
// join. Revert decorrelatedInnerPlan and every non-recursive case here fails.
func TestDecorrelationBuildsACTEOrDerivedInner(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"cte in", `WITH src AS (SELECT c_custkey FROM customer) SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.c_custkey FROM src b)`},
		{"cte exists", `WITH src AS (SELECT c_custkey FROM customer) SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM src b WHERE b.c_custkey = a.o_custkey)`},
		{"derived exists", `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM (SELECT c_custkey FROM customer) b WHERE b.c_custkey = a.o_custkey)`},
		{"derived in", `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.c_custkey FROM (SELECT c_custkey FROM customer) b)`},
		{"comma inner exists", `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM customer b, nation k WHERE k.n_nationkey = b.c_nationkey AND b.c_custkey = a.o_custkey)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildPlan(t, tc.sql)
			annotateScanColumnsForTest(plan)
			opt := Optimize(plan, annotateScanColumnsForTest)
			if join := findNodeMatching(opt, func(n *Node) bool {
				return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
			}); join == nil {
				t.Fatalf("the subquery was NOT decorrelated — it stays a per-row re-run, "+
					"reading its inner relation once per outer row (#852):\n%s", opt.PrettyPrint(0))
			}
		})
	}
}

// What the build side still declines, and why each one.
//
// A RECURSIVE CTE reference is a TAGGED SCAN the physical planner resolves by
// fixed-point iteration from its own cache, and a semi-join build side is not
// prepared through that cache; the decline is also what keeps the
// materialized-IN route's own refusal reachable (#F1).
//
// A derived table or an ordinary CTE reference JOINED to another relation
// declines for a different reason, measured rather than assumed: the build
// side then carries two renamings and the stage DAG's carried-column
// derivation answers a different number than the single-process arm does.
func TestDecorrelationDeclinesTheInnersItCannotName(t *testing.T) {
	rec := `WITH RECURSIVE r(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM r WHERE x < 3) `
	cases := []struct{ name, sql string }{
		{"recursive direct", rec + `SELECT o_orderkey FROM orders a WHERE a.o_shippriority IN (SELECT r.x FROM r)`},
		{"recursive derived wrapper", rec + `SELECT o_orderkey FROM orders a WHERE a.o_shippriority IN (SELECT y.x FROM (SELECT x FROM r) y)`},
		// A derived table or a CTE reference JOINED to another relation: the
		// build side would carry two renamings — the join's and the derived
		// Project's — and the stage DAG's carried-column derivation answers a
		// DIFFERENT number for it than the single-process arm does. See
		// decorrelatedInnerPlan for the measured pair.
		{"cte joined to a base table", `WITH src AS (SELECT c_custkey, c_nationkey FROM customer) SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.c_custkey FROM src b JOIN nation k ON k.n_nationkey = b.c_nationkey)`},
		{"derived table joined to a base table", `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.c_custkey FROM (SELECT c_custkey, c_nationkey FROM customer) b JOIN nation k ON k.n_nationkey = b.c_nationkey)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildPlan(t, tc.sql)
			annotateScanColumnsForTest(plan)
			opt := Optimize(plan, annotateScanColumnsForTest)
			if join := findNodeMatching(opt, func(n *Node) bool {
				return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
			}); join != nil {
				t.Fatalf("this inner was decorrelated onto a semi/anti build side "+
					"instead of declined:\n%s", opt.PrettyPrint(0))
			}
		})
	}
}

// innerRelationsAreBuildable declines a RECURSIVE CTE — directly, through the
// subquery's own WITH, and through a derived wrapper — and nothing else. A
// base table, an ordinary CTE reference and a derived table are all buildable.
func TestInnerRelationsAreBuildable(t *testing.T) {
	info := func(sql string) *plansql.SelectInfo {
		t.Helper()
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		si, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		return si
	}
	plain := []plansql.CTEDef{{Name: "src"}}
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"base table", "SELECT k FROM base t"},
		{"cte reference", "SELECT k FROM src t"},
		{"derived table", "SELECT k FROM (SELECT k FROM base) t"},
	} {
		if !innerRelationsAreBuildable(info(tc.sql), plain) {
			t.Errorf("%s must be buildable", tc.name)
		}
	}

	recursive := []plansql.CTEDef{{Name: "r", Recursive: true}}
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"recursive cte reference", "SELECT x FROM r"},
		{"recursive cte through a derived wrapper", "SELECT x FROM (SELECT x FROM r) y"},
		{"recursive cte on the right of a join", "SELECT x FROM base b JOIN r ON r.x = b.k"},
	} {
		if innerRelationsAreBuildable(info(tc.sql), recursive) {
			t.Errorf("%s must NOT be buildable", tc.name)
		}
	}
}
