package logical

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A CTE feeding a decorrelated subquery's FROM must be DECLINED, exactly as a
// derived table is: it resolves to NewScan of the CTE's bare name — a table no
// catalog has — so the semi/anti build side is empty and IN answers 0 / NOT IN
// every row (#535, #581). Declining routes it to the materialize/local paths,
// which resolve the CTE. A recursive CTE is declined too, through a derived
// wrapper as well (#F1 then refuses the materialized set).
func TestDecorrelationDeclinesACTEInner(t *testing.T) {
	rec := `WITH RECURSIVE r(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM r WHERE x < 3) `
	cases := []struct{ name, sql string }{
		{"cte in", `WITH src AS (SELECT c_custkey FROM customer) SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.c_custkey FROM src b)`},
		{"cte exists", `WITH src AS (SELECT c_custkey FROM customer) SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM src b WHERE b.c_custkey = a.o_custkey)`},
		{"cte joined", `WITH src AS (SELECT c_custkey, c_nationkey FROM customer) SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.c_custkey FROM src b JOIN nation k ON k.n_nationkey = b.c_nationkey)`},
		{"recursive direct", rec + `SELECT o_orderkey FROM orders a WHERE a.o_shippriority IN (SELECT r.x FROM r)`},
		{"recursive derived wrapper", rec + `SELECT o_orderkey FROM orders a WHERE a.o_shippriority IN (SELECT y.x FROM (SELECT x FROM r) y)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildPlan(t, tc.sql)
			annotateScanColumnsForTest(plan)
			opt := Optimize(plan, annotateScanColumnsForTest)
			if join := findNodeMatching(opt, func(n *Node) bool {
				return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
			}); join != nil {
				t.Fatalf("a CTE inner was decorrelated onto a semi/anti build side instead of declined:\n%s", opt.PrettyPrint(0))
			}
		})
	}
}

// innerRelationsAreScannable declines a derived table, a CTE reference, and a
// CTE the subquery declares for itself; a base table stays scannable.
func TestInnerRelationsAreScannable(t *testing.T) {
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
	ctes := []plansql.CTEDef{{Name: "src"}}
	if !innerRelationsAreScannable(info("SELECT k FROM base t"), ctes) {
		t.Fatal("a base table should be scannable")
	}
	if innerRelationsAreScannable(info("SELECT k FROM src t"), ctes) {
		t.Fatal("a CTE reference must not be scannable")
	}
	if innerRelationsAreScannable(info("SELECT k FROM (SELECT k FROM base) t"), ctes) {
		t.Fatal("a derived table must not be scannable")
	}
}
