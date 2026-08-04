package logical

import (
	"strings"
	"testing"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// testAnnotateTPCHScans populates ScanColumns the way the coordinator's
// catalog annotator does, so ownership-based passes resolve in tests.
func testAnnotateTPCHScans(plan *Node) {
	schema := map[string][]string{
		"customer": {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
		"orders":   {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"},
		"lineitem": {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"},
	}
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeScan && len(n.ScanColumns) == 0 {
			if cols, ok := schema[strings.ToLower(n.TableName)]; ok {
				n.ScanColumns = cols
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
}

func planFor(t *testing.T, sql string) *Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := BuildFromSelectWithCTEs(info, info.CTEs)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return Optimize(plan, testAnnotateTPCHScans)
}

func countPlanJoins(n *Node) (total, condlessInner int) {
	if n == nil {
		return 0, 0
	}
	if n.Type == NodeJoin {
		total++
		if isInnerJoin(n) && n.JoinCond == "" {
			condlessInner++
		}
	}
	for _, c := range n.Children {
		t, ci := countPlanJoins(c)
		total += t
		condlessInner += ci
	}
	return total, condlessInner
}

// TestCommaFromJoins_Issue281 pins the fix for issue #281: comma-separated
// FROM lists parse into bare info.Tables entries (the parser emits JoinInfo
// only for explicit JOIN syntax), and the builder silently dropped every
// table after the first — the plan collapsed to a single scan and queries
// returned wrong results with no error. The builder now folds the extra
// tables in as cross joins and liftWhereEquiPredsIntoJoins moves the WHERE
// equalities onto them, so comma-FROM plans match their explicit-JOIN
// equivalents.
func TestCommaFromJoins_Issue281(t *testing.T) {
	const q18Comma = `select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from orders, lineitem, customer
where o_orderkey in (
    select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300)
  and c_custkey = o_custkey and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100`

	const q18CTE = `with ol as (
  select o_orderkey, o_custkey, o_orderdate, o_totalprice, l_quantity
  from orders, lineitem
  where o_orderkey = l_orderkey
    and o_orderkey in (
      select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300)
)
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, ol
where c_custkey = o_custkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100`

	const plainComma = `select c_name from customer, orders where c_custkey = o_custkey`

	// Explicit-JOIN control: identical semantics to q18Comma; the lift
	// pass must not disturb it.
	const q18Explicit = `select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer
join orders on c_custkey = o_custkey
join lineitem on o_orderkey = l_orderkey
where o_orderkey in (
    select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300)
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100`

	cases := []struct {
		name     string
		sql      string
		minJoins int
	}{
		{"q18_comma_from", q18Comma, 3},   // customer + lineitem inner joins + semi
		{"q18_cte_comma_from", q18CTE, 3}, // issue #281's original repro
		{"plain_two_table_comma", plainComma, 1},
		{"q18_explicit_join_control", q18Explicit, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := planFor(t, tc.sql)
			total, condless := countPlanJoins(opt)
			if total < tc.minJoins {
				t.Fatalf("optimized plan has %d joins, want >= %d (comma-FROM tables dropped — issue #281):\n%s",
					total, tc.minJoins, opt.PrettyPrint(0))
			}
			if condless > 0 {
				t.Fatalf("optimized plan retains %d condition-less inner joins (WHERE equalities not lifted — residual cross product):\n%s",
					condless, opt.PrettyPrint(0))
			}
		})
	}
}
