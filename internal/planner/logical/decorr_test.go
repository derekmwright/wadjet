package logical

import (
	"fmt"
	"testing"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

func TestQ17Decorrelation(t *testing.T) {
	q17 := `SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
		FROM lineitem
		JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23'
			AND p_container = 'MED BOX'
			AND l_quantity < (
				SELECT 0.2 * AVG(l_quantity)
				FROM lineitem
				WHERE l_partkey = p_partkey
			)`

	parsed, err := plansql.Parse(q17)
	if err != nil {
		t.Fatal("Parse error:", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal("Extract error:", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatal("Build error:", err)
	}

	// Simulate AnnotateScanColumns — in production the physical planner
	// populates ScanColumns from the catalog before calling Optimize.
	annotateScanColumnsForTest(plan)

	t.Log("=== Before optimize ===")
	printPlanT(t, plan, 0)

	optimized := Optimize(plan)
	t.Log("\n=== After optimize ===")
	printPlanT(t, optimized, 0)

	found := findNodeMatch(optimized, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "left"
	})
	if found {
		t.Log("SUCCESS: Scalar subquery was decorrelated into LEFT JOIN")
	} else {
		t.Error("FAIL: No LEFT JOIN found - decorrelation did not fire")
	}
}

func TestQ21Decorrelation(t *testing.T) {
	q21 := `SELECT s_name, COUNT(*) AS numwait
		FROM supplier
		JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
		JOIN orders ON o_orderkey = l1.l_orderkey
		JOIN nation ON s_nationkey = n_nationkey
		WHERE o_orderstatus = 'F'
			AND n_name = 'SAUDI ARABIA'
			AND l1.l_receiptdate > l1.l_commitdate
			AND EXISTS (
				SELECT 1
				FROM lineitem l2
				WHERE l2.l_orderkey = l1.l_orderkey
					AND l2.l_suppkey <> l1.l_suppkey
			)
			AND NOT EXISTS (
				SELECT 1
				FROM lineitem l3
				WHERE l3.l_orderkey = l1.l_orderkey
					AND l3.l_suppkey <> l1.l_suppkey
					AND l3.l_receiptdate > l3.l_commitdate
			)
		GROUP BY s_name
		ORDER BY numwait DESC, s_name
		LIMIT 100`

	parsed, err := plansql.Parse(q21)
	if err != nil {
		t.Fatal("Parse error:", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal("Extract error:", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatal("Build error:", err)
	}

	annotateScanColumnsForTest(plan)

	t.Log("=== Before optimize ===")
	printPlanT(t, plan, 0)

	optimized := Optimize(plan)
	t.Log("\n=== After optimize ===")
	printPlanT(t, optimized, 0)

	hasSemi := findNodeMatch(optimized, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "semi"
	})
	hasAnti := findNodeMatch(optimized, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "anti"
	})
	t.Logf("Semi join found: %v, Anti join found: %v", hasSemi, hasAnti)
	if !hasSemi {
		t.Error("Expected EXISTS to decorrelate into semi join")
	}
	if !hasAnti {
		t.Error("Expected NOT EXISTS to decorrelate into anti join")
	}
}

func printPlanT(t *testing.T, n *Node, depth int) {
	if n == nil {
		return
	}
	indent := ""
	for i := 0; i < depth; i++ {
		indent += "  "
	}
	msg := fmt.Sprintf("%s%s", indent, n.Type)
	switch n.Type {
	case NodeScan:
		msg += fmt.Sprintf(" table=%s alias=%s", n.TableName, n.TableAlias)
	case NodeJoin:
		msg += fmt.Sprintf(" type=%s cond=%q filter=%q", n.JoinType, n.JoinCond, n.JoinFilter)
	case NodeFilter:
		for _, p := range n.Predicates {
			msg += fmt.Sprintf(" pred=%q", p.Raw)
		}
	case NodeAggregate:
		msg += fmt.Sprintf(" groupBy=%v aggs=%v", n.GroupBy, n.AggExprs)
	}
	t.Log(msg)
	for _, child := range n.Children {
		printPlanT(t, child, depth+1)
	}
}

func findNodeMatch(n *Node, match func(*Node) bool) bool {
	if n == nil {
		return false
	}
	if match(n) {
		return true
	}
	for _, c := range n.Children {
		if findNodeMatch(c, match) {
			return true
		}
	}
	return false
}

func TestQ20Decorrelation(t *testing.T) {
	// Q20 has a nested correlated subquery: IN (SELECT ... WHERE EXISTS (SELECT ... WHERE ...))
	q20 := `SELECT s_name, s_address
		FROM supplier
		JOIN nation ON s_nationkey = n_nationkey
		WHERE n_name = 'CANADA'
			AND s_suppkey IN (
				SELECT ps_suppkey
				FROM partsupp
				WHERE ps_partkey IN (
					SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
				)
				AND ps_availqty > (
					SELECT 0.5 * SUM(l_quantity)
					FROM lineitem
					WHERE l_partkey = ps_partkey
						AND l_suppkey = ps_suppkey
						AND l_shipdate >= '1994-01-01'
						AND l_shipdate < '1995-01-01'
				)
			)
		ORDER BY s_name`

	parsed, err := plansql.Parse(q20)
	if err != nil {
		t.Fatal("Parse error:", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal("Extract error:", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatal("Build error:", err)
	}

	annotateScanColumnsForTest(plan)

	t.Log("=== Before optimize ===")
	printPlanT(t, plan, 0)

	// Pass annotator to re-annotate scan nodes created by IN decorrelation,
	// matching what the physical planner does.
	optimized := Optimize(plan, annotateScanColumnsForTest)
	t.Log("\n=== After optimize ===")
	printPlanT(t, optimized, 0)

	// The IN subquery should become a semi join
	hasSemi := findNodeMatch(optimized, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "semi"
	})
	if !hasSemi {
		t.Error("Expected IN subquery to decorrelate into semi join")
	}

	// The nested scalar subquery should also decorrelate into a LEFT JOIN
	hasLeft := findNodeMatch(optimized, func(n *Node) bool {
		return n.Type == NodeJoin && n.JoinType == "left"
	})
	if hasLeft {
		t.Log("SUCCESS: Nested scalar subquery was also decorrelated into LEFT JOIN")
	} else {
		t.Log("NOTE: Nested scalar subquery was NOT decorrelated (may still run per-row)")
	}
}


// annotateScanColumnsForTest populates ScanColumns on Scan nodes with
// TPC-H table schemas. This simulates what the physical planner does
// via AnnotateScanColumns before calling Optimize.
func annotateScanColumnsForTest(n *Node) {
	if n == nil {
		return
	}
	if n.Type == NodeScan && len(n.ScanColumns) == 0 {
		switch n.TableName {
		case "lineitem":
			n.ScanColumns = []string{
				"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
				"l_quantity", "l_extendedprice", "l_discount", "l_tax",
				"l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
				"l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
			}
		case "part":
			n.ScanColumns = []string{
				"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type",
				"p_size", "p_container", "p_retailprice", "p_comment",
			}
		case "orders":
			n.ScanColumns = []string{
				"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
				"o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment",
			}
		case "customer":
			n.ScanColumns = []string{
				"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone",
				"c_acctbal", "c_mktsegment", "c_comment",
			}
		case "supplier":
			n.ScanColumns = []string{
				"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone",
				"s_acctbal", "s_comment",
			}
		case "nation":
			n.ScanColumns = []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}
		case "region":
			n.ScanColumns = []string{"r_regionkey", "r_name", "r_comment"}
		case "partsupp":
			n.ScanColumns = []string{
				"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment",
			}
		}
	}
	for _, c := range n.Children {
		annotateScanColumnsForTest(c)
	}
}
