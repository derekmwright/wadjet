package logical

import (
	"testing"
)

// Q18-shaped fixture: semi(o_orderkey IN sub) applied above
// (customer ⋈ orders) ⋈ lineitem. The semijoin must sink all the way to
// the orders scan so the customer and lineitem joins run on filtered
// rows.
func semiPushdownFixture() (*Node, *Node, *Node, *Node, *Node) {
	customer := &Node{Type: NodeScan, TableName: "customer", ScanColumns: []string{"c_custkey", "c_name"}}
	orders := &Node{Type: NodeScan, TableName: "orders", ScanColumns: []string{"o_orderkey", "o_custkey", "o_totalprice"}}
	lineitem := &Node{Type: NodeScan, TableName: "lineitem", ScanColumns: []string{"l_orderkey", "l_quantity"}}
	j1 := &Node{Type: NodeJoin, JoinType: "inner", JoinCond: "c_custkey = o_custkey", Children: []*Node{customer, orders}}
	j2 := &Node{Type: NodeJoin, JoinType: "inner", JoinCond: "o_orderkey = l_orderkey", Children: []*Node{j1, lineitem}}
	sub := &Node{Type: NodeAggregate, Children: []*Node{
		{Type: NodeScan, TableName: "lineitem", ScanColumns: []string{"l_orderkey", "l_quantity"}},
	}}
	semi := &Node{Type: NodeJoin, JoinType: "semi", JoinCond: "o_orderkey = l_orderkey", Children: []*Node{j2, sub}}
	return semi, j1, j2, orders, sub
}

func TestSemiPushdown_Q18ShapeSinksToOrders(t *testing.T) {
	semi, j1, j2, orders, sub := semiPushdownFixture()
	got := pushSemiAntiBelowInnerJoins(semi)
	if got != j2 {
		t.Fatalf("root = %v, want the outer inner join", got.Type)
	}
	if j2.Children[0] != j1 {
		t.Fatalf("outer join left child changed: %+v", j2.Children[0])
	}
	pushed := j1.Children[1]
	if pushed == nil || pushed.Type != NodeJoin || pushed.JoinType != "semi" {
		t.Fatalf("orders slot = %+v, want the semi join sunk onto orders", pushed)
	}
	if pushed.Children[0] != orders || pushed.Children[1] != sub {
		t.Fatalf("semi children = %+v, want (orders, sub)", pushed.Children)
	}
}

func TestSemiPushdown_DescendsThroughFilter(t *testing.T) {
	semi, j1, j2, orders, _ := semiPushdownFixture()
	filter := &Node{Type: NodeFilter, Children: []*Node{j2}}
	semi.Children[0] = filter
	got := pushSemiAntiBelowInnerJoins(semi)
	if got != filter {
		t.Fatalf("root = %+v, want the filter kept on top", got)
	}
	if j1.Children[1] == nil || j1.Children[1].JoinType != "semi" {
		t.Fatalf("semi did not sink below the filter to orders: %+v", j1.Children[1])
	}
	if j1.Children[1].Children[0] != orders {
		t.Fatalf("semi probe = %+v, want orders", j1.Children[1].Children[0])
	}
}

func TestSemiPushdown_AntiJoinSinks(t *testing.T) {
	semi, j1, _, orders, _ := semiPushdownFixture()
	semi.JoinType = "anti"
	pushSemiAntiBelowInnerJoins(semi)
	if j1.Children[1] == nil || j1.Children[1].JoinType != "anti" ||
		j1.Children[1].Children[0] != orders {
		t.Fatalf("anti join did not sink to orders: %+v", j1.Children[1])
	}
}

func TestSemiPushdown_Ineligible(t *testing.T) {
	cases := map[string]func(semi, j1, j2 *Node){
		"join filter present": func(semi, _, _ *Node) {
			semi.JoinFilter = "l_suppkey <> s_suppkey"
		},
		"ambiguous key on both sides": func(semi, _, j2 *Node) {
			// lineitem also exposes o_orderkey → at the top join neither
			// side owns the key exclusively; nothing may move.
			j2.Children[1].ScanColumns = append(j2.Children[1].ScanColumns, "o_orderkey")
			semi.JoinCond = "o_orderkey = l_orderkey"
		},
		"key resolves nowhere": func(semi, _, _ *Node) {
			semi.JoinCond = "nonexistent_col = l_orderkey"
		},
		"non-equality conjunct": func(semi, _, _ *Node) {
			semi.JoinCond = "o_orderkey = l_orderkey AND o_totalprice > 100"
		},
		"outer join below": func(semi, _, j2 *Node) {
			j2.JoinType = "left"
		},
	}
	for name, mutate := range cases {
		semi, j1, j2, _, _ := semiPushdownFixture()
		mutate(semi, j1, j2)
		got := pushSemiAntiBelowInnerJoins(semi)
		if got != semi {
			t.Errorf("%s: semi was pushed, want unchanged root", name)
		}
	}
}

func TestSemiPushdown_KillSwitch(t *testing.T) {
	semiPushdownEnabled = false
	defer func() { semiPushdownEnabled = true }()
	semi, _, _, _, _ := semiPushdownFixture()
	if got := pushSemiAntiBelowInnerJoins(semi); got != semi {
		t.Fatal("kill switch must disable the rewrite")
	}
}
