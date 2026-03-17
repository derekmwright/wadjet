package logical

import (
	"testing"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

func TestBuildFromSelectHaving(t *testing.T) {
	// Parse: SELECT status, COUNT(*) as cnt FROM events GROUP BY status HAVING COUNT(*) > 5
	pq, err := plansql.Parse("SELECT status, COUNT(*) as cnt FROM events GROUP BY status HAVING COUNT(*) > 5")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	// The plan tree should contain: Limit/Sort/... → Filter(HAVING) → Aggregate → ...
	// Verify by pretty printing
	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// Walk plan to find HAVING filter above aggregate
	found := findNodeType(plan, NodeFilter)
	if found == nil {
		t.Fatal("expected a Filter node for HAVING in the plan")
	}

	// The HAVING filter's child should lead to an Aggregate
	if len(found.Children) == 0 {
		t.Fatal("HAVING filter has no children")
	}
	child := found.Children[0]
	if child.Type != NodeAggregate {
		t.Fatalf("expected HAVING filter child to be Aggregate, got %s", child.Type)
	}
}

func TestBuildFromSelectDistinct(t *testing.T) {
	pq, err := plansql.Parse("SELECT DISTINCT status FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	found := findNodeType(plan, NodeDistinct)
	if found == nil {
		t.Fatal("expected a Distinct node in the plan")
	}
}

func TestBuildFromSelectDistinctWithOrderBy(t *testing.T) {
	pq, err := plansql.Parse("SELECT DISTINCT status, count FROM events ORDER BY count DESC LIMIT 10")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// Verify order: Limit → Sort → Distinct → Project → Scan
	if plan.Type != NodeLimit {
		t.Fatalf("expected top node to be Limit, got %s", plan.Type)
	}
	sortNode := plan.Children[0]
	if sortNode.Type != NodeSort {
		t.Fatalf("expected Sort below Limit, got %s", sortNode.Type)
	}
	distinctNode := sortNode.Children[0]
	if distinctNode.Type != NodeDistinct {
		t.Fatalf("expected Distinct below Sort, got %s", distinctNode.Type)
	}
}

func TestBuildFromSelectWindowFunction(t *testing.T) {
	pq, err := plansql.Parse("SELECT user_id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM employees")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// Should have a Window node in the plan
	winNode := findNodeType(plan, NodeWindow)
	if winNode == nil {
		t.Fatal("expected a Window node in the plan")
	}
	if len(winNode.WindowExprs) != 1 {
		t.Fatalf("expected 1 window expr, got %d", len(winNode.WindowExprs))
	}

	we := winNode.WindowExprs[0]
	if we.Func != "row_number" {
		t.Errorf("window func: got %q, want 'row_number'", we.Func)
	}
	if we.OutputCol != "rn" {
		t.Errorf("output col: got %q, want 'rn'", we.OutputCol)
	}
	if len(we.PartitionBy) != 1 || we.PartitionBy[0] != "dept" {
		t.Errorf("partition by: got %v, want [dept]", we.PartitionBy)
	}
	if len(we.OrderBy) != 1 || we.OrderBy[0].Column != "salary" || !we.OrderBy[0].Desc {
		t.Errorf("order by: got %v", we.OrderBy)
	}
}

func TestBuildFromSelectWindowWithAgg(t *testing.T) {
	// Window function with no GROUP BY and a running sum
	pq, err := plansql.Parse("SELECT name, SUM(amount) OVER (ORDER BY ts) as running FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	winNode := findNodeType(plan, NodeWindow)
	if winNode == nil {
		t.Fatal("expected a Window node")
	}
	if winNode.WindowExprs[0].Func != "sum" {
		t.Errorf("expected sum, got %q", winNode.WindowExprs[0].Func)
	}
}

func TestBuildFromSelectUnion(t *testing.T) {
	pq, err := plansql.Parse("SELECT id, name FROM events UNION SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	unionNode := findNodeType(plan, NodeUnion)
	if unionNode == nil {
		t.Fatal("expected a Union node in the plan")
	}
	if unionNode.UnionAll {
		t.Error("expected UNION (not ALL)")
	}
	if len(unionNode.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(unionNode.Children))
	}
}

func TestBuildFromSelectUnionAll(t *testing.T) {
	pq, err := plansql.Parse("SELECT id FROM events UNION ALL SELECT id FROM users")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	unionNode := findNodeType(plan, NodeUnion)
	if unionNode == nil {
		t.Fatal("expected a Union node in the plan")
	}
	if !unionNode.UnionAll {
		t.Error("expected UNION ALL")
	}
}

func TestBuildFromSelectUnionWithOrderByLimit(t *testing.T) {
	pq, err := plansql.Parse("SELECT id FROM events UNION SELECT id FROM users ORDER BY id DESC LIMIT 5")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// Top node should be Limit
	if plan.Type != NodeLimit {
		t.Fatalf("expected top node to be Limit, got %s", plan.Type)
	}
	if plan.LimitVal != 5 {
		t.Errorf("expected LIMIT 5, got %d", plan.LimitVal)
	}

	// Below Limit should be Sort
	sortNode := plan.Children[0]
	if sortNode.Type != NodeSort {
		t.Fatalf("expected Sort below Limit, got %s", sortNode.Type)
	}

	// Below Sort should be Union
	unionNode := sortNode.Children[0]
	if unionNode.Type != NodeUnion {
		t.Fatalf("expected Union below Sort, got %s", unionNode.Type)
	}
}

func TestBuildFromSelectIntersect(t *testing.T) {
	pq, err := plansql.Parse("SELECT id FROM events INTERSECT SELECT id FROM users")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	node := findNodeType(plan, NodeIntersect)
	if node == nil {
		t.Fatal("expected an Intersect node in the plan")
	}
	if node.UnionAll {
		t.Error("expected INTERSECT (not ALL)")
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
}

func TestBuildFromSelectExcept(t *testing.T) {
	pq, err := plansql.Parse("SELECT id FROM events EXCEPT ALL SELECT id FROM users")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	node := findNodeType(plan, NodeExcept)
	if node == nil {
		t.Fatal("expected an Except node in the plan")
	}
	if !node.UnionAll {
		t.Error("expected EXCEPT ALL")
	}
}

func TestBuildFromSelectIntersectWithOrderByLimit(t *testing.T) {
	pq, err := plansql.Parse("SELECT id FROM events EXCEPT SELECT id FROM users ORDER BY id LIMIT 3")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	if plan.Type != NodeLimit {
		t.Fatalf("expected top node to be Limit, got %s", plan.Type)
	}
	if plan.LimitVal != 3 {
		t.Errorf("expected LIMIT 3, got %d", plan.LimitVal)
	}

	sortNode := plan.Children[0]
	if sortNode.Type != NodeSort {
		t.Fatalf("expected Sort below Limit, got %s", sortNode.Type)
	}

	exceptNode := sortNode.Children[0]
	if exceptNode.Type != NodeExcept {
		t.Fatalf("expected Except below Sort, got %s", exceptNode.Type)
	}
}

func TestBuildFromSelectGroupingSets(t *testing.T) {
	pq, err := plansql.Parse("SELECT region, product, SUM(sales) as total FROM t GROUP BY GROUPING SETS ((region, product), (region), ())")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// With 3 grouping sets, the plan should contain UNION ALL nodes
	// combining 3 Aggregate nodes.
	// Structure: Project → Union → (Union, Agg2)
	//                        ↓
	//                   (Agg0, Agg1)

	// Count all aggregate nodes in the plan
	aggNodes := findAllNodeType(plan, NodeAggregate)
	if len(aggNodes) != 3 {
		t.Fatalf("expected 3 Aggregate nodes (one per grouping set), got %d", len(aggNodes))
	}

	// Count union nodes
	unionNodes := findAllNodeType(plan, NodeUnion)
	if len(unionNodes) != 2 {
		t.Fatalf("expected 2 Union nodes, got %d", len(unionNodes))
	}

	// Verify each union is UNION ALL
	for i, u := range unionNodes {
		if !u.UnionAll {
			t.Errorf("union node %d: expected UNION ALL", i)
		}
	}

	// First aggregate: GROUP BY (region, product) — no null cols
	if len(aggNodes[0].GroupingSetNulls) != 0 {
		t.Errorf("set 0: expected 0 null cols, got %v", aggNodes[0].GroupingSetNulls)
	}
	// Second aggregate: GROUP BY (region) — product is null
	if len(aggNodes[1].GroupingSetNulls) != 1 || aggNodes[1].GroupingSetNulls[0] != "product" {
		t.Errorf("set 1: expected null cols [product], got %v", aggNodes[1].GroupingSetNulls)
	}
	// Third aggregate: GROUP BY () — both null
	if len(aggNodes[2].GroupingSetNulls) != 2 {
		t.Errorf("set 2: expected 2 null cols, got %v", aggNodes[2].GroupingSetNulls)
	}
}

func TestBuildFromSelectCube(t *testing.T) {
	pq, err := plansql.Parse("SELECT a, b, COUNT(*) as cnt FROM t GROUP BY CUBE(a, b)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// CUBE(a,b) = 4 grouping sets → 4 aggregate nodes, 3 union nodes
	aggNodes := findAllNodeType(plan, NodeAggregate)
	if len(aggNodes) != 4 {
		t.Fatalf("expected 4 Aggregate nodes for CUBE(a,b), got %d", len(aggNodes))
	}

	unionNodes := findAllNodeType(plan, NodeUnion)
	if len(unionNodes) != 3 {
		t.Fatalf("expected 3 Union nodes, got %d", len(unionNodes))
	}
}

func TestBuildFromSelectRollup(t *testing.T) {
	pq, err := plansql.Parse("SELECT year, month, SUM(sales) as total FROM t GROUP BY ROLLUP(year, month)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}

	pp := plan.PrettyPrint(0)
	t.Logf("Plan:\n%s", pp)

	// ROLLUP(year, month) = 3 sets: (year,month), (year), ()
	aggNodes := findAllNodeType(plan, NodeAggregate)
	if len(aggNodes) != 3 {
		t.Fatalf("expected 3 Aggregate nodes for ROLLUP(year,month), got %d", len(aggNodes))
	}

	unionNodes := findAllNodeType(plan, NodeUnion)
	if len(unionNodes) != 2 {
		t.Fatalf("expected 2 Union nodes, got %d", len(unionNodes))
	}
}

func TestResolveOrderByColumn(t *testing.T) {
	tests := []struct {
		name       string
		col        string
		selectCols []plansql.SelectColumn
		want       string
	}{
		{
			name: "direct alias match",
			col:  "rx_total",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "rx_total",
		},
		{
			name: "expression match returns alias",
			col:  "sum(rx_bytes)",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "rx_total",
		},
		{
			name: "aggregate function match",
			col:  "sum(rx_bytes)",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "rx_total",
		},
		{
			name: "count star match",
			col:  "count(*)",
			selectCols: []plansql.SelectColumn{
				{Expr: "count(*)", Alias: "cnt", IsAgg: true, AggFunc: "count", AggArg: "*"},
			},
			want: "cnt",
		},
		{
			name: "count star with empty arg",
			col:  "count(*)",
			selectCols: []plansql.SelectColumn{
				{Expr: "count(*)", Alias: "total", IsAgg: true, AggFunc: "count", AggArg: ""},
			},
			want: "total",
		},
		{
			name: "no match returns original",
			col:  "unknown_col",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "unknown_col",
		},
		{
			name: "case insensitive alias match",
			col:  "RX_TOTAL",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "rx_total",
		},
		{
			name: "case insensitive expression match",
			col:  "SUM(rx_bytes)",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "rx_total",
		},
		{
			name: "expression match without alias returns expr",
			col:  "sum(rx_bytes)",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "sum(rx_bytes)",
		},
		{
			name: "non-agg column direct alias",
			col:  "device_name",
			selectCols: []plansql.SelectColumn{
				{Expr: "d.name", Alias: "device_name"},
				{Expr: "sum(rx_bytes)", Alias: "rx_total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "device_name",
		},
		{
			name: "aggregate match case insensitive agg func",
			col:  "SUM(RX_BYTES)",
			selectCols: []plansql.SelectColumn{
				{Expr: "sum(rx_bytes)", Alias: "total", IsAgg: true, AggFunc: "sum", AggArg: "rx_bytes"},
			},
			want: "total",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveOrderByColumn(tt.col, tt.selectCols)
			if got != tt.want {
				t.Errorf("resolveOrderByColumn(%q) = %q, want %q", tt.col, got, tt.want)
			}
		})
	}
}

// findAllNodeType collects all nodes of the given type via depth-first search.
func findAllNodeType(node *Node, typ NodeType) []*Node {
	var result []*Node
	if node.Type == typ {
		result = append(result, node)
	}
	for _, child := range node.Children {
		result = append(result, findAllNodeType(child, typ)...)
	}
	return result
}

// findNodeType does a depth-first search for the first node of the given type.
func findNodeType(node *Node, typ NodeType) *Node {
	if node.Type == typ {
		return node
	}
	for _, child := range node.Children {
		if found := findNodeType(child, typ); found != nil {
			return found
		}
	}
	return nil
}
