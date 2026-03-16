package logical

import (
	"testing"

	plansql "github.com/derekmwright/caelum/internal/planner/sql"
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
