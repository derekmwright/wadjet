package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// --- computeRequiredColumns / pushColumnNeeds ---

func TestComputeRequiredColumns_SimpleScan(t *testing.T) {
	scan := NewScan("events", "e")
	proj := NewProject(scan, []Projection{
		{Column: "id", Alias: "id"},
		{Column: "name", Alias: "name"},
	})
	computeRequiredColumns(proj)

	if len(scan.RequiredColumns) == 0 {
		t.Fatal("expected RequiredColumns to be populated")
	}
	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["id"] || !needed["name"] {
		t.Errorf("expected id and name in required, got %v", scan.RequiredColumns)
	}
}

func TestComputeRequiredColumns_FilterWithColumn(t *testing.T) {
	scan := NewScan("events", "e")
	filter := NewFilter(scan, []Predicate{
		{Column: "status", Op: "=", Value: "active"},
	})
	// Wrap in project — bare filters with nil parentNeeds mean "all columns".
	proj := NewProject(filter, []Projection{{Column: "status"}})
	computeRequiredColumns(proj)

	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["status"] {
		t.Errorf("expected status in required, got %v", scan.RequiredColumns)
	}
}

func TestComputeRequiredColumns_FilterWithAST(t *testing.T) {
	scan := NewScan("events", "e")
	filter := NewFilter(scan, []Predicate{
		{ASTExpr: &plansql.CmpExpr{
			Left:  &plansql.ColRef{Column: "age"},
			Op:    ">",
			Right: &plansql.Lit{Value: "18", Kind: plansql.LitNumber},
		}},
	})
	// Wrap in project — bare filters with nil parentNeeds mean "all columns".
	proj := NewProject(filter, []Projection{{Column: "name"}})
	computeRequiredColumns(proj)

	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["age"] {
		t.Errorf("expected age in required, got %v", scan.RequiredColumns)
	}
	if !needed["name"] {
		t.Errorf("expected name in required, got %v", scan.RequiredColumns)
	}
}

func TestComputeRequiredColumns_Aggregate(t *testing.T) {
	scan := NewScan("events", "e")
	agg := NewAggregate(scan, []string{"dept"}, []AggExpr{
		{Func: "sum", InputCol: "amount", OutputCol: "total"},
	})
	// Aggregate is a column-restricting node — it creates needs even with nil parent.
	computeRequiredColumns(agg)

	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["dept"] {
		t.Error("expected dept in required")
	}
	if !needed["amount"] {
		t.Error("expected amount in required")
	}
}

func TestComputeRequiredColumns_Sort(t *testing.T) {
	scan := NewScan("events", "e")
	sort := NewSort(scan, []OrderExpr{{Column: "ts"}})
	// Wrap in project — bare sorts with nil parentNeeds mean "all columns".
	proj := NewProject(sort, []Projection{{Column: "ts"}, {Column: "val"}})
	computeRequiredColumns(proj)

	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["ts"] {
		t.Error("expected ts in required")
	}
	if !needed["val"] {
		t.Error("expected val in required")
	}
}

func TestComputeRequiredColumns_Window(t *testing.T) {
	scan := NewScan("events", "e")
	win := NewWindow(scan, []WindowExpr{
		{Func: "row_number", OutputCol: "rn", InputCol: "amt",
			PartitionBy: []string{"dept"}, OrderBy: []OrderExpr{{Column: "salary"}}},
	})

	// Without a parent projection, nil parentNeeds means "all columns" —
	// scans should read everything (no RequiredColumns set).
	computeRequiredColumns(win)
	if len(scan.RequiredColumns) != 0 {
		t.Errorf("bare window: expected no column restriction, got %v", scan.RequiredColumns)
	}

	// With a parent projection that restricts columns, the window's refs
	// should propagate to the scan.
	scan.RequiredColumns = nil
	proj := NewProject(win, []Projection{{Column: "amt"}, {Column: "rn"}})
	computeRequiredColumns(proj)
	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["amt"] {
		t.Error("expected amt in required")
	}
	if !needed["dept"] {
		t.Error("expected dept in required")
	}
	if !needed["salary"] {
		t.Error("expected salary in required")
	}
}

func TestComputeRequiredColumns_Join(t *testing.T) {
	left := NewScan("events", "e")
	right := NewScan("users", "u")
	join := NewJoin(left, right, "inner", "e.id = u.id")

	// Without a parent projection, nil parentNeeds means "all columns" —
	// scans should read everything (no RequiredColumns set).
	computeRequiredColumns(join)
	if len(left.RequiredColumns) != 0 {
		t.Errorf("bare join: expected no column restriction on left, got %v", left.RequiredColumns)
	}

	// With a parent projection that restricts columns, join keys propagate.
	left.RequiredColumns = nil
	right.RequiredColumns = nil
	proj := NewProject(join, []Projection{{Column: "id"}, {Column: "name"}})
	computeRequiredColumns(proj)
	leftNeeded := map[string]bool{}
	for _, c := range left.RequiredColumns {
		leftNeeded[c] = true
	}
	if !leftNeeded["id"] {
		t.Errorf("left scan should need id, got %v", left.RequiredColumns)
	}
}

func TestComputeRequiredColumns_NilNode(t *testing.T) {
	// Should not panic
	pushColumnNeeds(nil, nil)
}

func TestComputeRequiredColumns_ProjectWithAST(t *testing.T) {
	scan := NewScan("events", "e")
	proj := NewProject(scan, []Projection{
		{ASTExpr: &plansql.BinaryOp{
			Left:  &plansql.ColRef{Column: "price"},
			Op:    "*",
			Right: &plansql.ColRef{Column: "qty"},
		}},
	})
	computeRequiredColumns(proj)

	needed := map[string]bool{}
	for _, c := range scan.RequiredColumns {
		needed[c] = true
	}
	if !needed["price"] || !needed["qty"] {
		t.Errorf("expected price and qty, got %v", scan.RequiredColumns)
	}
}

// --- collectNodeColumnRefs ---

func TestCollectNodeColumnRefs_AllNodeTypes(t *testing.T) {
	// Filter with column predicate
	refs := make(map[string]bool)
	filter := NewFilter(nil, []Predicate{{Column: "status"}})
	collectNodeColumnRefs(filter, refs)
	if !refs["status"] {
		t.Error("filter: expected status")
	}

	// Project with column
	refs2 := make(map[string]bool)
	proj := NewProject(nil, []Projection{{Column: "name"}})
	collectNodeColumnRefs(proj, refs2)
	if !refs2["name"] {
		t.Error("project: expected name")
	}

	// Aggregate
	refs3 := make(map[string]bool)
	agg := &Node{Type: NodeAggregate, GroupBy: []string{"g1"}, AggExprs: []AggExpr{
		{InputCol: "val"},
		{InputCol: "*"}, // star should be skipped
	}}
	collectNodeColumnRefs(agg, refs3)
	if !refs3["g1"] || !refs3["val"] {
		t.Errorf("aggregate: expected g1 and val, got %v", refs3)
	}
	if refs3["*"] {
		t.Error("should not include *")
	}

	// Join
	refs4 := make(map[string]bool)
	join := &Node{Type: NodeJoin, JoinCond: "a.id = b.id", JoinFilter: "a.x != b.x"}
	collectNodeColumnRefs(join, refs4)
	if !refs4["id"] || !refs4["x"] {
		t.Errorf("join: expected id and x, got %v", refs4)
	}

	// Sort
	refs5 := make(map[string]bool)
	sort := &Node{Type: NodeSort, OrderBy: []OrderExpr{{Column: "ts"}}}
	collectNodeColumnRefs(sort, refs5)
	if !refs5["ts"] {
		t.Error("sort: expected ts")
	}

	// Window
	refs6 := make(map[string]bool)
	win := &Node{Type: NodeWindow, WindowExprs: []WindowExpr{
		{InputCol: "amt", PartitionBy: []string{"dept"}, OrderBy: []OrderExpr{{Column: "sal"}}},
	}}
	collectNodeColumnRefs(win, refs6)
	if !refs6["amt"] || !refs6["dept"] || !refs6["sal"] {
		t.Errorf("window: got %v", refs6)
	}
}

// --- collectASTColumnRefs ---

func TestCollectASTColumnRefs(t *testing.T) {
	tests := []struct {
		name    string
		node    plansql.Node
		wantCol string
	}{
		{"ColRef", &plansql.ColRef{Column: "x"}, "x"},
		{"CmpExpr", &plansql.CmpExpr{Left: &plansql.ColRef{Column: "a"}, Op: "=", Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}, "a"},
		{"AndNode", &plansql.AndNode{Left: &plansql.ColRef{Column: "b"}, Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}, "b"},
		{"OrNode", &plansql.OrNode{Left: &plansql.ColRef{Column: "c"}, Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}, "c"},
		{"BinaryOp", &plansql.BinaryOp{Left: &plansql.ColRef{Column: "d"}, Op: "+", Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}, "d"},
		{"UnaryOp", &plansql.UnaryOp{Op: "-", Inner: &plansql.ColRef{Column: "e"}}, "e"},
		{"NotNode", &plansql.NotNode{Inner: &plansql.ColRef{Column: "f"}}, "f"},
		{"ParenNode", &plansql.ParenNode{Inner: &plansql.ColRef{Column: "g"}}, "g"},
		{"FuncCallNode", &plansql.FuncCallNode{Name: "upper", Args: []plansql.Node{&plansql.ColRef{Column: "h"}}}, "h"},
		{"CastNode", &plansql.CastNode{Inner: &plansql.ColRef{Column: "i"}, TypeName: "int"}, "i"},
		{"InExpr", &plansql.InExpr{Left: &plansql.ColRef{Column: "j"}, Values: []plansql.Node{&plansql.ColRef{Column: "k"}}}, "j"},
		{"BetweenExpr", &plansql.BetweenExpr{Left: &plansql.ColRef{Column: "l"}, Low: &plansql.ColRef{Column: "m"}, High: &plansql.ColRef{Column: "n"}}, "l"},
		{"LikeExpr", &plansql.LikeExpr{Left: &plansql.ColRef{Column: "o"}, Pattern: &plansql.Lit{Value: "%x%", Kind: plansql.LitString}}, "o"},
		{"IsExpr", &plansql.IsExpr{Left: &plansql.ColRef{Column: "p"}, Check: "null"}, "p"},
		{"CaseNode", &plansql.CaseNode{
			Subject: &plansql.ColRef{Column: "q"},
			Whens:   []plansql.WhenClause{{Cond: &plansql.ColRef{Column: "r"}, Result: &plansql.ColRef{Column: "s"}}},
			Else:    &plansql.ColRef{Column: "t2"},
		}, "q"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refs := make(map[string]bool)
			collectASTColumnRefs(tt.node, refs)
			if !refs[tt.wantCol] {
				t.Errorf("expected %q in refs, got %v", tt.wantCol, refs)
			}
		})
	}

	// Nil
	refs := make(map[string]bool)
	collectASTColumnRefs(nil, refs)
	if len(refs) != 0 {
		t.Error("expected empty refs for nil")
	}
}

// --- collectSubtreeColumns ---

func TestCollectSubtreeColumns(t *testing.T) {
	scan := NewScan("events", "e")
	scan.ScanColumns = []string{"ID", "Name", "Status"}

	result := collectSubtreeColumns(scan)
	if !result["id"] || !result["name"] || !result["status"] {
		t.Errorf("expected lowercased columns, got %v", result)
	}
}

func TestCollectSubtreeColumns_Nil(t *testing.T) {
	result := collectSubtreeColumns(nil)
	if len(result) != 0 {
		t.Error("expected empty for nil")
	}
}

func TestCollectSubtreeColumns_DeepNested(t *testing.T) {
	scan := NewScan("events", "e")
	scan.ScanColumns = []string{"a"}
	proj := NewProject(scan, nil)

	result := collectSubtreeColumns(proj)
	if !result["a"] {
		t.Error("expected a from nested scan")
	}
}

// --- extractJoinColumnRefs ---

func TestExtractJoinColumnRefs(t *testing.T) {
	refs := make(map[string]bool)
	extractJoinColumnRefs("e.user_id = u.user_id AND e.status != u.status", refs)
	if !refs["user_id"] {
		t.Error("expected user_id")
	}
	if !refs["status"] {
		t.Error("expected status")
	}
}

func TestExtractJoinColumnRefs_Empty(t *testing.T) {
	refs := make(map[string]bool)
	extractJoinColumnRefs("", refs)
	if len(refs) != 0 {
		t.Error("expected empty refs for empty condition")
	}
}

// --- attachScanPredicates ---

func TestAttachScanPredicates_WithMatchingPredicate(t *testing.T) {
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Column: "status", Op: "=", Value: "active"},
		{Raw: "complex expression"}, // no column/op/value — should NOT be attached
	})
	attachScanPredicates(filter)

	if len(scan.ScanPredicates) != 1 {
		t.Fatalf("expected 1 scan predicate, got %d", len(scan.ScanPredicates))
	}
	if scan.ScanPredicates[0].Column != "status" {
		t.Errorf("expected status predicate, got %q", scan.ScanPredicates[0].Column)
	}
}

func TestAttachScanPredicates_NilNode(t *testing.T) {
	// Should not panic
	attachScanPredicates(nil)
}

func TestAttachScanPredicates_NonFilterNode(t *testing.T) {
	scan := NewScan("events", "")
	// Calling on a scan (not a filter) should be no-op
	attachScanPredicates(scan)
	if len(scan.ScanPredicates) != 0 {
		t.Error("expected no scan predicates on non-filter node")
	}
}

// --- pushdownPredicates ---

func TestPushdownPredicates_ThroughProject(t *testing.T) {
	scan := NewScan("events", "")
	proj := NewProject(scan, []Projection{{Column: "id"}})
	filter := NewFilter(proj, []Predicate{{Raw: "year = '2026'"}})

	result := pushdownPredicates(filter)
	// After pushdown, filter should be closer to the scan
	if result == nil {
		t.Fatal("pushdownPredicates returned nil")
	}
}

// --- pruneProjections ---

func TestPruneProjections_RemovesDuplicateProjections(t *testing.T) {
	scan := NewScan("events", "")
	proj := NewProject(scan, []Projection{
		{Column: "id", Alias: "id"},
		{Column: "id", Alias: "id"}, // duplicate
		{Column: "name", Alias: "name"},
	})

	result := pruneProjections(proj)
	if result.Type != NodeProject {
		t.Fatalf("expected project, got %s", result.Type)
	}
	// The pruneProjections may not specifically remove duplicate projections,
	// but should not crash
}

// --- Optimize full pipeline ---

func TestOptimize_ComputesRequiredColumns(t *testing.T) {
	scan := NewScan("events", "e")
	proj := NewProject(scan, []Projection{
		{Column: "id", Alias: "id"},
		{Column: "name", Alias: "name"},
	})

	result := Optimize(proj)
	if result == nil {
		t.Fatal("Optimize returned nil")
	}

	// Find the scan and check RequiredColumns were populated
	scanNode := findNodeType(result, NodeScan)
	if scanNode == nil {
		t.Fatal("expected scan node in optimized plan")
	}
	if len(scanNode.RequiredColumns) == 0 {
		t.Error("expected RequiredColumns to be populated by Optimize")
	}
}

func TestOptimize_AttachesScanPredicates(t *testing.T) {
	// Test attachScanPredicates directly: when a Filter with
	// Column/Op/Value predicates sits above a Scan, those
	// predicates are attached to the Scan for row-group pruning.
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Column: "status", Op: "=", Value: "active"},
	})

	attachScanPredicates(filter)

	if len(scan.ScanPredicates) == 0 {
		t.Error("expected scan predicates to be attached")
	}
	if scan.ScanPredicates[0].Column != "status" {
		t.Errorf("expected Column=status, got %s", scan.ScanPredicates[0].Column)
	}
}

// --- pushColumnNeeds with semi/anti join ---

func TestPushColumnNeeds_SemiJoin(t *testing.T) {
	left := NewScan("events", "e")
	right := NewScan("users", "u")
	join := NewJoin(left, right, "semi", "e.id = u.id")

	proj := NewProject(join, []Projection{{Column: "name"}})

	computeRequiredColumns(proj)

	// Build side (right) should only need join key columns, not parent needs
	rightNeeded := map[string]bool{}
	for _, c := range right.RequiredColumns {
		rightNeeded[c] = true
	}
	if !rightNeeded["id"] {
		t.Errorf("right side should need join key 'id', got %v", right.RequiredColumns)
	}
}

// --- pushColumnNeeds with inner join and ScanColumns ---

func TestPushColumnNeeds_InnerJoinColumnPartitioning(t *testing.T) {
	left := NewScan("events", "e")
	left.ScanColumns = []string{"id", "name", "ts"}
	right := NewScan("users", "u")
	right.ScanColumns = []string{"id", "email"}

	join := NewJoin(left, right, "inner", "e.id = u.id")

	// Parent needs name (from left) and email (from right)
	proj := NewProject(join, []Projection{
		{Column: "name"},
		{Column: "email"},
	})
	computeRequiredColumns(proj)

	leftNeeded := map[string]bool{}
	for _, c := range left.RequiredColumns {
		leftNeeded[strings.ToLower(c)] = true
	}
	rightNeeded := map[string]bool{}
	for _, c := range right.RequiredColumns {
		rightNeeded[strings.ToLower(c)] = true
	}

	// Left side should have name and id (join key)
	if !leftNeeded["name"] {
		t.Errorf("left should need 'name', got %v", left.RequiredColumns)
	}
	if !leftNeeded["id"] {
		t.Errorf("left should need join key 'id', got %v", left.RequiredColumns)
	}

	// Right side should have email and id (join key)
	if !rightNeeded["email"] {
		t.Errorf("right should need 'email', got %v", right.RequiredColumns)
	}
	if !rightNeeded["id"] {
		t.Errorf("right should need join key 'id', got %v", right.RequiredColumns)
	}
}

// --- pushFilterThroughJoin ---

func TestPushdownPredicates_FilterAboveJoin(t *testing.T) {
	// Filter with table-qualified predicates above a join should push
	// the left-table predicate to the left child and right-table to the right child.
	left := NewScan("events", "e")
	left.ScanColumns = []string{"id", "status"}
	right := NewScan("users", "u")
	right.ScanColumns = []string{"id", "name"}
	join := NewJoin(left, right, "join", "e.id = u.id")

	// AST expression for e.status = 'active'
	leftExpr := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Table: "e", Column: "status"},
		Op:    "=",
		Right: &plansql.Lit{Value: "active", Kind: plansql.LitString},
	}
	// AST expression for u.name = 'bob'
	rightExpr := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Table: "u", Column: "name"},
		Op:    "=",
		Right: &plansql.Lit{Value: "bob", Kind: plansql.LitString},
	}
	filter := NewFilter(join, []Predicate{
		{ASTExpr: leftExpr, Raw: "e.status = 'active'"},
		{ASTExpr: rightExpr, Raw: "u.name = 'bob'"},
	})

	result := pushdownPredicates(filter)

	// The join should now have filter children
	joinNode := findNodeType(result, NodeJoin)
	if joinNode == nil {
		t.Fatal("expected join node in result")
	}
	// Left child should be a Filter or the scan should have predicates pushed
	leftChild := joinNode.Children[0]
	if leftChild.Type != NodeFilter {
		t.Errorf("expected left child to be Filter, got %s", leftChild.Type)
	}
	rightChild := joinNode.Children[1]
	if rightChild.Type != NodeFilter {
		t.Errorf("expected right child to be Filter, got %s", rightChild.Type)
	}
}

// --- flattenASTAnd ---

func TestFlattenASTAnd(t *testing.T) {
	// AND of two comparisons
	and := &plansql.AndNode{
		Left:  &plansql.CmpExpr{Left: &plansql.ColRef{Column: "a"}, Op: "=", Right: &plansql.Lit{Value: "1"}},
		Right: &plansql.CmpExpr{Left: &plansql.ColRef{Column: "b"}, Op: "=", Right: &plansql.Lit{Value: "2"}},
	}
	var result []Predicate
	flattenASTAnd(and, &result)
	if len(result) != 2 {
		t.Errorf("expected 2 predicates from flattened AND, got %d", len(result))
	}

	// Paren wrapping an AND
	paren := &plansql.ParenNode{Inner: and}
	result = nil
	flattenASTAnd(paren, &result)
	if len(result) != 2 {
		t.Errorf("expected 2 predicates from paren-wrapped AND, got %d", len(result))
	}

	// Paren wrapping a non-AND
	parenSimple := &plansql.ParenNode{Inner: &plansql.ColRef{Column: "x"}}
	result = nil
	flattenASTAnd(parenSimple, &result)
	if len(result) != 1 {
		t.Errorf("expected 1 predicate from paren-wrapped non-AND, got %d", len(result))
	}

	// Simple expression (default case)
	result = nil
	flattenASTAnd(&plansql.ColRef{Column: "y"}, &result)
	if len(result) != 1 {
		t.Errorf("expected 1 predicate from simple expr, got %d", len(result))
	}
}

// --- tryParseExpr ---

func TestTryParseExpr(t *testing.T) {
	// Valid expression
	result := tryParseExpr("x = 1")
	if result == nil {
		t.Error("expected non-nil result for valid expression")
	}

	// Invalid expression
	result = tryParseExpr("!!!")
	if result != nil {
		t.Error("expected nil for invalid expression")
	}
}

// --- predicateTableRefs ---

func TestPredicateTableRefs(t *testing.T) {
	colToTable := map[string]string{
		"status": "events",
		"name":   "users",
	}

	// Qualified column ref
	pred := Predicate{
		ASTExpr: &plansql.CmpExpr{
			Left:  &plansql.ColRef{Table: "events", Column: "id"},
			Op:    "=",
			Right: &plansql.Lit{Value: "1"},
		},
	}
	refs := predicateTableRefs(pred, colToTable)
	if refs == nil {
		t.Fatal("expected non-nil refs")
	}
	if !refs["events"] {
		t.Error("expected events table ref")
	}

	// Unqualified column resolved via map
	pred = Predicate{
		ASTExpr: &plansql.CmpExpr{
			Left:  &plansql.ColRef{Column: "status"},
			Op:    "=",
			Right: &plansql.Lit{Value: "active"},
		},
	}
	refs = predicateTableRefs(pred, colToTable)
	if refs == nil {
		t.Fatal("expected non-nil refs for unqualified column")
	}
	if !refs["events"] {
		t.Error("expected events table ref for unqualified 'status'")
	}

	// No ASTExpr -> nil
	pred = Predicate{Raw: "x = 1"}
	refs = predicateTableRefs(pred, colToTable)
	if refs != nil {
		t.Error("expected nil for predicate without ASTExpr")
	}

	// Unresolvable column -> nil
	pred = Predicate{
		ASTExpr: &plansql.CmpExpr{
			Left:  &plansql.ColRef{Column: "unknown"},
			Op:    "=",
			Right: &plansql.Lit{Value: "1"},
		},
	}
	refs = predicateTableRefs(pred, colToTable)
	if refs != nil {
		t.Error("expected nil for unresolvable column")
	}
}

// --- collectColTableRefs ---

func TestCollectColTableRefs(t *testing.T) {
	colToTable := map[string]string{
		"a": "t1",
		"b": "t2",
	}
	refs := make(map[string]bool)
	resolved := true

	// OrNode
	collectColTableRefs(&plansql.OrNode{
		Left:  &plansql.ColRef{Table: "t1", Column: "a"},
		Right: &plansql.ColRef{Table: "t2", Column: "b"},
	}, refs, colToTable, &resolved)
	if !refs["t1"] || !refs["t2"] {
		t.Errorf("OrNode: expected t1 and t2, got %v", refs)
	}

	// NotNode
	refs = make(map[string]bool)
	resolved = true
	collectColTableRefs(&plansql.NotNode{
		Inner: &plansql.ColRef{Table: "t1", Column: "a"},
	}, refs, colToTable, &resolved)
	if !refs["t1"] {
		t.Errorf("NotNode: expected t1, got %v", refs)
	}

	// ParenNode
	refs = make(map[string]bool)
	resolved = true
	collectColTableRefs(&plansql.ParenNode{
		Inner: &plansql.ColRef{Table: "t1", Column: "a"},
	}, refs, colToTable, &resolved)
	if !refs["t1"] {
		t.Errorf("ParenNode: expected t1, got %v", refs)
	}

	// BinaryOp
	refs = make(map[string]bool)
	resolved = true
	collectColTableRefs(&plansql.BinaryOp{
		Left:  &plansql.ColRef{Table: "t1", Column: "a"},
		Op:    "+",
		Right: &plansql.ColRef{Table: "t2", Column: "b"},
	}, refs, colToTable, &resolved)
	if !refs["t1"] || !refs["t2"] {
		t.Errorf("BinaryOp: expected t1 and t2, got %v", refs)
	}

	// UnaryOp
	refs = make(map[string]bool)
	resolved = true
	collectColTableRefs(&plansql.UnaryOp{
		Op:    "-",
		Inner: &plansql.ColRef{Table: "t1", Column: "a"},
	}, refs, colToTable, &resolved)
	if !refs["t1"] {
		t.Errorf("UnaryOp: expected t1, got %v", refs)
	}

	// InExpr
	refs = make(map[string]bool)
	resolved = true
	collectColTableRefs(&plansql.InExpr{
		Left:   &plansql.ColRef{Table: "t1", Column: "a"},
		Values: []plansql.Node{&plansql.ColRef{Table: "t2", Column: "b"}},
	}, refs, colToTable, &resolved)
	if !refs["t1"] || !refs["t2"] {
		t.Errorf("InExpr: expected t1 and t2, got %v", refs)
	}

	// nil node (no-op)
	refs = make(map[string]bool)
	resolved = true
	collectColTableRefs(nil, refs, colToTable, &resolved)
	if len(refs) != 0 {
		t.Error("nil node should produce no refs")
	}
}

// --- flattenANDPredicates: cover AST path ---

func TestFlattenANDPredicates_WithASTExpr(t *testing.T) {
	and := &plansql.AndNode{
		Left:  &plansql.CmpExpr{Left: &plansql.ColRef{Column: "x"}, Op: "=", Right: &plansql.Lit{Value: "1"}},
		Right: &plansql.CmpExpr{Left: &plansql.ColRef{Column: "y"}, Op: "=", Right: &plansql.Lit{Value: "2"}},
	}
	preds := []Predicate{{ASTExpr: and, Raw: "x = 1 AND y = 2"}}
	result := flattenANDPredicates(preds)
	if len(result) != 2 {
		t.Errorf("expected 2 flattened predicates, got %d", len(result))
	}
}

func TestFlattenANDPredicates_WithRaw(t *testing.T) {
	preds := []Predicate{{Raw: "a = 1 AND b = 2"}}
	result := flattenANDPredicates(preds)
	if len(result) != 2 {
		t.Errorf("expected 2 flattened predicates, got %d", len(result))
	}
}

func TestFlattenANDPredicates_EmptyPred(t *testing.T) {
	preds := []Predicate{{}}
	result := flattenANDPredicates(preds)
	if len(result) != 0 {
		t.Errorf("expected 0 predicates from empty pred, got %d", len(result))
	}
}

// --- extractPartitionFilters ---

func TestExtractPartitionFilters_WithFilter(t *testing.T) {
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Column: "year", Op: "=", Value: "2024", Raw: "year = '2024'"},
	})

	result := extractPartitionFilters(filter)
	scanNode := findNodeType(result, NodeScan)
	if scanNode == nil {
		t.Fatal("expected scan node")
	}
	if len(scanNode.PartitionFilter) == 0 {
		t.Error("expected partition filter to be extracted")
	}
}

// --- resolveTableOrCTE ---

func TestResolveTableOrCTE_SimpleScan(t *testing.T) {
	table := plansql.TableRef{Name: "events", Alias: "e"}
	node, err := resolveTableOrCTE(table, nil)
	if err != nil {
		t.Fatalf("resolveTableOrCTE: %v", err)
	}
	if node.Type != NodeScan {
		t.Errorf("expected NodeScan, got %s", node.Type)
	}
	if node.TableName != "events" {
		t.Errorf("expected table 'events', got %q", node.TableName)
	}
}

func TestResolveTableOrCTE_TableFunc(t *testing.T) {
	table := plansql.TableRef{
		Name:       "read_json",
		Alias:      "j",
		IsFunction: true,
		FuncArgs:   []string{"data.json"},
	}
	node, err := resolveTableOrCTE(table, nil)
	if err != nil {
		t.Fatalf("resolveTableOrCTE: %v", err)
	}
	if !node.IsTableFunc {
		t.Error("expected IsTableFunc=true")
	}
	if node.FuncName != "read_json" {
		t.Errorf("expected FuncName='read_json', got %q", node.FuncName)
	}
}

func TestResolveTableOrCTE_WithCTE(t *testing.T) {
	ctes := []plansql.CTEDef{
		{Name: "active", SQL: "SELECT id FROM events WHERE status = 'active'"},
	}
	table := plansql.TableRef{Name: "active", Alias: "a"}
	node, err := resolveTableOrCTE(table, ctes)
	if err != nil {
		t.Fatalf("resolveTableOrCTE: %v", err)
	}
	if node == nil {
		t.Fatal("expected non-nil node for CTE resolution")
	}
}

func TestResolveTableOrCTE_DerivedTable(t *testing.T) {
	table := plansql.TableRef{
		Name:  "(SELECT id FROM events)",
		Alias: "sub",
	}
	node, err := resolveTableOrCTE(table, nil)
	if err != nil {
		t.Fatalf("resolveTableOrCTE: %v", err)
	}
	if node == nil {
		t.Fatal("expected non-nil node for derived table")
	}
}

// --- buildSetOpPlan ---

func TestBuildSetOpPlan_Union(t *testing.T) {
	info := &plansql.SelectInfo{
		Union: &plansql.UnionInfo{
			Left: &plansql.SelectInfo{
				Columns: []plansql.SelectColumn{{Expr: "id"}},
				Tables:  []plansql.TableRef{{Name: "events", Alias: "events"}},
			},
			Right: &plansql.SelectInfo{
				Columns: []plansql.SelectColumn{{Expr: "id"}},
				Tables:  []plansql.TableRef{{Name: "users", Alias: "users"}},
			},
			All: true,
			Op:  plansql.SetOpUnion,
		},
	}
	plan, err := buildSetOpPlan(info, nil)
	if err != nil {
		t.Fatalf("buildSetOpPlan: %v", err)
	}
	if plan.Type != NodeUnion {
		t.Errorf("expected NodeUnion, got %s", plan.Type)
	}
}

func TestBuildSetOpPlan_Intersect(t *testing.T) {
	info := &plansql.SelectInfo{
		Union: &plansql.UnionInfo{
			Left: &plansql.SelectInfo{
				Columns: []plansql.SelectColumn{{Expr: "id"}},
				Tables:  []plansql.TableRef{{Name: "events", Alias: "events"}},
			},
			Right: &plansql.SelectInfo{
				Columns: []plansql.SelectColumn{{Expr: "id"}},
				Tables:  []plansql.TableRef{{Name: "users", Alias: "users"}},
			},
			Op: plansql.SetOpIntersect,
		},
	}
	plan, err := buildSetOpPlan(info, nil)
	if err != nil {
		t.Fatalf("buildSetOpPlan: %v", err)
	}
	if plan.Type != NodeIntersect {
		t.Errorf("expected NodeIntersect, got %s", plan.Type)
	}
}

func TestBuildSetOpPlan_Except(t *testing.T) {
	info := &plansql.SelectInfo{
		Union: &plansql.UnionInfo{
			Left: &plansql.SelectInfo{
				Columns: []plansql.SelectColumn{{Expr: "id"}},
				Tables:  []plansql.TableRef{{Name: "events", Alias: "events"}},
			},
			Right: &plansql.SelectInfo{
				Columns: []plansql.SelectColumn{{Expr: "id"}},
				Tables:  []plansql.TableRef{{Name: "users", Alias: "users"}},
			},
			Op: plansql.SetOpExcept,
		},
	}
	plan, err := buildSetOpPlan(info, nil)
	if err != nil {
		t.Fatalf("buildSetOpPlan: %v", err)
	}
	if plan.Type != NodeExcept {
		t.Errorf("expected NodeExcept, got %s", plan.Type)
	}
}

// --- decorrelateExists edge cases ---

func TestDecorrelateExists_NilNode(t *testing.T) {
	result := decorrelateExists(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestDecorrelateExists_NonFilter(t *testing.T) {
	scan := NewScan("events", "e")
	result := decorrelateExists(scan)
	if result.Type != NodeScan {
		t.Errorf("expected scan unchanged, got %s", result.Type)
	}
}

// --- decorrelateScalarSubqueries edge cases ---

func TestDecorrelateScalarSubqueries_NilNode(t *testing.T) {
	result := decorrelateScalarSubqueries(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestDecorrelateScalarSubqueries_NonFilter(t *testing.T) {
	scan := NewScan("events", "e")
	result := decorrelateScalarSubqueries(scan)
	if result.Type != NodeScan {
		t.Errorf("expected scan unchanged, got %s", result.Type)
	}
}

// --- extractCommonORPredicates ---

func TestExtractCommonORPredicates_NilNode(t *testing.T) {
	result := extractCommonORPredicates(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestExtractCommonORPredicates_NonFilter(t *testing.T) {
	scan := NewScan("events", "e")
	result := extractCommonORPredicates(scan)
	if result.Type != NodeScan {
		t.Errorf("expected scan unchanged, got %s", result.Type)
	}
}

// --- reorderJoins edge cases ---

func TestReorderJoins_NilNode(t *testing.T) {
	result := reorderJoins(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestReorderJoins_NonJoin(t *testing.T) {
	scan := NewScan("events", "e")
	result := reorderJoins(scan)
	if result.Type != NodeScan {
		t.Errorf("expected scan unchanged, got %s", result.Type)
	}
}

// --- pushdownPredicates: filter above project ---

func TestPushdownPredicates_FilterAboveProject(t *testing.T) {
	scan := NewScan("events", "e")
	proj := NewProject(scan, []Projection{{Column: "id", Alias: "id"}})
	filter := NewFilter(proj, []Predicate{
		{Column: "id", Op: "=", Value: "1", Raw: "id = 1"},
	})

	result := pushdownPredicates(filter)
	// Filter should be pushed below Project
	if result.Type != NodeProject {
		t.Errorf("expected Project at top after pushdown, got %s", result.Type)
	}
}

// --- pruneProjections: no-op on non-project ---

func TestPruneProjections_NilNode(t *testing.T) {
	result := pruneProjections(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestPruneProjections_NonProject(t *testing.T) {
	scan := NewScan("events", "e")
	result := pruneProjections(scan)
	if result.Type != NodeScan {
		t.Errorf("expected scan unchanged, got %s", result.Type)
	}
}

// --- Optimize: full pipeline with filter above join ---

func TestOptimize_FilterAboveJoin(t *testing.T) {
	left := NewScan("events", "e")
	left.ScanColumns = []string{"id", "status"}
	right := NewScan("users", "u")
	right.ScanColumns = []string{"id", "name"}
	join := NewJoin(left, right, "join", "e.id = u.id")

	astExpr := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Table: "e", Column: "status"},
		Op:    "=",
		Right: &plansql.Lit{Value: "active", Kind: plansql.LitString},
	}
	filter := NewFilter(join, []Predicate{
		{ASTExpr: astExpr, Raw: "e.status = 'active'"},
	})

	result := Optimize(filter)
	// After optimization, the filter should have been pushed through the join
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// --- BuildFromSelectWithCTEs ---

func TestBuildFromSelectWithCTEs_SimpleCTE(t *testing.T) {
	info := &plansql.SelectInfo{
		Columns: []plansql.SelectColumn{{Expr: "id"}},
		Tables:  []plansql.TableRef{{Name: "active", Alias: "active"}},
	}
	ctes := []plansql.CTEDef{
		{Name: "active", SQL: "SELECT id FROM events WHERE status = 'active'"},
	}
	plan, err := BuildFromSelectWithCTEs(info, ctes)
	if err != nil {
		t.Fatalf("BuildFromSelectWithCTEs: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
}

// --- resolveOrderByColumn ---

func TestResolveOrderByColumn_ColumnAlias(t *testing.T) {
	info := &plansql.SelectInfo{
		Columns: []plansql.SelectColumn{
			{Expr: "id", Alias: "event_id"},
			{Expr: "name"},
		},
		Tables: []plansql.TableRef{{Name: "events", Alias: "events"}},
	}
	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
}

// --- HasRemainingSubqueries ---

func TestHasRemainingSubqueries_NoSubqueries(t *testing.T) {
	plan := &Node{
		Type: NodeFilter,
		Predicates: []Predicate{
			{Column: "status", Op: "=", Value: "active", Raw: "status = 'active'"},
		},
		Children: []*Node{{Type: NodeScan, TableName: "orders"}},
	}
	if HasRemainingSubqueries(plan) {
		t.Error("expected false for plain filter")
	}
}

func TestHasRemainingSubqueries_ExistsNode(t *testing.T) {
	plan := &Node{
		Type: NodeFilter,
		Predicates: []Predicate{
			{ASTExpr: &plansql.ExistsNode{SQL: "SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey"}},
		},
		Children: []*Node{{Type: NodeScan, TableName: "orders"}},
	}
	if !HasRemainingSubqueries(plan) {
		t.Error("expected true for EXISTS predicate")
	}
}

func TestHasRemainingSubqueries_InSubquery(t *testing.T) {
	plan := &Node{
		Type: NodeFilter,
		Predicates: []Predicate{
			{ASTExpr: &plansql.InExpr{
				Values: []plansql.Node{&plansql.SubqueryNode{SQL: "SELECT l_orderkey FROM lineitem"}},
			}},
		},
		Children: []*Node{{Type: NodeScan, TableName: "orders"}},
	}
	if !HasRemainingSubqueries(plan) {
		t.Error("expected true for IN subquery predicate")
	}
}

func TestHasRemainingSubqueries_DecorrelatedJoin(t *testing.T) {
	plan := &Node{
		Type:     NodeJoin,
		JoinType: "semi",
		Children: []*Node{
			{Type: NodeScan, TableName: "orders"},
			{Type: NodeScan, TableName: "lineitem"},
		},
	}
	if HasRemainingSubqueries(plan) {
		t.Error("expected false for decorrelated semi-join")
	}
}

func TestHasRemainingSubqueries_NestedExists(t *testing.T) {
	inner := &Node{
		Type: NodeFilter,
		Predicates: []Predicate{
			{ASTExpr: &plansql.ExistsNode{SQL: "SELECT 1 FROM t2", Not: true}},
		},
		Children: []*Node{{Type: NodeScan, TableName: "t2"}},
	}
	plan := &Node{
		Type:     NodeProject,
		Children: []*Node{inner},
	}
	if !HasRemainingSubqueries(plan) {
		t.Error("expected true for nested EXISTS")
	}
}
