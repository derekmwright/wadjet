package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// --- NodeType.String() ---

func TestNodeTypeString(t *testing.T) {
	tests := []struct {
		nt   NodeType
		want string
	}{
		{NodeScan, "Scan"},
		{NodeFilter, "Filter"},
		{NodeProject, "Project"},
		{NodeAggregate, "Aggregate"},
		{NodeSort, "Sort"},
		{NodeLimit, "Limit"},
		{NodeJoin, "Join"},
		{NodeDistinct, "Distinct"},
		{NodeWindow, "Window"},
		{NodeUnion, "Union"},
		{NodeIntersect, "Intersect"},
		{NodeExcept, "Except"},
		{NodeType(999), "Unknown(999)"},
	}
	for _, tt := range tests {
		got := tt.nt.String()
		if got != tt.want {
			t.Errorf("NodeType(%d).String() = %q, want %q", tt.nt, got, tt.want)
		}
	}
}

// --- InjectRowFilter ---

func TestInjectRowFilter_MatchingTable(t *testing.T) {
	scan := NewScan("events", "e")
	result := InjectRowFilter(scan, "events", "year = '2026'")

	if result.Type != NodeFilter {
		t.Fatalf("expected Filter node wrapping scan, got %s", result.Type)
	}
	if len(result.Predicates) != 1 {
		t.Fatalf("expected 1 predicate, got %d", len(result.Predicates))
	}
	if result.Predicates[0].Raw != "year = '2026'" {
		t.Errorf("expected raw predicate, got %q", result.Predicates[0].Raw)
	}
	if result.Children[0].Type != NodeScan {
		t.Errorf("expected child to be Scan, got %s", result.Children[0].Type)
	}
}

func TestInjectRowFilter_MatchingAlias(t *testing.T) {
	scan := NewScan("events", "e")
	result := InjectRowFilter(scan, "e", "year = '2026'")

	if result.Type != NodeFilter {
		t.Fatalf("expected Filter node wrapping scan, got %s", result.Type)
	}
}

func TestInjectRowFilter_NonMatchingTable(t *testing.T) {
	scan := NewScan("events", "e")
	result := InjectRowFilter(scan, "users", "active = true")

	// Should not wrap with filter
	if result.Type != NodeScan {
		t.Fatalf("expected unchanged scan, got %s", result.Type)
	}
}

func TestInjectRowFilter_NilNode(t *testing.T) {
	result := injectRowFilter(nil, "events", "x = 1", nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestInjectRowFilter_MalformedSQL(t *testing.T) {
	scan := NewScan("events", "e")
	// Malformed filter SQL should return plan unchanged (not error)
	result := InjectRowFilter(scan, "events", "!!!BAD SQL!!!")
	if result.Type != NodeScan {
		t.Fatalf("expected original scan unchanged for malformed filter SQL, got %s", result.Type)
	}
}

func TestInjectRowFilter_DeepNested(t *testing.T) {
	scan := NewScan("events", "e")
	proj := NewProject(scan, []Projection{{Column: "id"}})
	sort := NewSort(proj, []OrderExpr{{Column: "id"}})

	result := InjectRowFilter(sort, "events", "year = '2026'")

	// The scan inside the tree should now be wrapped
	filterNode := findNodeType(result, NodeFilter)
	if filterNode == nil {
		t.Fatal("expected a filter node injected around scan")
	}
	if filterNode.Children[0].Type != NodeScan {
		t.Errorf("filter should wrap scan, got %s", filterNode.Children[0].Type)
	}
}

// --- InjectColumnPolicies ---

func TestInjectColumnPolicies_EmptyPolicies(t *testing.T) {
	scan := NewScan("events", "e")
	result := InjectColumnPolicies(scan, "events", nil)
	// No change
	if result.Type != NodeScan {
		t.Fatalf("expected unchanged scan for empty policies, got %s", result.Type)
	}
}

func TestInjectColumnPolicies_NilNode(t *testing.T) {
	result := injectColumnPolicies(nil, "events", []ColumnPolicy{{Column: "x", Denied: true}})
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestInjectColumnPolicies_NonMatchingTable(t *testing.T) {
	scan := NewScan("events", "e")
	scan.ScanColumns = []string{"id", "name", "secret"}
	policies := []ColumnPolicy{{Column: "secret", Denied: true}}
	result := InjectColumnPolicies(scan, "users", policies)
	if result.Type != NodeScan {
		t.Fatalf("expected unchanged scan for non-matching table, got %s", result.Type)
	}
}

func TestInjectColumnPolicies_DenyColumn(t *testing.T) {
	scan := NewScan("events", "e")
	scan.ScanColumns = []string{"id", "name", "secret"}
	policies := []ColumnPolicy{{Column: "secret", Denied: true}}
	result := InjectColumnPolicies(scan, "events", policies)

	if result.Type != NodeProject {
		t.Fatalf("expected project wrapping scan, got %s", result.Type)
	}
	// Should have 2 projections (id, name), not secret
	if len(result.Projections) != 2 {
		t.Fatalf("expected 2 projections, got %d", len(result.Projections))
	}
	for _, p := range result.Projections {
		if p.Column == "secret" || p.Alias == "secret" {
			t.Error("secret column should be denied/removed")
		}
	}
}

func TestInjectColumnPolicies_MaskColumn(t *testing.T) {
	scan := NewScan("events", "e")
	scan.ScanColumns = []string{"id", "name", "email"}
	policies := []ColumnPolicy{{Column: "email", MaskExpr: "'***'"}}
	result := InjectColumnPolicies(scan, "events", policies)

	if result.Type != NodeProject {
		t.Fatalf("expected project wrapping scan, got %s", result.Type)
	}
	// Should have 3 projections, with email masked
	if len(result.Projections) != 3 {
		t.Fatalf("expected 3 projections, got %d", len(result.Projections))
	}
	for _, p := range result.Projections {
		if p.Alias == "email" {
			if p.Expr != "'***'" {
				t.Errorf("expected masked email expression, got %q", p.Expr)
			}
		}
	}
}

func TestInjectColumnPolicies_ByAlias(t *testing.T) {
	scan := NewScan("events", "e")
	scan.ScanColumns = []string{"id", "secret"}
	policies := []ColumnPolicy{{Column: "secret", Denied: true}}
	result := InjectColumnPolicies(scan, "e", policies)

	if result.Type != NodeProject {
		t.Fatalf("expected project wrapping scan matched by alias, got %s", result.Type)
	}
}

func TestInjectColumnPolicies_NoColumnsAvailable(t *testing.T) {
	scan := NewScan("events", "e")
	// No ScanColumns or RequiredColumns set
	policies := []ColumnPolicy{{Column: "secret", Denied: true}}
	result := InjectColumnPolicies(scan, "events", policies)

	// Should return unchanged when no column info available
	if result.Type != NodeScan {
		t.Fatalf("expected unchanged scan when no columns available, got %s", result.Type)
	}
}

func TestInjectColumnPolicies_UsesRequiredColumns(t *testing.T) {
	scan := NewScan("events", "e")
	scan.RequiredColumns = []string{"id", "secret"}
	policies := []ColumnPolicy{{Column: "secret", Denied: true}}
	result := InjectColumnPolicies(scan, "events", policies)

	if result.Type != NodeProject {
		t.Fatalf("expected project from RequiredColumns, got %s", result.Type)
	}
	if len(result.Projections) != 1 || result.Projections[0].Column != "id" {
		t.Errorf("expected only id projection, got %v", result.Projections)
	}
}

// --- PrettyPrint ---

func TestPrettyPrint_AllNodeTypes(t *testing.T) {
	// Scan
	scan := NewScan("events", "e")
	pp := scan.PrettyPrint(0)
	if !strings.Contains(pp, "Scan: events") {
		t.Errorf("scan PrettyPrint missing table: %q", pp)
	}
	if !strings.Contains(pp, "AS e") {
		t.Errorf("scan PrettyPrint missing alias: %q", pp)
	}

	// Scan with same alias as table name
	scan2 := NewScan("events", "events")
	pp2 := scan2.PrettyPrint(0)
	if strings.Contains(pp2, "AS events") {
		t.Errorf("scan PrettyPrint should not show alias when same as table name: %q", pp2)
	}

	// Filter with raw predicates
	filter := NewFilter(scan, []Predicate{{Raw: "year = '2026'"}})
	ppf := filter.PrettyPrint(0)
	if !strings.Contains(ppf, "Filter") {
		t.Errorf("filter PrettyPrint missing label: %q", ppf)
	}

	// Filter with column/op/value predicates
	filter2 := NewFilter(scan, []Predicate{{Column: "x", Op: ">", Value: 5}})
	ppf2 := filter2.PrettyPrint(0)
	if !strings.Contains(ppf2, "Filter") {
		t.Errorf("filter PrettyPrint missing label: %q", ppf2)
	}

	// Project
	proj := NewProject(scan, []Projection{
		{Column: "id", Alias: "event_id"},
		{Expr: "count(*)", Alias: ""},
	})
	ppp := proj.PrettyPrint(0)
	if !strings.Contains(ppp, "Project") {
		t.Errorf("project PrettyPrint missing label: %q", ppp)
	}

	// Aggregate
	agg := NewAggregate(scan, []string{"user_id"}, []AggExpr{
		{Func: "count", InputCol: "id", OutputCol: "cnt"},
		{Func: "count", InputCol: "id", OutputCol: "cnt_d", Distinct: true},
	})
	ppa := agg.PrettyPrint(0)
	if !strings.Contains(ppa, "Aggregate") {
		t.Errorf("aggregate PrettyPrint missing label: %q", ppa)
	}
	if !strings.Contains(ppa, "DISTINCT") {
		t.Errorf("aggregate PrettyPrint missing DISTINCT: %q", ppa)
	}

	// Sort
	sort := NewSort(scan, []OrderExpr{{Column: "ts", Desc: true}, {Column: "id"}})
	pps := sort.PrettyPrint(0)
	if !strings.Contains(pps, "Sort") {
		t.Errorf("sort PrettyPrint missing label: %q", pps)
	}
	if !strings.Contains(pps, "DESC") {
		t.Errorf("sort PrettyPrint missing DESC: %q", pps)
	}

	// Limit
	limit := NewLimit(scan, 10, 5)
	ppl := limit.PrettyPrint(0)
	if !strings.Contains(ppl, "Limit: 10") {
		t.Errorf("limit PrettyPrint missing limit value: %q", ppl)
	}
	if !strings.Contains(ppl, "offset: 5") {
		t.Errorf("limit PrettyPrint missing offset: %q", ppl)
	}

	// Join
	right := NewScan("users", "u")
	join := NewJoin(scan, right, "inner", "e.id = u.id")
	ppj := join.PrettyPrint(0)
	if !strings.Contains(ppj, "Join: inner") {
		t.Errorf("join PrettyPrint missing type: %q", ppj)
	}

	// Distinct
	distinct := NewDistinct(scan)
	ppd := distinct.PrettyPrint(0)
	if !strings.Contains(ppd, "Distinct") {
		t.Errorf("distinct PrettyPrint missing label: %q", ppd)
	}

	// Window
	win := NewWindow(scan, []WindowExpr{{
		Func: "row_number", OutputCol: "rn",
		PartitionBy: []string{"dept"},
		OrderBy:     []OrderExpr{{Column: "salary", Desc: true}},
	}})
	ppw := win.PrettyPrint(0)
	if !strings.Contains(ppw, "Window") {
		t.Errorf("window PrettyPrint missing label: %q", ppw)
	}

	// Union
	union := NewUnion(scan, right, false)
	ppu := union.PrettyPrint(0)
	if !strings.Contains(ppu, "UNION") {
		t.Errorf("union PrettyPrint missing label: %q", ppu)
	}

	unionAll := NewUnion(scan, right, true)
	ppua := unionAll.PrettyPrint(0)
	if !strings.Contains(ppua, "UNION ALL") {
		t.Errorf("union all PrettyPrint missing label: %q", ppua)
	}

	// Intersect
	intersect := NewIntersect(scan, right, false)
	ppi := intersect.PrettyPrint(0)
	if !strings.Contains(ppi, "INTERSECT") {
		t.Errorf("intersect PrettyPrint missing label: %q", ppi)
	}

	intersectAll := NewIntersect(scan, right, true)
	ppia := intersectAll.PrettyPrint(0)
	if !strings.Contains(ppia, "INTERSECT ALL") {
		t.Errorf("intersect all PrettyPrint missing label: %q", ppia)
	}

	// Except
	except := NewExcept(scan, right, false)
	ppe := except.PrettyPrint(0)
	if !strings.Contains(ppe, "EXCEPT") {
		t.Errorf("except PrettyPrint missing label: %q", ppe)
	}

	exceptAll := NewExcept(scan, right, true)
	ppea := exceptAll.PrettyPrint(0)
	if !strings.Contains(ppea, "EXCEPT ALL") {
		t.Errorf("except all PrettyPrint missing label: %q", ppea)
	}
}

func TestPrettyPrint_Indentation(t *testing.T) {
	scan := NewScan("t", "")
	pp := scan.PrettyPrint(2)
	if !strings.HasPrefix(pp, "    ") { // 2 * 2 spaces
		t.Errorf("PrettyPrint(2) should have 4-space indent, got %q", pp)
	}
}

// --- constructors ---

func TestNewScan(t *testing.T) {
	n := NewScan("events", "e")
	if n.Type != NodeScan || n.TableName != "events" || n.TableAlias != "e" {
		t.Errorf("NewScan: %+v", n)
	}
}

func TestNewFilter(t *testing.T) {
	child := NewScan("t", "")
	n := NewFilter(child, []Predicate{{Raw: "x > 1"}})
	if n.Type != NodeFilter || len(n.Children) != 1 || len(n.Predicates) != 1 {
		t.Errorf("NewFilter: %+v", n)
	}
}

func TestNewProject(t *testing.T) {
	child := NewScan("t", "")
	n := NewProject(child, []Projection{{Column: "id"}})
	if n.Type != NodeProject || len(n.Projections) != 1 {
		t.Errorf("NewProject: %+v", n)
	}
}

func TestNewAggregate(t *testing.T) {
	child := NewScan("t", "")
	n := NewAggregate(child, []string{"g"}, []AggExpr{{Func: "count"}})
	if n.Type != NodeAggregate || len(n.GroupBy) != 1 || len(n.AggExprs) != 1 {
		t.Errorf("NewAggregate: %+v", n)
	}
}

func TestNewSort(t *testing.T) {
	child := NewScan("t", "")
	n := NewSort(child, []OrderExpr{{Column: "c"}})
	if n.Type != NodeSort || len(n.OrderBy) != 1 {
		t.Errorf("NewSort: %+v", n)
	}
}

func TestNewLimit(t *testing.T) {
	child := NewScan("t", "")
	n := NewLimit(child, 10, 5)
	if n.Type != NodeLimit || n.LimitVal != 10 || n.OffsetVal != 5 {
		t.Errorf("NewLimit: %+v", n)
	}
}

func TestNewDistinct(t *testing.T) {
	child := NewScan("t", "")
	n := NewDistinct(child)
	if n.Type != NodeDistinct || len(n.Children) != 1 {
		t.Errorf("NewDistinct: %+v", n)
	}
}

func TestNewWindow(t *testing.T) {
	child := NewScan("t", "")
	n := NewWindow(child, []WindowExpr{{Func: "row_number"}})
	if n.Type != NodeWindow || len(n.WindowExprs) != 1 {
		t.Errorf("NewWindow: %+v", n)
	}
}

func TestNewJoin(t *testing.T) {
	left := NewScan("a", "")
	right := NewScan("b", "")
	n := NewJoin(left, right, "inner", "a.id = b.id")
	if n.Type != NodeJoin || n.JoinType != "inner" || n.JoinCond != "a.id = b.id" {
		t.Errorf("NewJoin: %+v", n)
	}
	if len(n.Children) != 2 {
		t.Errorf("NewJoin: expected 2 children, got %d", len(n.Children))
	}
}

func TestNewUnion(t *testing.T) {
	n := NewUnion(NewScan("a", ""), NewScan("b", ""), true)
	if n.Type != NodeUnion || !n.UnionAll || len(n.Children) != 2 {
		t.Errorf("NewUnion: %+v", n)
	}
}

func TestNewIntersect(t *testing.T) {
	n := NewIntersect(NewScan("a", ""), NewScan("b", ""), false)
	if n.Type != NodeIntersect || n.UnionAll || len(n.Children) != 2 {
		t.Errorf("NewIntersect: %+v", n)
	}
}

func TestNewExcept(t *testing.T) {
	n := NewExcept(NewScan("a", ""), NewScan("b", ""), true)
	if n.Type != NodeExcept || !n.UnionAll || len(n.Children) != 2 {
		t.Errorf("NewExcept: %+v", n)
	}
}

// --- builder functions ---

func TestCleanExpr(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"  user_id  ", "user_id"},
		{"e.user_id", "e.user_id"},  // preserve table qualifiers for self-join disambiguation
		{"user_id", "user_id"},
		{"e.sub.col", "e.sub.col"},  // preserve all qualifiers
	}
	for _, tt := range tests {
		got := cleanExpr(tt.input)
		if got != tt.want {
			t.Errorf("cleanExpr(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsStarOnly(t *testing.T) {
	if !isStarOnly([]plansql.SelectColumn{{Star: true}}) {
		t.Error("expected true for single star column")
	}
	if isStarOnly([]plansql.SelectColumn{{Star: false}}) {
		t.Error("expected false for non-star column")
	}
	if isStarOnly([]plansql.SelectColumn{{Star: true}, {Star: true}}) {
		t.Error("expected false for multiple star columns")
	}
	if isStarOnly(nil) {
		t.Error("expected false for nil columns")
	}
}

func TestResolveOrderByPreProject(t *testing.T) {
	aggSynthetic := map[string]string{
		"sum(amount)": "__agg_0",
	}
	selectCols := []plansql.SelectColumn{
		{Expr: "sum(amount)", Alias: "total", IsAgg: true, AggFunc: "sum", AggArg: "amount"},
		{Expr: "name", Alias: "customer_name", ColumnRef: "name"},
	}

	// Matches aggregate synthetic name
	got := resolveOrderByPreProject("sum(amount)", selectCols, aggSynthetic)
	if got != "__agg_0" {
		t.Errorf("expected __agg_0, got %q", got)
	}

	// Direct alias → resolve to underlying column
	got = resolveOrderByPreProject("customer_name", selectCols, aggSynthetic)
	if got != "name" {
		t.Errorf("expected name, got %q", got)
	}

	// No match returns original
	got = resolveOrderByPreProject("unknown", selectCols, aggSynthetic)
	if got != "unknown" {
		t.Errorf("expected unknown, got %q", got)
	}
}

func TestConvertFrame(t *testing.T) {
	f := &plansql.WindowFrame{
		Mode: plansql.FrameRows,
		Start: plansql.FrameBound{
			Type: plansql.BoundUnboundedPreceding,
		},
		End: &plansql.FrameBound{
			Type:   plansql.BoundFollowing,
			Offset: &plansql.Lit{Kind: plansql.LitNumber, Value: "3"},
		},
	}
	spec := convertFrame(f)
	if spec.Mode != "rows" {
		t.Errorf("Mode = %q, want 'rows'", spec.Mode)
	}
	if spec.Start.Type != "unbounded_preceding" {
		t.Errorf("Start.Type = %q, want 'unbounded_preceding'", spec.Start.Type)
	}
	if spec.End.Type != "following" || spec.End.Offset != 3 {
		t.Errorf("End = %+v, want following/3", spec.End)
	}
}

func TestConvertFrame_Range(t *testing.T) {
	f := &plansql.WindowFrame{
		Mode: plansql.FrameRange,
		Start: plansql.FrameBound{
			Type:   plansql.BoundPreceding,
			Offset: &plansql.Lit{Kind: plansql.LitNumber, Value: "5"},
		},
		// nil End defaults to current_row
	}
	spec := convertFrame(f)
	if spec.Mode != "range" {
		t.Errorf("Mode = %q, want 'range'", spec.Mode)
	}
	if spec.Start.Type != "preceding" || spec.Start.Offset != 5 {
		t.Errorf("Start = %+v, want preceding/5", spec.Start)
	}
	if spec.End.Type != "current_row" {
		t.Errorf("End.Type = %q, want 'current_row'", spec.End.Type)
	}
}

func TestConvertBound_AllTypes(t *testing.T) {
	tests := []struct {
		input plansql.FrameBound
		want  string
	}{
		{plansql.FrameBound{Type: plansql.BoundUnboundedPreceding}, "unbounded_preceding"},
		{plansql.FrameBound{Type: plansql.BoundCurrentRow}, "current_row"},
		{plansql.FrameBound{Type: plansql.BoundUnboundedFollowing}, "unbounded_following"},
		{plansql.FrameBound{Type: plansql.BoundPreceding, Offset: &plansql.Lit{Kind: plansql.LitNumber, Value: "2"}}, "preceding"},
		{plansql.FrameBound{Type: plansql.BoundFollowing, Offset: &plansql.Lit{Kind: plansql.LitNumber, Value: "1"}}, "following"},
	}
	for _, tt := range tests {
		got := convertBound(tt.input)
		if got.Type != tt.want {
			t.Errorf("convertBound(%v) = %q, want %q", tt.input.Type, got.Type, tt.want)
		}
	}
}

func TestRewriteExpr_AllBranches(t *testing.T) {
	cols := []plansql.SelectColumn{
		{Expr: "count(*)", Alias: "cnt", IsAgg: true, AggFunc: "count", AggArg: "*",
			ASTExpr: &plansql.FuncCallNode{Name: "count", Star: true}},
	}

	// FuncCallNode (aggregate)
	fc := &plansql.FuncCallNode{Name: "count", Star: true}
	result := rewriteExpr(fc, cols)
	if cr, ok := result.(*plansql.ColRef); !ok || cr.Column != "cnt" {
		t.Errorf("rewriteExpr(FuncCall) = %v, expected ColRef{cnt}", result)
	}

	// AndNode
	andNode := &plansql.AndNode{Left: fc, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "1"}}
	result = rewriteExpr(andNode, cols)
	if _, ok := result.(*plansql.AndNode); !ok {
		t.Errorf("expected AndNode, got %T", result)
	}

	// OrNode
	orNode := &plansql.OrNode{Left: fc, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "1"}}
	result = rewriteExpr(orNode, cols)
	if _, ok := result.(*plansql.OrNode); !ok {
		t.Errorf("expected OrNode, got %T", result)
	}

	// CmpExpr
	cmpNode := &plansql.CmpExpr{Op: ">", Left: fc, Right: &plansql.Lit{Kind: plansql.LitNumber, Value: "5"}}
	result = rewriteExpr(cmpNode, cols)
	if _, ok := result.(*plansql.CmpExpr); !ok {
		t.Errorf("expected CmpExpr, got %T", result)
	}

	// ParenNode
	pn := &plansql.ParenNode{Inner: fc}
	result = rewriteExpr(pn, cols)
	if _, ok := result.(*plansql.ParenNode); !ok {
		t.Errorf("expected ParenNode, got %T", result)
	}

	// NotNode
	nn := &plansql.NotNode{Inner: fc}
	result = rewriteExpr(nn, cols)
	if _, ok := result.(*plansql.NotNode); !ok {
		t.Errorf("expected NotNode, got %T", result)
	}

	// Nil
	result = rewriteExpr(nil, cols)
	if result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}

	// Literal (passthrough)
	lit := &plansql.Lit{Kind: plansql.LitNumber, Value: "42"}
	result = rewriteExpr(lit, cols)
	if result != lit {
		t.Errorf("expected passthrough for literal, got %v", result)
	}
}

// --- setSubtreeAlias ---

func TestSetSubtreeAlias(t *testing.T) {
	scan := NewScan("events", "")
	proj := NewProject(scan, nil)

	setSubtreeAlias(proj, "derived")

	if scan.TableAlias != "derived" {
		t.Errorf("expected scan alias 'derived', got %q", scan.TableAlias)
	}
}

// --- countOutputCols / getOutputColNames ---

func TestCountOutputCols(t *testing.T) {
	info := &plansql.SelectInfo{
		Columns: []plansql.SelectColumn{{Expr: "a"}, {Expr: "b"}},
	}
	n := countOutputCols(nil, info)
	if n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
	n = countOutputCols(nil, nil)
	if n != 0 {
		t.Errorf("expected 0 for nil info, got %d", n)
	}
}

func TestGetOutputColNames(t *testing.T) {
	info := &plansql.SelectInfo{
		Columns: []plansql.SelectColumn{
			{Expr: "col1", Alias: "a"},
			{Expr: "col2", ColumnRef: "ref"},
			{Expr: "col3"},
		},
	}
	names := getOutputColNames(info)
	if names[0] != "a" {
		t.Errorf("expected alias 'a', got %q", names[0])
	}
	if names[1] != "ref" {
		t.Errorf("expected columnRef 'ref', got %q", names[1])
	}
	if names[2] != "col3" {
		t.Errorf("expected expr 'col3', got %q", names[2])
	}
}

// --- BuildFromSelect error paths ---

func TestBuildFromSelect_NoFromClause(t *testing.T) {
	// Table-less SELECT should produce a Dual node, not an error.
	info := &plansql.SelectInfo{
		Columns: []plansql.SelectColumn{{Expr: "1"}},
	}
	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("unexpected error for table-less SELECT: %v", err)
	}
	dual := findNodeType(plan, NodeDual)
	if dual == nil {
		t.Error("expected Dual node in plan for table-less SELECT")
	}
}

func TestBuildFromSelect_InvalidLimit(t *testing.T) {
	pq, err := plansql.Parse("SELECT id FROM events LIMIT abc")
	if err != nil {
		t.Skipf("parser rejected query: %v", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		t.Skipf("extract failed: %v", err)
	}
	_, err = BuildFromSelect(info)
	if err == nil {
		t.Error("expected error for invalid LIMIT")
	}
}

func TestBuildFromSelect_TableFunction(t *testing.T) {
	pq, err := plansql.Parse("SELECT * FROM read_json('http://example.com/data.json')")
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
	scanNode := findNodeType(plan, NodeScan)
	if scanNode == nil {
		t.Fatal("expected scan node")
	}
	if !scanNode.IsTableFunc {
		t.Error("expected IsTableFunc=true")
	}
}

// --- Optimize integration ---

func TestOptimize_SimpleFilter(t *testing.T) {
	scan := NewScan("events", "")
	filter := NewFilter(scan, []Predicate{
		{Column: "status", Op: "=", Value: "active", Raw: "status = 'active'"},
	})
	result := Optimize(filter)
	if result == nil {
		t.Fatal("Optimize returned nil")
	}
}

func TestOptimize_JoinReorder(t *testing.T) {
	a := NewScan("big", "a")
	a.ScanRowEstimate = 10000
	b := NewScan("small", "b")
	b.ScanRowEstimate = 10
	join := NewJoin(a, b, "inner", "a.id = b.id")
	result := Optimize(join)
	if result == nil {
		t.Fatal("Optimize returned nil")
	}
}

// --- ExtractMergeInfo ---

func TestExtractMergeInfoProjectRename(t *testing.T) {
	// Simulate: SELECT n_name AS nation, SUM(revenue) AS total_rev
	//           FROM ... GROUP BY n_name ORDER BY total_rev DESC LIMIT 10
	//
	// Logical plan: Limit → Sort → Project(n_name→nation, revenue→total_rev) → Aggregate
	agg := &Node{
		Type:    NodeAggregate,
		GroupBy: []string{"n_name"},
		AggExprs: []AggExpr{
			{Func: "sum", InputCol: "revenue", OutputCol: "revenue"},
		},
	}
	proj := &Node{
		Type: NodeProject,
		Projections: []Projection{
			{Column: "n_name", Alias: "nation"},
			{Expr: "revenue", Alias: "total_rev", IsAgg: true},
		},
		Children: []*Node{agg},
	}
	sort := &Node{
		Type:     NodeSort,
		OrderBy:  []OrderExpr{{Column: "total_rev", Desc: true}},
		Children: []*Node{proj},
	}
	limit := &Node{
		Type:     NodeLimit,
		LimitVal: 10,
		Children: []*Node{sort},
	}

	mi := ExtractMergeInfo(limit)
	if mi == nil {
		t.Fatal("ExtractMergeInfo returned nil")
	}
	if !mi.HasAggregate {
		t.Fatal("expected HasAggregate=true")
	}
	// GroupBy should be remapped to post-projection name
	if len(mi.GroupBy) != 1 || mi.GroupBy[0] != "nation" {
		t.Errorf("GroupBy = %v, want [nation]", mi.GroupBy)
	}
	// AggExpr OutputCol should be remapped to post-projection name
	if len(mi.AggExprs) != 1 || mi.AggExprs[0].OutputCol != "total_rev" {
		t.Errorf("AggExprs[0].OutputCol = %q, want %q", mi.AggExprs[0].OutputCol, "total_rev")
	}
	if mi.Limit != 10 {
		t.Errorf("Limit = %d, want 10", mi.Limit)
	}
}

func TestExtractMergeInfoNoProjectRename(t *testing.T) {
	// When Project has no renames, names should pass through unchanged.
	agg := &Node{
		Type:    NodeAggregate,
		GroupBy: []string{"n_name"},
		AggExprs: []AggExpr{
			{Func: "sum", InputCol: "revenue", OutputCol: "total_rev"},
		},
	}
	proj := &Node{
		Type: NodeProject,
		Projections: []Projection{
			{Column: "n_name", Alias: "n_name"},
			{Expr: "total_rev", Alias: "total_rev", IsAgg: true},
		},
		Children: []*Node{agg},
	}

	mi := ExtractMergeInfo(proj)
	if mi == nil {
		t.Fatal("ExtractMergeInfo returned nil")
	}
	if mi.GroupBy[0] != "n_name" {
		t.Errorf("GroupBy[0] = %q, want %q", mi.GroupBy[0], "n_name")
	}
	if mi.AggExprs[0].OutputCol != "total_rev" {
		t.Errorf("AggExprs[0].OutputCol = %q, want %q", mi.AggExprs[0].OutputCol, "total_rev")
	}
}

func TestExtractMergeInfoNoProject(t *testing.T) {
	// Plan with no Project node at all — names come directly from Aggregate.
	agg := &Node{
		Type:    NodeAggregate,
		GroupBy: []string{"l_returnflag"},
		AggExprs: []AggExpr{
			{Func: "sum", InputCol: "l_quantity", OutputCol: "sum_qty"},
		},
	}
	mi := ExtractMergeInfo(agg)
	if mi == nil {
		t.Fatal("ExtractMergeInfo returned nil")
	}
	if mi.GroupBy[0] != "l_returnflag" {
		t.Errorf("GroupBy[0] = %q, want %q", mi.GroupBy[0], "l_returnflag")
	}
	if mi.AggExprs[0].OutputCol != "sum_qty" {
		t.Errorf("AggExprs[0].OutputCol = %q, want %q", mi.AggExprs[0].OutputCol, "sum_qty")
	}
}

func TestExtractMergeInfoDistinct(t *testing.T) {
	// SELECT DISTINCT a, b FROM t ORDER BY a
	scan := NewScan("t", "t")
	proj := NewProject(scan, []Projection{
		{Column: "a", Alias: "a"},
		{Column: "b", Alias: "b"},
	})
	distinct := NewDistinct(proj)
	sorted := NewSort(distinct, []OrderExpr{{Column: "a"}})

	mi := ExtractMergeInfo(sorted)
	if mi == nil {
		t.Fatal("ExtractMergeInfo returned nil for DISTINCT query")
	}
	if !mi.HasDistinct {
		t.Error("expected HasDistinct = true")
	}
	if len(mi.OrderBy) != 1 || mi.OrderBy[0].Column != "a" {
		t.Errorf("OrderBy = %v, want [{a}]", mi.OrderBy)
	}
}

func TestExtractMergeInfoDistinctNoSort(t *testing.T) {
	// SELECT DISTINCT a, b FROM t  (no ORDER BY)
	scan := NewScan("t", "t")
	proj := NewProject(scan, []Projection{
		{Column: "a", Alias: "a"},
		{Column: "b", Alias: "b"},
	})
	distinct := NewDistinct(proj)

	mi := ExtractMergeInfo(distinct)
	if mi == nil {
		t.Fatal("ExtractMergeInfo returned nil for DISTINCT without ORDER BY")
	}
	if !mi.HasDistinct {
		t.Error("expected HasDistinct = true")
	}
}
