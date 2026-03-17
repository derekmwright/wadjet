package sql

import (
	"strings"
	"testing"
)

// --- AST Node String() methods ---

func TestColRefString(t *testing.T) {
	tests := []struct {
		node ColRef
		want string
	}{
		{ColRef{Column: "id"}, "id"},
		{ColRef{Table: "t", Column: "id"}, "t.id"},
	}
	for _, tt := range tests {
		got := tt.node.String()
		if got != tt.want {
			t.Errorf("ColRef{%q,%q}.String() = %q, want %q", tt.node.Table, tt.node.Column, got, tt.want)
		}
	}
}

func TestLitString(t *testing.T) {
	tests := []struct {
		node Lit
		want string
	}{
		{Lit{Value: "hello", Kind: LitString}, "'hello'"},
		{Lit{Value: "it's", Kind: LitString}, "'it''s'"}, // escaped quote
		{Lit{Value: "42", Kind: LitNumber}, "42"},
		{Lit{Value: "true", Kind: LitBool}, "true"},
		{Lit{Value: "", Kind: LitNull}, "null"},
	}
	for _, tt := range tests {
		got := tt.node.String()
		if got != tt.want {
			t.Errorf("Lit{%q,%d}.String() = %q, want %q", tt.node.Value, tt.node.Kind, got, tt.want)
		}
	}
}

func TestStarNodeString(t *testing.T) {
	if got := (&StarNode{}).String(); got != "*" {
		t.Errorf("StarNode{}.String() = %q, want '*'", got)
	}
	if got := (&StarNode{Table: "t"}).String(); got != "t.*" {
		t.Errorf("StarNode{t}.String() = %q, want 't.*'", got)
	}
}

func TestBinaryOpString(t *testing.T) {
	node := &BinaryOp{Left: &ColRef{Column: "a"}, Op: "+", Right: &Lit{Value: "1", Kind: LitNumber}}
	got := node.String()
	if got != "a + 1" {
		t.Errorf("BinaryOp.String() = %q, want 'a + 1'", got)
	}
}

func TestUnaryOpString(t *testing.T) {
	node := &UnaryOp{Op: "-", Inner: &ColRef{Column: "x"}}
	got := node.String()
	if got != "-x" {
		t.Errorf("UnaryOp.String() = %q, want '-x'", got)
	}
}

func TestCmpExprString(t *testing.T) {
	node := &CmpExpr{Left: &ColRef{Column: "a"}, Op: ">=", Right: &Lit{Value: "5", Kind: LitNumber}}
	got := node.String()
	if got != "a >= 5" {
		t.Errorf("CmpExpr.String() = %q, want 'a >= 5'", got)
	}
}

func TestInExprString(t *testing.T) {
	node := &InExpr{
		Left:   &ColRef{Column: "id"},
		Values: []Node{&Lit{Value: "1", Kind: LitNumber}, &Lit{Value: "2", Kind: LitNumber}},
	}
	got := node.String()
	if got != "id in (1, 2)" {
		t.Errorf("InExpr.String() = %q, want 'id in (1, 2)'", got)
	}
	// NOT IN
	node.Not = true
	got = node.String()
	if got != "id not in (1, 2)" {
		t.Errorf("InExpr.String() = %q, want 'id not in (1, 2)'", got)
	}
}

func TestBetweenExprString(t *testing.T) {
	node := &BetweenExpr{
		Left: &ColRef{Column: "x"},
		Low:  &Lit{Value: "1", Kind: LitNumber},
		High: &Lit{Value: "10", Kind: LitNumber},
	}
	got := node.String()
	if got != "x between 1 and 10" {
		t.Errorf("BetweenExpr.String() = %q, want 'x between 1 and 10'", got)
	}
	node.Not = true
	got = node.String()
	if got != "x not between 1 and 10" {
		t.Errorf("BetweenExpr NOT = %q", got)
	}
}

func TestLikeExprString(t *testing.T) {
	node := &LikeExpr{
		Left:    &ColRef{Column: "name"},
		Pattern: &Lit{Value: "%alice%", Kind: LitString},
	}
	got := node.String()
	if got != "name like '%alice%'" {
		t.Errorf("LikeExpr.String() = %q", got)
	}
	node.Not = true
	got = node.String()
	if got != "name not like '%alice%'" {
		t.Errorf("LikeExpr NOT = %q", got)
	}
}

func TestIsExprString(t *testing.T) {
	node := &IsExpr{Left: &ColRef{Column: "x"}, Check: "null"}
	got := node.String()
	if got != "x is null" {
		t.Errorf("IsExpr.String() = %q, want 'x is null'", got)
	}
	node.Not = true
	got = node.String()
	if got != "x is not null" {
		t.Errorf("IsExpr NOT = %q", got)
	}
}

func TestAndNodeString(t *testing.T) {
	node := &AndNode{
		Left:  &CmpExpr{Left: &ColRef{Column: "a"}, Op: "=", Right: &Lit{Value: "1", Kind: LitNumber}},
		Right: &CmpExpr{Left: &ColRef{Column: "b"}, Op: "=", Right: &Lit{Value: "2", Kind: LitNumber}},
	}
	got := node.String()
	if got != "a = 1 and b = 2" {
		t.Errorf("AndNode.String() = %q", got)
	}
}

func TestOrNodeString(t *testing.T) {
	node := &OrNode{
		Left:  &CmpExpr{Left: &ColRef{Column: "a"}, Op: "=", Right: &Lit{Value: "1", Kind: LitNumber}},
		Right: &CmpExpr{Left: &ColRef{Column: "b"}, Op: "=", Right: &Lit{Value: "2", Kind: LitNumber}},
	}
	got := node.String()
	if got != "a = 1 or b = 2" {
		t.Errorf("OrNode.String() = %q", got)
	}
}

func TestNotNodeString(t *testing.T) {
	node := &NotNode{Inner: &ColRef{Column: "active"}}
	got := node.String()
	if got != "not active" {
		t.Errorf("NotNode.String() = %q", got)
	}
}

func TestParenNodeString(t *testing.T) {
	node := &ParenNode{Inner: &ColRef{Column: "x"}}
	got := node.String()
	if got != "(x)" {
		t.Errorf("ParenNode.String() = %q, want '(x)'", got)
	}
}

func TestFuncCallNodeString(t *testing.T) {
	// Regular function
	node := &FuncCallNode{Name: "upper", Args: []Node{&ColRef{Column: "name"}}}
	got := node.String()
	if got != "upper(name)" {
		t.Errorf("FuncCallNode.String() = %q", got)
	}

	// Star function
	node2 := &FuncCallNode{Name: "count", Star: true}
	got2 := node2.String()
	if got2 != "count(*)" {
		t.Errorf("FuncCallNode star = %q", got2)
	}

	// DISTINCT
	node3 := &FuncCallNode{Name: "count", Args: []Node{&ColRef{Column: "id"}}, Distinct: true}
	got3 := node3.String()
	if got3 != "count(distinct id)" {
		t.Errorf("FuncCallNode distinct = %q", got3)
	}

	// Multiple args
	node4 := &FuncCallNode{Name: "concat", Args: []Node{&ColRef{Column: "a"}, &ColRef{Column: "b"}}}
	got4 := node4.String()
	if got4 != "concat(a, b)" {
		t.Errorf("FuncCallNode multi-arg = %q", got4)
	}
}

func TestCaseNodeString(t *testing.T) {
	// Searched CASE
	node := &CaseNode{
		Whens: []WhenClause{
			{Cond: &CmpExpr{Left: &ColRef{Column: "x"}, Op: ">", Right: &Lit{Value: "0", Kind: LitNumber}}, Result: &Lit{Value: "pos", Kind: LitString}},
		},
		Else: &Lit{Value: "neg", Kind: LitString},
	}
	got := node.String()
	if !strings.Contains(got, "case when") {
		t.Errorf("CaseNode.String() missing 'case when': %q", got)
	}
	if !strings.Contains(got, "else") {
		t.Errorf("CaseNode.String() missing 'else': %q", got)
	}
	if !strings.Contains(got, "end") {
		t.Errorf("CaseNode.String() missing 'end': %q", got)
	}

	// Simple CASE with subject
	node2 := &CaseNode{
		Subject: &ColRef{Column: "status"},
		Whens: []WhenClause{
			{Cond: &Lit{Value: "active", Kind: LitString}, Result: &Lit{Value: "1", Kind: LitNumber}},
		},
	}
	got2 := node2.String()
	if !strings.Contains(got2, "case status") {
		t.Errorf("CaseNode with subject = %q", got2)
	}
}

func TestCastNodeString(t *testing.T) {
	node := &CastNode{Inner: &ColRef{Column: "x"}, TypeName: "int64"}
	got := node.String()
	if got != "cast(x as int64)" {
		t.Errorf("CastNode.String() = %q", got)
	}
}

func TestArrayLitNodeString(t *testing.T) {
	node := &ArrayLitNode{Elements: []Node{
		&Lit{Value: "1", Kind: LitNumber},
		&Lit{Value: "2", Kind: LitNumber},
	}}
	got := node.String()
	if got != "ARRAY[1, 2]" {
		t.Errorf("ArrayLitNode.String() = %q", got)
	}
}

func TestSubqueryNodeString(t *testing.T) {
	node := &SubqueryNode{SQL: "SELECT 1"}
	got := node.String()
	if got != "(SELECT 1)" {
		t.Errorf("SubqueryNode.String() = %q", got)
	}
}

func TestExistsNodeString(t *testing.T) {
	node := &ExistsNode{SQL: "SELECT 1 FROM t"}
	got := node.String()
	if got != "exists (SELECT 1 FROM t)" {
		t.Errorf("ExistsNode.String() = %q", got)
	}
	node.Not = true
	got = node.String()
	if got != "not exists (SELECT 1 FROM t)" {
		t.Errorf("ExistsNode NOT = %q", got)
	}
}

func TestWindowFuncNodeString(t *testing.T) {
	node := &WindowFuncNode{
		Func: &FuncCallNode{Name: "row_number", Star: false},
	}
	got := node.String()
	if !strings.Contains(got, "row_number") {
		t.Errorf("WindowFuncNode.String() = %q", got)
	}
}

// --- IsAggregate ---

func TestIsAggregate(t *testing.T) {
	aggs := []string{"sum", "SUM", "count", "COUNT", "avg", "AVG", "min", "MIN", "max", "MAX",
		"grouping", "string_agg", "bool_and", "bool_or", "every", "stddev",
		"stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
		"approx_distinct", "corr", "covar_samp", "covar_pop",
		"percentile_cont", "percentile_disc", "mode", "min_by", "max_by", "median"}
	for _, name := range aggs {
		if !IsAggregate(name) {
			t.Errorf("IsAggregate(%q) = false, want true", name)
		}
	}
	nonAggs := []string{"upper", "lower", "substr", "concat", "format_bytes", "unknown"}
	for _, name := range nonAggs {
		if IsAggregate(name) {
			t.Errorf("IsAggregate(%q) = true, want false", name)
		}
	}
}

// --- FindAllAggregates ---

func TestFindAllAggregates(t *testing.T) {
	// Single aggregate
	node := &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}}
	aggs := FindAllAggregates(node)
	if len(aggs) != 1 || aggs[0].Name != "sum" {
		t.Errorf("expected 1 sum, got %v", aggs)
	}

	// Multi-aggregate expression: MAX(x) - MIN(x)
	node2 := &BinaryOp{
		Left:  &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}},
		Op:    "-",
		Right: &FuncCallNode{Name: "min", Args: []Node{&ColRef{Column: "x"}}},
	}
	aggs2 := FindAllAggregates(node2)
	if len(aggs2) != 2 {
		t.Errorf("expected 2 aggregates, got %d", len(aggs2))
	}

	// No aggregate
	node3 := &ColRef{Column: "name"}
	aggs3 := FindAllAggregates(node3)
	if len(aggs3) != 0 {
		t.Errorf("expected 0 aggregates, got %d", len(aggs3))
	}

	// Nil
	aggs4 := FindAllAggregates(nil)
	if len(aggs4) != 0 {
		t.Errorf("expected 0 for nil, got %d", len(aggs4))
	}

	// In ParenNode
	node5 := &ParenNode{Inner: &FuncCallNode{Name: "count", Star: true}}
	aggs5 := FindAllAggregates(node5)
	if len(aggs5) != 1 {
		t.Errorf("expected 1 aggregate in paren, got %d", len(aggs5))
	}

	// In UnaryOp
	node6 := &UnaryOp{Op: "-", Inner: &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}}}
	aggs6 := FindAllAggregates(node6)
	if len(aggs6) != 1 {
		t.Errorf("expected 1 aggregate in unary, got %d", len(aggs6))
	}

	// In CmpExpr
	node7 := &CmpExpr{
		Left:  &FuncCallNode{Name: "count", Star: true},
		Op:    ">",
		Right: &Lit{Value: "5", Kind: LitNumber},
	}
	aggs7 := FindAllAggregates(node7)
	if len(aggs7) != 1 {
		t.Errorf("expected 1 aggregate in cmp, got %d", len(aggs7))
	}

	// In CaseNode
	node8 := &CaseNode{
		Whens: []WhenClause{
			{Cond: &Lit{Value: "true", Kind: LitBool}, Result: &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "a"}}}},
		},
		Else: &FuncCallNode{Name: "count", Star: true},
	}
	aggs8 := FindAllAggregates(node8)
	if len(aggs8) != 2 {
		t.Errorf("expected 2 aggregates in case, got %d", len(aggs8))
	}

	// In CastNode
	node9 := &CastNode{Inner: &FuncCallNode{Name: "avg", Args: []Node{&ColRef{Column: "x"}}}, TypeName: "float"}
	aggs9 := FindAllAggregates(node9)
	if len(aggs9) != 1 {
		t.Errorf("expected 1 aggregate in cast, got %d", len(aggs9))
	}
}

// --- ReplaceAllAggregates ---

func TestReplaceAllAggregates(t *testing.T) {
	// MAX(x) - MIN(x) → __agg_0 - __agg_1
	replacements := map[string]string{
		"max(x)": "__agg_0",
		"min(x)": "__agg_1",
	}
	node := &BinaryOp{
		Left:  &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}},
		Op:    "-",
		Right: &FuncCallNode{Name: "min", Args: []Node{&ColRef{Column: "x"}}},
	}
	result := ReplaceAllAggregates(node, replacements)
	got := result.String()
	if got != "__agg_0 - __agg_1" {
		t.Errorf("ReplaceAllAggregates = %q, want '__agg_0 - __agg_1'", got)
	}

	// Nil
	result = ReplaceAllAggregates(nil, replacements)
	if result != nil {
		t.Error("expected nil for nil input")
	}

	// No match
	node2 := &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "y"}}}
	result2 := ReplaceAllAggregates(node2, replacements)
	if result2 != node2 {
		t.Error("expected same node when no match")
	}

	// UnaryOp
	node3 := &UnaryOp{Op: "-", Inner: &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}}}
	result3 := ReplaceAllAggregates(node3, replacements)
	if result3.String() != "-__agg_0" {
		t.Errorf("UnaryOp replacement = %q", result3.String())
	}

	// ParenNode
	node4 := &ParenNode{Inner: &FuncCallNode{Name: "min", Args: []Node{&ColRef{Column: "x"}}}}
	result4 := ReplaceAllAggregates(node4, replacements)
	if result4.String() != "(__agg_1)" {
		t.Errorf("ParenNode replacement = %q", result4.String())
	}

	// CastNode
	node5 := &CastNode{Inner: &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}}, TypeName: "int"}
	result5 := ReplaceAllAggregates(node5, replacements)
	if !strings.Contains(result5.String(), "__agg_0") {
		t.Errorf("CastNode replacement = %q", result5.String())
	}

	// CaseNode with aggregate in when result and else
	node6 := &CaseNode{
		Whens: []WhenClause{
			{Cond: &Lit{Value: "true", Kind: LitBool}, Result: &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}}},
		},
		Else: &FuncCallNode{Name: "min", Args: []Node{&ColRef{Column: "x"}}},
	}
	result6 := ReplaceAllAggregates(node6, replacements)
	got6 := result6.String()
	if !strings.Contains(got6, "__agg_0") || !strings.Contains(got6, "__agg_1") {
		t.Errorf("CaseNode replacement = %q", got6)
	}

	// Non-aggregate FuncCallNode with aggregate args
	node7 := &FuncCallNode{
		Name: "format_bytes",
		Args: []Node{&FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}}},
	}
	result7 := ReplaceAllAggregates(node7, replacements)
	if result7.String() != "format_bytes(__agg_0)" {
		t.Errorf("nested func replacement = %q", result7.String())
	}
}

// --- anyToLit ---

func TestAnyToLit(t *testing.T) {
	tests := []struct {
		input any
		want  string
		kind  LiteralKind
	}{
		{nil, "null", LitNull},
		{"hello", "'hello'", LitString},
		{int64(42), "42", LitNumber},
		{int(10), "10", LitNumber},
		{int32(5), "5", LitNumber},
		{float64(3.14), "3.14", LitNumber},
		{float32(2.5), "2.5", LitNumber},
		{true, "true", LitBool},
		{false, "false", LitBool},
		{[]byte{1, 2, 3}, "'[1 2 3]'", LitString}, // fallback
	}
	for _, tt := range tests {
		got := anyToLit(tt.input)
		if got.String() != tt.want {
			t.Errorf("anyToLit(%v).String() = %q, want %q", tt.input, got.String(), tt.want)
		}
		if got.Kind != tt.kind {
			t.Errorf("anyToLit(%v).Kind = %d, want %d", tt.input, got.Kind, tt.kind)
		}
	}
}

// --- ParseExpression edge cases ---

func TestParseExpression_CASE(t *testing.T) {
	expr, err := ParseExpression("CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	cn, ok := expr.(*CaseNode)
	if !ok {
		t.Fatalf("expected CaseNode, got %T", expr)
	}
	if len(cn.Whens) != 1 {
		t.Errorf("expected 1 WHEN clause, got %d", len(cn.Whens))
	}
	if cn.Else == nil {
		t.Error("expected ELSE clause")
	}
}

func TestParseExpression_CAST(t *testing.T) {
	expr, err := ParseExpression("CAST(x AS INTEGER)")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	cn, ok := expr.(*CastNode)
	if !ok {
		t.Fatalf("expected CastNode, got %T", expr)
	}
	if cn.TypeName != "integer" {
		t.Errorf("TypeName = %q, want 'integer'", cn.TypeName)
	}
}

func TestParseExpression_NOT(t *testing.T) {
	expr, err := ParseExpression("NOT active")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	nn, ok := expr.(*NotNode)
	if !ok {
		t.Fatalf("expected NotNode, got %T", expr)
	}
	if nn.Inner == nil {
		t.Error("expected inner expression")
	}
}

func TestParseExpression_IN(t *testing.T) {
	expr, err := ParseExpression("x IN (1, 2, 3)")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	ie, ok := expr.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", expr)
	}
	if len(ie.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(ie.Values))
	}
}

func TestParseExpression_BETWEEN(t *testing.T) {
	expr, err := ParseExpression("x BETWEEN 1 AND 10")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	be, ok := expr.(*BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", expr)
	}
	if be.Not {
		t.Error("expected Not=false")
	}
}

func TestParseExpression_LIKE(t *testing.T) {
	expr, err := ParseExpression("name LIKE '%alice%'")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	le, ok := expr.(*LikeExpr)
	if !ok {
		t.Fatalf("expected LikeExpr, got %T", expr)
	}
	if le.Not {
		t.Error("expected Not=false")
	}
}

func TestParseExpression_IS_NULL(t *testing.T) {
	expr, err := ParseExpression("x IS NULL")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	ie, ok := expr.(*IsExpr)
	if !ok {
		t.Fatalf("expected IsExpr, got %T", expr)
	}
	if ie.Check != "null" {
		t.Errorf("Check = %q, want 'null'", ie.Check)
	}
}

func TestParseExpression_IS_NOT_NULL(t *testing.T) {
	expr, err := ParseExpression("x IS NOT NULL")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	ie, ok := expr.(*IsExpr)
	if !ok {
		t.Fatalf("expected IsExpr, got %T", expr)
	}
	if !ie.Not {
		t.Error("expected Not=true")
	}
}

func TestParseExpression_OR(t *testing.T) {
	expr, err := ParseExpression("a = 1 OR b = 2")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	on, ok := expr.(*OrNode)
	if !ok {
		t.Fatalf("expected OrNode, got %T", expr)
	}
	if on.Left == nil || on.Right == nil {
		t.Error("expected both sides of OR")
	}
}

func TestParseExpression_AND(t *testing.T) {
	expr, err := ParseExpression("a = 1 AND b = 2")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	an, ok := expr.(*AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", expr)
	}
	if an.Left == nil || an.Right == nil {
		t.Error("expected both sides of AND")
	}
}

func TestParseExpression_Arithmetic(t *testing.T) {
	expr, err := ParseExpression("a + b * c")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	// Should be: a + (b * c) due to precedence
	bo, ok := expr.(*BinaryOp)
	if !ok {
		t.Fatalf("expected BinaryOp, got %T", expr)
	}
	if bo.Op != "+" {
		t.Errorf("top op = %q, want '+'", bo.Op)
	}
}

func TestParseExpression_UnaryMinus(t *testing.T) {
	expr, err := ParseExpression("-x")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	uo, ok := expr.(*UnaryOp)
	if !ok {
		t.Fatalf("expected UnaryOp, got %T", expr)
	}
	if uo.Op != "-" {
		t.Errorf("Op = %q, want '-'", uo.Op)
	}
}

func TestParseExpression_StringConcat(t *testing.T) {
	expr, err := ParseExpression("a || b")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	bo, ok := expr.(*BinaryOp)
	if !ok {
		t.Fatalf("expected BinaryOp, got %T", expr)
	}
	if bo.Op != "||" {
		t.Errorf("Op = %q, want '||'", bo.Op)
	}
}

func TestParseExpression_Parenthesized(t *testing.T) {
	expr, err := ParseExpression("(a + b) * c")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	bo, ok := expr.(*BinaryOp)
	if !ok {
		t.Fatalf("expected BinaryOp, got %T", expr)
	}
	if bo.Op != "*" {
		t.Errorf("top op = %q, want '*'", bo.Op)
	}
}

// --- RebuildSQL ---

func TestRebuildSQL_WithJoin(t *testing.T) {
	parsed, err := Parse("SELECT o.id, l.amount FROM orders o INNER JOIN line_items l ON o.id = l.order_id WHERE o.status = 'active' GROUP BY o.id HAVING count(*) > 1 ORDER BY o.id DESC LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	rewritten := &Lit{Value: "1", Kind: LitNumber}
	sql := RebuildSQL(info, rewritten)

	if !strings.Contains(sql, "SELECT") {
		t.Errorf("missing SELECT: %q", sql)
	}
	if !strings.Contains(sql, "FROM orders") {
		t.Errorf("missing FROM: %q", sql)
	}
	if !strings.Contains(sql, "WHERE 1") {
		t.Errorf("missing rewritten WHERE: %q", sql)
	}
	if !strings.Contains(sql, "JOIN") || !strings.Contains(sql, "line_items") {
		t.Errorf("missing JOIN: %q", sql)
	}
	if !strings.Contains(sql, "GROUP BY") {
		t.Errorf("missing GROUP BY: %q", sql)
	}
	if !strings.Contains(sql, "HAVING") {
		t.Errorf("missing HAVING: %q", sql)
	}
	if !strings.Contains(sql, "ORDER BY") {
		t.Errorf("missing ORDER BY: %q", sql)
	}
	if !strings.Contains(sql, "LIMIT 10") {
		t.Errorf("missing LIMIT: %q", sql)
	}
	if !strings.Contains(sql, "OFFSET 5") {
		t.Errorf("missing OFFSET: %q", sql)
	}
}

func TestRebuildSQL_Distinct(t *testing.T) {
	parsed, err := Parse("SELECT DISTINCT name FROM users")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	sql := RebuildSQL(info, nil)
	if !strings.Contains(sql, "DISTINCT") {
		t.Errorf("missing DISTINCT: %q", sql)
	}
}

func TestRebuildSQL_NilWhere(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}

	sql := RebuildSQL(info, nil)
	if strings.Contains(sql, "WHERE") {
		t.Errorf("should not have WHERE clause: %q", sql)
	}
}

// --- FindCorrelatedRefsWithColumns ---

func TestFindCorrelatedRefsWithColumns(t *testing.T) {
	outerTables := map[string]bool{"o": true}
	outerCols := map[string]string{"customer_id": "o"}

	// Unqualified ref that maps to outer table
	refs, err := FindCorrelatedRefsWithColumns(
		"SELECT 1 FROM items WHERE customer_id = 42",
		outerTables, outerCols,
	)
	if err != nil {
		t.Fatalf("FindCorrelatedRefsWithColumns: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Table != "o" || refs[0].Column != "customer_id" {
		t.Errorf("unexpected ref: %+v", refs[0])
	}
}

// --- RewriteUnqualifiedOuterRefs ---

func TestRewriteUnqualifiedOuterRefs(t *testing.T) {
	unqualOuter := map[string]string{"customer_id": "o"}
	vals := map[string]any{"o.customer_id": int64(42)}

	expr, err := ParseExpression("customer_id = status")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}

	result := RewriteUnqualifiedOuterRefs(expr, unqualOuter, vals)
	got := result.String()
	if !strings.Contains(got, "42") {
		t.Errorf("expected rewritten customer_id, got %q", got)
	}
}

func TestRewriteUnqualifiedOuterRefs_NilNode(t *testing.T) {
	result := RewriteUnqualifiedOuterRefs(nil, map[string]string{"x": "t"}, nil)
	if result != nil {
		t.Error("expected nil for nil node")
	}
}

func TestRewriteUnqualifiedOuterRefs_EmptyOuter(t *testing.T) {
	node := &ColRef{Column: "x"}
	result := RewriteUnqualifiedOuterRefs(node, nil, nil)
	if result != node {
		t.Error("expected unchanged node for empty outer")
	}
}

func TestRewriteUnqualifiedOuterRefs_AllBranches(t *testing.T) {
	unqualOuter := map[string]string{"x": "t"}
	vals := map[string]any{"t.x": int64(1)}

	// BinaryOp
	node := &BinaryOp{Left: &ColRef{Column: "x"}, Op: "+", Right: &Lit{Value: "1", Kind: LitNumber}}
	result := RewriteUnqualifiedOuterRefs(node, unqualOuter, vals)
	if !strings.Contains(result.String(), "1") {
		t.Errorf("BinaryOp rewrite: %q", result.String())
	}

	// UnaryOp
	node2 := &UnaryOp{Op: "-", Inner: &ColRef{Column: "x"}}
	result2 := RewriteUnqualifiedOuterRefs(node2, unqualOuter, vals)
	if result2 == nil {
		t.Error("UnaryOp rewrite returned nil")
	}

	// CmpExpr
	node3 := &CmpExpr{Left: &ColRef{Column: "x"}, Op: "=", Right: &Lit{Value: "1", Kind: LitNumber}}
	result3 := RewriteUnqualifiedOuterRefs(node3, unqualOuter, vals)
	if result3 == nil {
		t.Error("CmpExpr rewrite returned nil")
	}

	// AndNode
	node4 := &AndNode{Left: &ColRef{Column: "x"}, Right: &Lit{Value: "1", Kind: LitNumber}}
	result4 := RewriteUnqualifiedOuterRefs(node4, unqualOuter, vals)
	if result4 == nil {
		t.Error("AndNode rewrite returned nil")
	}

	// OrNode
	node5 := &OrNode{Left: &ColRef{Column: "x"}, Right: &Lit{Value: "1", Kind: LitNumber}}
	result5 := RewriteUnqualifiedOuterRefs(node5, unqualOuter, vals)
	if result5 == nil {
		t.Error("OrNode rewrite returned nil")
	}

	// NotNode
	node6 := &NotNode{Inner: &ColRef{Column: "x"}}
	result6 := RewriteUnqualifiedOuterRefs(node6, unqualOuter, vals)
	if result6 == nil {
		t.Error("NotNode rewrite returned nil")
	}

	// ParenNode
	node7 := &ParenNode{Inner: &ColRef{Column: "x"}}
	result7 := RewriteUnqualifiedOuterRefs(node7, unqualOuter, vals)
	if result7 == nil {
		t.Error("ParenNode rewrite returned nil")
	}

	// FuncCallNode
	node8 := &FuncCallNode{Name: "upper", Args: []Node{&ColRef{Column: "x"}}}
	result8 := RewriteUnqualifiedOuterRefs(node8, unqualOuter, vals)
	if result8 == nil {
		t.Error("FuncCallNode rewrite returned nil")
	}

	// InExpr
	node9 := &InExpr{Left: &ColRef{Column: "x"}, Values: []Node{&Lit{Value: "1", Kind: LitNumber}}}
	result9 := RewriteUnqualifiedOuterRefs(node9, unqualOuter, vals)
	if result9 == nil {
		t.Error("InExpr rewrite returned nil")
	}

	// BetweenExpr
	node10 := &BetweenExpr{Left: &ColRef{Column: "x"}, Low: &Lit{Value: "0", Kind: LitNumber}, High: &Lit{Value: "10", Kind: LitNumber}}
	result10 := RewriteUnqualifiedOuterRefs(node10, unqualOuter, vals)
	if result10 == nil {
		t.Error("BetweenExpr rewrite returned nil")
	}

	// CastNode
	node11 := &CastNode{Inner: &ColRef{Column: "x"}, TypeName: "int"}
	result11 := RewriteUnqualifiedOuterRefs(node11, unqualOuter, vals)
	if result11 == nil {
		t.Error("CastNode rewrite returned nil")
	}
}

// --- RewriteOuterRefs all branches ---

func TestRewriteOuterRefs_AllBranches(t *testing.T) {
	outerTables := map[string]bool{"o": true}
	vals := map[string]any{"o.x": int64(1)}

	// BinaryOp
	node := &BinaryOp{Left: &ColRef{Table: "o", Column: "x"}, Op: "+", Right: &Lit{Value: "1", Kind: LitNumber}}
	result := RewriteOuterRefs(node, outerTables, vals)
	if result == nil {
		t.Error("BinaryOp returned nil")
	}

	// UnaryOp
	node2 := &UnaryOp{Op: "-", Inner: &ColRef{Table: "o", Column: "x"}}
	result2 := RewriteOuterRefs(node2, outerTables, vals)
	if result2 == nil {
		t.Error("UnaryOp returned nil")
	}

	// AndNode
	node3 := &AndNode{Left: &ColRef{Table: "o", Column: "x"}, Right: &Lit{Value: "1", Kind: LitNumber}}
	result3 := RewriteOuterRefs(node3, outerTables, vals)
	if result3 == nil {
		t.Error("AndNode returned nil")
	}

	// OrNode
	node4 := &OrNode{Left: &ColRef{Table: "o", Column: "x"}, Right: &Lit{Value: "1", Kind: LitNumber}}
	result4 := RewriteOuterRefs(node4, outerTables, vals)
	if result4 == nil {
		t.Error("OrNode returned nil")
	}

	// NotNode
	node5 := &NotNode{Inner: &ColRef{Table: "o", Column: "x"}}
	result5 := RewriteOuterRefs(node5, outerTables, vals)
	if result5 == nil {
		t.Error("NotNode returned nil")
	}

	// ParenNode
	node6 := &ParenNode{Inner: &ColRef{Table: "o", Column: "x"}}
	result6 := RewriteOuterRefs(node6, outerTables, vals)
	if result6 == nil {
		t.Error("ParenNode returned nil")
	}

	// FuncCallNode
	node7 := &FuncCallNode{Name: "f", Args: []Node{&ColRef{Table: "o", Column: "x"}}}
	result7 := RewriteOuterRefs(node7, outerTables, vals)
	if result7 == nil {
		t.Error("FuncCallNode returned nil")
	}

	// InExpr
	node8 := &InExpr{Left: &ColRef{Table: "o", Column: "x"}, Values: []Node{&Lit{Value: "1", Kind: LitNumber}}}
	result8 := RewriteOuterRefs(node8, outerTables, vals)
	if result8 == nil {
		t.Error("InExpr returned nil")
	}

	// BetweenExpr
	node9 := &BetweenExpr{Left: &ColRef{Table: "o", Column: "x"}, Low: &Lit{Value: "0", Kind: LitNumber}, High: &Lit{Value: "10", Kind: LitNumber}}
	result9 := RewriteOuterRefs(node9, outerTables, vals)
	if result9 == nil {
		t.Error("BetweenExpr returned nil")
	}

	// LikeExpr
	node10 := &LikeExpr{Left: &ColRef{Table: "o", Column: "x"}, Pattern: &Lit{Value: "%a%", Kind: LitString}}
	result10 := RewriteOuterRefs(node10, outerTables, vals)
	if result10 == nil {
		t.Error("LikeExpr returned nil")
	}

	// IsExpr
	node11 := &IsExpr{Left: &ColRef{Table: "o", Column: "x"}, Check: "null"}
	result11 := RewriteOuterRefs(node11, outerTables, vals)
	if result11 == nil {
		t.Error("IsExpr returned nil")
	}

	// CaseNode
	node12 := &CaseNode{
		Subject: &ColRef{Table: "o", Column: "x"},
		Whens: []WhenClause{
			{Cond: &Lit{Value: "1", Kind: LitNumber}, Result: &ColRef{Table: "o", Column: "x"}},
		},
		Else: &ColRef{Table: "o", Column: "x"},
	}
	result12 := RewriteOuterRefs(node12, outerTables, vals)
	if result12 == nil {
		t.Error("CaseNode returned nil")
	}

	// CastNode
	node13 := &CastNode{Inner: &ColRef{Table: "o", Column: "x"}, TypeName: "int"}
	result13 := RewriteOuterRefs(node13, outerTables, vals)
	if result13 == nil {
		t.Error("CastNode returned nil")
	}

	// Nil
	result14 := RewriteOuterRefs(nil, outerTables, vals)
	if result14 != nil {
		t.Error("expected nil for nil")
	}
}

// --- SQL parsing for CASE/CAST/ARRAY/NOT BETWEEN/NOT LIKE/NOT IN ---

func TestParseExpression_NOT_IN(t *testing.T) {
	expr, err := ParseExpression("x NOT IN (1, 2)")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	ie, ok := expr.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", expr)
	}
	if !ie.Not {
		t.Error("expected Not=true")
	}
}

func TestParseExpression_NOT_BETWEEN(t *testing.T) {
	expr, err := ParseExpression("x NOT BETWEEN 1 AND 10")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	be, ok := expr.(*BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", expr)
	}
	if !be.Not {
		t.Error("expected Not=true")
	}
}

func TestParseExpression_NOT_LIKE(t *testing.T) {
	expr, err := ParseExpression("name NOT LIKE '%test%'")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	le, ok := expr.(*LikeExpr)
	if !ok {
		t.Fatalf("expected LikeExpr, got %T", expr)
	}
	if !le.Not {
		t.Error("expected Not=true")
	}
}

func TestParseExpression_SimpleCASE(t *testing.T) {
	expr, err := ParseExpression("CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	cn, ok := expr.(*CaseNode)
	if !ok {
		t.Fatalf("expected CaseNode, got %T", expr)
	}
	if cn.Subject == nil {
		t.Error("expected Subject for simple CASE")
	}
	if len(cn.Whens) != 2 {
		t.Errorf("expected 2 WHEN clauses, got %d", len(cn.Whens))
	}
}

// --- Parser: join types in FROM clause ---

func TestParse_InnerJoin(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a INNER JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}
	if info.Joins[0].Type != "join" {
		t.Errorf("expected join type 'join', got %q", info.Joins[0].Type)
	}
}

func TestParse_LeftOuterJoin(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a LEFT OUTER JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}
	if info.Joins[0].Type != "left join" {
		t.Errorf("expected 'left join', got %q", info.Joins[0].Type)
	}
}

func TestParse_RightOuterJoin(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a RIGHT OUTER JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}
	if info.Joins[0].Type != "right join" {
		t.Errorf("expected 'right join', got %q", info.Joins[0].Type)
	}
}

func TestParse_FullOuterJoin(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a FULL OUTER JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}
	if info.Joins[0].Type != "full outer join" {
		t.Errorf("expected 'full outer join', got %q", info.Joins[0].Type)
	}
}

func TestParse_CrossJoin(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a CROSS JOIN b")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}
	if info.Joins[0].Type != "cross join" {
		t.Errorf("expected 'cross join', got %q", info.Joins[0].Type)
	}
}

func TestParse_CommaJoin(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a, b")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(info.Tables))
	}
}

func TestParse_LeftJoinNoOuter(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Joins[0].Type != "left join" {
		t.Errorf("expected 'left join', got %q", info.Joins[0].Type)
	}
}

func TestParse_RightJoinNoOuter(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a RIGHT JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Joins[0].Type != "right join" {
		t.Errorf("expected 'right join', got %q", info.Joins[0].Type)
	}
}

func TestParse_FullJoinNoOuter(t *testing.T) {
	parsed, err := Parse("SELECT a.id FROM a FULL JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Joins[0].Type != "full outer join" {
		t.Errorf("expected 'full outer join', got %q", info.Joins[0].Type)
	}
}

// --- Parser: subquery in FROM ---

func TestParse_SubqueryInFrom(t *testing.T) {
	parsed, err := Parse("SELECT x.id FROM (SELECT id FROM events) AS x")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Tables) == 0 {
		t.Fatal("expected at least one table")
	}
	if info.Tables[0].Alias != "x" {
		t.Errorf("expected alias 'x', got %q", info.Tables[0].Alias)
	}
}

// --- Parser: subscript access ---

func TestParseExpression_Subscript(t *testing.T) {
	expr, err := ParseExpression("arr[1]")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	fn, ok := expr.(*FuncCallNode)
	if !ok {
		t.Fatalf("expected FuncCallNode (element_at rewrite), got %T", expr)
	}
	if fn.Name != "element_at" {
		t.Errorf("expected function name 'element_at', got %q", fn.Name)
	}
	if len(fn.Args) != 2 {
		t.Errorf("expected 2 args, got %d", len(fn.Args))
	}
}

// --- Parser: ARRAY literal ---

func TestParseExpression_ArrayLiteral(t *testing.T) {
	expr, err := ParseExpression("ARRAY[1, 2, 3]")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	al, ok := expr.(*ArrayLitNode)
	if !ok {
		t.Fatalf("expected ArrayLitNode, got %T", expr)
	}
	if len(al.Elements) != 3 {
		t.Errorf("expected 3 elements, got %d", len(al.Elements))
	}
}

// --- Parser: TRUE, FALSE, NULL literals ---

func TestParseExpression_TrueFalseNull(t *testing.T) {
	for _, tc := range []struct {
		input string
		kind  LiteralKind
	}{
		{"TRUE", LitBool},
		{"FALSE", LitBool},
		{"NULL", LitNull},
	} {
		expr, err := ParseExpression(tc.input)
		if err != nil {
			t.Fatalf("ParseExpression(%q): %v", tc.input, err)
		}
		lit, ok := expr.(*Lit)
		if !ok {
			t.Errorf("ParseExpression(%q): expected Lit, got %T", tc.input, expr)
			continue
		}
		if lit.Kind != tc.kind {
			t.Errorf("ParseExpression(%q): expected kind %v, got %v", tc.input, tc.kind, lit.Kind)
		}
	}
}

// --- Parser: subquery in expression ---

func TestParseExpression_Subquery(t *testing.T) {
	expr, err := ParseExpression("(SELECT MAX(id) FROM events)")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	sq, ok := expr.(*SubqueryNode)
	if !ok {
		t.Fatalf("expected SubqueryNode, got %T", expr)
	}
	if !strings.Contains(sq.SQL, "MAX") {
		t.Error("expected SQL to contain MAX")
	}
}

// --- Parser: table alias without AS ---

func TestParse_TableAliasWithoutAS(t *testing.T) {
	parsed, err := Parse("SELECT e.id FROM events e")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Tables[0].Alias != "e" {
		t.Errorf("expected alias 'e', got %q", info.Tables[0].Alias)
	}
}

// --- Parser: table alias with AS ---

func TestParse_TableAliasWithAS(t *testing.T) {
	parsed, err := Parse("SELECT e.id FROM events AS e")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Tables[0].Alias != "e" {
		t.Errorf("expected alias 'e', got %q", info.Tables[0].Alias)
	}
}

// --- Parser: EXISTS expression ---

func TestParseExpression_EXISTS(t *testing.T) {
	expr, err := ParseExpression("EXISTS (SELECT 1 FROM events)")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	en, ok := expr.(*ExistsNode)
	if !ok {
		t.Fatalf("expected ExistsNode, got %T", expr)
	}
	if en.Not {
		t.Error("expected Not=false")
	}
	if !strings.Contains(en.SQL, "events") {
		t.Error("expected SQL to contain 'events'")
	}
}

func TestParseExpression_NOT_EXISTS(t *testing.T) {
	expr, err := ParseExpression("NOT EXISTS (SELECT 1 FROM events)")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	en, ok := expr.(*ExistsNode)
	if !ok {
		t.Fatalf("expected ExistsNode, got %T", expr)
	}
	if !en.Not {
		t.Error("expected Not=true")
	}
}

// --- Parser: HAVING clause ---

func TestParse_Having(t *testing.T) {
	parsed, err := Parse("SELECT status, COUNT(*) FROM events GROUP BY status HAVING COUNT(*) > 5")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Having == "" {
		t.Error("expected HAVING clause to be populated")
	}
}

// --- Parser: multiple ORDER BY columns ---

func TestParse_OrderByMultiple(t *testing.T) {
	parsed, err := Parse("SELECT id, name FROM events ORDER BY name ASC, id DESC")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.OrderBy) != 2 {
		t.Fatalf("expected 2 order-by columns, got %d", len(info.OrderBy))
	}
	if info.OrderBy[1].Desc != true {
		t.Error("expected second order-by to be DESC")
	}
}

// --- Parser: UNION, INTERSECT, EXCEPT ---

func TestParse_UnionAll(t *testing.T) {
	parsed, err := Parse("SELECT id FROM a UNION ALL SELECT id FROM b")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Union == nil {
		t.Error("expected Union to be populated")
	}
}

func TestParse_Intersect(t *testing.T) {
	parsed, err := Parse("SELECT id FROM a INTERSECT SELECT id FROM b")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Union == nil || info.Union.Op != SetOpIntersect {
		t.Errorf("expected Union.Op=INTERSECT")
	}
}

func TestParse_Except(t *testing.T) {
	parsed, err := Parse("SELECT id FROM a EXCEPT SELECT id FROM b")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if info.Union == nil || info.Union.Op != SetOpExcept {
		t.Errorf("expected Union.Op=EXCEPT")
	}
}

// --- Parser: DISTINCT ---

func TestParse_Distinct(t *testing.T) {
	parsed, err := Parse("SELECT DISTINCT id FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if !info.Distinct {
		t.Error("expected Distinct=true")
	}
}

// --- Parser: CTE (WITH clause) ---

func TestParse_CTE(t *testing.T) {
	parsed, err := Parse("WITH active AS (SELECT id FROM events WHERE status = 'active') SELECT * FROM active")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.CTEs) == 0 {
		t.Error("expected CTEs to be populated")
	}
}

// --- Parser: WINDOW function ---

func TestParse_WindowFunction(t *testing.T) {
	parsed, err := Parse("SELECT id, ROW_NUMBER() OVER (PARTITION BY status ORDER BY id) FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Columns) < 2 {
		t.Fatal("expected at least 2 columns")
	}
}

// --- walkForOuterRefs: cover all branches ---

func TestWalkForOuterRefs_AllNodeTypes(t *testing.T) {
	outer := map[string]bool{"events": true}
	inner := map[string]bool{}
	outerCols := map[string]string{"status": "events"}

	// BinaryOp with outer ref in Left
	var refs []OuterRef
	walkForOuterRefs(&BinaryOp{
		Left:  &ColRef{Table: "events", Column: "id"},
		Op:    "+",
		Right: &Lit{Value: "1", Kind: LitNumber},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 1 || refs[0].Column != "id" {
		t.Errorf("BinaryOp: expected ref to id, got %+v", refs)
	}

	// UnaryOp
	refs = nil
	walkForOuterRefs(&UnaryOp{Op: "-", Inner: &ColRef{Table: "events", Column: "x"}}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("UnaryOp: expected 1 ref, got %d", len(refs))
	}

	// OrNode
	refs = nil
	walkForOuterRefs(&OrNode{
		Left:  &ColRef{Table: "events", Column: "a"},
		Right: &ColRef{Table: "events", Column: "b"},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 2 {
		t.Errorf("OrNode: expected 2 refs, got %d", len(refs))
	}

	// NotNode
	refs = nil
	walkForOuterRefs(&NotNode{Inner: &ColRef{Table: "events", Column: "c"}}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("NotNode: expected 1 ref, got %d", len(refs))
	}

	// ParenNode
	refs = nil
	walkForOuterRefs(&ParenNode{Inner: &ColRef{Table: "events", Column: "d"}}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("ParenNode: expected 1 ref, got %d", len(refs))
	}

	// FuncCallNode
	refs = nil
	walkForOuterRefs(&FuncCallNode{Name: "upper", Args: []Node{&ColRef{Table: "events", Column: "e"}}}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("FuncCallNode: expected 1 ref, got %d", len(refs))
	}

	// InExpr
	refs = nil
	walkForOuterRefs(&InExpr{
		Left:   &ColRef{Table: "events", Column: "f"},
		Values: []Node{&ColRef{Table: "events", Column: "g"}},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 2 {
		t.Errorf("InExpr: expected 2 refs, got %d", len(refs))
	}

	// BetweenExpr
	refs = nil
	walkForOuterRefs(&BetweenExpr{
		Left: &ColRef{Table: "events", Column: "h"},
		Low:  &ColRef{Table: "events", Column: "lo"},
		High: &ColRef{Table: "events", Column: "hi"},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 3 {
		t.Errorf("BetweenExpr: expected 3 refs, got %d", len(refs))
	}

	// LikeExpr
	refs = nil
	walkForOuterRefs(&LikeExpr{
		Left:    &ColRef{Table: "events", Column: "i"},
		Pattern: &ColRef{Table: "events", Column: "pat"},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 2 {
		t.Errorf("LikeExpr: expected 2 refs, got %d", len(refs))
	}

	// IsExpr
	refs = nil
	walkForOuterRefs(&IsExpr{Left: &ColRef{Table: "events", Column: "j"}}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("IsExpr: expected 1 ref, got %d", len(refs))
	}

	// CaseNode with subject and else
	refs = nil
	walkForOuterRefs(&CaseNode{
		Subject: &ColRef{Table: "events", Column: "k"},
		Whens: []WhenClause{
			{Cond: &ColRef{Table: "events", Column: "cond"}, Result: &Lit{Value: "1"}},
		},
		Else: &ColRef{Table: "events", Column: "els"},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 3 {
		t.Errorf("CaseNode: expected 3 refs, got %d", len(refs))
	}

	// CastNode
	refs = nil
	walkForOuterRefs(&CastNode{Inner: &ColRef{Table: "events", Column: "m"}, TypeName: "int"}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("CastNode: expected 1 ref, got %d", len(refs))
	}

	// CmpExpr
	refs = nil
	walkForOuterRefs(&CmpExpr{
		Left:  &ColRef{Table: "events", Column: "n"},
		Op:    "=",
		Right: &Lit{Value: "1"},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 1 {
		t.Errorf("CmpExpr: expected 1 ref, got %d", len(refs))
	}

	// AndNode
	refs = nil
	walkForOuterRefs(&AndNode{
		Left:  &ColRef{Table: "events", Column: "o"},
		Right: &ColRef{Table: "events", Column: "p"},
	}, outer, inner, outerCols, &refs)
	if len(refs) != 2 {
		t.Errorf("AndNode: expected 2 refs, got %d", len(refs))
	}

	// Unqualified column reference resolved via outerCols
	refs = nil
	walkForOuterRefs(&ColRef{Column: "status"}, outer, inner, outerCols, &refs)
	if len(refs) != 1 || refs[0].Table != "events" {
		t.Errorf("unqualified ColRef: expected ref to events.status, got %+v", refs)
	}

	// nil node
	refs = nil
	walkForOuterRefs(nil, outer, inner, outerCols, &refs)
	if len(refs) != 0 {
		t.Error("nil node should produce no refs")
	}
}

// --- nodeTag: cover via type assertions ---

func TestNodeTag_AllTypes(t *testing.T) {
	// Exercise nodeTag() on every AST node type by calling String()
	// (nodeTag returns nothing but ensures each type satisfies the Node interface)
	nodes := []Node{
		&ColRef{Column: "id"},
		&Lit{Value: "1", Kind: LitNumber},
		&StarNode{},
		&BinaryOp{Left: &Lit{Value: "1"}, Op: "+", Right: &Lit{Value: "2"}},
		&UnaryOp{Op: "-", Inner: &Lit{Value: "1"}},
		&CmpExpr{Left: &Lit{Value: "1"}, Op: "=", Right: &Lit{Value: "1"}},
		&InExpr{Left: &Lit{Value: "1"}, Values: []Node{&Lit{Value: "2"}}},
		&BetweenExpr{Left: &Lit{Value: "1"}, Low: &Lit{Value: "0"}, High: &Lit{Value: "2"}},
		&LikeExpr{Left: &Lit{Value: "'a'"}, Pattern: &Lit{Value: "'%'"}},
		&IsExpr{Left: &Lit{Value: "x"}, Check: "NULL"},
		&AndNode{Left: &Lit{Value: "true"}, Right: &Lit{Value: "false"}},
		&OrNode{Left: &Lit{Value: "true"}, Right: &Lit{Value: "false"}},
		&NotNode{Inner: &Lit{Value: "true"}},
		&ParenNode{Inner: &Lit{Value: "1"}},
		&FuncCallNode{Name: "count", Star: true},
		&CaseNode{Whens: []WhenClause{{Cond: &Lit{Value: "true"}, Result: &Lit{Value: "1"}}}},
		&CastNode{Inner: &Lit{Value: "1"}, TypeName: "int"},
		&ArrayLitNode{Elements: []Node{&Lit{Value: "1"}}},
		&SubqueryNode{SQL: "SELECT 1"},
		&ExistsNode{SQL: "SELECT 1"},
		&WindowFuncNode{Func: &FuncCallNode{Name: "row_number"}},
	}
	for _, n := range nodes {
		// Call nodeTag() to exercise the method and String() to verify it works
		n.nodeTag()
		s := n.String()
		if s == "" {
			t.Errorf("String() returned empty for %T", n)
		}
	}
}

// --- Parser: DML statements ---

func TestParse_Update(t *testing.T) {
	parsed, err := Parse("UPDATE events SET status = 'inactive' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryUpdate {
		t.Errorf("expected type QueryUpdate, got %v", parsed.Type)
	}
}

func TestParse_Delete(t *testing.T) {
	parsed, err := Parse("DELETE FROM events WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryDelete {
		t.Errorf("expected type QueryDelete, got %v", parsed.Type)
	}
}

func TestParse_Insert(t *testing.T) {
	parsed, err := Parse("INSERT INTO events (id, name) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryInsert {
		t.Errorf("expected type QueryInsert, got %v", parsed.Type)
	}
}

func TestParse_InsertMultipleValues(t *testing.T) {
	parsed, err := Parse("INSERT INTO events (id, name) VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryInsert {
		t.Errorf("expected type QueryInsert, got %v", parsed.Type)
	}
}

// --- Parser: DDL statements ---

func TestParse_CreateTable(t *testing.T) {
	parsed, err := Parse("CREATE TABLE events (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryCreateTable {
		t.Errorf("expected type QueryCreateTable, got %v", parsed.Type)
	}
}

func TestParse_DropTable(t *testing.T) {
	parsed, err := Parse("DROP TABLE events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryDropTable {
		t.Errorf("expected type QueryDropTable, got %v", parsed.Type)
	}
}

func TestParse_DropTableIfExists(t *testing.T) {
	parsed, err := Parse("DROP TABLE IF EXISTS events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryDropTable {
		t.Errorf("expected type QueryDropTable, got %v", parsed.Type)
	}
}

// --- Parser: SHOW statements ---

func TestParse_ShowTables(t *testing.T) {
	parsed, err := Parse("SHOW TABLES")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryShowTables {
		t.Errorf("expected type QueryShowTables, got %v", parsed.Type)
	}
}

func TestParse_ShowColumns(t *testing.T) {
	parsed, err := Parse("SHOW COLUMNS FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryDescribe {
		t.Errorf("expected type QueryDescribe, got %v", parsed.Type)
	}
}

// --- Parser: EXPLAIN ---

func TestParse_Explain(t *testing.T) {
	parsed, err := Parse("EXPLAIN SELECT id FROM events")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if parsed.Type != QueryExplain {
		t.Errorf("expected type QueryExplain, got %v", parsed.Type)
	}
}

// --- Parser: multiplication/division operators ---

func TestParseExpression_Multiply(t *testing.T) {
	expr, err := ParseExpression("a * b")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	bo, ok := expr.(*BinaryOp)
	if !ok {
		t.Fatalf("expected BinaryOp, got %T", expr)
	}
	if bo.Op != "*" {
		t.Errorf("expected op '*', got %q", bo.Op)
	}
}

func TestParseExpression_Divide(t *testing.T) {
	expr, err := ParseExpression("a / b")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	bo, ok := expr.(*BinaryOp)
	if !ok {
		t.Fatalf("expected BinaryOp, got %T", expr)
	}
	if bo.Op != "/" {
		t.Errorf("expected op '/', got %q", bo.Op)
	}
}

func TestParseExpression_Modulo(t *testing.T) {
	expr, err := ParseExpression("a % b")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	bo, ok := expr.(*BinaryOp)
	if !ok {
		t.Fatalf("expected BinaryOp, got %T", expr)
	}
	if bo.Op != "%" {
		t.Errorf("expected op '%%', got %q", bo.Op)
	}
}

// --- Parser: comparison operators ---

func TestParseExpression_ComparisonOps(t *testing.T) {
	for _, tc := range []struct {
		input string
		op    string
	}{
		{"a > b", ">"},
		{"a < b", "<"},
		{"a >= b", ">="},
		{"a <= b", "<="},
		{"a != b", "!="},
		{"a <> b", "!="},
	} {
		expr, err := ParseExpression(tc.input)
		if err != nil {
			t.Fatalf("ParseExpression(%q): %v", tc.input, err)
		}
		cmp, ok := expr.(*CmpExpr)
		if !ok {
			t.Errorf("ParseExpression(%q): expected CmpExpr, got %T", tc.input, expr)
			continue
		}
		if cmp.Op != tc.op {
			t.Errorf("ParseExpression(%q): expected op %q, got %q", tc.input, tc.op, cmp.Op)
		}
	}
}

// --- Parser: dot notation ---

func TestParseExpression_DotNotation(t *testing.T) {
	expr, err := ParseExpression("t.col")
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}
	cr, ok := expr.(*ColRef)
	if !ok {
		t.Fatalf("expected ColRef, got %T", expr)
	}
	if cr.Table != "t" || cr.Column != "col" {
		t.Errorf("expected t.col, got %s.%s", cr.Table, cr.Column)
	}
}

// --- Parser: GROUPING SETS ---

func TestParse_GroupingSets(t *testing.T) {
	parsed, err := Parse("SELECT a, b, SUM(c) FROM events GROUP BY GROUPING SETS ((a, b), (a), ())")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.GroupingSets) == 0 {
		t.Error("expected GroupingSets to be populated")
	}
}

func TestParse_Rollup(t *testing.T) {
	parsed, err := Parse("SELECT a, b, SUM(c) FROM events GROUP BY ROLLUP(a, b)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.GroupingSets) == 0 {
		t.Error("expected GroupingSets to be populated for ROLLUP")
	}
}

func TestParse_Cube(t *testing.T) {
	parsed, err := Parse("SELECT a, b, SUM(c) FROM events GROUP BY CUBE(a, b)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.GroupingSets) == 0 {
		t.Error("expected GroupingSets to be populated for CUBE")
	}
}

// --- FindNestedAggregate: cover ArrayLitNode and WindowFuncNode branches ---

func TestFindNestedAggregate_ArrayLit(t *testing.T) {
	// ArrayLitNode is not handled by FindNestedAggregate (falls through to default)
	inner := &FuncCallNode{Name: "count", Star: true}
	arr := &ArrayLitNode{Elements: []Node{inner}}
	result := FindNestedAggregate(arr)
	if result != nil {
		t.Error("expected nil since FindNestedAggregate does not recurse into ArrayLitNode")
	}
}

func TestFindNestedAggregate_CastNode(t *testing.T) {
	inner := &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}}
	cast := &CastNode{Inner: inner, TypeName: "float"}
	result := FindNestedAggregate(cast)
	if result == nil {
		t.Error("expected to find nested aggregate in CastNode")
	}
	if result.Name != "sum" {
		t.Errorf("expected aggregate name 'sum', got %q", result.Name)
	}
}

// --- ReplaceAllAggregates: cover more branches ---

func TestReplaceAllAggregates_UnaryOp(t *testing.T) {
	inner := &FuncCallNode{Name: "count", Star: true}
	unary := &UnaryOp{Op: "-", Inner: inner}
	replacements := map[string]string{
		strings.ToLower(inner.String()): "count_col",
	}
	replaced := ReplaceAllAggregates(unary, replacements)
	ru, ok := replaced.(*UnaryOp)
	if !ok {
		t.Fatalf("expected UnaryOp, got %T", replaced)
	}
	if cr, ok := ru.Inner.(*ColRef); !ok || cr.Column != "count_col" {
		t.Errorf("expected inner to be replaced ColRef, got %T", ru.Inner)
	}
}

func TestReplaceAllAggregates_ParenNode(t *testing.T) {
	inner := &FuncCallNode{Name: "sum", Args: []Node{&ColRef{Column: "x"}}}
	paren := &ParenNode{Inner: inner}
	replacements := map[string]string{
		strings.ToLower(inner.String()): "sum_col",
	}
	replaced := ReplaceAllAggregates(paren, replacements)
	rp, ok := replaced.(*ParenNode)
	if !ok {
		t.Fatalf("expected ParenNode, got %T", replaced)
	}
	if cr, ok := rp.Inner.(*ColRef); !ok || cr.Column != "sum_col" {
		t.Errorf("expected inner to be replaced ColRef, got %T", rp.Inner)
	}
}

func TestReplaceAllAggregates_CastNode(t *testing.T) {
	inner := &FuncCallNode{Name: "avg", Args: []Node{&ColRef{Column: "x"}}}
	cast := &CastNode{Inner: inner, TypeName: "float"}
	replacements := map[string]string{
		strings.ToLower(inner.String()): "avg_col",
	}
	replaced := ReplaceAllAggregates(cast, replacements)
	rc, ok := replaced.(*CastNode)
	if !ok {
		t.Fatalf("expected CastNode, got %T", replaced)
	}
	if cr, ok := rc.Inner.(*ColRef); !ok || cr.Column != "avg_col" {
		t.Errorf("expected inner to be replaced ColRef, got %T", rc.Inner)
	}
}

func TestReplaceAllAggregates_CaseNode(t *testing.T) {
	inner := &FuncCallNode{Name: "max", Args: []Node{&ColRef{Column: "x"}}}
	cn := &CaseNode{
		Whens: []WhenClause{
			{Cond: &Lit{Value: "true", Kind: LitBool}, Result: inner},
		},
		Else: &FuncCallNode{Name: "min", Args: []Node{&ColRef{Column: "y"}}},
	}
	replacements := map[string]string{
		strings.ToLower(inner.String()):    "max_col",
		strings.ToLower(cn.Else.String()): "min_col",
	}
	replaced := ReplaceAllAggregates(cn, replacements)
	rc, ok := replaced.(*CaseNode)
	if !ok {
		t.Fatalf("expected CaseNode, got %T", replaced)
	}
	if cr, ok := rc.Whens[0].Result.(*ColRef); !ok || cr.Column != "max_col" {
		t.Errorf("expected When result replaced, got %T", rc.Whens[0].Result)
	}
}

// --- collectInnerTables: ensure inner tables recognized correctly ---

func TestFindCorrelatedRefs_InnerTableNotOuter(t *testing.T) {
	sql := "SELECT x.id FROM orders x WHERE x.id = y.id"
	outer := map[string]bool{"y": true}
	refs, err := FindCorrelatedRefs(sql, outer)
	if err != nil {
		t.Fatalf("FindCorrelatedRefs: %v", err)
	}
	// y.id should be an outer ref since y is in outer
	found := false
	for _, r := range refs {
		if r.Table == "y" {
			found = true
		}
	}
	if !found {
		t.Error("expected to find outer ref to table y")
	}
}

// --- RebuildSQL: more branch coverage ---

func TestRebuildSQL_WithGroupBy(t *testing.T) {
	parsed, err := Parse("SELECT status, COUNT(*) FROM events GROUP BY status HAVING COUNT(*) > 1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	result := RebuildSQL(info, nil)
	upper := strings.ToUpper(result)
	if !strings.Contains(upper, "GROUP BY") {
		t.Error("expected GROUP BY in rebuilt SQL")
	}
	if !strings.Contains(upper, "HAVING") {
		t.Error("expected HAVING in rebuilt SQL")
	}
}

func TestRebuildSQL_WithLimit(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	result := RebuildSQL(info, nil)
	upper := strings.ToUpper(result)
	if !strings.Contains(upper, "LIMIT") {
		t.Error("expected LIMIT in rebuilt SQL")
	}
	if !strings.Contains(upper, "ORDER BY") {
		t.Error("expected ORDER BY in rebuilt SQL")
	}
}
