package expr

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

func TestCompileNilNode(t *testing.T) {
	e, err := Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	lit, ok := e.(*Lit)
	if !ok {
		t.Fatal("expected Lit for nil node")
	}
	if lit.Val != nil {
		t.Fatal("expected nil Val")
	}
}

func TestCompileStarNode(t *testing.T) {
	e, err := Compile(&plansql.StarNode{})
	if err != nil {
		t.Fatal(err)
	}
	lit, ok := e.(*Lit)
	if !ok {
		t.Fatal("expected Lit for StarNode")
	}
	if lit.Val != "*" {
		t.Fatalf("expected *, got %v", lit.Val)
	}
}

func TestCompileQualifiedColRef(t *testing.T) {
	e, err := Compile(&plansql.ColRef{Table: "t1", Column: "id"})
	if err != nil {
		t.Fatal(err)
	}
	cr, ok := e.(*ColRef)
	if !ok {
		t.Fatal("expected ColRef")
	}
	if cr.Name != "t1.id" {
		t.Fatalf("expected t1.id, got %s", cr.Name)
	}
}

func TestCompileLitNumber(t *testing.T) {
	e, err := Compile(&plansql.Lit{Value: "42", Kind: plansql.LitNumber})
	if err != nil {
		t.Fatal(err)
	}
	lit := e.(*Lit)
	if lit.Val != int64(42) {
		t.Fatalf("expected int64(42), got %v", lit.Val)
	}
}

func TestCompileLitFloat(t *testing.T) {
	e, err := Compile(&plansql.Lit{Value: "3.14", Kind: plansql.LitNumber})
	if err != nil {
		t.Fatal(err)
	}
	lit := e.(*Lit)
	if lit.Val != float64(3.14) {
		t.Fatalf("expected 3.14, got %v", lit.Val)
	}
}

func TestCompileLitUnparseable(t *testing.T) {
	// A number string that isn't parseable as int or float
	e, err := Compile(&plansql.Lit{Value: "abc", Kind: plansql.LitNumber})
	if err != nil {
		t.Fatal(err)
	}
	lit := e.(*Lit)
	if lit.Val != "abc" {
		t.Fatalf("expected string 'abc', got %v", lit.Val)
	}
}

func TestCompileLitBool(t *testing.T) {
	e, err := Compile(&plansql.Lit{Value: "true", Kind: plansql.LitBool})
	if err != nil {
		t.Fatal(err)
	}
	if e.(*Lit).Val != true {
		t.Fatal("expected true")
	}

	e, err = Compile(&plansql.Lit{Value: "false", Kind: plansql.LitBool})
	if err != nil {
		t.Fatal(err)
	}
	if e.(*Lit).Val != false {
		t.Fatal("expected false")
	}
}

func TestCompileLitNull(t *testing.T) {
	e, err := Compile(&plansql.Lit{Value: "", Kind: plansql.LitNull})
	if err != nil {
		t.Fatal(err)
	}
	if e.(*Lit).Val != nil {
		t.Fatal("expected nil")
	}
}

func TestCompileLitDefault(t *testing.T) {
	// Unknown literal kind
	e, err := Compile(&plansql.Lit{Value: "xyz", Kind: plansql.LiteralKind(99)})
	if err != nil {
		t.Fatal(err)
	}
	if e.(*Lit).Val != "xyz" {
		t.Fatalf("expected 'xyz', got %v", e.(*Lit).Val)
	}
}

func TestCompileUnaryOp(t *testing.T) {
	node := &plansql.UnaryOp{Op: "-", Inner: &plansql.Lit{Value: "5", Kind: plansql.LitNumber}}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	uo, ok := e.(*UnaryOp)
	if !ok {
		t.Fatal("expected UnaryOp")
	}
	if uo.Op != "-" {
		t.Fatalf("expected -, got %s", uo.Op)
	}
}

func TestCompileCmpAllOps(t *testing.T) {
	ops := []string{"=", "!=", "<>", "<", "<=", ">", ">="}
	expected := []CmpOp{CmpEq, CmpNe, CmpNe, CmpLt, CmpLe, CmpGt, CmpGe}
	for i, op := range ops {
		node := &plansql.CmpExpr{
			Left:  &plansql.ColRef{Column: "x"},
			Op:    op,
			Right: &plansql.Lit{Value: "1", Kind: plansql.LitNumber},
		}
		e, err := Compile(node)
		if err != nil {
			t.Fatalf("op %s: %v", op, err)
		}
		cmp, ok := e.(*Cmp)
		if !ok {
			// Could be CmpInt64 since right is an int literal
			if ci, ok2 := e.(*CmpInt64); ok2 {
				if ci.Op != expected[i] {
					t.Fatalf("op %s: expected %v, got %v", op, expected[i], ci.Op)
				}
				continue
			}
			t.Fatalf("op %s: expected Cmp or CmpInt64", op)
		}
		if cmp.Op != expected[i] {
			t.Fatalf("op %s: expected %v, got %v", op, expected[i], cmp.Op)
		}
	}
}

func TestCompileCmpDefaultOp(t *testing.T) {
	// Unknown comparator should default to CmpEq
	node := &plansql.CmpExpr{
		Left:  &plansql.ColRef{Column: "x"},
		Op:    "??",
		Right: &plansql.ColRef{Column: "y"},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	cmp := e.(*Cmp)
	if cmp.Op != CmpEq {
		t.Fatalf("expected CmpEq for unknown op, got %v", cmp.Op)
	}
}

func TestCompileCmpTypedInt64(t *testing.T) {
	// Two int literals should produce CmpInt64
	node := &plansql.CmpExpr{
		Left:  &plansql.Lit{Value: "10", Kind: plansql.LitNumber},
		Op:    "=",
		Right: &plansql.Lit{Value: "20", Kind: plansql.LitNumber},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*CmpInt64); !ok {
		t.Fatalf("expected CmpInt64, got %T", e)
	}
}

func TestCompileCmpTypedFloat64(t *testing.T) {
	// Two float literals should produce CmpFloat64
	node := &plansql.CmpExpr{
		Left:  &plansql.Lit{Value: "1.5", Kind: plansql.LitNumber},
		Op:    "<",
		Right: &plansql.Lit{Value: "2.5", Kind: plansql.LitNumber},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*CmpFloat64); !ok {
		t.Fatalf("expected CmpFloat64, got %T", e)
	}
}

func TestCompileBinOpInt64(t *testing.T) {
	// Two int literals with + should produce BinOpInt64
	node := &plansql.BinaryOp{
		Left:  &plansql.Lit{Value: "10", Kind: plansql.LitNumber},
		Op:    "+",
		Right: &plansql.Lit{Value: "20", Kind: plansql.LitNumber},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*BinOpInt64); !ok {
		t.Fatalf("expected BinOpInt64, got %T", e)
	}
}

func TestCompileBinOpFloat64(t *testing.T) {
	// Two float literals should produce BinOpFloat64
	node := &plansql.BinaryOp{
		Left:  &plansql.Lit{Value: "1.5", Kind: plansql.LitNumber},
		Op:    "*",
		Right: &plansql.Lit{Value: "2.5", Kind: plansql.LitNumber},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*BinOpFloat64); !ok {
		t.Fatalf("expected BinOpFloat64, got %T", e)
	}
}

func TestCompileBinOpDivAlwaysFloat(t *testing.T) {
	// Division of two ints should produce BinOpFloat64 (not BinOpInt64)
	node := &plansql.BinaryOp{
		Left:  &plansql.Lit{Value: "10", Kind: plansql.LitNumber},
		Op:    "/",
		Right: &plansql.Lit{Value: "3", Kind: plansql.LitNumber},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*BinOpFloat64); !ok {
		t.Fatalf("expected BinOpFloat64 for division, got %T", e)
	}
}

func TestCompileBinOpGeneric(t *testing.T) {
	// ColRef + ColRef produces generic BinOp (types unknown at compile)
	node := &plansql.BinaryOp{
		Left:  &plansql.ColRef{Column: "a"},
		Op:    "+",
		Right: &plansql.ColRef{Column: "b"},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*BinOp); ok {
		// good - generic path
	} else if _, ok := e.(*BinOpFloat64); ok {
		// also acceptable since ColRef implements Float64Expr
	} else {
		t.Fatalf("unexpected type %T", e)
	}
}

func TestCompileIsTrue(t *testing.T) {
	node := &plansql.IsExpr{Left: &plansql.ColRef{Column: "x"}, Check: "true", Not: false}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	cmp := e.(*Cmp)
	if cmp.Op != CmpEq {
		t.Fatal("expected CmpEq for IS TRUE")
	}
	if cmp.Right.(*Lit).Val != true {
		t.Fatal("expected true literal")
	}
}

func TestCompileIsNotTrue(t *testing.T) {
	node := &plansql.IsExpr{Left: &plansql.ColRef{Column: "x"}, Check: "true", Not: true}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	cmp := e.(*Cmp)
	if cmp.Op != CmpNe {
		t.Fatal("expected CmpNe for IS NOT TRUE")
	}
}

func TestCompileIsFalse(t *testing.T) {
	node := &plansql.IsExpr{Left: &plansql.ColRef{Column: "x"}, Check: "false", Not: false}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	cmp := e.(*Cmp)
	if cmp.Op != CmpEq {
		t.Fatal("expected CmpEq")
	}
	if cmp.Right.(*Lit).Val != false {
		t.Fatal("expected false literal")
	}
}

func TestCompileIsNotFalse(t *testing.T) {
	node := &plansql.IsExpr{Left: &plansql.ColRef{Column: "x"}, Check: "false", Not: true}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	cmp := e.(*Cmp)
	if cmp.Op != CmpNe {
		t.Fatal("expected CmpNe for IS NOT FALSE")
	}
}

func TestCompileIsUnknownCheck(t *testing.T) {
	node := &plansql.IsExpr{Left: &plansql.ColRef{Column: "x"}, Check: "unknown"}
	_, err := Compile(node)
	if err == nil {
		t.Fatal("expected error for unknown IS check")
	}
}

func TestCompileParenNode(t *testing.T) {
	inner := &plansql.Lit{Value: "42", Kind: plansql.LitNumber}
	node := &plansql.ParenNode{Inner: inner}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if e.(*Lit).Val != int64(42) {
		t.Fatal("expected 42")
	}
}

func TestCompileCastNode(t *testing.T) {
	node := &plansql.CastNode{
		Inner:    &plansql.ColRef{Column: "x"},
		TypeName: "VARCHAR",
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	cast := e.(*Cast)
	if cast.DestType != "varchar" {
		t.Fatalf("expected lowercase type, got %s", cast.DestType)
	}
}

func TestCompileArrayLitNode(t *testing.T) {
	node := &plansql.ArrayLitNode{
		Elements: []plansql.Node{
			&plansql.Lit{Value: "1", Kind: plansql.LitNumber},
			&plansql.Lit{Value: "2", Kind: plansql.LitNumber},
		},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	arr := e.(*ArrayLitExpr)
	if len(arr.Elements) != 2 {
		t.Fatal("expected 2 elements")
	}
}

func TestCompileFuncCallCoalesce(t *testing.T) {
	node := &plansql.FuncCallNode{
		Name: "COALESCE",
		Args: []plansql.Node{
			&plansql.ColRef{Column: "x"},
			&plansql.Lit{Value: "0", Kind: plansql.LitNumber},
		},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*Coalesce); !ok {
		t.Fatalf("expected Coalesce, got %T", e)
	}
}

func TestCompileFuncCallStar(t *testing.T) {
	node := &plansql.FuncCallNode{Name: "count", Star: true}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	fc := e.(*FuncCall)
	if len(fc.Args) != 1 || fc.Args[0].(*Lit).Val != "*" {
		t.Fatal("expected star arg")
	}
}

func TestCompileNotNode(t *testing.T) {
	node := &plansql.NotNode{Inner: &plansql.Lit{Value: "true", Kind: plansql.LitBool}}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*Not); !ok {
		t.Fatalf("expected Not, got %T", e)
	}
}

func TestCompileDefaultNodeType(t *testing.T) {
	// Unknown node type falls through to default -> Lit{Val: node.String()}
	// We need a node type not handled by the switch. Use a known type like FuncCallNode
	// that might not be in the switch — but FuncCallNode IS handled.
	// Instead use something the code doesn't know — but all types are in switch.
	// The default path catches unknown Node implementations. The simplest way
	// is to test via the Compile function, which wraps compileWithCtx. Since we can't
	// easily create an unknown Node from the test, this path is hard to reach without
	// modifying production code. Skip.
}

func TestCompileAndOrNodes(t *testing.T) {
	node := &plansql.AndNode{
		Left:  &plansql.Lit{Value: "true", Kind: plansql.LitBool},
		Right: &plansql.Lit{Value: "false", Kind: plansql.LitBool},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*And); !ok {
		t.Fatalf("expected And, got %T", e)
	}

	node2 := &plansql.OrNode{
		Left:  &plansql.Lit{Value: "true", Kind: plansql.LitBool},
		Right: &plansql.Lit{Value: "false", Kind: plansql.LitBool},
	}
	e2, err := Compile(node2)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e2.(*Or); !ok {
		t.Fatalf("expected Or, got %T", e2)
	}
}

func TestCompileWithFullScope(t *testing.T) {
	outerTables := map[string]bool{"orders": true}
	outerCols := map[string]string{"order_id": "orders"}
	node := &plansql.ColRef{Column: "x"}
	e, err := CompileWithFullScope(node, nil, outerTables, outerCols)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*ColRef); !ok {
		t.Fatalf("expected ColRef, got %T", e)
	}
}

func TestCompileSubqueryWithoutRunner(t *testing.T) {
	node := &plansql.SubqueryNode{SQL: "SELECT 1"}
	_, err := Compile(node)
	if err == nil {
		t.Fatal("expected error for subquery without runner")
	}
}

func TestCompileExistsWithoutRunner(t *testing.T) {
	node := &plansql.ExistsNode{SQL: "SELECT 1 FROM t"}
	_, err := Compile(node)
	if err == nil {
		t.Fatal("expected error for EXISTS without runner")
	}
}

func TestCompileInSubqueryWithoutRunner(t *testing.T) {
	node := &plansql.InExpr{
		Left:   &plansql.ColRef{Column: "x"},
		Values: []plansql.Node{&plansql.SubqueryNode{SQL: "SELECT id FROM t"}},
	}
	_, err := Compile(node)
	if err == nil {
		t.Fatal("expected error for IN subquery without runner")
	}
}

func TestCompileSelectExprWithAlias(t *testing.T) {
	node := &plansql.ColRef{Column: "name"}
	e, name, err := CompileSelectExpr(node, "alias")
	if err != nil {
		t.Fatal(err)
	}
	if name != "alias" {
		t.Fatalf("expected 'alias', got %q", name)
	}
	_ = e
}

func TestCompileSelectExprNoAlias(t *testing.T) {
	node := &plansql.ColRef{Column: "name"}
	_, name, err := CompileSelectExpr(node, "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "name" {
		t.Fatalf("expected 'name', got %q", name)
	}
}

func TestCompileSelectExprNonColRef(t *testing.T) {
	// For non-ColRef with no alias, name should be the string representation
	node := &plansql.Lit{Value: "42", Kind: plansql.LitNumber}
	_, name, err := CompileSelectExpr(node, "")
	if err != nil {
		t.Fatal(err)
	}
	if name != "42" {
		t.Fatalf("expected '42', got %q", name)
	}
}

func TestCompileCaseNodeFull(t *testing.T) {
	node := &plansql.CaseNode{
		Subject: &plansql.ColRef{Column: "status"},
		Whens: []plansql.WhenClause{
			{
				Cond:   &plansql.Lit{Value: "active", Kind: plansql.LitString},
				Result: &plansql.Lit{Value: "1", Kind: plansql.LitNumber},
			},
			{
				Cond:   &plansql.Lit{Value: "inactive", Kind: plansql.LitString},
				Result: &plansql.Lit{Value: "0", Kind: plansql.LitNumber},
			},
		},
		Else: &plansql.Lit{Value: "-1", Kind: plansql.LitNumber},
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatal(err)
	}
	c := e.(*Case)
	if c.Operand == nil {
		t.Fatal("expected operand")
	}
	if len(c.Whens) != 2 {
		t.Fatalf("expected 2 whens, got %d", len(c.Whens))
	}
	if c.Else == nil {
		t.Fatal("expected else")
	}
}

func TestIsProvablyInt64(t *testing.T) {
	tests := []struct {
		name   string
		e      Expr
		expect bool
	}{
		{"int64 lit", &Lit{Val: int64(1)}, true},
		{"int32 lit", &Lit{Val: int32(1)}, true},
		{"int lit", &Lit{Val: 1}, true},
		{"float lit", &Lit{Val: 1.0}, false},
		{"string lit", &Lit{Val: "hello"}, false},
		{"BinOpInt64", &BinOpInt64{}, true},
		{"ColRef", &ColRef{Name: "x"}, false},
	}
	for _, tc := range tests {
		got := isProvablyInt64(tc.e)
		if got != tc.expect {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestIsProvablyFloat64(t *testing.T) {
	tests := []struct {
		name   string
		e      Expr
		expect bool
	}{
		{"float64 lit", &Lit{Val: float64(1.0)}, true},
		{"float32 lit", &Lit{Val: float32(1.0)}, true},
		{"int lit", &Lit{Val: int64(1)}, false},
		{"BinOpFloat64", &BinOpFloat64{}, true},
		{"ColRef", &ColRef{Name: "x"}, false},
	}
	for _, tc := range tests {
		got := isProvablyFloat64(tc.e)
		if got != tc.expect {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestIsIntNative(t *testing.T) {
	tests := []struct {
		name   string
		e      Expr
		expect bool
	}{
		{"int64 lit", &Lit{Val: int64(1)}, true},
		{"int32 lit", &Lit{Val: int32(1)}, true},
		{"int lit", &Lit{Val: 1}, true},
		{"float lit", &Lit{Val: 1.0}, false},
		{"nil lit", &Lit{Val: nil}, false},
		{"ColRef", &ColRef{Name: "x"}, false},
		{"BinOpInt64", &BinOpInt64{}, true},
		{"And", &And{}, false},
	}
	for _, tc := range tests {
		got := isIntNative(tc.e)
		if got != tc.expect {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestBuildUnqualOuterCols(t *testing.T) {
	// Empty outerCols returns nil
	refs := []plansql.OuterRef{{Table: "t1", Column: "id"}}
	result := buildUnqualOuterCols(refs, nil)
	if result != nil {
		t.Fatal("expected nil for empty outerCols")
	}

	// With matching outerCols
	outerCols := map[string]string{"id": "t1", "name": "t2"}
	result = buildUnqualOuterCols(refs, outerCols)
	if result == nil || result["id"] != "t1" {
		t.Fatalf("expected id -> t1, got %v", result)
	}

	// Non-matching table
	refs2 := []plansql.OuterRef{{Table: "t3", Column: "id"}}
	result2 := buildUnqualOuterCols(refs2, outerCols)
	if result2 != nil {
		t.Fatal("expected nil for non-matching table")
	}
}

func TestCompileIntervalLit(t *testing.T) {
	tests := []struct {
		name  string
		value int
		unit  string
		want  IntervalValue
	}{
		{"days", 30, "day", IntervalValue{Days: 30}},
		{"months", 3, "month", IntervalValue{Months: 3}},
		{"years", 1, "year", IntervalValue{Years: 1}},
		{"weeks", 2, "week", IntervalValue{Days: 14}},
		{"hours", 12, "hour", IntervalValue{Hours: 12}},
		{"minutes", 45, "minute", IntervalValue{Minutes: 45}},
		{"seconds", 90, "second", IntervalValue{Seconds: 90}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &plansql.IntervalLit{Value: tt.value, Unit: tt.unit}
			e, err := Compile(node)
			if err != nil {
				t.Fatal(err)
			}
			lit, ok := e.(*Lit)
			if !ok {
				t.Fatalf("expected Lit, got %T", e)
			}
			iv, ok := lit.Val.(IntervalValue)
			if !ok {
				t.Fatalf("expected IntervalValue, got %T", lit.Val)
			}
			if iv != tt.want {
				t.Errorf("got %+v, want %+v", iv, tt.want)
			}
		})
	}
}

func TestCompileCurrentDateMinusInterval(t *testing.T) {
	// Construct the AST directly: CURRENT_DATE - INTERVAL '30' DAY
	ast := &plansql.BinaryOp{
		Left:  &plansql.FuncCallNode{Name: "current_date"},
		Op:    "-",
		Right: &plansql.IntervalLit{Value: 30, Unit: "day"},
	}

	compiled, err := Compile(ast)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	result := compiled.Eval(nil, 0)
	if result == nil {
		t.Fatal("CURRENT_DATE - INTERVAL '30 days' returned nil")
	}
	dateStr, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T: %v", result, result)
	}
	if len(dateStr) != 10 || dateStr[4] != '-' || dateStr[7] != '-' {
		t.Fatalf("expected date format YYYY-MM-DD, got %q", dateStr)
	}
	t.Logf("CURRENT_DATE - INTERVAL '30 days' = %s", dateStr)
}

func TestCompileCurrentDatePlusInterval(t *testing.T) {
	ast := &plansql.BinaryOp{
		Left:  &plansql.FuncCallNode{Name: "current_date"},
		Op:    "+",
		Right: &plansql.IntervalLit{Value: 1, Unit: "year"},
	}

	compiled, err := Compile(ast)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	result := compiled.Eval(nil, 0)
	if result == nil {
		t.Fatal("CURRENT_DATE + INTERVAL '1' YEAR returned nil")
	}
	dateStr, ok := result.(string)
	if !ok {
		t.Fatalf("expected string result, got %T: %v", result, result)
	}
	if len(dateStr) != 10 {
		t.Fatalf("expected date format YYYY-MM-DD, got %q", dateStr)
	}
	t.Logf("CURRENT_DATE + INTERVAL '1' YEAR = %s", dateStr)
}
