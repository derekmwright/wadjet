package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// helper: create a batch with named columns
func testBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
		{Name: "active", Type: parquet.TypeBool},
	}
	b := batch.NewRecordBatch(schema, 3)
	// Row 0: id=1, name="alice", amount=100.5, active=true
	b.Columns[0].SetValue(0, int64(1))
	b.Columns[1].SetValue(0, "alice")
	b.Columns[2].SetValue(0, 100.5)
	b.Columns[3].SetValue(0, true)
	// Row 1: id=2, name="bob", amount=200.0, active=false
	b.Columns[0].SetValue(1, int64(2))
	b.Columns[1].SetValue(1, "bob")
	b.Columns[2].SetValue(1, 200.0)
	b.Columns[3].SetValue(1, false)
	// Row 2: id=3, name="carol", amount=50.25, active=true
	b.Columns[0].SetValue(2, int64(3))
	b.Columns[1].SetValue(2, "carol")
	b.Columns[2].SetValue(2, 50.25)
	b.Columns[3].SetValue(2, true)
	return b
}

func TestColRef(t *testing.T) {
	b := testBatch()
	e := &ColRef{Name: "name"}
	if v := e.Eval(b, 0); v != "alice" {
		t.Fatalf("expected alice, got %v", v)
	}
	if v := e.Eval(b, 1); v != "bob" {
		t.Fatalf("expected bob, got %v", v)
	}
}

func TestLiteral(t *testing.T) {
	b := testBatch()
	e := &Lit{Val: int64(42)}
	if v := e.Eval(b, 0); v != int64(42) {
		t.Fatalf("expected 42, got %v", v)
	}
}

func TestBinOp(t *testing.T) {
	b := testBatch()
	// amount + 10
	e := &BinOp{
		Left:  &ColRef{Name: "amount"},
		Right: &Lit{Val: float64(10)},
		Op:    "+",
	}
	if v := e.Eval(b, 0); v != 110.5 {
		t.Fatalf("expected 110.5, got %v", v)
	}
	// amount * 2
	e2 := &BinOp{
		Left:  &ColRef{Name: "amount"},
		Right: &Lit{Val: float64(2)},
		Op:    "*",
	}
	if v := e2.Eval(b, 1); v != 400.0 {
		t.Fatalf("expected 400, got %v", v)
	}
	// division by zero
	e3 := &BinOp{
		Left:  &ColRef{Name: "amount"},
		Right: &Lit{Val: float64(0)},
		Op:    "/",
	}
	if v := e3.Eval(b, 0); v != nil {
		t.Fatalf("expected nil for div by zero, got %v", v)
	}
}

func TestComparison(t *testing.T) {
	b := testBatch()
	// id > 1
	e := &Cmp{
		Left:  &ColRef{Name: "id"},
		Right: &Lit{Val: int64(1)},
		Op:    CmpGt,
	}
	if e.EvalBool(b, 0) {
		t.Fatal("1 > 1 should be false")
	}
	if !e.EvalBool(b, 1) {
		t.Fatal("2 > 1 should be true")
	}
}

func TestAndOr(t *testing.T) {
	b := testBatch()
	// id > 1 AND amount < 100
	e := &And{
		Left:  &Cmp{Left: &ColRef{Name: "id"}, Right: &Lit{Val: int64(1)}, Op: CmpGt},
		Right: &Cmp{Left: &ColRef{Name: "amount"}, Right: &Lit{Val: float64(100)}, Op: CmpLt},
	}
	if e.EvalBool(b, 0) {
		t.Fatal("row 0: should be false (id=1)")
	}
	if e.EvalBool(b, 1) {
		t.Fatal("row 1: should be false (amount=200)")
	}
	if !e.EvalBool(b, 2) {
		t.Fatal("row 2: should be true (id=3, amount=50.25)")
	}

	// id = 1 OR id = 3
	e2 := &Or{
		Left:  &Cmp{Left: &ColRef{Name: "id"}, Right: &Lit{Val: int64(1)}, Op: CmpEq},
		Right: &Cmp{Left: &ColRef{Name: "id"}, Right: &Lit{Val: int64(3)}, Op: CmpEq},
	}
	if !e2.EvalBool(b, 0) {
		t.Fatal("row 0: should be true (id=1)")
	}
	if e2.EvalBool(b, 1) {
		t.Fatal("row 1: should be false (id=2)")
	}
}

func TestIn(t *testing.T) {
	b := testBatch()
	e := &In{
		Expr: &ColRef{Name: "id"},
		Values: []Expr{
			&Lit{Val: int64(1)},
			&Lit{Val: int64(3)},
		},
	}
	if !e.EvalBool(b, 0) {
		t.Fatal("1 should be in (1,3)")
	}
	if e.EvalBool(b, 1) {
		t.Fatal("2 should not be in (1,3)")
	}
}

func TestBetween(t *testing.T) {
	b := testBatch()
	e := &Between{
		Expr: &ColRef{Name: "amount"},
		Low:  &Lit{Val: float64(50)},
		Hi:   &Lit{Val: float64(150)},
	}
	if !e.EvalBool(b, 0) {
		t.Fatal("100.5 should be between 50 and 150")
	}
	if e.EvalBool(b, 1) {
		t.Fatal("200 should not be between 50 and 150")
	}
}

func TestLike(t *testing.T) {
	b := testBatch()
	// name LIKE 'al%'
	e := &Like{
		Expr:    &ColRef{Name: "name"},
		Pattern: &Lit{Val: "al%"},
	}
	if !e.EvalBool(b, 0) {
		t.Fatal("alice should match al%")
	}
	if e.EvalBool(b, 1) {
		t.Fatal("bob should not match al%")
	}

	// name LIKE '_o_'
	e2 := &Like{
		Expr:    &ColRef{Name: "name"},
		Pattern: &Lit{Val: "_o_"},
	}
	if !e2.EvalBool(b, 1) {
		t.Fatal("bob should match _o_")
	}
	if e2.EvalBool(b, 0) {
		t.Fatal("alice should not match _o_")
	}
}

func TestCase(t *testing.T) {
	b := testBatch()
	// CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END
	e := &Case{
		Whens: []CaseWhen{
			{Cond: &Cmp{Left: &ColRef{Name: "id"}, Right: &Lit{Val: int64(1)}, Op: CmpEq}, Result: &Lit{Val: "one"}},
			{Cond: &Cmp{Left: &ColRef{Name: "id"}, Right: &Lit{Val: int64(2)}, Op: CmpEq}, Result: &Lit{Val: "two"}},
		},
		Else: &Lit{Val: "other"},
	}
	if v := e.Eval(b, 0); v != "one" {
		t.Fatalf("expected one, got %v", v)
	}
	if v := e.Eval(b, 1); v != "two" {
		t.Fatalf("expected two, got %v", v)
	}
	if v := e.Eval(b, 2); v != "other" {
		t.Fatalf("expected other, got %v", v)
	}
}

func TestScalarFunctions(t *testing.T) {
	b := testBatch()

	tests := []struct {
		name     string
		expr     Expr
		row      int
		expected any
	}{
		{"UPPER", &FuncCall{Name: "upper", Args: []Expr{&ColRef{Name: "name"}}}, 0, "ALICE"},
		{"LOWER", &FuncCall{Name: "lower", Args: []Expr{&Lit{Val: "HELLO"}}}, 0, "hello"},
		{"LENGTH", &FuncCall{Name: "length", Args: []Expr{&ColRef{Name: "name"}}}, 0, float64(5)},
		{"ABS", &FuncCall{Name: "abs", Args: []Expr{&Lit{Val: float64(-5)}}}, 0, float64(5)},
		{"CEIL", &FuncCall{Name: "ceil", Args: []Expr{&ColRef{Name: "amount"}}}, 0, float64(101)},
		{"FLOOR", &FuncCall{Name: "floor", Args: []Expr{&ColRef{Name: "amount"}}}, 0, float64(100)},
		{"ROUND", &FuncCall{Name: "round", Args: []Expr{&Lit{Val: float64(3.456)}, &Lit{Val: float64(1)}}}, 0, float64(3.5)},
		{"CONCAT", &FuncCall{Name: "concat", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: "_suffix"}}}, 0, "alice_suffix"},
		{"SUBSTR", &FuncCall{Name: "substr", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: float64(1)}, &Lit{Val: float64(3)}}}, 0, "ali"},
		{"TRIM", &FuncCall{Name: "trim", Args: []Expr{&Lit{Val: "  hello  "}}}, 0, "hello"},
		{"REPLACE", &FuncCall{Name: "replace", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: "ice"}, &Lit{Val: "ex"}}}, 0, "alex"},
		{"REVERSE", &FuncCall{Name: "reverse", Args: []Expr{&ColRef{Name: "name"}}}, 1, "bob"},
		{"LEFT", &FuncCall{Name: "left", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: float64(3)}}}, 0, "ali"},
		{"RIGHT", &FuncCall{Name: "right", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: float64(3)}}}, 0, "ice"},
		{"COALESCE", &Coalesce{Args: []Expr{&Lit{Val: nil}, &Lit{Val: "fallback"}}}, 0, "fallback"},
		{"NULLIF equal", &FuncCall{Name: "nullif", Args: []Expr{&Lit{Val: int64(1)}, &Lit{Val: int64(1)}}}, 0, nil},
		{"NULLIF not equal", &FuncCall{Name: "nullif", Args: []Expr{&Lit{Val: int64(1)}, &Lit{Val: int64(2)}}}, 0, int64(1)},
		{"IFNULL non-null", &FuncCall{Name: "ifnull", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: "default"}}}, 0, "alice"},
		{"IF true", &FuncCall{Name: "if", Args: []Expr{&Lit{Val: true}, &Lit{Val: "yes"}, &Lit{Val: "no"}}}, 0, "yes"},
		{"IF false", &FuncCall{Name: "if", Args: []Expr{&Lit{Val: false}, &Lit{Val: "yes"}, &Lit{Val: "no"}}}, 0, "no"},
		{"SQRT", &FuncCall{Name: "sqrt", Args: []Expr{&Lit{Val: float64(16)}}}, 0, float64(4)},
		{"POW", &FuncCall{Name: "pow", Args: []Expr{&Lit{Val: float64(2)}, &Lit{Val: float64(3)}}}, 0, float64(8)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.Eval(b, tt.row)
			if result != tt.expected {
				t.Fatalf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestNullPropagation(t *testing.T) {
	b := testBatch()
	// NULL + 1 = NULL
	e := &BinOp{Left: &Lit{Val: nil}, Right: &Lit{Val: float64(1)}, Op: "+"}
	if v := e.Eval(b, 0); v != nil {
		t.Fatalf("NULL + 1 should be NULL, got %v", v)
	}
	// UPPER(NULL) = NULL
	e2 := &FuncCall{Name: "upper", Args: []Expr{&Lit{Val: nil}}}
	if v := e2.Eval(b, 0); v != nil {
		t.Fatalf("UPPER(NULL) should be NULL, got %v", v)
	}
}

func TestCast(t *testing.T) {
	b := testBatch()
	// CAST(amount AS int)
	e := &Cast{Operand: &ColRef{Name: "amount"}, DestType: "int"}
	if v := e.Eval(b, 0); v != int64(100) {
		t.Fatalf("expected 100, got %v", v)
	}
	// CAST(id AS string)
	e2 := &Cast{Operand: &ColRef{Name: "id"}, DestType: "string"}
	if v := e2.Eval(b, 0); v != "1" {
		t.Fatalf("expected '1', got %v", v)
	}
}

// --- Compiler tests ---

// compileWhere parses a SQL statement and compiles the WHERE clause expression.
func compileWhere(t *testing.T, sql string) Expr {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if info.WhereExpr == nil {
		t.Fatal("no WHERE clause")
	}
	compiled, err := Compile(info.WhereExpr)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

// compileSelectCol parses a SQL statement and compiles the first SELECT column expression.
func compileSelectCol(t *testing.T, sql string) Expr {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Columns) == 0 {
		t.Fatal("no columns")
	}
	col := info.Columns[0]
	if col.ASTExpr == nil {
		t.Fatal("no AST expression for column")
	}
	compiled, err := Compile(col.ASTExpr)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestCompileColumnRef(t *testing.T) {
	b := testBatch()
	compiled := compileWhere(t, "SELECT name FROM t WHERE id > 1")
	// Row 0: id=1, should be false
	if toBool(compiled, b, 0) {
		t.Fatal("1 > 1 should be false")
	}
	// Row 1: id=2, should be true
	if !toBool(compiled, b, 1) {
		t.Fatal("2 > 1 should be true")
	}
}

func TestCompileArithExpr(t *testing.T) {
	b := testBatch()
	compiled := compileSelectCol(t, "SELECT amount + 10 FROM t")
	v := compiled.Eval(b, 0)
	if v != 110.5 {
		t.Fatalf("expected 110.5, got %v", v)
	}
}

func TestCompileAndOr(t *testing.T) {
	b := testBatch()
	compiled := compileWhere(t, "SELECT * FROM t WHERE id > 1 AND amount < 100")
	// Row 2: id=3, amount=50.25 → true
	if !toBool(compiled, b, 2) {
		t.Fatal("row 2 should match")
	}
	// Row 0: id=1 → false
	if toBool(compiled, b, 0) {
		t.Fatal("row 0 should not match")
	}
}

func TestCompileIn(t *testing.T) {
	b := testBatch()
	compiled := compileWhere(t, "SELECT * FROM t WHERE id IN (1, 3)")
	if !toBool(compiled, b, 0) {
		t.Fatal("id=1 should be in (1,3)")
	}
	if toBool(compiled, b, 1) {
		t.Fatal("id=2 should not be in (1,3)")
	}
	if !toBool(compiled, b, 2) {
		t.Fatal("id=3 should be in (1,3)")
	}
}

func TestCompileBetween(t *testing.T) {
	b := testBatch()
	compiled := compileWhere(t, "SELECT * FROM t WHERE amount BETWEEN 50 AND 150")
	if !toBool(compiled, b, 0) {
		t.Fatal("100.5 should be between 50 and 150")
	}
	if toBool(compiled, b, 1) {
		t.Fatal("200 should not be between 50 and 150")
	}
}

func TestCompileLike(t *testing.T) {
	b := testBatch()
	compiled := compileWhere(t, "SELECT * FROM t WHERE name LIKE 'al%'")
	if !toBool(compiled, b, 0) {
		t.Fatal("alice should match al%")
	}
	if toBool(compiled, b, 1) {
		t.Fatal("bob should not match al%")
	}
}

func TestCompileCaseWhen(t *testing.T) {
	b := testBatch()
	compiled := compileSelectCol(t, "SELECT CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM t")
	if v := compiled.Eval(b, 0); v != "one" {
		t.Fatalf("expected one, got %v", v)
	}
	if v := compiled.Eval(b, 1); v != "two" {
		t.Fatalf("expected two, got %v", v)
	}
	if v := compiled.Eval(b, 2); v != "other" {
		t.Fatalf("expected other, got %v", v)
	}
}

func TestCompileFuncCall(t *testing.T) {
	b := testBatch()
	compiled := compileSelectCol(t, "SELECT upper(name) FROM t")
	if v := compiled.Eval(b, 0); v != "ALICE" {
		t.Fatalf("expected ALICE, got %v", v)
	}
}

func TestCompileNestedExpr(t *testing.T) {
	b := testBatch()
	// amount * 2 + 10
	compiled := compileSelectCol(t, "SELECT amount * 2 + 10 FROM t")
	// Row 0: 100.5 * 2 + 10 = 211
	v := compiled.Eval(b, 0)
	if v != 211.0 {
		t.Fatalf("expected 211, got %v", v)
	}
}

func TestCompileIsNull(t *testing.T) {
	b := testBatch()
	compiled := compileWhere(t, "SELECT * FROM t WHERE name IS NOT NULL")
	// All rows have name set
	if !toBool(compiled, b, 0) {
		t.Fatal("name should not be null")
	}
}

func TestCompileSelectExpr(t *testing.T) {
	sql := "SELECT amount * 2 AS doubled, upper(name) AS uname FROM t"
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	for _, col := range info.Columns {
		if col.ASTExpr == nil {
			continue
		}
		_, name, err := CompileSelectExpr(col.ASTExpr, col.Alias)
		if err != nil {
			t.Fatal(err)
		}
		if name == "" {
			t.Fatal("expected non-empty name")
		}
	}
}

func TestMatchLike(t *testing.T) {
	tests := []struct {
		s       string
		pattern string
		want    bool
	}{
		{"hello", "hello", true},
		{"hello", "hell%", true},
		{"hello", "%llo", true},
		{"hello", "%ll%", true},
		{"hello", "h_llo", true},
		{"hello", "h__lo", true},
		{"hello", "h%o", true},
		{"hello", "abc", false},
		{"", "%", true},
		{"", "_", false},
		{"a", "_", true},
		{"abc", "a_c", true},
		{"abc", "a__", true},
		{"abc", "___", true},
		{"abc", "____", false},
	}
	for _, tt := range tests {
		got := matchLike(tt.s, tt.pattern)
		if got != tt.want {
			t.Errorf("matchLike(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
		}
	}
}

func TestRegisterFunc(t *testing.T) {
	RegisterFunc("double", func(args []any) any {
		if len(args) < 1 || args[0] == nil {
			return nil
		}
		return ToFloat64(args[0]) * 2
	})
	b := testBatch()
	e := &FuncCall{Name: "double", Args: []Expr{&ColRef{Name: "amount"}}}
	v := e.Eval(b, 0)
	if v != 201.0 {
		t.Fatalf("expected 201, got %v", v)
	}
}

func TestNetworkFunctions(t *testing.T) {
	b := testBatch()

	tests := []struct {
		name     string
		expr     Expr
		row      int
		expected any
	}{
		// ip_to_string
		{
			"ip_to_string IPv4",
			&FuncCall{Name: "ip_to_string", Args: []Expr{&Lit{Val: "192.168.1.1"}}},
			0, "192.168.1.1",
		},
		{
			"ip_to_string IPv6",
			&FuncCall{Name: "ip_to_string", Args: []Expr{&Lit{Val: "::1"}}},
			0, "::1",
		},
		{
			"ip_to_string invalid",
			&FuncCall{Name: "ip_to_string", Args: []Expr{&Lit{Val: "not-an-ip"}}},
			0, nil,
		},

		// cidr_contains
		{
			"cidr_contains match",
			&FuncCall{Name: "cidr_contains", Args: []Expr{&Lit{Val: "192.168.1.0/24"}, &Lit{Val: "192.168.1.50"}}},
			0, true,
		},
		{
			"cidr_contains no match",
			&FuncCall{Name: "cidr_contains", Args: []Expr{&Lit{Val: "10.0.0.0/8"}, &Lit{Val: "192.168.1.1"}}},
			0, false,
		},
		{
			"cidr_contains invalid cidr",
			&FuncCall{Name: "cidr_contains", Args: []Expr{&Lit{Val: "invalid"}, &Lit{Val: "192.168.1.1"}}},
			0, nil,
		},

		// ip_version
		{
			"ip_version IPv4",
			&FuncCall{Name: "ip_version", Args: []Expr{&Lit{Val: "192.168.1.1"}}},
			0, float64(4),
		},
		{
			"ip_version IPv6",
			&FuncCall{Name: "ip_version", Args: []Expr{&Lit{Val: "::1"}}},
			0, float64(6),
		},
		{
			"ip_version invalid",
			&FuncCall{Name: "ip_version", Args: []Expr{&Lit{Val: "nope"}}},
			0, nil,
		},

		// mask_ip
		{
			"mask_ip last 1 octet",
			&FuncCall{Name: "mask_ip", Args: []Expr{&Lit{Val: "192.168.1.100"}, &Lit{Val: float64(1)}}},
			0, "192.168.1.0",
		},
		{
			"mask_ip last 2 octets",
			&FuncCall{Name: "mask_ip", Args: []Expr{&Lit{Val: "10.20.30.40"}, &Lit{Val: float64(2)}}},
			0, "10.20.0.0",
		},

		// mac_to_string
		{
			"mac_to_string valid",
			&FuncCall{Name: "mac_to_string", Args: []Expr{&Lit{Val: "aa:bb:cc:dd:ee:ff"}}},
			0, "aa:bb:cc:dd:ee:ff",
		},
		{
			"mac_to_string invalid",
			&FuncCall{Name: "mac_to_string", Args: []Expr{&Lit{Val: "not-a-mac"}}},
			0, nil,
		},

		// ip_subnet
		{
			"ip_subnet",
			&FuncCall{Name: "ip_subnet", Args: []Expr{&Lit{Val: "192.168.1.100/24"}}},
			0, "192.168.1.0",
		},

		// ip_netmask
		{
			"ip_netmask /24",
			&FuncCall{Name: "ip_netmask", Args: []Expr{&Lit{Val: "192.168.1.0/24"}}},
			0, "255.255.255.0",
		},
		{
			"ip_netmask /16",
			&FuncCall{Name: "ip_netmask", Args: []Expr{&Lit{Val: "10.0.0.0/16"}}},
			0, "255.255.0.0",
		},

		// null handling
		{
			"ip_to_string null",
			&FuncCall{Name: "ip_to_string", Args: []Expr{&Lit{Val: nil}}},
			0, nil,
		},
		{
			"cidr_contains null",
			&FuncCall{Name: "cidr_contains", Args: []Expr{&Lit{Val: nil}, &Lit{Val: "1.2.3.4"}}},
			0, nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.Eval(b, tt.row)
			if result != tt.expected {
				t.Fatalf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestNewScalarFunctions(t *testing.T) {
	b := testBatch()

	tests := []struct {
		name     string
		expr     Expr
		row      int
		expected any
	}{
		// starts_with
		{"starts_with true", &FuncCall{Name: "starts_with", Args: []Expr{&Lit{Val: "hello world"}, &Lit{Val: "hello"}}}, 0, true},
		{"starts_with false", &FuncCall{Name: "starts_with", Args: []Expr{&Lit{Val: "hello"}, &Lit{Val: "world"}}}, 0, false},
		{"starts_with null", &FuncCall{Name: "starts_with", Args: []Expr{&Lit{Val: nil}, &Lit{Val: "x"}}}, 0, nil},

		// ends_with
		{"ends_with true", &FuncCall{Name: "ends_with", Args: []Expr{&Lit{Val: "hello world"}, &Lit{Val: "world"}}}, 0, true},
		{"ends_with false", &FuncCall{Name: "ends_with", Args: []Expr{&Lit{Val: "hello"}, &Lit{Val: "world"}}}, 0, false},

		// contains
		{"contains true", &FuncCall{Name: "contains", Args: []Expr{&Lit{Val: "hello world"}, &Lit{Val: "lo wo"}}}, 0, true},
		{"contains false", &FuncCall{Name: "contains", Args: []Expr{&Lit{Val: "hello"}, &Lit{Val: "xyz"}}}, 0, false},
		{"contains column", &FuncCall{Name: "contains", Args: []Expr{&ColRef{Name: "name"}, &Lit{Val: "lic"}}}, 0, true},

		// repeat
		{"repeat", &FuncCall{Name: "repeat", Args: []Expr{&Lit{Val: "ab"}, &Lit{Val: float64(3)}}}, 0, "ababab"},
		{"repeat zero", &FuncCall{Name: "repeat", Args: []Expr{&Lit{Val: "x"}, &Lit{Val: float64(0)}}}, 0, ""},
		{"repeat null", &FuncCall{Name: "repeat", Args: []Expr{&Lit{Val: nil}, &Lit{Val: float64(3)}}}, 0, nil},

		// sign
		{"sign positive", &FuncCall{Name: "sign", Args: []Expr{&Lit{Val: float64(42)}}}, 0, float64(1)},
		{"sign negative", &FuncCall{Name: "sign", Args: []Expr{&Lit{Val: float64(-7)}}}, 0, float64(-1)},
		{"sign zero", &FuncCall{Name: "sign", Args: []Expr{&Lit{Val: float64(0)}}}, 0, float64(0)},

		// greatest
		{"greatest", &FuncCall{Name: "greatest", Args: []Expr{&Lit{Val: float64(1)}, &Lit{Val: float64(5)}, &Lit{Val: float64(3)}}}, 0, float64(5)},
		{"greatest with null", &FuncCall{Name: "greatest", Args: []Expr{&Lit{Val: nil}, &Lit{Val: float64(3)}}}, 0, float64(3)},
		{"greatest all null", &FuncCall{Name: "greatest", Args: []Expr{&Lit{Val: nil}}}, 0, nil},

		// least
		{"least", &FuncCall{Name: "least", Args: []Expr{&Lit{Val: float64(5)}, &Lit{Val: float64(1)}, &Lit{Val: float64(3)}}}, 0, float64(1)},
		{"least with null", &FuncCall{Name: "least", Args: []Expr{&Lit{Val: nil}, &Lit{Val: float64(3)}}}, 0, float64(3)},

		// second
		{"second", &FuncCall{Name: "second", Args: []Expr{&Lit{Val: "2026-03-15T14:30:45Z"}}}, 0, float64(45)},
		{"second null", &FuncCall{Name: "second", Args: []Expr{&Lit{Val: nil}}}, 0, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.expr.Eval(b, tt.row)
			if result != tt.expected {
				t.Fatalf("expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestArrayFunctions(t *testing.T) {
	b := testBatch()

	// Test array literal
	arrExpr := &ArrayLitExpr{Elements: []Expr{
		&Lit{Val: int64(10)},
		&Lit{Val: int64(20)},
		&Lit{Val: int64(30)},
	}}
	arr := arrExpr.Eval(b, 0).([]any)
	if len(arr) != 3 || arr[0] != int64(10) || arr[2] != int64(30) {
		t.Fatalf("array literal: expected [10, 20, 30], got %v", arr)
	}

	t.Run("cardinality", func(t *testing.T) {
		fn := DefaultRegistry.Lookup("cardinality")
		if fn == nil {
			t.Fatal("cardinality function not registered")
		}
		if result := fn([]any{arr}); result != int64(3) {
			t.Fatalf("expected 3, got %v", result)
		}
	})

	t.Run("element_at", func(t *testing.T) {
		fn := DefaultRegistry.Lookup("element_at")
		if result := fn([]any{arr, int64(1)}); result != int64(10) {
			t.Fatalf("element_at(arr, 1) = %v, want 10", result)
		}
		if result := fn([]any{arr, int64(3)}); result != int64(30) {
			t.Fatalf("element_at(arr, 3) = %v, want 30", result)
		}
		if result := fn([]any{arr, int64(-1)}); result != int64(30) {
			t.Fatalf("element_at(arr, -1) = %v, want 30", result)
		}
		if result := fn([]any{arr, int64(5)}); result != nil {
			t.Fatalf("element_at(arr, 5) = %v, want nil", result)
		}
	})

	t.Run("array_contains", func(t *testing.T) {
		fn := DefaultRegistry.Lookup("array_contains")
		if result := fn([]any{arr, int64(20)}); result != true {
			t.Fatalf("expected true, got %v", result)
		}
		if result := fn([]any{arr, int64(99)}); result != false {
			t.Fatalf("expected false, got %v", result)
		}
	})

	t.Run("array_join", func(t *testing.T) {
		strArr := []any{"hello", "world", "go"}
		fn := DefaultRegistry.Lookup("array_join")
		if result := fn([]any{strArr, ", "}); result != "hello, world, go" {
			t.Fatalf("expected 'hello, world, go', got %v", result)
		}
	})

	t.Run("array_min_max", func(t *testing.T) {
		strArr := []any{"banana", "apple", "cherry"}
		fnMin := DefaultRegistry.Lookup("array_min")
		fnMax := DefaultRegistry.Lookup("array_max")
		if result := fnMin([]any{strArr}); result != "apple" {
			t.Fatalf("array_min = %v, want apple", result)
		}
		if result := fnMax([]any{strArr}); result != "cherry" {
			t.Fatalf("array_max = %v, want cherry", result)
		}
	})
}

func TestRowFieldAccess(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{
			Name: "person",
			Type: parquet.TypeRow,
			Fields: []parquet.Column{
				{Name: "name", Type: parquet.TypeString},
				{Name: "age", Type: parquet.TypeInt64},
			},
		},
	}

	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, int64(1))
	b.Columns[1].SetValue(0, map[string]any{"name": "Alice", "age": int64(30)})
	b.Columns[0].SetValue(1, int64(2))
	b.Columns[1].SetValue(1, map[string]any{"name": "Bob", "age": int64(25)})

	// Test dot notation: person.name
	nameRef := &ColRef{Name: "person.name"}
	if result := nameRef.Eval(b, 0); result != "Alice" {
		t.Fatalf("person.name row 0 = %v, want Alice", result)
	}
	if result := nameRef.Eval(b, 1); result != "Bob" {
		t.Fatalf("person.name row 1 = %v, want Bob", result)
	}

	// Test dot notation: person.age
	ageRef := &ColRef{Name: "person.age"}
	if result := ageRef.Eval(b, 0); result != int64(30) {
		t.Fatalf("person.age row 0 = %v, want 30", result)
	}

	// Test row_field function
	fn := DefaultRegistry.Lookup("row_field")
	row := b.Columns[1].GetValue(0)
	if result := fn([]any{row, "name"}); result != "Alice" {
		t.Fatalf("row_field(person, 'name') = %v, want Alice", result)
	}
}

func TestMapFunctions(t *testing.T) {
	// MAP represented as ARRAY(ROW("key","value"))
	mapArr := []any{
		map[string]any{"key": "a", "value": int64(1)},
		map[string]any{"key": "b", "value": int64(2)},
		map[string]any{"key": "c", "value": int64(3)},
	}

	t.Run("map_keys", func(t *testing.T) {
		fn := DefaultRegistry.Lookup("map_keys")
		result := fn([]any{mapArr}).([]any)
		if len(result) != 3 {
			t.Fatalf("expected 3 keys, got %d", len(result))
		}
	})

	t.Run("map_values", func(t *testing.T) {
		fn := DefaultRegistry.Lookup("map_values")
		result := fn([]any{mapArr}).([]any)
		if len(result) != 3 {
			t.Fatalf("expected 3 values, got %d", len(result))
		}
	})

	t.Run("element_at_map", func(t *testing.T) {
		// element_at on a map[string]any should do key lookup
		m := map[string]any{"x": int64(10), "y": int64(20)}
		fn := DefaultRegistry.Lookup("element_at")
		if result := fn([]any{m, "x"}); result != int64(10) {
			t.Fatalf("element_at(map, 'x') = %v, want 10", result)
		}
		if result := fn([]any{m, "z"}); result != nil {
			t.Fatalf("element_at(map, 'z') = %v, want nil", result)
		}
	})

	t.Run("map_from_entries", func(t *testing.T) {
		fn := DefaultRegistry.Lookup("map_from_entries")
		result := fn([]any{mapArr})
		m, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", result)
		}
		if m["a"] != int64(1) || m["b"] != int64(2) {
			t.Fatalf("unexpected map: %v", m)
		}
	})
}

func TestArraySubscriptParsing(t *testing.T) {
	input := "SELECT ARRAY[1, 2, 3][2] AS val"
	parsed, err := plansql.Parse(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract error: %v", err)
	}
	if len(info.Columns) == 0 {
		t.Fatal("expected at least one column")
	}
	// The expression should contain element_at
	expr := info.Columns[0].Expr
	if expr == "" {
		t.Fatal("expected non-empty expression")
	}
	t.Logf("parsed expression: %s", expr)
}
