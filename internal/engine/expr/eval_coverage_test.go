package expr

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// helper to build a simple test batch with typed columns
func makeTestBatch(cols []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(cols, rows)
}

func TestLitEval(t *testing.T) {
	lit := &Lit{Val: "hello"}
	result := lit.Eval(nil, 0)
	if result != "hello" {
		t.Errorf("expected hello, got %v", result)
	}
}

func TestLitEvalNil(t *testing.T) {
	lit := &Lit{Val: nil}
	result := lit.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestLitEvalFloat64(t *testing.T) {
	lit := &Lit{Val: float64(3.14)}
	f, ok := lit.EvalFloat64(nil, 0)
	if !ok || f != 3.14 {
		t.Errorf("expected 3.14, got %v ok=%v", f, ok)
	}

	nilLit := &Lit{Val: nil}
	_, ok = nilLit.EvalFloat64(nil, 0)
	if ok {
		t.Error("expected !ok for nil literal")
	}
}

func TestLitEvalInt64(t *testing.T) {
	lit := &Lit{Val: int64(42)}
	i, ok := lit.EvalInt64(nil, 0)
	if !ok || i != 42 {
		t.Errorf("expected 42, got %v ok=%v", i, ok)
	}

	nilLit := &Lit{Val: nil}
	_, ok = nilLit.EvalInt64(nil, 0)
	if ok {
		t.Error("expected !ok for nil literal")
	}
}

func TestColRefEval(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "score", Type: parquet.TypeFloat64, Nullable: true},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
		{"id": int64(2), "name": "bob", "score": 87.3},
	})

	idRef := &ColRef{Name: "id"}
	result := idRef.Eval(b, 0)
	if result != int64(1) {
		t.Errorf("expected 1, got %v", result)
	}

	nameRef := &ColRef{Name: "name"}
	result = nameRef.Eval(b, 0)
	if result != "alice" {
		t.Errorf("expected alice, got %v", result)
	}

	scoreRef := &ColRef{Name: "score"}
	result = scoreRef.Eval(b, 0)
	if result != 95.5 {
		t.Errorf("expected 95.5, got %v", result)
	}
}

func TestColRefMissing(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"id": int64(1)},
	})

	ref := &ColRef{Name: "nonexistent"}
	result := ref.Eval(b, 0)
	if result != nil {
		t.Errorf("expected nil for missing column, got %v", result)
	}
}

func TestColRefEvalFloat64(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64, Nullable: true},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"val": 3.14},
	})

	ref := &ColRef{Name: "val"}
	f, ok := ref.EvalFloat64(b, 0)
	if !ok {
		t.Fatal("expected ok")
	}
	if f != 3.14 {
		t.Errorf("expected 3.14, got %v", f)
	}
}

func TestColRefEvalFloat64Missing(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeFloat64},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"val": 1.0},
	})

	ref := &ColRef{Name: "missing"}
	_, ok := ref.EvalFloat64(b, 0)
	if ok {
		t.Error("expected !ok for missing column")
	}
}

func TestColRefEvalString(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"name": "hello"},
	})

	ref := &ColRef{Name: "name"}
	s, ok := ref.EvalString(b, 0)
	if !ok || s != "hello" {
		t.Errorf("expected hello, got %q ok=%v", s, ok)
	}
}

func TestColRefEvalStringMissing(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"name": "test"},
	})

	ref := &ColRef{Name: "missing"}
	_, ok := ref.EvalString(b, 0)
	if ok {
		t.Error("expected !ok for missing column")
	}
}

func TestColRefEvalInt64(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"id": int64(42)},
	})

	ref := &ColRef{Name: "id"}
	i, ok := ref.EvalInt64(b, 0)
	if !ok || i != 42 {
		t.Errorf("expected 42, got %v ok=%v", i, ok)
	}
}

func TestColRefEvalInt64Missing(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"id": int64(1)},
	})

	ref := &ColRef{Name: "missing"}
	_, ok := ref.EvalInt64(b, 0)
	if ok {
		t.Error("expected !ok for missing column")
	}
}

func TestBinOpAllOps(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}
	b := makeTestBatch(schema, []map[string]any{
		{"a": 10.0, "b": 3.0},
	})

	tests := []struct {
		op   string
		want float64
	}{
		{"+", 13.0},
		{"-", 7.0},
		{"*", 30.0},
		{"/", 10.0 / 3.0},
	}

	for _, tt := range tests {
		binop := &BinOp{Left: &ColRef{Name: "a"}, Right: &ColRef{Name: "b"}, Op: tt.op}
		result := binop.Eval(b, 0)
		if f, ok := result.(float64); !ok || f != tt.want {
			t.Errorf("op %s: expected %v, got %v", tt.op, tt.want, result)
		}
	}
}

func TestBinOpModulo(t *testing.T) {
	binop := &BinOp{Left: &Lit{Val: int64(10)}, Right: &Lit{Val: int64(3)}, Op: "%"}
	result := binop.Eval(nil, 0)
	if result != float64(1) {
		t.Errorf("expected 1, got %v", result)
	}
}

func TestBinOpDivideByZero(t *testing.T) {
	binop := &BinOp{Left: &Lit{Val: float64(10)}, Right: &Lit{Val: float64(0)}, Op: "/"}
	result := binop.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil for divide by zero, got %v", result)
	}

	modop := &BinOp{Left: &Lit{Val: int64(10)}, Right: &Lit{Val: int64(0)}, Op: "%"}
	result = modop.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil for mod by zero, got %v", result)
	}
}

func TestBinOpNullOperand(t *testing.T) {
	binop := &BinOp{Left: &Lit{Val: nil}, Right: &Lit{Val: int64(5)}, Op: "+"}
	result := binop.Eval(nil, 0)
	if result != nil {
		t.Error("expected nil when left operand is null")
	}

	binop2 := &BinOp{Left: &Lit{Val: int64(5)}, Right: &Lit{Val: nil}, Op: "+"}
	result = binop2.Eval(nil, 0)
	if result != nil {
		t.Error("expected nil when right operand is null")
	}
}

func TestBinOpUnknownOp(t *testing.T) {
	binop := &BinOp{Left: &Lit{Val: float64(1)}, Right: &Lit{Val: float64(2)}, Op: "^"}
	result := binop.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil for unknown op, got %v", result)
	}
}

func TestBinOpInt64AllOps(t *testing.T) {
	tests := []struct {
		op   string
		l, r int64
		want int64
		ok   bool
	}{
		{"+", 10, 3, 13, true},
		{"-", 10, 3, 7, true},
		{"*", 10, 3, 30, true},
		{"/", 10, 3, 3, true},
		{"%", 10, 3, 1, true},
		{"/", 10, 0, 0, false},
		{"%", 10, 0, 0, false},
		{"^", 10, 3, 0, false},
	}
	for _, tt := range tests {
		binop := &BinOpInt64{Left: &Lit{Val: tt.l}, Right: &Lit{Val: tt.r}, Op: tt.op}
		result, ok := binop.EvalInt64(nil, 0)
		if ok != tt.ok {
			t.Errorf("op %s(%d,%d): ok=%v, want %v", tt.op, tt.l, tt.r, ok, tt.ok)
		}
		if ok && result != tt.want {
			t.Errorf("op %s(%d,%d): got %d, want %d", tt.op, tt.l, tt.r, result, tt.want)
		}
	}
}

func TestBinOpInt64Eval(t *testing.T) {
	binop := &BinOpInt64{Left: &Lit{Val: int64(5)}, Right: &Lit{Val: int64(3)}, Op: "+"}
	result := binop.Eval(nil, 0)
	if result != int64(8) {
		t.Errorf("expected 8, got %v", result)
	}

	// Null case
	binop2 := &BinOpInt64{Left: &Lit{Val: nil}, Right: &Lit{Val: int64(3)}, Op: "+"}
	result = binop2.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestBinOpInt64EvalFloat64(t *testing.T) {
	binop := &BinOpInt64{Left: &Lit{Val: int64(5)}, Right: &Lit{Val: int64(3)}, Op: "+"}
	f, ok := binop.EvalFloat64(nil, 0)
	if !ok || f != 8.0 {
		t.Errorf("expected 8.0, got %v ok=%v", f, ok)
	}
}

func TestBinOpFloat64AllOps(t *testing.T) {
	tests := []struct {
		op   string
		l, r float64
		want float64
		ok   bool
	}{
		{"+", 10.5, 3.5, 14.0, true},
		{"-", 10.5, 3.5, 7.0, true},
		{"*", 10.0, 3.0, 30.0, true},
		{"/", 10.0, 4.0, 2.5, true},
		{"/", 10.0, 0.0, 0, false},
		{"%", 10.0, 0.0, 0, false},
		{"^", 1.0, 2.0, 0, false},
	}
	for _, tt := range tests {
		binop := &BinOpFloat64{Left: &Lit{Val: tt.l}, Right: &Lit{Val: tt.r}, Op: tt.op}
		result, ok := binop.EvalFloat64(nil, 0)
		if ok != tt.ok {
			t.Errorf("op %s: ok=%v, want %v", tt.op, ok, tt.ok)
		}
		if ok && result != tt.want {
			t.Errorf("op %s: got %v, want %v", tt.op, result, tt.want)
		}
	}
}

func TestBinOpFloat64Eval(t *testing.T) {
	binop := &BinOpFloat64{Left: &Lit{Val: float64(1.5)}, Right: &Lit{Val: float64(2.5)}, Op: "+"}
	result := binop.Eval(nil, 0)
	if result != 4.0 {
		t.Errorf("expected 4.0, got %v", result)
	}
}

func TestUnaryOp(t *testing.T) {
	neg := &UnaryOp{Operand: &Lit{Val: float64(5)}, Op: "-"}
	result := neg.Eval(nil, 0)
	if result != float64(-5) {
		t.Errorf("expected -5, got %v", result)
	}

	pos := &UnaryOp{Operand: &Lit{Val: float64(5)}, Op: "+"}
	result = pos.Eval(nil, 0)
	if result != float64(5) {
		t.Errorf("expected 5, got %v", result)
	}

	unknown := &UnaryOp{Operand: &Lit{Val: float64(5)}, Op: "~"}
	result = unknown.Eval(nil, 0)
	if result != float64(5) {
		t.Errorf("expected 5 for unknown unary op, got %v", result)
	}

	nilOp := &UnaryOp{Operand: &Lit{Val: nil}, Op: "-"}
	result = nilOp.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil for null unary, got %v", result)
	}
}

func TestCmpInt64(t *testing.T) {
	tests := []struct {
		op   CmpOp
		l, r int64
		want bool
	}{
		{CmpEq, 5, 5, true},
		{CmpEq, 5, 6, false},
		{CmpNe, 5, 6, true},
		{CmpNe, 5, 5, false},
		{CmpLt, 5, 6, true},
		{CmpLt, 6, 5, false},
		{CmpLe, 5, 5, true},
		{CmpLe, 6, 5, false},
		{CmpGt, 6, 5, true},
		{CmpGt, 5, 6, false},
		{CmpGe, 5, 5, true},
		{CmpGe, 4, 5, false},
	}
	for _, tt := range tests {
		cmp := &CmpInt64{Left: &Lit{Val: tt.l}, Right: &Lit{Val: tt.r}, Op: tt.op}
		result := cmp.EvalBool(nil, 0)
		if result != tt.want {
			t.Errorf("CmpInt64(%d op%d %d) = %v, want %v", tt.l, tt.op, tt.r, result, tt.want)
		}
	}
}

func TestCmpInt64DefaultCase(t *testing.T) {
	cmp := &CmpInt64{Left: &Lit{Val: int64(1)}, Right: &Lit{Val: int64(2)}, Op: CmpOp(99)}
	if cmp.EvalBool(nil, 0) {
		t.Error("unknown op should return false")
	}
}

func TestCmpInt64NullOperand(t *testing.T) {
	cmp := &CmpInt64{Left: &Lit{Val: nil}, Right: &Lit{Val: int64(5)}, Op: CmpEq}
	if cmp.EvalBool(nil, 0) {
		t.Error("null comparison should return false")
	}
}

func TestCmpInt64Eval(t *testing.T) {
	cmp := &CmpInt64{Left: &Lit{Val: int64(5)}, Right: &Lit{Val: int64(5)}, Op: CmpEq}
	result := cmp.Eval(nil, 0)
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestCmpFloat64(t *testing.T) {
	tests := []struct {
		op   CmpOp
		l, r float64
		want bool
	}{
		{CmpEq, 3.14, 3.14, true},
		{CmpEq, 3.14, 2.71, false},
		{CmpNe, 3.14, 2.71, true},
		{CmpLt, 2.71, 3.14, true},
		{CmpLe, 3.14, 3.14, true},
		{CmpGt, 3.14, 2.71, true},
		{CmpGe, 3.14, 3.14, true},
	}
	for _, tt := range tests {
		cmp := &CmpFloat64{Left: &Lit{Val: tt.l}, Right: &Lit{Val: tt.r}, Op: tt.op}
		result := cmp.EvalBool(nil, 0)
		if result != tt.want {
			t.Errorf("CmpFloat64(%v op%d %v) = %v, want %v", tt.l, tt.op, tt.r, result, tt.want)
		}
	}
}

func TestCmpFloat64DefaultCase(t *testing.T) {
	cmp := &CmpFloat64{Left: &Lit{Val: float64(1)}, Right: &Lit{Val: float64(2)}, Op: CmpOp(99)}
	if cmp.EvalBool(nil, 0) {
		t.Error("unknown op should return false")
	}
}

func TestCmpFloat64Eval(t *testing.T) {
	cmp := &CmpFloat64{Left: &Lit{Val: float64(1)}, Right: &Lit{Val: float64(1)}, Op: CmpEq}
	result := cmp.Eval(nil, 0)
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestCmpFloat64NullOperand(t *testing.T) {
	cmp := &CmpFloat64{Left: &Lit{Val: nil}, Right: &Lit{Val: float64(5)}, Op: CmpEq}
	if cmp.EvalBool(nil, 0) {
		t.Error("null comparison should return false")
	}
}

func TestIsNull(t *testing.T) {
	isNull := &IsNull{Operand: &Lit{Val: nil}, Not: false}
	result := isNull.EvalBool(nil, 0)
	if !result {
		t.Error("IS NULL of nil should be true")
	}

	isNotNull := &IsNull{Operand: &Lit{Val: nil}, Not: true}
	result = isNotNull.EvalBool(nil, 0)
	if result {
		t.Error("IS NOT NULL of nil should be false")
	}

	isNull2 := &IsNull{Operand: &Lit{Val: "hello"}, Not: false}
	if isNull2.EvalBool(nil, 0) {
		t.Error("IS NULL of non-nil should be false")
	}

	// Test Eval wrapper
	result2 := isNull.Eval(nil, 0)
	if result2 != true {
		t.Errorf("expected true from Eval, got %v", result2)
	}
}

func TestNotExpr(t *testing.T) {
	not := &Not{Operand: &Lit{Val: true}}
	if not.EvalBool(nil, 0) {
		t.Error("NOT true should be false")
	}

	not2 := &Not{Operand: &Lit{Val: false}}
	if !not2.EvalBool(nil, 0) {
		t.Error("NOT false should be true")
	}

	// Test Eval wrapper
	result := not.Eval(nil, 0)
	if result != false {
		t.Errorf("expected false from Eval, got %v", result)
	}
}

func TestInExpr(t *testing.T) {
	in := &In{
		Expr:   &Lit{Val: int64(5)},
		Values: []Expr{&Lit{Val: int64(1)}, &Lit{Val: int64(5)}, &Lit{Val: int64(10)}},
		Not:    false,
	}
	if !in.EvalBool(nil, 0) {
		t.Error("5 IN (1,5,10) should be true")
	}

	notIn := &In{
		Expr:   &Lit{Val: int64(5)},
		Values: []Expr{&Lit{Val: int64(1)}, &Lit{Val: int64(5)}, &Lit{Val: int64(10)}},
		Not:    true,
	}
	if notIn.EvalBool(nil, 0) {
		t.Error("5 NOT IN (1,5,10) should be false")
	}

	notFound := &In{
		Expr:   &Lit{Val: int64(99)},
		Values: []Expr{&Lit{Val: int64(1)}, &Lit{Val: int64(5)}},
		Not:    false,
	}
	if notFound.EvalBool(nil, 0) {
		t.Error("99 IN (1,5) should be false")
	}

	// Null check
	nullIn := &In{Expr: &Lit{Val: nil}, Values: []Expr{&Lit{Val: int64(1)}}, Not: false}
	if nullIn.EvalBool(nil, 0) {
		t.Error("NULL IN (...) should be false")
	}

	// Test Eval wrapper
	result := in.Eval(nil, 0)
	if result != true {
		t.Errorf("expected true from Eval, got %v", result)
	}
}

func TestBetweenExpr(t *testing.T) {
	between := &Between{
		Expr: &Lit{Val: int64(5)},
		Low:  &Lit{Val: int64(1)},
		Hi:   &Lit{Val: int64(10)},
		Not:  false,
	}
	if !between.EvalBool(nil, 0) {
		t.Error("5 BETWEEN 1 AND 10 should be true")
	}

	notBetween := &Between{
		Expr: &Lit{Val: int64(5)},
		Low:  &Lit{Val: int64(1)},
		Hi:   &Lit{Val: int64(10)},
		Not:  true,
	}
	if notBetween.EvalBool(nil, 0) {
		t.Error("5 NOT BETWEEN 1 AND 10 should be false")
	}

	// Null case
	nullBetween := &Between{
		Expr: &Lit{Val: nil},
		Low:  &Lit{Val: int64(1)},
		Hi:   &Lit{Val: int64(10)},
	}
	if nullBetween.EvalBool(nil, 0) {
		t.Error("NULL BETWEEN should be false")
	}

	// Test Eval wrapper
	result := between.Eval(nil, 0)
	if result != true {
		t.Errorf("expected true from Eval, got %v", result)
	}
}

func TestLikeExpr(t *testing.T) {
	like := &Like{
		Expr:    &Lit{Val: "hello world"},
		Pattern: &Lit{Val: "hello%"},
		Not:     false,
	}
	if !like.EvalBool(nil, 0) {
		t.Error("'hello world' LIKE 'hello%' should be true")
	}

	notLike := &Like{
		Expr:    &Lit{Val: "hello world"},
		Pattern: &Lit{Val: "hello%"},
		Not:     true,
	}
	if notLike.EvalBool(nil, 0) {
		t.Error("'hello world' NOT LIKE 'hello%' should be false")
	}

	// Null case
	nullLike := &Like{
		Expr:    &Lit{Val: nil},
		Pattern: &Lit{Val: "hello%"},
	}
	if nullLike.EvalBool(nil, 0) {
		t.Error("NULL LIKE should be false")
	}

	// Test Eval wrapper
	result := like.Eval(nil, 0)
	if result != true {
		t.Errorf("expected true from Eval, got %v", result)
	}
}

func TestCaseExpr(t *testing.T) {
	// Searched CASE
	c := &Case{
		Whens: []CaseWhen{
			{Cond: &Lit{Val: false}, Result: &Lit{Val: "no"}},
			{Cond: &Lit{Val: true}, Result: &Lit{Val: "yes"}},
		},
		Else: &Lit{Val: "default"},
	}
	result := c.Eval(nil, 0)
	if result != "yes" {
		t.Errorf("expected yes, got %v", result)
	}

	// Simple CASE with operand
	c2 := &Case{
		Operand: &Lit{Val: "x"},
		Whens: []CaseWhen{
			{Cond: &Lit{Val: "a"}, Result: &Lit{Val: "letter a"}},
			{Cond: &Lit{Val: "x"}, Result: &Lit{Val: "letter x"}},
		},
	}
	result = c2.Eval(nil, 0)
	if result != "letter x" {
		t.Errorf("expected letter x, got %v", result)
	}

	// ELSE case
	c3 := &Case{
		Whens: []CaseWhen{
			{Cond: &Lit{Val: false}, Result: &Lit{Val: "no"}},
		},
		Else: &Lit{Val: "default"},
	}
	result = c3.Eval(nil, 0)
	if result != "default" {
		t.Errorf("expected default, got %v", result)
	}

	// No match no ELSE
	c4 := &Case{
		Whens: []CaseWhen{
			{Cond: &Lit{Val: false}, Result: &Lit{Val: "no"}},
		},
	}
	result = c4.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestCoalesceExpr(t *testing.T) {
	c := &Coalesce{
		Args: []Expr{&Lit{Val: nil}, &Lit{Val: nil}, &Lit{Val: "found"}},
	}
	result := c.Eval(nil, 0)
	if result != "found" {
		t.Errorf("expected found, got %v", result)
	}

	// All nil
	c2 := &Coalesce{
		Args: []Expr{&Lit{Val: nil}, &Lit{Val: nil}},
	}
	result = c2.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}

	// First non-nil
	c3 := &Coalesce{
		Args: []Expr{&Lit{Val: "first"}},
	}
	result = c3.Eval(nil, 0)
	if result != "first" {
		t.Errorf("expected first, got %v", result)
	}
}

func TestArrayLitExpr(t *testing.T) {
	arr := &ArrayLitExpr{
		Elements: []Expr{&Lit{Val: int64(1)}, &Lit{Val: int64(2)}, &Lit{Val: int64(3)}},
	}
	result := arr.Eval(nil, 0)
	slice, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result)
	}
	if len(slice) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(slice))
	}
	if slice[0] != int64(1) {
		t.Errorf("expected 1, got %v", slice[0])
	}
}

func TestFuncCallUnknownFunc(t *testing.T) {
	fc := &FuncCall{
		Name: "nonexistent_function",
		Args: []Expr{&Lit{Val: "hello"}},
	}
	result := fc.Eval(nil, 0)
	if result != nil {
		t.Errorf("expected nil for unknown function, got %v", result)
	}
}

func TestFuncRegistryOperations(t *testing.T) {
	reg := NewFuncRegistry()

	// Register
	reg.Register("MyFunc", func(args []any) any { return "ok" })
	if !reg.Has("myfunc") {
		t.Error("expected function to be registered (case-insensitive)")
	}

	// Lookup
	fn := reg.Lookup("MYFUNC")
	if fn == nil {
		t.Fatal("expected function to be found")
	}
	if fn(nil) != "ok" {
		t.Error("expected 'ok' from function")
	}

	// Names
	names := reg.Names()
	if len(names) != 1 {
		t.Fatalf("expected 1 name, got %d", len(names))
	}

	// Unregister
	existed := reg.Unregister("myfunc")
	if !existed {
		t.Error("expected function to have existed")
	}
	if reg.Has("myfunc") {
		t.Error("expected function to be removed")
	}

	// Unregister non-existent
	existed = reg.Unregister("nonexistent")
	if existed {
		t.Error("expected non-existent function")
	}
}

func TestAndOrEval(t *testing.T) {
	// AND: Eval wrapper
	and := &And{Left: &Lit{Val: true}, Right: &Lit{Val: false}}
	result := and.Eval(nil, 0)
	if result != false {
		t.Errorf("expected false, got %v", result)
	}

	// OR: Eval wrapper
	or := &Or{Left: &Lit{Val: false}, Right: &Lit{Val: true}}
	result = or.Eval(nil, 0)
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestCastExprEval(t *testing.T) {
	// Cast is compiled, test it via compilation
	castInt := &Cast{Operand: &Lit{Val: "42"}, DestType: "integer"}
	result := castInt.Eval(nil, 0)
	// Cast.Eval should produce a typed value
	if result == nil {
		t.Error("expected non-nil from cast")
	}
}
