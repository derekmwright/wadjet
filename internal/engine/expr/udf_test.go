package expr

import (
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestUDFRegisterAndCall(t *testing.T) {
	store := NewUDFStore()

	def := UDFDef{
		Name:   "double",
		Params: []string{"x"},
		Body:   "x * 2",
	}
	if err := store.Register(def, false); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Verify it's registered
	got, ok := store.Get("double")
	if !ok {
		t.Fatal("expected double to be registered")
	}
	if got.Body != "x * 2" {
		t.Fatalf("expected body 'x * 2', got %q", got.Body)
	}

	// Compile a UDF call
	argExprs := []Expr{&Lit{Val: float64(21)}}
	call, err := store.CompileUDFCall("double", argExprs)
	if err != nil {
		t.Fatalf("CompileUDFCall: %v", err)
	}

	result := call.Eval(nil, 0)
	if result != float64(42) {
		t.Fatalf("expected 42, got %v", result)
	}
}

func TestUDFWithMultipleParams(t *testing.T) {
	store := NewUDFStore()

	def := UDFDef{
		Name:   "weighted_sum",
		Params: []string{"val", "weight"},
		Body:   "val * weight + 10",
	}
	if err := store.Register(def, false); err != nil {
		t.Fatalf("Register: %v", err)
	}

	argExprs := []Expr{&Lit{Val: float64(5)}, &Lit{Val: float64(3)}}
	call, err := store.CompileUDFCall("weighted_sum", argExprs)
	if err != nil {
		t.Fatalf("CompileUDFCall: %v", err)
	}

	result := call.Eval(nil, 0)
	// 5 * 3 + 10 = 25
	if result != float64(25) {
		t.Fatalf("expected 25, got %v", result)
	}
}

func TestUDFWithColumnRef(t *testing.T) {
	store := NewUDFStore()

	def := UDFDef{
		Name:   "add_offset",
		Params: []string{"x"},
		Body:   "x + 100",
	}
	if err := store.Register(def, false); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Create a batch with a column
	schema := []parquet.Column{
		{Name: "amount", Type: parquet.TypeInt64},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, int64(10))
	b.Columns[0].SetValue(1, int64(20))
	b.Columns[0].SetValue(2, int64(30))

	// Call UDF with column reference as argument
	argExprs := []Expr{&ColRef{Name: "amount"}}
	call, err := store.CompileUDFCall("add_offset", argExprs)
	if err != nil {
		t.Fatalf("CompileUDFCall: %v", err)
	}

	// Eval for each row
	for i, expected := range []float64{110, 120, 130} {
		result := call.Eval(b, i)
		if ToFloat64(result) != expected {
			t.Errorf("row %d: expected %v, got %v", i, expected, result)
		}
	}
}

func TestUDFWithBuiltinFunction(t *testing.T) {
	store := NewUDFStore()

	def := UDFDef{
		Name:   "abs_double",
		Params: []string{"x"},
		Body:   "abs(x) * 2",
	}
	if err := store.Register(def, false); err != nil {
		t.Fatalf("Register: %v", err)
	}

	argExprs := []Expr{&Lit{Val: float64(-5)}}
	call, err := store.CompileUDFCall("abs_double", argExprs)
	if err != nil {
		t.Fatalf("CompileUDFCall: %v", err)
	}

	result := call.Eval(nil, 0)
	if result != float64(10) {
		t.Fatalf("expected 10, got %v", result)
	}
}

func TestUDFCircularDependency(t *testing.T) {
	store := NewUDFStore()

	// Self-referencing
	def := UDFDef{
		Name:   "loop",
		Params: []string{"x"},
		Body:   "loop(x + 1)",
	}
	err := store.Register(def, false)
	if err == nil {
		t.Fatal("expected error for circular dependency")
	}
}

func TestUDFCannotOverwriteBuiltin(t *testing.T) {
	store := NewUDFStore()

	def := UDFDef{
		Name:   "upper",
		Params: []string{"x"},
		Body:   "x * 2",
	}
	err := store.Register(def, false)
	if err == nil {
		t.Fatal("expected error when overwriting builtin")
	}
}

func TestUDFOwnershipLock(t *testing.T) {
	store := NewUDFStore()

	// Alice creates a locked function
	def := UDFDef{
		Name:   "alice_fn",
		Params: []string{"x"},
		Body:   "x + 1",
		Owner:  "alice",
		Locked: true,
	}
	if err := store.Register(def, false); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Bob tries to replace it
	def2 := UDFDef{
		Name:   "alice_fn",
		Params: []string{"x"},
		Body:   "x + 999",
		Owner:  "bob",
	}
	err := store.Register(def2, false)
	if err == nil {
		t.Fatal("expected error: bob cannot overwrite alice's locked function")
	}

	// Alice can replace her own
	def3 := UDFDef{
		Name:   "alice_fn",
		Params: []string{"x"},
		Body:   "x + 2",
		Owner:  "alice",
		Locked: true,
	}
	if err := store.Register(def3, false); err != nil {
		t.Fatalf("alice should be able to replace her own: %v", err)
	}

	// Admin can replace anyone's (keep it locked)
	def4 := UDFDef{
		Name:   "alice_fn",
		Params: []string{"x"},
		Body:   "x + 3",
		Owner:  "admin_user",
		Locked: true,
	}
	if err := store.Register(def4, true); err != nil {
		t.Fatalf("admin should be able to replace: %v", err)
	}

	// Bob tries to drop it (locked by admin_user)
	err = store.Unregister("alice_fn", "bob", false)
	if err == nil {
		t.Fatal("expected error: bob cannot drop admin_user's locked function")
	}

	// Admin can drop it (as owner)
	err = store.Unregister("alice_fn", "admin_user", false)
	if err != nil {
		t.Fatalf("admin_user should be able to drop own function: %v", err)
	}
}

func TestUDFDropNonExistent(t *testing.T) {
	store := NewUDFStore()

	err := store.Unregister("nonexistent", "", false)
	if err == nil {
		t.Fatal("expected error dropping nonexistent function")
	}
}

func TestUDFList(t *testing.T) {
	store := NewUDFStore()

	store.Register(UDFDef{Name: "fn_a", Params: []string{"x"}, Body: "x + 1"}, false)
	store.Register(UDFDef{Name: "fn_b", Params: []string{"x", "y"}, Body: "x * y"}, false)

	list := store.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 UDFs, got %d", len(list))
	}

	names := []string{list[0].Name, list[1].Name}
	sort.Strings(names)
	if names[0] != "fn_a" || names[1] != "fn_b" {
		t.Fatalf("expected [fn_a, fn_b], got %v", names)
	}
}

func TestUDFWrongArgCount(t *testing.T) {
	store := NewUDFStore()
	store.Register(UDFDef{Name: "needs_two", Params: []string{"a", "b"}, Body: "a + b"}, false)

	_, err := store.CompileUDFCall("needs_two", []Expr{&Lit{Val: float64(1)}})
	if err == nil {
		t.Fatal("expected error for wrong arg count")
	}
}

func TestUDFCaseExpression(t *testing.T) {
	store := NewUDFStore()

	def := UDFDef{
		Name:   "classify",
		Params: []string{"score"},
		Body:   "CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END",
	}
	if err := store.Register(def, false); err != nil {
		t.Fatalf("Register: %v", err)
	}

	tests := []struct {
		score    float64
		expected string
	}{
		{95, "A"},
		{85, "B"},
		{70, "C"},
	}
	for _, tt := range tests {
		call, err := store.CompileUDFCall("classify", []Expr{&Lit{Val: tt.score}})
		if err != nil {
			t.Fatalf("CompileUDFCall: %v", err)
		}
		result := call.Eval(nil, 0)
		if result != tt.expected {
			t.Errorf("classify(%v) = %v, want %v", tt.score, result, tt.expected)
		}
	}
}
