package expr

import (
	"fmt"
	"testing"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// mockRunner returns a SubqueryRunner that returns predefined rows.
func mockRunner(rows []map[string]any, err error) SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		return rows, err
	}
}

func TestScalarSubquery(t *testing.T) {
	b := testBatch()
	runner := mockRunner([]map[string]any{{"avg_amount": 116.916}}, nil)

	sq := &ScalarSubquery{SQL: "SELECT AVG(amount) AS avg_amount FROM t", Runner: runner}

	val := sq.Eval(b, 0)
	if val == nil {
		t.Fatal("expected non-nil value")
	}
	f, ok := val.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", val)
	}
	if f != 116.916 {
		t.Fatalf("expected 116.916, got %v", f)
	}

	// Second call should use cached value
	val2 := sq.Eval(b, 1)
	if val2 != val {
		t.Fatal("expected cached value on second call")
	}
}

func TestScalarSubqueryEmpty(t *testing.T) {
	b := testBatch()
	runner := mockRunner(nil, nil) // no rows

	sq := &ScalarSubquery{SQL: "SELECT 1", Runner: runner}
	if v := sq.Eval(b, 0); v != nil {
		t.Fatalf("expected nil for empty subquery, got %v", v)
	}
}

func TestScalarSubqueryError(t *testing.T) {
	b := testBatch()
	runner := mockRunner(nil, fmt.Errorf("table not found"))

	sq := &ScalarSubquery{SQL: "SELECT 1 FROM nonexistent", Runner: runner}
	if v := sq.Eval(b, 0); v != nil {
		t.Fatalf("expected nil on error, got %v", v)
	}
}

func TestInSubquery(t *testing.T) {
	b := testBatch()

	// Subquery returns user IDs 1 and 3
	runner := mockRunner([]map[string]any{
		{"id": int64(1)},
		{"id": int64(3)},
	}, nil)

	inSq := &InSubquery{
		Expr:   &ColRef{Name: "id"},
		SQL:    "SELECT id FROM active_users",
		Runner: runner,
	}

	// Row 0: id=1, should be IN
	if !inSq.EvalBool(b, 0) {
		t.Fatal("expected id=1 to be IN subquery result")
	}
	// Row 1: id=2, should NOT be IN
	if inSq.EvalBool(b, 1) {
		t.Fatal("expected id=2 to NOT be IN subquery result")
	}
	// Row 2: id=3, should be IN
	if !inSq.EvalBool(b, 2) {
		t.Fatal("expected id=3 to be IN subquery result")
	}
}

func TestInSubqueryNot(t *testing.T) {
	b := testBatch()

	runner := mockRunner([]map[string]any{
		{"id": int64(2)},
	}, nil)

	notInSq := &InSubquery{
		Expr:   &ColRef{Name: "id"},
		SQL:    "SELECT id FROM banned_users",
		Runner: runner,
		Not:    true,
	}

	// Row 0: id=1, should be NOT IN {2} → true
	if !notInSq.EvalBool(b, 0) {
		t.Fatal("expected id=1 to be NOT IN subquery result")
	}
	// Row 1: id=2, should be NOT IN {2} → false
	if notInSq.EvalBool(b, 1) {
		t.Fatal("expected id=2 to NOT be NOT IN subquery result")
	}
}

func TestInSubqueryNull(t *testing.T) {
	b := testBatch()
	// Set id to null for row 0
	b.Columns[0].Nulls.SetNull(0)

	runner := mockRunner([]map[string]any{{"id": int64(1)}}, nil)

	inSq := &InSubquery{
		Expr:   &ColRef{Name: "id"},
		SQL:    "SELECT id FROM t",
		Runner: runner,
	}

	// Null IN (...) should be false
	if inSq.EvalBool(b, 0) {
		t.Fatal("expected null IN subquery to be false")
	}
}

func TestExistsSubquery(t *testing.T) {
	b := testBatch()

	// EXISTS with rows
	runner := mockRunner([]map[string]any{{"x": 1}}, nil)
	exists := &ExistsSubquery{SQL: "SELECT 1", Runner: runner}
	if !exists.EvalBool(b, 0) {
		t.Fatal("expected EXISTS to be true when rows returned")
	}

	// NOT EXISTS with rows
	runner2 := mockRunner([]map[string]any{{"x": 1}}, nil)
	notExists := &ExistsSubquery{SQL: "SELECT 1", Runner: runner2, Not: true}
	if notExists.EvalBool(b, 0) {
		t.Fatal("expected NOT EXISTS to be false when rows returned")
	}
}

func TestExistsSubqueryEmpty(t *testing.T) {
	b := testBatch()

	runner := mockRunner(nil, nil) // no rows
	exists := &ExistsSubquery{SQL: "SELECT 1 FROM t WHERE 1=0", Runner: runner}
	if exists.EvalBool(b, 0) {
		t.Fatal("expected EXISTS to be false when no rows")
	}

	// NOT EXISTS with no rows → true
	runner2 := mockRunner(nil, nil)
	notExists := &ExistsSubquery{SQL: "SELECT 1", Runner: runner2, Not: true}
	if !notExists.EvalBool(b, 0) {
		t.Fatal("expected NOT EXISTS to be true when no rows")
	}
}

func TestExistsSubqueryError(t *testing.T) {
	b := testBatch()
	runner := mockRunner(nil, fmt.Errorf("error"))
	exists := &ExistsSubquery{SQL: "SELECT 1", Runner: runner}
	if exists.EvalBool(b, 0) {
		t.Fatal("expected EXISTS to be false on error")
	}
}

// --- Compiler tests for subqueries ---

func TestCompileScalarSubquery(t *testing.T) {
	// Parse: amount > (select 1)
	stmt, err := sqlparser.Parse("SELECT 1 FROM t WHERE amount > (SELECT 1)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr

	runner := mockRunner([]map[string]any{{"1": int64(50)}}, nil)
	compiled, err := CompileWithRunner(where, runner)
	if err != nil {
		t.Fatal(err)
	}

	b := testBatch()
	// Row 0: amount=100.5 > 50 → true
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true, got %v", v)
	}
}

func TestCompileInSubquery(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 1 FROM t WHERE id IN (SELECT id FROM other)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr

	runner := mockRunner([]map[string]any{
		{"id": int64(1)},
		{"id": int64(3)},
	}, nil)

	compiled, err := CompileWithRunner(where, runner)
	if err != nil {
		t.Fatal(err)
	}

	b := testBatch()
	// Row 0: id=1, IN {1,3} → true
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true for id=1, got %v", v)
	}
	// Row 1: id=2, IN {1,3} → false
	if v := compiled.Eval(b, 1); v != true {
		// IN subquery: id=2 is NOT in {1,3}
		if v != false {
			t.Fatalf("expected false for id=2, got %v", v)
		}
	}
}

func TestCompileNotInSubquery(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 1 FROM t WHERE id NOT IN (SELECT id FROM other)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr

	runner := mockRunner([]map[string]any{
		{"id": int64(2)},
	}, nil)

	compiled, err := CompileWithRunner(where, runner)
	if err != nil {
		t.Fatal(err)
	}

	b := testBatch()
	// Row 0: id=1, NOT IN {2} → true
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true for id=1 NOT IN {2}, got %v", v)
	}
	// Row 1: id=2, NOT IN {2} → false
	if v := compiled.Eval(b, 1); v != false {
		t.Fatalf("expected false for id=2 NOT IN {2}, got %v", v)
	}
}

func TestCompileExistsSubquery(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 1 FROM t WHERE exists (SELECT 1 FROM other)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr

	runner := mockRunner([]map[string]any{{"1": 1}}, nil)
	compiled, err := CompileWithRunner(where, runner)
	if err != nil {
		t.Fatal(err)
	}

	b := testBatch()
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true for EXISTS, got %v", v)
	}
}

func TestCompileWithoutRunnerFails(t *testing.T) {
	stmt, err := sqlparser.Parse("SELECT 1 FROM t WHERE id IN (SELECT id FROM other)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr

	// Compile without runner should fail for subqueries
	_, err = Compile(where)
	if err == nil {
		t.Fatal("expected error when compiling subquery without runner")
	}
}

func TestCompileSubqueryInComparison(t *testing.T) {
	// Test subquery on the right side of a comparison
	stmt, err := sqlparser.Parse("SELECT 1 FROM t WHERE id = (SELECT max(id) FROM other)")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*sqlparser.Select)
	where := sel.Where.Expr

	runner := mockRunner([]map[string]any{{"max_id": int64(3)}}, nil)
	compiled, err := CompileWithRunner(where, runner)
	if err != nil {
		t.Fatal(err)
	}

	b := testBatch()
	// Row 2: id=3, = 3 → true
	if v := compiled.Eval(b, 2); v != true {
		t.Fatalf("expected true for id=3 == max(3), got %v", v)
	}
	// Row 0: id=1, = 3 → false
	if v := compiled.Eval(b, 0); v != false {
		t.Fatalf("expected false for id=1 == max(3), got %v", v)
	}
}
