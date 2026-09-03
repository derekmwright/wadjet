package expr

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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

// TestScalarSubqueryError: a subquery that could not be RUN is not a subquery
// whose value is NULL.
//
// This test used to assert the opposite — `expected nil on error` — which is
// the shape a test takes when it was written from the implementation. NULL is
// a value: it makes every comparison above it UNKNOWN and the row silently
// vanishes, so a failed subquery answered "no rows" with no error anywhere
// (#734/#679/#535, protocol item 8). It fails the query now, through the
// FatalEvalError channel the pipeline recovers into a query error.
func TestScalarSubqueryError(t *testing.T) {
	b := testBatch()
	runner := mockRunner(nil, fmt.Errorf("table not found"))

	sq := &ScalarSubquery{SQL: "SELECT 1 FROM nonexistent", Runner: runner}
	err := catchFatalEval(t, func() { sq.Eval(b, 0) })
	if err == nil {
		t.Fatal("a scalar subquery whose runner failed answered a value instead of failing")
	}
	var sfe *SubqueryRunFailedError
	if !errors.As(err, &sfe) {
		t.Fatalf("got %T (%v), want *SubqueryRunFailedError", err, err)
	}
	if !strings.Contains(err.Error(), "table not found") {
		t.Errorf("the error does not carry the runner's own message: %v", err)
	}
}

// TestSubqueryDanglingReferenceIsRefused: an UNCORRELATED evaluator handed a
// subquery whose text still names a relation it does not read is a
// misclassified CORRELATED subquery. Run standalone it does not fail — the
// resolver strips the qualifier and rebinds — so it answers a query-wide
// constant. It is refused instead (#734/#679/#535).
func TestSubqueryDanglingReferenceIsRefused(t *testing.T) {
	b := testBatch()
	called := 0
	runner := func(string) ([]map[string]any, error) {
		called++
		return []map[string]any{{"n": int64(1)}}, nil
	}
	for _, tc := range []struct {
		name string
		eval func()
	}{
		{"exists", func() {
			(&ExistsSubquery{SQL: "SELECT 1 FROM inner_t y WHERE y.id = outer_t.id", Runner: runner}).EvalBool(b, 0)
		}},
		{"scalar", func() {
			(&ScalarSubquery{SQL: "SELECT COUNT(*) FROM inner_t y WHERE y.id = outer_t.id", Runner: runner}).Eval(b, 0)
		}},
		{"in", func() {
			(&InSubquery{Expr: &ColRef{Name: "id"},
				SQL: "SELECT y.id FROM inner_t y WHERE y.id = outer_t.id", Runner: runner}).EvalBool(b, 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := called
			err := catchFatalEval(t, tc.eval)
			if err == nil {
				t.Fatal("a subquery correlated on an outer relation was executed as " +
					"uncorrelated and answered a constant")
			}
			var dse *DanglingSubqueryError
			if !errors.As(err, &dse) {
				t.Fatalf("got %T (%v), want *DanglingSubqueryError", err, err)
			}
			if called != before {
				t.Error("the subquery was RUN before it was refused; the guard has to " +
					"fire before the runner, or the failure depends on what the run " +
					"happens to answer")
			}
			if dse.SQLState() != "0A000" {
				t.Errorf("SQLSTATE %s, want 0A000 (feature_not_supported)", dse.SQLState())
			}
		})
	}
}

// TestUncorrelatedSubqueryWithNoDanglingRefStillRuns is the boundary the
// guard above claims: a genuinely uncorrelated subquery, qualified references
// and all, is untouched by it.
func TestUncorrelatedSubqueryWithNoDanglingRefStillRuns(t *testing.T) {
	b := testBatch()
	runner := mockRunner([]map[string]any{{"n": int64(7)}}, nil)
	sq := &ScalarSubquery{SQL: "SELECT COUNT(*) AS n FROM inner_t y WHERE y.id = 2", Runner: runner}
	if err := catchFatalEval(t, func() { sq.Eval(b, 0) }); err != nil {
		t.Fatalf("an uncorrelated subquery was refused: %v", err)
	}
	if got := sq.Eval(b, 0); got != int64(7) {
		t.Errorf("value = %v, want 7", got)
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

// TestExistsSubqueryError: like TestScalarSubqueryError, this asserted the
// old fold — `expected EXISTS to be false on error`, which is the whole of
// `runErr == nil && len(rows) > 0`. A subquery that could not be RUN did not
// "not exist"; nobody was told anything (#734/#679/#535).
func TestExistsSubqueryError(t *testing.T) {
	b := testBatch()
	runner := mockRunner(nil, fmt.Errorf("boom"))
	exists := &ExistsSubquery{SQL: "SELECT 1", Runner: runner}
	err := catchFatalEval(t, func() { exists.EvalBool(b, 0) })
	if err == nil {
		t.Fatal("an EXISTS whose runner failed answered FALSE instead of failing")
	}
	var sfe *SubqueryRunFailedError
	if !errors.As(err, &sfe) {
		t.Fatalf("got %T (%v), want *SubqueryRunFailedError", err, err)
	}
}

// --- Compiler tests for subqueries ---

// compileWhereWithRunner parses SQL and compiles the WHERE clause with a SubqueryRunner.
func compileWhereWithRunner(t *testing.T, sql string, runner SubqueryRunner) Expr {
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
	compiled, err := CompileWithRunner(info.WhereExpr, runner)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestCompileScalarSubquery(t *testing.T) {
	runner := mockRunner([]map[string]any{{"1": int64(50)}}, nil)
	compiled := compileWhereWithRunner(t, "SELECT 1 FROM t WHERE amount > (SELECT 1)", runner)

	b := testBatch()
	// Row 0: amount=100.5 > 50 → true
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true, got %v", v)
	}
}

func TestCompileInSubquery(t *testing.T) {
	runner := mockRunner([]map[string]any{
		{"id": int64(1)},
		{"id": int64(3)},
	}, nil)
	compiled := compileWhereWithRunner(t, "SELECT 1 FROM t WHERE id IN (SELECT id FROM other)", runner)

	b := testBatch()
	// Row 0: id=1, IN {1,3} → true
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true for id=1, got %v", v)
	}
	// Row 1: id=2, IN {1,3} → false
	if v := compiled.Eval(b, 1); v != false {
		t.Fatalf("expected false for id=2, got %v", v)
	}
}

func TestCompileNotInSubquery(t *testing.T) {
	runner := mockRunner([]map[string]any{
		{"id": int64(2)},
	}, nil)
	compiled := compileWhereWithRunner(t, "SELECT 1 FROM t WHERE id NOT IN (SELECT id FROM other)", runner)

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
	runner := mockRunner([]map[string]any{{"1": 1}}, nil)
	compiled := compileWhereWithRunner(t, "SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM other)", runner)

	b := testBatch()
	if v := compiled.Eval(b, 0); v != true {
		t.Fatalf("expected true for EXISTS, got %v", v)
	}
}

func TestCompileWithoutRunnerFails(t *testing.T) {
	parsed, err := plansql.Parse("SELECT 1 FROM t WHERE id IN (SELECT id FROM other)")
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

	// Compile without runner should fail for subqueries
	_, err = Compile(info.WhereExpr)
	if err == nil {
		t.Fatal("expected error when compiling subquery without runner")
	}
}

func TestCompileSubqueryInComparison(t *testing.T) {
	// Test subquery on the right side of a comparison
	runner := mockRunner([]map[string]any{{"max_id": int64(3)}}, nil)
	compiled := compileWhereWithRunner(t, "SELECT 1 FROM t WHERE id = (SELECT max(id) FROM other)", runner)

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
