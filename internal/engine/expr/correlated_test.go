package expr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// correlatedBatch creates a test batch with id and customer_id columns.
func correlatedBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "customer_id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	b := batch.NewRecordBatch(schema, 3)
	// Row 0: id=1, customer_id=10, amount=100.0
	b.Columns[0].SetValue(0, int64(1))
	b.Columns[1].SetValue(0, int64(10))
	b.Columns[2].SetValue(0, 100.0)
	// Row 1: id=2, customer_id=20, amount=200.0
	b.Columns[0].SetValue(1, int64(2))
	b.Columns[1].SetValue(1, int64(20))
	b.Columns[2].SetValue(1, 200.0)
	// Row 2: id=3, customer_id=10, amount=50.0
	b.Columns[0].SetValue(2, int64(3))
	b.Columns[1].SetValue(2, int64(10))
	b.Columns[2].SetValue(2, 50.0)
	b.Len = 3
	return b
}

// correlatedRunner returns a SubqueryRunner that checks the SQL for expected
// parameter substitution and returns appropriate results.
func correlatedRunner() SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		lower := strings.ToLower(sql)
		// Check for customer_id = 10 or customer_id = 20
		if strings.Contains(lower, "customer_id = 10") {
			return []map[string]any{{"avg_amount": 75.0}}, nil
		}
		if strings.Contains(lower, "customer_id = 20") {
			return []map[string]any{{"avg_amount": 200.0}}, nil
		}
		return nil, fmt.Errorf("unexpected SQL: %s", sql)
	}
}

func TestCorrelatedScalarSubquery(t *testing.T) {
	b := correlatedBatch()

	// Parse the subquery
	subSQL := "SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id"
	parsed, err := plansql.Parse(subSQL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	outerTables := map[string]bool{"o": true}
	outerRefs := []plansql.OuterRef{{Table: "o", Column: "customer_id"}}

	sq := &CorrelatedScalarSubquery{
		Runner:      correlatedRunner(),
		OuterRefs:   outerRefs,
		OuterTables: outerTables,
		ParsedInfo:  info,
	}

	// Row 0: customer_id=10 → avg=75.0
	val0 := sq.Eval(b, 0)
	if val0 == nil {
		t.Fatal("expected non-nil for row 0")
	}
	if f, ok := val0.(float64); !ok || f != 75.0 {
		t.Fatalf("row 0: expected 75.0, got %v", val0)
	}

	// Row 1: customer_id=20 → avg=200.0
	val1 := sq.Eval(b, 1)
	if f, ok := val1.(float64); !ok || f != 200.0 {
		t.Fatalf("row 1: expected 200.0, got %v", val1)
	}

	// Row 2: customer_id=10 → avg=75.0 (same as row 0, but NOT cached)
	val2 := sq.Eval(b, 2)
	if f, ok := val2.(float64); !ok || f != 75.0 {
		t.Fatalf("row 2: expected 75.0, got %v", val2)
	}
}

func TestCorrelatedExistsSubquery(t *testing.T) {
	b := correlatedBatch()

	subSQL := "SELECT 1 FROM orders WHERE customer_id = o.customer_id"
	parsed, err := plansql.Parse(subSQL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	outerTables := map[string]bool{"o": true}
	outerRefs := []plansql.OuterRef{{Table: "o", Column: "customer_id"}}

	// EXISTS: customer_id=10 has rows, customer_id=20 has rows
	runner := func(sql string) ([]map[string]any, error) {
		if strings.Contains(sql, "customer_id = 10") {
			return []map[string]any{{"1": 1}}, nil
		}
		if strings.Contains(sql, "customer_id = 20") {
			return nil, nil // no rows for customer 20
		}
		return nil, fmt.Errorf("unexpected: %s", sql)
	}

	exists := &CorrelatedExistsSubquery{
		Runner:      runner,
		OuterRefs:   outerRefs,
		OuterTables: outerTables,
		ParsedInfo:  info,
	}

	// Row 0: customer_id=10 → EXISTS true
	if !exists.EvalBool(b, 0) {
		t.Fatal("row 0: expected EXISTS true")
	}
	// Row 1: customer_id=20 → EXISTS false
	if exists.EvalBool(b, 1) {
		t.Fatal("row 1: expected EXISTS false")
	}
}

func TestCorrelatedExistsNotSubquery(t *testing.T) {
	b := correlatedBatch()

	subSQL := "SELECT 1 FROM orders WHERE customer_id = o.customer_id"
	parsed, err := plansql.Parse(subSQL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	outerTables := map[string]bool{"o": true}
	outerRefs := []plansql.OuterRef{{Table: "o", Column: "customer_id"}}

	runner := func(sql string) ([]map[string]any, error) {
		if strings.Contains(sql, "customer_id = 10") {
			return []map[string]any{{"1": 1}}, nil
		}
		return nil, nil
	}

	notExists := &CorrelatedExistsSubquery{
		Runner:      runner,
		Not:         true,
		OuterRefs:   outerRefs,
		OuterTables: outerTables,
		ParsedInfo:  info,
	}

	// Row 0: customer_id=10 → NOT EXISTS false
	if notExists.EvalBool(b, 0) {
		t.Fatal("row 0: expected NOT EXISTS false")
	}
	// Row 1: customer_id=20 → NOT EXISTS true
	if !notExists.EvalBool(b, 1) {
		t.Fatal("row 1: expected NOT EXISTS true")
	}
}

func TestCorrelatedInSubquery(t *testing.T) {
	b := correlatedBatch()

	subSQL := "SELECT order_id FROM line_items WHERE customer_id = o.customer_id"
	parsed, err := plansql.Parse(subSQL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	outerTables := map[string]bool{"o": true}
	outerRefs := []plansql.OuterRef{{Table: "o", Column: "customer_id"}}

	runner := func(sql string) ([]map[string]any, error) {
		if strings.Contains(sql, "customer_id = 10") {
			return []map[string]any{
				{"order_id": int64(1)},
				{"order_id": int64(3)},
			}, nil
		}
		return nil, nil // no orders for customer 20
	}

	inSq := &CorrelatedInSubquery{
		Expr:        &ColRef{Name: "id"},
		Runner:      runner,
		OuterRefs:   outerRefs,
		OuterTables: outerTables,
		ParsedInfo:  info,
	}

	// Row 0: id=1, customer_id=10 → IN {1,3} → true
	if !inSq.EvalBool(b, 0) {
		t.Fatal("row 0: expected id=1 IN correlated result")
	}
	// Row 1: id=2, customer_id=20 → IN {} → false
	if inSq.EvalBool(b, 1) {
		t.Fatal("row 1: expected id=2 NOT IN correlated result")
	}
	// Row 2: id=3, customer_id=10 → IN {1,3} → true
	if !inSq.EvalBool(b, 2) {
		t.Fatal("row 2: expected id=3 IN correlated result")
	}
}

func TestCompileCorrelatedSubquery(t *testing.T) {
	outerTables := map[string]bool{"o": true, "orders": true}

	// Create a runner that echoes back the parameterized SQL for verification
	runner := func(sql string) ([]map[string]any, error) {
		if strings.Contains(sql, "customer_id = 42") {
			return []map[string]any{{"avg": 100.0}}, nil
		}
		return nil, fmt.Errorf("expected parameterized SQL, got: %s", sql)
	}

	// Parse outer query
	sql := "SELECT * FROM orders o WHERE amount > (SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id)"
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if info.WhereExpr == nil {
		t.Fatal("no WHERE expression")
	}

	// Compile with outer scope
	compiled, err := CompileWithScope(info.WhereExpr, runner, outerTables)
	if err != nil {
		t.Fatal(err)
	}

	// The compiled expression should contain a CorrelatedScalarSubquery
	// Verify by evaluating with a batch that has customer_id=42
	schema := []parquet.Column{
		{Name: "amount", Type: parquet.TypeFloat64},
		{Name: "customer_id", Type: parquet.TypeInt64},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, 150.0)
	b.Columns[1].SetValue(0, int64(42))
	b.Len = 1

	// amount=150 > avg=100 → true
	result := compiled.Eval(b, 0)
	if result != true {
		t.Fatalf("expected true (150 > 100), got %v", result)
	}
}

func TestCompileUncorrelatedWithOuterScope(t *testing.T) {
	// When outer tables are provided but the subquery is uncorrelated,
	// it should still compile as a normal cached subquery.
	outerTables := map[string]bool{"o": true}

	callCount := 0
	runner := func(sql string) ([]map[string]any, error) {
		callCount++
		return []map[string]any{{"avg": 100.0}}, nil
	}

	sql := "SELECT * FROM orders o WHERE amount > (SELECT AVG(amount) FROM orders)"
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	compiled, err := CompileWithScope(info.WhereExpr, runner, outerTables)
	if err != nil {
		t.Fatal(err)
	}

	b := correlatedBatch()
	compiled.Eval(b, 0)
	compiled.Eval(b, 1)

	// Uncorrelated subquery should only execute once (cached)
	if callCount != 1 {
		t.Fatalf("uncorrelated subquery executed %d times, expected 1", callCount)
	}
}
