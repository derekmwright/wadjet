package coordinator

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// mergeScalarAggHelper creates a minimal Coordinator and calls mergeScalarAggregates.
// The method only uses its receiver for dispatch, not internal state, so a zero-value works.
func mergeScalarAggHelper(t *testing.T, batches []*batch.RecordBatch, columns []string, mi *logical.MergeInfo) []*batch.RecordBatch {
	t.Helper()
	colIdx := make(map[string]int, len(columns))
	for i, col := range columns {
		colIdx[col] = i
	}
	c := &Coordinator{}
	return c.mergeScalarAggregates(batches, columns, colIdx, mi)
}

func TestMergeScalarAggregatesSinglePartial(t *testing.T) {
	// Single worker partial → passthrough (no merge needed)
	schema := []parquet.Column{
		{Name: "total", Type: parquet.TypeFloat64},
		{Name: "cnt", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"total": float64(100), "cnt": int64(5)},
	})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "sum", OutputCol: "total"},
			{Func: "count", OutputCol: "cnt"},
		},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b}, []string{"total", "cnt"}, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	// Single partial passes through unchanged
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestMergeScalarAggregatesSumCount(t *testing.T) {
	// Two worker partials: SUM(total) and COUNT(cnt) should be summed
	schema := []parquet.Column{
		{Name: "total", Type: parquet.TypeFloat64},
		{Name: "cnt", Type: parquet.TypeFloat64},
	}
	b1 := batch.FromRows(schema, []map[string]any{
		{"total": float64(100), "cnt": float64(5)},
	})
	b2 := batch.FromRows(schema, []map[string]any{
		{"total": float64(200), "cnt": float64(10)},
	})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "sum", OutputCol: "total"},
			{Func: "count", OutputCol: "cnt"},
		},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"total", "cnt"}, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 merged row, got %d", len(rows))
	}

	total, ok := rows[0]["total"].(float64)
	if !ok {
		t.Fatalf("total is not float64: %T", rows[0]["total"])
	}
	if total != 300 {
		t.Errorf("SUM(total): got %v, want 300", total)
	}

	cnt, ok := rows[0]["cnt"].(float64)
	if !ok {
		t.Fatalf("cnt is not float64: %T", rows[0]["cnt"])
	}
	if cnt != 15 {
		t.Errorf("COUNT(cnt): got %v, want 15", cnt)
	}
}

func TestMergeScalarAggregatesMinMax(t *testing.T) {
	schema := []parquet.Column{
		{Name: "lo", Type: parquet.TypeFloat64},
		{Name: "hi", Type: parquet.TypeFloat64},
	}
	b1 := batch.FromRows(schema, []map[string]any{
		{"lo": float64(10), "hi": float64(50)},
	})
	b2 := batch.FromRows(schema, []map[string]any{
		{"lo": float64(5), "hi": float64(80)},
	})
	b3 := batch.FromRows(schema, []map[string]any{
		{"lo": float64(20), "hi": float64(30)},
	})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "min", OutputCol: "lo"},
			{Func: "max", OutputCol: "hi"},
		},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2, b3}, []string{"lo", "hi"}, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	lo, ok := rows[0]["lo"].(float64)
	if !ok {
		t.Fatalf("lo is not float64: %T", rows[0]["lo"])
	}
	if lo != 5 {
		t.Errorf("MIN(lo): got %v, want 5", lo)
	}

	hi, ok := rows[0]["hi"].(float64)
	if !ok {
		t.Fatalf("hi is not float64: %T", rows[0]["hi"])
	}
	if hi != 80 {
		t.Errorf("MAX(hi): got %v, want 80", hi)
	}
}

func TestMergeScalarAggregatesAvgFallback(t *testing.T) {
	// AVG takes the first non-nil value (documented as "imprecise but safe")
	schema := []parquet.Column{
		{Name: "avg_price", Type: parquet.TypeFloat64},
	}
	b1 := batch.FromRows(schema, []map[string]any{
		{"avg_price": float64(25.5)},
	})
	b2 := batch.FromRows(schema, []map[string]any{
		{"avg_price": float64(30.0)},
	})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "avg", OutputCol: "avg_price"},
		},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"avg_price"}, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	avg, ok := rows[0]["avg_price"].(float64)
	if !ok {
		t.Fatalf("avg_price is not float64: %T", rows[0]["avg_price"])
	}
	// AVG fallback takes first non-nil value
	if avg != 25.5 {
		t.Errorf("AVG fallback: got %v, want 25.5 (first non-nil)", avg)
	}
}

func TestMergeScalarAggregatesEmptyBatchFromWorker(t *testing.T) {
	// One worker returns data, another returns an empty batch.
	// The empty batch contributes 0 rows via ToRows(), so the merge
	// sees only 1 total row and passes through unchanged.
	schema := []parquet.Column{
		{Name: "total", Type: parquet.TypeFloat64},
		{Name: "cnt", Type: parquet.TypeFloat64},
	}
	b1 := batch.FromRows(schema, []map[string]any{
		{"total": float64(100), "cnt": float64(5)},
	})
	b2 := batch.NewRecordBatch(schema, 0) // empty batch

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "sum", OutputCol: "total"},
			{Func: "count", OutputCol: "cnt"},
		},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"total", "cnt"}, mi)
	// With only 1 real row total (empty batch contributes 0), passthrough is correct
	if len(result) != 2 {
		t.Fatalf("expected 2 batches (passthrough), got %d", len(result))
	}
	// The first batch should still contain the original data
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in first batch, got %d", len(rows))
	}
	total, ok := rows[0]["total"].(float64)
	if !ok {
		t.Fatalf("total is not float64: %T", rows[0]["total"])
	}
	if total != 100 {
		t.Errorf("SUM with empty worker: got %v, want 100", total)
	}
}

func TestMergeScalarAggregatesTwoEmptyBatches(t *testing.T) {
	// Both workers return empty batches — 0 total rows, passthrough
	schema := []parquet.Column{{Name: "total", Type: parquet.TypeFloat64}}
	b1 := batch.NewRecordBatch(schema, 0)
	b2 := batch.NewRecordBatch(schema, 0)

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs:     []logical.AggExpr{{Func: "sum", OutputCol: "total"}},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"total"}, mi)
	// 0 rows total → passthrough
	if len(result) != 2 {
		t.Fatalf("expected 2 batches (passthrough), got %d", len(result))
	}
}

func TestMergeScalarAggregatesNoBatches(t *testing.T) {
	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "sum", OutputCol: "total"},
		},
	}

	result := mergeScalarAggHelper(t, nil, []string{"total"}, mi)
	if result != nil {
		t.Errorf("expected nil for empty batches, got %d batches", len(result))
	}
}

func TestMergeScalarAggregatesNoAggExprs(t *testing.T) {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeFloat64}}
	b := batch.FromRows(schema, []map[string]any{{"x": float64(1)}})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs:     nil,
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b}, []string{"x"}, mi)
	// No agg expressions → passthrough
	if len(result) != 1 {
		t.Errorf("expected 1 batch passthrough, got %d", len(result))
	}
}

func TestMergeScalarAggregatesSumInt64(t *testing.T) {
	// Verify int64 values are properly summed (the switch in sum handles int64).
	// Note: mergeScalarAggregates computes as float64 internally, but FromRows
	// with TypeInt64 schema stores the result as int64. So ToRows() returns int64.
	schema := []parquet.Column{{Name: "cnt", Type: parquet.TypeInt64}}
	b1 := batch.FromRows(schema, []map[string]any{{"cnt": int64(100)}})
	b2 := batch.FromRows(schema, []map[string]any{{"cnt": int64(200)}})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs:     []logical.AggExpr{{Func: "count", OutputCol: "cnt"}},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"cnt"}, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// The merged value is float64(300) internally, but FromRows with TypeInt64
	// schema truncates to int64.
	switch v := rows[0]["cnt"].(type) {
	case int64:
		if v != 300 {
			t.Errorf("COUNT(cnt): got %d, want 300", v)
		}
	case float64:
		if v != 300 {
			t.Errorf("COUNT(cnt): got %v, want 300", v)
		}
	default:
		t.Fatalf("cnt unexpected type: %T", rows[0]["cnt"])
	}
}

func TestMergeScalarAggregatesMinWithNil(t *testing.T) {
	// One worker returns nil for a column (e.g., empty partition)
	schema := []parquet.Column{{Name: "lo", Type: parquet.TypeFloat64}}
	b1 := batch.FromRows(schema, []map[string]any{{"lo": nil}})
	b2 := batch.FromRows(schema, []map[string]any{{"lo": float64(42)}})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs:     []logical.AggExpr{{Func: "min", OutputCol: "lo"}},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2}, []string{"lo"}, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	lo, ok := rows[0]["lo"].(float64)
	if !ok {
		t.Fatalf("lo type: %T", rows[0]["lo"])
	}
	if lo != 42 {
		t.Errorf("MIN with nil partial: got %v, want 42", lo)
	}
}

func TestMergeScalarAggregatesMixedFunctions(t *testing.T) {
	// Test a realistic scenario: SUM, COUNT, MIN, MAX in one merge
	schema := []parquet.Column{
		{Name: "revenue", Type: parquet.TypeFloat64},
		{Name: "orders", Type: parquet.TypeFloat64},
		{Name: "min_price", Type: parquet.TypeFloat64},
		{Name: "max_price", Type: parquet.TypeFloat64},
	}
	columns := []string{"revenue", "orders", "min_price", "max_price"}

	b1 := batch.FromRows(schema, []map[string]any{
		{"revenue": float64(1000), "orders": float64(10), "min_price": float64(5.5), "max_price": float64(99.9)},
	})
	b2 := batch.FromRows(schema, []map[string]any{
		{"revenue": float64(2000), "orders": float64(20), "min_price": float64(3.0), "max_price": float64(150.0)},
	})
	b3 := batch.FromRows(schema, []map[string]any{
		{"revenue": float64(500), "orders": float64(5), "min_price": float64(10.0), "max_price": float64(50.0)},
	})

	mi := &logical.MergeInfo{
		HasAggregate: true,
		AggExprs: []logical.AggExpr{
			{Func: "sum", OutputCol: "revenue"},
			{Func: "count", OutputCol: "orders"},
			{Func: "min", OutputCol: "min_price"},
			{Func: "max", OutputCol: "max_price"},
		},
	}

	result := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2, b3}, columns, mi)
	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	rows := result[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	tests := []struct {
		col  string
		want float64
	}{
		{"revenue", 3500},
		{"orders", 35},
		{"min_price", 3.0},
		{"max_price", 150.0},
	}
	for _, tt := range tests {
		v, ok := row[tt.col].(float64)
		if !ok {
			t.Errorf("%s: unexpected type %T", tt.col, row[tt.col])
			continue
		}
		if math.Abs(v-tt.want) > 0.001 {
			t.Errorf("%s: got %v, want %v", tt.col, v, tt.want)
		}
	}
}
