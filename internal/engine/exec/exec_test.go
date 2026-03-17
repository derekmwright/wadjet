package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestFilter(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"id": int64(1), "value": 10.0},
		{"id": int64(2), "value": 25.0},
		{"id": int64(3), "value": 5.0},
		{"id": int64(4), "value": 30.0},
	}

	source := NewSliceSource(schema, rows)
	filter := NewFilter(ColumnCompare("value", OpGt, 15.0))
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{filter}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(sink.Rows))
	}
}

func TestHashAggregate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"group": "a", "amount": 10.0},
		{"group": "b", "amount": 20.0},
		{"group": "a", "amount": 30.0},
		{"group": "b", "amount": 40.0},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggSum, InputCol: "amount", OutputCol: "total", OutputType: parquet.TypeFloat64},
		{Func: AggCount, InputCol: "amount", OutputCol: "cnt", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	sink := &CollectSink{}

	// Run source -> agg (as sink)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read from agg (as source)
	ctx := context.Background()
	b, err := agg.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = sink

	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	resultRows := b.ToRows()
	if len(resultRows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resultRows))
	}

	// Find group "a"
	for _, row := range resultRows {
		if row["group"] == "a" {
			total, ok := row["total"].(float64)
			if !ok || total != 40.0 {
				t.Fatalf("expected total=40.0 for group 'a', got %v", row["total"])
			}
			cnt, ok := row["cnt"].(int64)
			if !ok || cnt != 2 {
				t.Fatalf("expected cnt=2 for group 'a', got %v", row["cnt"])
			}
		}
	}
}

func TestHashAggregateCountDistinct(t *testing.T) {
	schema := []parquet.Column{
		{Name: "group", Type: parquet.TypeString},
		{Name: "status", Type: parquet.TypeString},
	}

	rows := []map[string]any{
		{"group": "a", "status": "active"},
		{"group": "a", "status": "active"},   // duplicate
		{"group": "a", "status": "inactive"},
		{"group": "b", "status": "active"},
		{"group": "b", "status": "active"},   // duplicate
		{"group": "b", "status": "active"},   // duplicate
		{"group": "b", "status": "pending"},
	}

	agg := NewHashAggregate([]string{"group"}, []AggColumn{
		{Func: AggCountDistinct, InputCol: "status", OutputCol: "distinct_statuses", OutputType: parquet.TypeInt64},
		{Func: AggCount, InputCol: "status", OutputCol: "total_count", OutputType: parquet.TypeInt64},
	})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: agg}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	b, err := agg.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	resultRows := b.ToRows()
	if len(resultRows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resultRows))
	}

	for _, row := range resultRows {
		g := row["group"]
		dc := row["distinct_statuses"].(int64)
		tc := row["total_count"].(int64)
		switch g {
		case "a":
			if dc != 2 {
				t.Errorf("group 'a': expected 2 distinct statuses, got %d", dc)
			}
			if tc != 3 {
				t.Errorf("group 'a': expected total count 3, got %d", tc)
			}
		case "b":
			if dc != 2 {
				t.Errorf("group 'b': expected 2 distinct statuses, got %d", dc)
			}
			if tc != 4 {
				t.Errorf("group 'b': expected total count 4, got %d", tc)
			}
		default:
			t.Errorf("unexpected group: %v", g)
		}
	}
}

func TestSort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"name": "charlie", "score": 75.0},
		{"name": "alice", "score": 95.0},
		{"name": "bob", "score": 85.0},
	}

	sortOp := NewSort([]SortKey{{Column: "score", Order: Descending}})

	source := NewSliceSource(schema, rows)
	pipe := &Pipeline{Source: source, Ops: nil, Sink: sortOp}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	result, err := sortOp.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	resultRows := result.ToRows()
	if len(resultRows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(resultRows))
	}
	if resultRows[0]["score"].(float64) != 95.0 {
		t.Fatalf("expected first score=95.0, got %v", resultRows[0]["score"])
	}
	if resultRows[2]["score"].(float64) != 75.0 {
		t.Fatalf("expected last score=75.0, got %v", resultRows[2]["score"])
	}
}

func TestLimit(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
	}

	source := NewSliceSource(schema, rows)
	limit := NewLimit(5, 0)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}
}

func TestLimitOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	var rows []map[string]any
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
	}

	source := NewSliceSource(schema, rows)
	limit := NewLimit(5, 10) // skip 10, take 5
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}
	// First row should be id=10 (after skipping 10)
	if id, ok := sink.Rows[0]["id"].(int64); !ok || id != 10 {
		t.Errorf("expected first row id=10, got %v", sink.Rows[0]["id"])
	}
	// Last row should be id=14
	if id, ok := sink.Rows[4]["id"].(int64); !ok || id != 14 {
		t.Errorf("expected last row id=14, got %v", sink.Rows[4]["id"])
	}
}

func TestTopNSort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeFloat64},
	}

	var rows []map[string]any
	for i := 0; i < 1000; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "val": float64(1000 - i)})
	}

	source := NewSliceSource(schema, rows)
	sortOp := NewSort([]SortKey{{Column: "val", Order: Ascending}})
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Sink: sortOp}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	sortOp.Truncate(5) // Top-5

	var result []map[string]any
	for {
		b, err := sortOp.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		result = append(result, b.ToRows()...)
	}
	_ = sink

	if len(result) != 5 {
		t.Fatalf("expected 5 rows after TopN, got %d", len(result))
	}
	// Ascending sort: smallest val first. val=1 (id=999), val=2 (id=998), etc.
	if v, ok := result[0]["val"].(float64); !ok || v != 1.0 {
		t.Errorf("expected first val=1.0, got %v", result[0]["val"])
	}
	if v, ok := result[4]["val"].(float64); !ok || v != 5.0 {
		t.Errorf("expected last val=5.0, got %v", result[4]["val"])
	}
}

func TestLimit_EarlyTermination(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	// Create 10,000 rows — LIMIT 5 should stop pulling after first batch
	rows := make([]map[string]any, 10000)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}

	source := &countingSource{inner: NewSliceSource(schema, rows)}
	limit := NewLimit(5, 0)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}

	// With 10,000 rows at batch size 2048, without early termination we'd
	// need 5 batches. With early termination, only 1 batch should be read
	// (first batch has 2048 rows, LIMIT 5 is satisfied).
	if source.nextCalls > 2 {
		t.Errorf("expected ≤2 source.Next() calls (early termination), got %d", source.nextCalls)
	}
}

func TestLimit_WithOffset(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}

	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}

	source := NewSliceSource(schema, rows)
	limit := NewLimit(3, 10) // OFFSET 10 LIMIT 3
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(sink.Rows))
	}
	// First row should be id=10 (offset skips 0-9)
	if sink.Rows[0]["id"].(int64) != 10 {
		t.Errorf("expected first id=10, got %v", sink.Rows[0]["id"])
	}
}

func TestLimit_Done(t *testing.T) {
	limit := NewLimit(10, 0)
	if limit.Done() {
		t.Error("should not be done initially")
	}
}

// countingSource wraps a Source and counts Next() calls.
type countingSource struct {
	inner     Source
	nextCalls int
}

func (c *countingSource) Init(ctx context.Context) error { return c.inner.Init(ctx) }
func (c *countingSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	c.nextCalls++
	return c.inner.Next(ctx)
}
func (c *countingSource) Close() error { return c.inner.Close() }

func TestProject(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeFloat64},
		{Name: "b", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"a": 10.0, "b": 3.0},
		{"a": 20.0, "b": 7.0},
	}

	proj := NewProject([]ProjectColumn{
		{Name: "a", Type: parquet.TypeFloat64, Expr: ColumnRef("a")},
		{Name: "sum_ab", Type: parquet.TypeFloat64, Expr: ArithExpr(ColumnRef("a"), ColumnRef("b"), "+")},
	})

	source := NewSliceSource(schema, rows)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{proj}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(sink.Rows))
	}
	if sink.Rows[0]["sum_ab"].(float64) != 13.0 {
		t.Fatalf("expected sum_ab=13.0, got %v", sink.Rows[0]["sum_ab"])
	}
}

func TestColumnLike(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
	}

	rows := []map[string]any{
		{"name": "alice"},
		{"name": "bob"},
		{"name": "alex"},
		{"name": "carol"},
		{"name": "ali"},
	}

	tests := []struct {
		name     string
		pattern  string
		not      bool
		expected int
	}{
		{"prefix", "al%", false, 3},      // alice, alex, ali
		{"suffix", "%ob", false, 1},       // bob
		{"contains", "%li%", false, 2},    // alice, ali
		{"single char", "al_x", false, 1}, // alex
		{"exact", "bob", false, 1},
		{"not like", "al%", true, 2}, // bob, carol
		{"no match", "xyz%", false, 0},
		{"match all", "%", false, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := NewSliceSource(schema, rows)
			filter := NewFilter(ColumnLike("name", tt.pattern, tt.not))
			sink := &CollectSink{}
			pipe := &Pipeline{Source: source, Ops: []UnaryOperator{filter}, Sink: sink}
			if err := pipe.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(sink.Rows) != tt.expected {
				t.Fatalf("pattern %q not=%v: expected %d rows, got %d", tt.pattern, tt.not, tt.expected, len(sink.Rows))
			}
		})
	}
}
