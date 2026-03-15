package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
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
	limit := NewLimit(5)
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{limit}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(sink.Rows))
	}
}

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
