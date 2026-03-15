package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestDistinct(t *testing.T) {
	schema := []parquet.Column{
		{Name: "city", Type: parquet.TypeString},
		{Name: "state", Type: parquet.TypeString},
	}

	rows := []map[string]any{
		{"city": "Portland", "state": "OR"},
		{"city": "Seattle", "state": "WA"},
		{"city": "Portland", "state": "OR"},
		{"city": "Seattle", "state": "WA"},
		{"city": "Portland", "state": "ME"},
	}

	source := NewSliceSource(schema, rows)
	distinct := NewDistinct()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{distinct}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Portland,OR + Seattle,WA + Portland,ME = 3 unique
	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 unique rows, got %d", len(sink.Rows))
	}
}

func TestDistinctNumeric(t *testing.T) {
	schema := []parquet.Column{
		{Name: "val", Type: parquet.TypeInt64},
	}

	rows := []map[string]any{
		{"val": int64(1)},
		{"val": int64(2)},
		{"val": int64(1)},
		{"val": int64(3)},
		{"val": int64(2)},
	}

	source := NewSliceSource(schema, rows)
	distinct := NewDistinct()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{distinct}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.Rows) != 3 {
		t.Fatalf("expected 3 unique rows, got %d", len(sink.Rows))
	}
}

func TestDistinctAfterFilter(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}

	rows := []map[string]any{
		{"name": "alice", "score": 10.0},
		{"name": "bob", "score": 20.0},
		{"name": "alice", "score": 10.0},
		{"name": "carol", "score": 5.0},
		{"name": "bob", "score": 20.0},
	}

	source := NewSliceSource(schema, rows)
	filter := NewFilter(ColumnCompare("score", OpGt, 8.0))
	distinct := NewDistinct()
	sink := &CollectSink{}

	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{filter, distinct}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// After filter: alice,10 + bob,20 + alice,10 + bob,20 = 4 rows
	// After distinct: alice,10 + bob,20 = 2 unique
	if len(sink.Rows) != 2 {
		t.Fatalf("expected 2 unique rows, got %d", len(sink.Rows))
	}
}
