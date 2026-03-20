package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestColumnPrune_Basic(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
		{Name: "c", Type: parquet.TypeFloat64},
		{Name: "d", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"a": int64(1), "b": "x", "c": 1.5, "d": int64(10)},
		{"a": int64(2), "b": "y", "c": 2.5, "d": int64(20)},
	}
	b := batch.FromRows(schema, rows)

	prune := NewColumnPrune([]string{"a", "c"})
	prune.Init(context.Background())
	out, err := prune.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Schema) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(out.Schema))
	}
	if out.Schema[0].Name != "a" || out.Schema[1].Name != "c" {
		t.Fatalf("expected columns [a, c], got [%s, %s]", out.Schema[0].Name, out.Schema[1].Name)
	}
	if out.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", out.Len)
	}
	// Verify zero-copy: vector pointers should be shared
	if out.Columns[0] != b.Columns[0] {
		t.Error("expected zero-copy: column 'a' vector pointer should be shared")
	}
	if out.Columns[1] != b.Columns[2] {
		t.Error("expected zero-copy: column 'c' vector pointer should be shared")
	}
}

func TestColumnPrune_KeepAll(t *testing.T) {
	schema := []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
		{Name: "y", Type: parquet.TypeInt64},
	}
	b := batch.FromRows(schema, []map[string]any{{"x": int64(1), "y": int64(2)}})

	prune := NewColumnPrune([]string{"x", "y"})
	prune.Init(context.Background())
	out, err := prune.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	// When keeping all columns, should pass through the original batch
	if out != b {
		t.Error("expected pass-through when all columns kept")
	}
}

func TestColumnPrune_SelectionVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "extra", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": int64(1), "extra": "a"},
		{"id": int64(2), "extra": "b"},
		{"id": int64(3), "extra": "c"},
	}
	b := batch.FromRows(schema, rows)
	b.Sel = []uint32{0, 2} // only rows 0 and 2

	prune := NewColumnPrune([]string{"id"})
	prune.Init(context.Background())
	out, err := prune.Execute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Schema) != 1 {
		t.Fatalf("expected 1 column, got %d", len(out.Schema))
	}
	// Selection vector should be preserved
	if len(out.Sel) != 2 || out.Sel[0] != 0 || out.Sel[1] != 2 {
		t.Errorf("expected sel [0, 2], got %v", out.Sel)
	}
}

func TestColumnPrune_Clone(t *testing.T) {
	prune := NewColumnPrune([]string{"a", "b"})
	clone := prune.Clone().(*ColumnPrune)

	if clone.resolved {
		t.Error("clone should be unresolved")
	}
	if len(clone.keepCols) != 2 || !clone.keepCols["a"] || !clone.keepCols["b"] {
		t.Errorf("clone should have same keepCols, got %v", clone.keepCols)
	}
}

func TestColumnPrune_Nil(t *testing.T) {
	prune := NewColumnPrune([]string{"a"})
	prune.Init(context.Background())
	out, err := prune.Execute(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Error("expected nil output for nil input")
	}
}
