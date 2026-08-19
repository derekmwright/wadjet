package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// shadowBatch is a nation scan's output: a string column and an int column,
// both of which the SELECT list below renames across each other.
func shadowBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "n_name", Type: parquet.TypeString},
		{Name: "n_regionkey", Type: parquet.TypeInt32},
		{Name: "n_comment", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Len = 3
	for i, name := range []string{"ALGERIA", "ARGENTINA", "BRAZIL"} {
		b.Columns[0].SetValue(i, name)
		b.Columns[1].SetValue(i, int32(i))
		b.Columns[2].SetValue(i, "Nation "+name+" comment")
	}
	return b
}

// TestProject_AliasShadowsDifferentlyTypedColumn is the operator-level
// regression for #323: `SELECT n_name AS n_regionkey, n_regionkey AS r`.
// The first output column is NAMED n_regionkey while its value comes from the
// string n_name. Resolving the output type by output name first typed the
// output vector from the shadowed int column, while every value path read the
// string source — the DirectCopy spelling the DAG's fragments use panicked in
// BulkCopy, and the per-row spelling the single-process planner emits wrote
// nothing at all (an all-zero int column: wrong data, no error).
//
// Both spellings are covered because the two execution paths build the same
// projection differently: the worker's fragment projections carry DirectCopy,
// while the single-process planner compiles the column reference into Expr and
// leaves DirectCopy empty.
func TestProject_AliasShadowsDifferentlyTypedColumn(t *testing.T) {
	tests := []struct {
		name  string
		projs []ProjectColumn
	}{
		{
			// The DAG spelling: a bulk-copy source.
			name: "DirectCopy",
			projs: []ProjectColumn{
				{Name: "n_regionkey", Type: parquet.TypeString, DirectCopy: "n_name", SourceCol: "n_name", Expr: ColumnRef("n_name")},
				{Name: "r", Type: parquet.TypeString, DirectCopy: "n_regionkey", SourceCol: "n_regionkey", Expr: ColumnRef("n_regionkey")},
			},
		},
		{
			// The single-process spelling: per-row evaluation, no DirectCopy.
			name: "PerRowExpr",
			projs: []ProjectColumn{
				{Name: "n_regionkey", Type: parquet.TypeString, SourceCol: "n_name", Expr: ColumnRef("n_name")},
				{Name: "r", Type: parquet.TypeString, SourceCol: "n_regionkey", Expr: ColumnRef("n_regionkey")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := shadowBatch(t)
			out, err := NewProject(tt.projs).Execute(context.Background(), in)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if out.Schema[0].Type != parquet.TypeString {
				t.Errorf("output column n_regionkey has type %v, want String — it renames the string n_name, "+
					"so the shadowed int column of the same name must not decide its type", out.Schema[0].Type)
			}
			if out.Schema[1].Type != parquet.TypeInt32 {
				t.Errorf("output column r has type %v, want Int32", out.Schema[1].Type)
			}
			for row, want := range []string{"ALGERIA", "ARGENTINA", "BRAZIL"} {
				if got := out.Columns[0].GetValue(row); got != want {
					t.Errorf("row %d: alias n_regionkey = %v, want %q (the value of the n_name it renames)", row, got, want)
				}
				if got := out.Columns[1].GetValue(row); got != int32(row) {
					t.Errorf("row %d: alias r = %v, want %d", row, got, int32(row))
				}
			}
		})
	}
}

// TestProject_AliasShadowsDifferentlyTypedColumnSelected covers the same
// shadow under a selection vector, which resolves the output type through the
// same schema pass but copies through projectGatherColumn instead.
func TestProject_AliasShadowsDifferentlyTypedColumnSelected(t *testing.T) {
	in := shadowBatch(t)
	in.Sel = []uint32{0, 2}

	out, err := NewProject([]ProjectColumn{
		{Name: "n_regionkey", Type: parquet.TypeString, DirectCopy: "n_name", SourceCol: "n_name", Expr: ColumnRef("n_name")},
		{Name: "r", Type: parquet.TypeString, DirectCopy: "n_regionkey", SourceCol: "n_regionkey", Expr: ColumnRef("n_regionkey")},
	}).Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out.Schema[0].Type != parquet.TypeString {
		t.Fatalf("output column n_regionkey has type %v, want String", out.Schema[0].Type)
	}
	want := []struct {
		name string
		key  int32
	}{{"ALGERIA", 0}, {"BRAZIL", 2}}
	for row, w := range want {
		if got := out.Columns[0].GetValue(row); got != w.name {
			t.Errorf("row %d: alias n_regionkey = %v, want %q", row, got, w.name)
		}
		if got := out.Columns[1].GetValue(row); got != w.key {
			t.Errorf("row %d: alias r = %v, want %d", row, got, w.key)
		}
	}
}

// TestProject_AliasShadowsSameTypedColumn is the control: the same shadow
// where both columns are strings (`SELECT n_name AS n_comment, n_comment AS
// c`). Preferring the explicit source over the same-name lookup must return
// the same answer it always did — the source and the shadowed column agree on
// type here, so only the VALUES distinguish a regression.
func TestProject_AliasShadowsSameTypedColumn(t *testing.T) {
	in := shadowBatch(t)
	out, err := NewProject([]ProjectColumn{
		{Name: "n_comment", Type: parquet.TypeString, DirectCopy: "n_name", SourceCol: "n_name", Expr: ColumnRef("n_name")},
		{Name: "c", Type: parquet.TypeString, DirectCopy: "n_comment", SourceCol: "n_comment", Expr: ColumnRef("n_comment")},
	}).Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for row, name := range []string{"ALGERIA", "ARGENTINA", "BRAZIL"} {
		if got := out.Columns[0].GetValue(row); got != name {
			t.Errorf("row %d: alias n_comment = %v, want %q (the n_name it renames)", row, got, name)
		}
		if want := "Nation " + name + " comment"; out.Columns[1].GetValue(row) != want {
			t.Errorf("row %d: alias c = %v, want %q", row, out.Columns[1].GetValue(row), want)
		}
	}
}

// TestProject_PlainColumnKeepsSchemaType pins the reason the same-name lookup
// exists at all: a projection that is NOT a rename carries the planner's
// placeholder type (String by default), and the input schema is what upgrades
// it. Preferring an explicit source must not cost that — here there is no
// source to prefer.
func TestProject_PlainColumnKeepsSchemaType(t *testing.T) {
	in := shadowBatch(t)
	out, err := NewProject([]ProjectColumn{
		{Name: "n_regionkey", Type: parquet.TypeString, Expr: ColumnRef("n_regionkey")},
	}).Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out.Schema[0].Type != parquet.TypeInt32 {
		t.Fatalf("n_regionkey has type %v, want Int32 resolved from the input schema", out.Schema[0].Type)
	}
	for row := 0; row < out.Len; row++ {
		if got := out.Columns[0].GetValue(row); got != int32(row) {
			t.Errorf("row %d: n_regionkey = %v, want %d", row, got, int32(row))
		}
	}
}
