package batch

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression tests for the advance-on-NULL sweep (2026-06-12): a NULL write
// on a bytes or nested column must advance its variable-length bookkeeping
// (bytes offsets, ARRAY/MAP offsets, ROW children) or every later row in
// the column reads back shifted/concatenated.

// TestSetValueNil_RowChildAdjacency: SetValue(i, nil) on a ROW column with a
// bytes-typed child must advance the child's offset slot — the row AFTER the
// null used to read back the concatenation of all prior values.
func TestSetValueNil_RowChildAdjacency(t *testing.T) {
	schema := parquet.Column{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}
	v := newVectorFromColumn(schema, 3)
	v.SetValue(0, map[string]any{"name": "alpha"})
	v.SetValue(1, nil) // whole-row null
	v.SetValue(2, map[string]any{"name": "beta"})

	got := v.GetValue(2)
	want := map[string]any{"name": "beta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("row after null: got %#v want %#v", got, want)
	}
	if v.GetValue(1) != nil {
		t.Fatalf("null row: got %#v want nil", v.GetValue(1))
	}
}

// TestSetValueNil_TrailingNullArrayOffsets: trailing nulls on an ARRAY
// column must keep Offsets[n] equal to the child length — the spill writer
// uses Offsets[n] as the child element count, so a stale slot silently
// truncated the spilled child vector.
func TestSetValueNil_TrailingNullArrayOffsets(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	schema := parquet.Column{Name: "arr", Type: parquet.TypeArray, ElementType: &elem}
	v := newVectorFromColumn(schema, 3)
	v.SetValue(0, []any{int64(1), int64(2)})
	v.SetValue(1, nil)
	v.SetValue(2, nil) // trailing null — used to leave Offsets[3] stale at 0

	if got := v.Offsets[3]; got != int32(v.Child.Len) {
		t.Fatalf("Offsets[n] = %d, want child len %d", got, v.Child.Len)
	}
	if v.Child.Len != 2 {
		t.Fatalf("child len = %d, want 2", v.Child.Len)
	}
}

// TestReset_NestedPooledReuse: pooled batch reuse must not leak the previous
// cycle's nested child data — Reset used to skip nested columns entirely, so
// the first reused row read back the prior batch's arena concatenated with
// the new value, and child arenas grew without bound.
func TestReset_NestedPooledReuse(t *testing.T) {
	schema := []parquet.Column{
		{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "name", Type: parquet.TypeString, Nullable: true},
		}},
		{Name: "arr", Type: parquet.TypeArray, ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64}},
	}
	b := NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, map[string]any{"name": "first-cycle-a"})
	b.Columns[0].SetValue(1, map[string]any{"name": "first-cycle-b"})
	b.Columns[1].SetValue(0, []any{int64(1), int64(2), int64(3)})
	b.Columns[1].SetValue(1, []any{int64(4)})

	arenaBefore := len(b.Columns[0].Children[0].BytesData.Data)

	b.Reset(2)
	b.Columns[0].SetValue(0, map[string]any{"name": "x"})
	b.Columns[0].SetValue(1, map[string]any{"name": "y"})
	b.Columns[1].SetValue(0, []any{int64(9)})
	b.Columns[1].SetValue(1, []any{})

	if got := b.Columns[0].GetValue(0); !reflect.DeepEqual(got, map[string]any{"name": "x"}) {
		t.Fatalf("reused row 0: got %#v want {name:x}", got)
	}
	if got := b.Columns[1].GetValue(0); !reflect.DeepEqual(got, []any{int64(9)}) {
		t.Fatalf("reused arr row 0: got %#v want [9]", got)
	}
	arenaAfter := len(b.Columns[0].Children[0].BytesData.Data)
	if arenaAfter >= arenaBefore {
		t.Fatalf("child arena did not shrink on Reset: before=%d after=%d (monotonic growth)", arenaBefore, arenaAfter)
	}
	if b.Columns[1].Child.Len != 1 {
		t.Fatalf("array child len after reuse = %d, want 1 (stale elements retained)", b.Columns[1].Child.Len)
	}
}

// TestWriteNullAt_AllShapes: the exported primitive advances bookkeeping for
// flat bytes, ARRAY and ROW shapes alike.
func TestWriteNullAt_AllShapes(t *testing.T) {
	str := NewVector(parquet.TypeString, 3)
	str.SetValue(0, "a")
	str.WriteNullAt(1)
	str.SetValue(2, "c")
	if s, _ := str.GetString(2); s != "c" {
		t.Fatalf("string after WriteNullAt: got %q want %q", s, "c")
	}

	rec := newVectorFromColumn(parquet.Column{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}, 3)
	rec.SetValue(0, map[string]any{"s": "a"})
	rec.WriteNullAt(1)
	rec.SetValue(2, map[string]any{"s": "c"})
	if got := rec.GetValue(2); !reflect.DeepEqual(got, map[string]any{"s": "c"}) {
		t.Fatalf("row after WriteNullAt: got %#v", got)
	}
}
