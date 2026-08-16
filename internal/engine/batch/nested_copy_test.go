package batch

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func nestedSchemaForTest() []parquet.Column {
	elem := parquet.Column{Name: "element", Type: parquet.TypeString}
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "tags", Type: parquet.TypeArray, ElementType: &elem, Nullable: true},
		{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeString, Nullable: true},
		}},
	}
}

func nestedRowsForTest() []map[string]any {
	return []map[string]any{
		{"id": int64(0), "tags": []any{"t0", "u0"}, "rec": map[string]any{"a": int64(0), "b": "b0"}},
		{"id": int64(1), "tags": nil, "rec": map[string]any{"a": int64(2), "b": nil}}, // null array + NULL row field
		{"id": int64(2), "tags": []any{}, "rec": map[string]any{"a": int64(4), "b": "b2"}},
		{"id": int64(3), "tags": []any{"t3"}, "rec": map[string]any{"a": int64(6), "b": "b3"}},
		{"id": int64(4), "tags": []any{"t4", nil, "u4"}, "rec": map[string]any{"a": int64(8), "b": "b4"}},
	}
}

// TestCompact_RowChildNullableString is the regression test for the Compact
// ROW-child offsets bug: a NULL value in a nullable string field inside a
// ROW column must still advance the child's offset slot, or every later
// row in the compacted batch reads back as concatenated garbage.
func TestCompact_RowChildNullableString(t *testing.T) {
	rows := nestedRowsForTest()
	b := FromRows(nestedSchemaForTest(), rows)
	b.Sel = []uint32{0, 1, 3, 4} // drop row 2, keep the null-b row in the middle

	got := b.Compact().ToRows()
	want := []map[string]any{rows[0], rows[1], rows[3], rows[4]}
	if len(got) != len(want) {
		t.Fatalf("rows: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("row %d differs:\n got  %#v\n want %#v", i, got[i], want[i])
		}
	}
}

// TestCopyValueFrom_SequentialNested writes nested rows one by one into a
// pre-allocated destination (NewRecordBatch shape) and checks value-identity,
// including null arrays, empty arrays, null elements and null row fields.
func TestCopyValueFrom_SequentialNested(t *testing.T) {
	rows := nestedRowsForTest()
	src := FromRows(nestedSchemaForTest(), rows)

	dst := NewRecordBatch(nestedSchemaForTest(), src.Len)
	for j := range src.Columns {
		for i := 0; i < src.Len; i++ {
			dst.Columns[j].CopyValueFrom(i, src.Columns[j], i)
		}
	}
	got := dst.ToRows()
	for i := range rows {
		if !reflect.DeepEqual(got[i], rows[i]) {
			t.Fatalf("row %d differs:\n got  %#v\n want %#v", i, got[i], rows[i])
		}
	}
}

// TestAppendFrom_NestedLazyStructure builds the destination with
// NewVectorLike (no pre-allocation at all) and appends every row, covering
// the append-built shape of ROW children and ARRAY child growth — plus an
// array-of-row column (the MAP storage shape) for nested-of-nested.
func TestAppendFrom_NestedLazyStructure(t *testing.T) {
	rows := nestedRowsForTest()
	src := FromRows(nestedSchemaForTest(), rows)

	for j := range src.Columns {
		dst := NewVectorLike(src.Columns[j])
		for i := 0; i < src.Len; i++ {
			dst.AppendFrom(src.Columns[j], i)
		}
		if dst.Len != src.Len {
			t.Fatalf("col %d: len got %d want %d", j, dst.Len, src.Len)
		}
		for i := 0; i < src.Len; i++ {
			if !reflect.DeepEqual(dst.GetValue(i), src.Columns[j].GetValue(i)) {
				t.Fatalf("col %d row %d: got %#v want %#v", j, i, dst.GetValue(i), src.Columns[j].GetValue(i))
			}
		}
	}

	// MAP column: offsets over a ROW(key,value) child.
	mapSchema := []parquet.Column{{Name: "m", Type: parquet.TypeMap,
		ElementType: &parquet.Column{Name: "kv", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "key", Type: parquet.TypeString},
			{Name: "value", Type: parquet.TypeInt64},
		}}}}
	mapRows := []map[string]any{
		{"m": []any{map[string]any{"key": "k1", "value": int64(1)}, map[string]any{"key": "k2", "value": int64(2)}}},
		{"m": []any{}},
		{"m": []any{map[string]any{"key": "k3", "value": int64(3)}}},
	}
	msrc := FromRows(mapSchema, mapRows)
	mdst := NewVectorLike(msrc.Columns[0])
	for i := 0; i < msrc.Len; i++ {
		mdst.AppendFrom(msrc.Columns[0], i)
	}
	for i := 0; i < msrc.Len; i++ {
		if !reflect.DeepEqual(mdst.GetValue(i), msrc.Columns[0].GetValue(i)) {
			t.Fatalf("map row %d: got %#v want %#v", i, mdst.GetValue(i), msrc.Columns[0].GetValue(i))
		}
	}
}
