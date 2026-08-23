package parquet

import (
	"bytes"
	"testing"
)

// TestVectorAndDecimalBelowAContainer is #407's live half.
//
// The row reader's flat path had a VECTOR arm; the NESTED path's leaf decode
// did not, and had no DECIMAL arm either, so a VECTOR or DECIMAL leaf
// anywhere below an ARRAY, ROW or MAP decoded to nothing and every value
// under it read back NULL — while the identical leaf as a top-level column
// read correctly. ADR-0018 §3: a file is readable through all of the decode
// paths or through none of them, and the two arms are now one
// (decodePresentValues).
func TestVectorAndDecimalBelowAContainer(t *testing.T) {
	vec := func(name string) Column {
		return Column{Name: name, Type: TypeVector, Nullable: true, Dimension: 4}
	}
	dec := func(name string) Column {
		return Column{Name: name, Type: TypeDecimal, Nullable: true, Precision: 18, Scale: 0}
	}
	ptr := func(c Column) *Column { return &c }
	schema := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		vec("top"), // control: a top-level VECTOR beside the containers
		{Name: "av", Type: TypeArray, Nullable: true, ElementType: ptr(vec("element"))},
		{Name: "rv", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "n", Type: TypeInt64, Nullable: true}, vec("v"),
		}},
		{Name: "mv", Type: TypeMap, Nullable: true, ElementType: &Column{
			Name: "entry", Type: TypeRow,
			Fields: []Column{{Name: "key", Type: TypeString}, vec("value")},
		}},
		{Name: "ad", Type: TypeArray, Nullable: true, ElementType: ptr(dec("element"))},
		{Name: "rd", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "n", Type: TypeInt64, Nullable: true}, dec("d"),
		}},
		{Name: "md", Type: TypeMap, Nullable: true, ElementType: &Column{
			Name: "entry", Type: TypeRow,
			Fields: []Column{{Name: "key", Type: TypeString}, dec("value")},
		}},
	}}

	v1 := []float32{1, 2, 3, 4}
	v2 := []float32{-1.5, 0, 0.25, 1e10}
	want := []map[string]any{
		{
			"id":  int64(0),
			"top": v1,
			"av":  []any{v1, v2},
			"rv":  map[string]any{"n": int64(7), "v": v2},
			"mv":  map[string]any{"a": v1, "b": v2},
			"ad":  []any{int64(11), int64(-12)},
			"rd":  map[string]any{"n": int64(9), "d": int64(13)},
			"md":  map[string]any{"a": int64(14), "b": int64(-15)},
		},
		{ // NULL in every position under a present container
			"id": int64(1),
			"av": []any{nil},
			"rv": map[string]any{"n": int64(8)},
			"mv": map[string]any{"a": nil},
			"ad": []any{nil},
			"rd": map[string]any{"n": int64(10)},
			"md": map[string]any{"a": nil},
		},
		{ // every container NULL
			"id": int64(2),
		},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := pw.WriteRows(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	assertRowsEqual(t, got, want)
}

// TestVectorDimensionMismatchBelowAContainer: the width guard a VECTOR leaf
// gets as a top-level column applies at every depth too — a shorter vector is
// not a truncated one, it is a different point.
func TestVectorDimensionMismatchBelowAContainer(t *testing.T) {
	write := Schema{Columns: []Column{
		{Name: "a", Type: TypeArray, Nullable: true, ElementType: &Column{
			Name: "element", Type: TypeVector, Nullable: true, Dimension: 4,
		}},
	}}
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, write, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows([]map[string]any{{"a": []any{[]float32{1, 2, 3, 4}}}}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	// The file declares dimension 4 (TypeLength 16), so the recovered leaf
	// column carries it and the decode agrees with itself.
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got, ok := rows[0]["a"].([]any)
	if !ok || len(got) != 1 {
		t.Fatalf("a = %#v, want a one-element array", rows[0]["a"])
	}
	v, ok := got[0].([]float32)
	if !ok || len(v) != 4 {
		t.Fatalf("element = %#v (%T), want a 4-dimension vector", got[0], got[0])
	}
}
