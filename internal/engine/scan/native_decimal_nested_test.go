package scan

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression tests for the native row-group decoder (issue #144 suite
// findings): DECIMAL pages decoded to all-zeros (no TypeDecimal case in the
// copy switch), and ARRAY columns were silently emitted as ALL-NULL (the
// leaf-name lookup missed and fell into the schema-evolution branch)
// instead of erroring so callers fall back to the row-based reader.

func writeOneRG(tb testing.TB, schema parquet.Schema, rows []map[string]any) *parquet.Reader {
	tb.Helper()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	data := buf.Bytes()
	r, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	return r
}

func TestReadRowGroupNative_DecimalValues(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Nullable: true, Precision: 12, Scale: 2},
	}}
	rows := []map[string]any{
		{"d": 3.25}, {"d": nil}, {"d": -1.5}, {"d": int64(7)}, {"d": "12.34"},
	}
	r := writeOneRG(t, schema, rows)
	b, err := ReadRowGroupNative(r.FileReader(), 0, schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupNative: %v", err)
	}
	want := []float64{3.25, 0, -1.5, 7, 12.34}
	isNull := []bool{false, true, false, false, false}
	for i := range want {
		if isNull[i] {
			if !b.Columns[0].Nulls.IsNullFast(i) {
				t.Fatalf("row %d: want NULL, got %v", i, b.Columns[0].GetValue(i))
			}
			continue
		}
		// The null-interleaved page exercises the scatter path; values
		// must land in their own slots, not shift into null positions.
		got, err := strconv.ParseFloat(fmt.Sprintf("%v", b.Columns[0].GetValue(i)), 64)
		if err != nil || got != want[i] {
			t.Fatalf("row %d: got %v, want %v (decimal page decoded to zeros or misaligned)",
				i, b.Columns[0].GetValue(i), want[i])
		}
	}
}

func TestReadRowGroupNative_ArrayErrorsLoudly(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "tags", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}}
	r := writeOneRG(t, schema, []map[string]any{{"tags": []any{"a", "b"}}})
	_, err := ReadRowGroupNative(r.FileReader(), 0, schema.Columns, nil)
	if err == nil {
		t.Fatal("ARRAY through the native reader must error (callers fall back to the row reader); silent all-NULL output is wrong results")
	}
}
