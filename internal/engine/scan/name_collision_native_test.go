package scan

import (
	"bytes"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The native columnar path resolved a column's leaf by leaf NAME too, so a
// top-level `x` and a `r ROW{x}` collided there exactly as they did on the
// row path: the scan answered the query's `x` with the struct field's
// values. ADR-0018 §3 — the paths agree, so this is the same file and the
// same expectation as parquet.TestTopLevelColumnWinsOverSameNamedStructField.
func TestNativeTopLevelColumnWinsOverSameNamedStructField(t *testing.T) {
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "x", Type: pqt.TypeInt64, Nullable: true},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "x", Type: pqt.TypeInt64, Nullable: true},
		}},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"x": int64(10), "r": map[string]any{"x": int64(999)}},
		{"x": int64(20), "r": map[string]any{"x": int64(888)}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	batches, err := ReadFileBatchesNative(r.FileReader(), schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadFileBatchesNative: %v", err)
	}
	want := []int64{10, 20}
	wantField := []int64{999, 888}
	row := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			if row >= len(want) {
				t.Fatalf("more rows than written")
			}
			if got := b.Columns[0].GetValue(i); got != any(want[row]) {
				t.Errorf("row %d: x = %#v, want %d", row, got, want[row])
			}
			if got := b.Columns[1].Children[0].GetValue(i); got != any(wantField[row]) {
				t.Errorf("row %d: r.x = %#v, want %d", row, got, wantField[row])
			}
			row++
		}
	}
	if row != len(want) {
		t.Fatalf("read %d rows, want %d", row, len(want))
	}
}
