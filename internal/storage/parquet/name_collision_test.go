package parquet

import (
	"bytes"
	"reflect"
	"testing"
)

// A top-level column and a struct FIELD may carry the same name.
//
// Every read entry point resolved a top-level column's name against a
// map[leafName]index built over every leaf in the file, so the two leaves
// named "x" in `x INT64, r ROW{x INT64}` collided and the LAST one won:
// ReadRows(nil) answered with the column's own values (the nested path had
// already been made node-first) while ReadRows(["x"]) and ReadRowGroup
// answered with the STRUCT FIELD's, silently. A top-level column's name
// means the leaf whose PATH is that one name.

func collidingNamesSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "x", Type: TypeInt64, Nullable: true},
		{Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "x", Type: TypeInt64, Nullable: true},
		}},
	}}
}

func collidingNamesRows() []map[string]any {
	return []map[string]any{
		{"x": int64(10), "r": map[string]any{"x": int64(999)}},
		{"x": int64(20), "r": map[string]any{"x": int64(888)}},
	}
}

func writeCollidingNames(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, collidingNamesSchema(), DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows(collidingNamesRows()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

func TestTopLevelColumnWinsOverSameNamedStructField(t *testing.T) {
	raw := writeCollidingNames(t)
	wantX := []int64{10, 20}
	wantField := []int64{999, 888}

	read := map[string]func(r *Reader) ([]map[string]any, error){
		"ReadRows(nil)":     func(r *Reader) ([]map[string]any, error) { return r.ReadRows(nil) },
		"ReadRows(x)":       func(r *Reader) ([]map[string]any, error) { return r.ReadRows([]string{"x"}) },
		"ReadRows(x,r)":     func(r *Reader) ([]map[string]any, error) { return r.ReadRows([]string{"x", "r"}) },
		"ReadRowGroup(nil)": func(r *Reader) ([]map[string]any, error) { return r.ReadRowGroup(0, nil) },
		"ReadRowGroup(x)":   func(r *Reader) ([]map[string]any, error) { return r.ReadRowGroup(0, []string{"x"}) },
		"ReadRowsAs(catalog,x)": func(r *Reader) ([]map[string]any, error) {
			return r.ReadRowsAs(collidingNamesSchema().Columns, []string{"x"})
		},
		"ReadRowsAs(catalog,nil)": func(r *Reader) ([]map[string]any, error) { return r.ReadRowsAs(collidingNamesSchema().Columns, nil) },
	}
	for name, fn := range read {
		t.Run(name, func(t *testing.T) {
			r, err := NewReaderFromBytes(raw)
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			rows, err := fn(r)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("read %d rows, want 2", len(rows))
			}
			for i, row := range rows {
				if got := row["x"]; got != any(wantX[i]) {
					t.Errorf("row %d: x = %#v, want %d", i, got, wantX[i])
				}
				if f, ok := row["r"]; ok {
					want := map[string]any{"x": wantField[i]}
					if !reflect.DeepEqual(f, want) {
						t.Errorf("row %d: r = %#v, want %#v", i, f, want)
					}
				}
			}
		})
	}
}
