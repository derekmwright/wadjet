package json

import (
	"reflect"
	"testing"
)

// TestColumnarReader_NullNestedObject is the sweep's live repro: a null (or
// missing) nested object must not shift the following rows' child data.
// Before the WriteNullAt fix, row 3 of "meta.name" read back "alphabeta".
func TestColumnarReader_NullNestedObject(t *testing.T) {
	data := []byte(`[{"meta":{"name":"alpha"}},{"meta":null},{"meta":{"name":"beta"}},{}]`)
	r, err := NewColumnarReader(data)
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 4 {
		t.Fatalf("expected 4 rows, got %+v", b)
	}
	metaIdx := -1
	for i, c := range b.Schema {
		if c.Name == "meta" {
			metaIdx = i
		}
	}
	if metaIdx < 0 {
		t.Fatalf("no meta column in schema %+v", b.Schema)
	}
	col := b.Columns[metaIdx]
	if got := col.GetValue(0); !reflect.DeepEqual(got, map[string]any{"name": "alpha"}) {
		t.Fatalf("row 0: got %#v", got)
	}
	if col.GetValue(1) != nil {
		t.Fatalf("row 1 (null): got %#v want nil", col.GetValue(1))
	}
	if got := col.GetValue(2); !reflect.DeepEqual(got, map[string]any{"name": "beta"}) {
		t.Fatalf("row 2 after null: got %#v want {name:beta} (offsets shifted)", got)
	}
	if col.GetValue(3) != nil {
		t.Fatalf("row 3 (missing key): got %#v want nil", col.GetValue(3))
	}
}
