package scan

import (
	"bytes"
	"reflect"
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

// TestNativeRowLeafByPathDoesNotCollideOnPathSuffix guards a second, deeper
// collision than the one above: leafByPath used to key on only the LAST TWO
// path components, so leaf [s, c] (top-level s's own field) and leaf
// [r, s, c] (a doubly-nested r.s.c, present purely to exercise the file's
// footer schema) both hashed to "s.c" — the leaf declared later in the
// schema won the map entry. Reading the projected top-level ROW s then
// resolved to r's INNER "s" leaf instead of s's own, which is wrong twice
// over: the VALUE came from r.s.c, and the presence check ran against
// r.s's group (MaxDefLevel 2, one optional level deeper than top-level s's
// own group at MaxDefLevel 1) instead of s's — so roughly half of s's
// always-present rows decoded as NULL.
//
// "r" is left in the FILE but out of the projected read: this is a defect
// in how s resolves, not a claim that a doubly-nested ROW-in-ROW column
// itself reads through the native path.
func TestNativeRowLeafByPathDoesNotCollideOnPathSuffix(t *testing.T) {
	const n = 200
	fileSchema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "c", Type: pqt.TypeInt64, Nullable: true},
		}},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "s", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
				{Name: "c", Type: pqt.TypeInt64, Nullable: true},
			}},
		}},
	}}

	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		// Top-level s is present on EVERY row, with a value distinct from
		// r.s.c's so a leaf mix-up shows up as a wrong VALUE, not only a
		// wrong null bit.
		s := map[string]any{"c": int64(i)}
		var r any
		if i%2 == 1 {
			// r present, r.s NULL: the group-absence state the collision
			// mistook for top-level s's own group being absent.
			r = map[string]any{"s": nil}
		} else {
			r = map[string]any{"s": map[string]any{"c": int64(i*1000 + 7)}}
		}
		rows[i] = map[string]any{"id": int64(i), "s": s, "r": r}
	}

	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	fr, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	// Project to id+s only. "r" stays in the file's footer schema, so its
	// r.s.c leaf still exists in fr.Leaves() and still collides — the
	// collision lives in leafByPath, built from the FULL file schema, not
	// from the projected read schema.
	batches, err := ReadFileBatchesNative(fr.FileReader(), fileSchema.Columns, []string{"id", "s"})
	if err != nil {
		t.Fatalf("ReadFileBatchesNative: %v", err)
	}

	// The row reader is the oracle: it walks records, not column chunks,
	// and cannot suffer a leaf-PATH collision the same way (ADR-0018 §3).
	rr, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("row reader: %v", err)
	}
	wantRows, err := rr.ReadRows([]string{"id", "s"})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	wantByID := make(map[int64]map[string]any, len(wantRows))
	for _, row := range wantRows {
		id, _ := row["id"].(int64)
		wantByID[id] = row
	}

	seen := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			id := b.Columns[0].Int64Data[i]

			// Direct check against the literal input: top-level s is
			// present, with its own value, on every row.
			if b.Columns[1].Nulls.IsNull(i) {
				t.Fatalf("row id=%d: s decoded NULL, want present", id)
			}
			gotS := b.Columns[1].GetValue(i)
			wantS := rows[id]["s"]
			if !reflect.DeepEqual(gotS, wantS) {
				t.Fatalf("row id=%d: s = %#v, want %#v", id, gotS, wantS)
			}

			// Cross-check against the independent row-based reader.
			wantRow, ok := wantByID[id]
			if !ok {
				t.Fatalf("row id=%d: missing from row-reader output", id)
			}
			if !reflect.DeepEqual(gotS, wantRow["s"]) {
				t.Fatalf("row id=%d: native s = %#v, row reader s = %#v", id, gotS, wantRow["s"])
			}
			seen++
		}
	}
	if seen != n {
		t.Fatalf("read %d rows, want %d", seen, n)
	}
}
