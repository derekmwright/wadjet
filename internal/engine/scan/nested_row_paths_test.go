package scan

import (
	"bytes"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The two read paths over a ROW column (#448, #449).
//
// A ROW is the one container the native columnar reader claims: it resolves
// each field as its own leaf chunk. Both defects were about what that claim
// covers.
//
// #448 — it covers only fields that ARE leaves. readRowGroupNative keys the
// lookup by the two-element path {column, field}, and a field that is itself
// a ROW/ARRAY/MAP is a GROUP whose leaves sit below that path, so the lookup
// missed and the field came back all-NULL with no error. The fix refuses the
// shape in HasUnsupportedColumnarTypes, which routes it to the row reader.
//
// #449 — the row reader omitted a NULL field from the map it hands back,
// while the columnar box carries one entry per child. Same value, two boxes.
//
// Both are ADR-0018 §3: a file is readable through all of the decode paths or
// through none of them, and a value means the same thing on each.

// rowPathsSchema is ROW-of-primitive (the columnar path's own shape) beside
// the three container-field shapes the columnar path cannot address.
func rowPathsSchema() pqt.Schema {
	i64 := func(name string) pqt.Column { return pqt.Column{Name: name, Type: pqt.TypeInt64, Nullable: true} }
	arr := func(name string, elem pqt.Column) pqt.Column {
		return pqt.Column{Name: name, Type: pqt.TypeArray, Nullable: true, ElementType: &elem}
	}
	mp := func(name string, val pqt.Column) pqt.Column {
		val.Name = "value"
		return pqt.Column{Name: name, Type: pqt.TypeMap, Nullable: true, ElementType: &pqt.Column{
			Name: "entry", Type: pqt.TypeRow, Fields: []pqt.Column{{Name: "key", Type: pqt.TypeString}, val},
		}}
	}
	row := func(name string, fields ...pqt.Column) pqt.Column {
		return pqt.Column{Name: name, Type: pqt.TypeRow, Nullable: true, Fields: fields}
	}
	return pqt.Schema{Columns: []pqt.Column{
		i64("id"),
		row("r_row", i64("a"), row("s", i64("b"))),
		row("r_arr", i64("a"), arr("l", i64("element"))),
		row("r_map", i64("a"), mp("m", i64(""))),
	}}
}

func rowPathsData() []map[string]any {
	return []map[string]any{
		{
			"id":    int64(0),
			"r_row": map[string]any{"a": int64(5), "s": map[string]any{"b": int64(9)}},
			"r_arr": map[string]any{"a": int64(5), "l": []any{int64(1), int64(2)}},
			"r_map": map[string]any{"a": int64(5), "m": map[string]any{"k": int64(9)}},
		},
		{ // empty inner containers, and a NULL field beside them
			"id":    int64(1),
			"r_row": map[string]any{"a": nil, "s": map[string]any{"b": nil}},
			"r_arr": map[string]any{"a": int64(6), "l": []any{}},
			"r_map": map[string]any{"a": int64(6), "m": map[string]any{}},
		},
		{ // the inner container NULL, the struct present
			"id":    int64(2),
			"r_row": map[string]any{"a": int64(7), "s": nil},
			"r_arr": map[string]any{"a": int64(7), "l": nil},
			"r_map": map[string]any{"a": int64(7), "m": nil},
		},
		{ // every column NULL
			"id": int64(3),
		},
	}
}

// TestRowWithContainerFieldGoesToTheRowReader is #448: the columnar entry
// point must answer the values that are in the file, and the native decoder
// must REFUSE the shape rather than emit the all-NULL column it used to.
func TestRowWithContainerFieldGoesToTheRowReader(t *testing.T) {
	schema := rowPathsSchema()
	if !HasUnsupportedColumnarTypes(schema.Columns) {
		t.Fatal("a ROW whose fields are containers must be refused by the columnar reader: " +
			"its leaves are below the {column, field} path the ROW arm can address (#448)")
	}
	// One container field is enough — each of the three shapes on its own.
	for _, col := range schema.Columns[1:] {
		if !HasUnsupportedColumnarTypes([]pqt.Column{col}) {
			t.Errorf("column %q alone must be refused by the columnar reader", col.Name)
		}
	}
	// A ROW of primitives is still the columnar reader's own shape.
	flatRow := []pqt.Column{{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
		{Name: "a", Type: pqt.TypeString, Nullable: true},
		{Name: "b", Type: pqt.TypeInt64, Nullable: true},
	}}}
	if HasUnsupportedColumnarTypes(flatRow) {
		t.Error("a ROW of primitive leaves must stay on the columnar path")
	}

	data := rowPathsWadjetFile(t)
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}

	// The native decoder refuses rather than answering with nulls.
	if _, err := ReadRowGroupNative(r.FileReader(), 0, schema.Columns, nil); err == nil {
		t.Error("ReadRowGroupNative accepted a ROW with a container field — it must refuse, " +
			"because the field's leaves do not resolve and the miss reads as all-NULL")
	}

	// The entry point every caller uses falls back and answers correctly.
	want := rowPathsRowReaderRows(t, r, schema)
	got := rowPathsBatchRows(t, r, schema)
	assertNestedRowsEqual(t, "wadjet-written", got, want)

	// The values are the ones that were written, not merely two agreeing
	// readings of nothing: spot-check the field the bug nulled.
	for i, row := range got {
		outer, ok := row["r_row"].(map[string]any)
		if !ok {
			continue // the NULL-column row
		}
		if i == 0 && !reflect.DeepEqual(outer["s"], map[string]any{"b": int64(9)}) {
			t.Errorf("row 0 r_row.s = %#v, want map[b:9]", outer["s"])
		}
	}
}

// TestRowWithContainerFieldFromPyArrow is the same property over a file the
// Apache implementation wrote, so the routing is checked against a real
// nested layout and not only against wadjet's own writer.
func TestRowWithContainerFieldFromPyArrow(t *testing.T) {
	const fixture = "../../storage/parquet/testdata/nested_containers.parquet"
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Skipf("fixture missing (regen with internal/storage/parquet/testdata/gen_nested_containers.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	// Read the file's own schema and keep the three ROW-of-container columns.
	fileSchema := r.Schema()
	var cols []pqt.Column
	for _, c := range fileSchema.Columns {
		switch c.Name {
		case "id", "r_row", "r_arr", "r_map":
			cols = append(cols, c)
		}
	}
	if len(cols) != 4 {
		t.Fatalf("fixture columns: got %d of the 4 expected (%v)", len(cols), fileSchema.ColumnNames())
	}
	schema := pqt.Schema{Columns: cols}
	if !HasUnsupportedColumnarTypes(schema.Columns) {
		t.Fatal("pyarrow's ROW-of-container columns must be refused by the columnar reader (#448)")
	}
	want := rowPathsRowReaderRows(t, r, schema)
	got := rowPathsBatchRows(t, r, schema)
	assertNestedRowsEqual(t, "pyarrow", got, want)
}

// TestRowFieldNullsAgreeAcrossReadPaths is #449 from the scan side: a ROW of
// primitive leaves stays on the columnar path, so the two boxes for the same
// value are produced by two different decoders and must match key for key.
//
// Both comparisons are here on purpose. The row reader's RAW map against the
// columnar vector's is where #449 lives — map[b:-3] versus map[a:<nil> b:-3].
// The same values through FromRows is the property a QUERY sees, and it holds
// either way, because FromRows drives SetValue by field name and a missing key
// lands on the same NULL: that is exactly why the defect survived every gate
// until the raw boxes were compared.
func TestRowFieldNullsAgreeAcrossReadPaths(t *testing.T) {
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "a", Type: pqt.TypeString, Nullable: true},
			{Name: "b", Type: pqt.TypeInt64, Nullable: true},
		}},
	}}
	rows := []map[string]any{
		{"id": int64(0), "r": map[string]any{"a": "x", "b": int64(-3)}},
		{"id": int64(1), "r": map[string]any{"a": nil, "b": int64(-3)}}, // the issue's row
		{"id": int64(2), "r": map[string]any{"a": "x", "b": nil}},
		{"id": int64(3), "r": map[string]any{"a": nil, "b": nil}},
		{"id": int64(4)}, // the whole ROW NULL
	}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if HasUnsupportedColumnarTypes(schema.Columns) {
		t.Fatal("a ROW of primitive leaves must stay on the columnar path — " +
			"this test compares the two decoders and needs both of them")
	}
	native, err := ReadRowGroupNative(r.FileReader(), 0, schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupNative: %v", err)
	}

	// The raw boxes: what each reader hands its caller before anything
	// normalises them.
	raw, err := r.ReadRowsAs(schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowsAs: %v", err)
	}
	if len(raw) != native.Len {
		t.Fatalf("row reader returned %d rows, columnar %d", len(raw), native.Len)
	}
	for i := range raw {
		got, want := native.Columns[1].GetValue(i), raw[i]["r"]
		if !reflect.DeepEqual(got, want) {
			t.Errorf("row %d: the two readers box the same ROW differently\n"+
				"  columnar   %#v\n  row reader %#v", i, got, want)
		}
	}

	// And the same values as a query sees them.
	assertNestedRowsEqual(t, "columnar vs row reader",
		nestedBatchToRows(native), rowPathsRowReaderRows(t, r, schema))
}

// --- helpers ---------------------------------------------------------------

func rowPathsWadjetFile(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, rowPathsSchema(), pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rowPathsData()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// rowPathsRowReaderRows reads through the row path and boxes the result the
// way a batch does, so the two arms are compared in ONE box space: the row
// reader's maps go through FromRows and back out through GetValue, which is
// exactly what readFileBatchesViaRows hands its callers.
func rowPathsRowReaderRows(t *testing.T, r *pqt.Reader, schema pqt.Schema) []map[string]any {
	t.Helper()
	rows, err := r.ReadRowsAs(schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowsAs: %v", err)
	}
	return nestedBatchToRows(batch.FromRows(schema.Columns, rows))
}

func rowPathsBatchRows(t *testing.T, r *pqt.Reader, schema pqt.Schema) []map[string]any {
	t.Helper()
	batches, err := ReadFileBatches(r, schema.Columns, nil)
	if err != nil {
		t.Fatalf("ReadFileBatches: %v", err)
	}
	var out []map[string]any
	for _, b := range batches {
		out = append(out, nestedBatchToRows(b)...)
	}
	return out
}

// nestedBatchToRows boxes a batch as one map per row. A NULL top-level column
// is an absent key, which is how every row shape in the reader spells a
// column with no value.
func nestedBatchToRows(b *batch.RecordBatch) []map[string]any {
	if b == nil {
		return nil
	}
	out := make([]map[string]any, b.Len)
	for i := 0; i < b.Len; i++ {
		row := make(map[string]any, len(b.Columns))
		for c, vec := range b.Columns {
			if vec == nil || vec.Nulls.IsNullFast(i) {
				continue
			}
			row[b.Schema[c].Name] = vec.GetValue(i)
		}
		out[i] = row
	}
	return out
}

func assertNestedRowsEqual(t *testing.T, what string, got, want []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: read %d rows, want %d", what, len(got), len(want))
	}
	for i := range want {
		keys := map[string]bool{}
		for k := range want[i] {
			keys[k] = true
		}
		for k := range got[i] {
			keys[k] = true
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			gv, gok := got[i][k]
			wv, wok := want[i][k]
			if gok != wok || !reflect.DeepEqual(gv, wv) {
				t.Errorf("%s row %d column %q:\n   got %#v (present=%v)\n  want %#v (present=%v)",
					what, i, k, gv, gok, wv, wok)
			}
		}
	}
}
