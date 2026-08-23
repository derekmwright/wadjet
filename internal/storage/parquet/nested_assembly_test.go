package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

// Nested container assembly (#409).
//
// Every MAP that was not a top-level column of leaf-typed values read back
// absent or wrong, with no error: a MAP field inside a ROW was dropped, a MAP
// of ARRAY or of ROW lost the whole column, and an ARRAY of MAP answered with
// the array of its KEYS. The cause was a leaf lookup at a fixed path depth;
// see nested_assembly.go.
//
// The reference file is written by PyArrow 23.0.1 (gen_nested_containers.py),
// so the assembler is checked against the Apache implementation and not
// against wadjet's own writer. The same logical values are then round-tripped
// through wadjet's writer, and the file wadjet writes is handed BACK to
// PyArrow, so all three directions have to agree.

const nestedFixture = "testdata/nested_containers.parquet"

// nestedContainersWant is the fixture's content in the shape ReadRows
// returns: a NULL top-level COLUMN is an absent key (that is how a row spells
// a column it has no value for), while everything below the top level is a
// present entry holding nil — a NULL struct FIELD, a NULL map VALUE and a
// NULL array ELEMENT alike. A struct's field set is fixed by the schema, so
// its keys are always all there; #449 is what omitting them cost.
func nestedContainersWant() []map[string]any {
	return []map[string]any{
		{ // ordinary values
			"id":       int64(0),
			"m_int":    map[string]any{"k": int64(9)},
			"m_list":   map[string]any{"k": []any{int64(1), int64(2)}},
			"m_struct": map[string]any{"k": map[string]any{"x": int64(3), "y": "three"}},
			"m_map":    map[string]any{"k": map[string]any{"inner": int64(11)}},
			"r_map":    map[string]any{"a": int64(5), "m": map[string]any{"k": int64(9)}},
			"r_arr":    map[string]any{"a": int64(5), "l": []any{int64(1), int64(2)}},
			"r_row":    map[string]any{"a": int64(5), "s": map[string]any{"b": int64(9)}},
			"a_map":    []any{map[string]any{"k": int64(1)}},
			"a_row":    []any{map[string]any{"x": int64(1)}},
			"a_arr":    []any{[]any{int64(1), int64(2)}},
		},
		{ // empty containers everywhere — NOT the same value as NULL
			"id":       int64(1),
			"m_int":    map[string]any{},
			"m_list":   map[string]any{},
			"m_struct": map[string]any{},
			"m_map":    map[string]any{},
			"r_map":    map[string]any{"a": int64(6), "m": map[string]any{}},
			"r_arr":    map[string]any{"a": int64(6), "l": []any{}},
			"r_row":    map[string]any{"a": int64(6), "s": map[string]any{"b": nil}},
			"a_map":    []any{},
			"a_row":    []any{},
			"a_arr":    []any{},
		},
		{ // NULL one level in
			"id":       int64(2),
			"m_int":    map[string]any{"k": nil},
			"m_list":   map[string]any{"k": nil},
			"m_struct": map[string]any{"k": nil},
			"m_map":    map[string]any{"k": nil},
			"r_map":    map[string]any{"a": nil, "m": nil},
			"r_arr":    map[string]any{"a": nil, "l": nil},
			"r_row":    map[string]any{"a": nil, "s": nil},
			"a_map":    []any{nil},
			"a_row":    []any{nil},
			"a_arr":    []any{nil},
		},
		{ // every container NULL
			"id": int64(3),
		},
		{ // several entries / elements, with NULLs among them
			"id":     int64(4),
			"m_int":  map[string]any{"a": int64(1), "b": int64(2)},
			"m_list": map[string]any{"a": []any{}, "b": []any{int64(3), nil, int64(5)}},
			"m_struct": map[string]any{
				"a": map[string]any{"x": nil, "y": "a"},
				"b": map[string]any{"x": int64(7), "y": nil},
			},
			"m_map": map[string]any{
				"a": map[string]any{},
				"b": map[string]any{"p": int64(1), "q": nil},
			},
			"r_map": map[string]any{"a": int64(8), "m": map[string]any{"p": int64(1), "q": nil}},
			"r_arr": map[string]any{"a": int64(8), "l": []any{int64(3), nil}},
			"r_row": map[string]any{"a": int64(8), "s": map[string]any{"b": int64(4)}},
			"a_map": []any{
				map[string]any{"a": int64(1)},
				map[string]any{},
				map[string]any{"b": nil, "c": int64(3)},
			},
			"a_row": []any{map[string]any{"x": int64(2)}, map[string]any{"x": nil}},
			"a_arr": []any{[]any{int64(3)}, []any{}, []any{nil, int64(5)}},
		},
	}
}

// nestedContainersSchema is the same shape in wadjet's own Column grammar.
func nestedContainersSchema() Schema {
	i64 := func(name string) Column { return Column{Name: name, Type: TypeInt64, Nullable: true} }
	str := func(name string) Column { return Column{Name: name, Type: TypeString, Nullable: true} }
	arr := func(name string, elem Column) Column {
		return Column{Name: name, Type: TypeArray, Nullable: true, ElementType: &elem}
	}
	mp := func(name string, val Column) Column {
		val.Name = "value"
		return Column{Name: name, Type: TypeMap, Nullable: true, ElementType: &Column{
			Name: "entry", Type: TypeRow, Fields: []Column{{Name: "key", Type: TypeString}, val},
		}}
	}
	row := func(name string, fields ...Column) Column {
		return Column{Name: name, Type: TypeRow, Nullable: true, Fields: fields}
	}
	return Schema{Columns: []Column{
		i64("id"),
		mp("m_int", i64("")),
		mp("m_list", arr("", i64("element"))),
		mp("m_struct", row("", i64("x"), str("y"))),
		mp("m_map", mp("", i64(""))),
		row("r_map", i64("a"), mp("m", i64(""))),
		row("r_arr", i64("a"), arr("l", i64("element"))),
		row("r_row", i64("a"), row("s", i64("b"))),
		arr("a_map", mp("element", i64(""))),
		arr("a_row", row("element", i64("x"))),
		arr("a_arr", arr("element", i64("element"))),
	}}
}

func TestNestedContainersFromPyArrow(t *testing.T) {
	data, err := os.ReadFile(nestedFixture)
	if err != nil {
		t.Fatalf("fixture: %v (regenerate with gen_nested_containers.py)", err)
	}
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	// The catalog types the file's own schema recovers must be the container
	// kinds too, or the value shape and the declared type disagree.
	wantType := map[string]TypeID{
		"id": TypeInt64, "m_int": TypeMap, "m_list": TypeMap, "m_struct": TypeMap,
		"m_map": TypeMap, "r_map": TypeRow, "r_arr": TypeRow, "r_row": TypeRow,
		"a_map": TypeArray, "a_row": TypeArray, "a_arr": TypeArray,
	}
	for _, c := range r.Schema().Columns {
		if w, ok := wantType[c.Name]; ok && c.Type != w {
			t.Errorf("column %s recovered as %v, want %v", c.Name, c.Type, w)
		}
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	assertRowsEqual(t, got, nestedContainersWant())
}

func TestNestedContainersWadjetRoundTrip(t *testing.T) {
	want := nestedContainersWant()
	raw := writeNestedContainers(t, want)
	r, err := NewReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	assertRowsEqual(t, got, want)
}

// TestNestedContainersWadjetRoundTripPerRowGroup writes the same rows one row
// group at a time, so the assembler's cursors have to restart cleanly at each
// group rather than only at the file.
func TestNestedContainersWadjetRoundTripPerRowGroup(t *testing.T) {
	want := nestedContainersWant()
	var buf bytes.Buffer
	cfg := DefaultWriterConfig()
	cfg.RowGroupSize = 2
	pw, err := NewWriter(&buf, nestedContainersSchema(), cfg)
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
	if r.NumRowGroups() < 3 {
		t.Fatalf("expected several row groups, got %d", r.NumRowGroups())
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	assertRowsEqual(t, got, want)
}

// TestNestedContainersProjection: a projected read must answer exactly what
// the full read answered for the columns it asked for, and must not need the
// leaves of the ones it did not.
//
// Both halves are asserted. The second one is not visible in the values — a
// read that pages in every leaf in the file and then uses three of them
// answers identically — so it is asserted against nestedReadPlans, which is
// the set readRowsNested itself pages in.
func TestNestedContainersProjection(t *testing.T) {
	data, err := os.ReadFile(nestedFixture)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	full := nestedContainersWant()
	for _, cols := range [][]string{
		{"id", "m_list"},
		{"r_map"},
		{"a_map", "m_struct"},
		{"m_map"},
	} {
		t.Run(fmt.Sprint(cols), func(t *testing.T) {
			r, err := NewReaderFromBytes(data)
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			got, err := r.ReadRows(cols)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			assertOnlyProjectedLeavesRead(t, r, cols)
			want := make([]map[string]any, len(full))
			for i, row := range full {
				w := map[string]any{}
				for _, c := range cols {
					if v, ok := row[c]; ok {
						w[c] = v
					}
				}
				want[i] = w
			}
			assertRowsEqual(t, got, want)
		})
	}
}

// assertOnlyProjectedLeavesRead checks the leaf set the read pages in: every
// leaf under a projected NESTED column and no other. A flat column is not in
// the set — it is read through readColumnToAny, by leaf index, one column at
// a time.
func assertOnlyProjectedLeavesRead(t *testing.T, r *Reader, cols []string) {
	t.Helper()
	leaves := r.fr.Leaves()
	readCols := filterSchemaColumns(r.Schema().Columns, cols)
	_, needLeaf := nestedReadPlans(r.fr.SchemaRoot(), readCols, len(leaves))

	projected := make(map[string]bool, len(readCols))
	for _, c := range readCols {
		if isNestedType(c.Type) {
			projected[c.Name] = true
		}
	}
	for i, leaf := range leaves {
		want := len(leaf.Path) > 0 && projected[leaf.Path[0]]
		if needLeaf[i] != want {
			t.Errorf("projection %v: leaf %v read=%v, want %v", cols, leaf.Path, needLeaf[i], want)
		}
	}
}

// TestNestedContainersPyArrowReadsWadjetWrite closes the loop: PyArrow must
// read the file wadjet writes and get the same values back. A wrong
// repetition level is invisible to a wadjet-only round trip if the reader
// makes the same mistake as the writer.
func TestNestedContainersPyArrowReadsWadjetWrite(t *testing.T) {
	if !havePyArrow() {
		t.Skip("python3 with pyarrow not available")
	}
	want := nestedContainersWant()
	raw := writeNestedContainers(t, want)
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet_nested.parquet")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("python3", "-c", pyArrowDumpScript, path).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow read failed: %v\n%s", err, out)
	}
	var got []map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decoding pyarrow output %q: %v", out, err)
	}
	// PyArrow's JSON carries numbers as float64 and spells absence as null;
	// compare against the same normalisation of the expectation.
	assertJSONEqual(t, got, want)
}

// pyArrowDumpScript reads a parquet file and prints its rows as JSON, with a
// MAP rendered as an object and a NULL as JSON null. The conversion is driven
// by the ARROW TYPE rather than by the shape of the Python value, because
// PyArrow renders a map as a list of pairs and an EMPTY map is therefore
// indistinguishable from an empty list once the type is discarded — which is
// exactly one of the distinctions under test.
const pyArrowDumpScript = `
import json, sys, pyarrow as pa, pyarrow.parquet as pq

def conv(ty, v):
    if v is None:
        return None
    if pa.types.is_map(ty):
        return {str(k): conv(ty.item_type, x) for k, x in v}
    if pa.types.is_list(ty) or pa.types.is_large_list(ty):
        return [conv(ty.value_type, e) for e in v]
    if pa.types.is_struct(ty):
        return {ty.field(i).name: conv(ty.field(i).type, v[ty.field(i).name])
                for i in range(ty.num_fields)}
    return v

t = pq.read_table(sys.argv[1])
rows = []
for i in range(t.num_rows):
    row = {}
    for name in t.column_names:
        row[name] = conv(t.schema.field(name).type, t.column(name)[i].as_py())
    rows.append(row)
json.dump(rows, sys.stdout)
`

func havePyArrow() bool {
	return exec.Command("python3", "-c", "import pyarrow").Run() == nil
}

func writeNestedContainers(t *testing.T, rows []map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, nestedContainersSchema(), DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

func assertRowsEqual(t *testing.T, got, want []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		for _, k := range sortedKeysOf(want[i], got[i]) {
			gv, gok := got[i][k]
			wv, wok := want[i][k]
			if gok != wok || !reflect.DeepEqual(gv, wv) {
				t.Errorf("row %d column %q:\n   got %#v (present=%v)\n  want %#v (present=%v)",
					i, k, gv, gok, wv, wok)
			}
		}
	}
}

func sortedKeysOf(ms ...map[string]any) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range ms {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// assertJSONEqual compares PyArrow's JSON rendering against the Go
// expectation, normalising the two differences that are about JSON and not
// about the data: JSON has one number type, and an absent Go key is a JSON
// null.
func assertJSONEqual(t *testing.T, got []map[string]any, want []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("pyarrow read %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		g, _ := json.Marshal(normalizeJSON(got[i]))
		w, _ := json.Marshal(normalizeJSON(want[i]))
		if string(g) != string(w) {
			t.Errorf("row %d:\n  pyarrow %s\n  wadjet  %s", i, g, w)
		}
	}
}

// normalizeJSON drops nil-valued keys (so an absent Go key and a JSON null
// compare equal) and renders every number as a float64.
func normalizeJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range t {
			if e == nil {
				continue
			}
			out[k] = normalizeJSON(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normalizeJSON(e)
		}
		return out
	case int64:
		return float64(t)
	case int:
		return float64(t)
	default:
		return v
	}
}
