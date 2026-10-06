// SPDX-License-Identifier: MIT

package parquet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// pgNumericGoldenSchema is a table of every shape the marker could reach —
// a DOUBLE leaf, a DOUBLE array element, a DOUBLE ROW field — beside the
// columns the unconstrained marker rides on, none of them marked.
func pgNumericGoldenSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "f", Type: TypeFloat64, Nullable: true},
		{Name: "r", Type: TypeFloat32, Nullable: true},
		{Name: "n", Type: TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "u", Type: TypeDecimal, Precision: 38, Scale: 10, Nullable: true, Unconstrained: true},
		{Name: "af", Type: TypeArray, Nullable: true, ElementType: &Column{Name: "element", Type: TypeFloat64, Nullable: true}},
		{Name: "rw", Type: TypeRow, Nullable: true, Fields: []Column{{Name: "x", Type: TypeFloat64, Nullable: true}, {Name: "s", Type: TypeString, Nullable: true}}},
		{Name: "s", Type: TypeString, Nullable: true},
	}}
}

func pgNumericGoldenFile(t *testing.T, schema Schema) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "f": 2.5, "r": float32(0.5), "n": Decimal128{Lo: 25000000000}, "u": Decimal128{Lo: 12500000000},
			"af": []any{2.5, 1.5}, "rw": map[string]any{"x": 2.5, "s": "a"}, "s": "x"},
		{"id": int64(2), "f": nil, "r": nil, "n": nil, "u": nil, "af": nil, "rw": nil, "s": nil},
		{"id": int64(3), "f": -0.5, "r": float32(-2.5), "n": Decimal128{Lo: 5000000000}, "u": Decimal128{Lo: 7550000000},
			"af": []any{}, "rw": map[string]any{"x": nil, "s": "b"}, "s": ""},
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// pgNumericGoldenSHA256 is the sha256 of pgNumericGoldenFile over the
// unmarked schema, measured at 1f580f7d — the tree before
// Column.PGNumeric existed.
const pgNumericGoldenSHA256 = "0613b87f325473b699becf06d3cc83c1df1e762c1e3c4e2f7642192713defa6d"

// An UNMARKED column writes the bytes it wrote before the marker existed —
// the whole file, footer and declared-schema blob included — and its record
// is the record it was (`omitempty`): every existing file and catalog entry
// is byte-identical.
func TestAnUnmarkedColumnWritesTheBytesItWroteBeforePGNumeric(t *testing.T) {
	sum := sha256.Sum256(pgNumericGoldenFile(t, pgNumericGoldenSchema()))
	if got := hex.EncodeToString(sum[:]); got != pgNumericGoldenSHA256 {
		t.Errorf("an unmarked schema's file changed: sha256 %s, want %s", got, pgNumericGoldenSHA256)
	}
	b, err := json.Marshal(Column{Name: "f", Type: TypeFloat64, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), `{"name":"f","type":`+pgnJSONInt(TypeFloat64)+`,"nullable":true}`; got != want {
		t.Errorf("an unmarked FLOAT64 record is %s, want %s", got, want)
	}
	var old Column
	if err := json.Unmarshal([]byte(`{"name":"f","type":`+pgnJSONInt(TypeFloat64)+`,"nullable":true}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.PGNumeric {
		t.Error("a record with no pg_numeric key reads as marked")
	}
}

func pgnJSONInt(id TypeID) string {
	b, _ := json.Marshal(id)
	return string(b)
}

// A MARKED column's footer carries the marker in its declared-schema blob,
// and a reader that has only the file reads it back on the DOUBLE leaf it
// was written on — the leaf's type, the values and every other column
// unchanged.
func TestTheFooterCarriesThePGNumericMarker(t *testing.T) {
	plain := pgNumericGoldenSchema()
	marked := plain.Clone()
	marked.Columns[1].PGNumeric = true
	data := pgNumericGoldenFile(t, marked)
	if !bytes.Contains(data, []byte(`"name":"f","type":`+pgnJSONInt(TypeFloat64)+`,"nullable":true,"pg_numeric":true}`)) {
		t.Error("the marked column's footer blob does not carry pg_numeric")
	}
	if n := bytes.Count(data, []byte(`pg_numeric`)); n != 1 {
		t.Errorf("the footer names pg_numeric %d times, want once (the marked column only)", n)
	}
	read := func(data []byte) (Schema, []map[string]any) {
		t.Helper()
		r, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := r.ReadRows(nil)
		if err != nil {
			t.Fatal(err)
		}
		return r.Schema(), rows
	}
	ms, mrows := read(data)
	ps, prows := read(pgNumericGoldenFile(t, plain))
	for i, c := range ms.Columns {
		want := ps.Columns[i]
		want.PGNumeric = c.Name == "f"
		if !pgnSameColumn(c, want) {
			t.Errorf("column %s reads back %+v, want %+v", c.Name, c, want)
		}
	}
	for _, c := range ps.Columns {
		if c.PGNumeric {
			t.Errorf("unmarked column %s reads back marked", c.Name)
		}
	}
	if len(mrows) != len(prows) {
		t.Fatalf("marked file reads %d rows, unmarked %d", len(mrows), len(prows))
	}
	for i := range mrows {
		for k, v := range prows[i] {
			if a, b := pgnRender(mrows[i][k]), pgnRender(v); a != b {
				t.Errorf("row %d column %s: marked file reads %s, unmarked %s", i, k, a, b)
			}
		}
	}
}

func pgnSameColumn(a, b Column) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func pgnRender(v any) string {
	b, _ := json.Marshal(v)
	return strings.TrimSpace(string(b))
}
